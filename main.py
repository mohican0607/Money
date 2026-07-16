"""
KOSPI·KOSDAQ 뉴스–급등 상관 및 익일 후보 리포트.

실행 (프로젝트 루트, 출력: output/):

  python main.py
    → 오늘이 거래일 N일 때, N+1 거래일(T) 급등 후보. output/report_dated_by_MMDD.html 에 해당 N 블록 갱신(표는 예측 후보만)
    → 거래일 14:30 자동 실행·리포트 열기: scripts/run_daily_1500.ps1 (등록 예: scripts/register_task_scheduler_example.ps1)

  python main.py 20260401
    → 관측일 T=2026-04-01(거래일), 기준일 N=T 직전 거래일.
      T가 현재 N+1을 넘으면 거부. T=현재 N+1이면 N일 14:30(KST)부터 실행.
      output/report_dated_by_MMDD.html 에 해당 T 블록 추가·재실행 시 해당 블록만 갱신

  python main.py 20260102 20260410
    → 관측일 From~To 구간(양 끝 포함, **KRX 거래일만**). To는 현재 기준 N+1 거래일 이하.
      구간에 현재 N+1이 포함되면 N일 14:30(KST)부터 실행.
      월별 HTML·목차. --no-report-expand 로 병합 끔
      **주의:** 장중 매수용으로는 `python main.py`(인자 없음)만 쓰세요.
      From~To 로 과거·미래를 한꺼번에 돌리면 학습·백필로 수십 분~1시간+ 걸립니다.

  python main.py --weekly
    → config REPORT_TEST_DAY_START~END 구간을 달력 월 단위로 묶어
      output/report_YYYY.MM.html (주간 ISO 탭, 탭 안 일자 위→아래) 및 report_index_monthly.html

뉴스: .env 에 NAVER_CLIENT_ID/SECRET 있으면 네이버 API, 없으면 기본 Google News RSS(키 불필요, USE_GOOGLE_NEWS_RSS_FALLBACK=0 으로 끔).
테스트: MOCK_NEWS=1
자동 열기: 실행 후 이번에 만든 리포트 HTML 중 수정 시각이 가장 최근인 파일 하나만 연다(목차 제외). 끄려면 NO_AUTO_OPEN_OUTPUT=1

매수 시나리오: N일 장마감 전(약 14:00~14:50) 주문, N+1 급등 노림.
예측 뉴스는 관측일 T 직전 KRX 거래일의 ``NEWS_CUTOFF_*``(KST)까지(USE_DECISION_NEWS_INTRADAY_CUTOFF).
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_KST = ZoneInfo("Asia/Seoul")


def main() -> None:
    """
    CLI 진입점: ``_parse_cli`` 결과에 따라 주간/구간/단일일 모드로 파이프라인 실행 후 HTML 저장.

    환경 변수 ``NO_AUTO_OPEN_OUTPUT`` 이 설정되지 않았으면 Windows/macOS에서 이번에 생성한 리포트 HTML 중 최신 파일 하나만 연다.
    """
    from src import config, prediction_accuracy_cache, report, trading_calendar
    from src.pipeline import (
        observation_day_forward_mode,
        omit_target_calendar_before_close,
        open_report_outputs,
        parse_cli,
        print_usage,
        render_monthly_batch,
        run_pipeline,
        should_skip_ohlcv_right_gap,
    )
    if sys.platform == "win32":
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.reconfigure(encoding="utf-8")
            except (AttributeError, OSError, ValueError):
                pass

    mode, arg_date, range_end, snap_mode, use_freeze = parse_cli()
    if mode == "usage":
        print_usage()
        sys.exit(0 if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help") else 2)

    if mode == "serve_live_quotes":
        from src.report.live_quotes import run_live_quotes_server

        run_live_quotes_server()
        return

    from src.report.live_quotes import ensure_live_quotes_server_running

    ensure_live_quotes_server_running()

    now_kst = datetime.now(trading_calendar.KST)
    today = now_kst.date()
    train_start = config.TRAIN_START_DEFAULT
    test_start = config.TEST_START

    if mode == "weekly":
        report_day_end = config.REPORT_TEST_DAY_END
        end_date = min(today, date(2026, 12, 31))
        if report_day_end:
            end_date = min(end_date, report_day_end)
        trading_days = trading_calendar.trading_sessions_in_range(train_start, end_date)
        test_days = [t for t in trading_days if t >= test_start]
        rs, re = config.REPORT_TEST_DAY_START, config.REPORT_TEST_DAY_END
        if rs and re:
            test_days = [t for t in test_days if rs <= t <= re]
        elif config.MAX_TEST_DAYS > 0:
            test_days = test_days[: config.MAX_TEST_DAYS]
        if not test_days:
            print("월간 배치 모드: 테스트할 거래일이 없습니다.")
            return

        omit_t_cal = omit_target_calendar_before_close(test_days, now_kst=now_kst)
        if omit_t_cal:
            for t in sorted(omit_t_cal):
                print(
                    f"관측일 T={t} 14:30 전: 해당일 캘린더 뉴스는 수집하지 않습니다.",
                    flush=True,
                )

        po = run_pipeline(
            test_days,
            end_date,
            train_snapshot_mode=snap_mode,
            train_snapshot_cal_scope=None,
            include_target_calendar_news=True,
            omit_target_calendar_days=omit_t_cal,
        )
        test_range_label = (
            f"{rs} ~ {re} (데이터·거래일: {end_date}까지)" if rs and re else f"{test_start} ~ {end_date}"
        )
        merge_monthly = "--no-report-expand" not in sys.argv[1:]
        out_files = render_monthly_batch(
            po,
            test_range_label=test_range_label,
            merge_existing_monthly_days=merge_monthly,
        )
        open_report_outputs(out_files)
        return

    if mode == "range":
        assert arg_date is not None and range_end is not None
        d_from, d_to = arg_date, range_end
        end_date = min(date(2026, 12, 31), d_to)
        test_days = trading_calendar.trading_sessions_in_range(d_from, d_to)
        if not test_days:
            print(f"구간 {d_from} ~ {d_to} 에 포함되는 거래일(관측일)이 없습니다.")
            return
        merge_monthly = "--no-report-expand" not in sys.argv[1:]
        if not merge_monthly:
            print(
                "--no-report-expand: 파이프라인·월간 HTML 모두 From~To 일자만 반영합니다.",
                flush=True,
            )

        future_td = [t for t in test_days if t > today]
        if future_td:
            print(
                f"구간 {d_from.isoformat()}~{d_to.isoformat()}: "
                f"KST 오늘({today}) 이후 관측일 {len(future_td)}일 "
                f"({future_td[0].isoformat()}~{future_td[-1].isoformat()})은 "
                "예측 전용(실제 상승률·누적 정확도는 확정 후 갱신)으로 처리합니다.",
                flush=True,
            )

        omit_t_cal = omit_target_calendar_before_close(test_days, now_kst=now_kst)
        if omit_t_cal:
            for t in sorted(omit_t_cal):
                print(
                    f"관측일 T={t} 14:30 전: 해당일 캘린더 뉴스는 수집하지 않습니다.",
                    flush=True,
                )

        if snap_mode == "append_learning":
            print(
                "--append-rebuild-learning: ML joblib 재사용, "
                "마지막 train_events 이후 급등 이벤트 증분 병합 + "
                f"거래일 {len(test_days)}일 예측·freeze·rebuild_learning 병합.",
                flush=True,
            )

        if use_freeze:
            print(
                "--use-freeze: --rebuild-train-snapshot 구간에서도 prediction_freeze_by_t.json 을 재사용합니다.",
                flush=True,
            )
        elif snap_mode != "rebuild":
            print(
                "구간 실행: 각 T에 예측 고정 캐시가 있으면 예측 후보를 유지하고, "
                "없는 일만 신규 계산합니다.",
                flush=True,
            )

        po = run_pipeline(
            test_days,
            end_date,
            train_snapshot_mode=snap_mode,
            train_snapshot_cal_scope=(d_from, d_to),
            include_target_calendar_news=True,
            skip_news_fetch_after=today,
            omit_target_calendar_days=omit_t_cal,
            respect_prediction_freeze=use_freeze,
        )
        test_range_label = (
            f"{d_from.isoformat()} ~ {d_to.isoformat()} "
            f"(거래일만 · OHLCV/뉴스 조회 기준일 {today}까지, 리포트 범위는 {end_date}까지)"
        )
        out_files = render_monthly_batch(
            po,
            test_range_label=test_range_label,
            merge_existing_monthly_days=merge_monthly,
        )
        open_report_outputs(out_files)
        return

    # daily: N=today → T=next trading day
    # dated: argv YYYYMMDD 를 관측일 T로 해석, 기준일 N=T 직전 거래일
    if mode == "daily":
        n_day = today
        if not trading_calendar.is_trading_day(n_day):
            print(f"{n_day} 은(는) 거래일이 아닙니다. 거래일에 14:30 스케줄로 실행하세요.")
            return
        try:
            t_day = trading_calendar.next_trading_day_after(n_day)
        except ValueError as e:
            print(e)
            return
    else:
        assert arg_date is not None
        t_day = arg_date
        try:
            n_day = trading_calendar.last_trading_day_before(t_day)
        except ValueError as e:
            print(e)
            return

    end_date = max(today, t_day)
    end_date = min(end_date, date(2026, 12, 31))

    test_days = [t_day]
    if t_day < train_start or t_day > end_date:
        print(f"관측일 {t_day} 이(가) 캘린더 범위에 없습니다. end_date={end_date}")
        return

    forward_prediction_only = observation_day_forward_mode(
        t_day,
        today=today,
        pipeline_forward_only=False,
        now_kst=now_kst,
    )
    before_open_n = (
        n_day == today
        and trading_calendar.is_trading_day(n_day)
        and trading_calendar.is_before_krx_regular_open_kst(n_day, now_kst=now_kst)
    )

    skip_ohlcv_gap = should_skip_ohlcv_right_gap(n_day, now_kst=now_kst)
    skip_news_fetch_after = today if n_day > today else None

    n_calendar_not_closed = (
        n_day == today
        and trading_calendar.is_trading_day(n_day)
        and trading_calendar.is_before_krx_regular_close_kst(n_day, now_kst=now_kst)
    )
    omit_t_calendar = frozenset({t_day}) if n_calendar_not_closed else frozenset()
    if n_calendar_not_closed:
        print(
            f"기준일 N={n_day} 장 마감 전: 관측일 T={t_day} 캘린더 뉴스는 수집하지 않습니다.",
            flush=True,
        )
    if snap_mode == "append_learning":
        print(
            f"--append-rebuild-learning: T={t_day.isoformat()} - "
            "예측 고정 캐시가 있으면 14:30 예측 후보를 유지하고, "
            "실제 상승률·테마·rebuild_learning(미스 진단) 병합 저장합니다.",
            flush=True,
        )

    cal_scope: tuple[date, date] | None = None
    if snap_mode in ("rebuild", "append_learning") and not forward_prediction_only:
        cal_scope = (t_day, t_day)

    po = run_pipeline(
        test_days,
        end_date,
        include_target_calendar_news=True,
        forward_prediction_only=forward_prediction_only,
        train_snapshot_mode=snap_mode,
        train_snapshot_cal_scope=cal_scope,
        skip_ohlcv_gap_download=skip_ohlcv_gap,
        omit_target_calendar_days=omit_t_calendar,
        skip_news_fetch_after=skip_news_fetch_after,
        respect_prediction_freeze=use_freeze,
    )
    if po.day_reports and po.day_reports[0].trading_day != t_day:
        actual_t = po.day_reports[0].trading_day
        print(
            f"관측일 보정: 계산된 T={t_day.isoformat()} 대신 "
            f"리포트 데이터 T={actual_t.isoformat()} 를 사용합니다.",
            flush=True,
        )
        t_day = actual_t

    meta_compact = {
        "train_range": (
            f"{po.train_start} ~ 각 관측일 T 직전까지(워크포워드, "
            f"기본 관측 시작 {po.test_start})"
        ),
        "test_range": f"단일 실행 N={n_day} → T={t_day}",
        "threshold": f"{config.BIG_MOVE_THRESHOLD*100:.0f}%",
        "news_source": po.news_source,
        "use_decision_cutoff": config.USE_DECISION_NEWS_INTRADAY_CUTOFF,
        "cutoff_kst": f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}",
        "run_subtitle": f"기준일 N={n_day.isoformat()} → 관측 거래일 T={t_day.isoformat()}",
        "n_days": 1,
        "total_preds": len(po.day_reports[0].predictions) if po.day_reports else 0,
        "prediction_only": forward_prediction_only,
    }
    if po.movers_data_note:
        meta_compact["movers_data_note"] = po.movers_data_note
    if meta_compact.get("prediction_only"):
        meta_compact["cumulative_track_hint"] = (
            "저장된 예측≥임계 이력이 있으면 누적 앞 숫자(A%)는 실제≥임계 적중 비율(맞춘 건수÷이력 개수). "
            f"{prediction_accuracy_cache.track_path_display()}. "
            "이번 T 확정 후 같은 종목이 다시 표에 오면 캐시가 갱신됩니다."
        )

    rollup_path = config.OUTPUT_DIR / f"report_dated_by_{t_day.strftime('%m%d')}.html"
    report.render_dated_n_report(
        n_day=n_day,
        t_day=t_day,
        day=po.day_reports[0],
        meta=meta_compact,
        is_live_n=n_day == today,
        before_open_n=before_open_n,
        rollup_path=rollup_path,
        row_id_prefix=f"n{n_day.strftime('%Y%m%d')}-",
    )
    print(
        f"완료: {rollup_path} "
        f"(기준일 N={n_day.isoformat()} · 관측일 T={t_day.isoformat()} 블록 반영)"
    )
    open_report_outputs([rollup_path])


if __name__ == "__main__":
    _t0 = time.perf_counter()
    _started_kst = datetime.now(_KST)
    try:
        from src.pipeline.cli import _parse_cli
        from src.pipeline.early_validate import validate_cli_or_none

        _mode, _arg_date, _range_end, _, _ = _parse_cli()
        _cli_err = validate_cli_or_none(_mode, _arg_date, _range_end)
        if _cli_err:
            print(_cli_err, file=sys.stderr)
            sys.exit(2)
        main()
    finally:
        _ended_kst = datetime.now(_KST)
        _sec = time.perf_counter() - _t0
        if _sec >= 60:
            _total_s = int(round(_sec))
            _min, _srem = divmod(_total_s, 60)
            _t_start = f"{_started_kst.hour:02d}시 {_started_kst.minute:02d}분"
            _t_end = f"{_ended_kst.hour:02d}시 {_ended_kst.minute:02d}분"
            print(f"총 소요시간: {_min:02d}분 {_srem:02d}초 ({_t_start} ~ {_t_end})")
