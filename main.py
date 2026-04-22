"""
KOSPI·KOSDAQ 뉴스–급등 상관 및 익일 후보 리포트.

실행 (프로젝트 루트, 출력: output/):

  python main.py
    → 오늘이 거래일 N일 때, N+1 거래일(T) 급등 후보. output/report_dated_by_n.html 에 해당 N 블록 갱신(표는 예측 후보만)
    → 거래일 15:00 자동 실행·리포트 열기: scripts/run_daily_1500.ps1 (등록 예: scripts/register_task_scheduler_example.ps1)

  python main.py 20260401
    → N=2026-04-01, T=N+1 거래일 후보. output/report_dated_by_n.html 에 20260401 블록 추가·재실행 시 해당 블록만 갱신
    → N이 오늘이면 예측 후보만 표시. N이 과거면 pykrx로 시장 20%↑와 예측을 함께 표시(OHLCV 샘플 밖 급등 포함).

  python main.py 20260102 20260410
    → 관측 거래일 T가 위 구간에 있는 날만 배치, 월별 HTML·목차 (--weekly 와 동일 형식)

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
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from tqdm import tqdm

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src import (
    config,
    disclosure,
    features,
    market_index,
    news,
    predict,
    prediction_accuracy_cache,
    report,
    stocks,
    train_snapshot,
    trading_calendar,
)


def _collect_calendar_days_for_trading_range(
    trading_days: list[date],
    *,
    include_target_calendar_days: bool = False,
    target_calendar_trading_days: frozenset[date] | None = None,
    omit_target_calendar_days: frozenset[date] | None = None,
) -> list[date]:
    """
    뉴스를 내려받아야 하는 **캘린더 일자** 집합을 만듭니다.

    각 거래일 ``t`` 에 대해 ``news.news_fetch_calendar_span(t)`` 의 연속 구간을 합치고,
    ``include_target_calendar_days`` 가 True이면 **모든** ``t`` 의 캘린더일(관측일 당일)도 포함합니다.
    ``target_calendar_trading_days`` 가 있으면, 그 집합에 들어 있는 ``t`` 만 당일 캘린더를 추가합니다
    (배치 리포트에서 비교 표·상승 이유용으로 **테스트 관측일만** 받을 때 사용).
    ``omit_target_calendar_days`` 에 있는 관측일 ``t`` 는 당일 추가를 생략합니다.
    """
    omit = omit_target_calendar_days or frozenset()
    s: set[date] = set()
    for t in trading_days:
        a, b = news.news_fetch_calendar_span(t)
        d = a
        while d <= b:
            s.add(d)
            d += timedelta(days=1)
        if include_target_calendar_days and t not in omit:
            s.add(t)
        elif (
            target_calendar_trading_days is not None
            and t in target_calendar_trading_days
            and t not in omit
        ):
            s.add(t)
    return sorted(s)


def _fetch_news_for_calendar_days(cal_days: list[date], *, fetch_until: date | None = None) -> dict:
    """
    ``<naver|google|mock>/YYYY/day_YYYYMMDD.json`` 단위로 뉴스를 채워 ``{날짜: rows}`` 딕셔너리를 만듭니다.

    종목코드별 조회가 아니라 **캘린더 일자별** 시장 뉴스입니다(네이버/RSS 쿼리는 날짜+증시 키워드).
    ``NEWS_FETCH_MAX_WORKERS`` > 1 이면 일자를 병렬로 처리(일자마다 별도 ``Session``).
    ``fetch_until`` 이 있으면 그 날짜를 초과한 미래 캘린더일은 조회하지 않고 빈 리스트로 둡니다.
    """
    if fetch_until is not None:
        allowed_days = [d for d in cal_days if d <= fetch_until]
        skipped_days = [d for d in cal_days if d > fetch_until]
    else:
        allowed_days = list(cal_days)
        skipped_days = []

    if not allowed_days:
        return {d: [] for d in skipped_days}

    if len(allowed_days) <= 1 or config.NEWS_FETCH_MAX_WORKERS <= 1:
        out: dict = {}
        sess = requests.Session()
        try:
            for d in tqdm(allowed_days, desc="뉴스(일자별 캐시)"):
                out[d] = news.fetch_news_for_calendar_day(d, session=sess)
        finally:
            sess.close()
        for d in skipped_days:
            out[d] = []
        return out

    def _one_day(day: date) -> tuple[date, list]:
        sess = requests.Session()
        try:
            return day, news.fetch_news_for_calendar_day(day, session=sess)
        finally:
            sess.close()

    out = {}
    with ThreadPoolExecutor(max_workers=config.NEWS_FETCH_MAX_WORKERS) as ex:
        futures = [ex.submit(_one_day, d) for d in allowed_days]
        for fut in tqdm(
            as_completed(futures),
            total=len(allowed_days),
            desc=f"뉴스(일자별·워커 {config.NEWS_FETCH_MAX_WORKERS})",
        ):
            d, rows = fut.result()
            out[d] = rows
    for d in skipped_days:
        out[d] = []
    return out


def _parse_yyyymmdd(s: str) -> date | None:
    """``YYYYMMDD`` 8자리 숫자 문자열을 ``date`` 로 파싱. 형식 오류 시 None."""
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        return None
    y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
    return date(y, m, d)


def _parse_cli() -> tuple[str, date | None, date | None, str]:
    """
    ``sys.argv`` 를 파싱해 실행 모드와 날짜 인자를 돌려줍니다.

    Returns:
        ``(mode, arg_date, range_end, train_snapshot_mode)``.
        ``train_snapshot_mode`` 는 ``none`` | ``use`` | ``rebuild``.
        ``range`` 일 때만 ``arg_date`` 와 ``range_end`` 가 둘 다 채워짐.
    """
    raw = [a for a in sys.argv[1:] if a]
    has_rebuild = "--rebuild-train-snapshot" in raw
    has_no_snap = "--no-train-snapshot" in raw
    if has_rebuild:
        snap_mode = "rebuild"
    elif has_no_snap:
        snap_mode = "none"
    else:
        # 기본: 스냅샷 로드 + 미반영 캘린더 뉴스만 병합(--use-train-snapshot 생략 가능)
        snap_mode = "use"
    argv = [
        a
        for a in raw
        if a
        not in (
            "--use-train-snapshot",
            "--rebuild-train-snapshot",
            "--no-train-snapshot",
        )
    ]
    if not argv:
        return "daily", None, None, snap_mode
    if argv[0] in ("--weekly", "--weekly-report", "-w"):
        return "weekly", None, None, snap_mode
    if argv[0] in ("-h", "--help"):
        return "usage", None, None, snap_mode
    if len(argv) >= 2:
        d0 = _parse_yyyymmdd(argv[0])
        d1 = _parse_yyyymmdd(argv[1])
        if d0 is not None and d1 is not None:
            if d0 > d1:
                d0, d1 = d1, d0
            return "range", d0, d1, snap_mode
    d = _parse_yyyymmdd(argv[0])
    if d is not None:
        return "dated", d, None, snap_mode
    print(f"인식할 수 없는 인자: {argv[0]}", file=sys.stderr)
    return "usage", None, None, snap_mode


def _print_usage() -> None:
    """표준 출력에 CLI 도움말을 인쇄합니다."""
    print(
        """사용법:
  python main.py
      오늘(N) 기준 → N+1 거래일(T) 후보, output/report_dated_by_n.html (해당 N 블록)
  python main.py YYYYMMDD
      지정 N → T=N+1 거래일 후보, 동일 파일에서 해당 N 블록만 추가·갱신
  python main.py YYYYMMDD YYYYMMDD
      From~To 거래일 구간 배치, report_YYYY.MM.html 및 report_index_monthly.html
  python main.py --weekly
      월간 배치 (config REPORT_TEST_DAY_START ~ END, --weekly 이름은 호환용)

  학습 스냅샷 (과거 급등-뉴스 BreakoutEvent, data/cache/train/breakout_train_snapshot.json):
  (플래그 없음, 기본)
      스냅샷을 읽어 재사용하고, 이번 실행에 필요한 캘린더 일 중 스냅샷에 없는 뉴스 일자만 반영해
      이벤트를 병합·저장. 두 날짜 구간이면 미반영 판단은 인자 캘린더 구간으로 한정.
  --no-train-snapshot
      스냅샷을 읽지도 쓰지도 않고, 매번 전체 재계산만 수행(예전 동작).
  --use-train-snapshot
      기본과 동일(명시용·호환).
  --rebuild-train-snapshot
      항상 전체 재계산 후 스냅샷을 새로 저장(이 플래그가 최우선).

  예: python main.py 20260401 20260414
"""
    )


@dataclass
class PipelineOut:
    """
    ``_run_pipeline`` 한 번 호출의 집계 결과.

    월간 HTML 메타(훈련 구간, 뉴스 출처, 상관 키워드)와 late-뉴스 프로브 카운터,
    pykrx 실패 시 리포트 각주 문구를 담습니다.
    """

    day_reports: list[report.DayReport]
    news_source: str
    correlation_rows: list[tuple[str, int]]
    train_start: date
    test_start: date
    end_date: date
    late_below_n: int
    late_below_kw: int
    late_gte_n: int
    late_gte_kw: int
    movers_data_note: str | None = None


def _movers_data_note_for_report(krx_unavailable: bool) -> str | None:
    """
    pykrx·KRX 전종목 등락률을 쓰지 못할 때 리포트 상단에 넣을 HTML 메타 각주 문구.

    ``SAMPLE_TICKERS`` 사용 여부에 따라 안내 문장이 달라집니다.
    """
    if not krx_unavailable:
        return None
    if config.SAMPLE_TICKERS_N:
        return (
            "시장 전체 20%↑: pykrx·KRX 일별 등락률 조회에 실패했습니다. "
            f"아래 표의 「실제」 급등은 다운로드한 OHLCV 안에서만 집계됩니다. SAMPLE_TICKERS={config.SAMPLE_TICKERS_N}(으)로 "
            "표본이 좁혀져 네이버 상한가 등 전 시장 현황과 다를 수 있습니다. SAMPLE_TICKERS를 비우고 재실행하는 것을 권장합니다."
        )
    return (
        "pykrx·KRX 전종목 등락률 조회에 실패했습니다. 아래 「실제」 20%↑는 OHLCV에 담긴 상장 종목(전종목 일봉 다운로드 시 전 시장에 가깝게) "
        "기준입니다. 네이버 시세와 맞추려면 pykrx가 동작하도록 네트워크·영업일을 확인하세요."
    )


def _enrich_rows_news_evidence(
    rows: list[dict],
    early_rows: list[tuple[date, dict]],
    actual_ctx_rows: list[tuple[date, dict]],
) -> None:
    """
    비교 표 row에 ``pred_news_hits`` / ``actual_news_hits`` 를 채웁니다.

    ``include_target_calendar_news`` 일 때 호출됩니다(단일일·월간·구간 공통).
    """
    for r in rows:
        nm = r["name"]
        kw = r.get("keywords") or []
        r["pred_news_hits"] = news.match_stock_news_rows(early_rows, nm, kw, limit=8)
        r["actual_news_hits"] = news.match_stock_news_rows(actual_ctx_rows, nm, kw, limit=8)


def _open_report_outputs(html_paths: Sequence[Path]) -> None:
    """
    ``NO_AUTO_OPEN_OUTPUT`` 이 비어 있으면, 이번 실행에서 전달된 경로 중 **하나만** 연다.

    ``report_*.html`` 이 있으면 그중 ``index`` 가 이름에 들어가지 않은 파일만 후보로 두고,
    수정 시각(``st_mtime``)이 가장 최근인 파일을 고른다. 후보가 없으면 전달된 파일 전체에서 최신만 연다.
    ``output`` 폴더는 열지 않는다.

    Windows: ``os.startfile`` — macOS: ``open``.
    """
    if os.getenv("NO_AUTO_OPEN_OUTPUT", "").strip().lower() in ("1", "true", "yes"):
        return
    existing = [Path(p).resolve() for p in html_paths if Path(p).is_file()]
    if not existing:
        return

    def _is_primary_report(p: Path) -> bool:
        n = p.name.lower()
        return n.startswith("report_") and n.endswith(".html") and "index" not in n

    primary = [p for p in existing if _is_primary_report(p)]
    pool = primary if primary else existing
    latest = max(pool, key=lambda p: p.stat().st_mtime)
    try:
        if sys.platform == "win32":
            os.startfile(str(latest))  # noqa: S606
        elif sys.platform == "darwin":
            import subprocess

            subprocess.run(["open", str(latest)], check=False)
    except OSError:
        pass


def _pred_reason_hit_line(pr: predict.PredictionRow | None) -> str:
    """표 ``이유/차이`` 열: 키워드 일치 개수만 짧게 (예: ``36개 일치``). 상세 문장은 툴팁."""
    if pr is None:
        return "—"
    for line in pr.reasons:
        if "급등일 뉴스 키워드" in line and "일치" in line:
            m = re.search(r"(\d+)\s*개\s*일치", line)
            if m:
                return f"{m.group(1)}개 일치"
            return line
    return "—"


def _actual_over_pred_ratio(pred_ret: float | None, actual_ret: float | None) -> float | None:
    """
    일별 **정합도** ``min(|실제%|, |예측%|) / max(|실제%|, |예측%|)`` (0~1).

    ``actual_ret`` 은 소수(0.2=20%), ``pred_ret`` 은 퍼센트 값.
    두 값의 크기가 완전히 같을 때만 1(100%), 차이가 커질수록 0에 가까워집니다.
    """
    if pred_ret is None or actual_ret is None:
        return None
    p = abs(float(pred_ret))
    a = float(actual_ret)
    if not math.isfinite(p) or not math.isfinite(a):
        return None
    if a < 0:
        return 0.0
    if abs(p) < 1e-9:
        return None
    a_pct = abs(a * 100.0)
    den = max(a_pct, p)
    if den < 1e-9:
        return None
    return min(a_pct, p) / den


def _prediction_row_strict_or_loose(
    code: str,
    names: dict[str, str],
    train_events: list,
    blob: str,
    scoring_ctx: tuple,
    min_hits: int,
) -> predict.PredictionRow | None:
    """
    엄격(``min_hits``) 행이 없으면 완화(``min_hits=0``)로 한 번 더 조회합니다.
    뉴스 재보강 후 갭 HTML·툴팁이 예측 행 없이 끊기지 않게 합니다.
    """
    pr = predict.prediction_row_for_code(
        code, names, train_events, blob, scoring_ctx, min_hits
    )
    if pr is not None:
        return pr
    return predict.prediction_row_for_code(
        code, names, train_events, blob, scoring_ctx, 0
    )


def _enrich_cumulative_actual_over_pred_from_history(day_reports: list) -> None:
    """
    ``pred_high_history`` 가 있으면 ``cumulative_accuracy_avg`` 를 이력만으로 다시 씁니다.

    각 이력 행에 대해 ``min(|실제%|,|예측%|)/max(|실제%|,|예측%|)``(0~1)을 더합니다.
    예측이 ``0`` 에 가깝거나 실적·예측 값이 없으면 그 항목은 ``0``을 더합니다.
    평균은 **이력 행 개수**로 나눕니다.
    """
    for dr in day_reports:
        for r in dr.rows_compare:
            if not bool(r.get("pred_high")):
                continue
            hist = r.get("pred_high_history") or []
            items = sorted(
                (h for h in hist if isinstance(h, dict)),
                key=lambda h: str(h.get("t", "")),
            )
            n = len(items)
            if n == 0:
                continue
            acc = 0.0
            for h in items:
                pr = h.get("pred_pct")
                ap = h.get("actual_pct")
                if ap is None or pr is None:
                    acc += 0.0
                    continue
                prf = abs(float(pr))
                apv = float(ap)
                if apv < 0:
                    acc += 0.0
                    continue
                apf = abs(apv)
                den = max(prf, apf)
                if den < 1e-9:
                    acc += 0.0
                else:
                    acc += min(prf, apf) / den
            r["cumulative_accuracy_avg"] = acc / n
            r["cumulative_accuracy_from_hist"] = True


def _enrich_cumulative_hit_rate(
    day_reports: list[report.DayReport], *, threshold_pct: float
) -> None:
    """
    ``pred_high_history`` 기준으로, 예측이 ``threshold_pct`` 이상이었던 날의 실제 분포를 집계합니다.
    괄호 표기용으로 ``cumulative_hit_x``(실제≥threshold 건수), ``cumulative_hit_z``(0<실제<threshold 건수),
    ``cumulative_hit_neg``(실제<0 건수), ``cumulative_hit_y``(실적 확정·예측≥임계 전체)를 넣습니다.
    """
    thr = float(threshold_pct)
    for dr in day_reports:
        for r in dr.rows_compare:
            if not bool(r.get("pred_high")):
                r["cumulative_hit_rate_pct"] = None
                r["cumulative_hit_x"] = None
                r["cumulative_hit_z"] = None
                r["cumulative_hit_neg"] = None
                r["cumulative_hit_y"] = None
                continue
            hist = r.get("pred_high_history") or []
            hits = 0
            mid_band = 0
            neg_band = 0
            n_known = 0
            for h in hist:
                if not isinstance(h, dict):
                    continue
                pr = h.get("pred_pct")
                if pr is None or float(pr) + 1e-9 < thr:
                    continue
                ap = h.get("actual_pct")
                if ap is None:
                    continue
                n_known += 1
                apf = float(ap)
                if apf >= thr - 1e-9:
                    hits += 1
                elif apf > 1e-9 and apf < thr - 1e-9:
                    mid_band += 1
                elif apf < -1e-9:
                    neg_band += 1
            r["cumulative_hit_rate_pct"] = (100.0 * hits / n_known) if n_known else None
            r["cumulative_hit_x"] = hits if n_known else None
            r["cumulative_hit_z"] = mid_band if n_known else None
            r["cumulative_hit_neg"] = neg_band if n_known else None
            r["cumulative_hit_y"] = n_known if n_known else None


def _enrich_cumulative_accuracy_avg(day_reports: list[report.DayReport]) -> None:
    """
    각 ``rows_compare`` 행에 ``cumulative_accuracy_avg`` 를 넣습니다.

    같은 종목코드에 대해, **이번 파이프라인 실행의 관측 거래일 T를 시간순**으로 보며
    일별 달성률 ``min(|실제%|/|예측%|,1)`` 를 쌓고, 해당 행 시점까지의 **산술 평균**(0~1)을 기록합니다.
    당일 비율을 계산할 수 없으면(예측·실제 중 하나 없음, 예측 0) 직전까지 평균만 씁니다.
    """
    history: dict[str, list[float]] = defaultdict(list)
    for dr in sorted(day_reports, key=lambda d: d.trading_day):
        for r in dr.rows_compare:
            # 전략 목표(익일 20%↑ 후보)와 맞추기 위해, 누적 정확도는 예측 20% 이상 후보만 집계.
            if not bool(r.get("pred_high")):
                r["cumulative_accuracy_avg"] = None
                continue
            code = str(r["code"])
            cur = _actual_over_pred_ratio(r.get("pred_ret"), r.get("actual_ret"))
            past = history[code]
            if cur is not None:
                r["cumulative_accuracy_avg"] = (sum(past) + cur) / (len(past) + 1)
                past.append(cur)
            elif past:
                r["cumulative_accuracy_avg"] = sum(past) / len(past)
            else:
                r["cumulative_accuracy_avg"] = None


def _pred_reason_fields(
    pr: predict.PredictionRow | None,
    reasons_html: str,
    *,
    summary_max: int = 72,
) -> dict[str, str | bool]:
    """
    표 ``이유/차이`` 열·툴팁용.

    ``pred_reason_hit_line`` 은 표용 짧은 표기(예: ``36개 일치``). 전체 문장은 툴팁 ``pr.reasons`` 에 있습니다.
    ``reasons_html`` 은 레거시(카드 등)용으로 유지.
    """
    hit_line = _pred_reason_hit_line(pr)
    if pr is not None:
        detail_html = "<br/>".join(pr.reasons)
        plain = " ".join(pr.reasons)
    else:
        detail_html = reasons_html
        plain = re.sub(r"<[^>]+>", " ", reasons_html or "")
        plain = " ".join(plain.split())
    plain = plain.strip()
    if not plain:
        return {
            "pred_reason_summary": "—",
            "pred_reason_hit_line": hit_line,
            "pred_reason_detail_html": "—",
            "pred_reason_use_tooltip": False,
        }
    use_tooltip = len(plain) > summary_max or (pr is not None and len(pr.reasons) > 2)
    if len(plain) <= summary_max:
        summary = plain
    else:
        summary = plain[: summary_max - 1].rstrip() + "…"
    return {
        "pred_reason_summary": summary,
        "pred_reason_hit_line": hit_line,
        "pred_reason_detail_html": detail_html,
        "pred_reason_use_tooltip": use_tooltip,
    }


def _gap_analysis_html_for_row(
    pred_ret: float | None,
    act: float | None,
    pr: predict.PredictionRow | None,
    keywords: list[str],
    blob: str,
    kospi_hint: str | None,
    late_blob: str,
    disclosure_hits: list[dict] | None = None,
    *,
    actual_intraday_pct: float | None = None,
) -> str:
    """
    표 한 줄에 붙는 「예측 vs 실제」 갭 설명 HTML 조각을 생성합니다.

    컷오프가 켜져 있으면 late 뉴스에 키워드가 있었는지 ``late_blob_covers_keywords`` 로 넘깁니다.
    """
    late_hit: bool | None = None
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF and late_blob and keywords:
        late_hit = news.late_blob_covers_keywords(late_blob, list(keywords))
    return predict.explain_return_gap_html(
        pred_ret_pct=pred_ret,
        actual_ret=act,
        actual_intraday_pct=actual_intraday_pct,
        prediction_row=pr,
        news_blob_early=blob,
        kospi_change_hint=kospi_hint,
        late_keywords_matched=late_hit,
        disclosure_hits=disclosure_hits,
    )


def _enrich_rows_disclosure_hits(rows: list[dict], target_day: date) -> None:
    """rows_compare 각 행에 네이버 종목 공시 hit를 붙입니다."""
    codes = [str(r.get("code", "")).zfill(6) for r in rows if r.get("code")]
    by_code = disclosure.fetch_disclosures_for_codes_on_day(target_day, codes)
    for r in rows:
        code = str(r.get("code", "")).zfill(6)
        hits = by_code.get(code, [])
        r["disclosure_hits"] = hits[:8]


def _apply_preclose_actual_snapshot_rows(
    rows_compare: list[dict],
    *,
    snapshot_pct_by_code: dict[str, float] | None,
) -> None:
    """
    장 마감 전 표시용으로 ``rows_compare`` 각 행에 ``actual_ret_intraday_pct`` 를 채웁니다.

    - ``snapshot_pct_by_code``(pykrx 퍼센트 포인트)가 있으면 우선 사용
    - 없으면 이미 계산된 ``actual_ret``(소수)를 퍼센트 포인트로 변환해 보조 표시
    """
    if not rows_compare:
        return
    for r in rows_compare:
        code = str(r.get("code", "")).zfill(6)
        v: float | None = None
        if snapshot_pct_by_code is not None:
            raw = snapshot_pct_by_code.get(code)
            if raw is not None and math.isfinite(float(raw)):
                v = float(raw)
        if v is None:
            ar = r.get("actual_ret")
            if ar is not None and math.isfinite(float(ar)):
                v = float(ar) * 100.0
        if v is None:
            continue
        r["actual_ret_intraday_pct"] = v
        r["actual_cell_pre_close_snapshot"] = True


def _fetch_live_intraday_snapshot_pct_by_code(
    t_day: date,
    *,
    returns_df=None,
    attempts: int = 3,
    sleep_sec: float = 0.8,
) -> dict[str, float] | None:
    """
    main 실행 시점에 pykrx 장중 등락률 스냅샷을 강제 재조회합니다.

    장중에는 응답이 일시적으로 비는 경우가 있어 짧게 재시도합니다.
    ``returns_df`` 가 있으면 pykrx 전종목 실패 시 OHLCV 기반 등락률 맵으로 폴백합니다.
    """
    last: dict[str, float] | None = None
    for i in range(max(1, attempts)):
        snap = stocks.try_krx_change_pct_by_code(t_day, returns_df=returns_df)
        if snap:
            return snap
        last = snap
        if i + 1 < max(1, attempts):
            time.sleep(max(0.0, sleep_sec))
    return last


def _apply_postclose_actual_snapshot_rows(
    rows_compare: list[dict],
    *,
    snapshot_pct_by_code: dict[str, float] | None,
    threshold: float,
) -> None:
    """
    장 마감 후(pykrx 조회 가능 시) ``rows_compare`` 실제값을 스냅샷으로 즉시 갱신합니다.

    - ``actual_ret`` 는 pykrx 퍼센트 포인트를 소수 수익률로 변환해 반영
    - 종가 확정 전용 괄호 표시는 제거
    """
    if not rows_compare or not snapshot_pct_by_code:
        return
    for r in rows_compare:
        code = str(r.get("code", "")).zfill(6)
        raw = snapshot_pct_by_code.get(code)
        if raw is None or not math.isfinite(float(raw)):
            continue
        act = float(raw) / 100.0
        r["actual_ret"] = act
        r["actual_big"] = bool(act >= threshold)
        r.pop("actual_cell_pre_close_snapshot", None)


def _backfill_day_actuals_from_returns(
    dr: report.DayReport,
    *,
    returns,
    threshold: float,
) -> None:
    """
    ``returns``(일봉 수익률)에서 당일 실제값을 다시 읽어 ``DayReport``를 보정합니다.

    긴 실행이 장중에 시작되어도, 실행 말미에 장이 끝났다면 오늘(T)의 실제값 표기를 업데이트하기 위한 후처리.
    """
    t_day = dr.trading_day
    any_changed = False
    for r in dr.rows_compare:
        code = str(r.get("code", "")).zfill(6)
        act = stocks.actual_return_on_date(returns, code, t_day)
        if act is None:
            continue
        r["actual_ret"] = float(act)
        r["actual_big"] = bool(float(act) >= threshold)
        # 종가 확정값이 있으면 장중 스냅샷 표시는 제거
        r.pop("actual_cell_pre_close_snapshot", None)
        any_changed = True

    if any_changed:
        dr.rows_compare.sort(key=lambda r: (not r["actual_big"], not r["pred_high"], r["code"]))

    movers = stocks.big_movers_on_date(returns, t_day, threshold)
    dr.actual_big_movers = [
        {
            "code": str(r["Code"]).zfill(6),
            "name": str(r["Name"]),
            "ret_pct": float(r["return_pct"]) * 100.0,
        }
        for _, r in movers.iterrows()
    ]


def _run_pipeline(
    test_days: list[date],
    end_date: date,
    *,
    include_target_calendar_news: bool = False,
    forward_prediction_only: bool = False,
    train_snapshot_mode: str = "none",
    train_snapshot_cal_scope: tuple[date, date] | None = None,
    skip_ohlcv_gap_download: bool = False,
    omit_target_calendar_days: frozenset[date] | None = None,
    skip_news_fetch_after: date | None = None,
) -> PipelineOut:
    """
    공통 데이터 파이프라인: OHLCV → 뉴스 캐시 → 급등 이벤트 → 일자별 예측·비교.

    Args:
        test_days: 관측 거래일 T 목록(루프 돌며 ``DayReport`` 생성).
        end_date: 가격·뉴스 달력 상한(오늘과 설정에 맞게 잘림).
        include_target_calendar_news: True면 각 **테스트 관측일 T** 캘린더 뉴스를 받고,
            비교 표 row에 뉴스·공시 매칭·상승 이유(참고)를 채웁니다.
        forward_prediction_only: True면 실제 급등(pykrx/OHLCV) 채우기·과거 비교 일부 생략(라이브 N=오늘).
        omit_target_calendar_days: 뉴스 fetch 시 해당 관측일 T의 **캘린더 일** 본문은 내려받지 않음( N 미마감 등).
        train_snapshot_mode: ``none`` | ``use`` | ``rebuild`` — 학습 스냅샷 재사용·갱신·전체 재생성.
        train_snapshot_cal_scope: ``(시작, 끝)`` 캘린더 일(포함) — ``use`` 모드에서 미반영 판단만 이 구간으로 한정.
            ``None`` 이면 이번 실행에 필요한 전체 캘린더 일자를 기준으로 병합합니다.
        skip_ohlcv_gap_download: True면 캐시 최신일 이후를 채우는 우측 보강 다운로드를 생략.
        skip_news_fetch_after: 지정 시 해당 일자를 초과한 미래 캘린더일 뉴스 조회를 생략한다.

    Returns:
        ``PipelineOut`` — ``_render_monthly_batch`` 또는 ``render_dated_n_report`` 에 넘김.
    """
    train_start = config.TRAIN_START_DEFAULT
    test_start = config.TEST_START

    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    today_kst = datetime.now(trading_calendar.KST).date()
    # 당일 장 마감 전·미래 일봉은 소스에 없을 수 있어 항상 OHLCV 요청 끝을 상한과 맞춤
    ohlcv_cap = trading_calendar.ohlcv_request_end_cap_today()
    ohlcv_end = min(end_date, ohlcv_cap)
    if ohlcv_end < end_date:
        print(
            f"가격 데이터: 일봉 상한을 {ohlcv_end} 로 둡니다 "
            f"(요청 끝 {end_date}, 장 마감 전·미확정 봉 제외).",
            flush=True,
        )

    # 오늘 데이터가 포함되면 캐시가 장중 값으로 고정되지 않도록 마지막 1일은 재조회해 덮어쓴다.
    refresh_tail_days = 1 if ohlcv_end >= today_kst else 0

    print("가격 데이터 수집(캐시 있으면 재사용)…")
    ohlcv = stocks.build_ohlcv_long(
        train_start - timedelta(days=10),
        ohlcv_end,
        force_full_listing=not forward_prediction_only,
        skip_gap_download=skip_ohlcv_gap_download,
        refresh_tail_days=refresh_tail_days,
    )
    returns = stocks.daily_returns_table(ohlcv)
    returns_ml = stocks.enrich_daily_returns_for_ml(returns)

    trading_days = trading_calendar.trading_sessions_in_range(train_start, end_date)
    news_trading_days = [t for t in trading_days if train_start <= t < test_start] + test_days
    target_cal_days = frozenset(test_days) if include_target_calendar_news else None
    cal_days = _collect_calendar_days_for_trading_range(
        news_trading_days,
        include_target_calendar_days=False,
        target_calendar_trading_days=target_cal_days,
        omit_target_calendar_days=omit_target_calendar_days,
    )

    news_source = news.describe_news_fetch_source()

    if skip_news_fetch_after is not None:
        skipped_n = sum(1 for d in cal_days if d > skip_news_fetch_after)
        if skipped_n > 0:
            print(
                f"뉴스 수집 일수 {len(cal_days)} ({news_source})… "
                f"[일자 병렬 워커 {config.NEWS_FETCH_MAX_WORKERS}] "
                f"(미래 {skipped_n}일은 조회 생략)"
            )
        else:
            print(
                f"뉴스 수집 일수 {len(cal_days)} ({news_source})… "
                f"[일자 병렬 워커 {config.NEWS_FETCH_MAX_WORKERS}]"
            )
    else:
        print(
            f"뉴스 수집 일수 {len(cal_days)} ({news_source})… "
            f"[일자 병렬 워커 {config.NEWS_FETCH_MAX_WORKERS}]"
        )
    news_by_calendar = _fetch_news_for_calendar_days(cal_days, fetch_until=skip_news_fetch_after)

    cal_day_set = set(cal_days)
    fp_snap = train_snapshot.fingerprint()

    def _filter_train_events(ev: list[features.BreakoutEvent]) -> list[features.BreakoutEvent]:
        return [e for e in ev if train_start <= e.trading_day < test_start]

    def _full_breakout_train() -> list[features.BreakoutEvent]:
        all_ev = features.build_breakout_events(
            returns,
            news_by_calendar,
            trading_calendar.news_window_for_target_trading_day,
            config.BIG_MOVE_THRESHOLD,
        )
        return _filter_train_events(all_ev)

    train_td_set = {
        d
        for d in returns["Date"].dt.date.unique()
        if train_start <= d < test_start
    }

    if train_snapshot_mode == "none":
        print("과거 급등-뉴스 이벤트 구축…")
        train_events = _full_breakout_train()
    elif train_snapshot_mode == "rebuild":
        print("과거 급등-뉴스 이벤트 전체 재계산(학습 스냅샷 저장)…")
        train_events = _full_breakout_train()
        train_snapshot.save_snapshot(train_events, set(cal_day_set), fp=fp_snap)
        print(f"학습 스냅샷 저장: {config.TRAIN_SNAPSHOT_PATH} (캘린더 {len(cal_day_set)}일)")
    else:
        # use
        snap = train_snapshot.load_snapshot()
        fp_ok = snap is not None and snap.fingerprint == fp_snap
        if train_snapshot_cal_scope is not None:
            s0, s1 = train_snapshot_cal_scope
            scope_cal = {d for d in cal_day_set if s0 <= d <= s1}
        else:
            scope_cal = set(cal_day_set)
        missing_scope = scope_cal - (snap.calendar_days_covered if snap else set())

        if not fp_ok:
            if snap is not None and snap.fingerprint != fp_snap:
                print("학습 스냅샷: 설정 지문 불일치 → 전체 재계산 후 저장.")
            else:
                print("학습 스냅샷: 파일 없음 또는 형식 오류 → 전체 재계산 후 저장.")
            print("과거 급등-뉴스 이벤트 구축…")
            train_events = _full_breakout_train()
            train_snapshot.save_snapshot(train_events, set(cal_day_set), fp=fp_snap)
            print(f"학습 스냅샷 저장: {config.TRAIN_SNAPSHOT_PATH}")
        elif not missing_scope:
            train_events = _filter_train_events(snap.events)
            print(
                f"과거 급등-뉴스: 학습 스냅샷 재사용 "
                f"({config.TRAIN_SNAPSHOT_PATH.name}, 훈련 이벤트 {len(train_events)}건)"
            )
            new_cov = snap.calendar_days_covered | cal_day_set
            if new_cov != snap.calendar_days_covered:
                train_snapshot.save_snapshot(train_events, new_cov, fp=fp_snap)
        else:
            print(
                f"과거 급등-뉴스: 스냅샷 병합 — 미반영 캘린더 일 {len(missing_scope)}일 "
                f"(인자 구간 기준 {len(scope_cal)}일 중)"
            )
            recompute_td = {
                d
                for d in train_td_set
                if news.calendar_days_for_breakout_training(d) & missing_scope
            }
            old = [e for e in snap.events if e.trading_day not in recompute_td]
            new = features.build_breakout_events_for_trading_days(
                returns,
                news_by_calendar,
                trading_calendar.news_window_for_target_trading_day,
                config.BIG_MOVE_THRESHOLD,
                recompute_td,
            )
            train_events = _filter_train_events(old + new)
            new_cov = snap.calendar_days_covered | cal_day_set
            train_snapshot.save_snapshot(train_events, new_cov, fp=fp_snap)
            print(
                f"학습 스냅샷 갱신 저장: 관련 거래일 {len(recompute_td)}일 재계산 → "
                f"{config.TRAIN_SNAPSHOT_PATH.name}"
            )

    kw_cooccur = Counter()
    for e in train_events:
        for w in e.news_keywords:
            kw_cooccur[w] += 1
    correlation_rows = kw_cooccur.most_common(40)

    listing = stocks.load_listing()
    codes = listing["Code"].astype(str).str.zfill(6).tolist()
    names = dict(zip(codes, listing["Name"].astype(str)))
    market_by_code = stocks.market_segment_by_code()

    ml_bundle = None
    if config.PRED_USE_ML_RANKER:
        try:
            from src import ml_move_rank

            ml_bundle = ml_move_rank.fit_or_load_classifier(
                train_events=train_events,
                returns_ml=returns_ml,
                news_by_calendar=news_by_calendar,
                listing_names=names,
                fp=fp_snap,
            )
        except Exception as e:
            print(f"ML 랭커 초기화 실패(휴리스틱만 사용): {e}", flush=True)
            ml_bundle = None

    ks11 = market_index.load_index_frame("KS11", test_start - timedelta(days=5), end_date)

    day_reports: list[report.DayReport] = []
    late_below_n = late_below_kw = late_gte_n = late_gte_kw = 0
    krx_fallback_warned = False
    krx_movers_unavailable_any = False

    for T in tqdm(test_days, desc="테스트일별 예측"):
        if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
            blob, late_blob = news.aggregate_early_late_for_target(news_by_calendar, T)
            news_titles = news.sample_titles_early_for_target(news_by_calendar, T, limit=12)
            early_rows, _ = news.classified_rows_for_target(news_by_calendar, T)
            actual_ctx_rows = news.rows_for_actual_context(news_by_calendar, T)
        else:
            ws, we = trading_calendar.news_window_for_target_trading_day(T)
            blob = predict.aggregate_news_for_window(news_by_calendar, ws, we)
            late_blob = ""
            news_titles = []
            d = ws
            while d <= we:
                for row in news_by_calendar.get(d, []):
                    if row.get("title"):
                        news_titles.append(row["title"])
                d += timedelta(days=1)
            news_titles = news_titles[:12]
            early_rows, _ = news.classified_rows_for_target(news_by_calendar, T)
            actual_ctx_rows = news.rows_for_actual_context(news_by_calendar, T)

        min_hits = 1 if train_events else 0
        preds = predict.predict_for_trading_day(
            T,
            codes,
            names,
            train_events,
            blob,
            top_n=40,
            min_keyword_hits=min_hits,
            ml_bundle=ml_bundle,
            returns_ml=returns_ml,
        )
        scoring_ctx = predict.build_scoring_context(blob, train_events)

        kospi_r = market_index.index_daily_return_pct(ks11, T)
        kospi_hint = None
        if kospi_r is not None:
            kospi_hint = f"당일 KOSPI 지수 전일대비 약 {kospi_r*100:.2f}%였습니다."

        now_kst_td = datetime.now(trading_calendar.KST)
        is_today_t = T == now_kst_td.date()
        krx_pct_by_code: dict[str, float] | None = None
        if forward_prediction_only:
            actual_big_movers: list[dict] = []
            # 당일 실행에서는 장중/장마감 직후 참고 표시를 위해 pykrx 스냅샷을 시도합니다.
            if is_today_t:
                krx_pct_by_code = stocks.try_krx_change_pct_by_code(T, returns_df=returns)
        else:
            krx_pct_by_code = stocks.try_krx_change_pct_by_code(T, returns_df=returns)
            if krx_pct_by_code is not None:
                actual_big_movers = stocks.big_movers_from_krx_pct_map(
                    krx_pct_by_code, config.BIG_MOVE_THRESHOLD, names
                )
            else:
                krx_movers_unavailable_any = True
                movers = stocks.big_movers_on_date(returns, T, config.BIG_MOVE_THRESHOLD)
                actual_big_movers = [
                    {
                        "code": str(r["Code"]).zfill(6),
                        "name": str(r["Name"]),
                        "ret_pct": float(r["return_pct"]) * 100.0,
                    }
                    for _, r in movers.iterrows()
                ]
                if not krx_fallback_warned:
                    msg = (
                        "참고: pykrx·KRX 전종목 등락률 조회에 실패했습니다. "
                        "실제 20%↑는 OHLCV에 있는 종목 범위에서만 집계합니다."
                    )
                    if config.SAMPLE_TICKERS_N:
                        msg += (
                            f" SAMPLE_TICKERS={config.SAMPLE_TICKERS_N}(으)로 표본이 좁습니다. "
                            "비우면 전상장 일봉 기준에 가깝게 맞출 수 있습니다."
                        )
                    msg += " (pip install pykrx, 네트워크·영업일 확인)"
                    print(msg)
                    krx_fallback_warned = True

        def _actual_ret_for_code(code: str) -> float | None:
            a = stocks.actual_return_on_date(returns, code, T)
            if a is not None:
                return a
            if krx_pct_by_code is not None:
                p = krx_pct_by_code.get(code)
                if p is not None:
                    return p / 100.0
            return None

        hl_terms = features.top_keywords(blob, k=40)

        rows_compare: list[dict] = []
        false_negatives: list[dict] = []
        pred_pct_min = config.BIG_MOVE_THRESHOLD * 100.0

        if not forward_prediction_only:
            preds_by_code: dict[str, predict.PredictionRow] = {pr.code: pr for pr in preds}
            for m in actual_big_movers:
                code = m["code"]
                act = float(m["ret_pct"]) / 100.0
                pr = preds_by_code.get(code)
                if pr is not None:
                    reasons_html = "<br/>".join(pr.reasons)
                    if pr.matched_keywords:
                        reasons_html += "<br/><em>일치 키워드</em> " + ", ".join(
                            f"<mark>{w}</mark>" for w in pr.matched_keywords[:10]
                        )
                    keywords = pr.matched_keywords
                    pred_ret = pr.predicted_return_pct
                else:
                    reasons_html = "일일 예측 후보에 포함되지 않음(예측 수익률 재계산 안 함)"
                    keywords = []
                    pred_ret = None

                pred_high = pred_ret is not None and pred_ret >= pred_pct_min
                row_d = {
                    "code": code,
                    "market_segment": market_by_code.get(str(code).zfill(6), "other"),
                    "name": m["name"],
                    "reasons_html": reasons_html,
                    "keywords": keywords,
                    "pred_ret": pred_ret,
                    "actual_ret": act,
                    "actual_big": True,
                    "pred_high": pred_high,
                    "gap_analysis_html": _gap_analysis_html_for_row(
                        pred_ret, act, pr, keywords, blob, kospi_hint, late_blob
                    ),
                    **_pred_reason_fields(pr, reasons_html),
                }
                rows_compare.append(row_d)

                if (
                    config.USE_DECISION_NEWS_INTRADAY_CUTOFF
                    and late_blob
                    and keywords
                    and pr is not None
                ):
                    late_hit = news.late_blob_covers_keywords(late_blob, list(keywords))
                    late_gte_n += 1
                    if late_hit:
                        late_gte_kw += 1

        seen_row_codes = {r["code"] for r in rows_compare}
        for pr in preds:
            # 예측 20% 이상 후보만 표에 반영(장중/장후 동일).
            if pr.predicted_return_pct < pred_pct_min:
                continue
            if pr.code in seen_row_codes:
                continue
            seen_row_codes.add(pr.code)
            act = None if forward_prediction_only else _actual_ret_for_code(pr.code)
            reasons_html = "<br/>".join(pr.reasons)
            if pr.matched_keywords:
                reasons_html += "<br/><em>일치 키워드</em> " + ", ".join(
                    f"<mark>{w}</mark>" for w in pr.matched_keywords[:10]
                )
            ph = pr.predicted_return_pct >= pred_pct_min
            rows_compare.append(
                {
                    "code": pr.code,
                    "market_segment": market_by_code.get(str(pr.code).zfill(6), "other"),
                    "name": pr.name,
                    "reasons_html": reasons_html,
                    "keywords": pr.matched_keywords,
                    "pred_ret": pr.predicted_return_pct,
                    "actual_ret": act,
                    "actual_big": act is not None and act >= config.BIG_MOVE_THRESHOLD,
                    "pred_high": ph,
                    "gap_analysis_html": _gap_analysis_html_for_row(
                        pr.predicted_return_pct,
                        act,
                        pr,
                        pr.matched_keywords,
                        blob,
                        kospi_hint,
                        late_blob,
                    ),
                    **_pred_reason_fields(pr, reasons_html),
                }
            )

        rows_compare.sort(key=lambda r: (not r["actual_big"], not r["pred_high"], r["code"]))

        today_td = now_kst_td.date()
        force_intraday_snapshot = os.getenv("FORCE_INTRADAY_SNAPSHOT", "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        # 일봉이 아직 확정되지 않은 당일 거래일 T: 실제 상승률은 — (장중 등락%) 형태로 둡니다.
        pre_close_today = (
            T == today_td
            and trading_calendar.is_trading_day(T)
            and not trading_calendar.is_krx_daily_bar_effective_closed(T, now_kst=now_kst_td)
        )
        if force_intraday_snapshot and T == today_td and trading_calendar.is_trading_day(T):
            pre_close_today = True
        if pre_close_today and rows_compare:
            # 장중: pykrx(재시도)·종목별·네이버 실시간 순으로 등락률을 채운 뒤 — (xx%) 표기.
            row_codes = sorted(
                {str(r.get("code", "")).zfill(6) for r in rows_compare if r.get("code")}
            )
            merged = stocks.best_effort_intraday_pct_by_code(T, row_codes, returns_df=returns)
            _apply_preclose_actual_snapshot_rows(
                rows_compare, snapshot_pct_by_code=merged if merged else None
            )
            for r in rows_compare:
                r["actual_cell_pre_close_snapshot"] = True
        elif T == today_td and trading_calendar.is_trading_day(T) and rows_compare:
            # 장 마감 후에는 pykrx 값을 재조회해 실제 수익률 칸 자체를 최신화합니다.
            snap_post = stocks.try_krx_change_pct_by_code(T, returns_df=returns)
            _apply_postclose_actual_snapshot_rows(
                rows_compare,
                snapshot_pct_by_code=snap_post,
                threshold=config.BIG_MOVE_THRESHOLD,
            )

        if include_target_calendar_news:
            _enrich_rows_news_evidence(rows_compare, early_rows, actual_ctx_rows)
            _enrich_rows_disclosure_hits(rows_compare, T)
        for r in rows_compare:
            code_r = str(r.get("code", ""))
            pr_row = _prediction_row_strict_or_loose(
                code_r, names, train_events, blob, scoring_ctx, min_hits
            )
            r["gap_analysis_html"] = _gap_analysis_html_for_row(
                r.get("pred_ret"),
                r.get("actual_ret"),
                pr_row,
                list(r.get("keywords") or []),
                blob,
                kospi_hint,
                late_blob,
                disclosure_hits=list(r.get("disclosure_hits") or []),
                actual_intraday_pct=r.get("actual_ret_intraday_pct"),
            )
            r["rise_reason_html"] = predict.explain_rise_reason_html(
                actual_ret=r.get("actual_ret"),
                actual_intraday_pct=r.get("actual_ret_intraday_pct"),
                t_trading_day=T,
                actual_news_hits=r.get("actual_news_hits"),
                disclosure_hits=r.get("disclosure_hits"),
                news_evidence_collected=include_target_calendar_news,
            )

        for pr in preds:
            act = None if forward_prediction_only else _actual_ret_for_code(pr.code)
            if (
                not forward_prediction_only
                and act is not None
                and act < config.BIG_MOVE_THRESHOLD
                and config.USE_DECISION_NEWS_INTRADAY_CUTOFF
                and late_blob
                and pr.matched_keywords
            ):
                late_hit = news.late_blob_covers_keywords(late_blob, list(pr.matched_keywords))
                late_below_n += 1
                if late_hit:
                    late_below_kw += 1

            if (
                not forward_prediction_only
                and act is not None
                and act < 0
            ):
                false_negatives.append(
                    {
                        "code": pr.code,
                        "name": pr.name,
                        "pred_ret": pr.predicted_return_pct,
                        "actual_ret": act,
                        "keywords": list(pr.matched_keywords),
                        "analysis": predict.explain_miss(pr, act, blob, kospi_hint),
                    }
                )

        day_reports.append(
            report.DayReport(
                trading_day=T,
                predictions=preds,
                rows_compare=rows_compare,
                false_negatives=false_negatives,
                news_titles_sample=news_titles,
                news_highlight_terms=hl_terms,
                actual_big_movers=actual_big_movers,
            )
        )

    # 장중 시작 실행이 장마감 이후까지 이어진 경우: 오늘(T) 일봉을 한번 더 읽어 실제값을 보정.
    now_end_kst = datetime.now(trading_calendar.KST)
    today_end = now_end_kst.date()
    if (
        ohlcv_end < today_end <= end_date
        and any(dr.trading_day == today_end for dr in day_reports)
        and trading_calendar.is_trading_day(today_end)
        and not trading_calendar.is_before_krx_regular_close_kst(today_end, now_kst=now_end_kst)
    ):
        print(
            f"장 마감 반영 재조회: 오늘({today_end}) 일봉 실제값을 최종 반영합니다.",
            flush=True,
        )
        ohlcv_post = stocks.build_ohlcv_long(
            train_start - timedelta(days=10),
            today_end,
            force_full_listing=not forward_prediction_only,
            skip_gap_download=skip_ohlcv_gap_download,
            refresh_tail_days=1,
        )
        returns_post = stocks.daily_returns_table(ohlcv_post)
        for dr in day_reports:
            if dr.trading_day != today_end:
                continue
            _backfill_day_actuals_from_returns(
                dr,
                returns=returns_post,
                threshold=config.BIG_MOVE_THRESHOLD,
            )

    _enrich_cumulative_accuracy_avg(day_reports)
    prediction_accuracy_cache.merge_from_day_reports(day_reports)
    prediction_accuracy_cache.merge_high_pred_history_from_day_reports(
        day_reports, threshold_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    prediction_accuracy_cache.apply_cached_cumulative_fallback(day_reports)
    prediction_accuracy_cache.enrich_rows_pred_high_history(day_reports)
    _enrich_cumulative_hit_rate(
        day_reports, threshold_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    _enrich_cumulative_actual_over_pred_from_history(day_reports)

    return PipelineOut(
        day_reports=day_reports,
        news_source=news_source,
        correlation_rows=correlation_rows,
        train_start=train_start,
        test_start=test_start,
        end_date=end_date,
        late_below_n=late_below_n,
        late_below_kw=late_below_kw,
        late_gte_n=late_gte_n,
        late_gte_kw=late_gte_kw,
        movers_data_note=_movers_data_note_for_report(
            krx_movers_unavailable_any and not forward_prediction_only
        ),
    )


def _omit_target_calendar_before_close(
    test_days: Sequence[date],
    *,
    now_kst: datetime,
) -> frozenset[date]:
    """
    관측 거래일 ``T`` 가 KST ``오늘`` 이고 정규장(15:30) 전이면, 해당 ``T`` 캘린더 뉴스는 받지 않습니다.

    예측 입력은 직전 거래일 ``NEWS_CUTOFF_*`` 까지이므로 당일 ``T`` 본장 전 종목·시장 뉴스는 불필요합니다.
    """
    today = now_kst.date()
    out: list[date] = []
    for t in test_days:
        if (
            t == today
            and trading_calendar.is_trading_day(t)
            and trading_calendar.is_before_krx_regular_close_kst(t, now_kst=now_kst)
        ):
            out.append(t)
    return frozenset(out)


def _should_skip_ohlcv_right_gap(n_day: date, *, now_kst: datetime) -> bool:
    """
    N일 기준일 실행에서, 아직 존재하지 않는 일봉을 채우려는 우측 OHLCV 보강을 건너뛸지.

    - N이 KST 오늘보다 미래이거나
    - N이 KST 오늘인 거래일이면서 정규장 마감(15:30 KST) 전이면 True.
    """
    tk = now_kst.date()
    if n_day > tk:
        return True
    if n_day == tk and trading_calendar.is_trading_day(n_day):
        return trading_calendar.is_before_krx_regular_close_kst(n_day, now_kst=now_kst)
    return False


def _render_monthly_batch(po: PipelineOut, *, test_range_label: str) -> list[Path]:
    """
    ``PipelineOut.day_reports`` 를 달력 **월** 단위로 나눠 ``report_YYYY.MM.html`` 을 쓰고,
    ``report_index_monthly.html`` 목차를 갱신합니다.

    Returns:
        생성·갱신된 HTML 경로 목록(폴더 자동 열기용).
    """
    meta_base = {
        "train_range": f"{po.train_start} ~ {po.test_start - timedelta(days=1)}",
        "test_range": test_range_label,
        "threshold": f"{config.BIG_MOVE_THRESHOLD*100:.0f}%",
        "news_source": po.news_source,
        "correlation_rows": po.correlation_rows,
        "use_decision_cutoff": config.USE_DECISION_NEWS_INTRADAY_CUTOFF,
        "cutoff_kst": f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}",
        "run_subtitle": "",
    }
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:

        def _pct(num: int, den: int) -> str:
            if den <= 0:
                return "—"
            return f"{100.0 * num / den:.1f}%"

        meta_base["late_news_probe"] = {
            "below_n": po.late_below_n,
            "below_kw": po.late_below_kw,
            "below_pct": _pct(po.late_below_kw, po.late_below_n),
            "gte_n": po.late_gte_n,
            "gte_kw": po.late_gte_kw,
            "gte_pct": _pct(po.late_gte_kw, po.late_gte_n),
        }

    if po.movers_data_note:
        meta_base["movers_data_note"] = po.movers_data_note

    month_batches: dict[tuple[int, int], list[report.DayReport]] = {}
    for dr in po.day_reports:
        t = dr.trading_day
        month_batches.setdefault((t.year, t.month), []).append(dr)

    sorted_months = sorted(month_batches.keys())
    month_links: list[tuple[str, str]] = []
    written_paths: list[Path] = []

    for ym in sorted_months:
        batch = sorted(month_batches[ym], key=lambda x: x.trading_day)
        y, m = ym
        fname = f"report_{y}.{m:02d}.html"
        out_month = config.OUTPUT_DIR / fname
        first_d, last_d = batch[0].trading_day, batch[-1].trading_day
        month_note = (
            f"{y}년 {m}월 · 포함 거래일 {first_d.isoformat()} ~ {last_d.isoformat()} ({len(batch)}일, 주간 탭·탭 내 일자 순)"
        )
        report.render_compact_tabbed_report(
            title=f"실제 20%↑ 종목 · 예측 상승률 · {fname.replace('.html', '')}",
            days=batch,
            meta={**meta_base, "n_days": len(batch)},
            out_path=out_month,
            week_note=month_note,
            week_tabs_stack_days=True,
        )
        month_links.append((fname, f"{fname} · {first_d.isoformat()} ~ {last_d.isoformat()}"))
        written_paths.append(out_month)
        print(f"완료: {out_month}")

    index_html = config.OUTPUT_DIR / "report_index_monthly.html"
    report.render_movers_index(
        month_links,
        index_html,
        title="월간 리포트 목차 (달력 월 단위)",
    )
    written_paths.append(index_html)
    print(f"완료: {index_html}")
    return written_paths


def main() -> None:
    """
    CLI 진입점: ``_parse_cli`` 결과에 따라 주간/구간/단일일 모드로 파이프라인 실행 후 HTML 저장.

    환경 변수 ``NO_AUTO_OPEN_OUTPUT`` 이 설정되지 않았으면 Windows/macOS에서 이번에 생성한 리포트 HTML 중 최신 파일 하나만 연다.
    """
    mode, arg_date, range_end, snap_mode = _parse_cli()
    if mode == "usage":
        _print_usage()
        sys.exit(0 if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help") else 2)

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

        omit_t_cal = _omit_target_calendar_before_close(test_days, now_kst=now_kst)
        if omit_t_cal:
            for t in sorted(omit_t_cal):
                print(
                    f"관측일 T={t} 장 마감 전: 해당일 캘린더 뉴스는 수집하지 않습니다.",
                    flush=True,
                )

        po = _run_pipeline(
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
        out_files = _render_monthly_batch(po, test_range_label=test_range_label)
        _open_report_outputs(out_files)
        return

    if mode == "range":
        assert arg_date is not None and range_end is not None
        d_from, d_to = arg_date, range_end
        end_date = min(date(2026, 12, 31), d_to)
        sessions = trading_calendar.trading_sessions_in_range(train_start, end_date)
        test_days = [t for t in sessions if d_from <= t <= d_to]
        if not test_days:
            print(f"구간 {d_from} ~ {d_to} 에 포함되는 거래일이 없습니다. (데이터 상한 {end_date})")
            return

        omit_t_cal = _omit_target_calendar_before_close(test_days, now_kst=now_kst)
        if omit_t_cal:
            for t in sorted(omit_t_cal):
                print(
                    f"관측일 T={t} 장 마감 전: 해당일 캘린더 뉴스는 수집하지 않습니다.",
                    flush=True,
                )

        po = _run_pipeline(
            test_days,
            end_date,
            train_snapshot_mode=snap_mode,
            train_snapshot_cal_scope=(d_from, d_to),
            include_target_calendar_news=True,
            skip_news_fetch_after=today,
            omit_target_calendar_days=omit_t_cal,
        )
        test_range_label = (
            f"{d_from.isoformat()} ~ {d_to.isoformat()} "
            f"(거래일만 · OHLCV/뉴스 조회 기준일 {today}까지, 리포트 범위는 {end_date}까지)"
        )
        out_files = _render_monthly_batch(po, test_range_label=test_range_label)
        _open_report_outputs(out_files)
        return

    # daily / dated: N → T = next trading day after N
    if mode == "daily":
        n_day = today
        if not trading_calendar.is_trading_day(n_day):
            print(f"{n_day} 은(는) 거래일이 아닙니다. 거래일에 15:00 스케줄로 실행하세요.")
            return
    else:
        assert arg_date is not None
        n_day = arg_date  # dated

    try:
        t_day = trading_calendar.next_trading_day_after(n_day)
    except ValueError as e:
        print(e)
        return

    end_date = max(today, t_day)
    end_date = min(end_date, date(2026, 12, 31))

    test_days = [t_day]
    if t_day not in trading_calendar.trading_sessions_in_range(train_start, end_date):
        print(f"관측일 {t_day} 이(가) 캘린더 범위에 없습니다. end_date={end_date}")
        return

    forward_prediction_only = n_day == today or (
        not trading_calendar.is_trading_day(n_day)
    )
    before_open_n = (
        n_day == today
        and trading_calendar.is_trading_day(n_day)
        and trading_calendar.is_before_krx_regular_open_kst(n_day, now_kst=now_kst)
    )

    skip_ohlcv_gap = _should_skip_ohlcv_right_gap(n_day, now_kst=now_kst)
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

    po = _run_pipeline(
        test_days,
        end_date,
        include_target_calendar_news=True,
        forward_prediction_only=forward_prediction_only,
        train_snapshot_mode=snap_mode,
        train_snapshot_cal_scope=None,
        skip_ohlcv_gap_download=skip_ohlcv_gap,
        omit_target_calendar_days=omit_t_calendar,
        skip_news_fetch_after=skip_news_fetch_after,
    )

    meta_compact = {
        "train_range": f"{po.train_start} ~ {po.test_start - timedelta(days=1)}",
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
            "저장된 예측≥임계 이력이 있으면 누적 앞 숫자는 각 행 (실제%÷예측%) 합÷이력 개수(예측 0%·미확정·값 없음은 0). "
            "이력이 없으면 min(|실제%|÷|예측%|,100%) 평균을 씁니다. "
            f"{prediction_accuracy_cache.track_path_display()}. "
            "이번 T 확정 후 같은 종목이 다시 표에 오면 캐시가 갱신됩니다."
        )

    rollup_path = config.REPORT_DATED_ROLLUP_HTML
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
    print(f"완료: {rollup_path} (N={n_day.isoformat()} 블록 반영)")
    _open_report_outputs([rollup_path])


if __name__ == "__main__":
    _t0 = time.perf_counter()
    try:
        main()
    finally:
        _sec = time.perf_counter() - _t0
        _total_s = int(round(_sec))
        _min, _srem = divmod(_total_s, 60)
        print(f"총 소요시간: {_min:02d}분 {_srem:02d}초")
