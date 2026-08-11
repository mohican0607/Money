"""CLI 파싱·사용법·리포트 열기."""
from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

def _parse_yyyymmdd(s: str) -> date | None:
    """``YYYYMMDD`` 8자리 숫자 문자열을 ``date`` 로 파싱. 형식 오류 시 None."""
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        return None
    y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
    return date(y, m, d)


def _parse_cli() -> tuple[str, date | None, date | None, str, bool, bool]:
    """
    ``sys.argv`` 를 파싱해 실행 모드와 날짜 인자를 돌려줍니다.

    Returns:
        ``(mode, arg_date, range_end, train_snapshot_mode, use_freeze, use_n_day)``.
        ``train_snapshot_mode`` 는 ``none`` | ``use`` | ``rebuild`` | ``append_learning``.
        ``range`` 일 때만 ``arg_date`` 와 ``range_end`` 가 둘 다 채워짐.
        ``use_n_day`` 이면 단일 ``YYYYMMDD`` 는 관측일 T 가 아니라 기준일 N.
    """
    raw = [a for a in sys.argv[1:] if a]
    use_freeze = "--use-freeze" in raw
    use_n_day = "--n-day" in raw
    has_rebuild = "--rebuild-train-snapshot" in raw
    has_append = "--append-rebuild-learning" in raw
    has_no_snap = "--no-train-snapshot" in raw
    if has_rebuild:
        snap_mode = "rebuild"
    elif has_append:
        snap_mode = "append_learning"
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
            "--append-rebuild-learning",
            "--no-train-snapshot",
            "--no-report-expand",
            "--use-freeze",
            "--n-day",
        )
    ]
    if not argv:
        return "daily", None, None, snap_mode, use_freeze, use_n_day
    if argv[0] in ("--weekly", "--weekly-report", "-w"):
        return "weekly", None, None, snap_mode, use_freeze, use_n_day
    if argv[0] in ("--serve-live-quotes", "--live-quotes"):
        return "serve_live_quotes", None, None, snap_mode, use_freeze, use_n_day
    if argv[0] in ("-h", "--help"):
        return "usage", None, None, snap_mode, use_freeze, use_n_day
    if len(argv) >= 2:
        d0 = _parse_yyyymmdd(argv[0])
        d1 = _parse_yyyymmdd(argv[1])
        if d0 is not None and d1 is not None:
            if d0 > d1:
                d0, d1 = d1, d0
            return "range", d0, d1, snap_mode, use_freeze, use_n_day
    d = _parse_yyyymmdd(argv[0])
    if d is not None:
        return "dated", d, None, snap_mode, use_freeze, use_n_day
    print(f"인식할 수 없는 인자: {argv[0]}", file=sys.stderr)
    return "usage", None, None, snap_mode, use_freeze, use_n_day


def _print_usage() -> None:
    """표준 출력에 CLI 도움말을 인쇄합니다."""
    print(
        """사용법:
  python main.py
      오늘(N) 기준 → N+1 거래일(T) 후보, output/report_dated_by_MMDD.html (해당 N 블록)
  python main.py YYYYMMDD
      관측일 T(거래일). T는 현재 기준 N+1 거래일 이하. T=현재 N+1이면 N일 14:30부터.
      output/report_dated_by_MMDD.html 에 해당 T 블록만 추가·갱신
  python main.py --n-day YYYYMMDD
      기준일 N(거래일). T=N 다음 거래일. N=현재 N 이면 N일 14:30(KST)부터.
      (--append-rebuild-learning 과 함께 14:30 freeze 유지·리포트 갱신에 사용)
  python main.py YYYYMMDD YYYYMMDD
      관측일 From~To 구간(양 끝 포함, **KRX 거래일만**). To는 현재 기준 N+1 거래일 이하.
      구간에 현재 N+1이 포함되면 N일 14:30(KST)부터 실행.
      report_YYYY.MM.html 및 report_index_monthly.html (같은 달 HTML이 있으면 해당 일자만 갱신·추가)
  python main.py --weekly
      월간 배치 (config REPORT_TEST_DAY_START ~ END, --weekly 이름은 호환용)
  python main.py --serve-live-quotes
      리포트 tooltip 장중 등락률용 로컬 프록시(127.0.0.1:8765). 리포트 열기 전 자동 기동되기도 함.

  학습 스냅샷 (급등-뉴스 BreakoutEvent 풀, breakout_train_snapshot.json):
  (플래그 없음, 기본)
      스냅샷을 읽어 재사용·병합. 예측 시에는 관측일 T마다 T 직전 이벤트만 사용(워크포워드).
      TEST_START( config )는 --weekly 기본 관측 시작일이며 학습 상한이 아님.
  --no-train-snapshot
      스냅샷을 읽지도 쓰지도 않고, 매번 전체 재계산만 수행(예전 동작).
  --use-train-snapshot
      기본과 동일(명시용·호환).
  --rebuild-train-snapshot
      항상 전체 재계산 후 스냅샷을 새로 저장(이 플래그가 최우선).
      From~To 구간과 함께 쓰면 실행 말미에 breakout_train_snapshot.json 에
      rebuild_learning(일자별 괴리·적중 누적, 기존 일자와 병합·
      미예측 급등 missed_big_movers_today / 고예측 실패 pred_high_misses_today 원인 태그·한글 힌트)·
      market_theme_flow(일자별 early 뉴스·테마 시드·급등 다발 요약)·
      prediction_gap_rollup(예측–실제 달성률·버킷 통계 스냅샷)을 병합 저장하고,
      ML 랭커 joblib 은 재학습해 덮어씁니다(스냅샷에 쌓인 miss 진단으로 어려운 급등·오판 샘플 가중).
      다음 구간 재실행 시 갱신된 miss_diagnosis 가 재학습에 반영됩니다.
      From~To 구간 안의 관측일은 예측 고정 캐시를 무시하고 재계산·저장합니다.
  --append-rebuild-learning
      급등-뉴스 train_events 는 스냅샷 재사용(미반영 캘린더만 병합). ML joblib 도 재학습하지 않고
      기존 모델을 로드합니다. From~To·단일일 모두 장 마감 확정 T 에 대해
      rebuild_learning(미예측 급등·고예측 실패 원인 태그)·market_theme_flow·
      prediction_gap_rollup 을 breakout_train_snapshot.json 에 병합합니다.
      단일일 ``YYYYMMDD`` 는 freeze 가 있으면 14:30 예측 후보를 유지한 채 actual·테마·학습 진단을 갱신.
  (플래그 없음) From~To
      각 T 에 prediction_freeze_by_t.json 이 있으면 예측 후보 재사용(실제·테마·누적만 갱신).
      없는 T 만 신규 예측 후 freeze 저장.
  --no-report-expand
      월간 HTML 병합 없이 이번 From~To 거래일만으로 해당 월 파일을 덮어씁니다.
      (기존 일자 HTML 유지·append 안 함)
  --use-freeze
      --rebuild-train-snapshot 구간에서도 prediction_freeze_by_t.json 을 재사용합니다.
      (일반 From~To 는 기본적으로 freeze 재사용)

  예: python main.py 20260701 20260706
  예: python main.py --append-rebuild-learning --no-report-expand 20260701 20260706
"""
    )
def _open_report_outputs(html_paths: Sequence[Path]) -> None:
    """
    ``NO_AUTO_OPEN_OUTPUT`` 이 비어 있으면, 이번 실행에서 전달된 경로 중 **하나만** 연다.

    ``report_*.html`` 이 있으면 그중 ``index`` 가 이름에 들어가지 않은 파일만 후보로 두고,
    수정 시각(``st_mtime``)이 가장 최근인 파일을 고른다. 후보가 없으면 전달된 파일 전체에서 최신만 연다.
    ``output`` 폴더는 열지 않는다.

    Windows: ``os.startfile`` — macOS: ``open``.
    """
    try:
        from src.report.live_quotes import ensure_live_quotes_server_running

        ensure_live_quotes_server_running()
    except Exception:
        pass
    if os.getenv("NO_AUTO_OPEN_OUTPUT", "").strip().lower() in ("1", "true", "yes"):
        return
    existing = [Path(p).resolve() for p in html_paths if Path(p).is_file()]
    if not existing:
        return

    def _is_primary_report(p: Path) -> bool:
        """자동 열기 대상: report_*.html 이면서 index 가 아닌 파일."""
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
