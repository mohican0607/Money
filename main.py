"""
KOSPI·KOSDAQ 뉴스–급등 상관 및 익일 후보 리포트.

실행 (프로젝트 루트, 출력: output/):

  python main.py
    → 오늘이 거래일 N일 때, N+1 거래일(T) 급등 후보. output/report_dated_by_MMDD.html 에 해당 N 블록 갱신(표는 예측 후보만)
    → 거래일 14:30 자동 실행·리포트 열기: scripts/run_daily_1500.ps1 (등록 예: scripts/register_task_scheduler_example.ps1)

  python main.py 20260401
    → 관측일 T=2026-04-01 지정, 기준일 N은 T 직전 거래일로 자동 계산.
      output/report_dated_by_0401.html 에 해당 T 블록 추가·재실행 시 해당 블록만 갱신
    → T가 오늘/미래면 예측 후보 중심, T가 과거면 pykrx로 시장 20%↑와 예측을 함께 표시(OHLCV 샘플 밖 급등 포함).

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
import json
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
    snapshot_miss_diagnosis,
    snapshot_rebuild_learning,
    stocks,
    theme_carryover,
    train_snapshot,
    trading_calendar,
)


def _load_prediction_freeze_payload() -> dict[str, list[dict]]:
    path = config.PREDICTION_FREEZE_PATH
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[dict]] = {}
    for k, v in raw.items():
        if isinstance(k, str) and isinstance(v, list):
            out[k] = [x for x in v if isinstance(x, dict)]
    return out


def _save_prediction_freeze_payload(payload: dict[str, list[dict]]) -> None:
    path = config.PREDICTION_FREEZE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _frozen_predicted_return_pct(item: dict) -> float | None:
    raw = item.get("predicted_return_pct")
    if raw is None:
        raw = item.get("pred_ret")
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _freeze_entry_usable(items: list[dict]) -> bool:
    """관측일 T별 고정 캐시에 재사용할 예측 수익률이 하나라도 있으면 True."""
    if not items:
        return False
    for x in items:
        p = _frozen_predicted_return_pct(x)
        if p is not None and math.isfinite(p):
            return True
    return False


def _prediction_rows_from_frozen_items(items: list[dict]) -> list[predict.PredictionRow]:
    rows: list[predict.PredictionRow] = []
    for x in items:
        try:
            pct = _frozen_predicted_return_pct(x)
            if pct is None:
                pct = 0.0
            rows.append(
                predict.PredictionRow(
                    code=str(x.get("code", "")).zfill(6),
                    name=str(x.get("name", "")),
                    score=float(x.get("score", 0.0) or 0.0),
                    predicted_return_pct=pct,
                    matched_keywords=[str(k) for k in (x.get("matched_keywords") or [])],
                    reasons=[str(r) for r in (x.get("reasons") or [])],
                    ml_prob=(
                        float(x["ml_prob"])
                        if x.get("ml_prob") is not None
                        else None
                    ),
                    keyword_hits=int(x.get("keyword_hits", 0) or 0),
                    mention_score=float(x.get("mention_score", 0.0) or 0.0),
                )
            )
        except (TypeError, ValueError):
            continue
    return rows


def _prediction_rows_to_frozen_items(rows: list[predict.PredictionRow]) -> list[dict]:
    return [
        {
            "code": str(r.code).zfill(6),
            "name": r.name,
            "score": float(r.score),
            "predicted_return_pct": float(r.predicted_return_pct),
            "matched_keywords": list(r.matched_keywords),
            "reasons": list(r.reasons),
            "ml_prob": (None if r.ml_prob is None else float(r.ml_prob)),
            "keyword_hits": int(getattr(r, "keyword_hits", 0) or 0),
            "mention_score": float(getattr(r, "mention_score", 0.0) or 0.0),
        }
        for r in rows
    ]


def _append_compare_row_from_prediction(
    rows_compare: list[dict],
    pr: predict.PredictionRow,
    *,
    market_by_code: dict[str, str],
    pred_pct_min: float,
    pred_pct_mid_min: float,
    actual_ret: float | None,
    reasons_html: str,
    blob: str,
    kospi_hint: str | None,
    late_blob: str,
    pr_for_gap: predict.PredictionRow | None,
) -> None:
    ph = pr.predicted_return_pct >= pred_pct_min
    pm = pred_pct_mid_min <= pr.predicted_return_pct < pred_pct_min
    rows_compare.append(
        {
            "code": pr.code,
            "market_segment": market_by_code.get(str(pr.code).zfill(6), "other"),
            "name": pr.name,
            "reasons_html": reasons_html,
            "keywords": pr.matched_keywords,
            "keyword_hits": pr.keyword_hits,
            "mention_score": pr.mention_score,
            "pred_ret": pr.predicted_return_pct,
            "actual_ret": actual_ret,
            "actual_big": actual_ret is not None and actual_ret >= config.BIG_MOVE_THRESHOLD,
            "pred_high": ph,
            "pred_mid": pm,
            "rise_band": _rise_band_for_row(pr.predicted_return_pct, actual_ret),
            "gap_analysis_html": _gap_analysis_html_for_row(
                pr.predicted_return_pct,
                actual_ret,
                pr_for_gap or pr,
                pr.matched_keywords,
                blob,
                kospi_hint,
                late_blob,
            ),
            "late_news_hit": None,
            **_pred_reason_fields(pr_for_gap or pr, reasons_html),
        }
    )


def _sync_forward_day_rows_from_predictions(
    rows_compare: list[dict],
    preds: list[predict.PredictionRow],
    *,
    market_by_code: dict[str, str],
    pred_pct_min: float,
    pred_pct_mid_min: float,
    blob: str,
    kospi_hint: str | None,
    late_blob: str,
) -> None:
    """예측 전용일: 상위 후보 전부 표에 넣고, 빠진 ``pred_ret`` 를 보강."""
    by_code = {str(r.get("code", "")).zfill(6): r for r in rows_compare if r.get("code")}
    for pr in preds:
        if not math.isfinite(float(pr.predicted_return_pct)):
            continue
        code = str(pr.code).zfill(6)
        reasons_html = "<br/>".join(pr.reasons)
        if pr.matched_keywords:
            reasons_html += "<br/><em>일치 키워드</em> " + ", ".join(
                f"<mark>{w}</mark>" for w in pr.matched_keywords[:10]
            )
        existing = by_code.get(code)
        if existing is not None:
            existing["pred_ret"] = pr.predicted_return_pct
            existing["pred_high"] = pr.predicted_return_pct >= pred_pct_min
            existing["pred_mid"] = (
                pred_pct_mid_min <= pr.predicted_return_pct < pred_pct_min
            )
            existing["rise_band"] = _rise_band_for_row(
                pr.predicted_return_pct, existing.get("actual_ret")
            )
            if not existing.get("reasons_html"):
                existing["reasons_html"] = reasons_html
            if not existing.get("keywords"):
                existing["keywords"] = list(pr.matched_keywords)
            continue
        _append_compare_row_from_prediction(
            rows_compare,
            pr,
            market_by_code=market_by_code,
            pred_pct_min=pred_pct_min,
            pred_pct_mid_min=pred_pct_mid_min,
            actual_ret=None,
            reasons_html=reasons_html,
            blob=blob,
            kospi_hint=kospi_hint,
            late_blob=late_blob,
            pr_for_gap=pr,
        )
        by_code[code] = rows_compare[-1]


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
      오늘(N) 기준 → N+1 거래일(T) 후보, output/report_dated_by_MMDD.html (해당 N 블록)
  python main.py YYYYMMDD
      지정 관측일 T(YYYYMMDD), 기준일 N은 T 직전 거래일로 자동 계산.
      output/report_dated_by_MMDD.html 에 해당 T 블록만 추가·갱신
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
      From~To 구간과 함께 쓰면 실행 말미에 breakout_train_snapshot.json 에
      rebuild_learning(일자별 괴리·적중 누적, 기존 일자와 병합·
      미예측 급등 missed_big_movers_today / 고예측 실패 pred_high_misses_today 원인 태그·한글 힌트)·
      market_theme_flow(일자별 early 뉴스·테마 시드·급등 다발 요약)·
      prediction_gap_rollup(예측–실제 달성률·버킷 통계 스냅샷)을 병합 저장하고,
      ML 랭커 joblib 은 재학습해 덮어씁니다(스냅샷에 쌓인 miss 진단으로 어려운 급등·오판 샘플 가중).
      다음 구간 재실행 시 갱신된 miss_diagnosis 가 재학습에 반영됩니다.
      이 플래그가 있을 때만 data/cache/train/prediction_freeze_by_t.json 의
      해당 관측일 예측수익률·후보 목록이 다시 계산·저장됩니다(플래그 없이 재실행 시 고정).

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


def _enrich_cumulative_actual_over_pred_from_history_for_field(
    day_reports: list[report.DayReport],
    *,
    history_field: str,
    out_field: str,
) -> None:
    """지정 이력 필드 기준 누적 달성률 평균(0~1)을 ``out_field`` 에 씁니다."""
    for dr in day_reports:
        for r in dr.rows_compare:
            hist = r.get(history_field) or []
            items = sorted(
                (h for h in hist if isinstance(h, dict)),
                key=lambda h: str(h.get("t", "")),
            )
            n = len(items)
            if n == 0:
                r[out_field] = None
                continue
            acc = 0.0
            for h in items:
                pr = h.get("pred_pct")
                ap = h.get("actual_pct")
                if ap is None or pr is None:
                    continue
                prf = abs(float(pr))
                apv = float(ap)
                if apv < 0:
                    continue
                apf = abs(apv)
                den = max(prf, apf)
                if den >= 1e-9:
                    acc += min(prf, apf) / den
            r[out_field] = acc / n


def _enrich_rows_actual_ret_prev_day(
    rows: list[dict], returns, t_day: date
) -> None:
    """각 행에 관측일 T 직전 KRX 영업일의 일봉 수익률(소수, ``actual_ret`` 와 동일)을 붙입니다."""
    try:
        prev_td = trading_calendar.last_trading_day_before(t_day)
    except ValueError:
        for r in rows:
            r["actual_ret_prev_day"] = None
        return
    for r in rows:
        code = str(r.get("code", "")).zfill(6)
        if not code:
            r["actual_ret_prev_day"] = None
            continue
        r["actual_ret_prev_day"] = stocks.actual_return_on_date(returns, code, prev_td)


def _rise_band_for_row(
    pred_ret_pct: float | None,
    actual_ret_ratio: float | None,
) -> str:
    """표시 구간: ``high``(>=20), ``mid``(10~20), ``low``(<10 또는 값 없음)."""
    max_pct = float("-inf")
    if pred_ret_pct is not None and math.isfinite(float(pred_ret_pct)):
        max_pct = max(max_pct, float(pred_ret_pct))
    if actual_ret_ratio is not None and math.isfinite(float(actual_ret_ratio)):
        max_pct = max(max_pct, float(actual_ret_ratio) * 100.0)
    if max_pct >= config.BIG_MOVE_THRESHOLD * 100.0 - 1e-9:
        return "high"
    if 10.0 - 1e-9 <= max_pct < config.BIG_MOVE_THRESHOLD * 100.0 - 1e-9:
        return "mid"
    return "low"


def _build_rebuild_learning_payload(
    day_reports: list[report.DayReport],
    s0: date,
    s1: date,
) -> dict[str, object]:
    """
    From~To(포함) 캘린더 구간에 해당하는 ``DayReport`` 만 골라,
    일자별 뉴스 샘플 규모·예측%−실제% 괴리·누적 MAE·pred_high 적중 누적을 만듭니다.

    ``snapshot_rebuild_learning.merge_extended_rebuild_into_snapshot`` 에 넘길
    ``rebuild_learning`` 하위 dict(``daily``·``summary`` 등)를 만듭니다.
    """
    thr = float(config.BIG_MOVE_THRESHOLD)
    in_range = [dr for dr in day_reports if s0 <= dr.trading_day <= s1]
    in_range.sort(key=lambda dr: dr.trading_day)

    cum_abs_gap = 0.0
    cum_gap_n = 0
    cum_ph_hits = 0
    cum_ph_den = 0

    # 구간 전체 원인 누적(예측 과대/과소, 고확신 오판, 지연뉴스 관여, 약신호 관여 등)
    total_over_pred = 0
    total_under_pred = 0
    total_false_high = 0
    total_false_high_neg = 0
    total_false_high_low = 0
    total_false_high_late_hit = 0
    total_false_high_weak_signal = 0
    false_high_kw_counter: Counter[str] = Counter()

    daily: list[dict[str, object]] = []
    for dr in in_range:
        gaps: list[float] = []
        for r in dr.rows_compare:
            pr = r.get("pred_ret")
            ar = r.get("actual_ret")
            if pr is None or ar is None:
                continue
            g = float(pr) - float(ar) * 100.0
            if math.isfinite(g):
                gaps.append(g)

        n_both = len(gaps)
        day_mae = sum(abs(x) for x in gaps) / n_both if n_both else None
        day_mean_gap = sum(gaps) / n_both if n_both else None

        cum_abs_gap += sum(abs(x) for x in gaps)
        cum_gap_n += n_both
        cum_mae = cum_abs_gap / cum_gap_n if cum_gap_n else None

        ph_hits = 0
        ph_den = 0
        over_pred = 0
        under_pred = 0
        false_high = 0
        false_high_neg = 0
        false_high_low = 0
        false_high_late_hit = 0
        false_high_weak_signal = 0
        day_false_high_kw_counter: Counter[str] = Counter()
        for r in dr.rows_compare:
            pr = r.get("pred_ret")
            ar = r.get("actual_ret")
            if pr is not None and ar is not None:
                g = float(pr) - float(ar) * 100.0
                if math.isfinite(g):
                    if g > 1e-9:
                        over_pred += 1
                    elif g < -1e-9:
                        under_pred += 1
            if not r.get("pred_high"):
                continue
            if ar is None:
                continue
            ph_den += 1
            if float(ar) >= thr:
                ph_hits += 1
            else:
                false_high += 1
                ar_f = float(ar)
                if ar_f < 0:
                    false_high_neg += 1
                elif ar_f < thr:
                    false_high_low += 1
                if bool(r.get("late_news_hit")):
                    false_high_late_hit += 1
                if int(r.get("keyword_hits", 0) or 0) <= 1 or float(
                    r.get("mention_score", 0.0) or 0.0
                ) < 0.10:
                    false_high_weak_signal += 1
                for kw in (r.get("keywords") or [])[:12]:
                    if isinstance(kw, str) and kw.strip():
                        day_false_high_kw_counter[kw.strip()] += 1
        cum_ph_hits += ph_hits
        cum_ph_den += ph_den
        day_prec = ph_hits / ph_den if ph_den else None
        cum_prec = cum_ph_hits / cum_ph_den if cum_ph_den else None

        titles = dr.news_titles_sample or []
        terms = dr.news_highlight_terms or []

        total_over_pred += over_pred
        total_under_pred += under_pred
        total_false_high += false_high
        total_false_high_neg += false_high_neg
        total_false_high_low += false_high_low
        total_false_high_late_hit += false_high_late_hit
        total_false_high_weak_signal += false_high_weak_signal
        false_high_kw_counter.update(day_false_high_kw_counter)

        missed_rows, pred_miss_rows = snapshot_miss_diagnosis.build_miss_rows_for_day(dr)

        daily.append(
            {
                "trading_day": dr.trading_day.isoformat(),
                "news_titles_count": len(titles),
                "news_highlight_terms_count": len(terms),
                "news_highlight_terms_sample": terms[:12],
                "compare_rows": len(dr.rows_compare),
                "rows_pred_actual_both": n_both,
                "mean_abs_gap_pred_minus_actual_pct": round(day_mae, 4)
                if day_mae is not None
                else None,
                "mean_gap_pred_minus_actual_pct": round(day_mean_gap, 4)
                if day_mean_gap is not None
                else None,
                "cum_through_mean_abs_gap_pct": round(cum_mae, 4)
                if cum_mae is not None
                else None,
                "pred_high_precision_today": round(day_prec, 4)
                if day_prec is not None
                else None,
                "pred_high_precision_cumulative": round(cum_prec, 4)
                if cum_prec is not None
                else None,
                "pred_high_n_with_actual_today": ph_den,
                "pred_high_n_with_actual_cumulative": cum_ph_den,
                "over_pred_count_today": over_pred,
                "under_pred_count_today": under_pred,
                "false_positive_high_today": false_high,
                "false_positive_high_negative_today": false_high_neg,
                "false_positive_high_low_return_today": false_high_low,
                "false_positive_high_late_news_hit_today": false_high_late_hit,
                "false_positive_high_weak_signal_today": false_high_weak_signal,
                "false_positive_high_top_keywords_today": [
                    {"keyword": k, "count": int(v)}
                    for k, v in day_false_high_kw_counter.most_common(10)
                ],
                "missed_big_movers_today": missed_rows,
                "pred_high_misses_today": pred_miss_rows,
            }
        )

    miss_summary = snapshot_miss_diagnosis.recompute_miss_summary_from_daily(daily)

    return {
        "rebuild_learning": {
            "calendar_from": s0.isoformat(),
            "calendar_to": s1.isoformat(),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "big_move_threshold": thr,
            "daily": daily,
            "summary": {
                "days": len(daily),
                "final_cum_mean_abs_gap_pct": round(cum_abs_gap / cum_gap_n, 4)
                if cum_gap_n
                else None,
                "final_pred_high_precision": round(cum_ph_hits / cum_ph_den, 4)
                if cum_ph_den
                else None,
                "pred_high_total_with_actual": cum_ph_den,
                "over_pred_count_total": total_over_pred,
                "under_pred_count_total": total_under_pred,
                "false_positive_high_total": total_false_high,
                "false_positive_high_negative_total": total_false_high_neg,
                "false_positive_high_low_return_total": total_false_high_low,
                "false_positive_high_late_news_hit_total": total_false_high_late_hit,
                "false_positive_high_weak_signal_total": total_false_high_weak_signal,
                "false_positive_high_top_keywords_total": [
                    {"keyword": k, "count": int(v)}
                    for k, v in false_high_kw_counter.most_common(20)
                ],
                **miss_summary,
            },
        }
    }


def _enrich_cumulative_hit_rate(
    day_reports: list[report.DayReport], *, threshold_pct: float
) -> None:
    """
    ``pred_high_history`` 기준으로, 예측이 ``threshold_pct`` 이상이었던 날의 실제 분포를 집계합니다.
    괄호 표기용으로 ``cumulative_hit_x``(실제≥threshold 건수), ``cumulative_hit_z``(0<실제<threshold 건수),
    ``cumulative_hit_neg``(실제<0 건수), ``cumulative_hit_y``(실적 확정·예측≥임계 전체)와
    ``cumulative_nonneg_rate_pct``(실제>=0 비율, 0~100)를 넣습니다.
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
                r["cumulative_nonneg_rate_pct"] = None
                continue
            hist = r.get("pred_high_history") or []
            hits = 0
            mid_band = 0
            neg_band = 0
            nonneg_band = 0
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
                if apf >= -1e-9:
                    nonneg_band += 1
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
            r["cumulative_nonneg_rate_pct"] = (
                100.0 * nonneg_band / n_known
            ) if n_known else None


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
        forward_prediction_only: True면 모든 관측일을 예측 전용으로 처리(라이브 N=오늘).
            False여도 ``T`` 가 KST 오늘 이후이거나 일봉 상한 이후면 해당 일만 예측 전용(구간 종료일이 미래일 때).
        omit_target_calendar_days: 뉴스 fetch 시 해당 관측일 T의 **캘린더 일** 본문은 내려받지 않음( N 미마감 등).
        train_snapshot_mode: ``none`` | ``use`` | ``rebuild`` — 학습 스냅샷 재사용·갱신·전체 재생성.
        train_snapshot_cal_scope: ``(시작, 끝)`` 캘린더 일(포함) — ``use`` 모드에서 미반영 판단만 이 구간으로 한정.
            ``rebuild`` 모드이고 이 값이 있으면(구간 ``YYYYMMDD YYYYMMDD`` 실행) 파이프라인 말미에
            일자별 예측–실제 괴리 누적 분석을 ``breakout_train_snapshot.json`` 의 ``rebuild_learning`` 에 병합합니다.
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

    now_kst_pipe = datetime.now(trading_calendar.KST)
    today_kst = now_kst_pipe.date()
    ohlcv_cap = trading_calendar.ohlcv_request_end_cap_today()
    ohlcv_end = _resolve_pipeline_ohlcv_end(
        end_date, test_days, now_kst=now_kst_pipe
    )
    if ohlcv_end < end_date:
        print(
            f"가격 데이터: 일봉 상한을 {ohlcv_end} 로 둡니다 "
            f"(요청 끝 {end_date}, 미래·미확정 봉 제외).",
            flush=True,
        )
    elif ohlcv_end > min(end_date, ohlcv_cap):
        print(
            f"가격 데이터: 마감 확정된 관측 거래일 반영으로 일봉 요청 끝을 {ohlcv_end} 로 합니다.",
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
                force_retrain=train_snapshot_mode == "rebuild",
            )
        except Exception as e:
            print(f"ML 랭커 초기화 실패(휴리스틱만 사용): {e}", flush=True)
            ml_bundle = None

    ks11 = market_index.load_index_frame("KS11", test_start - timedelta(days=5), end_date)

    day_reports: list[report.DayReport] = []
    late_below_n = late_below_kw = late_gte_n = late_gte_kw = 0
    krx_movers_unavailable_any = False
    freeze_payload = (
        _load_prediction_freeze_payload() if config.PREDICTION_FREEZE_ENABLED else {}
    )
    freeze_changed = False

    for T in tqdm(test_days, desc="테스트일별 예측"):
        day_forward = _observation_day_forward_mode(
            T,
            today=today_kst,
            pipeline_forward_only=forward_prediction_only,
        )
        if day_forward and not forward_prediction_only and T > today_kst:
            print(
                f"관측일 T={T.isoformat()}: 예측 전용(KST 오늘={today_kst} 이후 거래일)",
                flush=True,
            )
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
        theme_w: dict[str, float] = {}
        if config.THEME_CARRYOVER_ENABLED:
            try:
                theme_w = theme_carryover.weights_for_observation_day(
                    T,
                    train_events=train_events,
                    news_by_calendar=news_by_calendar,
                    returns_df=returns,
                    threshold=config.BIG_MOVE_THRESHOLD,
                )
            except (ValueError, TypeError, OSError):
                theme_w = {}
        t_key = T.isoformat()
        frozen_items = freeze_payload.get(t_key) if config.PREDICTION_FREEZE_ENABLED else None
        refresh_frozen_preds = train_snapshot_mode == "rebuild"
        use_frozen = (
            config.PREDICTION_FREEZE_ENABLED
            and not refresh_frozen_preds
            and isinstance(frozen_items, list)
            and _freeze_entry_usable(frozen_items)
        )
        if use_frozen:
            preds = _prediction_rows_from_frozen_items(frozen_items)
            print(f"예측 고정 캐시 재사용: T={t_key} ({len(preds)}건)", flush=True)
        else:
            if refresh_frozen_preds and _freeze_entry_usable(frozen_items or []):
                print(
                    f"관측일 T={t_key}: --rebuild-train-snapshot — 예측 고정 캐시를 갱신합니다.",
                    flush=True,
                )
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
                theme_weights=theme_w or None,
            )
            if config.PREDICTION_FREEZE_ENABLED and (
                refresh_frozen_preds or not _freeze_entry_usable(freeze_payload.get(t_key) or [])
            ):
                freeze_payload[t_key] = _prediction_rows_to_frozen_items(preds)
                freeze_changed = True
        scoring_ctx = predict.build_scoring_context(blob, train_events)

        kospi_r = market_index.index_daily_return_pct(ks11, T)
        kospi_hint = None
        if kospi_r is not None:
            kospi_hint = f"당일 KOSPI 지수 전일대비 약 {kospi_r*100:.2f}%였습니다."

        now_kst_td = datetime.now(trading_calendar.KST)
        is_today_t = T == now_kst_td.date()
        krx_pct_by_code: dict[str, float] | None = None
        if day_forward:
            actual_big_movers: list[dict] = []
            # 당일 실행에서는 장중/장마감 직후 참고 표시를 위해 pykrx 스냅샷을 시도합니다.
            if is_today_t:
                krx_pct_by_code = stocks.try_krx_change_pct_by_code(T, returns_df=returns)
                if not krx_pct_by_code:
                    # pykrx 전종목 맵이 비면 종목별 pykrx·네이버 순으로 보강.
                    krx_pct_by_code = stocks.best_effort_intraday_pct_by_code(
                        T, codes, returns_df=returns
                    )
        else:
            krx_pct_by_code = stocks.try_krx_change_pct_by_code(T, returns_df=returns)
            # 오늘(T) 장중에는 일봉이 아직 없을 수 있어 best-effort 실시간 맵으로 한 번 더 보강.
            if (
                (not krx_pct_by_code)
                and is_today_t
                and trading_calendar.is_trading_day(T)
                and not trading_calendar.is_krx_daily_bar_effective_closed(
                    T, now_kst=now_kst_td
                )
            ):
                krx_pct_by_code = stocks.best_effort_intraday_pct_by_code(
                    T, codes, returns_df=returns
                )
            if krx_pct_by_code:
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
                # 경고 문구는 콘솔에 반복 노이즈를 만들어 비활성화.
                # 실제 집계는 아래 OHLCV 폴백(actual_big_movers)로 계속 진행합니다.

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
        pred_pct_mid_min = 10.0
        row_pred_min = 0.0 if day_forward else pred_pct_mid_min
        actual_10up_by_code: dict[str, dict] = {}
        if not day_forward:
            if krx_pct_by_code:
                actual_10up_rows = stocks.big_movers_from_krx_pct_map(
                    krx_pct_by_code, pred_pct_mid_min / 100.0, names
                )
            else:
                movers_10up = stocks.big_movers_on_date(returns, T, pred_pct_mid_min / 100.0)
                actual_10up_rows = [
                    {
                        "code": str(r["Code"]).zfill(6),
                        "name": str(r["Name"]),
                        "ret_pct": float(r["return_pct"]) * 100.0,
                    }
                    for _, r in movers_10up.iterrows()
                ]
            actual_10up_by_code = {
                str(x.get("code", "")).zfill(6): x for x in actual_10up_rows if x.get("code")
            }

        if not day_forward:
            preds_by_code: dict[str, predict.PredictionRow] = {pr.code: pr for pr in preds}
            for m in actual_10up_by_code.values():
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
                pred_mid = (
                    pred_ret is not None
                    and pred_pct_mid_min <= pred_ret < pred_pct_min
                )
                row_d = {
                    "code": code,
                    "market_segment": market_by_code.get(str(code).zfill(6), "other"),
                    "name": m["name"],
                    "reasons_html": reasons_html,
                    "keywords": keywords,
                    "keyword_hits": (pr.keyword_hits if pr is not None else len(keywords)),
                    "mention_score": (pr.mention_score if pr is not None else 0.0),
                    "pred_ret": pred_ret,
                    "actual_ret": act,
                    "actual_big": act >= config.BIG_MOVE_THRESHOLD,
                    "pred_high": pred_high,
                    "pred_mid": pred_mid,
                    "rise_band": _rise_band_for_row(pred_ret, act),
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
                    row_d["late_news_hit"] = bool(late_hit)
                else:
                    row_d["late_news_hit"] = None

        seen_row_codes = {r["code"] for r in rows_compare}
        for pr in preds:
            # 예측 10% 이상 후보를 표에 반영(필터 라디오로 20%+/10~20 전환).
            if pr.predicted_return_pct < row_pred_min:
                continue
            if pr.code in seen_row_codes:
                continue
            seen_row_codes.add(pr.code)
            act = None if day_forward else _actual_ret_for_code(pr.code)
            reasons_html = "<br/>".join(pr.reasons)
            if pr.matched_keywords:
                reasons_html += "<br/><em>일치 키워드</em> " + ", ".join(
                    f"<mark>{w}</mark>" for w in pr.matched_keywords[:10]
                )
            ph = pr.predicted_return_pct >= pred_pct_min
            pm = pred_pct_mid_min <= pr.predicted_return_pct < pred_pct_min
            rows_compare.append(
                {
                    "code": pr.code,
                    "market_segment": market_by_code.get(str(pr.code).zfill(6), "other"),
                    "name": pr.name,
                    "reasons_html": reasons_html,
                    "keywords": pr.matched_keywords,
                    "keyword_hits": pr.keyword_hits,
                    "mention_score": pr.mention_score,
                    "pred_ret": pr.predicted_return_pct,
                    "actual_ret": act,
                    "actual_big": act is not None and act >= config.BIG_MOVE_THRESHOLD,
                    "pred_high": ph,
                    "pred_mid": pm,
                    "rise_band": _rise_band_for_row(pr.predicted_return_pct, act),
                    "gap_analysis_html": _gap_analysis_html_for_row(
                        pr.predicted_return_pct,
                        act,
                        pr,
                        pr.matched_keywords,
                        blob,
                        kospi_hint,
                        late_blob,
                    ),
                    "late_news_hit": (
                        news.late_blob_covers_keywords(late_blob, list(pr.matched_keywords))
                        if (
                            config.USE_DECISION_NEWS_INTRADAY_CUTOFF
                            and late_blob
                            and pr.matched_keywords
                        )
                        else None
                    ),
                    **_pred_reason_fields(pr, reasons_html),
                }
            )

        if day_forward and preds:
            _sync_forward_day_rows_from_predictions(
                rows_compare,
                preds,
                market_by_code=market_by_code,
                pred_pct_min=pred_pct_min,
                pred_pct_mid_min=pred_pct_mid_min,
                blob=blob,
                kospi_hint=kospi_hint,
                late_blob=late_blob,
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

        _enrich_rows_disclosure_hits(rows_compare, T)
        if include_target_calendar_news:
            _enrich_rows_news_evidence(rows_compare, early_rows, actual_ctx_rows)
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
            r["rise_band"] = _rise_band_for_row(r.get("pred_ret"), r.get("actual_ret"))

        for pr in preds:
            act = None if day_forward else _actual_ret_for_code(pr.code)
            if (
                not day_forward
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
                not day_forward
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

        _enrich_rows_actual_ret_prev_day(rows_compare, returns, T)

        if config.THEME_CARRYOVER_ENABLED and rows_compare:
            theme_carryover.persist_rich_snapshot(
                T,
                actual_big_movers=actual_big_movers,
                rows_compare=rows_compare,
                highlight_terms=hl_terms,
                threshold=config.BIG_MOVE_THRESHOLD,
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
                forward_observation=day_forward,
            )
        )

    # 장중 시작 실행이 장마감 이후까지 이어진 경우: 확정된 관측 거래일 일봉을 다시 읽어 실제값 보정.
    now_end_kst = datetime.now(trading_calendar.KST)
    refresh_ohlcv_end = _resolve_pipeline_ohlcv_end(
        end_date, test_days, now_kst=now_end_kst
    )
    if refresh_ohlcv_end > ohlcv_end:
        print(
            f"장 마감 반영 재조회: 일봉을 {refresh_ohlcv_end} 까지 읽어 실제 수익률을 반영합니다.",
            flush=True,
        )
        ohlcv_post = stocks.build_ohlcv_long(
            train_start - timedelta(days=10),
            refresh_ohlcv_end,
            force_full_listing=not forward_prediction_only,
            skip_gap_download=skip_ohlcv_gap_download,
            refresh_tail_days=1,
        )
        returns_post = stocks.daily_returns_table(ohlcv_post)
        for dr in day_reports:
            if dr.forward_observation:
                continue
            if dr.trading_day > refresh_ohlcv_end:
                continue
            if not trading_calendar.is_krx_daily_bar_effective_closed(
                dr.trading_day, now_kst=now_end_kst
            ):
                continue
            _backfill_day_actuals_from_returns(
                dr,
                returns=returns_post,
                threshold=config.BIG_MOVE_THRESHOLD,
            )
        returns = returns_post

    _enrich_cumulative_accuracy_avg(day_reports)
    prediction_accuracy_cache.merge_from_day_reports(day_reports)
    prediction_accuracy_cache.merge_feedback_buckets_from_day_reports(day_reports)
    prediction_accuracy_cache.merge_high_pred_history_from_day_reports(
        day_reports, threshold_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    prediction_accuracy_cache.merge_mid_pred_history_from_day_reports(
        day_reports, low_pct=10.0, high_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    prediction_accuracy_cache.merge_all_pred_history_from_day_reports(
        day_reports, min_pct=10.0
    )
    prediction_accuracy_cache.apply_cached_cumulative_fallback(day_reports)
    prediction_accuracy_cache.enrich_rows_pred_high_history(day_reports)
    _enrich_cumulative_hit_rate(
        day_reports, threshold_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    _enrich_cumulative_actual_over_pred_from_history(day_reports)
    _enrich_cumulative_actual_over_pred_from_history_for_field(
        day_reports,
        history_field="pred_mid_history",
        out_field="cumulative_accuracy_10_20_avg",
    )
    _enrich_cumulative_actual_over_pred_from_history_for_field(
        day_reports,
        history_field="pred_all_history",
        out_field="cumulative_accuracy_all_avg",
    )

    if (
        train_snapshot_mode == "rebuild"
        and train_snapshot_cal_scope is not None
        and not forward_prediction_only
    ):
        s0, s1 = train_snapshot_cal_scope
        rl = _build_rebuild_learning_payload(day_reports, s0, s1)["rebuild_learning"]
        theme_days = sorted(
            {dr.trading_day for dr in day_reports if s0 <= dr.trading_day <= s1}
        )
        theme = snapshot_rebuild_learning.build_market_theme_flow(
            theme_days,
            news_by_calendar,
            returns,
            names,
        )
        gap = prediction_accuracy_cache.export_gap_rollup_for_calendar_range(s0, s1)
        if snapshot_rebuild_learning.merge_extended_rebuild_into_snapshot(
            rebuild_learning=rl,
            market_theme_flow=theme,
            prediction_gap_rollup=gap,
        ):
            print(
                "학습 스냅샷: rebuild_learning(미예측 급등·고예측 실패 진단 포함) + "
                "market_theme_flow + prediction_gap_rollup 병합 저장.",
                flush=True,
            )
    if config.PREDICTION_FREEZE_ENABLED and freeze_changed:
        _save_prediction_freeze_payload(freeze_payload)

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


def _resolve_pipeline_ohlcv_end(
    end_date: date,
    test_days: Sequence[date],
    *,
    now_kst: datetime,
) -> date:
    """
    일봉 OHLCV 요청 ``end`` 날짜.

    ``ohlcv_request_end_cap_today()`` 상한에 더해, 이미 마감·확정된 관측 거래일(``test_days``)은
    구간 종료일이 더 뒤여도 해당 일까지 요청합니다. (캐시가 전 거래일까지만 있어도 5/18 실제값 조회)
    """
    cap = trading_calendar.ohlcv_request_end_cap_today()
    today = now_kst.date()
    ohlcv_end = min(end_date, cap)
    for t in test_days:
        if t > end_date or not trading_calendar.is_trading_day(t):
            continue
        if t > today:
            continue
        if t < today or trading_calendar.is_krx_daily_bar_effective_closed(
            t, now_kst=now_kst
        ):
            ohlcv_end = max(ohlcv_end, min(t, end_date))
    return min(ohlcv_end, end_date)


def _observation_day_forward_mode(
    t: date,
    *,
    today: date,
    pipeline_forward_only: bool,
) -> bool:
    """
    관측일 ``T`` 가 KST **오늘 이후** 거래일이면 예측 전용.

    과거·당일(장 마감 후) 거래일은 실제 수익률을 채웁니다. 일봉 캐시가 짧아도
    ``T > ohlcv_end`` 로 예측 전용 처리하지 않습니다(구간 배치 회귀 방지).
    """
    if pipeline_forward_only:
        return True
    return t > today


def _omit_target_calendar_before_close(
    test_days: Sequence[date],
    *,
    now_kst: datetime,
) -> frozenset[date]:
    """
    관측 거래일 ``T`` 가 KST ``오늘`` 이고 14:30 전이면, 해당 ``T`` 캘린더 뉴스는 받지 않습니다.

    예측 입력은 직전 거래일 ``NEWS_CUTOFF_*`` 까지이므로 당일 ``T`` 본장 전 종목·시장 뉴스는 불필요합니다.
    """
    today = now_kst.date()
    out: list[date] = []
    for t in test_days:
        if (
            t == today
            and trading_calendar.is_trading_day(t)
            and (now_kst.hour, now_kst.minute) < (14, 30)
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


def _expand_range_test_days_with_existing_report_days(
    test_days: list[date],
    *,
    all_sessions: list[date],
) -> list[date]:
    """
    구간 실행 시, 기존 월간 리포트에 이미 들어 있는 날짜만 추가로 포함합니다.

    즉, ``test_days``(이번 실행 범위) + ``기존 파일에 이미 있던 day-YYYY-MM-DD`` 집합으로
    재계산 범위를 구성합니다. 월 전체 강제 확장은 하지 않습니다.
    """
    if not test_days:
        return test_days
    session_set = set(all_sessions)
    months = {(d.year, d.month) for d in test_days}
    expanded: set[date] = set(test_days)
    for y, m in months:
        report_path = config.OUTPUT_DIR / f"report_{y}.{m:02d}.html"
        if not report_path.is_file():
            continue
        try:
            html = report_path.read_text(encoding="utf-8")
        except OSError:
            continue
        hits = re.findall(r'id="day-(\d{4}-\d{2}-\d{2})"', html)
        old_days: set[date] = set()
        for s in hits:
            try:
                yy, mm, dd = s.split("-")
                d = date(int(yy), int(mm), int(dd))
            except ValueError:
                continue
            if d.year == y and d.month == m and d in session_set:
                old_days.add(d)
        expanded.update(old_days)
        if old_days:
            print(
                f"기존 월간 리포트 감지: {report_path.name} → "
                f"기존 포함일 {len(old_days)}일 + 이번 범위를 병합해 갱신합니다.",
                flush=True,
            )
    return sorted(expanded)


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
                    f"관측일 T={t} 14:30 전: 해당일 캘린더 뉴스는 수집하지 않습니다.",
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
        test_days = _expand_range_test_days_with_existing_report_days(
            test_days,
            all_sessions=sessions,
        )

        future_td = [t for t in test_days if t > today]
        if future_td:
            print(
                f"구간 {d_from.isoformat()}~{d_to.isoformat()}: "
                f"KST 오늘({today}) 이후 거래일 {len(future_td)}일 "
                f"({future_td[0].isoformat()}~{future_td[-1].isoformat()})은 "
                "예측 전용(실제 상승률·누적 정확도는 확정 후 갱신)으로 처리합니다.",
                flush=True,
            )

        omit_t_cal = _omit_target_calendar_before_close(test_days, now_kst=now_kst)
        if omit_t_cal:
            for t in sorted(omit_t_cal):
                print(
                    f"관측일 T={t} 14:30 전: 해당일 캘린더 뉴스는 수집하지 않습니다.",
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

    # daily: N=today → T=next trading day
    # dated: argv YYYYMMDD 를 관측일 T로 해석, 기준일 N은 T 직전 거래일
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
        t_day = arg_date  # dated: target observation day
        if not trading_calendar.is_trading_day(t_day):
            print(f"지정한 관측일 T={t_day} 은(는) 거래일이 아닙니다.")
            return
        try:
            n_day = trading_calendar.last_trading_day_before(t_day)
        except ValueError as e:
            print(e)
            return

    end_date = max(today, t_day)
    end_date = min(end_date, date(2026, 12, 31))

    test_days = [t_day]
    if t_day not in trading_calendar.trading_sessions_in_range(train_start, end_date):
        print(f"관측일 {t_day} 이(가) 캘린더 범위에 없습니다. end_date={end_date}")
        return

    forward_prediction_only = t_day >= today
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
    if po.day_reports and po.day_reports[0].trading_day != t_day:
        actual_t = po.day_reports[0].trading_day
        print(
            f"관측일 보정: 계산된 T={t_day.isoformat()} 대신 "
            f"리포트 데이터 T={actual_t.isoformat()} 를 사용합니다.",
            flush=True,
        )
        t_day = actual_t

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
