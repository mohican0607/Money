"""뉴스 수집·freeze·스케줄링 (news_io + freeze + scheduling)."""
from __future__ import annotations

import json
import math
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta

import requests
from tqdm import tqdm

from src import config, news, predict, report, snapshot_rebuild_learning, trading_calendar
from src.prediction import prediction_ranking


# --- news_io (from news_io.py) ---

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
        """단일 캘린더 일 뉴스를 세션 재사용으로 수집해 (일자, rows) 를 반환."""
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

# --- freeze (from freeze.py) ---

def _load_prediction_freeze_payload() -> dict[str, list[dict]]:
    """``prediction_freeze_by_t.json`` 에서 관측일 T별 고정 예측 후보를 읽습니다. 스키마 불일치·손상 시 빈 dict."""
    path = config.PREDICTION_FREEZE_PATH
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    if raw.get("_schema_version") != config.PREDICTION_FREEZE_SCHEMA_VERSION:
        return {}
    out: dict[str, list[dict]] = {}
    for k, v in raw.items():
        if k.startswith("_"):
            continue
        if isinstance(k, str) and isinstance(v, list):
            out[k] = [x for x in v if isinstance(x, dict)]
    return out


def _save_prediction_freeze_payload(payload: dict[str, list[dict]]) -> None:
    """관측일 T별 고정 예측 후보를 ``prediction_freeze_by_t.json`` 에 저장합니다."""
    path = config.PREDICTION_FREEZE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    to_write = {
        "_schema_version": config.PREDICTION_FREEZE_SCHEMA_VERSION,
        **payload,
    }
    path.write_text(json.dumps(to_write, ensure_ascii=False, indent=2), encoding="utf-8")


def _frozen_predicted_return_pct(item: dict) -> float | None:
    """고정 캐시 항목에서 ``predicted_return_pct``(또는 레거시 ``pred_ret``)를 유한 실수로 꺼냅니다."""
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


def _ignore_freeze_for_trading_day(
    T: date,
    *,
    train_snapshot_mode: str,
    cal_scope: tuple[date, date] | None,
    respect_prediction_freeze: bool = False,
) -> bool:
    """
    예측 고정 캐시를 쓰지 않고 다시 예측할 관측일 T 인지 판단.

    - ``python main.py From To`` (기본): **freeze 가 있으면 재사용**, 없는 T 만 신규 예측·저장.
      (예: N일 14:30 ``main.py T`` 로 확정한 예측은 이후 구간 실행에서 T 블록에 유지)
    - ``--rebuild-train-snapshot`` + From~To: 구간 안 모든 T 를 freeze 무시·재계산.
    - ``--use-freeze``: ``--rebuild`` 구간에서도 freeze 재사용(명시적).
    - ``--weekly``·단일일 등 ``cal_scope`` 없음: ``rebuild`` 일 때만 freeze 무시.
      ``append_learning`` 단일일(장 마감 후 actual·테마만 갱신)은 freeze 유지.
    """
    if respect_prediction_freeze:
        return False
    if train_snapshot_mode == "rebuild" and cal_scope is None:
        return True
    if cal_scope is None:
        return False
    if train_snapshot_mode == "rebuild":
        s0, s1 = cal_scope
        return s0 <= T <= s1
    return False


def _prediction_rows_from_frozen_items(items: list[dict]) -> list[predict.PredictionRow]:
    """고정 캐시 dict 목록을 ``PredictionRow`` 리스트로 변환합니다. 파싱 실패 항목은 건너뜁니다."""
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
                    rank_score=(
                        float(x["rank_score"])
                        if x.get("rank_score") is not None
                        else None
                    ),
                    rank_position=(
                        int(x["rank_position"])
                        if x.get("rank_position") is not None
                        else None
                    ),
                    confidence_tier=str(x.get("confidence_tier") or "none"),
                    investor_flow_score=float(x.get("investor_flow_score", 0.0) or 0.0),
                    foreign_net_vol_ratio=float(x.get("foreign_net_vol_ratio", 0.0) or 0.0),
                )
            )
        except (TypeError, ValueError):
            continue
    return rows


def _display_prediction_rows_for_freeze(rows: list[predict.PredictionRow]) -> list[predict.PredictionRow]:
    """리포트 표에 고정할 예측 후보(고·중 확신)만 골라 freeze 에 저장합니다."""
    shown = [
        r
        for r in rows
        if prediction_ranking.is_high_confidence_prediction(r)
        or prediction_ranking.is_mid_confidence_prediction(r)
    ]
    if shown:
        return shown
    cap = max(1, int(config.PRED_FORWARD_SHOW_MAX))
    return list(rows[:cap])


def _prediction_rows_to_frozen_items(rows: list[predict.PredictionRow]) -> list[dict]:
    """``PredictionRow`` 리스트를 고정 캐시 JSON에 넣을 dict 목록으로 직렬화합니다."""
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
            "rank_score": (
                None if getattr(r, "rank_score", None) is None else float(r.rank_score)
            ),
            "rank_position": (
                None
                if getattr(r, "rank_position", None) is None
                else int(r.rank_position)
            ),
            "confidence_tier": str(getattr(r, "confidence_tier", "none") or "none"),
            "investor_flow_score": float(getattr(r, "investor_flow_score", 0.0) or 0.0),
            "foreign_net_vol_ratio": float(getattr(r, "foreign_net_vol_ratio", 0.0) or 0.0),
        }
        for r in rows
    ]

# --- scheduling (from scheduling.py) ---

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


def validate_range_n_plus_one(
    n_day: date,
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    from src.pipeline.early_validate import validate_range_n_plus_one as _validate

    return _validate(n_day, t_day, now_kst=now_kst)


def validate_dated_n_day(
    n_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    from src.pipeline.early_validate import validate_dated_n_day as _validate

    return _validate(n_day, now_kst=now_kst)


def validate_dated_t_day(
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    from src.pipeline.early_validate import validate_dated_t_day as _validate

    return _validate(t_day, now_kst=now_kst)


def _observation_day_forward_mode(
    t: date,
    *,
    today: date,
    pipeline_forward_only: bool,
    now_kst: datetime | None = None,
) -> bool:
    """
    관측일 ``T`` 를 예측 전용(실적·당일 테마 미확정)으로 볼지.

    - ``T > today`` (미래 캘린더 관측일)
    - ``T == today`` 거래일이고 **정규장 마감(15:30 KST) 전**
    - ``pipeline_forward_only`` (레거시 일괄 플래그; dated/daily 경로는 아래 규칙 우선)
    """
    if now_kst is None:
        now_kst = datetime.now(trading_calendar.KST)
    if t > today:
        return True
    if t < today:
        return False
    if (
        t == today
        and trading_calendar.is_trading_day(t)
        and trading_calendar.is_before_krx_regular_close_kst(t, now_kst=now_kst)
    ):
        return True
    if pipeline_forward_only:
        return True
    return False


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


def _refresh_incomplete_market_theme_on_day_reports(
    day_reports: list[report.DayReport],
    *,
    news_by_calendar: dict,
    returns_df,
    listing_names: dict[str, str],
    news_cutoff_label: str,
    now_kst: datetime | None = None,
) -> None:
    """장 마감 후에도 테마 요약이 placeholder 인 ``DayReport`` 를 종가·뉴스 기준으로 다시 채웁니다."""
    now = now_kst or datetime.now(trading_calendar.KST)
    refreshed = 0
    for dr in day_reports:
        if dr.forward_observation:
            continue
        if not trading_calendar.is_krx_daily_bar_effective_closed(
            dr.trading_day, now_kst=now
        ):
            continue
        if not snapshot_rebuild_learning.market_theme_html_is_incomplete(
            dr.market_theme_html
        ):
            continue
        theme_inner = snapshot_rebuild_learning.market_theme_html_for_trading_day(
            dr.trading_day,
            news_by_calendar,
            returns_df,
            listing_names,
            news_cutoff_label=news_cutoff_label,
        )
        if snapshot_rebuild_learning.market_theme_html_is_incomplete(theme_inner):
            continue
        flow_rows = snapshot_rebuild_learning.build_market_theme_flow(
            [dr.trading_day],
            news_by_calendar,
            returns_df,
            listing_names,
        )
        flow_row = flow_rows[0] if flow_rows else None
        early_rows = snapshot_rebuild_learning._early_news_rows_for_trading_day(
            news_by_calendar, dr.trading_day
        )
        if flow_row and dr.rows_compare:
            snapshot_rebuild_learning._patch_flow_row_strong_movers_from_compare(
                flow_row, dr.rows_compare, listing_names, early_rows
            )
        dr.market_theme_html = (
            snapshot_rebuild_learning.format_market_theme_flow_html(
                flow_row,
                news_cutoff_label=news_cutoff_label,
                threshold_pct=float(config.BIG_MOVE_THRESHOLD) * 100.0,
            )
            if flow_row
            else theme_inner
        )
        snapshot_rebuild_learning.enrich_compare_rows_theme_for_day(
            dr,
            flow_row,
            listing_names=listing_names,
            early_rows=early_rows,
            news_cutoff_label=news_cutoff_label,
        )
        refreshed += 1
    if refreshed:
        print(
            f"장 마감 반영: 당일 테마 요약 재생성 {refreshed}일(이번 실행 거래일)",
            flush=True,
        )