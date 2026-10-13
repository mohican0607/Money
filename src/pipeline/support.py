"""뉴스 수집·freeze·스케줄링 (news_io + freeze + scheduling)."""
from __future__ import annotations

import json
import math
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta
from typing import Any

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
        print(
            "예측 고정 캐시 스키마 불일치 "
            f"(저장={raw.get('_schema_version')!r}, "
            f"현재={config.PREDICTION_FREEZE_SCHEMA_VERSION}) "
            f"→ 재사용하지 않고 신규 계산합니다. ({path})",
            flush=True,
        )
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


_EMPTY_SLATE_KEY = "_empty_slate"


def _is_empty_slate_freeze(items: list[dict] | None) -> bool:
    """고·중 확신 0건으로 확정한 빈 slate 마커인지."""
    if not items or len(items) != 1:
        return False
    return bool(items[0].get(_EMPTY_SLATE_KEY))


def _freeze_entry_usable(items: list[dict]) -> bool:
    """관측일 T별 고정 캐시에 재사용할 예측이 있으면 True.

    빈 slate 마커는 재사용하지 않는다(다음 실행이 다시 후보를 채운다).
    이미 저장된 freeze(고·중·none)는 재사용한다.
    """
    if not items:
        return False
    if _is_empty_slate_freeze(items):
        return False
    real = [x for x in items if not x.get(_EMPTY_SLATE_KEY)]
    if not real:
        return False
    ok = False
    for x in real:
        p = _frozen_predicted_return_pct(x)
        if p is not None and math.isfinite(p):
            ok = True
            break
    if not ok:
        return False
    cap = int(config.PRED_FORWARD_SHOW_MAX)
    if len(real) <= 1:
        # 1종목 freeze 는 재사용하지 않음(레짐 축소로 말라 버린 slate).
        return False
    if len(real) >= 3 and cap >= 6 and len(real) < min(cap, 6):
        if all(str(x.get("confidence_tier") or "none") == "high" for x in real):
            return False
    return True


def effective_train_snapshot_cal_scope(
    *,
    train_snapshot_mode: str,
    train_snapshot_cal_scope: tuple[date, date] | None,
    day_reports: list,
    forward_prediction_only: bool,
) -> tuple[date, date] | None:
    """
    ``rebuild_learning`` 병합 대상 캘린더 구간.

  ``--append-rebuild-learning`` 단일일 실행은 ``train_snapshot_cal_scope`` 가
    ``None`` 으로 들어오던 경우가 있어, 장 마감 확정 ``DayReport`` 의 T 로 보정합니다.
    """
    if forward_prediction_only:
        return None
    if train_snapshot_mode not in ("rebuild", "append_learning"):
        return None
    if train_snapshot_cal_scope is not None:
        return train_snapshot_cal_scope
    closed_days = sorted(
        {
            dr.trading_day
            for dr in day_reports
            if not getattr(dr, "forward_observation", False)
        }
    )
    if not closed_days:
        return None
    return (closed_days[0], closed_days[-1])


def _freeze_saved_on_date(items: list[dict]) -> date | None:
    """freeze 항목에 기록된 저장일. 없으면 None(구 캐시는 연휴 갭 때 재계산)."""
    for x in items:
        if not isinstance(x, dict) or x.get(_EMPTY_SLATE_KEY):
            continue
        raw = str(x.get("saved_on") or "").strip()[:10]
        if len(raw) == 10:
            try:
                return date.fromisoformat(raw)
            except ValueError:
                return None
        break
    return None


def _freeze_is_stale_long_gap(
    observation_day: date,
    items: list[dict] | None,
    *,
    now_kst: datetime | None = None,
) -> bool:
    """연휴 갭 freeze를 관측일 T 당일에 다시 계산할지."""
    if not config.PRED_FREEZE_REFRESH_LONG_GAP:
        return False
    if not items:
        return False
    if not trading_calendar.is_long_session_gap(observation_day):
        return False
    now = now_kst or datetime.now(trading_calendar.KST)
    if now.date() < observation_day:
        return False
    saved = _freeze_saved_on_date(items)
    if saved is None:
        return now.date() >= observation_day
    return saved < observation_day


def _should_reuse_prediction_freeze(
    *,
    ignore_freeze_for_trading_day: bool,
    frozen_items: list[dict] | None,
    observation_day: date | None = None,
    now_kst: datetime | None = None,
) -> bool:
    """
    14:30에 확정·저장된 예측 후보를 다시 계산하지 않고 재사용할지.

    장 마감 후(``day_forward=False``)에도 freeze 가 있으면 True — actual·테마만 갱신합니다.
    연휴 갭(직전 세션과 T 사이 4일+)이고 저장일이 T 이전이면 T 당일 재랭킹합니다.
    """
    if not config.PREDICTION_FREEZE_ENABLED:
        return False
    if ignore_freeze_for_trading_day:
        return False
    if not isinstance(frozen_items, list):
        return False
    if not _freeze_entry_usable(frozen_items):
        return False
    if observation_day is not None and _freeze_is_stale_long_gap(
        observation_day, frozen_items, now_kst=now_kst
    ):
        return False
    return True


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


def _filter_frozen_items_to_listed(
    items: list[dict] | None,
    listed_codes: set[str] | list[str],
    *,
    returns_df: Any | None = None,
    observation_day: date | None = None,
) -> list[dict]:
    """현재 KRX 상장 목록에 없거나 관측일 기준 거래정지·상장폐지인 freeze 항목을 뺍니다."""
    if not items:
        return []
    if _is_empty_slate_freeze(items):
        return list(items)
    allowed = {str(c).zfill(6) for c in listed_codes if str(c).strip()}
    out: list[dict] = []
    for x in items:
        if x.get(_EMPTY_SLATE_KEY):
            out.append(x)
            continue
        code = str(x.get("code", "")).zfill(6)
        if code in allowed:
            out.append(x)
    if returns_df is None or observation_day is None:
        return out
    from src import stocks as stocks_mod

    kept: list[dict] = []
    for x in out:
        if x.get(_EMPTY_SLATE_KEY):
            kept.append(x)
            continue
        code = str(x.get("code", "")).zfill(6)
        if stocks_mod.is_observation_day_trading_halted(returns_df, code, observation_day):
            continue
        kept.append(x)
    return kept


def _prediction_rows_from_frozen_items(items: list[dict]) -> list[predict.PredictionRow]:
    """고정 캐시 dict 목록을 ``PredictionRow`` 리스트로 변환합니다. 파싱 실패 항목은 건너뜁니다."""
    if _is_empty_slate_freeze(items):
        return []
    rows: list[predict.PredictionRow] = []
    for x in items:
        if x.get(_EMPTY_SLATE_KEY):
            continue
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
                    ml_precision_score=(
                        float(x["ml_precision_score"])
                        if x.get("ml_precision_score") is not None
                        else None
                    ),
                    keyword_hits=int(x.get("keyword_hits", 0) or 0),
                    mention_score=float(x.get("mention_score", 0.0) or 0.0),
                    theme_carryover_score=float(x.get("theme_carryover_score", 0.0) or 0.0),
                    industry_theme_overlap=float(
                        x.get("industry_theme_overlap", 0.0) or 0.0
                    ),
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
                    pred_source=str(x.get("pred_source") or ""),
                    ret_lag1=float(x.get("ret_lag1", 0.0) or 0.0),
                    prior_limit_up=bool(x.get("prior_limit_up", False)),
                )
            )
        except (TypeError, ValueError):
            continue
    return rows


def _display_prediction_rows_for_freeze(rows: list[predict.PredictionRow]) -> list[predict.PredictionRow]:
    """리포트·freeze 에 고정할 **확신 후보만**.

    고·중확신, 또는 급등 점수(``PRED_REPORT_CONVICTION_MIN``) 이상만 넣는다.
    맞출 때까지 none 으로 채우지 않는다 — 없으면 빈 표.
    """
    cap = max(1, int(config.PRED_FORWARD_SHOW_MAX))
    min_slate = max(0, int(config.PRED_FORWARD_MIN_SLATE))
    conviction_min = float(config.PRED_REPORT_CONVICTION_MIN)
    pool = [
        r
        for r in rows
        if not prediction_ranking.is_report_noise_ticker(r)
    ]
    if not pool:
        return []
    pool = prediction_ranking.drop_exhausted_slate_rows(pool)
    if not pool:
        return []

    def _conviction_ok(r: predict.PredictionRow) -> bool:
        tier = str(getattr(r, "confidence_tier", "") or "")
        if tier in ("high", "mid"):
            return True
        return prediction_ranking.report_breakout_pick_score(r) + 1e-12 >= conviction_min

    eligible = [r for r in pool if _conviction_ok(r)]
    if not eligible:
        return []

    scored = sorted(
        eligible,
        key=lambda r: (
            0
            if str(getattr(r, "confidence_tier", "") or "") == "high"
            else (1 if str(getattr(r, "confidence_tier", "") or "") == "mid" else 2),
            -prediction_ranking.report_breakout_pick_score(r),
            int(getattr(r, "rank_position", None) or 9999),
            str(r.code).zfill(6),
        ),
    )
    max_per_ind = max(1, int(getattr(config, "PRED_SECTOR_DIVERSITY_MAX_PER_INDUSTRY", 3) or 3))
    selected: list[predict.PredictionRow] = []
    ind_counts: dict[str, int] = {}
    overflow: list[predict.PredictionRow] = []
    for r in scored:
        try:
            from src import stocks as stocks_mod

            ind = str(stocks_mod.industry_name_for_code(str(r.code)) or "") or "_unknown"
        except Exception:
            ind = "_unknown"
        n = ind_counts.get(ind, 0)
        if n >= max_per_ind:
            overflow.append(r)
            continue
        ind_counts[ind] = n + 1
        selected.append(r)
        if len(selected) >= cap:
            break
    if len(selected) < cap:
        seen = {str(r.code).zfill(6) for r in selected}
        for r in overflow:
            if len(selected) >= cap:
                break
            c = str(r.code).zfill(6)
            if c in seen:
                continue
            seen.add(c)
            selected.append(r)
    # min_slate>0 일 때만 보충 — 기본 0(패딩 금지)
    if min_slate > 0 and len(selected) < min_slate:
        seen = {str(r.code).zfill(6) for r in selected}
        for r in scored:
            if len(selected) >= min_slate:
                break
            c = str(r.code).zfill(6)
            if c in seen:
                continue
            seen.add(c)
            selected.append(r)

    if not selected:
        return []
    predict.apply_display_return_pct_ranking(
        selected,
        rank_value=lambda r: float(prediction_ranking.report_breakout_pick_score(r)),
        reason_prefix="표시 예측 상승률은 확신·로테이션 급등 점수 순위 기준으로",
    )
    return selected


def _prediction_rows_to_frozen_items(
    rows: list[predict.PredictionRow],
    *,
    n_date: date | None = None,
    saved_on: date | None = None,
) -> list[dict]:
    """``PredictionRow`` 리스트를 고정 캐시 JSON에 넣을 dict 목록으로 직렬화합니다.

    후보가 없으면 빈 목록(빈 slate 마커로 잠그지 않음 — 다음 실행이 재계산).
    """
    if not rows:
        return []
    n_s = n_date.isoformat() if n_date is not None else None
    s_s = (saved_on or datetime.now(trading_calendar.KST).date()).isoformat()
    return [
        {
            "code": str(r.code).zfill(6),
            "name": r.name,
            "score": float(r.score),
            "predicted_return_pct": float(r.predicted_return_pct),
            "matched_keywords": list(r.matched_keywords),
            "reasons": list(r.reasons),
            "ml_prob": (None if r.ml_prob is None else float(r.ml_prob)),
            "ml_precision_score": (
                None
                if getattr(r, "ml_precision_score", None) is None
                else float(r.ml_precision_score)
            ),
            "keyword_hits": int(getattr(r, "keyword_hits", 0) or 0),
            "mention_score": float(getattr(r, "mention_score", 0.0) or 0.0),
            "theme_carryover_score": float(
                getattr(r, "theme_carryover_score", 0.0) or 0.0
            ),
            "industry_theme_overlap": float(
                getattr(r, "industry_theme_overlap", 0.0) or 0.0
            ),
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
            "pred_source": str(getattr(r, "pred_source", "") or ""),
            "ret_lag1": float(getattr(r, "ret_lag1", 0.0) or 0.0),
            "prior_limit_up": bool(getattr(r, "prior_limit_up", False)),
            "n_date": n_s,
            "saved_on": s_s,
        }
        for r in rows
    ]

# --- scheduling (from scheduling.py) ---

_INTRADAY_SNAPSHOT_MIN_COVERAGE = 0.7


def _price_model_inputs(
    returns: Any,
    target_day: date,
    codes: Sequence[str] | None,
    *,
    now_kst: datetime | None = None,
    fetch_snapshot: Any = None,
) -> tuple[Any, bool, str]:
    """
    가격 모델 입력 일봉 표와 (장중 스냅샷 여부, 근거 문구).

    장중에는 당일(N) 일봉을 받지 않으므로 그대로 쓰면 T=N+1 예측이 N-1 종가 기준이 되어
    전날 슬레이트를 반복한다. N 이 오늘이고 개장 후면 네이버 실시간 시세로 N 봉을 채운다.
    """
    import pandas as pd

    from src import stocks
    from src.prediction import price_breakout_model

    n_day = trading_calendar.last_trading_day_before(target_day)
    last = pd.to_datetime(returns["Date"]).max().date()
    now = now_kst or datetime.now(trading_calendar.KST)
    universe = sorted({str(c).zfill(6) for c in (codes or [])}) or sorted(
        set(returns.loc[pd.to_datetime(returns["Date"]).dt.date == last, "Code"].astype(str).str.zfill(6))
    )
    fetch = fetch_snapshot or stocks.try_naver_realtime_ohlcv_by_codes

    # 연휴 갭: 관측일 T 장중에 T 실시간 봉으로 재랭킹(전 세션 슬레이트 반복 방지)
    if (
        config.PRED_FREEZE_REFRESH_LONG_GAP
        and trading_calendar.is_long_session_gap(target_day)
        and now.date() == target_day
        and not trading_calendar.is_before_krx_regular_open_kst(target_day, now_kst=now)
        and not trading_calendar.is_krx_daily_bar_effective_closed(target_day, now_kst=now)
        and universe
    ):
        snap = fetch(universe)
        min_n = max(1, int(len(universe) * _INTRADAY_SNAPSHOT_MIN_COVERAGE))
        if snap is not None and len(snap) >= min_n:
            print(
                f"가격 모델: 연휴 갭 T={target_day} {now:%H:%M} 장중 시세 {len(snap)}종으로 재랭킹합니다.",
                flush=True,
            )
            return (
                price_breakout_model.with_intraday_session(returns, target_day, snap),
                True,
                f"{target_day:%m/%d} {now:%H:%M} 장중 시세 기준(연휴 갭 재랭킹, 거래량은 그 시점 누적)",
            )

    if last >= n_day:
        return returns, False, ""
    stale_note = f"{last:%m/%d} 종가 기준(직전 거래일 {n_day:%m/%d} 시세 없음)"
    if n_day != now.date() or trading_calendar.is_before_krx_regular_open_kst(n_day, now_kst=now):
        print(f"가격 모델: 직전 거래일 {n_day} 일봉 없음 — {last} 종가 기준으로 예측합니다.", flush=True)
        return returns, False, stale_note
    snap = fetch(universe)
    if snap is None or len(snap) < len(universe) * _INTRADAY_SNAPSHOT_MIN_COVERAGE:
        got = 0 if snap is None else len(snap)
        print(
            f"가격 모델: {n_day} 장중 시세 {got}/{len(universe)}종만 조회 — {last} 종가 기준으로 예측합니다.",
            flush=True,
        )
        return returns, False, stale_note
    print(f"가격 모델: {n_day} {now:%H:%M} 장중 시세 {len(snap)}종으로 당일 봉을 채워 예측합니다.", flush=True)
    return (
        price_breakout_model.with_intraday_session(returns, n_day, snap),
        True,
        f"{n_day:%m/%d} {now:%H:%M} 장중 시세 기준(거래량은 그 시점 누적)",
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