"""
랭킹 우선 예측 모드 — 문제 정의·확신 게이트·Hit@K 평가.

기존 ``20~30%`` 순위 매핑 대신 ML 확률(또는 휴리스틱 순위)을 1차 신호로 쓰고,
``pred_high`` / ``pred_mid`` 는 **순위 상위 + 엄격 확률 하한** 으로 각 최대 10건씩만 부여합니다.
"""
from __future__ import annotations

import math
from datetime import date
from typing import TYPE_CHECKING, Any

import pandas as pd

from .. import config, trading_calendar
from ..prediction import accuracy_cache as prediction_accuracy_cache
# --- hybrid / multi-factor (from pred_hybrid) ---

if TYPE_CHECKING:
    from .predict import PredictionRow


# --- multi-factor pillars (merged from pred_factors) ---


def _clamp01(x: float) -> float:
    """값을 [0, 1] 구간으로 클램프."""
    return max(0.0, min(1.0, float(x)))


def market_regime_score(ks11_ret_lag1: float | None) -> float:
    """KOSPI 전일 수익률 기반 리스크온(1)~오프(0)."""
    if ks11_ret_lag1 is None or not math.isfinite(float(ks11_ret_lag1)):
        return 1.0
    r = float(ks11_ret_lag1)
    if r >= 0.01:
        return 1.0
    if r <= -0.03:
        return 0.35
    if r < 0:
        return 0.55 + 15.0 * r
    return 0.85 + 15.0 * r


def relative_strength_from_row(
    row: Any,
    *,
    ks11_ret_lag1: float | None = None,
) -> float:
    """종목 전일 수익 − KOSPI 전일 수익 → 0~1 상대강도."""
    try:
        ret = float(row.get("ret_lag1") if hasattr(row, "get") else getattr(row, "ret_lag1", 0.0) or 0.0)
    except (TypeError, ValueError):
        ret = 0.0
    if ks11_ret_lag1 is None or not math.isfinite(float(ks11_ret_lag1)):
        return _clamp01(ret / 0.12) if ret > 0 else 0.0
    excess = ret - float(ks11_ret_lag1)
    return _clamp01((excess + 0.015) / 0.10)


def compute_pillar_scores(
    row: PredictionRow,
    *,
    ks11_ret_lag1: float | None = None,
) -> dict[str, float]:
    """0~1 기둥 점수: ml·모멘텀·수급·상대강도·섹터·뉴스·시장."""
    ml = float(getattr(row, "ml_prob", 0.0) or 0.0)
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    inv = float(getattr(row, "investor_flow_score", 0.0) or 0.0)
    fr = max(0.0, float(getattr(row, "foreign_net_vol_ratio", 0.0) or 0.0))
    ind_m = float(getattr(row, "industry_momentum", 0.0) or 0.0)
    ind_ov = float(getattr(row, "industry_theme_overlap", 0.0) or 0.0)
    ind_lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    rs = float(getattr(row, "relative_strength_score", 0.0) or 0.0)
    mkt = market_regime_score(ks11_ret_lag1)

    flow = _clamp01(inv + 0.40 * min(fr / 0.06, 1.0))
    sector = _clamp01(0.45 * ind_m / 0.38 + 0.35 * ind_ov + 0.20 * ind_lim)
    news = _clamp01(0.35 * nctx + 0.35 * min(1.0, nh / 4.0) + 0.30 * min(mention, 1.0))

    return {
        "ml": _clamp01(ml / 0.18),
        "momentum": _clamp01(mom / 0.55),
        "flow": flow,
        "relative_strength": rs,
        "sector": sector,
        "news": news,
        "market": mkt,
    }


def multi_factor_rank_score(
    row: PredictionRow,
    *,
    ks11_ret_lag1: float | None = None,
) -> float:
    """하이브리드 최종 랭킹 점수(0~1). 뉴스 비중 최소."""
    p = compute_pillar_scores(row, ks11_ret_lag1=ks11_ret_lag1)
    w_ml = float(config.PRED_FACTOR_W_ML)
    w_mom = float(config.PRED_FACTOR_W_MOMENTUM)
    w_flow = float(config.PRED_FACTOR_W_FLOW)
    w_rs = float(config.PRED_FACTOR_W_RELATIVE_STRENGTH)
    w_sec = float(config.PRED_FACTOR_W_SECTOR)
    w_news = float(config.PRED_FACTOR_W_NEWS)
    w_mkt = float(config.PRED_FACTOR_W_MARKET)

    ml_v = float(getattr(row, "ml_prob", 0.0) or 0.0)
    if ml_v <= 0:
        s = (
            0.38 * p["momentum"]
            + 0.28 * p["flow"]
            + 0.18 * p["relative_strength"]
            + 0.12 * p["sector"]
            + 0.04 * p["news"]
        )
    else:
        s = (
            w_ml * p["ml"]
            + w_mom * p["momentum"]
            + w_flow * p["flow"]
            + w_rs * p["relative_strength"]
            + w_sec * p["sector"]
            + w_news * p["news"]
            + w_mkt * p["market"]
        )
    if ks11_ret_lag1 is not None and float(ks11_ret_lag1) < float(config.PRED_REGIME_KS11_SOFT_MIN):
        s *= float(config.PRED_REGIME_OUTPUT_SCALE)
    if p["sector"] + 1e-12 >= 0.40 and p["momentum"] + 1e-12 >= 0.30:
        s = min(1.0, s * 1.07)
    ind_lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    if ind_lim + 1e-12 >= 0.22:
        s = min(1.0, s + 0.05 * ind_lim)
    prior_hot = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
    if prior_hot + 1e-12 >= 0.28:
        s = min(1.0, s + 0.06 * prior_hot + 0.04 * p["sector"])
    if prior_hot + 1e-12 >= 0.18:
        s = min(1.0, s + 0.04 * prior_hot)
    try:
        from .. import stocks as stocks_mod

        ind_name = stocks_mod.industry_name_for_code(str(row.code)) or ""
    except Exception:
        ind_name = ""
    if (
        "반도체" in ind_name
        and ks11_ret_lag1 is not None
        and float(ks11_ret_lag1) < float(config.PRED_REGIME_KS11_SOFT_MIN)
        and p["sector"] + 1e-12 < 0.36
        and p["momentum"] + 1e-12 < 0.34
    ):
        s *= 0.72
    return _clamp01(s)


def count_strong_pillars(
    pillars: dict[str, float],
    *,
    threshold: float = 0.48,
    exclude: frozenset[str] = frozenset(),
) -> int:
    """``threshold`` 이상인 기둥 수(``exclude`` 키는 제외)."""
    return sum(
        1
        for k, v in pillars.items()
        if k not in exclude and v + 1e-12 >= threshold
    )


def count_non_news_strong_pillars(pillars: dict[str, float], *, threshold: float = 0.45) -> int:
    """뉴스 기둥을 제외한 강한 신호 기둥 수 — 고확신·정밀 게이트의 핵심 지표."""
    return count_strong_pillars(pillars, threshold=threshold, exclude=frozenset({"news"}))


def passes_dual_factor_gate(
    row: PredictionRow,
    *,
    hybrid: float,
    top_hybrid: float,
    ks11_ret_lag1: float | None = None,
) -> bool:
    """고확신 1차 게이트: 상대 순위 + 뉴스 외 신호 + 복합 합의."""
    rel_hi = float(config.PRED_ML_HIGH_RELATIVE_PROB)
    if hybrid + 1e-12 < top_hybrid * rel_hi:
        return False

    pillars = compute_pillar_scores(row, ks11_ret_lag1=ks11_ret_lag1)
    non_news = count_non_news_strong_pillars(pillars, threshold=0.44)
    if non_news < 1:
        return False

    ml = float(getattr(row, "ml_prob", 0.0) or 0.0)
    ml_floor = max(0.05, float(config.PRED_ML_MIN_OUTPUT_PROB))

    if pillars["news"] + 1e-12 >= 0.50 and non_news == 0:
        return False

    strong_total = count_strong_pillars(pillars, threshold=0.46)
    if strong_total < int(config.PRED_FACTOR_MIN_STRONG_PILLARS):
        return False

    if ml + 1e-12 >= ml_floor:
        return True
    if pillars["momentum"] + 1e-12 >= 0.50 and pillars["flow"] + 1e-12 >= 0.42:
        return True
    if pillars["momentum"] + 1e-12 >= 0.55 and pillars["relative_strength"] + 1e-12 >= 0.45:
        return True
    if pillars["sector"] + 1e-12 >= 0.52 and non_news >= 2:
        return True
    return False


def conviction_score(row: PredictionRow, *, ks11_ret_lag1: float | None = None) -> float:
    """정밀 게이트용 확신 점수."""
    pillars = compute_pillar_scores(row, ks11_ret_lag1=ks11_ret_lag1)
    core = {k: v for k, v in pillars.items() if k != "news"}
    vals = sorted(core.values(), reverse=True)
    if len(vals) < 3:
        return 0.0
    w = (0.32, 0.26, 0.22, 0.12, 0.08)
    s = sum(v * w[i] for i, v in enumerate(vals[: len(w)]))
    s += 0.06 * pillars["news"]
    non_news = count_non_news_strong_pillars(pillars)
    if non_news < int(config.PRED_PRECISION_MIN_PILLARS):
        s *= 0.65
    return _clamp01(s)


def format_factor_summary(
    row: PredictionRow,
    *,
    ks11_ret_lag1: float | None = None,
) -> str:
    """리포트용 한 줄 요인 요약."""
    p = compute_pillar_scores(row, ks11_ret_lag1=ks11_ret_lag1)
    tags: list[str] = []
    for key, label in (
        ("ml", "ML"),
        ("momentum", "모멘텀"),
        ("flow", "수급"),
        ("relative_strength", "상대강도"),
        ("sector", "섹터"),
        ("news", "뉴스"),
    ):
        v = p[key]
        if v + 1e-12 >= 0.45:
            tags.append(f"{label}{v * 100:.0f}%")
    if not tags:
        tags.append("복합신호약")
    mkt = p["market"]
    return " · ".join(tags[:5]) + f" (시장{mkt * 100:.0f}%)"


_MOM_COLS = ("vol_surge_ratio", "ret_lag1", "ret_roll_std5", "close_ma20_ratio")


def momentum_raw_from_row(row: pd.Series | dict) -> float:
    """전일 시세 요약 → 0~1 모멘텀 점수."""
    try:
        vs = float(row.get("vol_surge_ratio") or 0.0)
        rl = max(0.0, float(row.get("ret_lag1") or 0.0))
        rs = float(row.get("ret_roll_std5") or 0.0)
        ma = float(row.get("close_ma20_ratio") or 1.0)
    except (TypeError, ValueError):
        return 0.0
    if not all(math.isfinite(x) for x in (vs, rl, rs, ma)):
        return 0.0
    s = (
        0.42 * min(vs / 2.8, 1.0)
        + 0.33 * min(rl / 0.12, 1.0)
        + 0.15 * min(rs / 0.07, 1.0)
        + 0.10 * min(max(ma - 0.92, 0.0) / 0.18, 1.0)
    )
    return max(0.0, min(1.0, s))


def momentum_candidate_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    top_k: int = 220,
) -> list[str]:
    """당일 관측 시점 시세로 모멘텀 상위 종목(뉴스 없어도 후보)."""
    allowed = {str(c).zfill(6) for c in listing_codes}
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    if sl.empty:
        return []
    scored: list[tuple[float, str]] = []
    surge_extra: list[tuple[float, str]] = []
    for _, r in sl.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        mom = momentum_raw_from_row(r)
        vs = float(r.get("vol_surge_ratio") or 0.0)
        rl = max(0.0, float(r.get("ret_lag1") or 0.0))
        if mom >= 0.18 or vs >= 1.22 or rl >= 0.038:
            scored.append((mom, code))
        elif vs + 1e-12 >= 0.52 or (mom + 1e-12 >= 0.14 and vs + 1e-12 >= 0.38):
            surge_extra.append((vs + 0.35 * mom, code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    surge_extra.sort(key=lambda x: (-x[0], x[1]))
    cap = max(40, int(top_k))
    out: list[str] = []
    seen: set[str] = set()
    for _, code in scored[:cap]:
        if code not in seen:
            seen.add(code)
            out.append(code)
    for _, code in surge_extra[: max(30, cap // 4)]:
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


def relative_strength_candidate_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    top_k: int = 120,
) -> list[str]:
    """KOSPI 대비 전일 상대강도 상위 종목(뉴스 없이도 후보)."""
    allowed = {str(c).zfill(6) for c in listing_codes}
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    if sl.empty:
        return []
    try:
        from .market_features import ks11_market_feats

        ks11_ret, _ = ks11_market_feats(target_day)
    except Exception:
        ks11_ret = None
    scored: list[tuple[float, str]] = []
    for _, r in sl.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        rs = relative_strength_from_row(r, ks11_ret_lag1=ks11_ret)
        rl = max(0.0, float(r.get("ret_lag1") or 0.0))
        if rs + 1e-12 >= 0.38 or rl + 1e-12 >= 0.05:
            scored.append((rs + 0.12 * min(rl / 0.10, 1.0), code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [c for _, c in scored[: max(15, int(top_k))]]


def hybrid_rank_score(row: PredictionRow) -> float:
    """ML·모멘텀·수급·상대강도·섹터·시장 다요인 점수(뉴스 보조)."""
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return multi_factor_rank_score(row, ks11_ret_lag1=ks11)


def recall_presort_score(row: PredictionRow) -> float:
    """finalize 풀 선정용 — 하이브리드·모멘텀·섹터열기 중 강한 신호를 반영."""
    h = hybrid_rank_score(row)
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    sec = max(
        float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0),
        float(getattr(row, "industry_momentum", 0.0) or 0.0),
        float(getattr(row, "prior_industry_hot", 0.0) or 0.0),
    )
    blend = 0.52 * h + 0.28 * mom + 0.20 * sec
    return max(h, blend)


def _dual_signal_ok(row: PredictionRow, *, hybrid: float, top_hybrid: float) -> bool:
    """고확신 1차 게이트: 상대 순위 + 뉴스 외 다요인 합의."""
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return passes_dual_factor_gate(
        row, hybrid=hybrid, top_hybrid=top_hybrid, ks11_ret_lag1=ks11
    )


def assign_hybrid_confidence_tiers(
    pool: list[PredictionRow],
    *,
    regime_scale: float = 1.0,
) -> None:
    """하이브리드 순위 + 이중 신호(뉴스∩모멘텀)로 고·중 확신 부여."""
    rs = max(0.25, float(regime_scale))
    max_high = max(1, int(round(int(config.PRED_OUTPUT_MAX) * rs)))
    max_mid = max(1, int(round(int(config.PRED_MID_OUTPUT_MAX) * rs)))
    high_floor = max(0.08, float(config.PRED_ML_HIGH_CONFIDENCE_PROB)) / rs
    mid_floor = max(0.045, float(config.PRED_ML_MID_CONFIDENCE_PROB)) / rs

    for row in pool:
        row.confidence_tier = "none"

    if not pool:
        return

    ranked = sorted(pool, key=hybrid_rank_score, reverse=True)
    top_hybrid = hybrid_rank_score(ranked[0])
    high_n = mid_n = 0

    for pos, row in enumerate(ranked, start=1):
        h = hybrid_rank_score(row)
        ml = float(row.ml_prob or 0.0)
        # 고확신: 상위 8위 이내 + 하이브리드·이중신호(비뉴스 기둥) 통과
        if high_n < max_high and pos <= 12 and h + 1e-12 >= high_floor and _dual_signal_ok(
            row, hybrid=h, top_hybrid=top_hybrid
        ):
            row.confidence_tier = "high"
            high_n += 1
            continue
        mid_ok = h + 1e-12 >= mid_floor
        nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
        nh = int(getattr(row, "keyword_hits", 0) or 0)
        if not mid_ok and nctx + 1e-12 >= 0.45 and nh >= 1:
            mid_ok = True
        if (
            mid_n < max_mid
            and mid_ok
            and (ml + 1e-12 >= mid_floor * 0.70 or float(row.momentum_score or 0) >= 0.28 or nctx + 1e-12 >= 0.40)
        ):
            row.confidence_tier = "mid"
            mid_n += 1

    if high_n == 0 and mid_n == 0 and ranked:
        for row in ranked[: min(max_mid, 5)]:
            row.confidence_tier = "mid"
            mid_n += 1

    if high_n < max_high:
        sector_ranked = sorted(
            ranked,
            key=lambda r: (
                float(getattr(r, "industry_limit_up_heat", 0.0) or 0.0)
                + float(getattr(r, "industry_momentum", 0.0) or 0.0),
                hybrid_rank_score(r),
            ),
            reverse=True,
        )
        for row in sector_ranked:
            if high_n >= max_high:
                break
            if row.confidence_tier == "high":
                continue
            lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
            sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
            if lim + 1e-12 < 0.12 and sec + 1e-12 < 0.26:
                continue
            pillars = compute_pillar_scores(
                row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None)
            )
            if count_non_news_strong_pillars(pillars, threshold=0.36) >= 1:
                row.confidence_tier = "high"
                high_n += 1

    for pos, row in enumerate(ranked, start=1):
        row.rank_position = pos
        row.rank_score = hybrid_rank_score(row)

if TYPE_CHECKING:
    from .predict import PredictionRow


def rank_score_for_row(row: PredictionRow) -> float:
    """행의 하이브리드 랭킹 점수."""
    return hybrid_rank_score(row)


def _industry_key(code: str) -> str:
    """업종 코드(없으면 종목 단위 키)."""
    try:
        from .. import stocks as stocks_mod

        ic = stocks_mod.industry_code_for_stock(code)
        if ic:
            return str(ic)
    except Exception:
        pass
    return f"__{str(code).zfill(6)}"


def _hot_industry_heat_map(
    returns_ml: pd.DataFrame,
    session_day: date,
    *,
    min_ret: float = 0.15,
) -> dict[str, float]:
    """거래일 ``session_day`` 15%+ 급등 업종별 열기(0~1)."""
    from .. import stocks as stocks_mod

    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(session_day)]
    if sl.empty:
        return {}
    heat: dict[str, float] = {}
    for _, r in sl.iterrows():
        try:
            rp = float(r.get("return_pct") or 0.0)
        except (TypeError, ValueError):
            continue
        if rp + 1e-12 < min_ret:
            continue
        ic = stocks_mod.industry_code_for_stock(str(r["Code"]))
        if not ic:
            continue
        k = str(ic)
        heat[k] = max(heat.get(k, 0.0), min(1.0, rp / 0.35))
    return heat


def _rerank_carryover_industry_leaders(
    pool: list[PredictionRow],
    *,
    target_day: date,
    returns_ml: pd.DataFrame | None,
) -> list[PredictionRow]:
    """전일·전전일 급등 업종 리더를 상위로 끌어올려 테마 이어짐일 Hit@K 를 높입니다."""
    if not pool or returns_ml is None or returns_ml.empty:
        return pool
    try:
        prev = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        return pool
    heat_map = _hot_industry_heat_map(returns_ml, prev, min_ret=0.12)
    if not heat_map:
        return pool
    try:
        prev2 = trading_calendar.last_trading_day_before(prev)
        heat2 = _hot_industry_heat_map(returns_ml, prev2, min_ret=0.12)
        for k, v in heat2.items():
            heat_map[k] = max(heat_map.get(k, 0.0), 0.55 * v)
    except ValueError:
        pass

    def _boosted(row: PredictionRow) -> float:
        base = rank_score_for_row(row)
        h = heat_map.get(_industry_key(str(row.code)), 0.0)
        if h <= 0:
            return base
        lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
        sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
        mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
        boost = 0.08 + 0.18 * h + 0.06 * max(lim, sec) + 0.05 * mom
        prior_hot = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
        if prior_hot + 1e-12 >= 0.15:
            boost += 0.05 * prior_hot
        return min(1.0, base + boost)

    return sorted(pool, key=_boosted, reverse=True)


def _rerank_sector_diversity(pool: list[PredictionRow]) -> list[PredictionRow]:
    """상위 구간 동일 업종 쏠림을 줄여 테마 전환일 리콜을 높입니다."""
    top_k = int(config.PRED_SECTOR_DIVERSITY_TOP_K)
    max_per = int(config.PRED_SECTOR_DIVERSITY_MAX_PER_INDUSTRY)
    if len(pool) <= 1 or top_k <= 0 or max_per <= 0:
        return pool
    ranked = sorted(pool, key=rank_score_for_row, reverse=True)
    head: list[PredictionRow] = []
    counts: dict[str, int] = {}
    for row in ranked:
        ik = _industry_key(str(row.code))
        if len(head) < top_k and counts.get(ik, 0) < max_per:
            head.append(row)
            counts[ik] = counts.get(ik, 0) + 1
    if len(head) < top_k:
        seen = {id(r) for r in head}
        for row in ranked:
            if len(head) >= top_k:
                break
            if id(row) in seen:
                continue
            head.append(row)
            seen.add(id(row))
    seen = {id(r) for r in head}
    return head + [r for r in ranked if id(r) not in seen]


def _inject_hot_sector_leaders(pool: list[PredictionRow]) -> list[PredictionRow]:
    """최근 급등 업종 리더를 상위 슬롯에 끼워 넣어 Hit@K 리콜을 올립니다."""
    slots = int(config.PRED_INDUSTRY_LEADER_SLOTS)
    top_k = min(int(config.PRED_SECTOR_DIVERSITY_TOP_K), len(pool))
    if slots <= 0 or top_k <= 0 or not pool:
        return pool
    ranked = sorted(pool, key=rank_score_for_row, reverse=True)
    head = list(ranked[:top_k])
    head_ids = {id(r) for r in head}
    leaders = sorted(
        pool,
        key=lambda r: (
            float(getattr(r, "industry_limit_up_heat", 0.0) or 0.0)
            + 0.75 * float(getattr(r, "industry_momentum", 0.0) or 0.0)
            + 0.45 * float(getattr(r, "industry_theme_overlap", 0.0) or 0.0),
            rank_score_for_row(r),
        ),
        reverse=True,
    )
    injected = 0
    for row in leaders:
        if injected >= slots:
            break
        if id(row) in head_ids:
            continue
        lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
        sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
        if lim + 1e-12 < 0.10 and sec + 1e-12 < 0.20:
            continue
        head.append(row)
        head_ids.add(id(row))
        injected += 1
    head = sorted(head, key=rank_score_for_row, reverse=True)[:top_k]
    seen = {id(r) for r in head}
    return head + [r for r in ranked if id(r) not in seen]


def _heuristic_pseudo_prob(rank_position: int, pool_size: int) -> float:
    """ML 없을 때 순위만으로 0~1 사이 의사 확률(평가·표시용)."""
    if pool_size <= 0:
        return 0.0
    t = 1.0 - (float(rank_position) - 1.0) / float(max(1, pool_size))
    return max(0.005, min(0.12, 0.012 + 0.10 * t))


def effective_probability(row: PredictionRow, *, rank_position: int, pool_size: int) -> float:
    """종목의 급등 추정 확률(ML ``ml_prob`` 또는 휴리스틱 의사 확률)."""
    p = getattr(row, "ml_prob", None)
    if p is not None and math.isfinite(float(p)):
        return float(p)
    return _heuristic_pseudo_prob(rank_position, pool_size)


def _tier_thresholds(*, regime_scale: float) -> tuple[float, float, float]:
    """레짐 축소 배율을 반영한 (고확신, 중확신, 최소 출력) ML 확률 하한."""
    rs = max(0.25, float(regime_scale))
    high_floor = 0.12
    mid_floor = 0.06
    high = max(high_floor, float(config.PRED_ML_HIGH_CONFIDENCE_PROB)) / rs
    mid = max(mid_floor, float(config.PRED_ML_MID_CONFIDENCE_PROB)) / rs
    min_out = float(config.PRED_ML_MIN_OUTPUT_PROB) / rs
    return high, mid, min_out


def _ml_high_signal_ok(row: PredictionRow, *, rank_position: int) -> bool:
    """고확신 슬롯: ML 상위권 + 종목 관련 키워드·언급."""
    if rank_position > 8:
        return False
    n_hit = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    need = int(config.PRED_ML_HIGH_MIN_KEYWORD_HITS)
    if n_hit >= need:
        return True
    if mention + 1e-12 >= float(config.PRED_MENTION_GATE_MIN):
        return True
    return False


def _pool_top_ml_prob(pool: list[PredictionRow]) -> float:
    """풀 내 유한한 ML 확률의 최댓값(상대 확신 게이트용)."""
    top = 0.0
    for row in pool:
        mp = getattr(row, "ml_prob", None)
        if mp is not None and math.isfinite(float(mp)):
            top = max(top, float(mp))
    return top


def assign_confidence_tiers_by_rank(
    pool: list[PredictionRow],
    *,
    regime_scale: float = 1.0,
) -> None:
    """
    ML 순위대로 고확신·중확신 슬롯을 각 최대 ``PRED_OUTPUT_MAX`` / ``PRED_MID_OUTPUT_MAX`` 까지 채웁니다.

    임계 확률·최소 신호를 통과한 종목만 슬롯에 들어가며, 부족하면 10건 미만으로 둡니다.
    """
    pool_size = len(pool)
    high_thr, mid_thr, min_out = _tier_thresholds(regime_scale=regime_scale)
    max_high = max(1, int(round(int(config.PRED_OUTPUT_MAX) * regime_scale)))
    max_mid = max(1, int(round(int(config.PRED_MID_OUTPUT_MAX) * regime_scale)))
    min_score = float(config.PRED_HEURISTIC_MIN_SCORE)

    for row in pool:
        row.confidence_tier = "none"

    top_prob = _pool_top_ml_prob(pool)
    rel_hi = float(config.PRED_ML_HIGH_RELATIVE_PROB)
    high_n = 0
    mid_n = 0
    for pos, row in enumerate(pool, start=1):
        prob = effective_probability(row, rank_position=pos, pool_size=pool_size)
        score = float(getattr(row, "score", 0.0) or 0.0)
        raw_ml = getattr(row, "ml_prob", None)
        has_ml = raw_ml is not None and math.isfinite(float(raw_ml))

        if prob + 1e-12 < min_out and score + 1e-12 < min_score:
            continue

        if has_ml:
            rel_ok = top_prob <= 0 or prob + 1e-12 >= top_prob * rel_hi
            if (
                high_n < max_high
                and prob + 1e-12 >= high_thr
                and rel_ok
                and _ml_high_signal_ok(row, rank_position=pos)
            ):
                row.confidence_tier = "high"
                high_n += 1
                continue
            if mid_n < max_mid and prob + 1e-12 >= mid_thr:
                row.confidence_tier = "mid"
                mid_n += 1
                continue

        # ML 없음: 휴리스틱만 — 상위 소수·고점수만 중확신 이하로 제한
        hi_rank = int(config.PRED_HEURISTIC_HIGH_RANK_MAX)
        if hi_rank > 0 and high_n < max_high and pos <= hi_rank and score + 1e-12 >= min_score:
            row.confidence_tier = "high"
            high_n += 1


def confidence_tier(
    row: PredictionRow,
    *,
    rank_position: int,
    pool_size: int,
    regime_scale: float = 1.0,
) -> str:
    """레거시 호출용 — ``assign_confidence_tiers_by_rank`` 가 최종 tier 를 덮어씁니다."""
    prob = effective_probability(row, rank_position=rank_position, pool_size=pool_size)
    high_thr, mid_thr, min_out = _tier_thresholds(regime_scale=regime_scale)
    if prob + 1e-12 < min_out:
        return "none"
    if prob + 1e-12 >= high_thr:
        return "high"
    if prob + 1e-12 >= mid_thr:
        return "mid"
    return "none"


def is_high_confidence_prediction(row: PredictionRow) -> bool:
    """``pred_high`` 대체: 확신 ``high`` 또는 (레거시) 표시 % ≥ 급등 임계."""
    if config.PRED_RANKING_MODE:
        tier = str(getattr(row, "confidence_tier", "") or "")
        return tier == "high"
    pct = float(getattr(row, "predicted_return_pct", 0.0) or 0.0)
    return pct >= float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def is_mid_confidence_prediction(row: PredictionRow) -> bool:
    """``pred_mid`` 판정: 랭킹 모드면 ``confidence_tier==mid``, 레거시면 표시 % 10~20% 구간."""
    if config.PRED_RANKING_MODE:
        return str(getattr(row, "confidence_tier", "") or "") == "mid"
    pct = float(getattr(row, "predicted_return_pct", 0.0) or 0.0)
    return 10.0 - 1e-9 <= pct < float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def row_pred_high_from_dict(row: dict) -> bool:
    """``rows_compare`` dict 에서 ``pred_high`` 판정."""
    if config.PRED_RANKING_MODE:
        tier = str(row.get("confidence_tier") or "")
        if tier == "high":
            return True
        if tier:
            return False
    pr = row.get("pred_ret")
    if pr is None:
        return False
    return float(pr) >= float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def row_pred_mid_from_dict(row: dict) -> bool:
    """``rows_compare`` dict 에서 ``pred_mid`` 판정."""
    if config.PRED_RANKING_MODE:
        return str(row.get("confidence_tier") or "") == "mid"
    pr = row.get("pred_ret")
    if pr is None:
        return False
    return 10.0 - 1e-9 <= float(pr) < float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def probability_to_display_pct(prob: float) -> float:
    """(레거시) 급등 확률(0~1)을 [20%, PRED_RETURN_MAX] 표시 %%로 변환. 랭킹 모드 기본 경로에서는 쓰지 않음."""
    p = max(0.0, min(1.0, float(prob)))
    lo = float(config.BIG_MOVE_THRESHOLD) * 100.0
    hi = float(config.PRED_RETURN_MAX) * 100.0
    scaled = lo + (hi - lo) * min(1.0, p / max(0.02, float(config.PRED_ML_HIGH_CONFIDENCE_PROB) * 2.5))
    return float(max(0.0, min(hi, scaled)))


def regime_output_scale(ks11_ret_lag1: float | None) -> float:
    """KOSPI 전일 수익률 기반 출력 축소 배율(1.0=정상, 0.5=리스크오프)."""
    if not config.PRED_REGIME_GATE_ENABLED or ks11_ret_lag1 is None:
        return 1.0
    if not math.isfinite(float(ks11_ret_lag1)):
        return 1.0
    soft = float(config.PRED_REGIME_KS11_SOFT_MIN)
    if float(ks11_ret_lag1) >= soft:
        return 1.0
    return float(config.PRED_REGIME_OUTPUT_SCALE)


def _forward_conviction_score(row: PredictionRow) -> float:
    """장 전 관측일: 뉴스 단독보다 시세·섹터·ML 합의를 우선하는 확신 점수."""
    ks11 = getattr(row, "ks11_ret_lag1", None)
    pillars = compute_pillar_scores(row, ks11_ret_lag1=ks11)
    return (
        0.32 * pillars["momentum"]
        + 0.28 * pillars["sector"]
        + 0.22 * pillars["ml"]
        + 0.10 * pillars["flow"]
        + 0.08 * pillars["relative_strength"]
        + 0.05 * min(pillars["news"], 0.65)
    )


def assign_forward_confidence_tiers(
    pool: list[PredictionRow],
    *,
    regime_scale: float = 1.0,
) -> None:
    """
    예측 전용(장 시작 전) 관측일: 과거 종목 적중 이력 게이트 없이
    시세·섹터·ML 합의 상위만 고·중 확신 슬롯에 배정합니다.
    """
    rs = max(0.45, float(regime_scale))
    max_high = max(2, int(round(int(config.PRED_OUTPUT_MAX) * rs)))
    max_mid = max(2, int(round(int(config.PRED_MID_OUTPUT_MAX) * rs)))

    for row in pool:
        row.confidence_tier = "none"
    if not pool:
        return

    ranked = sorted(pool, key=_forward_conviction_score, reverse=True)
    top_hybrid = hybrid_rank_score(ranked[0])
    high_n = 0
    for pos, row in enumerate(ranked, start=1):
        if high_n >= max_high:
            break
        pillars = compute_pillar_scores(row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None))
        non_news = count_non_news_strong_pillars(pillars, threshold=0.40)
        mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
        sec = max(
            float(getattr(row, "industry_momentum", 0.0) or 0.0),
            float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0),
        )
        h = hybrid_rank_score(row)
        if non_news < 2:
            continue
        if mom + 1e-12 < 0.30 and sec + 1e-12 < 0.28:
            continue
        if pillars["news"] + 1e-12 >= 0.55 and non_news < 2:
            continue
        if not passes_dual_factor_gate(
            row,
            hybrid=h,
            top_hybrid=top_hybrid,
            ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None),
        ):
            if non_news < 3 and mom + 1e-12 < 0.38:
                continue
        row.confidence_tier = "high"
        high_n += 1

    mid_n = 0
    for row in ranked:
        if row.confidence_tier == "high":
            continue
        if mid_n >= max_mid:
            break
        if _forward_conviction_score(row) + 1e-12 >= 0.20:
            row.confidence_tier = "mid"
            mid_n += 1

    if high_n == 0 and ranked:
        for row in ranked[:max_high]:
            pillars = compute_pillar_scores(
                row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None)
            )
            if count_non_news_strong_pillars(pillars, threshold=0.35) >= 1:
                row.confidence_tier = "high"
                high_n += 1

    for pos, row in enumerate(ranked, start=1):
        row.rank_position = pos
        row.rank_score = hybrid_rank_score(row)


def finalize_ranked_predictions(
    rows: list[PredictionRow],
    *,
    target_day: date,
    ks11_ret_lag1: float | None = None,
    forward_observation: bool = False,
    returns_ml: pd.DataFrame | None = None,
) -> list[PredictionRow]:
    """
    순위·확신 구간·표시 % 를 일괄 확정합니다.

    랭킹 모드: 순위 상위부터 고확신 최대 10·중확신 최대 10(엄격 ML 확률 하한 통과분만).
    """
    if not rows:
        return rows

    from . import predict

    pool = _rerank_sector_diversity(rows)
    pool = _inject_hot_sector_leaders(pool)
    if returns_ml is not None and not forward_observation:
        pool = _rerank_carryover_industry_leaders(
            pool, target_day=target_day, returns_ml=returns_ml
        )
    pool = sorted(pool, key=rank_score_for_row, reverse=True)
    pool_size = len(pool)
    r_scale = regime_output_scale(ks11_ret_lag1)

    if config.PRED_USE_DISPLAY_RANK_MAPPING:
        predict.apply_display_return_pct_ranking(
            pool,
            rank_value=rank_score_for_row,
            reason_prefix="표시 예측 상승률은 순위 지표(ML 확률·휴리스틱 점수) 기준으로",
        )

    for pos, row in enumerate(pool, start=1):
        row.rank_position = pos
        row.rank_score = rank_score_for_row(row)
        if getattr(row, "ks11_ret_lag1", None) is None and ks11_ret_lag1 is not None:
            row.ks11_ret_lag1 = ks11_ret_lag1
        prob = effective_probability(row, rank_position=pos, pool_size=pool_size)
        if getattr(row, "ml_prob", None) is None:
            row.ml_prob = prob if config.PRED_RANKING_MODE else None

    if config.PRED_RANKING_MODE:
        if forward_observation:
            assign_forward_confidence_tiers(pool, regime_scale=r_scale)
        else:
            assign_hybrid_confidence_tiers(pool, regime_scale=r_scale)
            # 1차 하이브리드 tier → 정밀 게이트로 high 재필터(뉴스 단독·저적중 종목 제거)
            refine_confidence_tiers(
                pool,
                feedback_ctx=prediction_accuracy_cache.build_feedback_context(),
                regime_scale=r_scale,
            )
        pool = sorted(pool, key=rank_score_for_row, reverse=True)
    else:
        for pos, row in enumerate(pool, start=1):
            row.confidence_tier = confidence_tier(
                row, rank_position=pos, pool_size=pool_size, regime_scale=r_scale
            )

    if config.PRED_RANKING_MODE and not config.PRED_USE_DISPLAY_RANK_MAPPING:
        for pos, row in enumerate(pool, start=1):
            prob = effective_probability(row, rank_position=pos, pool_size=pool_size)
            tier = str(row.confidence_tier or "none")
            rank_note = (
                f"다요인 랭킹: 익일 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) 추정 "
                f"점수 {row.rank_score * 100:.1f}% · 순위 {pos}/{pool_size} · 확신 {tier} "
                f"({format_factor_summary(row, ks11_ret_lag1=getattr(row, 'ks11_ret_lag1', None))})"
            )
            row.reasons = [rank_note] + [
                x for x in row.reasons if not x.startswith("랭킹 모드:")
            ]

    return pool


# --- precision gate (merged from pred_precision_gate) ---


def _pillar_scores(row: PredictionRow) -> dict[str, float]:
    """``compute_pillar_scores`` 래퍼(행에 KS11 lag1 이 있으면 전달)."""
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return compute_pillar_scores(row, ks11_ret_lag1=ks11)


def precision_conviction_score(row: PredictionRow) -> float:
    """다요인 기둥 합의 기반 확신 점수(0~1). ``conviction_score`` 위임."""
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return conviction_score(row, ks11_ret_lag1=ks11)


def _bucket_ok(row: PredictionRow, feedback_ctx: dict[str, object] | None) -> bool:
    """과거 예측–실적 버킷 평균 정합도가 하한 이상이면 True(표본 부족 시 통과)."""
    if not feedback_ctx:
        return True
    stats = feedback_ctx.get("signal_bucket_stats")
    if not isinstance(stats, dict):
        return True
    key = prediction_accuracy_cache._signal_bucket_key(
        pred_ret=float(getattr(row, "predicted_return_pct", 0.0) or 0.0),
        keyword_hits=int(getattr(row, "keyword_hits", 0) or 0),
        mention_score=float(getattr(row, "mention_score", 0.0) or 0.0),
    )
    b = stats.get(key)
    if not isinstance(b, dict):
        return True
    n = int(b.get("count", 0) or 0)
    if n < int(config.PRED_PRECISION_BUCKET_MIN_SAMPLES):
        return True
    mean_r = float(b.get("mean_ratio", 0.0) or 0.0)
    return mean_r + 1e-12 >= float(config.PRED_PRECISION_BUCKET_MIN_RATIO)


def _code_history_ok(code: str) -> bool:
    """종목별 과거 고확신 예측의 급등 적중률이 ``PRED_PRECISION_CODE_MIN_HIT_RATE`` 이상이면 True."""
    payload = prediction_accuracy_cache._load_payload()
    hp = payload.get("high_pred_by_code")
    tap = payload.get("t_code_actual_pct")
    if not isinstance(hp, dict) or not isinstance(tap, dict):
        return True
    hist = hp.get(str(code).zfill(6))
    if not isinstance(hist, list) or len(hist) < int(config.PRED_PRECISION_CODE_MIN_TRIES):
        return True
    hits = 0
    n = 0
    thr = float(config.BIG_MOVE_THRESHOLD) * 100.0
    for ent in hist[-12:]:
        if not isinstance(ent, dict):
            continue
        t = str(ent.get("trading_day") or ent.get("day") or "")
        if not t:
            continue
        key = f"{t}:{str(code).zfill(6)}"
        act = tap.get(key)
        if act is None:
            continue
        n += 1
        if float(act) + 1e-9 >= thr:
            hits += 1
    if n < int(config.PRED_PRECISION_CODE_MIN_TRIES):
        return True
    rate = hits / n
    return rate + 1e-12 >= float(config.PRED_PRECISION_CODE_MIN_HIT_RATE)


def passes_precision_gate(
    row: PredictionRow,
    *,
    top_ml: float,
    rank_position: int,
    feedback_ctx: dict[str, object] | None = None,
) -> bool:
    """
    고확신 슬롯 승격 조건: 순위·확신·비뉴스 기둥·ML 절대/상대 하한·피드백 버킷·종목 이력.

    ``top_ml`` 은 당일 풀 최고 ML 확률(상대 하한 ``PRED_ML_HIGH_RELATIVE_PROB`` 용).
    """
    if rank_position > int(config.PRED_PRECISION_MAX_RANK):
        return False
    conv = precision_conviction_score(row)
    if conv + 1e-12 < float(config.PRED_PRECISION_MIN_CONVICTION):
        return False
    pillars = _pillar_scores(row)
    non_news = count_non_news_strong_pillars(pillars, threshold=0.46)
    if non_news < int(config.PRED_PRECISION_MIN_PILLARS):
        return False
    ml = float(getattr(row, "ml_prob", 0.0) or 0.0)
    floor = max(
        float(config.PRED_ML_HIGH_CONFIDENCE_PROB),
        float(config.PRED_PRECISION_ML_FLOOR),
    )
    if ml + 1e-12 < floor:
        return False
    if top_ml > 1e-9 and ml + 1e-12 < top_ml * float(config.PRED_ML_HIGH_RELATIVE_PROB):
        return False
    if pillars["news"] + 1e-12 >= 0.52 and non_news < 2:
        return False
    non_news_signals = 0
    if pillars["momentum"] + 1e-12 >= 0.40:
        non_news_signals += 1
    if pillars["flow"] + 1e-12 >= 0.40:
        non_news_signals += 1
    if pillars["relative_strength"] + 1e-12 >= 0.38:
        non_news_signals += 1
    if pillars["sector"] + 1e-12 >= 0.42:
        non_news_signals += 1
    if pillars["ml"] + 1e-12 >= 0.45:
        non_news_signals += 1
    if non_news_signals < 2:
        return False
    if not _bucket_ok(row, feedback_ctx):
        return False
    if not _code_history_ok(str(row.code).zfill(6)):
        return False
    return True


def refine_confidence_tiers(
    pool: list[PredictionRow],
    *,
    feedback_ctx: dict[str, object] | None = None,
    regime_scale: float = 1.0,
) -> None:
    """
    하이브리드 tier 부여 후 ``confidence_tier=high`` 를 정밀 게이트로 재필터링합니다.

    통과 종목만 ``high`` 유지, 기존 ``high`` 미통과는 ``mid`` 로 강등. ``PRED_PRECISION_GATE_ENABLED=0`` 이면 no-op.
    """
    if not pool or not config.PRED_PRECISION_GATE_ENABLED:
        return
    rs = max(0.25, float(regime_scale))
    max_high = max(1, int(round(int(config.PRED_PRECISION_MAX_HIGH) * rs)))
    top_ml = max(
        (float(getattr(r, "ml_prob", 0.0) or 0.0) for r in pool),
        default=0.0,
    )
    ranked = sorted(
        pool,
        key=lambda r: (precision_conviction_score(r), float(getattr(r, "ml_prob", 0.0) or 0.0)),
        reverse=True,
    )
    promoted: list[str] = []
    for pos, row in enumerate(ranked, start=1):
        if len(promoted) >= max_high:
            break
        if not passes_precision_gate(
            row, top_ml=top_ml, rank_position=pos, feedback_ctx=feedback_ctx
        ):
            continue
        promoted.append(str(row.code).zfill(6))

    promoted_set = set(promoted)
    for row in pool:
        code = str(row.code).zfill(6)
        if code in promoted_set:
            row.confidence_tier = "high"
        elif str(getattr(row, "confidence_tier", "")) == "high":
            row.confidence_tier = "mid"

    if promoted:
        # 섹터 열기 상위 1~2종 추가 승격(정밀 게이트 통과분 외 리콜 보강).
        extra_n = max(0, min(2, max_high - len(promoted)))
        if extra_n:
            rescue_ranked = sorted(
                pool,
                key=lambda r: (
                    float(getattr(r, "industry_limit_up_heat", 0.0) or 0.0)
                    + float(getattr(r, "industry_momentum", 0.0) or 0.0)
                    + float(getattr(r, "momentum_score", 0.0) or 0.0),
                    hybrid_rank_score(r),
                ),
                reverse=True,
            )
            added = 0
            for row in rescue_ranked:
                if added >= extra_n:
                    break
                code = str(row.code).zfill(6)
                if code in promoted_set:
                    continue
                lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
                sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
                if lim + 1e-12 < 0.14 and sec + 1e-12 < 0.20:
                    continue
                row.confidence_tier = "high"
                promoted_set.add(code)
                added += 1
        return

    # 정밀 게이트가 전원 탈락 시: 전일·섹터 모멘텀 상위 1~3종만 고확신 승격(리콜 구제).
    rescue_n = max(1, min(3, max_high))
    rescue_ranked = sorted(
        pool,
        key=lambda r: (
            float(getattr(r, "industry_limit_up_heat", 0.0) or 0.0)
            + float(getattr(r, "momentum_score", 0.0) or 0.0)
            + float(getattr(r, "industry_momentum", 0.0) or 0.0)
            + 0.5 * float(getattr(r, "industry_theme_overlap", 0.0) or 0.0),
            hybrid_rank_score(r),
        ),
        reverse=True,
    )
    for row in rescue_ranked[:rescue_n]:
        mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
        sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
        lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
        if mom + 1e-12 < 0.22 and sec + 1e-12 < 0.18 and lim + 1e-12 < 0.15:
            continue
        row.confidence_tier = "high"


def compute_hit_at_k_metrics(
    ranked_codes: list[str],
    actual_big_codes: set[str],
    *,
    universe_size: int,
    k_list: tuple[int, ...] | None = None,
) -> dict[str, Any]:
    """순위 리스트 대비 Hit@K·Precision@K·Recall·Lift."""
    ks = tuple(k_list or config.PRED_EVAL_HIT_AT_K)
    n_actual = len(actual_big_codes)
    base_rate = (n_actual / universe_size) if universe_size > 0 else 0.0
    out: dict[str, Any] = {
        "universe_size": int(universe_size),
        "actual_big_count": int(n_actual),
        "base_rate": round(base_rate, 6),
    }
    codes = [str(c).zfill(6) for c in ranked_codes]
    actual = {str(c).zfill(6) for c in actual_big_codes}
    best_k_hit = 0
    for k in ks:
        if k <= 0:
            continue
        top = codes[:k]
        hit = sum(1 for c in top if c in actual)
        prec = hit / k if k else 0.0
        out[f"hit_at_{k}"] = int(hit)
        out[f"precision_at_{k}"] = round(prec, 6)
        if base_rate > 1e-12:
            out[f"lift_at_{k}"] = round(prec / base_rate, 4)
        else:
            out[f"lift_at_{k}"] = None
        best_k_hit = max(best_k_hit, hit)
    if n_actual > 0:
        out["recall_at_max_k"] = round(best_k_hit / n_actual, 6)
    else:
        out["recall_at_max_k"] = None
    return out
