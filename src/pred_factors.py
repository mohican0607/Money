"""
다요인(멀티팩터) 예측 스코어 — 뉴스·키워드 외 시장·시세·수급·섹터·ML 합성.

퀀트/헤지펀드식으로 **독립 신호 기둥(pillar)** 을 만든 뒤 가중 합산합니다.
뉴스는 보조(5% 내외)이며, 고확신은 **뉴스 단독**으로는 통과하지 못합니다.
"""
from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from . import config

if TYPE_CHECKING:
    from .predict import PredictionRow


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

