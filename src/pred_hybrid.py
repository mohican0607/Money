"""뉴스·ML·시세 모멘텀 하이브리드 랭킹·확신 구간(예측 파이프라인 단일 진실 소스)."""
from __future__ import annotations

import math
from datetime import date
from typing import TYPE_CHECKING

import pandas as pd

from . import config

if TYPE_CHECKING:
    from .predict import PredictionRow

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
    top_k: int = 110,
) -> list[str]:
    """당일 관측 시점 시세로 모멘텀 상위 종목(뉴스 없어도 후보)."""
    allowed = {str(c).zfill(6) for c in listing_codes}
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    if sl.empty:
        return []
    scored: list[tuple[float, str]] = []
    for _, r in sl.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        mom = momentum_raw_from_row(r)
        vs = float(r.get("vol_surge_ratio") or 0.0)
        rl = max(0.0, float(r.get("ret_lag1") or 0.0))
        if mom >= 0.20 or vs >= 1.35 or rl >= 0.055:
            scored.append((mom, code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [c for _, c in scored[: max(20, int(top_k))]]


def hybrid_rank_score(row: PredictionRow) -> float:
    """ML·모멘텀·뉴스를 합친 최종 순위 점수."""
    ml = getattr(row, "ml_prob", None)
    ml_v = float(ml) if ml is not None and math.isfinite(float(ml)) else 0.0
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    news = min(1.0, nh / 5.0) * 0.65 + min(mention, 1.0) * 0.35
    if ml_v > 0:
        return 0.48 * ml_v + 0.38 * mom + 0.14 * news
    return 0.55 * mom + 0.30 * float(getattr(row, "score", 0.0) or 0.0) / 12.0 + 0.15 * news


def _dual_signal_ok(row: PredictionRow, *, hybrid: float, top_hybrid: float) -> bool:
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    rel = hybrid + 1e-12 >= top_hybrid * float(config.PRED_ML_HIGH_RELATIVE_PROB)
    if not rel:
        return False
    if nh >= int(config.PRED_ML_HIGH_MIN_KEYWORD_HITS) and mom + 1e-12 >= 0.18:
        return True
    if mention + 1e-12 >= float(config.PRED_MENTION_GATE_MIN):
        return True
    if mom + 1e-12 >= 0.58 and nh >= 1:
        return True
    if mom + 1e-12 >= 0.72:
        return True
    return False


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
        if high_n < max_high and pos <= 10 and h + 1e-12 >= high_floor and _dual_signal_ok(
            row, hybrid=h, top_hybrid=top_hybrid
        ):
            row.confidence_tier = "high"
            high_n += 1
            continue
        if (
            mid_n < max_mid
            and h + 1e-12 >= mid_floor
            and (ml + 1e-12 >= mid_floor * 0.85 or float(row.momentum_score or 0) >= 0.35)
        ):
            row.confidence_tier = "mid"
            mid_n += 1

    for pos, row in enumerate(ranked, start=1):
        row.rank_position = pos
        row.rank_score = hybrid_rank_score(row)
