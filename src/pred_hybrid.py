"""뉴스·ML·시세 모멘텀 하이브리드 랭킹·확신 구간(예측 파이프라인 단일 진실 소스).

구성:
  - ``momentum_candidate_codes``: 거래량·전일 수익 등 시세만으로 후보 확장(뉴스 없는 급등 대비)
  - ``hybrid_rank_score``: ML 확률 + 모멘텀 + 전체뉴스 맥락(news_context_score) 가중 합
  - ``assign_hybrid_confidence_tiers``: 하이브리드 순위·이중 신호 게이트로 pred_high / pred_mid 부여

``prediction_ranking``·``ml_move_rank.rank_predictions_ml`` 에서 import 해 사용합니다.
"""
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
    """ML·모멘텀·전체뉴스 맥락·업종 하이브리드 점수."""
    ml = getattr(row, "ml_prob", None)
    ml_v = float(ml) if ml is not None and math.isfinite(float(ml)) else 0.0
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    ind_m = float(getattr(row, "industry_momentum", 0.0) or 0.0)
    ind_ov = float(getattr(row, "industry_theme_overlap", 0.0) or 0.0)
    news = 0.50 * nctx + 0.22 * min(1.0, nh / 5.0) + 0.18 * min(mention, 1.0) + 0.10 * ind_ov
    ind_lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    sector = 0.50 * ind_m + 0.30 * ind_ov + 0.20 * ind_lim
    if ml_v > 0:
        return 0.34 * ml_v + 0.26 * mom + 0.26 * news + 0.14 * sector
    return 0.42 * mom + 0.38 * news + 0.20 * sector


def _dual_signal_ok(row: PredictionRow, *, hybrid: float, top_hybrid: float) -> bool:
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
    ind_ov = float(getattr(row, "industry_theme_overlap", 0.0) or 0.0)
    ml = float(row.ml_prob or 0.0)
    rel_hi = float(config.PRED_ML_HIGH_RELATIVE_PROB)
    rel = hybrid + 1e-12 >= top_hybrid * rel_hi
    if not rel:
        return False
    ml_floor = max(0.04, float(config.PRED_ML_MIN_OUTPUT_PROB))
    strong_news = nctx + 1e-12 >= 0.50 and nh >= 1
    if not strong_news and ml + 1e-12 < ml_floor:
        return False
    if ind_ov + 1e-12 >= 0.45 and (nh >= 1 or mom + 1e-12 >= 0.10):
        return True
    if nctx + 1e-12 >= 0.32 and (mom + 1e-12 >= 0.14 or nh >= 1):
        return True
    if nh >= max(1, int(config.PRED_ML_HIGH_MIN_KEYWORD_HITS) - 1) and mom + 1e-12 >= 0.14:
        return True
    if mention + 1e-12 >= float(config.PRED_MENTION_GATE_MIN) * 0.85 and nh >= 1:
        return True
    if mom + 1e-12 >= 0.48 and nh >= 1:
        return True
    if nh >= 1 and nctx + 1e-12 >= 0.18:
        return True
    if strong_news and nh >= 2:
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
        for row in ranked[:max_mid]:
            row.confidence_tier = "mid"
            mid_n += 1

    for pos, row in enumerate(ranked, start=1):
        row.rank_position = pos
        row.rank_score = hybrid_rank_score(row)
