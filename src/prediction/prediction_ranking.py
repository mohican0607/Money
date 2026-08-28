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


def prior_day_exhaustion_penalty(row: PredictionRow) -> float:
    """전일 급등·과열 후 익일 추격 위험(0~1). ``ret_lag1`` 양수 구간만 사용."""
    if not config.PRED_PRIOR_DAY_EXHAUSTION_ENABLED:
        return 0.0
    lag = float(getattr(row, "ret_lag1", 0.0) or 0.0)
    if lag <= 0:
        return 0.0
    block = float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)
    warn = float(config.PRED_PRIOR_DAY_EXHAUSTION_WARN_RET)
    if lag + 1e-12 >= block:
        return 1.0
    if lag + 1e-12 >= warn:
        return min(0.9, (lag - warn) / max(1e-9, block - warn))
    return 0.0


def prior_day_exhaustion_blocks_confidence(row: PredictionRow) -> bool:
    """전일 급등(기본 18%+) 종목은 고·중 확신 슬롯에서 제외."""
    if not config.PRED_PRIOR_DAY_EXHAUSTION_ENABLED:
        return False
    lag = float(getattr(row, "ret_lag1", 0.0) or 0.0)
    return lag + 1e-12 >= float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)


def blocks_high_confidence(row: PredictionRow) -> bool:
    """
    고확신 전용 차단 — 중확신보다 엄격.

    전일 이미 크게 오른 종목·급락 반등 추격·상한가 연속은 high 에서 제외해 정밀도를 올립니다.
    """
    if prior_day_exhaustion_blocks_confidence(row):
        return True
    lag = float(getattr(row, "ret_lag1", 0.0) or 0.0)
    if lag + 1e-12 >= float(config.PRED_HIGH_EXHAUSTION_BLOCK_RET):
        return True
    if prior_day_drop_penalty(row) + 1e-12 >= 0.85:
        return True
    # 전일 상한가권 연속은 익일 추격 실패가 많음
    try:
        lim_proxy = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
        if lag + 1e-12 >= 0.18 and lim_proxy + 1e-12 >= 0.35:
            return True
    except (TypeError, ValueError):
        pass
    return False


def high_precision_select_score(row: PredictionRow) -> float:
    """
    고확신 선정용 점수 — 하이브리드·ML 합의 + 전일 과열 페널티 + 로테이션 가산.
    """
    ml = _ml_rank_signal(row)
    h = hybrid_rank_score(row)
    ml_n = min(1.0, ml / 0.55) if ml > 0 else 0.0
    score = 0.50 * ml_n + 0.50 * h
    rl = float(getattr(row, "ret_lag1", 0.0) or 0.0)
    if 0.03 <= rl + 1e-12 < 0.10:
        score += 0.10
    elif 0.0 <= rl + 1e-12 < 0.03:
        score += 0.04
    elif rl + 1e-12 >= 0.10:
        score -= 0.20 * min(1.0, (rl - 0.10) / 0.08)
    rot = theme_rotation_score(row)
    if rot + 1e-12 >= 0.28:
        score += 0.08 * rot
    burst = sector_burst_rotation_bonus(row)
    if burst > 0 and rl + 1e-12 < 0.10:
        score += 0.06 * burst
    pillars = compute_pillar_scores(row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None))
    if pillars["news"] + 1e-12 >= 0.55 and count_non_news_strong_pillars(pillars, threshold=0.40) < 1:
        score *= 0.72
    if int(getattr(row, "keyword_hits", 0) or 0) <= 0:
        score *= 0.62
    if _carryover_without_news(row):
        score *= 0.55
    return float(score)


def prior_day_drop_penalty(row: PredictionRow) -> float:
    """전일 급락 종목의 익일 급등 추격 위험(0~1). ``ret_lag1`` 은 부호 유지."""
    lag = float(getattr(row, "ret_lag1", 0.0) or 0.0)
    floor = float(config.PRED_PRIOR_DAY_DROP_WARN_RET)
    block = float(config.PRED_PRIOR_DAY_DROP_BLOCK_RET)
    if lag + 1e-12 >= floor:
        return 0.0
    if lag + 1e-12 <= block:
        return 1.0
    return min(1.0, (floor - lag) / max(1e-9, floor - block))


def sector_burst_rotation_bonus(row: PredictionRow) -> float:
    """당일 업종 상한 열기 + 낮은 전일 업종 과열 → 후발주·로테이션 가산."""
    prior = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
    lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    br = float(getattr(row, "sector_breadth_hot", 0.0) or 0.0)
    ov = float(getattr(row, "industry_theme_overlap", 0.0) or 0.0)
    if prior + 1e-12 >= float(config.PRED_SECTOR_BURST_PRIOR_MAX):
        return 0.0
    if lim + 1e-12 < float(config.PRED_SECTOR_BURST_LIM_MIN):
        return 0.0
    bonus = (
        float(config.PRED_SECTOR_BURST_LIM_W) * lim
        + float(config.PRED_SECTOR_BURST_BREADTH_W) * br
        + float(config.PRED_SECTOR_BURST_THEME_W) * ov
    )
    if br + 1e-12 >= 0.18:
        bonus += float(config.PRED_SECTOR_BURST_BREADTH_EXTRA)
    return min(float(config.PRED_SECTOR_BURST_MAX), bonus)


def theme_rotation_score(row: PredictionRow) -> float:
    """
    핫 섹터 내 전일 중간 모멘텀(로테이션) 점수(0~1).

    상한가·과열 리더(ret_lag1 높음)보다 익일 +20% 로테이션 후보를 우선합니다.
    """
    if not config.PRED_THEME_ROTATION_ENABLED:
        return 0.0
    lag = float(getattr(row, "ret_lag1", 0.0) or 0.0)
    if lag <= 0:
        return 0.0
    lag_min = float(config.PRED_THEME_ROTATION_LAG_MIN)
    lag_max = float(config.PRED_THEME_ROTATION_LAG_MAX)
    block = float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)
    if lag + 1e-12 < lag_min or lag + 1e-12 >= min(lag_max, block):
        return 0.0
    prior_hot = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
    sec = max(
        float(getattr(row, "industry_momentum", 0.0) or 0.0),
        float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0),
        float(getattr(row, "sector_breadth_hot", 0.0) or 0.0),
    )
    if prior_hot + 1e-12 < 0.12 and sec + 1e-12 < 0.18:
        return 0.0
    sweet = 1.0 - abs(lag - 0.10) / max(0.06, block - lag_min)
    return _clamp01(0.30 * prior_hot + 0.38 * sec + 0.32 * sweet)


def enrich_prediction_rows_from_returns_ml(
    rows: list[PredictionRow],
    returns_ml: pd.DataFrame | None,
    *,
    target_day: date,
    ks11_ret_lag1: float | None = None,
) -> None:
    """고정 캐시에서 복원한 행에 시세·업종 피처를 다시 채웁니다."""
    if not rows or returns_ml is None or returns_ml.empty:
        return
    from .market_features import IndustryFeatureCache, momentum_for_code, ohlcv_lookup, ohlcv_row_for_code

    ohlcv_idx = ohlcv_lookup(returns_ml)
    ind_cache = IndustryFeatureCache(target_day, ohlcv_idx, returns_ml, frozenset())
    ind_cache.prewarm([r.code for r in rows])
    for row in rows:
        if ks11_ret_lag1 is not None:
            row.ks11_ret_lag1 = ks11_ret_lag1
        row.momentum_score = momentum_for_code(ohlcv_idx, str(row.code), target_day)
        ohlcv_row = ohlcv_row_for_code(ohlcv_idx, str(row.code), target_day)
        if ohlcv_row is not None:
            row.relative_strength_score = relative_strength_from_row(
                ohlcv_row, ks11_ret_lag1=ks11_ret_lag1
            )
            row.vol_surge_ratio = float(ohlcv_row.get("vol_surge_ratio") or 0.0)
            row.ret_lag1 = float(ohlcv_row.get("ret_lag1") or 0.0)
            row.open_gap = float(ohlcv_row.get("open_gap") or 0.0)
            row.investor_flow_score = float(ohlcv_row.get("investor_flow_score") or 0.0)
            row.foreign_net_vol_ratio = float(
                ohlcv_row.get("foreign_net_vol_ratio_lag1") or 0.0
            )
        ind_mom, ind_ov, ind_lim = ind_cache.industry_feats(str(row.code))
        row.industry_momentum = ind_mom
        row.industry_theme_overlap = ind_ov
        row.industry_limit_up_heat = ind_lim
        row.prior_industry_hot = ind_cache.prior_hot(str(row.code))
        row.sector_breadth_hot = ind_cache.sector_breadth(str(row.code))


def _promote_theme_rotation_confidence(
    ranked: list[PredictionRow],
    *,
    max_high: int,
    max_mid: int,
    high_n: int,
    mid_n: int,
) -> tuple[int, int]:
    """로테이션 후보는 **중확신만** 보강(고확신은 정밀 게이트 전용 — 정밀도 보호)."""
    if not config.PRED_THEME_ROTATION_ENABLED:
        return high_n, mid_n
    tier_min = float(config.PRED_THEME_ROTATION_TIER_MIN)
    for row in ranked:
        if mid_n >= max_mid:
            break
        if row.confidence_tier in ("high", "mid"):
            continue
        if prior_day_exhaustion_blocks_confidence(row):
            continue
        if blocks_high_confidence(row):
            # 과열은 mid 도 스킵
            continue
        rot = theme_rotation_score(row)
        if rot + 1e-12 < tier_min:
            continue
        pillars = compute_pillar_scores(
            row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None)
        )
        if count_non_news_strong_pillars(pillars, threshold=0.34) < 1:
            continue
        row.confidence_tier = "mid"
        mid_n += 1
    return high_n, mid_n


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


def _ml_rank_signal(row: PredictionRow) -> float:
    """하이브리드·정렬용 ML 신호: raw 혼합점수 우선, 없으면 보정 ml_prob."""
    raw = getattr(row, "ml_rank_score", None)
    if raw is not None:
        try:
            v = float(raw)
            if math.isfinite(v) and v > 0.0:
                return v
        except (TypeError, ValueError):
            pass
    try:
        return float(getattr(row, "ml_prob", 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _calibrated_ml_prob(row: PredictionRow) -> float | None:
    """보정 급등 확률(0~1). 없으면 None — raw ``ml_rank_score`` 와 구분."""
    p = getattr(row, "ml_prob", None)
    if p is None:
        return None
    try:
        v = float(p)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v < 0.0:
        return None
    return v


def _pool_top_calibrated_ml(pool: list[PredictionRow]) -> float:
    best = 0.0
    for row in pool:
        p = _calibrated_ml_prob(row)
        if p is not None and p > best:
            best = p
    return best


def _adaptive_calibrated_high_floor(
    pool: list[PredictionRow],
    *,
    regime_scale: float,
) -> float:
    """풀 상위 보정확률·레짐에 따른 고확신 ML 하한."""
    rs = max(0.25, min(1.0, float(regime_scale)))
    top = _pool_top_calibrated_ml(pool)
    rel = float(config.PRED_HIGH_CALIBRATED_RELATIVE)
    abs_min = float(config.PRED_HIGH_CALIBRATED_ABS_MIN) / rs
    base = float(config.PRED_FORWARD_HIGH_CALIBRATED_MIN) / rs
    if top > 1e-12:
        return max(base, abs_min, top * rel)
    return max(base, abs_min)


def row_has_news_evidence(row: PredictionRow) -> bool:
    """early 뉴스 키워드·종목명 언급·TF-IDF 맥락 중 하나라도 있으면 True."""
    return _news_evidence_strength(row) > 1e-12


def _must_keep_bypass_codes(
    returns_ml: pd.DataFrame | None,
    target_day: date,
) -> frozenset[str]:
    """뉴스 필터 예외 — 갭돌파·업종 must_keep 후보."""
    if returns_ml is None or returns_ml.empty:
        return frozenset()
    from .candidate_pool import industry_must_keep_codes

    listing = sorted(
        {
            str(c).zfill(6)
            for c in returns_ml["Code"].astype(str).str.zfill(6).unique()
            if str(c).strip()
        }
    )
    if not listing:
        return frozenset()
    return frozenset(
        str(c).zfill(6)
        for c in industry_must_keep_codes(returns_ml, target_day, listing)
    )


def _news_backed_rows(
    rows: list[PredictionRow],
    *,
    bypass_codes: frozenset[str] | set[str] | None = None,
) -> list[PredictionRow]:
    """``PRED_REQUIRE_NEWS_EVIDENCE`` 가 켜져 있으면 뉴스 근거 없는 행을 제외합니다.

    ``bypass_codes``(must_keep·갭돌파 등)는 뉴스 없어도 랭킹 풀에 유지합니다.
    """
    if not config.PRED_REQUIRE_NEWS_EVIDENCE:
        return list(rows)
    bypass = {str(c).zfill(6) for c in (bypass_codes or ())}
    return [
        r
        for r in rows
        if str(r.code).zfill(6) in bypass or row_has_news_evidence(r)
    ]


def _news_evidence_strength(row: PredictionRow) -> float:
    """
    early 뉴스·키워드·종목명 언급·TF-IDF 맥락 합성 근거(0~1).

    테마 캐리오버·섹터만 강하고 뉴스가 없으면 0.5 미만으로 떨어집니다.
    """
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
    gate = float(config.PRED_MENTION_GATE_MIN)

    if nh >= 3:
        return 1.0
    if nh >= 2:
        return min(1.0, 0.88 + 0.06 * min(1.0, mention / max(gate, 0.08)))
    if nh >= 1 and mention + 1e-12 >= gate * 0.25:
        return 0.72
    if nh >= 1:
        return 0.58
    if mention + 1e-12 >= gate:
        return 0.78
    if mention + 1e-12 >= gate * 0.45:
        return 0.48
    if nctx + 1e-12 >= 0.42:
        return 0.40
    return 0.0


def _high_tier_news_ok(row: PredictionRow) -> bool:
    return _news_evidence_strength(row) + 1e-12 >= float(
        config.PRED_HIGH_NEWS_EVIDENCE_MIN
    )


def _carryover_without_news(row: PredictionRow) -> bool:
    """섹터·캐리오버만 강하고 뉴스 근거가 약한 패턴(7월 오탐 다수)."""
    pillars = compute_pillar_scores(
        row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None)
    )
    news_ev = _news_evidence_strength(row)
    sector_hot = pillars["sector"] + 1e-12 >= 0.52
    if sector_hot and news_ev + 1e-12 < float(config.PRED_HIGH_NEWS_EVIDENCE_MIN):
        return True
    if (
        pillars["ml"] + 1e-12 >= 0.82
        and news_ev + 1e-12 < 0.50
        and int(getattr(row, "keyword_hits", 0) or 0) <= 0
    ):
        return True
    return False


def compute_pillar_scores(
    row: PredictionRow,
    *,
    ks11_ret_lag1: float | None = None,
) -> dict[str, float]:
    """0~1 기둥 점수: ml·모멘텀·수급·상대강도·섹터·뉴스·시장."""
    ml = _ml_rank_signal(row)
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
    prior_hot = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
    br_hot = float(getattr(row, "sector_breadth_hot", 0.0) or 0.0)
    if (
        prior_hot + 1e-12 < float(config.PRED_SECTOR_BURST_PRIOR_MAX)
        and ind_lim + 1e-12 >= float(config.PRED_SECTOR_BURST_LIM_MIN)
    ):
        sector = min(1.0, sector + 0.14 * ind_lim + 0.08 * br_hot + 0.06 * ind_ov)
    news = _clamp01(0.35 * nctx + 0.35 * min(1.0, nh / 4.0) + 0.30 * min(mention, 1.0))
    # raw 혼합점수는 보정확률보다 스케일이 큼 → 0.35 기준 정규화
    ml_scale = 0.35 if getattr(row, "ml_rank_score", None) is not None else 0.15

    return {
        "ml": _clamp01(ml / ml_scale),
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

    ml_v = _ml_rank_signal(row)
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
    pen = prior_day_exhaustion_penalty(row)
    if pen > 0:
        s *= max(0.35, 1.0 - 0.62 * pen)
    drop = prior_day_drop_penalty(row)
    if drop > 0:
        s *= max(0.38, 1.0 - 0.58 * drop)
    burst = sector_burst_rotation_bonus(row)
    if burst > 0:
        s = min(1.0, s + burst)
    rot = theme_rotation_score(row)
    if rot > 0:
        s = min(1.0, s + float(config.PRED_THEME_ROTATION_RANK_BOOST) * rot)
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
        rl_raw = float(r.get("ret_lag1") or 0.0)
        rl = max(0.0, rl_raw)
        if mom >= 0.18 or vs >= 1.22 or rl >= 0.038:
            scored.append((mom, code))
        elif rl_raw <= -0.025 and vs + 1e-12 >= 0.07:
            scored.append((0.14 + min(0.18, abs(rl_raw)) + 0.08 * min(vs, 2.0), code))
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
    """finalize 풀 선정용 — ML·하이브리드·모멘텀·섹터열기 중 강한 신호를 반영."""
    h = hybrid_rank_score(row)
    ml = _ml_rank_signal(row)
    ml_den = 0.40 if getattr(row, "ml_rank_score", None) is not None else 0.11
    ml_n = min(1.0, ml / ml_den) if ml > 0 else 0.0
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    sec = max(
        float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0),
        float(getattr(row, "industry_momentum", 0.0) or 0.0),
        float(getattr(row, "prior_industry_hot", 0.0) or 0.0),
    )
    br = float(getattr(row, "sector_breadth_hot", 0.0) or 0.0)
    prior = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
    lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    burst = sector_burst_rotation_bonus(row)
    sec = max(sec, br * 0.85, lim * 0.9 if prior < 0.22 else 0.0)
    blend = 0.34 * ml_n + 0.32 * h + 0.16 * mom + 0.10 * sec + 0.08 * burst
    rot = theme_rotation_score(row)
    blend = min(1.0, blend + 0.22 * rot + 0.10 * burst)
    return max(h, blend, 0.72 * ml_n + 0.28 * h if ml_n > 0 else h, blend)


def _hot_sector_presort_signal(row: PredictionRow) -> float:
    """pre_pool 승격용 — 업종 열기·로테이션 신호(ML 확률과 독립)."""
    lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    sec = max(
        lim,
        float(getattr(row, "industry_momentum", 0.0) or 0.0),
        float(getattr(row, "prior_industry_hot", 0.0) or 0.0),
        float(getattr(row, "sector_breadth_hot", 0.0) or 0.0),
    )
    rot = theme_rotation_score(row)
    return max(rot, sec, lim * 0.95, sec * 0.85 + 0.12 * rot)


def select_pre_pool_with_hot_promotion(
    buf: list[PredictionRow],
    *,
    finalize_n: int,
) -> list[PredictionRow]:
    """ML prescore 상위 + 핫 업종(enrich 하위) 구제 승격으로 finalize 풀을 만듭니다."""
    n = min(len(buf), max(1, int(finalize_n)))
    promote_max = int(config.PRED_PREPOOL_HOT_PROMOTE_MAX)
    if n <= 0 or not buf:
        return []
    if promote_max <= 0 or len(buf) <= n:
        return sorted(buf, key=lambda r: (-recall_presort_score(r), str(r.code).zfill(6)))[:n]

    def _prescore(r: PredictionRow) -> float:
        return recall_presort_score(r)

    ranked = sorted(buf, key=lambda r: (-_prescore(r), str(r.code).zfill(6)))
    base = list(ranked[:n])
    base_ids = {id(r) for r in base}
    candidates: list[tuple[float, PredictionRow]] = []
    for r in ranked[n:]:
        if id(r) in base_ids:
            continue
        lim = float(getattr(r, "industry_limit_up_heat", 0.0) or 0.0)
        prior = float(getattr(r, "prior_industry_hot", 0.0) or 0.0)
        sec_br = float(getattr(r, "sector_breadth_hot", 0.0) or 0.0)
        rot = theme_rotation_score(r)
        sec = max(
            lim,
            float(getattr(r, "industry_momentum", 0.0) or 0.0),
            prior,
            sec_br,
        )
        sig = _hot_sector_presort_signal(r)
        ok = (
            rot + 1e-12 >= 0.24
            or (
                lim + 1e-12 >= 0.30
                and sec + 1e-12 >= 0.22
                and prior + 1e-12 < 0.20
            )
        )
        if ok:
            candidates.append((sig, r))
    candidates.sort(key=lambda x: (-x[0], -_prescore(x[1]), str(x[1].code).zfill(6)))
    replaceable = sorted(base, key=_prescore)
    promoted = 0
    for _, r in candidates:
        if promoted >= promote_max or not replaceable:
            break
        if id(r) in base_ids:
            continue
        worst = replaceable.pop(0)
        try:
            wi = base.index(worst)
        except ValueError:
            continue
        base[wi] = r
        base_ids.discard(id(worst))
        base_ids.add(id(r))
        promoted += 1
    return base


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
    """하이브리드 순위 + 이중 신호로 고·중 확신 부여(고확신은 비과열·ML 합의 우선)."""
    rs = max(0.25, float(regime_scale))
    max_high = max(1, int(round(int(config.PRED_OUTPUT_MAX) * rs)))
    max_mid = max(1, int(round(int(config.PRED_MID_OUTPUT_MAX) * rs)))
    # 최종 high 는 refine 이 다시 고름 — 여기선 후보만 넉넉히 mid/high 표시
    high_floor = max(0.08, float(config.PRED_ML_HIGH_CONFIDENCE_PROB)) / rs
    mid_floor = max(0.045, float(config.PRED_ML_MID_CONFIDENCE_PROB)) / rs

    for row in pool:
        row.confidence_tier = "none"

    if not pool:
        return

    ranked = sorted(pool, key=rank_score_for_row, reverse=True)
    top_hybrid = hybrid_rank_score(ranked[0]) if ranked else 0.0
    high_n = mid_n = 0

    # 고확신 1차: 비과열 + 선정점수 상위만
    high_cand = sorted(
        (r for r in ranked if not blocks_high_confidence(r)),
        key=high_precision_select_score,
        reverse=True,
    )
    top_sel = high_precision_select_score(high_cand[0]) if high_cand else 0.0
    for pos, row in enumerate(high_cand, start=1):
        if high_n >= max_high or pos > 10:
            break
        sel = high_precision_select_score(row)
        ml = _ml_rank_signal(row)
        if sel + 1e-12 < max(0.35, top_sel * 0.55):
            continue
        if ml + 1e-12 < float(config.PRED_HIGH_ABS_ML_FLOOR) * 0.85:
            continue
        if not _dual_signal_ok(row, hybrid=hybrid_rank_score(row), top_hybrid=top_hybrid):
            # 이중신호 실패여도 ML·선정점수가 매우 높으면 허용
            if ml + 1e-12 < 0.45 or sel + 1e-12 < 0.55:
                continue
        row.confidence_tier = "high"
        high_n += 1

    for pos, row in enumerate(ranked, start=1):
        if row.confidence_tier == "high":
            continue
        h = hybrid_rank_score(row)
        ml = _ml_rank_signal(row)
        if prior_day_exhaustion_blocks_confidence(row):
            continue
        mid_ok = h + 1e-12 >= mid_floor or ml + 1e-12 >= mid_floor
        nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
        nh = int(getattr(row, "keyword_hits", 0) or 0)
        if not mid_ok and nctx + 1e-12 >= 0.45 and nh >= 1:
            mid_ok = True
        if (
            mid_n < max_mid
            and mid_ok
            and (
                ml + 1e-12 >= mid_floor * 0.70
                or float(row.momentum_score or 0) >= 0.28
                or nctx + 1e-12 >= 0.40
            )
        ):
            row.confidence_tier = "mid"
            mid_n += 1

    if high_n == 0 and mid_n == 0 and ranked and config.PRED_SECTOR_RESCUE_HIGH_ENABLED:
        for row in ranked[: min(max_mid, 3)]:
            if prior_day_exhaustion_blocks_confidence(row):
                continue
            row.confidence_tier = "mid"
            mid_n += 1

    # 섹터 구조 승격은 mid 만 (high 정밀도 보호)
    if mid_n < max_mid and config.PRED_SECTOR_RESCUE_HIGH_ENABLED:
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
            if mid_n >= max_mid:
                break
            if row.confidence_tier in ("high", "mid"):
                continue
            if blocks_high_confidence(row):
                continue
            lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
            sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
            if lim + 1e-12 < 0.12 and sec + 1e-12 < 0.26:
                continue
            pillars = compute_pillar_scores(
                row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None)
            )
            if count_non_news_strong_pillars(pillars, threshold=0.36) >= 1:
                row.confidence_tier = "mid"
                mid_n += 1

    rot_ranked = sorted(ranked, key=theme_rotation_score, reverse=True)
    high_n, mid_n = _promote_theme_rotation_confidence(
        rot_ranked,
        max_high=max_high,
        max_mid=max_mid,
        high_n=high_n,
        mid_n=mid_n,
    )

    for pos, row in enumerate(ranked, start=1):
        if mid_n >= max_mid or pos > 12:
            break
        if row.confidence_tier != "none":
            continue
        ml = _ml_rank_signal(row)
        pillars = compute_pillar_scores(
            row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None)
        )
        if ml + 1e-12 >= 0.36 and pillars["sector"] + 1e-12 >= 0.42:
            row.confidence_tier = "mid"
            mid_n += 1

    for pos, row in enumerate(ranked, start=1):
        row.rank_position = pos
        row.rank_score = hybrid_rank_score(row)


def rank_score_for_row(row: PredictionRow) -> float:
    """행의 최종 랭킹 점수 — ML 확률·업종열기가 있으면 하이브리드보다 가중."""
    h = hybrid_rank_score(row)
    ml = _ml_rank_signal(row)
    prior = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
    lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    sec_br = float(getattr(row, "sector_breadth_hot", 0.0) or 0.0)
    burst = sector_burst_rotation_bonus(row)
    if ml <= 0:
        return min(1.0, h + burst)
    # raw 혼합점수 스케일(보정 0.11 대비 완화) — 순위 변별력 유지
    ml_den = 0.40 if getattr(row, "ml_rank_score", None) is not None else 0.11
    ml_n = min(1.0, ml / ml_den)
    if (
        burst + 1e-12 >= float(config.PRED_SECTOR_BURST_RANK_MIN)
        and prior + 1e-12 < float(config.PRED_SECTOR_BURST_PRIOR_MAX)
        and lim + 1e-12 >= float(config.PRED_SECTOR_BURST_LIM_MIN)
    ):
        s = max(
            h,
            min(
                1.0,
                0.55 * h
                + 0.28 * ml_n
                + 0.22 * burst
                + 0.12 * lim
                + 0.08 * sec_br,
            ),
        )
    else:
        # ML 정렬을 더 강하게 보존(하이브리드가 순위를 과도하게 뒤집지 않도록)
        s = max(h, 0.28 * h + 0.72 * ml_n)
    if ml_n + 1e-12 >= 0.55 and (prior + 1e-12 >= 0.18 or lim + 1e-12 >= 0.14):
        s = min(1.0, s + 0.09)
    if sec_br + 1e-12 >= 0.35 and ml_n + 1e-12 >= 0.40:
        s = min(1.0, s + 0.07)
    if burst > 0:
        s = min(1.0, s + 0.50 * burst)
    rot = theme_rotation_score(row)
    if rot + 1e-12 >= 0.32 and ml_n + 1e-12 >= 0.28:
        s = min(1.0, s + 0.11 * rot)
    elif rot + 1e-12 >= 0.28:
        s = min(1.0, s + 0.14 * rot)
    drop = prior_day_drop_penalty(row)
    if drop > 0:
        s *= max(0.40, 1.0 - 0.55 * drop)
    if ml_n + 1e-12 >= 0.72:
        s = min(1.0, s + 0.05)
    # 전일 과열 추격 억제 · 조용한 ML 강신호·시가갭 돌파 가산 (Hit@K)
    rl = float(getattr(row, "ret_lag1", 0.0) or 0.0)
    gap = float(getattr(row, "open_gap", 0.0) or 0.0)
    if rl + 1e-12 >= 0.18:
        s *= 0.80
    elif rl + 1e-12 >= 0.14:
        s *= 0.90
    elif rl + 1e-12 < 0.08 and ml_n + 1e-12 >= 0.45:
        s = min(1.0, s + 0.07)
    if gap + 1e-12 >= 0.06 and rl + 1e-12 < 0.12:
        s = min(1.0, s + 0.08 + 0.35 * min(gap, 0.20))
    elif gap + 1e-12 >= 0.03 and rl + 1e-12 < 0.08:
        s = min(1.0, s + 0.04)
    return s


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


def _focused_industry_heat_map(
    returns_ml: pd.DataFrame,
    session_day: date,
    *,
    min_ret: float = 0.15,
    min_count: int = 2,
    top_k: int = 4,
) -> dict[str, float]:
    """당일 다수 급등 업종만 열기 반환 — 단일 종목 급등으로 인한 캐리오버 오탐을 줄입니다."""
    from .. import stocks as stocks_mod

    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(session_day)]
    if sl.empty:
        return {}
    counts: dict[str, int] = {}
    best: dict[str, float] = {}
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
        counts[k] = counts.get(k, 0) + 1
        best[k] = max(best.get(k, 0.0), rp)
    ranked = sorted(
        [k for k, n in counts.items() if n >= min_count],
        key=lambda k: (counts[k], best[k]),
        reverse=True,
    )[:top_k]
    out: dict[str, float] = {}
    for k in ranked:
        out[k] = min(1.0, 0.22 * counts[k] + best[k] / 0.38)
    return out


def _carryover_rerank_enabled(
    returns_ml: pd.DataFrame | None,
    target_day: date,
) -> bool:
    """테마 이어짐일에는 carryover rerank를 자동 활성화."""
    if config.PRED_CARRYOVER_INDUSTRY_RERANK_ENABLED:
        return True
    if not config.PRED_CARRYOVER_AUTO_ON_HEAT or returns_ml is None or returns_ml.empty:
        return False
    try:
        prev = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        return False
    heat_map = _hot_industry_heat_map(returns_ml, prev, min_ret=0.12)
    return max(heat_map.values(), default=0.0) + 1e-12 >= float(
        config.PRED_CARRYOVER_MIN_HEAT
    )


def _rerank_carryover_industry_leaders(
    pool: list[PredictionRow],
    *,
    target_day: date,
    returns_ml: pd.DataFrame | None,
) -> list[PredictionRow]:
    """전일·전전일 급등 업종 리더를 상위로 끌어올려 테마 이어짐일 Hit@K 를 높입니다."""
    if not _carryover_rerank_enabled(returns_ml, target_day):
        return pool
    if not pool or returns_ml is None or returns_ml.empty:
        return pool
    try:
        prev = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        return pool
    if config.PRED_CARRYOVER_FOCUS_MODE:
        heat_map = _focused_industry_heat_map(
            returns_ml,
            prev,
            min_ret=float(config.PRED_CARRYOVER_FOCUS_MIN_RET),
            min_count=int(config.PRED_CARRYOVER_FOCUS_MIN_COUNT),
            top_k=int(config.PRED_CARRYOVER_FOCUS_TOP_K),
        )
        try:
            prev2 = trading_calendar.last_trading_day_before(prev)
            heat2 = _focused_industry_heat_map(
                returns_ml,
                prev2,
                min_ret=float(config.PRED_CARRYOVER_FOCUS_MIN_RET),
                min_count=int(config.PRED_CARRYOVER_FOCUS_MIN_COUNT),
                top_k=int(config.PRED_CARRYOVER_FOCUS_TOP_K),
            )
            for k, v in heat2.items():
                heat_map[k] = max(heat_map.get(k, 0.0), 0.50 * v)
        except ValueError:
            pass
    else:
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
    if not heat_map:
        return pool
    if max(heat_map.values(), default=0.0) + 1e-12 < float(config.PRED_CARRYOVER_MIN_HEAT):
        return pool

    def _boosted(row: PredictionRow) -> float:
        base = rank_score_for_row(row)
        h = heat_map.get(_industry_key(str(row.code)), 0.0)
        if h <= 0:
            return base
        lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
        sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
        mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
        boost = 0.10 + 0.22 * h + 0.08 * max(lim, sec) + 0.06 * mom
        prior_hot = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
        if prior_hot + 1e-12 >= 0.15:
            boost += 0.07 * prior_hot
        ml = float(getattr(row, "ml_prob", 0.0) or 0.0)
        if ml + 1e-12 >= 0.09 and h + 1e-12 >= 0.25:
            boost += 0.05
        exh = prior_day_exhaustion_penalty(row)
        if exh > 0:
            boost *= max(0.12, 1.0 - 0.88 * exh)
        rot = theme_rotation_score(row)
        if rot > 0:
            boost += 0.16 * rot
        return min(1.0, base + boost)

    return sorted(pool, key=_boosted, reverse=True)


def _rerank_sector_diversity(
    pool: list[PredictionRow],
    *,
    max_per_industry: int | None = None,
) -> list[PredictionRow]:
    """상위 구간 동일 업종 쏠림을 줄여 테마 전환일 리콜을 높입니다."""
    top_k = int(config.PRED_SECTOR_DIVERSITY_TOP_K)
    max_per = int(max_per_industry or config.PRED_SECTOR_DIVERSITY_MAX_PER_INDUSTRY)
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


def _dominant_burst_industry(pool: list[PredictionRow]) -> str | None:
    """당일 상한 열기·낮은 전일 과열 업종(로테이션/후발) 키."""
    lims: dict[str, list[float]] = {}
    priors: dict[str, list[float]] = {}
    for row in pool:
        ik = _industry_key(str(row.code))
        lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
        if lim + 1e-12 < float(config.PRED_SECTOR_BURST_LIM_MIN):
            continue
        lims.setdefault(ik, []).append(lim)
        priors.setdefault(ik, []).append(
            float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
        )
    best_ik: str | None = None
    best_score = 0.0
    for ik, vals in lims.items():
        avg_lim = sum(vals) / len(vals)
        avg_prior = sum(priors[ik]) / len(priors[ik])
        if avg_prior + 1e-12 >= float(config.PRED_SECTOR_BURST_PRIOR_MAX):
            continue
        score = avg_lim * (1.0 - min(0.85, avg_prior))
        if score + 1e-12 > best_score:
            best_score = score
            best_ik = ik
    if best_ik is None or best_score + 1e-12 < float(config.PRED_BURST_INDUSTRY_MIN_SCORE):
        return None
    return best_ik


def _burst_industry_leader_score(row: PredictionRow) -> float:
    burst = sector_burst_rotation_bonus(row)
    if burst <= 0:
        return 0.0
    lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    br = float(getattr(row, "sector_breadth_hot", 0.0) or 0.0)
    return burst + 0.34 * lim + 0.22 * br + 0.12 * hybrid_rank_score(row)


def _inject_burst_sector_leaders(pool: list[PredictionRow]) -> list[PredictionRow]:
    """버스트 업종(건설 등 후발주) 리더를 상위 슬롯에 조건부 주입."""
    slots = int(config.PRED_BURST_INDUSTRY_LEADER_SLOTS)
    top_k = min(int(config.PRED_SECTOR_DIVERSITY_TOP_K), len(pool))
    if slots <= 0 or top_k <= 0 or not pool:
        return pool
    burst_ik = _dominant_burst_industry(pool)
    if not burst_ik:
        return pool
    ranked = sorted(pool, key=rank_score_for_row, reverse=True)
    head = list(ranked[:top_k])
    head_ids = {id(r) for r in head}
    candidates = [
        r
        for r in pool
        if _industry_key(str(r.code)) == burst_ik
        and not prior_day_exhaustion_blocks_confidence(r)
        and sector_burst_rotation_bonus(r) + 1e-12
        >= float(config.PRED_SECTOR_BURST_RANK_MIN)
    ]
    if not candidates:
        return pool
    leaders = sorted(candidates, key=_burst_industry_leader_score, reverse=True)[:slots]
    insert_at = max(4, top_k - slots - 2)
    for leader in leaders:
        if id(leader) in head_ids:
            continue
        if insert_at >= len(head):
            head.append(leader)
        else:
            head.insert(insert_at, leader)
            head.pop()
        head_ids.add(id(leader))
        insert_at += 1
    seen = {id(r) for r in head}
    return head + [r for r in ranked if id(r) not in seen]


def _inject_hot_sector_leaders(pool: list[PredictionRow]) -> list[PredictionRow]:
    """핫 섹터 로테이션 후보를 상위 슬롯에 끼워 넣어 Hit@K 리콜을 올립니다."""
    slots = int(config.PRED_INDUSTRY_LEADER_SLOTS)
    top_k = min(int(config.PRED_SECTOR_DIVERSITY_TOP_K), len(pool))
    if slots <= 0 or top_k <= 0 or not pool:
        return pool
    ranked = sorted(pool, key=rank_score_for_row, reverse=True)
    head = list(ranked[:top_k])
    head_ids = {id(r) for r in head}

    def _leader_key(r: PredictionRow) -> tuple[float, float]:
        if prior_day_exhaustion_blocks_confidence(r):
            return (-1.0, 0.0)
        rot = theme_rotation_score(r)
        if rot > 0:
            return (rot + 0.25, rank_score_for_row(r))
        lim = float(getattr(r, "industry_limit_up_heat", 0.0) or 0.0)
        sec = float(getattr(r, "industry_momentum", 0.0) or 0.0)
        lag = float(getattr(r, "ret_lag1", 0.0) or 0.0)
        if lag + 1e-12 >= float(config.PRED_PRIOR_DAY_EXHAUSTION_WARN_RET):
            return (-0.5, 0.0)
        legacy = lim + 0.55 * sec + 0.30 * float(
            getattr(r, "industry_theme_overlap", 0.0) or 0.0
        )
        return (legacy * 0.55, rank_score_for_row(r))

    leaders = sorted(pool, key=_leader_key, reverse=True)
    injected = 0
    for row in leaders:
        if injected >= slots:
            break
        if id(row) in head_ids:
            continue
        rot = theme_rotation_score(row)
        lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
        sec = float(getattr(row, "industry_momentum", 0.0) or 0.0)
        if rot + 1e-12 < 0.20 and lim + 1e-12 < 0.08 and sec + 1e-12 < 0.16:
            continue
        prior = float(getattr(row, "prior_industry_hot", 0.0) or 0.0)
        sec_br = float(getattr(row, "sector_breadth_hot", 0.0) or 0.0)
        if prior + 1e-12 < 0.16 and sec_br + 1e-12 < 0.22 and rot + 1e-12 < 0.24:
            continue
        if prior_day_exhaustion_blocks_confidence(row):
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
    """풀 내 최고 ML 신호(raw 혼합점수 우선, 상대 확신 게이트용)."""
    best = 0.0
    for row in pool:
        v = _ml_rank_signal(row)
        if v > best:
            best = v
    return best


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
        rank_sig = _ml_rank_signal(row)

        if prob + 1e-12 < min_out and score + 1e-12 < min_score:
            continue

        if has_ml:
            # 상대 하한은 raw 혼합점수 기준(보정 상한 0.42 뭉개짐 방지)
            rel_ok = top_prob <= 0 or rank_sig + 1e-12 >= top_prob * rel_hi
            high_ok = (
                rank_sig + 1e-12 >= 0.22
                if getattr(row, "ml_rank_score", None) is not None
                else prob + 1e-12 >= high_thr
            )
            if (
                high_n < max_high
                and high_ok
                and rel_ok
                and _ml_high_signal_ok(row, rank_position=pos)
            ):
                row.confidence_tier = "high"
                high_n += 1
                continue
            mid_ok = (
                rank_sig + 1e-12 >= 0.12
                if getattr(row, "ml_rank_score", None) is not None
                else prob + 1e-12 >= mid_thr
            )
            if mid_n < max_mid and mid_ok:
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


def _use_confidence_tier_for_pred_flags() -> bool:
    """확신 티어 게이트 ON 이면 tier none 은 pred_high/mid 로 승격하지 않음(표시 % fallback 금지)."""
    return config.PRED_RANKING_MODE and config.PRED_CONFIDENCE_OUTPUT_ENABLED


def is_high_confidence_prediction(row: PredictionRow) -> bool:
    """``pred_high``: 랭킹 모드면 ``confidence_tier==high`` 만. 레거시 모드만 표시 % fallback."""
    if config.PRED_RANKING_MODE:
        return str(getattr(row, "confidence_tier", "") or "").strip().lower() == "high"
    tier = str(getattr(row, "confidence_tier", "") or "")
    if tier == "high":
        return True
    if tier == "mid":
        return False
    pct = float(getattr(row, "predicted_return_pct", 0.0) or 0.0)
    return pct >= float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def is_mid_confidence_prediction(row: PredictionRow) -> bool:
    """``pred_mid`` 판정: 랭킹 모드면 ``confidence_tier==mid`` 만."""
    if config.PRED_RANKING_MODE:
        return str(getattr(row, "confidence_tier", "") or "").strip().lower() == "mid"
    tier = str(getattr(row, "confidence_tier", "") or "")
    if tier == "mid":
        return True
    if tier == "high":
        return False
    pct = float(getattr(row, "predicted_return_pct", 0.0) or 0.0)
    return 10.0 - 1e-9 <= pct < float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def row_pred_high_from_dict(row: dict) -> bool:
    """``rows_compare`` dict 에서 ``pred_high`` 판정."""
    if config.PRED_RANKING_MODE:
        return str(row.get("confidence_tier") or "").strip().lower() == "high"
    tier = str(row.get("confidence_tier") or "")
    if tier == "high":
        return True
    if tier == "mid":
        return False
    pr = row.get("pred_ret")
    if pr is None:
        return False
    return float(pr) >= float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def row_pred_mid_from_dict(row: dict) -> bool:
    """``rows_compare`` dict 에서 ``pred_mid`` 판정."""
    if config.PRED_RANKING_MODE:
        return str(row.get("confidence_tier") or "").strip().lower() == "mid"
    tier = str(row.get("confidence_tier") or "")
    if tier == "mid":
        return True
    if tier == "high":
        return False
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
    s = (
        0.32 * pillars["momentum"]
        + 0.28 * pillars["sector"]
        + 0.22 * pillars["ml"]
        + 0.10 * pillars["flow"]
        + 0.08 * pillars["relative_strength"]
        + 0.05 * min(pillars["news"], 0.65)
        + 0.14 * theme_rotation_score(row)
        + 0.10 * sector_burst_rotation_bonus(row)
    )
    drop = prior_day_drop_penalty(row)
    if drop > 0:
        s *= max(0.35, 1.0 - 0.62 * drop)
    return s


def assign_forward_confidence_tiers(
    pool: list[PredictionRow],
    *,
    regime_scale: float = 1.0,
    feedback_ctx: dict[str, object] | None = None,
) -> None:
    """
    N일 → N+1일 실전 확신 게이트.

    보정 ML 확률·최종 순위·키워드 근거·비뉴스 기둥 합의를 모두 요구합니다.
    조건을 통과한 행이 없으면 high/mid를 비워 둡니다. 과거 구현처럼 슬롯을
    강제로 채우면 희귀 사건에서 ``고확신``이 사실상 상위 N개라는 뜻으로
    변질되므로 fallback 승격과 테마/섹터 자동 승격을 금지합니다.
    """
    rs = max(0.25, min(1.0, float(regime_scale)))
    max_high = max(
        0,
        min(
            int(config.PRED_PRECISION_MAX_HIGH),
            int(round(int(config.PRED_PRECISION_MAX_HIGH) * rs)),
        ),
    )
    if feedback_ctx and config.PRED_FEEDBACK_ADAPTIVE_ENABLED:
        streak = int(feedback_ctx.get("recent_miss_streak_days", 0) or 0)
        # tightness는 regime_scale·확률 하한에 이미 반영. 연속 장기 오판만 슬롯 축소.
        if streak >= 6:
            max_high = 0
        elif streak >= 4:
            max_high = min(max_high, 1)
        elif streak >= 2:
            max_high = min(max_high, max(1, max_high))
    max_mid = (
        max(0, int(round(int(config.PRED_MID_OUTPUT_MAX) * rs)))
        if config.PRED_FORWARD_MID_ENABLED
        else 0
    )

    for row in pool:
        row.confidence_tier = "none"
    if not pool:
        return
    if not config.PRED_CONFIDENCE_OUTPUT_ENABLED:
        for pos, row in enumerate(
            sorted(pool, key=rank_score_for_row, reverse=True), start=1
        ):
            row.rank_position = pos
            row.rank_score = hybrid_rank_score(row)
        return

    ranked = sorted(pool, key=rank_score_for_row, reverse=True)
    confidence_ranked = sorted(
        pool,
        key=lambda row: (
            float(getattr(row, "ml_precision_score", 0.0) or 0.0),
            float(getattr(row, "ml_prob", 0.0) or 0.0),
            rank_score_for_row(row),
        ),
        reverse=True,
    )
    top_precision = max(
        (
            float(getattr(row, "ml_precision_score", 0.0) or 0.0)
            for row in confidence_ranked
        ),
        default=0.0,
    )
    high_prob_floor = _adaptive_calibrated_high_floor(pool, regime_scale=rs)
    high_rank_max = int(config.PRED_FORWARD_HIGH_MAX_RANK)
    high_kw_min = int(config.PRED_FORWARD_HIGH_MIN_KEYWORD_HITS)
    high_relative = float(config.PRED_FORWARD_HIGH_RELATIVE_PRECISION)
    high_n = 0
    for pos, row in enumerate(confidence_ranked, start=1):
        if high_n >= max_high or pos > high_rank_max:
            break
        if blocks_high_confidence(row):
            continue
        calibrated = _calibrated_ml_prob(row)
        if calibrated is None:
            continue
        if calibrated + 1e-12 < high_prob_floor:
            continue
        precision_raw = float(
            getattr(row, "ml_precision_score", 0.0) or 0.0
        )
        if (
            top_precision > 1e-12
            and precision_raw + 1e-12 < top_precision * high_relative
        ):
            continue
        if not _high_tier_news_ok(row):
            continue
        if _carryover_without_news(row):
            continue
        keyword_hits = int(getattr(row, "keyword_hits", 0) or 0)
        mention = float(getattr(row, "mention_score", 0.0) or 0.0)
        if keyword_hits < high_kw_min:
            continue
        if (
            mention + 1e-12 < float(config.PRED_MENTION_GATE_MIN)
            and keyword_hits < 2
        ):
            continue
        pillars = compute_pillar_scores(row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None))
        non_news = count_non_news_strong_pillars(pillars, threshold=0.40)
        if non_news < 2:
            continue
        if not _code_history_ok(str(row.code).zfill(6)):
            continue
        row.confidence_tier = "high"
        high_n += 1

    mid_n = 0
    mid_prob_floor = float(config.PRED_FORWARD_MID_CALIBRATED_MIN) / rs
    mid_rank_max = int(config.PRED_FORWARD_MID_MAX_RANK)
    for pos, row in enumerate(ranked, start=1):
        if mid_n >= max_mid or pos > mid_rank_max:
            break
        if row.confidence_tier == "high":
            continue
        if blocks_high_confidence(row):
            continue
        calibrated = float(getattr(row, "ml_prob", 0.0) or 0.0)
        if calibrated + 1e-12 < mid_prob_floor:
            continue
        if int(getattr(row, "keyword_hits", 0) or 0) < 1:
            continue
        pillars = compute_pillar_scores(
            row, ks11_ret_lag1=getattr(row, "ks11_ret_lag1", None)
        )
        if count_non_news_strong_pillars(pillars, threshold=0.38) < 1:
            continue
        row.confidence_tier = "mid"
        mid_n += 1

    for pos, row in enumerate(ranked, start=1):
        row.rank_position = pos
        row.rank_score = hybrid_rank_score(row)


def fill_forward_review_slate(
    pool: list[PredictionRow],
    *,
    regime_scale: float = 1.0,
    feedback_ctx: dict[str, object] | None = None,
) -> None:
    """
    20분 검토용: 고·중 합이 ``PRED_FORWARD_SHOW_MAX`` 에 못 미치면 mid 슬롯을 순위로 채웁니다.

    엄격 게이트 통과가 적어도 표·freeze 에 ``PRED_FORWARD_SHOW_MAX`` 근처 후보가 남도록 합니다.
    연속 오판·낮은 tightness 구간에서는 패딩을 건너뜁니다(약한 종목 mid 승격 방지).
    """
    if not pool or not config.PRED_CONFIDENCE_OUTPUT_ENABLED:
        return
    if not config.PRED_FORWARD_MID_ENABLED:
        return
    streak = 0
    tightness = 1.0
    if feedback_ctx and config.PRED_FEEDBACK_ADAPTIVE_ENABLED:
        streak = int(feedback_ctx.get("recent_miss_streak_days", 0) or 0)
        tightness = float(feedback_ctx.get("adaptive_tightness", 1.0) or 1.0)
    if streak > int(config.PRED_FORWARD_SLATE_PAD_MAX_MISS_STREAK):
        return
    if tightness + 1e-12 < float(config.PRED_FORWARD_SLATE_PAD_MIN_TIGHTNESS):
        return
    rs = max(0.25, min(1.0, float(regime_scale)))
    max_mid = max(0, int(round(int(config.PRED_MID_OUTPUT_MAX) * rs)))
    cap = int(config.PRED_FORWARD_SHOW_MAX)
    target = min(cap, len(pool))
    ranked = sorted(pool, key=rank_score_for_row, reverse=True)
    mid_n = sum(
        1 for r in pool if str(getattr(r, "confidence_tier", "") or "") == "mid"
    )
    rank_limit = max(int(config.PRED_FORWARD_MID_MAX_RANK), cap)

    for pos, row in enumerate(ranked, start=1):
        if mid_n >= max_mid:
            break
        tiered = sum(
            1
            for r in pool
            if str(getattr(r, "confidence_tier", "") or "") in ("high", "mid")
        )
        if tiered >= target:
            break
        if str(getattr(row, "confidence_tier", "") or "") in ("high", "mid"):
            continue
        if pos > rank_limit:
            break
        if config.PRED_REQUIRE_NEWS_EVIDENCE:
            if not row_has_news_evidence(row):
                continue
        else:
            mention_gate = float(config.PRED_MENTION_GATE_MIN)
            nh = int(getattr(row, "keyword_hits", 0) or 0)
            mention = float(getattr(row, "mention_score", 0.0) or 0.0)
            if nh < 1 and mention + 1e-12 < mention_gate * 0.5:
                continue
        if prior_day_exhaustion_blocks_confidence(row):
            continue
        row.confidence_tier = "mid"
        mid_n += 1


def finalize_ranked_predictions(
    rows: list[PredictionRow],
    *,
    target_day: date,
    ks11_ret_lag1: float | None = None,
    forward_observation: bool = False,
    returns_ml: pd.DataFrame | None = None,
    feedback_ctx: dict[str, object] | None = None,
) -> list[PredictionRow]:
    """
    순위·확신 구간·표시 % 를 일괄 확정합니다.

    랭킹 모드: 순위 상위부터 고확신 최대 10·중확신 최대 10(엄격 ML 확률 하한 통과분만).
    """
    if not rows:
        return rows

    from . import predict

    if returns_ml is not None:
        from .. import stocks as stocks_mod

        rows = [
            r
            for r in rows
            if not stocks_mod.is_observation_day_trading_halted(
                returns_ml, str(r.code), target_day
            )
        ]
        if not rows:
            return rows

    rows = _news_backed_rows(
        rows,
        bypass_codes=_must_keep_bypass_codes(returns_ml, target_day),
    )
    if not rows:
        return rows

    max_per_ind = int(config.PRED_SECTOR_DIVERSITY_MAX_PER_INDUSTRY)
    if returns_ml is not None and not returns_ml.empty:
        try:
            prev = trading_calendar.last_trading_day_before(target_day)
            heat_map = _hot_industry_heat_map(returns_ml, prev, min_ret=0.12)
            if max(heat_map.values(), default=0.0) + 1e-12 >= float(
                config.PRED_SECTOR_HOT_DIVERSITY_HEAT
            ):
                max_per_ind = max(
                    max_per_ind, int(config.PRED_SECTOR_HOT_DIVERSITY_MAX_PER_INDUSTRY)
                )
        except ValueError:
            pass

    pool = _rerank_sector_diversity(rows, max_per_industry=max_per_ind)
    pool = _inject_hot_sector_leaders(pool)
    pool = _inject_burst_sector_leaders(pool)
    if returns_ml is not None:
        pool = _rerank_carryover_industry_leaders(
            pool, target_day=target_day, returns_ml=returns_ml
        )
    pool = sorted(pool, key=rank_score_for_row, reverse=True)
    pool_size = len(pool)
    r_scale = regime_output_scale(ks11_ret_lag1)
    if feedback_ctx and config.PRED_FEEDBACK_ADAPTIVE_ENABLED:
        from . import feedback_loop

        tightness = float(feedback_ctx.get("adaptive_tightness", 1.0) or 1.0)
        r_scale *= tightness
        feedback_loop.apply_feedback_rank_penalties(pool, feedback_ctx)
        pool = sorted(pool, key=rank_score_for_row, reverse=True)

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
        # 과거 재생과 실전이 같은 point-in-time 확신 규칙을 써야 검증 수치가
        # 실전 의미를 갖는다. 장후 재생에서만 permissive tier를 쓰지 않는다.
        if config.PRED_CONFIDENCE_OUTPUT_ENABLED:
            assign_forward_confidence_tiers(
                pool, regime_scale=r_scale, feedback_ctx=feedback_ctx
            )
            if config.PRED_PRECISION_GATE_ENABLED:
                refine_confidence_tiers(
                    pool,
                    feedback_ctx=feedback_ctx,
                    regime_scale=r_scale,
                )
        pool = sorted(pool, key=rank_score_for_row, reverse=True)
        if config.PRED_FEEDBACK_ADAPTIVE_ENABLED and feedback_ctx:
            streak = int(feedback_ctx.get("recent_miss_streak_days", 0) or 0)
            tightness = float(feedback_ctx.get("adaptive_tightness", 1.0) or 1.0)
            if (
                streak <= int(config.PRED_FORWARD_SLATE_PAD_MAX_MISS_STREAK)
                and tightness + 1e-12
                >= float(config.PRED_FORWARD_SLATE_PAD_MIN_TIGHTNESS)
            ):
                fill_forward_review_slate(
                    pool, regime_scale=r_scale, feedback_ctx=feedback_ctx
                )
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
        return not config.PRED_PRECISION_CODE_FAIL_CLOSED
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
        return not config.PRED_PRECISION_CODE_FAIL_CLOSED
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
    고확신 슬롯 유지 조건(정밀도 우선).

    ``ml_prob``(보정)와 뉴스 근거를 우선하고, ``ml_rank_score`` 단독 승격을 막습니다.
    """
    if blocks_high_confidence(row):
        return False
    if rank_position > int(config.PRED_PRECISION_MAX_RANK):
        return False
    if not _high_tier_news_ok(row):
        return False
    if _carryover_without_news(row):
        return False

    cal = _calibrated_ml_prob(row)
    if cal is not None:
        abs_floor = max(
            float(config.PRED_HIGH_CALIBRATED_ABS_MIN),
            float(config.PRED_PRECISION_ML_FLOOR),
            float(config.PRED_ML_HIGH_CONFIDENCE_PROB),
        )
        if cal + 1e-12 < abs_floor:
            return False
        rel = float(config.PRED_HIGH_CALIBRATED_RELATIVE)
        if top_ml > 1e-9 and cal + 1e-12 < top_ml * rel:
            return False
        ml = cal
    else:
        ml = _ml_rank_signal(row)
        abs_floor = float(config.PRED_HIGH_ABS_ML_FLOOR)
        if ml + 1e-12 < abs_floor:
            return False
        rel = float(config.PRED_HIGH_RELATIVE_ML)
        if top_ml > 1e-9 and ml + 1e-12 < top_ml * rel:
            return False

    sel = high_precision_select_score(row)
    if sel + 1e-12 < float(config.PRED_HIGH_SELECT_FLOOR):
        return False
    pillars = _pillar_scores(row)
    non_news = count_non_news_strong_pillars(pillars, threshold=0.40)
    if non_news < int(config.PRED_PRECISION_MIN_PILLARS) and ml + 1e-12 < 0.45:
        return False
    if pillars["news"] + 1e-12 >= 0.55 and non_news < 1:
        return False
    if pillars["momentum"] + 1e-12 >= 0.70 and float(getattr(row, "ret_lag1", 0.0) or 0.0) >= 0.10:
        if ml + 1e-12 < abs_floor + 0.15:
            return False
    if not _code_history_ok(str(row.code).zfill(6)):
        return False
    if not _bucket_ok(row, feedback_ctx):
        if ml + 1e-12 < abs_floor + 0.20 or sel + 1e-12 < 0.50:
            return False
    return True


def refine_confidence_tiers(
    pool: list[PredictionRow],
    *,
    feedback_ctx: dict[str, object] | None = None,
    regime_scale: float = 1.0,
) -> None:
    """
    고확신 **감사(audit) 전용** — 승격은 ``assign_forward_confidence_tiers`` 에만 맡깁니다.

    ``ml_rank_score`` 로 raw ML100%·키워드 0인 행이 재승격되던 경로를 차단합니다.
    """
    if not pool or not config.PRED_PRECISION_GATE_ENABLED:
        return
    eligible = [r for r in pool if not blocks_high_confidence(r)]
    top_cal = _pool_top_calibrated_ml(eligible) if eligible else 0.0
    if top_cal <= 1e-12:
        top_cal = max((_ml_rank_signal(r) for r in eligible), default=0.0)

    high_rows = [
        r for r in pool if str(getattr(r, "confidence_tier", "") or "") == "high"
    ]
    ranked_high = sorted(high_rows, key=high_precision_select_score, reverse=True)
    for pos, row in enumerate(ranked_high, start=1):
        if not passes_precision_gate(
            row, top_ml=top_cal, rank_position=pos, feedback_ctx=feedback_ctx
        ):
            row.confidence_tier = "none"


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
