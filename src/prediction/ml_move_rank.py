"""
훈련 구간 라벨(당일 급등 여부)로 **감독학습 랭커**를 학습해, 관측일 후보 종목을 확률 순으로 정렬합니다.

피처: N−1~N일 **15:30(KST)까지** 뉴스·테마·확정 일봉·KOSPI 흐름·과거 급등 이력(보조) 등.
``stocks.enrich_daily_returns_for_ml`` 로 만든 **전일 수익·거래량·단기 변동성·이평·시가갭·고저폭** 등 시세 요약.
학습: 이진 분류 + 소프트라벨 회귀 이중 헤드, 시간순 보정, 샘플가중·하드네거티브.
랭킹 점수는 raw 분류확률과 회귀점수를 혼합하고, 표시용만 분위 보정합니다.
학습 행은 거래일 ``d`` 기준으로 ``d`` **이전**의 급등 이벤트만 써서 시간 누수를 줄입니다.
"""
from __future__ import annotations

import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .. import config, stocks
from ..learning import support as snapshot_miss_diagnosis
from ..learning import support as theme_carryover
from ..prediction import accuracy_cache as prediction_accuracy_cache
from .. import trading_calendar
from ..features import BreakoutEvent, filter_specific_keywords, keyword_set, name_mention_score
from . import predict
from .candidate_pool import (
    day_candidate_codes,
    industry_must_keep_codes,
    ml_scoring_candidate_codes,
    theme_rotation_peer_codes,
)
from .market_features import (
    blended_hist_return,
    industry_feats,
    IndustryFeatureCache,
    investor_flow_feats_row,
    ks11_market_feats,
    limit_up_streak_norm,
    momentum_for_code,
    ohlcv_lookup,
    ohlcv_row_for_code,
    peer_ret_lag1_mean,
    price_feats_row,
    prior_industry_hot_score,
    prior_return_pct,
    ret_cum3_norm,
    early_blob_for_trading_day,
)
from .news_context import (
    context_feature_vector,
    feats_for_code,
    make_news_ctx_bundle,
    tfidf_vector,
)

try:
    import joblib
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    _SKLEARN_OK = True
except ImportError:  # pragma: no cover
    joblib = None  # type: ignore[misc, assignment]
    Pipeline = None  # type: ignore[misc, assignment]
    StandardScaler = None  # type: ignore[misc, assignment]
    HistGradientBoostingClassifier = None  # type: ignore[misc, assignment]
    HistGradientBoostingRegressor = None  # type: ignore[misc, assignment]
    _SKLEARN_OK = False

# ``_feat_vector`` 반환 순서와 일치해야 함(메타·디버그용).
FEATURE_NAMES = (
    "n_hit",
    "mention",
    "sqrt_n_hit",
    "base_ret",
    "log1p_events",
    "heuristic_score",
    "mention_x_hit",
    "news_kw_count",
    "news_blob_len_log",
    "theme_kw_overlap",
    "ks11_ret_lag1",
    "ks11_ret_std5",
    "ret_lag1",
    "log_vol_lag1",
    "ret_roll_std5",
    "log_vol_roll_mean5",
    "close_ma20_ratio",
    "ret_roll_mean5",
    "vol_surge_ratio",
    "open_gap",
    "hl_range_lag1",
    "ret_lag3",
    "close_ma5_ratio",
    "ret_max5",
    "ret_min5",
    "foreign_net_vol_ratio_lag1",
    "inst_net_vol_ratio_lag1",
    "foreign_holding_pct_lag1",
    "foreign_net_sum3_ratio",
    "investor_flow_score",
    "ret_vs_ks11_lag1",
    "ks11_regime_risk_off",
    "news_cos_code",
    "news_cos_global",
    "news_dot_code",
    "news_lift",
    "news_today_norm",
    "industry_momentum",
    "industry_theme_overlap",
    "industry_limit_up_heat",
    "prior_industry_hot",
    "ret_lag2",
    "peer_ret_lag1_mean",
    "mom_x_news_hit",
    "vol_mom_interaction",
    "ret_cum3",
    "sector_breadth_hot",
    "limit_up_streak",
    "news_x_sector_heat",
    "gap_x_news",
    "gap_x_vol",
    "near_limit_pressure",
)

ML_MODEL_VERSION = 24
MAX_NEG_PER_DAY = 150
MIN_TOTAL_SAMPLES = 200
MIN_POS_SAMPLES = 25


def _soft_move_label(ret: float, *, threshold: float) -> float:
    """수익률 → [0,1] 소프트 라벨(랭킹·회귀용). 임계값 부근·초대형 급등에 민감."""
    r = float(ret)
    thr = float(threshold)
    if r <= 0.0:
        return 0.0
    if r < thr * 0.5:
        return 0.12 * (r / max(1e-6, thr * 0.5))
    if r < thr:
        return 0.12 + 0.38 * ((r - thr * 0.5) / max(1e-6, thr * 0.5))
    # ≥ threshold: 0.5 → 1.0 as ret → thr+0.15
    return min(1.0, 0.50 + 0.50 * min(1.0, (r - thr) / 0.15))


def _row_sample_weight(y_bin: int, ret: float, *, threshold: float) -> float:
    """극단 급등·임계 근접 음성에 더 큰 가중(행 복제 대신)."""
    r = float(ret)
    thr = float(threshold)
    if int(y_bin) == 1:
        return 1.0 + min(2.5, max(0.0, (r - thr) / 0.08))
    if r + 1e-12 >= thr * 0.6:
        return 1.45
    if r + 1e-12 >= thr * 0.4:
        return 1.20
    return 1.0


def _blend_rank_score(clf_prob: float, reg_score: float) -> float:
    """보정 분류 확률 + 회귀 소프트 라벨 예측을 Hit@K용 점수로 혼합."""
    w_c = float(getattr(config, "ML_RANK_BLEND_CLF", 0.55))
    w_r = float(getattr(config, "ML_RANK_BLEND_REG", 0.45))
    s = w_c + w_r
    if s <= 1e-12:
        return float(clf_prob)
    w_c, w_r = w_c / s, w_r / s
    reg = max(0.0, min(1.0, float(reg_score)))
    return max(1e-6, min(0.95, w_c * float(clf_prob) + w_r * reg))


def _build_prob_calibration_table(
    raw_probs: np.ndarray,
    y_true: np.ndarray,
    *,
    population_base_rate: float | None = None,
    n_bins: int = 10,
) -> list[dict[str, float]]:
    """
    검증 구간 raw 확률 → 모집단 급등확률 보정 테이블.

    학습/검증 배열은 급등 전수 + 일별 최대 N개 음성의 case-control 표본이다.
    표본 내 적중률을 그대로 확률로 쓰면 실제 전 종목 모집단보다 수십 배
    과대평가되므로 표본 사전확률을 모집단 사전확률로 odds 보정한다.
    """
    if raw_probs.size == 0:
        return []
    sample_base_rate = float(np.mean(y_true))
    pop_base_rate = (
        float(population_base_rate)
        if population_base_rate is not None
        else sample_base_rate
    )
    sample_base_rate = max(1e-6, min(1.0 - 1e-6, sample_base_rate))
    pop_base_rate = max(1e-6, min(1.0 - 1e-6, pop_base_rate))
    prior_odds_ratio = (
        (pop_base_rate / (1.0 - pop_base_rate))
        / (sample_base_rate / (1.0 - sample_base_rate))
    )

    def _population_rate(sample_rate: float) -> float:
        rate = max(1e-6, min(1.0 - 1e-6, float(sample_rate)))
        corrected_odds = (rate / (1.0 - rate)) * prior_odds_ratio
        return corrected_odds / (1.0 + corrected_odds)

    order = np.argsort(raw_probs)
    raw_s = raw_probs[order]
    y_s = y_true[order]
    n = int(raw_s.size)
    bins = max(3, min(n_bins, n // 8))
    out: list[dict[str, float]] = []
    for b in range(bins):
        i0 = int(b * n / bins)
        i1 = int((b + 1) * n / bins)
        if i1 <= i0:
            continue
        sl_raw = raw_s[i0:i1]
        sl_y = y_s[i0:i1]
        out.append(
            {
                "lo": float(sl_raw[0]),
                "hi": float(sl_raw[-1]),
                "rate": float(_population_rate(float(np.mean(sl_y)))),
                "population_corrected": 1.0,
            }
        )
    return out


def _population_move_base_rate(base_rate: float | None = None) -> float:
    """전 종목 기준 익일 ≥20% 대략 사전확률. case-control 표본률과 분리한다."""
    if base_rate is not None and 1e-5 < float(base_rate) < 0.03:
        return float(base_rate)
    return 0.006


def _rebase_case_control_rate(
    sample_rate: float,
    *,
    sample_base_rate: float,
    population_base_rate: float,
) -> float:
    """case-control 표본률 → 모집단 급등확률."""
    sample = max(1e-6, min(1.0 - 1e-6, float(sample_rate)))
    sample_base = max(1e-6, min(1.0 - 1e-6, float(sample_base_rate)))
    pop_base = max(1e-6, min(1.0 - 1e-6, float(population_base_rate)))
    prior_odds_ratio = (pop_base / (1.0 - pop_base)) / (
        sample_base / (1.0 - sample_base)
    )
    corrected_odds = (sample / (1.0 - sample)) * prior_odds_ratio
    return corrected_odds / (1.0 + corrected_odds)


def _sanitize_calibration_artifacts(
    cal_table: list[dict[str, float]] | None,
    *,
    cal_base_rate: float,
    population_base_rate: float | None = None,
) -> tuple[list[dict[str, float]], float]:
    """
    구버전 캐시처럼 표본 적중률이 들어 있는 보정 테이블을 모집단 확률로 정규화.
    """
    pop = _population_move_base_rate(population_base_rate)
    sample_base = float(cal_base_rate or 0.0)
    table = list(cal_table or [])
    if table and all(
        float(row.get("population_corrected", 0.0) or 0.0) >= 1.0
        for row in table
    ):
        return table, pop
    contaminated = sample_base >= 0.05 or any(
        float(row.get("rate", 0.0) or 0.0) >= 0.20 for row in table
    )
    if not contaminated:
        return table, pop if sample_base <= 0 else sample_base
    if sample_base < 0.05:
        sample_base = max(
            0.05,
            max((float(row.get("rate", 0.0) or 0.0) for row in table), default=0.15),
        )
    fixed = [
        {
            "lo": float(row.get("lo", 0.0)),
            "hi": float(row.get("hi", 1.0)),
            "rate": float(
                _rebase_case_control_rate(
                    float(row.get("rate", sample_base) or sample_base),
                    sample_base_rate=sample_base,
                    population_base_rate=pop,
                )
            ),
            "population_corrected": 1.0,
        }
        for row in table
    ]
    return fixed, pop


def calibrate_ml_probability(
    raw_prob: float,
    *,
    cal_table: list[dict[str, float]] | None,
    base_rate: float,
) -> float:
    """raw ``predict_proba`` → 검증 구간 보정(있으면) + 희귀 사전확률 수축."""
    raw = max(1e-6, min(1.0 - 1e-6, float(raw_prob)))
    table, br = _sanitize_calibration_artifacts(
        cal_table,
        cal_base_rate=float(base_rate),
    )
    if table:
        for row in table:
            lo = float(row.get("lo", 0.0))
            hi = float(row.get("hi", 1.0))
            if lo - 1e-12 <= raw <= hi + 1e-12:
                rate = float(row.get("rate", br))
                return max(1e-5, min(0.12, rate))
        if raw + 1e-12 < float(table[0].get("lo", 0.0)):
            return max(1e-5, min(0.12, float(table[0].get("rate", br))))
        return max(1e-5, min(0.12, float(table[-1].get("rate", br))))
    br = max(1e-5, min(0.03, float(br)))
    logit_r = math.log(raw / (1.0 - raw))
    logit_b = math.log(br / (1.0 - br))
    shrink = 0.35
    z = shrink * logit_r + (1.0 - shrink) * logit_b
    p = 1.0 / (1.0 + math.exp(-z))
    return max(br, min(0.08, p))


def _feat_vector(
    train_events: list[BreakoutEvent],
    code: str,
    name: str,
    news_blob: str,
    kw_news: frozenset[str],
    before_exclusive: date,
    *,
    ohlcv_idx: pd.DataFrame | None = None,
    theme_weights: dict[str, float] | None = None,
    news_ctx: dict[str, Any] | None = None,
    ctx_profiles: dict[str, np.ndarray] | None = None,
    ctx_global: np.ndarray | None = None,
    ctx_lift: np.ndarray | None = None,
    hist_kw: set[str] | frozenset[str] | None = None,
    base_ret: float | None = None,
    code_event_count: int | None = None,
    today_tfidf: np.ndarray | None = None,
    returns_ml: pd.DataFrame | None = None,
    ks11_feats: tuple[float, float] | None = None,
    ind_cache: IndustryFeatureCache | None = None,
) -> list[float]:
    """뉴스·이력 피터 + (선택) 시세 피처. ``ohlcv_idx`` 가 있으면 ``before_exclusive`` 거래일 행을 붙입니다."""
    if hist_kw is None:
        sub = [e for e in train_events if e.trading_day < before_exclusive]
        hist_kw_set: set[str] = set()
        for e in sub:
            if e.code == code:
                hist_kw_set |= set(filter_specific_keywords(e.news_keywords))
    else:
        hist_kw_set = set(hist_kw)
    inter = filter_specific_keywords(hist_kw_set & kw_news)
    n_hit = len(inter)
    mention = name_mention_score(news_blob, name)
    score = n_hit * 1.0 + mention * 5.0
    if base_ret is None:
        sub = [e for e in train_events if e.trading_day < before_exclusive]
        base_ret = predict._historical_mean_return(sub, code)
    if code_event_count is None:
        sub = [e for e in train_events if e.trading_day < before_exclusive]
        ce = predict._count_code_events(sub, code)
    else:
        ce = int(code_event_count)
    overlap = theme_carryover.theme_kw_overlap_score(kw_news, theme_weights)
    if ks11_feats is not None:
        ks11_ret_lag1, ks11_std5 = float(ks11_feats[0]), float(ks11_feats[1])
    else:
        ks11_ret_lag1, ks11_std5 = ks11_market_feats(before_exclusive)
    price = price_feats_row(ohlcv_idx, code, before_exclusive)
    # 목표는 N일 장 마감 전 → N+1일 급등이다. T일 시가갭은 N일에 알 수
    # 없는 미래 정보이므로 학습·백테스트·실전 모두 같은 0으로 고정한다.
    if len(price) > 7:
        price = list(price)
        price[7] = 0.0
    ret_lag1 = price[0] if price else 0.0
    ret_vs_ks11 = float(ret_lag1) - float(ks11_ret_lag1)
    risk_off = (
        1.0
        if float(ks11_ret_lag1) < float(config.PRED_REGIME_KS11_SOFT_MIN)
        else 0.0
    )
    base = [
        float(n_hit),
        float(mention),
        float(math.sqrt(n_hit)),
        float(base_ret),
        float(math.log1p(ce)),
        float(score),
        float(mention * math.sqrt(max(0, n_hit))),
        float(len(kw_news)),
        float(math.log1p(max(0, len(news_blob)))),
        float(overlap),
        float(ks11_ret_lag1),
        float(ks11_std5),
    ]
    ctx_part = [0.0] * 5
    if news_ctx and news_ctx.get("vocab"):
        vocab = list(news_ctx["vocab"])
        idf = news_ctx["idf"]
        lift = ctx_lift if ctx_lift is not None else news_ctx.get("lift")
        global_c = ctx_global if ctx_global is not None else news_ctx.get("global_c")
        profiles = ctx_profiles if ctx_profiles is not None else news_ctx.get("profiles")
        today_v = today_tfidf if today_tfidf is not None else tfidf_vector(news_blob, vocab, idf)
        prof = None
        if isinstance(profiles, dict):
            prof = profiles.get(str(code).zfill(6))
        if prof is None:
            prof = np.zeros(len(vocab), dtype=np.float64)
        if global_c is None:
            global_c = np.zeros(len(vocab), dtype=np.float64)
        if lift is None:
            lift = np.zeros(len(vocab), dtype=np.float64)
        ctx_part = context_feature_vector(today_v, prof, global_c, lift)
    if ind_cache is not None:
        ind_mom, ind_ov, ind_lim = ind_cache.industry_feats(code)
        prior_hot = ind_cache.prior_hot(code)
        peer_lag1 = ind_cache.peer_lag1_mean(code)
        sector_br = ind_cache.sector_breadth(code)
    else:
        ind_mom, ind_ov, ind_lim = industry_feats(
            code, before_exclusive, ohlcv_idx, kw_news, returns_ml=returns_ml
        )
        prior_hot = prior_industry_hot_score(code, before_exclusive, returns_ml)
        peer_lag1 = peer_ret_lag1_mean(code, before_exclusive, returns_ml)
        sector_br = 0.0
    investor = investor_flow_feats_row(ohlcv_idx, code, before_exclusive)
    ret_lag2 = prior_return_pct(ohlcv_idx, code, before_exclusive, lag_sessions=2)
    vol_surge = float(price[6]) if len(price) > 6 else 0.0
    open_gap = float(price[7]) if len(price) > 7 else 0.0
    ret_max5 = float(price[11]) if len(price) > 11 else 0.0
    mom_news = float(ret_lag1) * math.sqrt(max(0.0, float(n_hit)))
    vol_mom = float(ret_lag1) * min(3.0, max(0.0, vol_surge))
    ret_cum3 = ret_cum3_norm(ohlcv_idx, code, before_exclusive)
    lim_streak = limit_up_streak_norm(ohlcv_idx, code, before_exclusive)
    news_sector = math.sqrt(max(0.0, float(n_hit))) * float(ind_lim)
    gap_news = float(open_gap) * math.sqrt(max(0.0, float(n_hit)))
    gap_vol = float(open_gap) * min(3.0, max(0.0, vol_surge))
    near_lim = max(0.0, float(open_gap)) * max(0.0, float(ret_max5))
    return base + price + investor + [float(ret_vs_ks11), float(risk_off)] + ctx_part + [
        float(ind_mom),
        float(ind_ov),
        float(ind_lim),
        float(prior_hot),
        float(ret_lag2),
        float(peer_lag1),
        float(mom_news),
        float(vol_mom),
        float(ret_cum3),
        float(sector_br),
        float(lim_streak),
        float(news_sector),
        float(gap_news),
        float(gap_vol),
        float(near_lim),
    ]


def _code_stats_before_exclusive(
    train_events: list[BreakoutEvent],
    before_exclusive: date,
) -> tuple[dict[str, int], dict[str, float]]:
    """관측일 이전 급등 이벤트 건수·베이스 수익률(피처 반복 계산 방지)."""
    code_returns: dict[str, list[float]] = {}
    all_returns: list[float] = []
    counts: dict[str, int] = {}
    for e in train_events:
        if e.trading_day >= before_exclusive:
            continue
        c6 = str(e.code).zfill(6)
        counts[c6] = counts.get(c6, 0) + 1
        code_returns.setdefault(c6, []).append(float(e.return_pct))
        all_returns.append(float(e.return_pct))
    base_ret = {
        c6: blended_hist_return(all_returns, rets)
        for c6, rets in code_returns.items()
    }
    return counts, base_ret


def _cheap_prescore_code(
    code: str,
    *,
    target_day: date,
    ohlcv_idx: pd.DataFrame,
    kw_news: frozenset[str],
    profile: dict[str, frozenset[str]],
    news_blob: str,
    listing_names: dict[str, str],
) -> float:
    """ML 피처 전 1차 컷 — 뉴스·모멘텀만으로 후보 순위."""
    from . import prediction_ranking as pred_hybrid

    c6 = str(code).zfill(6)
    name = listing_names.get(c6, "") or ""
    hist = profile.get(c6, frozenset())
    n_hit = len(filter_specific_keywords(hist & kw_news))
    mention = name_mention_score(news_blob, name)
    score = n_hit * 2.5 + mention * 4.0
    try:
        row = ohlcv_idx.loc[(target_day, c6)]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[-1]
        mom = pred_hybrid.momentum_raw_from_row(row)
        rl = float(row.get("ret_lag1") or 0.0)
        vs = float(row.get("vol_surge_ratio") or 0.0)
        gap = float(row.get("open_gap") or 0.0)
        rs_proxy = float(row.get("ret_lag1") or 0.0)
        rmin5 = float(row.get("ret_min5") or 0.0)
        rmax5 = float(row.get("ret_max5") or 0.0)
        # 모멘텀 가중 완화 — 전일 급등주가 캡을 독점하면 당일 급등 리콜이 무너짐
        score += mom * 2.2 + max(0.0, min(rl, 0.12)) * 6.0 + min(vs, 2.5) * 0.55
        if 0.04 <= rl + 1e-12 < 0.12:
            score += 1.0
        if rl + 1e-12 >= 0.18:
            score -= 2.5  # 과열은 must_keep 경로로만 생존
        elif rl + 1e-12 >= 0.14:
            score -= 1.0
        if config.PRED_THEME_ROTATION_ENABLED:
            lag_min = float(config.PRED_THEME_ROTATION_LAG_MIN)
            lag_max = float(config.PRED_THEME_ROTATION_LAG_MAX)
            block = float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)
            if lag_min <= rl + 1e-12 < min(lag_max, block):
                sweet = 1.0 - abs(rl - 0.10) / max(0.06, block - lag_min)
                score += 2.8 + 7.0 * sweet
        # 눌림 후 반등·갭 돌파·콜드 RS (캡 컷 완화)
        if rl <= -0.02 and vs + 1e-12 >= 0.06:
            score += 1.35
        if gap + 1e-12 >= 0.03:
            score += 1.4 + 5.0 * min(gap, 0.12)
        if gap + 1e-12 >= 0.08:
            score += 3.5 + 12.0 * min(gap, 0.25)  # 시가갭 돌파 — 당일 급등 리콜 핵심
        if gap + 1e-12 >= 0.015 and n_hit >= 1:
            score += 1.1
        if gap + 1e-12 >= 0.05 and rl + 1e-12 <= 0.05:
            score += 2.2  # 전일 조용 + 당일 갭 돌파
        if rmin5 <= -0.04 and (gap >= 0.02 or vs >= 0.08):
            score += 1.8
        if rs_proxy + 1e-12 >= 0.06 and rmax5 + 1e-12 < 0.18:
            score += 0.9
        if rl <= 0.03 and vs + 1e-12 >= 0.10 and n_hit >= 1:
            score += 1.4
        if rl <= 0.02 and mention + 1e-12 >= 0.35:
            score += 1.2  # 가격 조용 + 종목 직접 언급
    except (KeyError, TypeError, ValueError):
        pass
    try:
        from .. import stocks as stocks_mod

        ic = stocks_mod.industry_code_for_stock(c6)
        if ic:
            peers = stocks_mod.peers_for_industry(ic)[:16]
            hot_peers = 0
            for p in peers:
                try:
                    pr = ohlcv_idx.loc[(target_day, str(p).zfill(6))]
                    if isinstance(pr, pd.DataFrame):
                        pr = pr.iloc[-1]
                    pl = float(pr.get("ret_lag1") or 0.0)
                    if pl + 1e-12 >= 0.05:
                        hot_peers += 1
                except (KeyError, TypeError, ValueError):
                    continue
            if hot_peers >= 2:
                score += 1.4 + 0.28 * min(hot_peers, 8)
            elif hot_peers == 1:
                score += 0.45
    except Exception:
        pass
    return score


def _cap_score_codes_for_ml(
    score_codes: list[str],
    *,
    cap: int,
    must_keep: frozenset[str] | list[str] | tuple[str, ...] | None = None,
    target_day: date,
    ohlcv_idx: pd.DataFrame,
    kw_news: frozenset[str],
    profile: dict[str, frozenset[str]],
    news_blob: str,
    listing_names: dict[str, str],
) -> list[str]:
    """추론 풀 상한 — 저신호 후보 제외. 핫 업종 peer는 cap 밖이면 하위 슬롯과 교체.

    ``must_keep`` 는 **우선순위 리스트**를 권장합니다. frozenset 만 넘기면 순서가
    사라져 상위 must 가 swap 한도에 못 들어갈 수 있습니다.
    """
    if cap <= 0 or len(score_codes) <= cap:
        return score_codes

    def _prescore(c: str) -> float:
        return _cheap_prescore_code(
            c,
            target_day=target_day,
            ohlcv_idx=ohlcv_idx,
            kw_news=kw_news,
            profile=profile,
            news_blob=news_blob,
            listing_names=listing_names,
        )

    ranked = sorted(
        score_codes,
        key=lambda c: (-_prescore(c), str(c).zfill(6)),
    )
    if not must_keep:
        return ranked[:cap]
    # 리스트면 호출부 우선순위 유지, set 이면 안정 정렬
    if isinstance(must_keep, (list, tuple)):
        must_ordered = [str(c).zfill(6) for c in must_keep]
    else:
        must_ordered = sorted(str(c).zfill(6) for c in must_keep)
    # 상위 must 를 풀 앞쪽에 강제 배치(스왑 한도로 유실되던 버그 제거)
    score_set = {str(c).zfill(6) for c in score_codes}
    force = [c for c in must_ordered if c in score_set]
    force = list(dict.fromkeys(force))[:cap]
    force_set = set(force)
    rest = [c for c in ranked if str(c).zfill(6) not in force_set]
    return list(dict.fromkeys(force + rest))[:cap]


def _build_feat_matrix_parallel(
    *,
    cand_ix: list[int],
    listing_codes: list[str],
    listing_names: dict[str, str],
    train_events: list[BreakoutEvent],
    news_blob: str,
    kw_news: frozenset[str],
    target_day: date,
    ohlcv_idx: pd.DataFrame,
    theme_weights: dict[str, float],
    news_ctx: dict[str, Any] | None,
    returns_ml: pd.DataFrame,
    ks11_feats: tuple[float, float],
    event_counts: dict[str, int],
    base_rets: dict[str, float],
    profile: dict[str, frozenset[str]],
    today_tfidf: np.ndarray | None,
    ind_cache: IndustryFeatureCache | None,
) -> np.ndarray:
    """후보 종목 ML 피처 행렬(병렬)."""
    prof_t = (news_ctx or {}).get("profiles") or {}
    glob_t = (news_ctx or {}).get("global_c")
    lift_t = (news_ctx or {}).get("lift")

    def _one(i: int) -> list[float]:
        code = listing_codes[i]
        c6 = str(code).zfill(6)
        name = listing_names.get(c6, "") or ""
        return _feat_vector(
            train_events,
            c6,
            name,
            news_blob,
            kw_news,
            before_exclusive=target_day,
            ohlcv_idx=ohlcv_idx,
            theme_weights=theme_weights,
            news_ctx=news_ctx,
            ctx_profiles=prof_t,
            ctx_global=glob_t,
            ctx_lift=lift_t,
            hist_kw=profile.get(c6, frozenset()),
            base_ret=base_rets.get(c6),
            code_event_count=event_counts.get(c6, 0),
            today_tfidf=today_tfidf,
            returns_ml=returns_ml,
            ks11_feats=ks11_feats,
            ind_cache=ind_cache,
        )

    workers = min(max(1, int(config.PRED_ML_FEAT_WORKERS)), len(cand_ix))
    if workers <= 1 or len(cand_ix) < 24:
        feats = [_one(i) for i in cand_ix]
    else:
        feats = [None] * len(cand_ix)  # type: ignore[list-item]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_one, i): fi for fi, i in enumerate(cand_ix)}
            for fut in futs:
                feats[futs[fut]] = fut.result()
    return np.asarray(feats, dtype=np.float64)


def _build_training_arrays(
    returns_ml: pd.DataFrame,
    train_events: list[BreakoutEvent],
    news_by_calendar: dict[date, list[dict[str, str]]],
    names: dict[str, str],
    threshold: float,
    train_start: date,
    label_before_exclusive: date,
    *,
    pos_boost_keys: set[tuple[str, str]] | None = None,
    neg_boost_keys: set[tuple[str, str]] | None = None,
    fast_train: bool = False,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]] | None:
    """
    훈련 구간 거래일별 급등(양)·비급등(음) 샘플 행을 만들어 ``(X, y, meta)`` 로 반환.

    ``meta`` 에 ``news_ctx``, ``sample_weight``, ``y_soft``, ``row_days`` 포함.
    표본 부족 시 ``None``.
    """
    rng = np.random.default_rng(42)
    rows_x: list[list[float]] = []
    rows_y: list[int] = []
    rows_soft: list[float] = []
    rows_w: list[float] = []
    rows_precision_w: list[float] = []
    rows_day: list[date] = []
    ohlcv_idx = ohlcv_lookup(returns_ml)
    pos_boost = pos_boost_keys or set()
    neg_boost = neg_boost_keys or set()
    boost_cap = int(config.ML_MISS_BOOST_MAX_KEYS)
    if fast_train:
        boost_cap = min(boost_cap, int(config.ML_MISS_BOOST_MAX_KEYS_FAST))
    if boost_cap > 0:
        if len(pos_boost) > boost_cap:
            pick = rng.choice(len(pos_boost), size=boost_cap, replace=False)
            pos_boost = {list(pos_boost)[i] for i in pick}
        if len(neg_boost) > boost_cap:
            pick = rng.choice(len(neg_boost), size=boost_cap, replace=False)
            neg_boost = {list(neg_boost)[i] for i in pick}
    boost_dup = (
        int(config.ML_MISS_BOOST_DUP_FAST)
        if fast_train
        else max(0, int(config.ML_MISS_BOOST_DUP))
    )
    if fast_train:
        neg_cap = min(MAX_NEG_PER_DAY, int(config.ML_TRAIN_MAX_NEG_PER_DAY_FAST))
        train_day_cap = int(config.ML_TRAIN_DAY_CANDIDATE_CAP)
    else:
        neg_cap = min(MAX_NEG_PER_DAY, int(config.ML_TRAIN_MAX_NEG_PER_DAY))
        train_day_cap = 0

    days = sorted(
        d
        for d in returns_ml["Date"].dt.date.unique()
        if train_start <= d < label_before_exclusive
    )
    lookback = int(
        config.ML_TRAIN_LOOKBACK_DAYS_FAST
        if fast_train
        else config.ML_TRAIN_LOOKBACK_DAYS
    )
    if lookback > 0 and len(days) > lookback:
        days = days[-lookback:]
    # 전체 훈련 구간의 news_ctx를 과거 샘플 d에 주면 d 이후 뉴스가 피처에
    # 들어간다. 인과적 일별 context 구현 전까지 기본은 0 피처로 둔다.
    news_ctx = (
        make_news_ctx_bundle(
            train_events,
            news_by_calendar,
            train_start,
            label_before_exclusive,
        )
        if config.ML_USE_NEWS_CONTEXT_FEATURES
        else {}
    )
    vocab = list(news_ctx.get("vocab") or [])
    idf_arr = news_ctx.get("idf")
    prof_t = news_ctx.get("profiles") or {}
    glob_t = news_ctx.get("global_c")
    lift_t = news_ctx.get("lift")
    listing_codes = list(names.keys())
    sorted_ev = sorted(train_events, key=lambda e: e.trading_day)
    ev_i = 0
    hist_kw_by_code: dict[str, set[str]] = {}
    code_returns: dict[str, list[float]] = {}
    all_returns: list[float] = []
    ind_cache_by_day: dict[date, IndustryFeatureCache] = {}

    def _append_row(
        fv: list[float],
        y_bin: int,
        act_ret: float,
        d: date,
        *,
        extra_w: float = 1.0,
        precision_weight: float = 1.0,
    ) -> None:
        soft = _soft_move_label(act_ret, threshold=threshold)
        w = _row_sample_weight(y_bin, act_ret, threshold=threshold) * float(extra_w)
        rows_x.append(fv)
        rows_y.append(int(y_bin))
        rows_soft.append(float(soft))
        rows_w.append(float(w))
        rows_precision_w.append(float(precision_weight))
        rows_day.append(d)

    def _train_feat(
        code: str,
        name: str,
        blob: str,
        kw_news: frozenset[str],
        d: date,
        tw: dict[str, float],
        today_v: np.ndarray | None,
    ) -> list[float]:
        """학습 행 1건용 ``_feat_vector`` 래퍼(코드 정규화·히스토리 kw 누적)."""
        c6 = str(code).zfill(6)
        return _feat_vector(
            train_events,
            c6,
            name,
            blob,
            kw_news,
            before_exclusive=d,
            ohlcv_idx=ohlcv_idx,
            theme_weights=tw,
            news_ctx=news_ctx,
            ctx_profiles=prof_t,
            ctx_global=glob_t,
            ctx_lift=lift_t,
            hist_kw=hist_kw_by_code.get(c6, set()),
            base_ret=blended_hist_return(all_returns, code_returns.get(c6)),
            code_event_count=len(code_returns.get(c6, [])),
            today_tfidf=today_v,
            returns_ml=returns_ml,
            ind_cache=ind_cache_by_day.get(d),
        )

    n_days_total = len(days)
    for di, d in enumerate(days):
        while ev_i < len(sorted_ev) and sorted_ev[ev_i].trading_day < d:
            ev = sorted_ev[ev_i]
            c6 = str(ev.code).zfill(6)
            hist_kw_by_code.setdefault(c6, set()).update(
                filter_specific_keywords(ev.news_keywords)
            )
            code_returns.setdefault(c6, []).append(ev.return_pct)
            all_returns.append(ev.return_pct)
            ev_i += 1
        blob = early_blob_for_trading_day(news_by_calendar, d)
        if not blob.strip():
            continue
        kw_news = keyword_set(blob, k=100)
        ctx = predict.build_scoring_context(blob, train_events)
        day_slice = returns_ml[returns_ml["Date"] == pd.Timestamp(d)]
        if day_slice.empty:
            continue
        # 학습은 급등 전수 + 하드네거 샘플링으로 충분. day_candidate_codes 는 일당 수십 초라 생략.
        cand_set: set[str] = set()
        if di == 0 or (di + 1) % 10 == 0 or di + 1 == n_days_total:
            print(
                f"ML 랭커: 학습표본 구성 {di + 1}/{n_days_total}일 ({d.isoformat()})",
                flush=True,
            )
        ind_cache_by_day[d] = IndustryFeatureCache(
            d, ohlcv_idx, returns_ml, kw_news
        )
        tw = theme_carryover.weights_for_observation_day(
            d,
            train_events=train_events,
            news_by_calendar=news_by_calendar,
            returns_df=returns_ml,
            threshold=threshold,
        )
        today_v: np.ndarray | None = None
        if vocab and idf_arr is not None:
            today_v = tfidf_vector(blob, vocab, idf_arr)
        pos_mask = day_slice["return_pct"] >= threshold
        pos_df = day_slice.loc[pos_mask]
        neg_df = day_slice.loc[~pos_mask]
        if pos_df.empty:
            continue

        d_iso = d.isoformat()
        pos_codes_day = pos_df["Code"].astype(str).str.zfill(6).tolist()
        neg_codes = [
            c
            for c in neg_df["Code"].astype(str).str.zfill(6).tolist()
            if not cand_set or c in cand_set
        ]
        n_pos = int(pos_df.shape[0])
        # 정밀도 head가 실제 후보 풀의 거짓 양성을 충분히 보도록 하드 음성을
        # 가능한 상한까지 사용한다. 경량 학습만 기존 작은 비율을 유지한다.
        n_neg = min(
            neg_cap,
            max(30, 6 * n_pos) if fast_train else max(120, 20 * n_pos),
        )
        if len(neg_codes) > n_neg:
            # 하드 네거티브: 랜덤 후보군을 먼저 좁힌 뒤 고신호 절반 선택(전수 스코어링 방지)
            pool_n = min(len(neg_codes), max(n_neg * 3, n_neg + 40))
            if pool_n < len(neg_codes):
                pool = list(
                    rng.choice(np.array(neg_codes), size=pool_n, replace=False)
                )
            else:
                pool = list(neg_codes)
            scored = sorted(
                pool,
                key=lambda c: (
                    -_cheap_prescore_code(
                        c,
                        target_day=d,
                        ohlcv_idx=ohlcv_idx,
                        kw_news=kw_news,
                        profile=ctx[1],
                        news_blob=blob,
                        listing_names=names,
                    ),
                    str(c).zfill(6),
                ),
            )
            n_hard = max(1, n_neg // 2)
            hard = scored[:n_hard]
            rest = scored[n_hard:]
            n_rand = n_neg - len(hard)
            if n_rand > 0 and rest:
                pick = list(
                    rng.choice(
                        np.array(rest),
                        size=min(n_rand, len(rest)),
                        replace=False,
                    )
                )
            else:
                pick = []
            neg_codes = list(dict.fromkeys(hard + pick))[:n_neg]

        warm = set(pos_codes_day) | set(neg_codes)
        if cand_set:
            warm |= set(list(cand_set)[:80])
        ind_cache_by_day[d].prewarm(warm)

        neg_ret_map = {
            str(c).zfill(6): float(v)
            for c, v in zip(
                neg_df["Code"].astype(str).tolist(),
                pd.to_numeric(neg_df["return_pct"], errors="coerce")
                .fillna(0.0)
                .tolist(),
            )
        }
        for _, r in pos_df.iterrows():
            code = str(r["Code"]).zfill(6)
            name = str(names.get(code, r.get("Name", "")))
            fv = _train_feat(code, name, blob, kw_news, d, tw, today_v)
            try:
                act_ret = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                act_ret = 0.0
            _append_row(fv, 1, act_ret, d)
            # 1차 컷에서 밀릴 급등(저 프리점수)은 리콜용으로 가중
            try:
                ps = _cheap_prescore_code(
                    code,
                    target_day=d,
                    ohlcv_idx=ohlcv_idx,
                    kw_news=kw_news,
                    profile=ctx[1],
                    news_blob=blob,
                    listing_names=names,
                )
            except Exception:
                ps = 99.0
            if ps < 4.0:
                _append_row(
                    fv, 1, act_ret, d, extra_w=1.35, precision_weight=0.0
                )
            if boost_dup > 0 and (d_iso, code) in pos_boost:
                for _ in range(boost_dup):
                    _append_row(
                        fv, 1, act_ret, d, extra_w=1.15, precision_weight=0.0
                    )

        neg_seen: set[str] = set()
        for code in neg_codes:
            name = str(names.get(code, ""))
            fv = _train_feat(code, name, blob, kw_news, d, tw, today_v)
            act_ret = float(neg_ret_map.get(code, 0.0))
            _append_row(fv, 0, act_ret, d)
            neg_seen.add(code)
            if boost_dup > 0 and (d_iso, code) in neg_boost:
                for _ in range(boost_dup):
                    _append_row(
                        fv, 0, act_ret, d, extra_w=1.15, precision_weight=0.0
                    )

        if not fast_train:
            for t_iso, code6 in neg_boost:
                if t_iso != d_iso or code6 in neg_seen:
                    continue
                sub = neg_df[neg_df["Code"].astype(str).str.zfill(6) == code6]
                if sub.empty:
                    continue
                rr = sub.iloc[0]
                name = str(names.get(code6, rr.get("Name", "")))
                fv = _train_feat(code6, name, blob, kw_news, d, tw, today_v)
                try:
                    act_ret = float(rr.get("return_pct") or 0.0)
                except (TypeError, ValueError):
                    act_ret = 0.0
                _append_row(fv, 0, act_ret, d)
                if boost_dup > 0:
                    for _ in range(boost_dup):
                        _append_row(
                            fv,
                            0,
                            act_ret,
                            d,
                            extra_w=1.15,
                            precision_weight=0.0,
                        )
                neg_seen.add(code6)

    if len(rows_y) < MIN_TOTAL_SAMPLES or sum(rows_y) < MIN_POS_SAMPLES:
        return None
    meta: dict[str, Any] = {
        "news_ctx": news_ctx,
        "sample_weight": np.asarray(rows_w, dtype=np.float64),
        "precision_weight": np.asarray(rows_precision_w, dtype=np.float64),
        "y_soft": np.asarray(rows_soft, dtype=np.float64),
        "row_days": list(rows_day),
    }
    return np.asarray(rows_x, dtype=np.float64), np.asarray(rows_y, dtype=np.int32), meta


def _model_path(fp: str, label_before_exclusive: date | None = None) -> Path:
    """스냅샷 지문·워크포워드 컷오프(관측일 T)별 ML 랭커 joblib 캐시 경로."""
    suffix = (
        f"_{label_before_exclusive.isoformat()}" if label_before_exclusive is not None else ""
    )
    return config.TRAIN_CACHE_DIR / f"move_ranker_v{ML_MODEL_VERSION}_{fp}{suffix}.joblib"


def _try_load_joblib_bundle(path: Path, *, fp: str) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        loaded = joblib.load(path)
    except Exception:
        return None
    if (
        not isinstance(loaded, dict)
        or loaded.get("fp") != fp
        or loaded.get("version") != ML_MODEL_VERSION
        or loaded.get("pipeline") is None
    ):
        return None
    return loaded


def _persist_joblib_bundle(path: Path, bundle: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(bundle, path)
    except OSError:
        pass


def _try_load_nearest_prior_bundle(
    *,
    fp: str,
    label_before_exclusive: date,
    max_lookback: int = 12,
) -> tuple[dict[str, Any], date] | None:
    """``label_before_exclusive`` 직전 거래일부터 거슬러 가며 가용 ML joblib 을 찾습니다."""
    try:
        d = trading_calendar.last_trading_day_before(label_before_exclusive)
    except ValueError:
        return None
    for _ in range(max(1, max_lookback)):
        prior = _try_load_joblib_bundle(_model_path(fp, d), fp=fp)
        if prior is None:
            alt = _try_load_joblib_bundle(_model_path(f"{fp}_v22q", d), fp=f"{fp}_v22q")
            if alt is not None:
                alt = dict(alt)
                alt["fp"] = fp
                prior = alt
        if prior is not None and prior.get("pipeline") is not None:
            return prior, d
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break
    return None


def fit_or_load_classifier(
    *,
    train_events: list[BreakoutEvent],
    returns_ml: pd.DataFrame,
    news_by_calendar: dict[date, list[dict[str, str]]],
    listing_names: dict[str, str],
    fp: str,
    label_before_exclusive: date,
    force_retrain: bool = False,
    prefer_prior_reuse: bool = False,
) -> dict[str, Any] | None:
    """
    스냅샷 지문 ``fp`` 단위로 모델을 캐시에 두고, 없으면 학습 후 저장합니다.

    ``force_retrain=True`` 이면 기존 joblib 을 읽지 않고 항상 재학습해 덮어씁니다
    (``--rebuild-train-snapshot`` 과 함께 최신 ``BreakoutEvent`` 로 랭커를 맞출 때).

    ``prefer_prior_reuse=True``(예측 전용·장중) 이면 캐시 미스 시 직전 가용
    모델을 재사용해 학습을 생략합니다(장후 실행 시간 보호).

    Returns:
        ``{"pipeline": sklearn Pipeline | None, "fp": str, "feature_names": tuple}`` 또는
        ``None`` (ML 비활성·sklearn 없음).
    """
    if not config.PRED_USE_ML_RANKER:
        return None
    if not _SKLEARN_OK:
        print("ML 랭커: scikit-learn 미설치 → 휴리스틱만 사용합니다. (pip install scikit-learn)", flush=True)
        return None

    path = _model_path(fp, label_before_exclusive)
    fast_train = bool(
        not force_retrain
        and (
            config.ML_PIPELINE_ALWAYS_FAST_TRAIN
            or config.ML_TRAIN_FAST_ON_CACHE_MISS
        )
    )
    bundle: dict[str, Any] = {
        "pipeline": None,
        "precision_pipeline": None,
        "reg_pipeline": None,
        "fp": fp,
        "feature_names": FEATURE_NAMES,
        "version": ML_MODEL_VERSION,
    }
    if not force_retrain:
        loaded = _try_load_joblib_bundle(path, fp=fp)
        loaded_name = path.name if loaded is not None else ""
        if loaded is None:
            # 오프라인 평가용 v22q 캐시(지문에 _v22q 접미사) 재사용
            alt_fp = f"{fp}_v22q"
            alt_path = _model_path(alt_fp, label_before_exclusive)
            loaded = _try_load_joblib_bundle(alt_path, fp=alt_fp)
            if loaded is not None:
                loaded = dict(loaded)
                loaded["fp"] = fp
                loaded_name = alt_path.name
        if loaded is not None:
            bundle["pipeline"] = loaded["pipeline"]
            bundle["precision_pipeline"] = loaded.get("precision_pipeline")
            bundle["reg_pipeline"] = loaded.get("reg_pipeline")
            bundle["cal_base_rate"] = float(loaded.get("cal_base_rate") or 0.01)
            bundle["cal_table"] = list(loaded.get("cal_table") or [])
            bundle["news_ctx"] = loaded.get("news_ctx")
            if bundle["news_ctx"] is None:
                bundle["news_ctx"] = make_news_ctx_bundle(
                    train_events,
                    news_by_calendar,
                    config.TRAIN_START_DEFAULT,
                    label_before_exclusive,
                )
            print(f"ML 랭커: 캐시 로드 {loaded_name}", flush=True)
            return bundle
        reuse_prior = bool(
            prefer_prior_reuse
            or config.ML_REUSE_PRIOR_TRADING_DAY_MODEL
        )
        if reuse_prior:
            found = _try_load_nearest_prior_bundle(
                fp=fp, label_before_exclusive=label_before_exclusive
            )
            if found is not None:
                prior, prev_td = found
                bundle["pipeline"] = prior["pipeline"]
                bundle["precision_pipeline"] = prior.get("precision_pipeline")
                bundle["reg_pipeline"] = prior.get("reg_pipeline")
                bundle["cal_base_rate"] = float(
                    prior.get("cal_base_rate") or 0.01
                )
                bundle["cal_table"] = list(prior.get("cal_table") or [])
                bundle["news_ctx"] = prior.get("news_ctx")
                if bundle["news_ctx"] is None:
                    bundle["news_ctx"] = make_news_ctx_bundle(
                        train_events,
                        news_by_calendar,
                        config.TRAIN_START_DEFAULT,
                        label_before_exclusive,
                    )
                _persist_joblib_bundle(path, bundle)
                print(
                    f"ML 랭커: 직전 가용({prev_td}) 모델 재사용 -> {path.name} "
                    f"(T={label_before_exclusive} 학습 생략)",
                    flush=True,
                )
                return bundle

    pos_boost, neg_boost = snapshot_miss_diagnosis.load_ml_boost_sets_from_snapshot()
    if config.ML_INCREMENTAL_MISS_BOOST_ENABLED:
        from . import feedback_loop

        inc_pos, inc_neg = feedback_loop.load_incremental_miss_boost()
        pos_boost |= inc_pos
        neg_boost |= inc_neg
    if pos_boost or neg_boost:
        print(
            f"ML 랭커: 스냅샷 miss_diagnosis 가중 - 급등 보강 {len(pos_boost)}건·오판 보강 {len(neg_boost)}건",
            flush=True,
        )

    xy = _build_training_arrays(
        returns_ml,
        train_events,
        news_by_calendar,
        listing_names,
        config.BIG_MOVE_THRESHOLD,
        config.TRAIN_START_DEFAULT,
        label_before_exclusive,
        pos_boost_keys=pos_boost,
        neg_boost_keys=neg_boost,
        fast_train=fast_train,
    )
    if xy is None:
        print(
            "ML 랭커: 학습 표본 부족으로 비활성(휴리스틱만). "
            f"최소 {MIN_TOTAL_SAMPLES}행·급등 라벨 {MIN_POS_SAMPLES}건 이상 필요.",
            flush=True,
        )
        return bundle

    X, y, meta = xy
    news_ctx_tr = meta.get("news_ctx")
    sample_weight = np.asarray(meta.get("sample_weight"), dtype=np.float64)
    precision_weight = np.asarray(meta.get("precision_weight"), dtype=np.float64)
    y_soft = np.asarray(meta.get("y_soft"), dtype=np.float64)
    row_days: list[date] = list(meta.get("row_days") or [])
    if sample_weight.shape[0] != len(y):
        sample_weight = np.ones(len(y), dtype=np.float64)
    if precision_weight.shape[0] != len(y):
        precision_weight = np.ones(len(y), dtype=np.float64)
    if y_soft.shape[0] != len(y):
        y_soft = y.astype(np.float64)

    # 시간순 홀드아웃(마지막 ~18% 거래일) — IID split 낙관 편향 제거
    uniq_days = sorted(set(row_days)) if row_days else []
    n_cal_days = max(1, int(round(len(uniq_days) * 0.18))) if uniq_days else 0
    cal_day_set = set(uniq_days[-n_cal_days:]) if n_cal_days and uniq_days else set()
    if cal_day_set and row_days:
        mask_cal = np.asarray([d in cal_day_set for d in row_days], dtype=bool)
        # 보정 구간에 양·음이 모두 있어야 함
        if int(y[mask_cal].sum()) >= 5 and int((1 - y[mask_cal]).sum()) >= 10:
            mask_tr = ~mask_cal
        else:
            mask_tr = np.ones(len(y), dtype=bool)
            mask_cal = np.zeros(len(y), dtype=bool)
    else:
        mask_tr = np.ones(len(y), dtype=bool)
        mask_cal = np.zeros(len(y), dtype=bool)

    X_tr, y_tr = X[mask_tr], y[mask_tr]
    w_tr = sample_weight[mask_tr]
    p_keep_tr = precision_weight[mask_tr] > 0
    p_keep_cal = precision_weight[mask_cal] > 0
    soft_tr = y_soft[mask_tr]
    X_cal, y_cal = X[mask_cal], y[mask_cal]

    if fast_train:
        print(
            "ML 랭커: 캐시 미스 - 장마감(15:30) 전 완료용 경량 학습",
            flush=True,
        )

    clf = HistGradientBoostingClassifier(
        max_depth=5 if fast_train else 8,
        max_iter=48 if fast_train else 320,
        learning_rate=0.08 if fast_train else 0.04,
        random_state=42,
        class_weight="balanced",
        early_stopping=True,
        validation_fraction=0.12,
        n_iter_no_change=8 if fast_train else 18,
        min_samples_leaf=14 if fast_train else 10,
        l2_regularization=0.0 if fast_train else 0.08,
    )
    pipe: Pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", clf),
        ]
    )
    precision_pipe: Pipeline | None = None
    reg_pipe: Pipeline | None = None
    try:
        pipe.fit(X_tr, y_tr, clf__sample_weight=w_tr)
    except TypeError:
        try:
            pipe.fit(X_tr, y_tr)
        except Exception as e:
            print(f"ML 랭커: 학습 실패(휴리스틱만) - {e}", flush=True)
            return bundle
    except Exception as e:
        print(f"ML 랭커: 학습 실패(휴리스틱만) - {e}", flush=True)
        return bundle

    # confidence 전용 head: binary label만, class balancing/급등 복제 없이 학습.
    # 랭킹 head와 분리해야 soft-return/recall 최적화가 확신 확률을 부풀리지 않는다.
    if int(p_keep_tr.sum()) >= MIN_TOTAL_SAMPLES:
        try:
            precision_clf = HistGradientBoostingClassifier(
                max_depth=4,
                max_iter=80 if fast_train else 220,
                learning_rate=0.06 if fast_train else 0.035,
                random_state=43,
                class_weight=None,
                early_stopping=True,
                validation_fraction=0.12,
                n_iter_no_change=10 if fast_train else 20,
                min_samples_leaf=24,
                l2_regularization=0.35,
            )
            precision_pipe = Pipeline(
                [
                    ("scaler", StandardScaler()),
                    ("clf", precision_clf),
                ]
            )
            precision_pipe.fit(X_tr[p_keep_tr], y_tr[p_keep_tr])
        except Exception as e:
            print(f"ML 정밀도 head 학습 실패(랭킹 head 대체) - {e}", flush=True)
            precision_pipe = None

    # 소프트 라벨 회귀 — Hit@K 정렬용 연속 점수
    if HistGradientBoostingRegressor is not None and soft_tr.size >= MIN_TOTAL_SAMPLES // 2:
        try:
            reg = HistGradientBoostingRegressor(
                max_depth=5 if fast_train else 7,
                max_iter=40 if fast_train else 220,
                learning_rate=0.08 if fast_train else 0.045,
                random_state=42,
                early_stopping=True,
                validation_fraction=0.12,
                n_iter_no_change=8 if fast_train else 16,
                min_samples_leaf=14 if fast_train else 12,
                l2_regularization=0.0 if fast_train else 0.12,
            )
            reg_pipe = Pipeline(
                [
                    ("scaler", StandardScaler()),
                    ("reg", reg),
                ]
            )
            reg_pipe.fit(X_tr, soft_tr, reg__sample_weight=w_tr)
        except TypeError:
            try:
                reg_pipe.fit(X_tr, soft_tr)
            except Exception:
                reg_pipe = None
        except Exception:
            reg_pipe = None

    cal_table: list[dict[str, float]] = []
    cal_population_rate: float | None = None
    X_cal_precision = X_cal[p_keep_cal]
    y_cal_precision = y_cal[p_keep_cal]
    if X_cal_precision.shape[0] > 0:
        pop = returns_ml.loc[
            pd.to_datetime(returns_ml["Date"]).dt.date.isin(cal_day_set)
            & returns_ml["Code"].astype(str).str.zfill(6).isin(listing_names)
        ]
        pop_ret = pd.to_numeric(pop.get("return_pct"), errors="coerce").dropna()
        if not pop_ret.empty:
            cal_population_rate = float(
                (pop_ret >= float(config.BIG_MOVE_THRESHOLD)).mean()
            )
        cal_model = precision_pipe if precision_pipe is not None else pipe
        raw_cal = cal_model.predict_proba(X_cal_precision)[:, 1]
        cal_table = _build_prob_calibration_table(
            raw_cal,
            y_cal_precision,
            population_base_rate=cal_population_rate,
        )
    bundle["pipeline"] = pipe
    bundle["precision_pipeline"] = precision_pipe
    bundle["reg_pipeline"] = reg_pipe
    bundle["cal_base_rate"] = (
        cal_population_rate
        if cal_population_rate is not None
        else (
            float(y_cal_precision.mean())
            if y_cal_precision.size > 0
            else float(np.mean(y_tr[p_keep_tr]))
        )
    )
    bundle["cal_sample_rate"] = (
        float(y_cal_precision.mean())
        if y_cal_precision.size > 0
        else float(np.mean(y_tr[p_keep_tr]))
    )
    bundle["cal_population_rate"] = cal_population_rate
    bundle["cal_table"] = cal_table
    bundle["news_ctx"] = news_ctx_tr
    _persist_joblib_bundle(path, bundle)
    print(
        f"ML 랭커: 학습 완료 표본 {len(y)} (급등 {int(y.sum())}건"
        f"·보정일 {len(cal_day_set)}일"
        f"{'·회귀ON' if reg_pipe is not None else ''}"
        f") -> 저장 {path.name}"
        f"{' [경량]' if fast_train else ''}",
        flush=True,
    )
    return bundle


def rank_predictions_ml(
    *,
    target_day: date,
    listing_codes: list[str],
    listing_names: dict[str, str],
    train_events: list[BreakoutEvent],
    news_text_blob: str,
    returns_ml: pd.DataFrame,
    pipeline: Any,
    top_n: int = 40,
    min_keyword_hits: int = 0,
    theme_weights: dict[str, float] | None = None,
    feedback_ctx: dict[str, object] | None = None,
    ml_bundle: dict[str, Any] | None = None,
    forward_observation: bool = False,
) -> list[predict.PredictionRow]:
    """종목 관련 신호가 있는 후보만 급등 확률을 매기고 상위 ``top_n`` ``PredictionRow`` 를 만듭니다."""
    t0 = time.perf_counter()
    returns_ml = stocks.align_returns_ml_for_forecast(returns_ml, target_day)
    ctx = predict.build_scoring_context(news_text_blob, train_events)
    kw_news, profile = ctx
    if feedback_ctx is None:
        from . import feedback_loop

        feedback_ctx = feedback_loop.build_enriched_feedback_context(
            as_of=target_day
        )
    tw = theme_weights or {}
    ohlcv_idx = ohlcv_lookup(returns_ml)
    from .. import investor_flow
    from . import prediction_ranking as pred_hybrid, prediction_ranking

    news_ctx = (ml_bundle or {}).get("news_ctx")
    score_codes = day_candidate_codes(
        listing_codes,
        listing_names,
        news_text_blob,
        ctx,
        returns_ml,
        target_day,
        news_ctx_bundle=news_ctx,
    )
    pool_cap = int(config.PRED_ML_SCORE_POOL_CAP)
    n_raw = len(score_codes)
    must_keep_list = [
        str(c).zfill(6)
        for c in industry_must_keep_codes(returns_ml, target_day, listing_codes)
        if str(c).zfill(6).isdigit()
    ]
    score_codes = _cap_score_codes_for_ml(
        score_codes,
        cap=pool_cap,
        must_keep=must_keep_list,
        target_day=target_day,
        ohlcv_idx=ohlcv_idx,
        kw_news=kw_news,
        profile=profile,
        news_blob=news_text_blob,
        listing_names=listing_names,
    )
    rotation_must: list[str] = []
    if config.PRED_THEME_ROTATION_ENABLED:
        rotation_must = theme_rotation_peer_codes(
            returns_ml,
            target_day,
            listing_codes,
            max_codes=int(config.PRED_THEME_ROTATION_ENRICH_MAX),
        )
        have_sc = {str(c).zfill(6) for c in score_codes}
        for c in rotation_must:
            c6 = str(c).zfill(6)
            if c6 not in have_sc:
                score_codes.append(c6)
                have_sc.add(c6)
    news_only = set(
        ml_scoring_candidate_codes(
            listing_codes,
            listing_names,
            news_text_blob,
            ctx,
            int(config.PRED_ML_POOL_MIN_KEYWORD_HITS),
        )
    )
    mom_only = set(
        pred_hybrid.momentum_candidate_codes(returns_ml, target_day, listing_codes)
    )
    flow_only = set(
        investor_flow.investor_flow_candidate_codes(
            returns_ml, target_day, listing_codes
        )
    )
    flow_only -= news_only | mom_only
    news_ctx_only: set[str] = set(score_codes) - news_only - mom_only - flow_only
    code_to_ix = {c: i for i, c in enumerate(listing_codes)}
    cand_ix: list[int] = []
    for c in score_codes:
        ix = code_to_ix.get(c)
        if ix is None:
            continue
        if stocks.is_observation_day_trading_halted(returns_ml, c, target_day):
            continue
        cand_ix.append(ix)
    if not cand_ix:
        return []

    ks11_ret, ks11_std5 = ks11_market_feats(target_day)
    ks11_feats = (ks11_ret, ks11_std5)
    event_counts, base_rets = _code_stats_before_exclusive(train_events, target_day)
    today_tfidf: np.ndarray | None = None
    if news_ctx and news_ctx.get("vocab"):
        today_tfidf = tfidf_vector(
            news_text_blob,
            list(news_ctx["vocab"]),
            news_ctx.get("idf"),
        )

    ind_cache = IndustryFeatureCache(
        target_day, ohlcv_idx, returns_ml, kw_news
    )
    ind_cache.prewarm(score_codes)

    X = _build_feat_matrix_parallel(
        cand_ix=cand_ix,
        listing_codes=listing_codes,
        listing_names=listing_names,
        train_events=train_events,
        news_blob=news_text_blob,
        kw_news=kw_news,
        target_day=target_day,
        ohlcv_idx=ohlcv_idx,
        theme_weights=tw,
        news_ctx=news_ctx,
        returns_ml=returns_ml,
        ks11_feats=ks11_feats,
        event_counts=event_counts,
        base_rets=base_rets,
        profile=profile,
        today_tfidf=today_tfidf,
        ind_cache=ind_cache,
    )
    proba_raw = pipeline.predict_proba(X)[:, 1]
    precision_pipe = (ml_bundle or {}).get("precision_pipeline")
    if precision_pipe is not None:
        try:
            precision_raw = np.asarray(
                precision_pipe.predict_proba(X)[:, 1], dtype=np.float64
            )
        except Exception:
            precision_raw = np.asarray(proba_raw, dtype=np.float64)
    else:
        precision_raw = np.asarray(proba_raw, dtype=np.float64)
    cal_table = list((ml_bundle or {}).get("cal_table") or [])
    cal_base = float((ml_bundle or {}).get("cal_base_rate") or 0.01)
    reg_pipe = (ml_bundle or {}).get("reg_pipeline")
    reg_raw: np.ndarray | None = None
    if reg_pipe is not None:
        try:
            reg_raw = np.clip(
                np.asarray(reg_pipe.predict(X), dtype=np.float64), 0.0, 1.0
            )
        except Exception:
            reg_raw = None
    # 랭킹은 raw 확률(+회귀) 사용 — 분위 보정은 구간 내 순위를 뭉갬
    if reg_raw is not None:
        rank_score = np.asarray(
            [
                _blend_rank_score(float(c), float(r))
                for c, r in zip(proba_raw, reg_raw)
            ],
            dtype=np.float64,
        )
    else:
        rank_score = np.asarray(proba_raw, dtype=np.float64)
    # 보정 테이블은 confidence 전용 binary head 기준이다.
    proba = np.asarray(
        [
            calibrate_ml_probability(float(p), cal_table=cal_table, base_rate=cal_base)
            for p in precision_raw
        ],
        dtype=np.float64,
    )
    # 상위 선정은 rank_score; ml_prob=보정, ml_rank_score=혼합
    order = np.argsort(-rank_score)
    eval_k = max(config.PRED_EVAL_HIT_AT_K) if config.PRED_EVAL_HIT_AT_K else 40
    finalize_n = max(
        int(top_n),
        int(config.PRED_ML_FINALIZE_POOL_N),
        int(eval_k),
    )
    enrich_n = min(
        len(cand_ix),
        max(finalize_n, int(config.PRED_ML_ENRICH_TOP_N)),
    )
    enrich_order = order[:enrich_n]
    industry_must = industry_must_keep_codes(
        returns_ml, target_day, listing_codes
    )
    must_enrich_codes = list(
        dict.fromkeys(str(c).zfill(6) for c in industry_must)
    )
    rotation_code_set = {str(c).zfill(6) for c in rotation_must}
    for c in rotation_code_set:
        if c not in must_enrich_codes:
            must_enrich_codes.append(c)
    if must_enrich_codes:
        ix_by_code = {
            str(listing_codes[i]).zfill(6): pos for pos, i in enumerate(cand_ix)
        }
        extra: list[int] = []
        for c in must_enrich_codes:
            pos = ix_by_code.get(c)
            if pos is not None:
                extra.append(pos)
        if extra:
            merged = list(enrich_order)
            seen_enrich = set(merged)
            for p in extra:
                if p not in seen_enrich:
                    merged.append(p)
                    seen_enrich.add(p)
            enrich_order = np.asarray(merged, dtype=int)

    buf: list[predict.PredictionRow] = []
    for fi in enrich_order:
        i = cand_ix[int(fi)]
        code = listing_codes[i]
        c6 = str(code).zfill(6)
        is_rotation = c6 in rotation_code_set
        is_industry_must = c6 in must_enrich_codes
        news_bypass = not config.PRED_REQUIRE_NEWS_EVIDENCE
        pr = predict.prediction_row_for_code(
            code,
            listing_names,
            train_events,
            news_text_blob,
            ctx,
            int(config.PRED_ML_POOL_MIN_KEYWORD_HITS),
            feedback_ctx=feedback_ctx,
            theme_weights=tw,
            allow_momentum_only=(
                news_bypass
                and (
                    (code in mom_only and code not in news_only)
                    or is_rotation
                    or is_industry_must
                )
            ),
            allow_news_context_only=code in news_ctx_only
            or (news_bypass and (is_rotation or is_industry_must)),
            allow_investor_flow_only=(
                news_bypass
                and (code in flow_only or is_rotation or is_industry_must)
            ),
        )
        if pr is None:
            continue
        p = float(proba[int(fi)])
        pr.ml_prob = p
        pr.ml_precision_score = float(precision_raw[int(fi)])
        pr.ml_rank_score = float(rank_score[int(fi)])
        pr.ks11_ret_lag1 = ks11_ret
        pr.momentum_score = momentum_for_code(ohlcv_idx, code, target_day)
        ohlcv_row = ohlcv_row_for_code(ohlcv_idx, code, target_day)
        if ohlcv_row is not None:
            pr.relative_strength_score = pred_hybrid.relative_strength_from_row(
                ohlcv_row, ks11_ret_lag1=ks11_ret
            )
            pr.vol_surge_ratio = float(ohlcv_row.get("vol_surge_ratio") or 0.0)
            pr.ret_lag1 = float(ohlcv_row.get("ret_lag1") or 0.0)
            pr.open_gap = float(ohlcv_row.get("open_gap") or 0.0)
        try:
            row = ohlcv_row if ohlcv_row is not None else ohlcv_idx.loc[(target_day, code)]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            pr.investor_flow_score = float(row.get("investor_flow_score") or 0.0)
            pr.foreign_net_vol_ratio = float(row.get("foreign_net_vol_ratio_lag1") or 0.0)
        except (KeyError, TypeError, ValueError):
            pr.investor_flow_score = 0.0
            pr.foreign_net_vol_ratio = 0.0
        ind_mom, ind_ov, ind_lim = ind_cache.industry_feats(code)
        pr.industry_momentum = ind_mom
        pr.industry_theme_overlap = ind_ov
        pr.industry_limit_up_heat = ind_lim
        pr.prior_industry_hot = ind_cache.prior_hot(code)
        pr.sector_breadth_hot = ind_cache.sector_breadth(code)
        if news_ctx:
            _, nctx = feats_for_code(news_ctx, news_text_blob, code)
            pr.news_context_score = nctx
        if config.PRED_REQUIRE_NEWS_EVIDENCE and not pred_hybrid.row_has_news_evidence(pr):
            continue
        pr.reasons = [
            f"다요인 랭킹: {pred_hybrid.format_factor_summary(pr, ks11_ret_lag1=ks11_ret)} · "
            f"ML {p * 100:.1f}% · 키워드 {pr.keyword_hits} · "
            f"익일 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) 추정."
        ] + list(pr.reasons)
        buf.append(pr)

    pre_pool = pred_hybrid.select_pre_pool_with_hot_promotion(
        buf, finalize_n=min(len(buf), finalize_n)
    )
    if feedback_ctx and config.PRED_FEEDBACK_ADAPTIVE_ENABLED:
        from . import feedback_loop

        feedback_loop.apply_feedback_rank_penalties(pre_pool, feedback_ctx)
    t_ml = time.perf_counter() - t0
    cap_note = f" 풀 {n_raw}→{len(cand_ix)}" if n_raw != len(cand_ix) else ""
    print(
        f"ML 랭커: 추론{cap_note} enrich {len(enrich_order)}종 {t_ml:.1f}s",
        flush=True,
    )

    out = prediction_ranking.finalize_ranked_predictions(
        pre_pool,
        target_day=target_day,
        ks11_ret_lag1=ks11_ret,
        forward_observation=forward_observation,
        returns_ml=returns_ml,
        feedback_ctx=feedback_ctx,
    )
    out = out[:top_n]
    if config.PRED_ERROR_FEEDBACK_ENABLED:
        for pr in out:
            n_hit = int(getattr(pr, "keyword_hits", 0) or 0)
            mention = float(getattr(pr, "mention_score", 0.0) or 0.0)
            pr.predicted_return_pct = (
                predict._feedback_calibrated_return(
                    pr.predicted_return_pct / 100.0,
                    code=pr.code,
                    n_hit=n_hit,
                    mention=mention,
                    feedback_ctx=feedback_ctx,
                    clamp_lo=float(config.PRED_RETURN_MIN),
                    clamp_hi=float(config.PRED_RETURN_MAX),
                )
                * 100.0
            )
    return out


# 하위 호환(구 진단·외부 스크립트)
_day_candidate_codes = day_candidate_codes
_ks11_market_feats = ks11_market_feats
