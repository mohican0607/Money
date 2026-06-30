"""
훈련 구간 라벨(당일 급등 여부)로 **감독학습 랭커**를 학습해, 관측일 후보 종목을 확률 순으로 정렬합니다.

피처: N−1~N일 **14:30(KST)까지** 뉴스·테마·시세·KOSPI 흐름·과거 급등 이력(보조) 등.
``stocks.enrich_daily_returns_for_ml`` 로 만든 **전일 수익·거래량·단기 변동성·이평 대비 위치** 등 시세 요약.
학습 행은 거래일 ``d`` 기준으로 ``d`` **이전**의 급등 이벤트만 써서 시간 누수를 줄입니다.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .. import config, trading_calendar
from .. import news
from .. import stocks as market_index
from ..learning import support as snapshot_miss_diagnosis
from ..learning import support as theme_carryover
from ..prediction import accuracy_cache as prediction_accuracy_cache
from . import (
    predict,
)
from ..features import BreakoutEvent, filter_specific_keywords, keyword_set, name_mention_score

try:
    import joblib
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    _SKLEARN_OK = True
except ImportError:  # pragma: no cover
    joblib = None  # type: ignore[misc, assignment]
    Pipeline = None  # type: ignore[misc, assignment]
    StandardScaler = None  # type: ignore[misc, assignment]
    HistGradientBoostingClassifier = None  # type: ignore[misc, assignment]
    _SKLEARN_OK = False

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
    "ret_vs_ks11_lag1",
    "ks11_regime_risk_off",
    "foreign_net_vol_ratio_lag1",
    "inst_net_vol_ratio_lag1",
    "foreign_holding_pct_lag1",
    "foreign_net_sum3_ratio",
    "investor_flow_score",
    "news_cos_code",
    "news_cos_global",
    "news_dot_code",
    "news_lift",
    "news_today_norm",
    "industry_momentum",
    "industry_theme_overlap",
    "industry_limit_up_heat",
    "prior_industry_hot",
)

ML_MODEL_VERSION = 19
MAX_NEG_PER_DAY = 150
MIN_TOTAL_SAMPLES = 200
MIN_POS_SAMPLES = 25


def _build_prob_calibration_table(
    raw_probs: np.ndarray, y_true: np.ndarray, *, n_bins: int = 10
) -> list[dict[str, float]]:
    """검증 구간 raw 확률 → 실제 급등 비율(보정 테이블)."""
    if raw_probs.size == 0:
        return []
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
                "rate": float(np.mean(sl_y)),
            }
        )
    return out


def calibrate_ml_probability(
    raw_prob: float,
    *,
    cal_table: list[dict[str, float]] | None,
    base_rate: float,
) -> float:
    """raw ``predict_proba`` → 검증 구간 보정(있으면) + 희귀 사전확률 수축."""
    raw = max(1e-6, min(1.0 - 1e-6, float(raw_prob)))
    if cal_table:
        for row in cal_table:
            lo = float(row.get("lo", 0.0))
            hi = float(row.get("hi", 1.0))
            if lo - 1e-12 <= raw <= hi + 1e-12:
                rate = float(row.get("rate", base_rate))
                return max(1e-5, min(0.42, rate))
        if raw + 1e-12 < float(cal_table[0].get("lo", 0.0)):
            return max(1e-5, min(0.42, float(cal_table[0].get("rate", base_rate))))
        return max(1e-5, min(0.42, float(cal_table[-1].get("rate", base_rate))))
    br = max(1e-5, min(0.06, float(base_rate)))
    logit_r = math.log(raw / (1.0 - raw))
    logit_b = math.log(br / (1.0 - br))
    shrink = 0.35
    z = shrink * logit_r + (1.0 - shrink) * logit_b
    p = 1.0 / (1.0 + math.exp(-z))
    return max(br, min(0.35, p))


def _prior_day_hot_peer_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    min_prev_ret: float = 0.05,
    max_codes: int = 320,
) -> list[str]:
    """
    T 직전 거래일 급등·강한 전일 모멘텀 종목과 동업종 peer를 후보에 넣습니다.

    6/26처럼 전일·연속 급등 테마가 이어지는 날, 뉴스 키워드만으로는 풀에 안 잡히는
    실제 20%↑ 종목(금호계열 등)을 ML 랭킹 대상에 포함시키기 위함.
    """
    from .. import stocks as stocks_mod

    allowed = {str(c).zfill(6) for c in listing_codes}
    out: list[str] = []
    seen: set[str] = set()

    def _add(code: str) -> None:
        c6 = str(code).zfill(6)
        if c6 in allowed and c6 not in seen:
            seen.add(c6)
            out.append(c6)

    try:
        prev_td = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        prev_td = None

    if prev_td is not None:
        sl_prev = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(prev_td)]
        hot: list[tuple[float, str]] = []
        for _, r in sl_prev.iterrows():
            code = str(r["Code"]).zfill(6)
            if code not in allowed:
                continue
            try:
                ret = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                continue
            if ret + 1e-12 >= min_prev_ret:
                hot.append((ret, code))
        hot.sort(key=lambda x: (-x[0], x[1]))
        for _, code in hot[:85]:
            _add(code)
            ic = stocks_mod.industry_code_for_stock(code)
            if ic:
                for peer in stocks_mod.peers_for_industry(ic)[:16]:
                    _add(peer)

    lookback = max(1, int(config.PRED_INDUSTRY_HEAT_LOOKBACK_DAYS))
    session_days: list[date] = []
    d = prev_td
    for _ in range(lookback):
        if d is None:
            break
        session_days.append(d)
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break
    for sess in session_days:
        sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(sess)]
        for _, r in sl.iterrows():
            code = str(r["Code"]).zfill(6)
            if code not in allowed:
                continue
            try:
                ret = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                continue
            if ret + 1e-12 >= 0.08:
                _add(code)
                ic = stocks_mod.industry_code_for_stock(code)
                if ic:
                    for peer in stocks_mod.peers_for_industry(ic)[:14]:
                        _add(peer)

    sl_t = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    lag_hot: list[tuple[float, str]] = []
    for _, r in sl_t.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        try:
            rl = float(r.get("ret_lag1") or 0.0)
            vs = float(r.get("vol_surge_ratio") or 0.0)
        except (TypeError, ValueError):
            continue
        if rl + 1e-12 >= 0.035 or vs + 1e-12 >= 1.15:
            lag_hot.append((rl + 0.15 * min(vs, 3.0), code))
    lag_hot.sort(key=lambda x: (-x[0], x[1]))
    for _, code in lag_hot[:100]:
        _add(code)
        ic = stocks_mod.industry_code_for_stock(code)
        if ic:
            for peer in stocks_mod.peers_for_industry(ic)[:10]:
                _add(peer)

    return out[:max_codes]


def _burst_industry_peer_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    min_ret: float = 0.12,
    min_count: int = 2,
    max_industries: int = 18,
    peers_per_industry: int = 28,
) -> list[str]:
    """최근 거래일 업종 내 다수 급등 시 동업종 전체를 후보에 넣습니다."""
    from .. import stocks as stocks_mod

    allowed = {str(c).zfill(6) for c in listing_codes}
    out: list[str] = []
    seen: set[str] = set()
    lookback = max(1, int(config.PRED_INDUSTRY_HEAT_LOOKBACK_DAYS))
    session_days: list[date] = []
    d: date | None = target_day
    for _ in range(lookback + 1):
        if d is None:
            break
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break
        session_days.append(d)

    industry_hits: dict[str, int] = defaultdict(int)
    industry_best: dict[str, float] = defaultdict(float)
    for sess in session_days:
        sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(sess)]
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
            industry_hits[k] += 1
            industry_best[k] = max(industry_best[k], rp)

    hot_inds = sorted(
        industry_hits.keys(),
        key=lambda k: (industry_hits[k], industry_best[k]),
        reverse=True,
    )
    added_inds = 0
    for ic in hot_inds:
        if added_inds >= max_industries:
            break
        if industry_hits[ic] < min_count and industry_best[ic] + 1e-12 < 0.18:
            continue
        for peer in stocks_mod.peers_for_industry(ic)[:peers_per_industry]:
            c6 = str(peer).zfill(6)
            if c6 in allowed and c6 not in seen:
                seen.add(c6)
                out.append(c6)
        added_inds += 1
    return out


def _day_candidate_codes(
    listing_codes: list[str],
    listing_names: dict[str, str],
    news_text_blob: str,
    ctx: tuple[frozenset[str], dict[str, frozenset[str]]],
    returns_ml: pd.DataFrame,
    target_day: date,
    news_ctx_bundle: dict[str, Any] | None = None,
) -> list[str]:
    """뉴스 프로필 ∪ 시세 모멘텀 후보(예측·학습 동일 유니버스)."""
    from . import prediction_ranking as pred_hybrid

    news = _ml_scoring_candidate_codes(
        listing_codes,
        listing_names,
        news_text_blob,
        ctx,
        int(config.PRED_ML_POOL_MIN_KEYWORD_HITS),
    )
    mom = pred_hybrid.momentum_candidate_codes(
        returns_ml, target_day, listing_codes
    )
    from .. import investor_flow

    flow_codes = investor_flow.investor_flow_candidate_codes(
        returns_ml, target_day, listing_codes
    )
    news_ctx_codes: list[str] = []
    if news_ctx_bundle:
        today_v = tfidf_vector(
            news_text_blob,
            list(news_ctx_bundle.get("vocab") or []),
            news_ctx_bundle.get("idf"),
        )
        news_ctx_codes = affinity_candidate_codes(
            listing_codes,
            dict(news_ctx_bundle.get("profiles") or {}),
            today_v,
        )
    hot_peers = _prior_day_hot_peer_codes(returns_ml, target_day, listing_codes)
    burst_peers = _burst_industry_peer_codes(returns_ml, target_day, listing_codes)
    rs_codes = pred_hybrid.relative_strength_candidate_codes(
        returns_ml, target_day, listing_codes
    )
    return list(
        dict.fromkeys(
            news + mom + news_ctx_codes + flow_codes + hot_peers + burst_peers + rs_codes
        )
    )


def _momentum_for_code(
    ohlcv_idx: pd.DataFrame | None, code: str, trading_day: date
) -> float:
    """OHLCV 인덱스에서 당일 종목 모멘텀 점수(없으면 0)."""
    from . import prediction_ranking as pred_hybrid

    if ohlcv_idx is None:
        return 0.0
    try:
        row = ohlcv_idx.loc[(trading_day, code)]
    except KeyError:
        return 0.0
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return pred_hybrid.momentum_raw_from_row(row)


def _ohlcv_row_for_code(
    ohlcv_idx: pd.DataFrame | None, code: str, trading_day: date
) -> pd.Series | None:
    """``(trading_day, code)`` 멀티인덱스에서 일봉 행(없으면 ``None``)."""
    if ohlcv_idx is None:
        return None
    try:
        row = ohlcv_idx.loc[(trading_day, code)]
    except KeyError:
        return None
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return row


def _ml_scoring_candidate_codes(
    listing_codes: list[str],
    listing_names: dict[str, str],
    news_text_blob: str,
    ctx: tuple[frozenset[str], dict[str, frozenset[str]]],
    min_keyword_hits: int,
) -> list[str]:
    """
    ML ``predict_proba`` 대상 종목을 종목 관련 신호가 있는 코드로만 축소합니다.

    후보가 적어도 전 종목 폴백은 하지 않습니다(저유동·범용 키워드 오탐 방지).
    """
    kw_news, profile = ctx
    spec_news = filter_specific_keywords(kw_news)
    mention_gate = float(config.PRED_MENTION_GATE_MIN)
    need_hits = max(1, int(min_keyword_hits))
    pool_hits = max(1, int(config.PRED_ML_POOL_MIN_KEYWORD_HITS))
    need_hits = min(need_hits, pool_hits)
    out: list[str] = []
    for code in listing_codes:
        name = listing_names.get(code, "") or ""
        if name_mention_score(news_text_blob, name) >= mention_gate:
            out.append(code)
            continue
        hist = profile.get(code)
        if hist and len(filter_specific_keywords(hist & spec_news)) >= need_hits:
            out.append(code)
            continue
    return out


def _early_blob_for_trading_day(
    news_by_calendar: dict[date, list[dict[str, str]]], d: date
) -> str:
    """거래일 ``d`` 의 early 뉴스 텍스트 blob."""
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by_calendar, d)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(d)
    return predict.aggregate_news_for_window(news_by_calendar, ws, we)


def _ohlcv_lookup(returns_ml: pd.DataFrame) -> pd.DataFrame:
    """(거래일 date, 6자리 Code) → 행 조회용 인덱스."""
    t = returns_ml.copy()
    t["_d"] = pd.to_datetime(t["Date"]).dt.normalize().dt.date
    t["_c"] = t["Code"].astype(str).str.zfill(6)
    return t.set_index(["_d", "_c"])


@lru_cache(maxsize=512)
def _ks11_market_feats(trading_day: date) -> tuple[float, float]:
    """
    시장흐름 피처(지수):
    - ks11_ret_lag1: trading_day 직전 거래일의 KOSPI 수익률
    - ks11_ret_std5: 최근 5개 값 표준편차(변동성)
    """
    try:
        prev = trading_calendar.last_trading_day_before(trading_day)
    except ValueError:
        return 0.0, 0.0
    # 충분한 과거를 확보하기 위해 넉넉히 60일 범위로 로드(캐시됨)
    start_d = pd.Timestamp(prev - pd.Timedelta(days=90)).date()
    end_d   = pd.Timestamp(prev + pd.Timedelta(days=2)).date()
    df      = _load_ks11_cached(start_d, end_d)
    if df is None or df.empty:
        return 0.0, 0.0
    r1 = market_index.index_daily_return_pct(df, prev)
    if r1 is None:
        r1v = 0.0
    else:
        r1v = float(r1)
    # 최근 5개(전일 포함) 지수 수익률 표준편차
    try:
        s = df.sort_values("Date").reset_index(drop=True)
        s["ret"] = s["Close"].pct_change()
        ts = pd.Timestamp(prev)
        hit = s.index[s["Date"] == ts]
        if len(hit) == 0:
            return r1v, 0.0
        i = int(hit[0])
        w = s.loc[max(0, i - 6) : i, "ret"].dropna().astype(float).tolist()
        if len(w) < 2:
            return r1v, 0.0
        mean = sum(w) / len(w)
        var = sum((x - mean) ** 2 for x in w) / max(1.0, float(len(w) - 1))
        std = float(math.sqrt(var))
        return r1v, std
    except Exception:
        return r1v, 0.0


@lru_cache(maxsize=4)
def _load_ks11_cached(start: date, end: date) -> pd.DataFrame:
    """KOSPI(KS11) 지수 OHLCV를 구간별로 로드(``lru_cache``)."""
    try:
        return market_index.load_index_frame("KS11", start, end)
    except Exception:
        return pd.DataFrame()

def _investor_flow_feats_row(
    idx: pd.DataFrame | None, code: str, trading_day: date
) -> list[float]:
    """``returns_ml`` 인덱스에서 수급 lag 피처 5종(없으면 0 패딩)."""
    cols = (
        "foreign_net_vol_ratio_lag1",
        "inst_net_vol_ratio_lag1",
        "foreign_holding_pct_lag1",
        "foreign_net_sum3_ratio",
        "investor_flow_score",
    )
    if idx is None:
        return [0.0] * len(cols)
    try:
        row = idx.loc[(trading_day, code)]
    except KeyError:
        return [0.0] * len(cols)
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    out: list[float] = []
    for c in cols:
        try:
            v = float(row.get(c, 0.0) or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        if math.isnan(v) or math.isinf(v):
            v = 0.0
        if c == "foreign_holding_pct_lag1":
            v = v / 100.0
        out.append(v)
    return out


def _price_feats_row(idx: pd.DataFrame | None, code: str, trading_day: date) -> list[float]:
    """``returns_ml`` 인덱스에서 종목·거래일 시세 피처. 없으면 0으로 채움."""
    cols = (
        "ret_lag1",
        "log_vol_lag1",
        "ret_roll_std5",
        "log_vol_roll_mean5",
        "close_ma20_ratio",
        "ret_roll_mean5",
        "vol_surge_ratio",
    )
    if idx is None:
        return [0.0] * len(cols)
    try:
        row = idx.loc[(trading_day, code)]
    except KeyError:
        return [0.0] * len(cols)
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    out: list[float] = []
    for c in cols:
        try:
            v = float(row.get(c, 0.0) or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        if math.isnan(v) or math.isinf(v):
            v = 0.0
        out.append(v)
    return out


def _blended_hist_return(
    all_returns: list[float],
    code_returns: list[float] | None,
) -> float:
    """``predict._historical_mean_return`` 과 동일 로직, 누적 통계로 O(1) 계산."""
    lo = float(config.PRED_RETURN_MIN)
    hi = float(config.PRED_RETURN_MAX)
    global_mean = sum(all_returns) / len(all_returns) if all_returns else lo
    prior = float(min(hi, max(lo, global_mean)))
    if not code_returns:
        return prior
    code_mean = sum(code_returns) / len(code_returns)
    prior_strength = 5.0
    n = float(len(code_returns))
    blended = (n * code_mean + prior_strength * prior) / (n + prior_strength)
    return float(min(hi, max(lo, blended)))


def _industry_feats(
    code: str,
    day: date,
    ohlcv_idx: pd.DataFrame | None,
    kw_news: frozenset[str],
    returns_ml: pd.DataFrame | None = None,
) -> tuple[float, float, float]:
    """동종 업종 모멘텀·당일 뉴스-업종 겹침·최근 동종 급등 열기."""
    from .. import stocks as stocks_mod

    ind_ov = float(stocks_mod.industry_theme_overlap(code, kw_news))
    ind_lim = 0.0
    if ohlcv_idx is None:
        return 0.0, ind_ov, ind_lim
    ic = stocks_mod.industry_code_for_stock(code)
    if not ic:
        return 0.0, ind_ov, ind_lim
    peer_set = stocks_mod.peers_for_industry(ic)
    rets: list[float] = []
    lim_n = 0
    peer_n = 0
    for p in peer_set:
        try:
            row = ohlcv_idx.loc[(day, str(p).zfill(6))]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            peer_n += 1
            v = float(row.get("ret_lag1") or 0.0)
            if math.isfinite(v):
                rets.append(v)
            if v + 1e-12 >= 0.20:
                lim_n += 1
        except (KeyError, TypeError, ValueError):
            continue
    mom = max(0.0, min(1.0, (sum(rets) / len(rets)) / 0.12)) if rets else 0.0
    if peer_n > 0:
        ind_lim = min(1.0, lim_n / max(1, min(peer_n, 8)))

    if returns_ml is not None and not returns_ml.empty:
        session_days: list[date] = []
        try:
            d = trading_calendar.last_trading_day_before(day)
        except ValueError:
            d = None
        lookback = max(1, int(config.PRED_INDUSTRY_HEAT_LOOKBACK_DAYS))
        for _ in range(lookback):
            if d is None:
                break
            session_days.append(d)
            try:
                d = trading_calendar.last_trading_day_before(d)
            except ValueError:
                break
        hot_peer_rets: list[float] = []
        hot_n = 0
        for sess in session_days:
            sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(sess)]
            for p in peer_set[:24]:
                sub = sl[sl["Code"].astype(str).str.zfill(6) == str(p).zfill(6)]
                if sub.empty:
                    continue
                try:
                    rp = float(sub.iloc[0].get("return_pct") or 0.0)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(rp):
                    continue
                hot_peer_rets.append(rp)
                if rp + 1e-12 >= 0.15:
                    hot_n += 1
        if hot_peer_rets:
            avg_hot = sum(hot_peer_rets) / len(hot_peer_rets)
            mom = max(mom, min(1.0, max(0.0, avg_hot) / 0.10))
            ind_lim = max(ind_lim, min(1.0, hot_n / max(1, min(len(peer_set), 10))))

    return mom, ind_ov, ind_lim


def _prior_industry_hot_score(
    code: str,
    target_day: date,
    returns_ml: pd.DataFrame | None,
) -> float:
    """직전 거래일 동업종 15%+ 급등 밀도 → 0~1."""
    if returns_ml is None or returns_ml.empty:
        return 0.0
    from .. import stocks as stocks_mod

    try:
        prev = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        return 0.0
    ic = stocks_mod.industry_code_for_stock(code)
    if not ic:
        return 0.0
    peers = stocks_mod.peers_for_industry(ic)
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(prev)]
    hot = 0.0
    n = 0
    for p in peers[:32]:
        sub = sl[sl["Code"].astype(str).str.zfill(6) == str(p).zfill(6)]
        if sub.empty:
            continue
        try:
            rp = float(sub.iloc[0].get("return_pct") or 0.0)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(rp):
            continue
        n += 1
        if rp + 1e-12 >= 0.12:
            hot += min(1.0, rp / 0.30)
    if n == 0:
        return 0.0
    return min(1.0, hot / max(1, min(n, 8)))


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
    # 시장흐름(지수) 피처: KOSPI(KS11) 전일 수익률 및 최근 변동성
    ks11_ret_lag1, ks11_std5 = _ks11_market_feats(before_exclusive)
    price = _price_feats_row(ohlcv_idx, code, before_exclusive)
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
    ind_mom, ind_ov, ind_lim = _industry_feats(
        code, before_exclusive, ohlcv_idx, kw_news, returns_ml=returns_ml
    )
    investor = _investor_flow_feats_row(ohlcv_idx, code, before_exclusive)
    prior_hot = _prior_industry_hot_score(code, before_exclusive, returns_ml)
    return base + price + investor + [float(ret_vs_ks11), float(risk_off)] + ctx_part + [
        float(ind_mom),
        float(ind_ov),
        float(ind_lim),
        float(prior_hot),
    ]


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
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]] | None:
    """
    훈련 구간 거래일별 급등(양)·비급등(음) 샘플 행을 만들어 ``(X, y)`` 배열로 반환.

    표본 부족 시 ``None``.
    """
    rng = np.random.default_rng(42)
    rows_x: list[list[float]] = []
    rows_y: list[int] = []
    ohlcv_idx = _ohlcv_lookup(returns_ml)
    pos_boost = pos_boost_keys or set()
    neg_boost = neg_boost_keys or set()
    boost_cap = int(config.ML_MISS_BOOST_MAX_KEYS)
    if boost_cap > 0:
        if len(pos_boost) > boost_cap:
            pick = rng.choice(len(pos_boost), size=boost_cap, replace=False)
            pos_boost = {list(pos_boost)[i] for i in pick}
        if len(neg_boost) > boost_cap:
            pick = rng.choice(len(neg_boost), size=boost_cap, replace=False)
            neg_boost = {list(neg_boost)[i] for i in pick}
    boost_dup = max(0, int(config.ML_MISS_BOOST_DUP))
    neg_cap = min(MAX_NEG_PER_DAY, int(config.ML_TRAIN_MAX_NEG_PER_DAY))

    days = sorted(
        d
        for d in returns_ml["Date"].dt.date.unique()
        if train_start <= d < label_before_exclusive
    )
    lookback = int(config.ML_TRAIN_LOOKBACK_DAYS)
    if lookback > 0 and len(days) > lookback:
        days = days[-lookback:]
    news_ctx = make_news_ctx_bundle(
        train_events,
        news_by_calendar,
        train_start,
        label_before_exclusive,
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
            base_ret=_blended_hist_return(all_returns, code_returns.get(c6)),
            code_event_count=len(code_returns.get(c6, [])),
            today_tfidf=today_v,
            returns_ml=returns_ml,
        )

    for d in days:
        while ev_i < len(sorted_ev) and sorted_ev[ev_i].trading_day < d:
            ev = sorted_ev[ev_i]
            c6 = str(ev.code).zfill(6)
            hist_kw_by_code.setdefault(c6, set()).update(
                filter_specific_keywords(ev.news_keywords)
            )
            code_returns.setdefault(c6, []).append(ev.return_pct)
            all_returns.append(ev.return_pct)
            ev_i += 1
        blob = _early_blob_for_trading_day(news_by_calendar, d)
        if not blob.strip():
            continue
        kw_news = keyword_set(blob, k=100)
        ctx = predict.build_scoring_context(blob, train_events)
        cand_set = set(
            _day_candidate_codes(
                listing_codes,
                names,
                blob,
                ctx,
                returns_ml,
                d,
                news_ctx_bundle=news_ctx,
            )
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
        day_slice = returns_ml[returns_ml["Date"] == pd.Timestamp(d)]
        if day_slice.empty:
            continue
        pos_mask = day_slice["return_pct"] >= threshold
        pos_df = day_slice.loc[pos_mask]
        neg_df = day_slice.loc[~pos_mask]
        if pos_df.empty:
            continue

        d_iso = d.isoformat()
        for _, r in pos_df.iterrows():
            code = str(r["Code"]).zfill(6)
            in_pool = not cand_set or code in cand_set
            if not in_pool:
                # 풀 밖 급등도 학습 — 실제 적중률(리콜) 개선. 풀 내 급등보다 1회 추가 가중.
                pass
            name = str(names.get(code, r.get("Name", "")))
            fv = _train_feat(code, name, blob, kw_news, d, tw, today_v)
            rows_x.append(fv)
            rows_y.append(1)
            if not in_pool:
                rows_x.append(fv)
                rows_y.append(1)
            if boost_dup > 0 and (d_iso, code) in pos_boost:
                for _ in range(boost_dup):
                    rows_x.append(fv)
                    rows_y.append(1)

        neg_codes = [
            c
            for c in neg_df["Code"].astype(str).str.zfill(6).tolist()
            if not cand_set or c in cand_set
        ]
        n_pos = int(pos_df.shape[0])
        n_neg = min(neg_cap, max(30, 6 * n_pos))
        if len(neg_codes) > n_neg:
            neg_codes = list(rng.choice(np.array(neg_codes), size=n_neg, replace=False))
        neg_seen: set[str] = set()
        for code in neg_codes:
            name = str(names.get(code, ""))
            fv = _train_feat(code, name, blob, kw_news, d, tw, today_v)
            rows_x.append(fv)
            rows_y.append(0)
            neg_seen.add(code)
            if boost_dup > 0 and (d_iso, code) in neg_boost:
                for _ in range(boost_dup):
                    rows_x.append(fv)
                    rows_y.append(0)

        for t_iso, code6 in neg_boost:
            if t_iso != d_iso or code6 in neg_seen:
                continue
            sub = neg_df[neg_df["Code"].astype(str).str.zfill(6) == code6]
            if sub.empty:
                continue
            rr = sub.iloc[0]
            name = str(names.get(code6, rr.get("Name", "")))
            fv = _train_feat(code6, name, blob, kw_news, d, tw, today_v)
            rows_x.append(fv)
            rows_y.append(0)
            if boost_dup > 0:
                for _ in range(boost_dup):
                    rows_x.append(fv)
                    rows_y.append(0)
            neg_seen.add(code6)

    if len(rows_y) < MIN_TOTAL_SAMPLES or sum(rows_y) < MIN_POS_SAMPLES:
        return None
    return np.asarray(rows_x, dtype=np.float64), np.asarray(rows_y, dtype=np.int32), news_ctx


def _model_path(fp: str, label_before_exclusive: date | None = None) -> Path:
    """스냅샷 지문·워크포워드 컷오프(관측일 T)별 ML 랭커 joblib 캐시 경로."""
    suffix = (
        f"_{label_before_exclusive.isoformat()}" if label_before_exclusive is not None else ""
    )
    return config.TRAIN_CACHE_DIR / f"move_ranker_v{ML_MODEL_VERSION}_{fp}{suffix}.joblib"


def fit_or_load_classifier(
    *,
    train_events: list[BreakoutEvent],
    returns_ml: pd.DataFrame,
    news_by_calendar: dict[date, list[dict[str, str]]],
    listing_names: dict[str, str],
    fp: str,
    label_before_exclusive: date,
    force_retrain: bool = False,
) -> dict[str, Any] | None:
    """
    스냅샷 지문 ``fp`` 단위로 모델을 캐시에 두고, 없으면 학습 후 저장합니다.

    ``force_retrain=True`` 이면 기존 joblib 을 읽지 않고 항상 재학습해 덮어씁니다
    (``--rebuild-train-snapshot`` 과 함께 최신 ``BreakoutEvent`` 로 랭커를 맞출 때).

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
    bundle: dict[str, Any] = {
        "pipeline": None,
        "fp": fp,
        "feature_names": FEATURE_NAMES,
        "version": ML_MODEL_VERSION,
    }
    if path.is_file() and not force_retrain:
        try:
            loaded = joblib.load(path)
            if (
                isinstance(loaded, dict)
                and loaded.get("fp") == fp
                and loaded.get("version") == ML_MODEL_VERSION
                and loaded.get("pipeline") is not None
            ):
                bundle["pipeline"] = loaded["pipeline"]
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
                print(f"ML 랭커: 캐시 로드 {path.name}", flush=True)
                return bundle
        except Exception:
            pass

    pos_boost, neg_boost = snapshot_miss_diagnosis.load_ml_boost_sets_from_snapshot()
    if pos_boost or neg_boost:
        print(
            f"ML 랭커: 스냅샷 miss_diagnosis 가중 — 급등 보강 {len(pos_boost)}건·오판 보강 {len(neg_boost)}건",
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
    )
    if xy is None:
        print(
            "ML 랭커: 학습 표본 부족으로 비활성(휴리스틱만). "
            f"최소 {MIN_TOTAL_SAMPLES}행·급등 라벨 {MIN_POS_SAMPLES}건 이상 필요.",
            flush=True,
        )
        return bundle

    X, y, news_ctx_tr = xy
    from sklearn.model_selection import train_test_split

    try:
        X_tr, X_cal, y_tr, y_cal = train_test_split(
            X, y, test_size=0.18, random_state=42, stratify=y
        )
    except ValueError:
        X_tr, y_tr = X, y
        X_cal, y_cal = X[:0], y[:0]

    clf = HistGradientBoostingClassifier(
        max_depth=7,
        max_iter=175,
        learning_rate=0.065,
        random_state=42,
        class_weight="balanced",
        early_stopping=True,
        validation_fraction=0.12,
        n_iter_no_change=12,
    )
    pipe: Pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", clf),
        ]
    )
    try:
        pipe.fit(X_tr, y_tr)
    except Exception as e:
        print(f"ML 랭커: 학습 실패(휴리스틱만) — {e}", flush=True)
        return bundle
    cal_table: list[dict[str, float]] = []
    if X_cal.shape[0] > 0:
        raw_cal = pipe.predict_proba(X_cal)[:, 1]
        cal_table = _build_prob_calibration_table(raw_cal, y_cal)
    bundle["pipeline"] = pipe
    bundle["cal_base_rate"] = (
        float(y_cal.mean()) if y_cal.size > 0 else float(np.mean(y_tr))
    )
    bundle["cal_table"] = cal_table
    bundle["news_ctx"] = news_ctx_tr
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, path)
    print(
        f"ML 랭커: 학습 완료 표본 {len(y)} (급등 {int(y.sum())}건) → 저장 {path.name}",
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
    ctx = predict.build_scoring_context(news_text_blob, train_events)
    if feedback_ctx is None:
        feedback_ctx = prediction_accuracy_cache.build_feedback_context()
    kw_news, _ = ctx
    tw = theme_weights or {}
    ohlcv_idx = _ohlcv_lookup(returns_ml)
    from .. import investor_flow
    from . import prediction_ranking as pred_hybrid, prediction_ranking

    news_ctx = (ml_bundle or {}).get("news_ctx")
    score_codes = _day_candidate_codes(
        listing_codes,
        listing_names,
        news_text_blob,
        ctx,
        returns_ml,
        target_day,
        news_ctx_bundle=news_ctx,
    )
    news_only = set(
        _ml_scoring_candidate_codes(
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
    news_ctx_only: set[str] = set(score_codes) - news_only - mom_only
    flow_only = set(
        investor_flow.investor_flow_candidate_codes(
            returns_ml, target_day, listing_codes
        )
    )
    flow_only -= news_only | mom_only
    code_to_ix = {c: i for i, c in enumerate(listing_codes)}
    cand_ix: list[int] = []
    for c in score_codes:
        ix = code_to_ix.get(c)
        if ix is not None:
            cand_ix.append(ix)
    if not cand_ix:
        return []
    feats: list[list[float]] = []
    for i in cand_ix:
        code = listing_codes[i]
        name = listing_names.get(code, "")
        feats.append(
            _feat_vector(
                train_events,
                code,
                name,
                news_text_blob,
                kw_news,
                before_exclusive=target_day,
                ohlcv_idx=ohlcv_idx,
                theme_weights=tw,
                news_ctx=news_ctx,
                returns_ml=returns_ml,
            )
        )
    X = np.asarray(feats, dtype=np.float64)
    proba_raw = pipeline.predict_proba(X)[:, 1]
    cal_table = list((ml_bundle or {}).get("cal_table") or [])
    cal_base = float((ml_bundle or {}).get("cal_base_rate") or 0.01)
    proba = np.asarray(
        [
            calibrate_ml_probability(float(p), cal_table=cal_table, base_rate=cal_base)
            for p in proba_raw
        ],
        dtype=np.float64,
    )
    buf: list[predict.PredictionRow] = []
    ks11_ret, _ = _ks11_market_feats(target_day)
    from . import prediction_ranking as pred_hybrid

    for fi in range(len(cand_ix)):
        i = cand_ix[fi]
        code = listing_codes[i]
        pr = predict.prediction_row_for_code(
            code,
            listing_names,
            train_events,
            news_text_blob,
            ctx,
            int(config.PRED_ML_POOL_MIN_KEYWORD_HITS),
            feedback_ctx=feedback_ctx,
            theme_weights=tw,
            allow_momentum_only=code in mom_only and code not in news_only,
            allow_news_context_only=code in news_ctx_only,
            allow_investor_flow_only=code in flow_only,
        )
        if pr is None:
            continue
        p = float(proba[fi])
        pr.ml_prob = p
        pr.ks11_ret_lag1 = ks11_ret
        pr.momentum_score = _momentum_for_code(ohlcv_idx, code, target_day)
        ohlcv_row = _ohlcv_row_for_code(ohlcv_idx, code, target_day)
        if ohlcv_row is not None:
            pr.relative_strength_score = pred_hybrid.relative_strength_from_row(
                ohlcv_row, ks11_ret_lag1=ks11_ret
            )
            pr.vol_surge_ratio = float(ohlcv_row.get("vol_surge_ratio") or 0.0)
            pr.ret_lag1 = max(0.0, float(ohlcv_row.get("ret_lag1") or 0.0))
        try:
            row = ohlcv_row if ohlcv_row is not None else ohlcv_idx.loc[(target_day, code)]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            pr.investor_flow_score = float(row.get("investor_flow_score") or 0.0)
            pr.foreign_net_vol_ratio = float(row.get("foreign_net_vol_ratio_lag1") or 0.0)
        except (KeyError, TypeError, ValueError):
            pr.investor_flow_score = 0.0
            pr.foreign_net_vol_ratio = 0.0
        ind_mom, ind_ov, ind_lim = _industry_feats(code, target_day, ohlcv_idx, kw_news, returns_ml=returns_ml)
        pr.industry_momentum = ind_mom
        pr.industry_theme_overlap = ind_ov
        pr.industry_limit_up_heat = ind_lim
        pr.prior_industry_hot = _prior_industry_hot_score(code, target_day, returns_ml)
        if news_ctx:
            _, nctx = feats_for_code(news_ctx, news_text_blob, code)
            pr.news_context_score = nctx
        pr.reasons = [
            f"다요인 랭킹: {pred_hybrid.format_factor_summary(pr, ks11_ret_lag1=ks11_ret)} · "
            f"ML {p * 100:.1f}% · 키워드 {pr.keyword_hits} · "
            f"익일 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) 추정."
        ] + list(pr.reasons)
        buf.append(pr)

    buf.sort(key=lambda r: -pred_hybrid.recall_presort_score(r))
    eval_k = max(config.PRED_EVAL_HIT_AT_K) if config.PRED_EVAL_HIT_AT_K else 40
    finalize_n = max(
        int(top_n),
        int(config.PRED_ML_FINALIZE_POOL_N),
        int(eval_k),
    )
    pre_pool = buf[: min(len(buf), finalize_n)]

    out = prediction_ranking.finalize_ranked_predictions(
        pre_pool,
        target_day=target_day,
        ks11_ret_lag1=ks11_ret,
        forward_observation=forward_observation,
        returns_ml=returns_ml,
    )
    out = out[:top_n]
    if config.PRED_USE_DISPLAY_RANK_MAPPING and config.PRED_ERROR_FEEDBACK_ENABLED:
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


# --- news context ML (from news_context_ml.py) ---

VOCAB_SIZE = 56  # TF-IDF 상위 용어 수(학습·추론 공통)
_CONTEXT_FEAT_NAMES = (
    "news_cos_code",
    "news_cos_global",
    "news_dot_code",
    "news_lift",
    "news_today_norm",
)


def _early_blob(news_by: dict[date, list[dict[str, str]]], d: date) -> str:
    """거래일 ``d`` 의 early 뉴스 텍스트 blob(14:30 컷오프 또는 전체 윈도우)."""
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by, d)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(d)
    from . import predict

    return predict.aggregate_news_for_window(news_by, ws, we)


def build_vocab_idf(
    news_by: dict[date, list[dict[str, str]]],
    days: list[date],
) -> tuple[list[str], np.ndarray, np.ndarray]:
    """훈련 구간 전체 뉴스로 어휘·IDF·급등일 term lift."""
    tf_counter: Counter[str] = Counter()
    doc_freq: Counter[str] = Counter()
    n_docs = 0
    for d in days:
        blob = _early_blob(news_by, d)
        if not blob.strip():
            continue
        toks = filter_specific_keywords(keyword_set(blob, k=280))
        if not toks:
            continue
        n_docs += 1
        tc = Counter(toks)
        tf_counter.update(tc)
        for t in set(toks):
            doc_freq[t] += 1
    vocab = [w for w, _ in tf_counter.most_common(VOCAB_SIZE)]
    if not vocab:
        vocab = ["__pad__"]
    idf = np.array(
        [math.log((1.0 + n_docs) / (1.0 + doc_freq.get(w, 0))) + 1.0 for w in vocab],
        dtype=np.float64,
    )
    lift = np.zeros(len(vocab), dtype=np.float64)
    return vocab, idf, lift


def enrich_vocab_with_breakouts(
    vocab: list[str],
    idf: np.ndarray,
    lift: np.ndarray,
    train_events: list[BreakoutEvent],
    news_by: dict[date, list[dict[str, str]]],
    before_exclusive: date,
) -> np.ndarray:
    """급등 이벤트 당일 뉴스로 term lift 재계산."""
    idx = {w: i for i, w in enumerate(vocab)}
    breakout_tf: Counter[str] = Counter()
    global_tf: Counter[str] = Counter()
    for e in train_events:
        if e.trading_day >= before_exclusive:
            continue
        blob = _early_blob(news_by, e.trading_day)
        toks = filter_specific_keywords(keyword_set(blob, k=280))
        breakout_tf.update(toks)
        global_tf.update(toks)
    out = lift.copy()
    for w, i in idx.items():
        g = global_tf.get(w, 0) / max(1.0, sum(global_tf.values()))
        b = breakout_tf.get(w, 0) / max(1.0, sum(breakout_tf.values()))
        out[i] = math.log1p(max(0.0, b / max(g, 1e-8)))
    return out


def tfidf_vector(
    blob: str, vocab: list[str], idf: np.ndarray
) -> np.ndarray:
    """뉴스 blob → L2 정규화 TF-IDF 벡터(어휘 크기 ``len(vocab)``)."""
    idx = {w: i for i, w in enumerate(vocab)}
    tf = Counter(filter_specific_keywords(keyword_set(blob, k=280)))
    v = np.zeros(len(vocab), dtype=np.float64)
    for w, c in tf.items():
        j = idx.get(w)
        if j is not None:
            v[j] = float(c)
    v = v * idf
    n = float(np.linalg.norm(v))
    if n > 1e-12:
        v /= n
    return v


def _profiles(
    train_events: list[BreakoutEvent],
    news_by: dict[date, list[dict[str, str]]],
    vocab: list[str],
    idf: np.ndarray,
    before_exclusive: date,
) -> tuple[dict[str, np.ndarray], np.ndarray]:
    """급등일 뉴스 TF-IDF 평균 → 종목별 프로필·전체 급등일 global centroid."""
    by_code: dict[str, list[np.ndarray]] = defaultdict(list)
    all_vecs: list[np.ndarray] = []
    for e in train_events:
        if e.trading_day >= before_exclusive:
            continue
        blob = _early_blob(news_by, e.trading_day)
        if not blob.strip():
            continue
        vec = tfidf_vector(blob, vocab, idf)
        by_code[e.code].append(vec)
        all_vecs.append(vec)
    prof = {c: np.mean(vs, axis=0) for c, vs in by_code.items() if vs}
    if all_vecs:
        global_c = np.mean(all_vecs, axis=0)
        gn = float(np.linalg.norm(global_c))
        if gn > 1e-12:
            global_c /= gn
    else:
        global_c = np.zeros(len(vocab), dtype=np.float64)
    return prof, global_c


def context_feature_vector(
    today_vec: np.ndarray,
    code_prof: np.ndarray,
    global_c: np.ndarray,
    lift: np.ndarray,
) -> list[float]:
    """
    ML 랭커용 뉴스 맥락 피처 5종.

    Returns:
        ``[news_cos_code, news_cos_global, news_dot_code, news_lift, news_today_norm]``
    """
    cos_code = float(np.dot(today_vec, code_prof))
    cos_glob = float(np.dot(today_vec, global_c))
    dot_code = float(np.sum(today_vec * code_prof))
    lift_sc = float(np.dot(today_vec, lift))
    norm = float(np.linalg.norm(today_vec))
    return [cos_code, cos_glob, dot_code, lift_sc, norm]


def context_score_from_feats(feats: list[float]) -> float:
    """0~1 뉴스 맥락 적합도(종목별 과거 급등일 뉴스 패턴 유사도 중심)."""
    if len(feats) < 5:
        return 0.0
    cos_c, cos_g, dot_c, lift, _norm = feats
    if cos_c + 1e-12 < 0.10:
        return max(0.0, min(0.22, 0.18 * max(0.0, cos_g)))
    s = (
        0.62 * max(0.0, cos_c)
        + 0.18 * min(1.0, max(0.0, lift) / 2.2)
        + 0.12 * min(1.0, max(0.0, dot_c))
        + 0.08 * max(0.0, cos_g)
    )
    return max(0.0, min(1.0, s))


def affinity_candidate_codes(
    listing_codes: list[str],
    profiles: dict[str, np.ndarray],
    today_vec: np.ndarray,
    *,
    top_k: int = 100,
    min_score: float = 0.26,
) -> list[str]:
    """당일 뉴스와 과거 급등 프로필 유사도가 높은 종목 — 키워드 교집합 없이도 ML 풀에 편입."""
    allowed = {str(c).zfill(6) for c in listing_codes}
    scored: list[tuple[float, str]] = []
    for code in allowed:
        prof = profiles.get(code)
        if prof is None:
            continue
        feats = context_feature_vector(today_vec, prof, np.zeros_like(today_vec), np.zeros_like(today_vec))
        sc = context_score_from_feats(feats)
        if sc + 1e-12 >= min_score:
            scored.append((sc, code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [c for _, c in scored[:top_k]]


def make_news_ctx_bundle(
    train_events: list[BreakoutEvent],
    news_by: dict[date, list[dict[str, str]]],
    train_start: date,
    before_exclusive: date,
) -> dict[str, Any]:
    """T 직전 구간 기준 뉴스 맥락 bundle — ML 학습·추론·joblib 캐시에 공통 저장."""
    days = sorted(
        d for d in news_by if train_start <= d < before_exclusive
    )
    vocab, idf, lift = build_vocab_idf(news_by, days)
    lift = enrich_vocab_with_breakouts(
        vocab, idf, lift, train_events, news_by, before_exclusive
    )
    profiles, global_c = _profiles(
        train_events, news_by, vocab, idf, before_exclusive
    )
    return {
        "vocab": vocab,
        "idf": idf,
        "lift": lift,
        "profiles": profiles,
        "global_c": global_c,
    }


def feats_for_code(
    bundle: dict[str, Any],
    blob: str,
    code: str,
) -> tuple[list[float], float]:
    """bundle·당일 뉴스 blob·종목코드 → (ML 피처 5종, 0~1 맥락 점수)."""
    vocab = bundle["vocab"]
    idf = bundle["idf"]
    lift = bundle["lift"]
    profiles: dict[str, np.ndarray] = bundle["profiles"]
    global_c: np.ndarray = bundle["global_c"]
    today = tfidf_vector(blob, vocab, idf)
    prof = profiles.get(str(code).zfill(6))
    if prof is None:
        prof = np.zeros(len(vocab), dtype=np.float64)
    feats = context_feature_vector(today, prof, global_c, lift)
    return feats, context_score_from_feats(feats)