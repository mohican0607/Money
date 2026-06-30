"""시세·지수·수급·업종 피처 추출(OHLCV 인덱스 기반)."""
from __future__ import annotations

import math
from datetime import date
from functools import lru_cache

import pandas as pd

from .. import config, trading_calendar
from .. import stocks as market_index
from . import prediction_ranking as pred_hybrid
from .news_context import early_blob_for_day

__all__ = [
    "ohlcv_lookup",
    "ks11_market_feats",
    "load_ks11_cached",
    "investor_flow_feats_row",
    "price_feats_row",
    "industry_feats",
    "prior_industry_hot_score",
    "momentum_for_code",
    "ohlcv_row_for_code",
    "blended_hist_return",
    "early_blob_for_trading_day",
]


def ohlcv_lookup(returns_ml: pd.DataFrame) -> pd.DataFrame:
    """(거래일 date, 6자리 Code) → 행 조회용 인덱스."""
    t = returns_ml.copy()
    t["_d"] = pd.to_datetime(t["Date"]).dt.normalize().dt.date
    t["_c"] = t["Code"].astype(str).str.zfill(6)
    return t.set_index(["_d", "_c"])


@lru_cache(maxsize=512)
def ks11_market_feats(trading_day: date) -> tuple[float, float]:
    """
    시장흐름 피처(지수):
    - ks11_ret_lag1: trading_day 직전 거래일의 KOSPI 수익률
    - ks11_ret_std5: 최근 5개 값 표준편차(변동성)
    """
    try:
        prev = trading_calendar.last_trading_day_before(trading_day)
    except ValueError:
        return 0.0, 0.0
    start_d = pd.Timestamp(prev - pd.Timedelta(days=90)).date()
    end_d = pd.Timestamp(prev + pd.Timedelta(days=2)).date()
    df = load_ks11_cached(start_d, end_d)
    if df is None or df.empty:
        return 0.0, 0.0
    r1 = market_index.index_daily_return_pct(df, prev)
    if r1 is None:
        r1v = 0.0
    else:
        r1v = float(r1)
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


_ks11_market_feats = ks11_market_feats


@lru_cache(maxsize=4)
def load_ks11_cached(start: date, end: date) -> pd.DataFrame:
    """KOSPI(KS11) 지수 OHLCV를 구간별로 로드(``lru_cache``)."""
    try:
        return market_index.load_index_frame("KS11", start, end)
    except Exception:
        return pd.DataFrame()


def investor_flow_feats_row(
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


def price_feats_row(idx: pd.DataFrame | None, code: str, trading_day: date) -> list[float]:
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


def blended_hist_return(
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


def industry_feats(
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


def prior_industry_hot_score(
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


def momentum_for_code(
    ohlcv_idx: pd.DataFrame | None, code: str, trading_day: date
) -> float:
    """OHLCV 인덱스에서 당일 종목 모멘텀 점수(없으면 0)."""
    if ohlcv_idx is None:
        return 0.0
    try:
        row = ohlcv_idx.loc[(trading_day, code)]
    except KeyError:
        return 0.0
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return pred_hybrid.momentum_raw_from_row(row)


def ohlcv_row_for_code(
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


def early_blob_for_trading_day(
    news_by_calendar: dict[date, list[dict[str, str]]], d: date
) -> str:
    """거래일 ``d`` 의 early 뉴스 텍스트 blob."""
    return early_blob_for_day(news_by_calendar, d)
