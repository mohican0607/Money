"""
가격·거래량 + 중장기 추세 + 업종·시장 레짐 + 뉴스 강도 기반 익일 20%↑ 랭커.

단순 전일 등락 규칙이 아니라, 다기간 추세·KOSPI/나스닥 흐름·종목·업종 뉴스 강도를
학습 피처로 넣어 모델이 맥락을 보게 한다.
관측일 T 예측의 가격 피처는 T 직전 거래일 종가까지, 뉴스 맥락은 T 장전(캐시)까지.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from email.utils import parsedate_to_datetime

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

from .. import config
from .predict import PredictionRow

FEATURE_COLS = [
    # 단기·중장기 추세
    "r1", "r5", "r10", "r20", "r60", "ret_cum3", "ret_roll_mean5", "ret_max5",
    "ret_min5", "n_crash5", "close_ma20_ratio", "trend_slope20",
    "vol_ratio", "turnover", "close_pos", "upper_wick", "dist_hi20",
    "dist_hi120", "n_big60", "volat20", "logp", "limit_up",
    # 업종·시장 횡단면
    "ind_big", "ind_big_share", "ind_ret", "ind_big5", "ind_ret5", "ind_ret_rank",
    "mkt_ret", "mkt_big", "rel_ind",
    # 국내·국제 레짐 (일별, 전일 종가 기준 — training 시 shift(1))
    "ks11_ret", "ks11_vol5", "ks11_risk_off", "ixic_ret", "ixic_vol5",
]
# 장전 뉴스 맥락 — 라벨일/관측일 T 기준(shift 하지 않음). 캐시만 사용.
CONTEXT_FEATURE_COLS = ["news_n", "news_5d", "news_surge", "ind_news"]
# 매수 가능(전일 상한가 아님) 전용 모델 추가 피처. 상한가 제외 후보 walk-forward(2025-10~2026-10, 246일)
# 상위 5종 적중: 기본 피처·전체 학습 72건 → 상한가 제외 학습+추가 피처 84건.
EXTRA_FEATURE_COLS = [
    "r1_prev", "lu_streak", "n_lu20", "gap", "close_at_high", "vol_ratio_prev", "r2",
    "ret_max4_prev",
]
BUYABLE_FEATURE_COLS = FEATURE_COLS + EXTRA_FEATURE_COLS + CONTEXT_FEATURE_COLS
MODEL_FEATURE_COLS = FEATURE_COLS + CONTEXT_FEATURE_COLS
_LIMIT = 0.31
_MIN_POSITIVES = 200

# 원본 DataFrame 참조를 같이 보관해 id 재사용으로 캐시가 엇갈리지 않게 한다.
_raw_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
_model_cache: dict[
    str,
    tuple[
        pd.DataFrame,
        pd.Timestamp,
        HistGradientBoostingClassifier,
        HistGradientBoostingClassifier,
        HistGradientBoostingRegressor,
    ],
] = {}
_RET_SAMPLE = 0.20
# 전일 상한가 별도표시: 연속 상한은 익일 급락이 잦아 제외.
_LU_EXTRA_MAX_STREAK = 1
# 단일 상한가라도 기대수익률을 상한가처럼 보이지 않게 보수 상한(%).
_LU_EXTRA_DISPLAY_PCT_CAP = 12.0
# 메인 고확신: 절대 확률이 이보다 낮으면 mid 로만 표기 (10/19: 7~10%를 고확신으로 달아 오인).
_HIGH_TIER_MIN_PROB = 0.15
# 최근 표본을 더 세게 학습(반감기, 달력일).
_RECENCY_HALF_LIFE_DAYS = 60.0
# 급락·잔열 오탐 / 잔잔한 급등 적중을 가중.
_HARD_NEG_WEIGHT = 4.0
_QUIET_POS_WEIGHT = 4.0


def _industry_map() -> dict[str, str]:
    p = config.LISTING_META_CACHE_DIR / "stock_industry_code.json"
    try:
        return {str(k).zfill(6): str(v or "na") for k, v in json.loads(p.read_text(encoding="utf-8")).items()}
    except (OSError, json.JSONDecodeError):
        return {}


def build_raw_features(returns: pd.DataFrame) -> pd.DataFrame:
    """(Code, Date) 행마다 그날 종가까지로 계산한 피처 + 그날 20%↑ 여부(``big``)."""
    hit = _raw_cache.get("raw")
    if hit is not None and hit[0] is returns:
        return hit[1]
    df = returns[["Date", "Code", "Name", "Open", "High", "Low", "Close", "Volume", "return_pct"]].copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    for c in ("Open", "High", "Low", "Close", "Volume", "return_pct"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
    ind_map = _industry_map()
    df["ind"] = df["Code"].map(lambda c: ind_map.get(c, "na"))

    g = df.groupby("Code", group_keys=False)
    prev_vol = g["Volume"].shift(1)
    traded = (df["Volume"] > 0) & (prev_vol > 0)
    df["valid"] = traded & (df["return_pct"].abs() <= _LIMIT) & (df["Open"] > 0)
    df["big"] = (df["valid"] & (df["return_pct"] >= config.BIG_MOVE_THRESHOLD)).astype(int)

    df["r1"] = df["return_pct"]
    df["r5"] = df["Close"] / g["Close"].shift(5) - 1.0
    df["r10"] = df["Close"] / g["Close"].shift(10) - 1.0
    df["r20"] = df["Close"] / g["Close"].shift(20) - 1.0
    df["r60"] = df["Close"] / g["Close"].shift(60) - 1.0
    r1s = g["r1"]
    df["ret_cum3"] = (1.0 + df["r1"]) * (1.0 + r1s.shift(1).fillna(0.0)) * (
        1.0 + r1s.shift(2).fillna(0.0)
    ) - 1.0
    df["ret_roll_mean5"] = g["return_pct"].transform(lambda s: s.rolling(5, min_periods=3).mean())
    df["ret_max5"] = g["return_pct"].transform(lambda s: s.rolling(5, min_periods=3).max())
    df["ret_min5"] = g["return_pct"].transform(lambda s: s.rolling(5, min_periods=3).min())
    df["n_crash5"] = g["r1"].transform(
        lambda s: (s <= -0.15).rolling(5, min_periods=1).sum()
    )
    df["ret_max4_prev"] = g["r1"].transform(
        lambda s: s.shift(1).rolling(4, min_periods=1).max()
    )
    ma20 = g["Close"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    df["close_ma20_ratio"] = df["Close"] / ma20 - 1.0
    # 20일 종가 기울기(로그 회귀 근사): 최근 구간 상대 위치
    df["trend_slope20"] = (df["Close"] / g["Close"].shift(20) - 1.0) / 20.0
    vavg = g["Volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    df["vol_ratio"] = np.log1p(df["Volume"] / (vavg + 1.0))
    df["turnover"] = np.log1p(df["Close"] * df["Volume"])
    rng = (df["High"] - df["Low"]).replace(0, np.nan)
    df["close_pos"] = ((df["Close"] - df["Low"]) / rng).fillna(0.5)
    df["upper_wick"] = ((df["High"] - df[["Open", "Close"]].max(axis=1)) / df["Close"]).fillna(0)
    df["dist_hi20"] = df["Close"] / g["High"].transform(lambda s: s.rolling(20, min_periods=5).max()) - 1.0
    df["dist_hi120"] = df["Close"] / g["High"].transform(lambda s: s.rolling(120, min_periods=20).max()) - 1.0
    df["n_big60"] = g["big"].transform(lambda s: s.rolling(60, min_periods=1).sum())
    df["volat20"] = g["return_pct"].transform(lambda s: s.rolling(20, min_periods=5).std())
    df["logp"] = np.log(df["Close"].clip(lower=1))
    df["limit_up"] = (df["return_pct"] >= 0.29).astype(int)
    df["r1_prev"] = g["r1"].shift(1)
    df["r2"] = df["Close"] / g["Close"].shift(2) - 1.0
    lu = df["limit_up"]
    run_id = (lu != g["limit_up"].shift(1).fillna(0).astype(int)).cumsum()
    df["lu_streak"] = lu.groupby([df["Code"], run_id]).cumsum() * lu
    df["n_lu20"] = g["limit_up"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    df["gap"] = (df["Open"] / g["Close"].shift(1) - 1.0).fillna(0)
    df["close_at_high"] = (df["Close"] >= df["High"] * 0.999).astype(int)
    df["vol_ratio_prev"] = g["vol_ratio"].shift(1)

    day_ind = df.groupby(["Date", "ind"]).agg(
        ind_n=("Code", "size"), ind_big=("big", "sum"), ind_ret=("return_pct", "mean")
    ).reset_index()
    day_ind["ind_big_share"] = day_ind["ind_big"] / day_ind["ind_n"]
    day_ind = day_ind.sort_values(["ind", "Date"])
    gi = day_ind.groupby("ind", group_keys=False)
    day_ind["ind_big5"] = gi["ind_big"].transform(lambda s: s.rolling(5, min_periods=1).sum())
    day_ind["ind_ret5"] = gi["ind_ret"].transform(lambda s: s.rolling(5, min_periods=1).mean())
    day_ind["ind_ret_rank"] = day_ind.groupby("Date")["ind_ret"].rank(pct=True)
    df = df.merge(
        day_ind[["Date", "ind", "ind_big", "ind_big_share", "ind_ret", "ind_big5", "ind_ret5", "ind_ret_rank"]],
        on=["Date", "ind"],
        how="left",
    )
    df["rel_ind"] = df["r1"] - df["ind_ret"]
    mkt = df.groupby("Date").agg(mkt_ret=("return_pct", "mean"), mkt_big=("big", "sum")).reset_index()
    df = df.merge(mkt, on="Date", how="left")
    df = _attach_index_regime(df)
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
    _raw_cache["raw"] = (returns, df)
    return df


_news_cache: dict[str, pd.DataFrame] = {}
_index_cache: dict[str, pd.DataFrame] = {}


def _index_ret_frame(symbol: str, start: date, end: date) -> pd.DataFrame:
    """지수 일별 수익률·5일 변동성. 실패 시 빈 표."""
    key = f"{symbol}:{start}:{end}"
    hit = _index_cache.get(key)
    if hit is not None:
        return hit
    try:
        from .. import stocks as stocks_mod

        raw = stocks_mod.load_index_frame(symbol, start, end)
    except Exception:
        raw = pd.DataFrame()
    if raw is None or raw.empty or "Close" not in getattr(raw, "columns", []):
        out = pd.DataFrame(columns=["Date", "idx_ret", "idx_vol5"])
        _index_cache[key] = out
        return out
    s = raw.sort_values("Date").copy()
    s["Date"] = pd.to_datetime(s["Date"]).dt.normalize()
    s["idx_ret"] = pd.to_numeric(s["Close"], errors="coerce").pct_change()
    s["idx_vol5"] = s["idx_ret"].rolling(5, min_periods=3).std()
    out = s[["Date", "idx_ret", "idx_vol5"]].dropna(subset=["Date"])
    _index_cache[key] = out
    return out


def _attach_index_regime(df: pd.DataFrame) -> pd.DataFrame:
    """KOSPI·나스닥 일별 흐름을 모든 종목 행에 붙인다(국제·국내 레짐)."""
    if df.empty:
        for c in ("ks11_ret", "ks11_vol5", "ks11_risk_off", "ixic_ret", "ixic_vol5"):
            df[c] = 0.0
        return df
    d0 = df["Date"].min().date() - pd.Timedelta(days=120)
    d1 = df["Date"].max().date() + pd.Timedelta(days=2)
    soft = float(getattr(config, "PRED_REGIME_KS11_SOFT_MIN", -0.01) or -0.01)
    ks = _index_ret_frame("KS11", d0, d1).rename(
        columns={"idx_ret": "ks11_ret", "idx_vol5": "ks11_vol5"}
    )
    ix = _index_ret_frame("IXIC", d0, d1).rename(
        columns={"idx_ret": "ixic_ret", "idx_vol5": "ixic_vol5"}
    )
    out = df.merge(ks, on="Date", how="left").merge(ix, on="Date", how="left")
    for c in ("ks11_ret", "ks11_vol5", "ixic_ret", "ixic_vol5"):
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    out["ks11_risk_off"] = (out["ks11_ret"] < soft).astype(float)
    return out


def _load_news_counts() -> pd.DataFrame:
    """캐시 뉴스 → (Code, NewsDay) 건수. NewsDay = 장전 뉴스가 귀속되는 예측일."""
    hit = _news_cache.get("counts")
    if hit is not None:
        return hit
    from .. import news as news_mod

    counts: dict[tuple[str, pd.Timestamp], int] = defaultdict(int)
    base = news_mod._CACHE_NEWS / "naver_both"
    if base.is_dir():
        for f in sorted(base.rglob("day_*.json")):
            try:
                rows = json.loads(f.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError):
                continue
            if not isinstance(rows, list):
                continue
            for r in rows:
                code = str(r.get("stock_code") or "").zfill(6)
                if not code.strip("0"):
                    continue
                try:
                    pub = parsedate_to_datetime(str(r.get("pub")))
                except (TypeError, ValueError, IndexError):
                    continue
                if pub is None:
                    continue
                pub = pub.replace(tzinfo=None)
                day = pd.Timestamp(pub.date())
                if pub.hour >= 9:
                    day = day + pd.Timedelta(days=1)
                counts[(code, day)] += 1
    out = pd.DataFrame(
        [(c, d, n) for (c, d), n in counts.items()],
        columns=["Code", "NewsDay", "news_n"],
    )
    _news_cache["counts"] = out
    return out


def _attach_news_context(df: pd.DataFrame, *, date_col: str = "Date") -> pd.DataFrame:
    """라벨일/관측일 행에 장전 뉴스 강도·업종 뉴스 열기를 붙인다."""
    out = df.copy()
    if "ind" not in out.columns and "Code" in out.columns:
        ind_map = _industry_map()
        out["ind"] = out["Code"].map(lambda c: ind_map.get(str(c).zfill(6), "na"))
    news = _load_news_counts()
    if news.empty:
        for c in CONTEXT_FEATURE_COLS:
            out[c] = 0.0
        return out
    tmp = out[[date_col, "Code"]].copy()
    tmp["Code"] = tmp["Code"].astype(str).str.zfill(6)
    tmp["_i"] = np.arange(len(tmp))
    merged = tmp.merge(
        news.rename(columns={"NewsDay": date_col}),
        on=["Code", date_col],
        how="left",
    )
    merged["news_n"] = merged["news_n"].fillna(0.0)
    merged = merged.sort_values(["Code", date_col])
    merged["news_5d"] = (
        merged.groupby("Code")["news_n"]
        .transform(lambda s: s.shift(1).rolling(5, min_periods=1).sum())
        .fillna(0.0)
    )
    merged["news_surge"] = merged["news_n"] / (merged["news_5d"] / 5.0 + 0.5)
    if "ind" in out.columns:
        ind = out[["ind"]].copy()
        ind["_i"] = np.arange(len(ind))
        merged = merged.merge(ind, on="_i", how="left")
        ind_news = (
            merged.groupby([date_col, "ind"], dropna=False)["news_n"].sum().rename("ind_news").reset_index()
        )
        merged = merged.merge(ind_news, on=[date_col, "ind"], how="left")
    else:
        merged["ind_news"] = 0.0
    merged["ind_news"] = merged["ind_news"].fillna(0.0)
    merged = merged.sort_values("_i")
    for c in CONTEXT_FEATURE_COLS:
        out[c] = merged[c].to_numpy(dtype=float)
    return out


def _training_pairs(raw: pd.DataFrame) -> pd.DataFrame:
    """전일 가격·레짐 피처 + 당일 장전 뉴스 맥락 → 당일 20%↑(y)."""
    g = raw.groupby("Code", group_keys=False)
    price_cols = list(dict.fromkeys(FEATURE_COLS + EXTRA_FEATURE_COLS))
    x = g[price_cols].shift(1)
    prev_valid = g["valid"].shift(1, fill_value=False).astype(bool)
    out = pd.concat(
        [raw[["Date", "Code", "ind", "big", "valid"]], raw["return_pct"].rename("y_ret"), x],
        axis=1,
    )
    out = out[out["valid"] & prev_valid & out["r1"].notna()]
    return _attach_news_context(out, date_col="Date")


def _sample_weights(tr: pd.DataFrame, before: pd.Timestamp) -> np.ndarray:
    """최근성 + 급락/잔열 오탐 + 잔잔한 급등 적중 가중.

    10/22처럼 급락 직후·잔열 종목을 고른 오탐, 서한·에이전트AI처럼 전일이 잔잔한
    적중을 같은 규칙으로 지우지 않고 학습 신호로 넣는다.
    """
    age = (pd.Timestamp(before) - pd.to_datetime(tr["Date"])).dt.days.to_numpy(dtype=float)
    w = np.power(0.5, np.clip(age, 0.0, None) / _RECENCY_HALF_LIFE_DAYS)
    big = pd.to_numeric(tr["big"], errors="coerce").fillna(0).to_numpy(dtype=float)
    r1 = (
        pd.to_numeric(tr["r1"], errors="coerce").fillna(0).to_numpy(dtype=float)
        if "r1" in tr.columns
        else np.zeros(len(tr), dtype=float)
    )
    rmax = (
        pd.to_numeric(tr["ret_max5"], errors="coerce").fillna(0).to_numpy(dtype=float)
        if "ret_max5" in tr.columns
        else np.zeros(len(tr), dtype=float)
    )
    rmin = (
        pd.to_numeric(tr["ret_min5"], errors="coerce").fillna(0).to_numpy(dtype=float)
        if "ret_min5" in tr.columns
        else np.zeros(len(tr), dtype=float)
    )
    hard_neg = (big < 0.5) & (
        (r1 <= -0.08) | (rmin <= -0.20) | (rmax >= 0.20) | (r1 >= 0.10)
    )
    quiet_pos = (big >= 0.5) & (np.abs(r1) < 0.08) & (rmax < 0.20) & (rmin > -0.20)
    w = np.where(hard_neg, w * _HARD_NEG_WEIGHT, w)
    w = np.where(quiet_pos, w * _QUIET_POS_WEIGHT, w)
    return np.clip(w, 1e-6, None)


def _fit_return(
    pairs: pd.DataFrame,
    before: pd.Timestamp,
    *,
    feats: list[str] | None = None,
) -> HistGradientBoostingRegressor:
    """전일 피처 → 당일 종가 기대수익률(제곱오차). 낙관 분위가 아니라 평균을 맞춘다.

    급등일은 모두, 나머지는 ``_RET_SAMPLE`` 만 뽑고 가중치로 원래 분포를 되돌린다.
    """
    tr = pairs[pairs["Date"] < before]
    rng = np.random.default_rng(1)
    keep = (tr["big"] == 1) | (rng.random(len(tr)) < _RET_SAMPLE)
    tr = tr[keep]
    w = np.where(tr["big"] == 1, 1.0, 1.0 / _RET_SAMPLE) * _sample_weights(tr, before)
    use = [c for c in (feats or BUYABLE_FEATURE_COLS) if c in tr.columns]
    reg = HistGradientBoostingRegressor(
        loss="squared_error", max_iter=200, learning_rate=0.05,
        max_leaf_nodes=31, min_samples_leaf=80, random_state=0,
    )
    reg.fit(tr[use].to_numpy(dtype=float), tr["y_ret"].to_numpy(dtype=float), sample_weight=w)
    return reg


def _fit(
    pairs: pd.DataFrame,
    before: pd.Timestamp,
    *,
    feats: list[str] | None = None,
    buyable_only: bool = False,
) -> HistGradientBoostingClassifier | None:
    """``buyable_only``: 전일 상한가가 아닌 표본만 학습(``limit_up`` 은 전일 피처)."""
    feats = [c for c in (feats or MODEL_FEATURE_COLS) if c in pairs.columns]
    tr = pairs[pairs["Date"] < before]
    if buyable_only:
        tr = tr[tr["limit_up"] < 1]
    if tr["big"].sum() < _MIN_POSITIVES:
        return None
    rng = np.random.default_rng(0)
    keep = (tr["big"] == 1) | (rng.random(len(tr)) < float(config.PRED_PRICE_MODEL_NEG_SAMPLE))
    tr = tr[keep]
    w = _sample_weights(tr, before)
    clf = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.06, max_leaf_nodes=31,
        min_samples_leaf=80, l2_regularization=1.0, random_state=0,
    )
    clf.fit(tr[feats].to_numpy(dtype=float), tr["big"].to_numpy(), sample_weight=w)
    return clf


def _true_prob(p: np.ndarray) -> np.ndarray:
    """음성 샘플링으로 부풀려진 확률을 원래 비율로 되돌림."""
    r = float(config.PRED_PRICE_MODEL_NEG_SAMPLE)
    odds = p / np.clip(1.0 - p, 1e-9, None) * r
    return odds / (1.0 + odds)


def with_intraday_session(
    returns: pd.DataFrame, session_day: date, snapshot: pd.DataFrame
) -> pd.DataFrame:
    """일봉 표에 ``session_day`` 장중 스냅샷 봉을 덧붙인 사본(``return_pct`` = 스냅샷 등락률)."""
    snap = snapshot.copy()
    snap["Date"] = pd.Timestamp(session_day)
    snap["return_pct"] = pd.to_numeric(snap["Change"], errors="coerce")
    base = returns[pd.to_datetime(returns["Date"]).dt.normalize() < pd.Timestamp(session_day)]
    return pd.concat([base, snap[[c for c in snap.columns if c in base.columns or c == "return_pct"]]], ignore_index=True)


def predict_top(
    returns: pd.DataFrame,
    target_day: date,
    *,
    listing_codes: list[str] | set[str] | None,
    listing_names: dict[str, str],
    top_n: int,
    partial_last_day: bool = False,
    basis_note: str = "",
) -> list[PredictionRow]:
    """관측일 ``target_day`` 예측(직전 거래일 종가까지 정보만 사용).

    메인 ``top_n`` 종은 전일 상한가가 아닌(시가 매수 가능) 종목만 매수 가능 전용 모델로 고른다.
    전일 상한가 종목은 전체 모델 상위 ``top_n`` 에 들 때만 ``prior_limit_up`` 으로 구분해 뒤에 붙인다
    (최대 ``PRED_LIMIT_UP_EXTRA_MAX``).

    ``partial_last_day``: 마지막 날이 장중 스냅샷이면 그날 등락을 학습 라벨로 쓰지 않는다.
    """
    raw = build_raw_features(returns)
    T = pd.Timestamp(target_day)
    hist = raw[raw["Date"] < T]
    if hist.empty:
        return []
    asof = hist["Date"].max()
    hit = _model_cache.get("model")
    if hit is not None and hit[0] is returns and hit[1] == asof:
        clf, clf_buy, reg = hit[2], hit[3], hit[4]
    else:
        pairs = _training_pairs(raw)
        before = asof if partial_last_day else asof + pd.Timedelta(days=1)
        clf = _fit(pairs, before=before)
        if clf is None:
            return []
        clf_buy = _fit(pairs, before=before, feats=BUYABLE_FEATURE_COLS, buyable_only=True)
        reg = _fit_return(pairs, before=before, feats=BUYABLE_FEATURE_COLS)
        _model_cache["model"] = (returns, asof, clf, clf_buy, reg)

    cur = hist[(hist["Date"] == asof) & hist["valid"]].copy()
    if listing_codes:
        allowed = {str(c).zfill(6) for c in listing_codes}
        cur = cur[cur["Code"].isin(allowed)]
    if cur.empty:
        return []
    # 장전 뉴스 맥락은 관측일 T 기준(가격 피처는 asof=T-1).
    news_bits = _attach_news_context(
        cur.assign(Date=T)[["Date", "Code", "ind"]],
        date_col="Date",
    )
    for c in CONTEXT_FEATURE_COLS:
        cur[c] = news_bits[c].to_numpy(dtype=float)
    model_feats = [c for c in MODEL_FEATURE_COLS if c in cur.columns]
    buy_feats = [c for c in BUYABLE_FEATURE_COLS if c in cur.columns]
    X = cur[model_feats].to_numpy(dtype=float)
    exp_X = cur[buy_feats].to_numpy(dtype=float) if buy_feats else X
    cur = cur.assign(
        prob=_true_prob(clf.predict_proba(X)[:, 1]),
        exp_ret=np.clip(reg.predict(exp_X), -0.30, 0.30),
    )
    is_lu = cur["limit_up"] >= 1

    # 매수 가능 후보는 전일 하락도 포함. 급락·잔열은 규칙으로 지우지 않고 학습 확률이 낮추게 한다.
    buy = cur[~is_lu]
    if clf_buy is not None and not buy.empty:
        buy = buy.assign(
            prob=_true_prob(clf_buy.predict_proba(buy[buy_feats].to_numpy(dtype=float))[:, 1])
        )
    else:
        buy = buy[buy["r1"] >= 0]
    main = buy.sort_values("prob", ascending=False).head(int(top_n))

    lu_rows = cur.iloc[0:0]
    extra_max = max(0, int(config.PRED_LIMIT_UP_EXTRA_MAX))
    if extra_max:
        overall = cur[cur["r1"] >= 0].sort_values("prob", ascending=False).head(int(top_n))
        cand = overall[overall["limit_up"] >= 1].copy()
        streak = (
            cand["lu_streak"]
            if "lu_streak" in cand.columns
            else pd.Series(0, index=cand.index)
        )
        lu_rows = cand[streak.fillna(0) <= _LU_EXTRA_MAX_STREAK].head(extra_max)

    rows: list[PredictionRow] = []
    for pos, (r, prior_lu) in enumerate(
        [(r, False) for r in main.itertuples()] + [(r, True) for r in lu_rows.itertuples()], start=1
    ):
        prob = float(r.prob)
        exp_pct = float(r.exp_ret) * 100.0
        vol_x = float(np.expm1(r.vol_ratio)) if pd.notna(r.vol_ratio) else 0.0
        if prior_lu:
            # 낙관 분위(q=0.9) 그대로면 상한가 다음날 +20%대처럼 보여 매수 오인됨 → 보수 상한.
            raw_exp = exp_pct
            exp_pct = min(exp_pct, _LU_EXTRA_DISPLAY_PCT_CAP)
            head = "전일 상한가(별도 구분 · 시가 매수 어려움 · 익일 반락 위험): "
            verify = (
                "전일 상한가 익일 20%↑ 기준율 약 18% · 연속 상한은 표시 제외 · "
                f"표시 상승률은 기대값 {raw_exp:+.1f}%를 {_LU_EXTRA_DISPLAY_PCT_CAP:.0f}%로 상한"
            )
        else:
            head = ""
            verify = (
                "추세·레짐·뉴스 강도 학습 모델 · 최근 표본·급락/잔열 오탐·잔잔한 급등 적중을 가중 · "
                "표시 상승률은 기대수익률(평균) · 전일 상한가 제외 후보"
            )
        r10 = float(getattr(r, "r10", 0.0) or 0.0)
        r60 = float(getattr(r, "r60", 0.0) or 0.0)
        ks = float(getattr(r, "ks11_ret", 0.0) or 0.0)
        ix = float(getattr(r, "ixic_ret", 0.0) or 0.0)
        nn = float(getattr(r, "news_n", 0.0) or 0.0)
        ns = float(getattr(r, "news_surge", 0.0) or 0.0)
        ctx = (
            f" · 추세 10일 {r10 * 100:+.1f}%/60일 {r60 * 100:+.1f}%"
            f" · KOSPI {ks * 100:+.2f}% · 나스닥 {ix * 100:+.2f}%"
            f" · 장전뉴스 {nn:.0f}건(급증 {ns:.1f}x)"
        )
        reason = (
            f"{head}가격·추세·레짐·뉴스 모델: 예측 상승률 {exp_pct:+.1f}%(기대수익률) · "
            f"익일 20%↑ 확률 {prob:.0%} · 전일 {float(r.r1) * 100:+.1f}% · "
            f"거래량 20일 평균의 {vol_x:.1f}배 · 같은 업종 전일 20%↑ {int(r.ind_big or 0)}종"
            f"{ctx} ({verify}. 종가 기준 적중)"
            + (f" · {basis_note}" if basis_note else "")
        )
        if prior_lu:
            tier = "mid"
        elif prob + 1e-12 >= _HIGH_TIER_MIN_PROB:
            tier = "high"
        else:
            # 확률 절대값이 낮은데 고확신 라벨을 달면 매수 오인 (10/19 메인 7~10%).
            tier = "mid"
        row = PredictionRow(
            code=str(r.Code).zfill(6),
            name=str(listing_names.get(str(r.Code).zfill(6)) or r.Name),
            score=prob,
            predicted_return_pct=exp_pct,
            matched_keywords=[],
            reasons=[reason],
            ml_prob=prob,
            ml_rank_score=prob,
            vol_surge_ratio=vol_x,
            ret_lag1=float(r.r1),
            industry_limit_up_heat=float(r.ind_big_share or 0.0),
            news_context_score=float(min(1.0, ns / 5.0)) if ns > 0 else 0.0,
            ks11_ret_lag1=ks,
            rank_score=prob,
            rank_position=pos,
            confidence_tier=tier,
            pred_source="price_model",
            prior_limit_up=prior_lu,
        )
        rows.append(row)
    return rows
