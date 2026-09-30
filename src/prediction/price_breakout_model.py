"""
가격·거래량 + 업종 흐름 기반 익일 20%↑ 랭커.

``scripts/signal_study.py`` walk-forward(2025-08~2026-09, 281거래일)에서
뉴스·테마 단독보다 상위 5종 적중률이 크게 높았던 신호군만 쓴다.
관측일 T 예측에는 T 직전 거래일 종가까지의 일봉만 쓴다.
"""
from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

from .. import config
from .predict import PredictionRow

FEATURE_COLS = [
    "r1", "r5", "r20", "vol_ratio", "turnover", "close_pos", "upper_wick", "dist_hi20",
    "dist_hi120", "n_big60", "volat20", "logp", "limit_up",
    "ind_big", "ind_big_share", "ind_ret", "ind_big5", "ind_ret5", "ind_ret_rank",
    "mkt_ret", "mkt_big", "rel_ind",
]
_LIMIT = 0.31
_MIN_POSITIVES = 200

# 원본 DataFrame 참조를 같이 보관해 id 재사용으로 캐시가 엇갈리지 않게 한다.
_raw_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
_model_cache: dict[
    str,
    tuple[pd.DataFrame, pd.Timestamp, HistGradientBoostingClassifier, HistGradientBoostingRegressor],
] = {}
_RET_SAMPLE = 0.20


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
    df["r20"] = df["Close"] / g["Close"].shift(20) - 1.0
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
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
    _raw_cache["raw"] = (returns, df)
    return df


def _training_pairs(raw: pd.DataFrame) -> pd.DataFrame:
    """전일 피처(X) → 당일 20%↑(y). 라벨일(``Date``) 기준."""
    g = raw.groupby("Code", group_keys=False)
    x = g[FEATURE_COLS].shift(1)
    prev_valid = g["valid"].shift(1, fill_value=False).astype(bool)
    out = pd.concat([raw[["Date", "Code", "big", "valid"]], raw["return_pct"].rename("y_ret"), x], axis=1)
    return out[out["valid"] & prev_valid & out["r1"].notna()]


def _fit_return(pairs: pd.DataFrame, before: pd.Timestamp) -> HistGradientBoostingRegressor:
    """전일 피처 → 당일 종가 수익률. 균등 표본이라 예측값이 그대로 기대 수익률이다."""
    tr = pairs[pairs["Date"] < before]
    rng = np.random.default_rng(1)
    tr = tr[rng.random(len(tr)) < _RET_SAMPLE]
    reg = HistGradientBoostingRegressor(
        max_iter=200, learning_rate=0.05, max_leaf_nodes=31,
        min_samples_leaf=200, l2_regularization=1.0, random_state=0,
    )
    reg.fit(tr[FEATURE_COLS].to_numpy(dtype=float), tr["y_ret"].to_numpy(dtype=float))
    return reg


def _fit(pairs: pd.DataFrame, before: pd.Timestamp) -> HistGradientBoostingClassifier | None:
    tr = pairs[pairs["Date"] < before]
    if tr["big"].sum() < _MIN_POSITIVES:
        return None
    rng = np.random.default_rng(0)
    keep = (tr["big"] == 1) | (rng.random(len(tr)) < float(config.PRED_PRICE_MODEL_NEG_SAMPLE))
    tr = tr[keep]
    clf = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.06, max_leaf_nodes=31,
        min_samples_leaf=80, l2_regularization=1.0, random_state=0,
    )
    clf.fit(tr[FEATURE_COLS].to_numpy(dtype=float), tr["big"].to_numpy())
    return clf


def _true_prob(p: np.ndarray) -> np.ndarray:
    """음성 샘플링으로 부풀려진 확률을 원래 비율로 되돌림."""
    r = float(config.PRED_PRICE_MODEL_NEG_SAMPLE)
    odds = p / np.clip(1.0 - p, 1e-9, None) * r
    return odds / (1.0 + odds)


def predict_top(
    returns: pd.DataFrame,
    target_day: date,
    *,
    listing_codes: list[str] | set[str] | None,
    listing_names: dict[str, str],
    top_n: int,
) -> list[PredictionRow]:
    """관측일 ``target_day`` 의 상위 ``top_n`` 종목(직전 거래일 종가까지 정보만 사용)."""
    raw = build_raw_features(returns)
    T = pd.Timestamp(target_day)
    hist = raw[raw["Date"] < T]
    if hist.empty:
        return []
    asof = hist["Date"].max()
    hit = _model_cache.get("model")
    if hit is not None and hit[0] is returns and hit[1] == asof:
        clf, reg = hit[2], hit[3]
    else:
        pairs = _training_pairs(raw)
        before = asof + pd.Timedelta(days=1)
        clf = _fit(pairs, before=before)
        if clf is None:
            return []
        reg = _fit_return(pairs, before=before)
        _model_cache["model"] = (returns, asof, clf, reg)

    # 전일 하락 종목은 제외: walk-forward 에서 적중 기여 거의 0, 정리매매·급락주가 섞여 익일 평균 -9%.
    cur = hist[(hist["Date"] == asof) & hist["valid"] & (hist["r1"] >= 0)]
    if listing_codes:
        allowed = {str(c).zfill(6) for c in listing_codes}
        cur = cur[cur["Code"].isin(allowed)]
    if cur.empty:
        return []
    X = cur[FEATURE_COLS].to_numpy(dtype=float)
    p = _true_prob(clf.predict_proba(X)[:, 1])
    er = np.clip(reg.predict(X), -_LIMIT, _LIMIT)
    cur = cur.assign(prob=p, exp_ret=er).sort_values("prob", ascending=False).head(int(top_n))

    rows: list[PredictionRow] = []
    for pos, r in enumerate(cur.itertuples(), start=1):
        prob = float(r.prob)
        exp_pct = float(r.exp_ret) * 100.0
        vol_x = float(np.expm1(r.vol_ratio)) if pd.notna(r.vol_ratio) else 0.0
        reason = (
            f"가격·거래량 모델: 예측 상승률 {exp_pct:+.1f}% · 익일 20%↑ 확률 {prob:.0%} · "
            f"전일 {float(r.r1) * 100:+.1f}% · "
            f"거래량 20일 평균의 {vol_x:.1f}배 · 같은 업종 전일 20%↑ {int(r.ind_big or 0)}종 "
            f"(과거 281거래일 검증 상위 5종 적중률 약 20%. 종가 기준 적중이며 시가 매수 시 평균 손실)"
        )
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
            rank_score=prob,
            rank_position=pos,
            confidence_tier="high",
            pred_source="price_model",
        )
        rows.append(row)
    return rows
