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
# 매수 가능(전일 상한가 아님) 전용 모델 추가 피처. 상한가 제외 후보 walk-forward(2025-10~2026-10, 246일)
# 상위 5종 적중: 기본 피처·전체 학습 72건 → 상한가 제외 학습+추가 피처 84건.
EXTRA_FEATURE_COLS = ["r1_prev", "lu_streak", "n_lu20", "gap", "close_at_high", "vol_ratio_prev", "r2"]
BUYABLE_FEATURE_COLS = FEATURE_COLS + EXTRA_FEATURE_COLS
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
# 2026-09 검증: q=0.9 예측 ≥20% 인 103건 중 실제 20%↑ 10.7% (전체 기준율 0.46%).
_RET_QUANTILE = 0.90
# 전일 상한가 별도표시: 연속 상한·2일 과열은 익일 급락이 잦아 제외.
# (2026-10-16 하이퍼코퍼레이션: 상한 연속 후 예측 +25.3% · 실제 -24.7%)
_LU_EXTRA_MAX_STREAK = 1
_LU_EXTRA_MAX_R2 = 0.40
# 단일 상한가라도 q=0.9 낙관 상승률을 그대로 매수 신호처럼 보이지 않게 보수 상한(%).
_LU_EXTRA_DISPLAY_PCT_CAP = 12.0


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
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
    _raw_cache["raw"] = (returns, df)
    return df


def _training_pairs(raw: pd.DataFrame) -> pd.DataFrame:
    """전일 피처(X) → 당일 20%↑(y). 라벨일(``Date``) 기준."""
    g = raw.groupby("Code", group_keys=False)
    x = g[list(dict.fromkeys(FEATURE_COLS + EXTRA_FEATURE_COLS))].shift(1)
    prev_valid = g["valid"].shift(1, fill_value=False).astype(bool)
    out = pd.concat([raw[["Date", "Code", "big", "valid"]], raw["return_pct"].rename("y_ret"), x], axis=1)
    return out[out["valid"] & prev_valid & out["r1"].notna()]


def _fit_return(pairs: pd.DataFrame, before: pd.Timestamp) -> HistGradientBoostingRegressor:
    """전일 피처 → 당일 종가 수익률의 상위 ``_RET_QUANTILE`` 분위(상승 시나리오 상승폭).

    급등일은 모두, 나머지는 ``_RET_SAMPLE`` 만 뽑고 가중치로 원래 분포를 되돌린다.
    """
    tr = pairs[pairs["Date"] < before]
    rng = np.random.default_rng(1)
    keep = (tr["big"] == 1) | (rng.random(len(tr)) < _RET_SAMPLE)
    tr = tr[keep]
    w = np.where(tr["big"] == 1, 1.0, 1.0 / _RET_SAMPLE)
    reg = HistGradientBoostingRegressor(
        loss="quantile", quantile=_RET_QUANTILE, max_iter=200, learning_rate=0.05,
        max_leaf_nodes=31, min_samples_leaf=200, random_state=0,
    )
    reg.fit(tr[FEATURE_COLS].to_numpy(dtype=float), tr["y_ret"].to_numpy(dtype=float), sample_weight=w)
    return reg


def _fit(
    pairs: pd.DataFrame,
    before: pd.Timestamp,
    *,
    feats: list[str] | None = None,
    buyable_only: bool = False,
) -> HistGradientBoostingClassifier | None:
    """``buyable_only``: 전일 상한가가 아닌 표본만 학습(``limit_up`` 은 전일 피처)."""
    feats = feats or FEATURE_COLS
    tr = pairs[pairs["Date"] < before]
    if buyable_only:
        tr = tr[tr["limit_up"] < 1]
    if tr["big"].sum() < _MIN_POSITIVES:
        return None
    rng = np.random.default_rng(0)
    keep = (tr["big"] == 1) | (rng.random(len(tr)) < float(config.PRED_PRICE_MODEL_NEG_SAMPLE))
    tr = tr[keep]
    clf = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.06, max_leaf_nodes=31,
        min_samples_leaf=80, l2_regularization=1.0, random_state=0,
    )
    clf.fit(tr[feats].to_numpy(dtype=float), tr["big"].to_numpy())
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
        reg = _fit_return(pairs, before=before)
        _model_cache["model"] = (returns, asof, clf, clf_buy, reg)

    cur = hist[(hist["Date"] == asof) & hist["valid"]]
    if listing_codes:
        allowed = {str(c).zfill(6) for c in listing_codes}
        cur = cur[cur["Code"].isin(allowed)]
    if cur.empty:
        return []
    X = cur[FEATURE_COLS].to_numpy(dtype=float)
    cur = cur.assign(
        prob=_true_prob(clf.predict_proba(X)[:, 1]),
        exp_ret=np.clip(reg.predict(X), -0.30, 0.30),
    )
    is_lu = cur["limit_up"] >= 1

    # 매수 가능 후보는 전일 하락도 포함: 상한가 제외 walk-forward 에서 하락 제외 시 57건 → 포함 84건.
    buy = cur[~is_lu]
    if clf_buy is not None and not buy.empty:
        buy = buy.assign(prob=_true_prob(clf_buy.predict_proba(buy[BUYABLE_FEATURE_COLS].to_numpy(dtype=float))[:, 1]))
    else:
        buy = buy[buy["r1"] >= 0]
    block_hot = config.PRED_PRIOR_DAY_EXHAUSTION_ENABLED and config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_SLATE
    if block_hot:
        buy = buy[buy["r1"] + 1e-12 < float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)]
    # 2일 누적 급등 추격도 메인에서 제외(상한가 직전 단계의 과열).
    if "r2" in buy.columns and not buy.empty:
        buy = buy[buy["r2"].fillna(0) + 1e-12 < _LU_EXTRA_MAX_R2]
    main = buy.sort_values("prob", ascending=False).head(int(top_n))

    lu_rows = cur.iloc[0:0]
    if not block_hot:
        overall = cur[cur["r1"] >= 0].sort_values("prob", ascending=False).head(int(top_n))
        cand = overall[overall["limit_up"] >= 1].copy()
        # 연속 상한가·2일 누적 급등은 익일 반락이 잦아 별도 표시에서도 뺀다.
        streak = cand["lu_streak"] if "lu_streak" in cand.columns else 0
        r2 = cand["r2"] if "r2" in cand.columns else 0.0
        overheated = (streak.fillna(0) > _LU_EXTRA_MAX_STREAK) | (
            r2.fillna(0) + 1e-12 >= _LU_EXTRA_MAX_R2
        )
        lu_rows = cand[~overheated].head(max(0, int(config.PRED_LIMIT_UP_EXTRA_MAX)))

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
                "전일 상한가 익일 20%↑ 기준율 약 18% · 연속 상한·2일 +40% 이상은 표시 제외 · "
                f"표시 상승률은 낙관치 {raw_exp:+.1f}%를 {_LU_EXTRA_DISPLAY_PCT_CAP:.0f}%로 상한"
            )
        else:
            head = ""
            verify = "전일 상한가 제외 후보 246거래일 검증 상위 5종 적중률 약 7%, 1종 이상 맞힌 날 약 26%"
        reason = (
            f"{head}가격·거래량 모델: 예측 상승률 {exp_pct:+.1f}%(상승 시나리오 상위 10%) · 익일 20%↑ 확률 {prob:.0%} · "
            f"전일 {float(r.r1) * 100:+.1f}% · "
            f"거래량 20일 평균의 {vol_x:.1f}배 · 같은 업종 전일 20%↑ {int(r.ind_big or 0)}종 "
            f"({verify}. 종가 기준 적중)"
            + (f" · {basis_note}" if basis_note else "")
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
            confidence_tier="mid" if prior_lu else "high",
            pred_source="price_model",
            prior_limit_up=prior_lu,
        )
        rows.append(row)
    return rows
