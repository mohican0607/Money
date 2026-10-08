"""가격·거래량 모델 walk-forward 검증: 관측일마다 상위 K종 중 실제 20%↑ 수.

  python scripts/bench_price_model.py --start 2026-01-02 --end 2026-10-08 --refit 5
  python scripts/bench_price_model.py --day 2026-10-08     # 하루 상세(급등주 순위)

모델은 ``--refit`` 거래일마다 다시 학습(운영은 매일)하고, 그 사이 날짜는 같은 모델로 점수만 낸다.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config, stocks  # noqa: E402
from src.prediction import price_breakout_model as pbm  # noqa: E402


def load_inputs() -> tuple[pd.DataFrame, set[str], dict[str, str]]:
    ohlcv = pd.read_parquet(config.CACHE_DIR / "ohlcv_long_full.parquet")
    returns = stocks.daily_returns_table(ohlcv)
    listing = stocks.load_listing()
    names = {str(r["Code"]).zfill(6): str(r["Name"]) for _, r in listing.iterrows()}
    codes = {c for c in names if stocks.is_common_equity_code(c)}
    return returns, codes, names


def slate_for_day(
    raw: pd.DataFrame,
    clf,
    T: pd.Timestamp,
    codes: set[str],
    *,
    k: int,
    features: list[str],
    drop_neg: bool = True,
    block_hot: bool = True,
    rotation: bool = True,
) -> pd.DataFrame:
    """운영 ``predict_top`` 과 같은 후보 필터·로테이션 가산으로 상위 k 행."""
    hist_days = raw.loc[raw["Date"] < T, "Date"]
    if hist_days.empty:
        return raw.iloc[0:0]
    asof = hist_days.max()
    cur = raw[(raw["Date"] == asof) & raw["valid"] & raw["Code"].isin(codes)]
    if drop_neg:
        cur = cur[cur["r1"] >= 0]
    if cur.empty:
        return cur
    p = pbm._true_prob(clf.predict_proba(cur[features].to_numpy(dtype=float))[:, 1])
    cur = cur.assign(prob=p)
    block = float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)
    if block_hot:
        cur = cur[(cur["r1"] + 1e-12 < block) & (cur["limit_up"] < 1)]
    if not rotation:
        return cur.assign(_rank=cur["prob"]).sort_values("_rank", ascending=False).head(k)
    rot_w = float(config.PRED_THEME_ROTATION_RANK_BOOST)
    lag_min = float(config.PRED_THEME_ROTATION_LAG_MIN)
    lag_max = float(config.PRED_THEME_ROTATION_LAG_MAX)
    in_lag = (cur["r1"] >= lag_min - 1e-12) & (cur["r1"] < min(lag_max, block)) & (cur["ind_big"] >= 1)
    bonus = np.where(
        in_lag, 0.08 * rot_w + 0.12 * np.minimum(1.0, cur["ind_big_share"].fillna(0) / 0.08), 0.0
    )
    cur = cur.assign(_rank=cur["prob"] + bonus)
    return cur.sort_values("_rank", ascending=False).head(k)


VARIANTS: dict[str, dict] = {
    "운영(현재)": {},
    "전일급등 허용": {"block_hot": False},
    "전일하락 허용": {"drop_neg": False},
    "둘다 허용": {"block_hot": False, "drop_neg": False},
    "로테이션 가산 끔": {"rotation": False},
    "둘다 허용+가산 끔": {"block_hot": False, "drop_neg": False, "rotation": False},
    "급등 허용+가산 끔": {"block_hot": False, "rotation": False},
}


def run(start: str, end: str, refit: int, k: int, features: list[str]) -> dict:
    returns, codes, _names = load_inputs()
    t0 = time.perf_counter()
    raw = pbm.build_raw_features(returns)
    pairs = pbm._training_pairs(raw)
    print(f"피처 준비 {time.perf_counter() - t0:.0f}s, 행 {len(raw):,}", flush=True)

    big_by_day = raw[raw["big"] == 1].groupby("Date")["Code"].apply(set).to_dict()
    days = sorted(d for d in raw["Date"].unique() if pd.Timestamp(start) <= d <= pd.Timestamp(end))
    clf = None
    stats = {name: {"hits": 0, "any": 0} for name in VARIANTS}
    n_big_total = 0
    for i, T in enumerate(days):
        T = pd.Timestamp(T)
        if clf is None or i % refit == 0:
            pbm.FEATURE_COLS[:] = features
            clf = pbm._fit(pairs, before=T)
        actual = big_by_day.get(T, set()) & codes
        n_big_total += len(actual)
        for name, kw in VARIANTS.items():
            top = slate_for_day(raw, clf, T, codes, k=k, features=features, **kw)
            h = len(set(top["Code"]) & actual)
            stats[name]["hits"] += h
            stats[name]["any"] += int(h > 0)
    n = len(days)
    print(f"[{start}~{end}] {n}거래일 · 전체 20%↑ {n_big_total}건 · 상위 {k}종 기준")
    for name, s in stats.items():
        print(
            f"  {name:<14} 적중 {s['hits']:>3}건  정밀도 {s['hits'] / max(1, n * k):6.1%}  "
            f"1종 이상 맞힌 날 {s['any'] / max(1, n):6.1%}",
            flush=True,
        )
    return stats


def detail(day: str, k: int) -> None:
    returns, codes, names = load_inputs()
    raw = pbm.build_raw_features(returns)
    pairs = pbm._training_pairs(raw)
    T = pd.Timestamp(day)
    clf = pbm._fit(pairs, before=T)
    full = slate_for_day(raw, clf, T, codes, k=10_000, features=list(pbm.FEATURE_COLS))
    full = full.reset_index(drop=True)
    full["rank"] = np.arange(1, len(full) + 1)
    actual = raw[(raw["Date"] == T) & (raw["big"] == 1) & raw["Code"].isin(codes)]
    asof = raw.loc[raw["Date"] < T, "Date"].max()
    prev = raw[(raw["Date"] == asof)].set_index("Code")
    print(f"T={day} 실제 20%↑ {len(actual)}종 (후보 {len(full)}종 중 순위)")
    for c, ret in zip(actual["Code"], actual["return_pct"]):
        hit = full[full["Code"] == c]
        r1 = prev["r1"].get(c, np.nan)
        rank = int(hit["rank"].iloc[0]) if not hit.empty else None
        why = "" if rank else ("전일 하락" if r1 < 0 else "전일 급등 제외" if r1 >= 0.18 else "후보 아님")
        print(
            f"  {names.get(c, c):<14} 당일 {ret:+.1%} 전일 {r1:+.1%} "
            f"순위 {rank if rank else '-'} {why}"
        )
    print("상위 슬레이트:")
    for r in full.head(k).itertuples():
        print(f"  {r.rank}. {names.get(r.Code, r.Code)} prob={r.prob:.2%} 전일 {r.r1:+.1%}")


EXTRA_FEATURES = ["r1_prev", "lu_streak", "n_lu20", "gap", "close_at_high", "vol_ratio_prev", "r2"]


def add_extra_features(raw: pd.DataFrame) -> pd.DataFrame:
    g = raw.groupby("Code", group_keys=False)
    prev_close = g["Close"].shift(1)
    raw["r1_prev"] = g["r1"].shift(1)
    raw["r2"] = raw["Close"] / g["Close"].shift(2) - 1.0
    lu = raw["limit_up"].astype(int)
    grp = (lu != g["limit_up"].shift(1).fillna(0).astype(int)).cumsum()
    raw["lu_streak"] = lu.groupby([raw["Code"], grp]).cumsum() * lu
    raw["n_lu20"] = g["limit_up"].transform(lambda s: s.rolling(20, min_periods=1).sum())
    raw["gap"] = (raw["Open"] / prev_close - 1.0).fillna(0)
    raw["close_at_high"] = (raw["Close"] >= raw["High"] * 0.999).astype(int)
    raw["vol_ratio_prev"] = g["vol_ratio"].shift(1)
    return raw


def run_features(start: str, end: str, refit: int, k: int) -> None:
    returns, codes, _names = load_inputs()
    raw = add_extra_features(pbm.build_raw_features(returns).copy())
    big_by_day = raw[raw["big"] == 1].groupby("Date")["Code"].apply(set).to_dict()
    days = sorted(d for d in raw["Date"].unique() if pd.Timestamp(start) <= d <= pd.Timestamp(end))
    base = list(pbm.FEATURE_COLS)
    sets = {"기본 피처": base, "기본+추가 피처": base + EXTRA_FEATURES}
    n_big_total = sum(len(big_by_day.get(pd.Timestamp(T), set()) & codes) for T in days)
    print(f"[{start}~{end}] {len(days)}거래일 · 전체 20%↑ {n_big_total}건 · 상위 {k}종 · 필터·가산 없음")
    for label, feats in sets.items():
        pbm.FEATURE_COLS[:] = feats
        pairs = pbm._training_pairs(raw)
        clf = None
        hits = anyd = 0
        for i, T in enumerate(days):
            T = pd.Timestamp(T)
            if clf is None or i % refit == 0:
                clf = pbm._fit(pairs, before=T)
            top = slate_for_day(
                raw, clf, T, codes, k=k, features=feats,
                drop_neg=False, block_hot=False, rotation=False,
            )
            h = len(set(top["Code"]) & big_by_day.get(T, set()) & codes)
            hits += h
            anyd += int(h > 0)
        print(
            f"  {label:<10} 적중 {hits:>3}건  정밀도 {hits / max(1, len(days) * k):6.1%}  "
            f"1종 이상 맞힌 날 {anyd / max(1, len(days)):6.1%}",
            flush=True,
        )
    pbm.FEATURE_COLS[:] = base


def _fit_hgb(
    tr: pd.DataFrame,
    feats: list[str],
    *,
    neg: float,
    seed: int = 0,
    before: pd.Timestamp | None = None,
    half_life: float | None = None,
    max_iter: int = 250,
    lr: float = 0.06,
):
    from sklearn.ensemble import HistGradientBoostingClassifier

    rng = np.random.default_rng(seed)
    keep = (tr["big"] == 1) | (rng.random(len(tr)) < neg)
    tr = tr[keep]
    w = None
    if half_life and before is not None:
        age = (before - tr["Date"]).dt.days.to_numpy(dtype=float)
        w = np.power(0.5, age / half_life)
    clf = HistGradientBoostingClassifier(
        max_iter=max_iter, learning_rate=lr, max_leaf_nodes=31,
        min_samples_leaf=80, l2_regularization=1.0, random_state=seed,
    )
    clf.fit(tr[feats].to_numpy(dtype=float), tr["big"].to_numpy(), sample_weight=w)
    return clf


def _prob(clf, X: np.ndarray, neg: float) -> np.ndarray:
    p = clf.predict_proba(X)[:, 1]
    if neg >= 1.0:
        return p
    odds = p / np.clip(1.0 - p, 1e-9, None) * neg
    return odds / (1.0 + odds)


HOT_R1 = 0.25


def model_configs(base: list[str]) -> dict[str, dict]:
    """이름 → fit(tr, before) 가 score(cur)->prob 을 돌려주는 설정."""
    full = base + EXTRA_FEATURES
    neg = 0.10

    def single(feats, **kw):
        def fit(tr, before):
            clf = _fit_hgb(tr, feats, neg=neg, before=before, **kw)
            return lambda cur: _prob(clf, cur[feats].to_numpy(dtype=float), neg)
        return fit

    def bag(feats, seeds=(0, 1, 2, 3, 4), **kw):
        def fit(tr, before):
            clfs = [_fit_hgb(tr, feats, neg=neg, seed=s, before=before, **kw) for s in seeds]
            return lambda cur: np.mean(
                [_prob(c, cur[feats].to_numpy(dtype=float), neg) for c in clfs], axis=0
            )
        return fit

    def specialist(feats_base, feats_hot, **kw):
        def fit(tr, before):
            gen = _fit_hgb(tr, feats_base, neg=neg, before=before)
            hot_tr = tr[tr["r1"] >= HOT_R1]
            hot = _fit_hgb(hot_tr, feats_hot, neg=1.0, before=before, **kw)

            def score(cur):
                p = _prob(gen, cur[feats_base].to_numpy(dtype=float), neg)
                m = (cur["r1"] >= HOT_R1).to_numpy()
                if m.any():
                    p = p.copy()
                    p[m] = _prob(hot, cur.loc[m, feats_hot].to_numpy(dtype=float), 1.0)
                return p
            return score
        return fit

    return {
        "기본(운영)": single(base),
        "5-seed 앙상블": bag(base),
        "최근 가중(반감 180일)": single(base, half_life=180.0),
        "학습량 2배(500회·lr .03)": single(base, max_iter=500, lr=0.03),
        "추가피처 앙상블": bag(full),
        "상한가 전용모델": specialist(base, full),
        "상한가 전용+최근가중": specialist(base, full, half_life=180.0),
    }


def run_models(start: str, end: str, refit: int, k: int, out_path: str) -> None:
    returns, codes, _names = load_inputs()
    raw = add_extra_features(pbm.build_raw_features(returns).copy())
    base = list(pbm.FEATURE_COLS)
    pbm.FEATURE_COLS[:] = base + EXTRA_FEATURES
    pairs = pbm._training_pairs(raw)
    pbm.FEATURE_COLS[:] = base
    big_by_day = raw[raw["big"] == 1].groupby("Date")["Code"].apply(set).to_dict()
    days = [pd.Timestamp(d) for d in sorted(raw["Date"].unique())
            if pd.Timestamp(start) <= d <= pd.Timestamp(end)]
    cfgs = model_configs(base)
    stats = {n: {"hits": 0, "any": 0} for n in cfgs}
    scorers: dict[str, object] = {}
    lines: list[str] = []
    n_big_total = 0
    block = float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)
    for i, T in enumerate(days):
        if i % refit == 0:
            tr = pairs[pairs["Date"] < T]
            scorers = {n: fit(tr, T) for n, fit in cfgs.items()}
            print(f"{T.date()} 재학습 ({i + 1}/{len(days)})", flush=True)
        asof = raw.loc[raw["Date"] < T, "Date"].max()
        cur = raw[(raw["Date"] == asof) & raw["valid"] & raw["Code"].isin(codes) & (raw["r1"] >= 0)]
        if config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_SLATE:
            cur = cur[(cur["r1"] + 1e-12 < block) & (cur["limit_up"] < 1)]
        actual = big_by_day.get(T, set()) & codes
        n_big_total += len(actual)
        for n, score in scorers.items():
            top = cur.assign(_p=score(cur)).nlargest(k, "_p")
            h = len(set(top["Code"]) & actual)
            stats[n]["hits"] += h
            stats[n]["any"] += int(h > 0)
    lines.append(f"[{start}~{end}] {len(days)}거래일 · 전체 20%↑ {n_big_total}건 · 상위 {k}종 · 재학습 {refit}일마다")
    for n, s in stats.items():
        lines.append(
            f"  {n:<22} 적중 {s['hits']:>3}건  정밀도 {s['hits'] / max(1, len(days) * k):6.1%}  "
            f"1종 이상 맞힌 날 {s['any'] / max(1, len(days)):6.1%}"
        )
    Path(out_path).write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines), flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-01-02")
    ap.add_argument("--end", default="2026-10-08")
    ap.add_argument("--refit", type=int, default=5)
    ap.add_argument("--k", type=int, default=int(config.PRED_FORWARD_SHOW_MAX))
    ap.add_argument("--day", default="")
    ap.add_argument("--features", action="store_true", help="추가 피처 비교")
    ap.add_argument("--models", default="", metavar="OUT_TXT", help="학습 방식 비교 결과를 UTF-8 로 저장")
    args = ap.parse_args()
    if args.models:
        run_models(args.start, args.end, args.refit, args.k, args.models)
        return 0
    if args.day:
        detail(args.day, args.k)
        return 0
    if args.features:
        run_features(args.start, args.end, args.refit, args.k)
        return 0
    run(args.start, args.end, args.refit, args.k, list(pbm.FEATURE_COLS))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
