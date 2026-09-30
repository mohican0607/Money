# -*- coding: utf-8 -*-
"""
어떤 신호 조합이 익일 20%↑ 급등을 가장 잘 맞히는지 walk-forward 로 비교.

T 예측에는 T-1 종가까지의 시세와 T 09:00 이전 뉴스만 쓴다.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src import config, news  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")
THR = 0.20
OUT = ROOT / "data" / "cache" / "signal_study_features.parquet"


def load_news_counts() -> pd.DataFrame:
    """(code, T) → T-1 00:00 ~ T 08:59 뉴스 건수."""
    counts: dict[tuple[str, pd.Timestamp], int] = defaultdict(int)
    base = news._CACHE_NEWS / "naver_both"
    for f in sorted(base.rglob("day_*.json")):
        try:
            rows = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(rows, list):
            continue
        for r in rows:
            code = str(r.get("stock_code") or "").zfill(6)
            if not code.strip("0"):
                continue
            try:
                pub = parsedate_to_datetime(str(r.get("pub")))
            except (TypeError, ValueError):
                continue
            pub = pub.replace(tzinfo=None)
            # 09:00 이전 뉴스는 당일 T 예측에, 이후 뉴스는 다음 거래일 예측에 귀속
            day = pd.Timestamp(pub.date())
            if pub.hour >= 9:
                day = day + pd.Timedelta(days=1)
            counts[(code, day)] += 1
    df = pd.DataFrame(
        [(c, d, n) for (c, d), n in counts.items()], columns=["Code", "NewsDay", "news_n"]
    )
    return df


def build_features() -> pd.DataFrame:
    df = pd.read_parquet(ROOT / "data" / "cache" / "ohlcv_long_full.parquet")
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
    for c in ("Open", "High", "Low", "Close", "Volume", "Change"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    ind_map = json.loads(
        (config.LISTING_META_CACHE_DIR / "stock_industry_code.json").read_text(encoding="utf-8")
    )
    df["ind"] = df["Code"].map(lambda c: str(ind_map.get(c) or "na"))

    g = df.groupby("Code", group_keys=False)
    traded = df["Volume"] > 0
    df["big"] = ((df["Change"] >= THR) & traded & (g["Volume"].shift(1) > 0)).astype(int)
    # 시가 매수 → 종가 +10% 이상(개장 후 실제로 먹을 수 있는 급등)
    oc = df["Close"] / df["Open"].where(df["Open"] > 0) - 1.0
    df["oc"] = oc
    df["gap"] = df["Open"] / g["Close"].shift(1) - 1.0
    df["oc10"] = ((oc >= 0.10) & traded & (g["Volume"].shift(1) > 0)).astype(int)

    # --- 가격·거래량 (당일 값 → 이후 shift(1) 로 T-1 기준화)
    df["r1"] = df["Change"]
    df["r5"] = df["Close"] / g["Close"].shift(5) - 1.0
    df["r20"] = df["Close"] / g["Close"].shift(20) - 1.0
    vavg = g["Volume"].transform(lambda s: s.shift(1).rolling(20, min_periods=5).mean())
    df["vol_ratio"] = np.log1p(df["Volume"] / (vavg + 1.0))
    df["turnover"] = np.log1p(df["Close"] * df["Volume"])
    rng = (df["High"] - df["Low"]).replace(0, np.nan)
    df["close_pos"] = ((df["Close"] - df["Low"]) / rng).fillna(0.5)
    df["upper_wick"] = ((df["High"] - df[["Open", "Close"]].max(axis=1)) / df["Close"]).fillna(0)
    hi20 = g["High"].transform(lambda s: s.rolling(20, min_periods=5).max())
    df["dist_hi20"] = df["Close"] / hi20 - 1.0
    hi120 = g["High"].transform(lambda s: s.rolling(120, min_periods=20).max())
    df["dist_hi120"] = df["Close"] / hi120 - 1.0
    df["n_big60"] = g["big"].transform(lambda s: s.rolling(60, min_periods=1).sum())
    df["volat20"] = g["Change"].transform(lambda s: s.rolling(20, min_periods=5).std())
    df["logp"] = np.log(df["Close"].clip(lower=1))
    df["limit_up"] = (df["Change"] >= 0.29).astype(int)

    # --- 테마(업종) 흐름
    day_ind = df.groupby(["Date", "ind"]).agg(
        ind_n=("Code", "size"), ind_big=("big", "sum"), ind_ret=("Change", "mean")
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
    mkt = df.groupby("Date").agg(mkt_ret=("Change", "mean"), mkt_big=("big", "sum")).reset_index()
    df = df.merge(mkt, on="Date", how="left")

    feat_cols = [
        "r1", "r5", "r20", "vol_ratio", "turnover", "close_pos", "upper_wick", "dist_hi20",
        "dist_hi120", "n_big60", "volat20", "logp", "limit_up",
        "ind_big", "ind_big_share", "ind_ret", "ind_big5", "ind_ret5", "ind_ret_rank", "rel_ind",
        "mkt_ret", "mkt_big",
    ]
    df = df.sort_values(["Code", "Date"]).reset_index(drop=True)
    g = df.groupby("Code", group_keys=False)
    shifted = g[feat_cols].shift(1)
    out = pd.concat(
        [df[["Date", "Code", "Name", "ind", "big", "oc10", "oc", "gap", "Volume"]], shifted], axis=1
    )
    # 전일 거래정지·신규상장·가격제한폭 밖 이상치(기준가 변경 등) 제외
    out = out[
        out["r1"].notna()
        & (g["Volume"].shift(1) > 0)
        & (out["r1"].abs() <= 0.31)
        & (df["Open"] > 0)
        & (df["Change"].abs() <= 0.31)
    ]

    # --- 뉴스
    nc = load_news_counts()
    out = out.merge(nc, left_on=["Code", "Date"], right_on=["Code", "NewsDay"], how="left")
    out["news_n"] = out["news_n"].fillna(0)
    out = out.drop(columns=["NewsDay"])
    out = out.sort_values(["Code", "Date"])
    out["news_5d"] = out.groupby("Code")["news_n"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=1).sum()
    ).fillna(0)
    out["news_surge"] = out["news_n"] / (out["news_5d"] / 5.0 + 0.5)
    # 업종 전체 뉴스 열기(테마 뉴스)
    ind_news = out.groupby(["Date", "ind"])["news_n"].sum().rename("ind_news").reset_index()
    out = out.merge(ind_news, on=["Date", "ind"], how="left")
    return out.reset_index(drop=True)


FAMILIES = {
    "가격·거래량": [
        "r1", "r5", "r20", "vol_ratio", "turnover", "close_pos", "upper_wick", "dist_hi20",
        "dist_hi120", "n_big60", "volat20", "logp", "limit_up",
    ],
    "테마흐름": [
        "ind_big", "ind_big_share", "ind_ret", "ind_big5", "ind_ret5", "ind_ret_rank", "mkt_ret", "mkt_big",
    ],
    "뉴스": ["news_n", "news_5d", "news_surge", "ind_news"],
}
FAMILIES["뉴스+테마"] = FAMILIES["뉴스"] + FAMILIES["테마흐름"]
FAMILIES["가격+테마"] = FAMILIES["가격·거래량"] + FAMILIES["테마흐름"] + ["rel_ind"]
FAMILIES["전부"] = FAMILIES["가격+테마"] + FAMILIES["뉴스"]


def run(feats: pd.DataFrame, label: str, *, skip_gap_over: float | None = None) -> pd.DataFrame:
    """월 단위 walk-forward. ``skip_gap_over`` 면 시초가 갭이 그 이상인 픽은 매수 불가로 제외."""
    feats = feats.copy()
    feats["month"] = feats["Date"].dt.to_period("M")
    months = sorted(feats["month"].unique())
    test_months = [m for m in months if m >= pd.Period("2025-08", "M")]
    rng = np.random.default_rng(0)
    rows = []
    for tm in test_months:
        train = feats[feats["month"] < tm]
        test = feats[feats["month"] == tm]
        keep = (train[label] == 1) | (rng.random(len(train)) < 0.10)
        tr = train[keep]
        for fam, cols in FAMILIES.items():
            clf = HistGradientBoostingClassifier(
                max_iter=250, learning_rate=0.06, max_leaf_nodes=31,
                min_samples_leaf=80, l2_regularization=1.0, random_state=0,
            )
            clf.fit(tr[cols].to_numpy(dtype=float), tr[label].to_numpy())
            t = test.assign(_p=clf.predict_proba(test[cols].to_numpy(dtype=float))[:, 1])
            if skip_gap_over is not None:
                t = t[t["gap"] < skip_gap_over]
            t = t.sort_values(["Date", "_p"], ascending=[True, False])
            for d, dd in t.groupby("Date"):
                top = dd.head(20)
                rows.append(
                    {
                        "fam": fam,
                        "Date": d,
                        "h5": int(top[label].head(5).sum()),
                        "h10": int(top[label].head(10).sum()),
                        "h20": int(top[label].sum()),
                        "big5": int(top["big"].head(5).sum()),
                        "oc5": float(top["oc"].head(5).clip(-0.3, 0.3).mean()),
                        "npos": int(dd[label].sum()),
                        "univ": len(dd),
                        "univ_oc": float(dd["oc"].clip(-0.3, 0.3).mean()),
                    }
                )
        print(f"  {tm} done", flush=True)
    dr = pd.DataFrame(rows)
    base = dr[dr["fam"] == "전부"]
    rand5 = (base["npos"] / base["univ"] * 5).sum()
    print(
        f"\n=== 기준={label} 갭제외={skip_gap_over} | 테스트 {len(base)}거래일 "
        f"정답 {int(base['npos'].sum())}건 무작위기대 Hit@5 {rand5:.1f} "
        f"전종목 시가→종가 평균 {base['univ_oc'].mean():+.2%} ==="
    )
    for fam, grp in dr.groupby("fam", sort=False):
        n = len(grp)
        print(
            f"{fam:8s} Hit@5 {grp.h5.sum():4d} ({grp.h5.sum()/(n*5):5.1%})  Hit@20 {grp.h20.sum():4d}  "
            f"1개↑ 맞힌 날 {(grp.h5 > 0).mean():4.0%}  "
            f"상위5 시가→종가 평균 {grp.oc5.mean():+.2%} (중앙 {grp.oc5.median():+.2%})  "
            f"(종가20%↑ {grp.big5.sum()})"
        )
    return dr


if __name__ == "__main__":
    if OUT.is_file() and "--rebuild" not in sys.argv:
        feats = pd.read_parquet(OUT)
    else:
        feats = build_features()
        feats.to_parquet(OUT)
    print("rows", len(feats), "big", int(feats["big"].sum()), "oc10", int(feats["oc10"].sum()), flush=True)
    run(feats, "big")
    run(feats, "big", skip_gap_over=0.10)
    dr = run(feats, "oc10", skip_gap_over=0.10)
    dr.to_csv(ROOT / "data" / "cache" / "signal_study_daily.csv", index=False, encoding="utf-8-sig")


