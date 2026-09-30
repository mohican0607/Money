# -*- coding: utf-8 -*-
"""signal_study 가격 모델 픽 검증: 어떤 종목인지, 시가 매수로 수익이 실현 가능한지."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8")
from scripts.signal_study import FAMILIES, OUT  # noqa: E402

feats = pd.read_parquet(OUT)
ohlcv = pd.read_parquet(ROOT / "data" / "cache" / "ohlcv_long_full.parquet")
ohlcv["Date"] = pd.to_datetime(ohlcv["Date"]).dt.normalize()
ohlcv["Code"] = ohlcv["Code"].astype(str).str.zfill(6)
ohlcv = ohlcv.sort_values(["Code", "Date"])
ohlcv["prev_close"] = ohlcv.groupby("Code")["Close"].shift(1)
ohlcv["gap"] = ohlcv["Open"] / ohlcv["prev_close"] - 1.0
ohlcv["oc"] = ohlcv["Close"] / ohlcv["Open"] - 1.0
ohlcv["oh"] = ohlcv["High"] / ohlcv["Open"] - 1.0
px = ohlcv[["Date", "Code", "gap", "oc", "oh", "Change"]]

feats["month"] = feats["Date"].dt.to_period("M")
cols = FAMILIES["가격+테마"]
rng = np.random.default_rng(0)
picks = []
for tm in [pd.Period(m, "M") for m in ["2026-03", "2026-04", "2026-05", "2026-06", "2026-07", "2026-08", "2026-09"]]:
    train = feats[feats["month"] < tm]
    test = feats[feats["month"] == tm]
    keep = (train["big"] == 1) | (rng.random(len(train)) < 0.10)
    tr = train[keep]
    clf = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.06, max_leaf_nodes=31,
        min_samples_leaf=80, l2_regularization=1.0, random_state=0,
    )
    clf.fit(tr[cols].to_numpy(dtype=float), tr["big"].to_numpy())
    t = test.assign(p=clf.predict_proba(test[cols].to_numpy(dtype=float))[:, 1])
    top = t.sort_values(["Date", "p"], ascending=[True, False]).groupby("Date").head(5)
    picks.append(top)
pk = pd.concat(picks).merge(px, on=["Date", "Code"], how="left")

print("픽 수", len(pk), "적중", int(pk["big"].sum()))
print("\n[픽의 전일 상태]")
print("전일 상한가(≥29%) 비율:", f"{(pk['r1'] >= 0.29).mean():.0%}")
print("전일 +10~29%:", f"{((pk['r1'] >= 0.10) & (pk['r1'] < 0.29)).mean():.0%}")
print("전일 +10% 미만:", f"{(pk['r1'] < 0.10).mean():.0%}")
print("\n[시가 매수 실현 가능성]")
print("당일 시초가 갭 평균:", f"{pk['gap'].mean():.1%}", " 갭≥20%로 시작 비율:", f"{(pk['gap'] >= 0.20).mean():.0%}")
print("시가 매수 → 종가 평균 수익:", f"{pk['oc'].mean():.2%}", " 중앙값:", f"{pk['oc'].median():.2%}")
print("시가 매수 → 장중 고가 +10% 이상 닿은 비율:", f"{(pk['oh'] >= 0.10).mean():.0%}")
hits = pk[pk["big"] == 1]
print("\n[적중 종목만] 갭 평균", f"{hits['gap'].mean():.1%}", "시가→종가", f"{hits['oc'].mean():.1%}")
print("적중 중 전일 상한가였던 비율", f"{(hits['r1'] >= 0.29).mean():.0%}")

print("\n[단순 규칙 비교: 전일 상한가 종목 전부 매수]")
lu = feats[(feats["r1"] >= 0.29) & (feats["month"] >= pd.Period("2026-03", "M"))].merge(px, on=["Date", "Code"])
print("건수", len(lu), "익일 20%↑ 비율", f"{lu['big'].mean():.1%}", "시가→종가 평균", f"{lu['oc'].mean():.2%}")

print("\n[2026-09 일자별 픽]")
for d, dd in pk[pk["Date"] >= "2026-09-14"].groupby("Date"):
    s = ", ".join(
        f"{r.Name}(전일{r.r1*100:+.0f}% 갭{r.gap*100:+.0f}% 종가{r.Change*100:+.0f}%{' ✔' if r.big else ''})"
        for r in dd.itertuples()
    )
    print(d.date(), s)
