# -*- coding: utf-8 -*-
"""가격 모델 walk-forward 상위 5종을 전일 등락 구간별로 나눠 성적 확인 + 9/29 실제 결과."""
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
feats["month"] = feats["Date"].dt.to_period("M")
cols = FAMILIES["가격+테마"]
rng = np.random.default_rng(0)
picks = []
for tm in sorted(m for m in feats["month"].unique() if m >= pd.Period("2025-08", "M")):
    train = feats[feats["month"] < tm]
    test = feats[feats["month"] == tm]
    tr = train[(train["big"] == 1) | (rng.random(len(train)) < 0.10)]
    clf = HistGradientBoostingClassifier(
        max_iter=250, learning_rate=0.06, max_leaf_nodes=31,
        min_samples_leaf=80, l2_regularization=1.0, random_state=0,
    )
    clf.fit(tr[cols].to_numpy(dtype=float), tr["big"].to_numpy())
    t = test.assign(p=clf.predict_proba(test[cols].to_numpy(dtype=float))[:, 1])
    picks.append(t.sort_values(["Date", "p"], ascending=[True, False]).groupby("Date").head(5))
    variant = next((a for a in sys.argv if a.startswith("--v=")), "")[4:]
    if variant:
        prev_close = np.exp(t["logp"])
        pref = t["Name"].astype(str).str.contains(r"우$|우B$|우C$|\d우", regex=True)
        ok = t["r1"] >= 0
        if "pref" in variant:
            ok &= ~pref
        if "penny" in variant:
            ok &= prev_close >= 1000
        picks[-1] = t[ok].sort_values(["Date", "p"], ascending=[True, False]).groupby("Date").head(5)
pk = pd.concat(picks)
ohlcv = pd.read_parquet(ROOT / "data" / "cache" / "ohlcv_long_full.parquet")
ohlcv["Date"] = pd.to_datetime(ohlcv["Date"]).dt.normalize()
ohlcv["Code"] = ohlcv["Code"].astype(str).str.zfill(6)
pk = pk.merge(ohlcv[["Date", "Code", "Change"]], on=["Date", "Code"], how="left")

bins = [-1, -0.25, -0.10, 0.0, 0.10, 0.29, 1]
labels = ["전일 -25% 이하", "-25~-10%", "-10~0%", "0~+10%", "+10~+29%", "상한가(+29%↑)"]
pk["bucket"] = pd.cut(pk["r1"], bins=bins, labels=labels)
print(f"픽 {len(pk)}개 (281거래일 × 5)")
for b, g in pk.groupby("bucket", observed=True):
    print(
        f"{b:14s} 픽 {len(g):4d} ({len(g)/len(pk):4.0%})  익일 20%↑ {g['big'].mean():5.1%}  "
        f"익일 종가등락 평균 {g['Change'].clip(-0.3, 0.3).mean():+6.2%}  "
        f"시가→종가 평균 {g['oc'].clip(-0.3, 0.3).mean():+6.2%}"
    )
down = pk[pk["r1"] < 0]
print(f"\n전일 하락 종목을 뺐을 때: 픽 {len(pk) - len(down)}  적중 {int(pk['big'].sum() - down['big'].sum())} (뺀 픽의 적중 {int(down['big'].sum())})")

print("\n[2026-09-29 슬레이트 실제 결과]")
for code, name in [("312610", ""), ("196490", ""), ("002787", ""), ("115450", ""), ("067630", "")]:
    s = ohlcv[(ohlcv["Code"] == code) & (ohlcv["Date"] >= "2026-09-28")].sort_values("Date")
    print(code, s[["Date", "Name", "Open", "Close", "Change"]].to_string(header=False, index=False).replace("\n", " | "))
