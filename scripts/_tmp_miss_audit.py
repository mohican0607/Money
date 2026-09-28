# -*- coding: utf-8 -*-
"""Recent freeze preds vs actual Change (daily return)."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src import config  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")

freeze = json.loads(config.PREDICTION_FREEZE_PATH.read_text(encoding="utf-8"))
ohlcv_path = ROOT / "data" / "cache" / "ohlcv_long_full.parquet"
df = pd.read_parquet(ohlcv_path)
df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
df["Code"] = df["Code"].astype(str).str.zfill(6)
# Change is daily return ratio
act_map = {
    (r.Code, r.Date.date()): float(r.Change)
    for r in df.itertuples()
    if pd.notna(r.Change)
}

days = ["2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"]
for k in days:
    rows = freeze.get(k, [])
    day = date.fromisoformat(k)
    print(f"\n==== T {k} preds={len(rows)}")
    hits20 = hits0 = n = 0
    for r in rows:
        code = str(r.get("code", "")).zfill(6)
        name = r.get("name")
        pred = float(r.get("predicted_return_pct") or 0)
        act = act_map.get((code, day))
        n += 1
        if act is None:
            print(f"  ? {code} {name} pred={pred:.1f} act=?")
            continue
        ap = act * 100.0
        if ap >= 20:
            hits20 += 1
        if ap >= 0:
            hits0 += 1
        mark = "HIT20" if ap >= 20 else ("OK+" if ap >= 0 else "MISS")
        kw = (r.get("matched_keywords") or [])[:3]
        print(
            f"  {mark} {code} {name} pred={pred:.1f} act={ap:.1f}% "
            f"tier={r.get('confidence_tier')} m={float(r.get('mention_score') or 0):.2f} kw={kw}"
        )
    print(f"  summary hit20={hits20}/{n} nonneg={hits0}/{n}")

print("\n==== ACTUAL 20%+ movers (top)")
for k in days:
    day = date.fromisoformat(k)
    day_df = df[df["Date"].dt.date == day].copy()
    big = day_df[day_df["Change"] >= 0.20].sort_values("Change", ascending=False)
    print(f"\n{k} n={len(big)}")
    pred_codes = {str(r.get("code", "")).zfill(6) for r in freeze.get(k, [])}
    for _, row in big.head(20).iterrows():
        code = str(row["Code"]).zfill(6)
        in_pred = "IN_PRED" if code in pred_codes else "MISS_PRED"
        print(f"  {in_pred} {code} {row['Name']} {float(row['Change']) * 100:.1f}%")
