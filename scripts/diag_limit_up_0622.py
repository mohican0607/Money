from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import stocks
from src.snapshot_rebuild_learning import (
    _LIMIT_UP_RET_DECIMAL,
    _excluded_strong_mover_codes_for_day,
)

T = date(2026, 6, 22)
ohlcv = pd.read_parquet(ROOT / "data/cache/ohlcv_long_full.parquet")
ohlcv["Date"] = pd.to_datetime(ohlcv["Date"])
ret = stocks.daily_returns_table(ohlcv)
day = ret[ret["Date"] == pd.Timestamp(T)]
lim = day[day["return_pct"] >= _LIMIT_UP_RET_DECIMAL].sort_values("return_pct", ascending=False)
big = day[day["return_pct"] >= 0.20].sort_values("return_pct", ascending=False)
print(f"OHLCV returns T={T}: 20%+={len(big)} limit-up={len(lim)}")
for _, r in lim.iterrows():
    print(f"  {str(r.Code).zfill(6)} {r.Name} {float(r.return_pct)*100:.2f}%")

ex = _excluded_strong_mover_codes_for_day(T, ret)
print(f"excluded_strong_mover_codes: {sorted(ex)}")

krx = stocks.try_krx_change_pct_by_code(T, returns_df=ret) or {}
kl = sorted([(c, p) for c, p in krx.items() if p >= 29.0], key=lambda x: -x[1])
print(f"pykrx limit-up (>={29}%): {len(kl)}")
for c, p in kl:
    nm = day.loc[day["Code"].astype(str).str.zfill(6) == c, "Name"]
    name = str(nm.iloc[0]) if len(nm) else c
    in_ohlcv_lim = c in {str(x.Code).zfill(6) for _, x in lim.iterrows()}
    print(f"  {c} {name:14s} {p:+.2f}% ohlcv_lim={in_ohlcv_lim}")

# main report 6/22 block
html = (ROOT / "output/report_2026.06.html").read_text(encoding="utf-8")
m = re.search(r'id="day-2026-06-22".*?(?=id="day-|<footer)', html, re.S)
block = m.group(0) if m else ""
codes6 = {x.split()[-1] for x in re.findall(r'data-sort-value="([^"]+ \d{6})"', block)}
print(f"\nreport rows={len(codes6)}")
for c, p in kl:
    if c not in codes6:
        print(f"  TABLE MISSING (krx lim) {c} {p:.2f}%")
for _, r in lim.iterrows():
    c = str(r.Code).zfill(6)
    if c not in codes6:
        print(f"  TABLE MISSING (ohlcv lim) {c} {float(r.return_pct)*100:.2f}%")
    else:
        print(f"  OK in table {c} {r.Name}")

# 20%+ not in table
for _, r in big.iterrows():
    c = str(r.Code).zfill(6)
    if c not in codes6:
        print(f"  20%+ MISSING {c} {r.Name} {float(r.return_pct)*100:.2f}%")
