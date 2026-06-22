from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import stocks

T = date(2026, 6, 22)
ohlcv = pd.read_parquet(ROOT / "data/cache/ohlcv_long_full.parquet")
ohlcv["Date"] = pd.to_datetime(ohlcv["Date"])
listing = stocks.load_listing()
all_codes = set(listing["Code"].astype(str).str.zfill(6))
have = set(
    ohlcv.loc[ohlcv["Date"] == pd.Timestamp(T), "Code"].astype(str).str.zfill(6)
)
missing = sorted(all_codes - have)
print(f"listing {len(all_codes)} have bar on T {len(have)} missing {len(missing)}")

ret = stocks.daily_returns_table(ohlcv)
krx = stocks.try_krx_change_pct_by_code(T, returns_df=ret) or {}
# limit-up in krx but no ohlcv bar
for c, p in sorted(krx.items(), key=lambda x: -x[1]):
    if p >= 29 and c not in have:
        print(f"LIMIT in krx but NO ohlcv bar: {c} {p:.2f}%")
for c in missing[:5]:
    p = krx.get(c)
    if p and p >= 20:
        print(f"missing bar but krx 20%+: {c} {p:.2f}%")
