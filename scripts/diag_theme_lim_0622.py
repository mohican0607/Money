from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import stocks
from src.snapshot_rebuild_learning import _LIMIT_UP_RET_DECIMAL

T = date(2026, 6, 22)
ohlcv = pd.read_parquet(ROOT / "data/cache/ohlcv_long_full.parquet")
ohlcv["Date"] = pd.to_datetime(ohlcv["Date"])
ret = stocks.daily_returns_table(ohlcv)
day = ret[ret["Date"] == pd.Timestamp(T)]
lim = {str(r.Code).zfill(6): str(r.Name) for _, r in day[day["return_pct"] >= _LIMIT_UP_RET_DECIMAL].iterrows()}

th = (ROOT / "output/report_theme_25pct_2026.06.html").read_text(encoding="utf-8")
# find day section
m = re.search(r"<section[^>]*data-trading-day=\"2026-06-22\".*?</section>", th, re.S)
if not m:
    m = re.search(r"2026-06-22.*?(?=<section[^>]*data-trading-day=|</body>)", th, re.S)
block = m.group(0) if m else ""
codes = set(re.findall(r"<td[^>]*>(\d{6})</td>", block))
print(f"theme section codes: {len(codes)}")
for c, nm in sorted(lim.items()):
    print(f"  {c} {nm}: {'IN theme' if c in codes else 'MISSING theme'}")
