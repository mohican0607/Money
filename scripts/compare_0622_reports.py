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
lim = {
    str(r.Code).zfill(6): (str(r.Name), float(r.return_pct) * 100)
    for _, r in ret[(ret["Date"] == pd.Timestamp(T)) & (ret["return_pct"] >= _LIMIT_UP_RET_DECIMAL)].iterrows()
}

for fname in ("report_2026.06.html", "report_2026.06-22-1.html", "report_2026.06-22-2.html"):
    p = ROOT / "output" / fname
    if not p.exists():
        continue
    html = p.read_text(encoding="utf-8")
    m = re.search(r'id="day-2026-06-22".*?(?=id="day-|<footer)', html, re.S)
    if not m:
        m = re.search(r"<h2>2026-06-22</h2>.*?(?=<h2>|</body>)", html, re.S)
    block = m.group(0) if m else ""
    codes = {x.split()[-1] for x in re.findall(r'data-sort-value="([^"]+ \d{6})"', block)}
    stats = re.search(r"상한≥29% (\d+)종", block)
    print(f"\n=== {fname} rows={len(codes)} stats_lim={stats.group(1) if stats else '?'} ===")
    for c, (nm, pp) in sorted(lim.items(), key=lambda x: -x[1][1]):
        print(f"  {'OK' if c in codes else 'MISS'} {c} {nm} {pp:.2f}%")
