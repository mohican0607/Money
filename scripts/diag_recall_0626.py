"""2026-06-26 급등 종목이 ML 후보 풀에 들어오는지 빠른 점검."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src import config, news, predict, stocks, train_snapshot
from src.prediction import ml_move_rank

T = date(2026, 6, 26)
ohlcv = pd.read_parquet(config.CACHE_DIR / "ohlcv_long_full.parquet")
returns_ml = stocks.enrich_daily_returns_for_ml(stocks.daily_returns_table(ohlcv))
listing = stocks.load_listing()
codes = listing["Code"].astype(str).str.zfill(6).tolist()
names = {str(r["Code"]).zfill(6): str(r["Name"]) for _, r in listing.iterrows()}

news_by: dict = {}
d = date(2026, 5, 1)
while d <= T:
    p = news.day_news_json_path(d)
    if p.is_file():
        news_by[d] = json.loads(p.read_text(encoding="utf-8"))
    d += timedelta(days=1)

blob, _ = news.aggregate_early_late_for_target(news_by, T)
snap = train_snapshot.load_snapshot(config.TRAIN_SNAPSHOT_PATH)
events = snap.events if snap else []
ctx = predict.build_scoring_context(blob, events)
cands = set(
    ml_move_rank._day_candidate_codes(codes, names, blob, ctx, returns_ml, T)
)

sl = returns_ml[returns_ml["Date"] == pd.Timestamp(T)]
big = sl[sl["return_pct"] >= 0.20]
in_pool = sum(1 for _, r in big.iterrows() if str(r["Code"]).zfill(6) in cands)
print(f"T={T} candidates={len(cands)} actual20+={len(big)} in_pool={in_pool}/{len(big)}")
for _, r in big.sort_values("return_pct", ascending=False).head(15).iterrows():
    c = str(r["Code"]).zfill(6)
    pct = float(r["return_pct"]) * 100.0
    tag = "OK" if c in cands else "MISS"
    print(f"  {tag} {c} {str(r['Name'])[:12]:12s} {pct:6.1f}%")
