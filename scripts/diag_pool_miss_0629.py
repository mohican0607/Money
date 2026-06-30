"""6/29 후보 풀 누락 급등주 분석."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src import config, news, stocks, train_snapshot, trading_calendar
from src.prediction import ml_move_rank, predict, prediction_ranking

T = date(2026, 6, 29)
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

prev = trading_calendar.last_trading_day_before(T)
blob, _ = news.aggregate_early_late_for_target(news_by, prev)
snap = train_snapshot.load_snapshot(config.TRAIN_SNAPSHOT_PATH)
events = snap.events if snap else []
ctx = predict.build_scoring_context(blob, events)
cands = set(
    ml_move_rank._day_candidate_codes(codes, names, blob, ctx, returns_ml, T)
)
sl = returns_ml[returns_ml["Date"] == pd.Timestamp(T)]
big = sl[sl["return_pct"] >= 0.20].copy()
big["code"] = big["Code"].astype(str).str.zfill(6)
miss = big[~big["code"].isin(cands)].sort_values("return_pct", ascending=False)
hit = big[big["code"].isin(cands)].sort_values("return_pct", ascending=False)
print(f"candidates={len(cands)} hit={len(hit)}/{len(big)} miss={len(miss)}")
print("\n--- MISSED (top 20) ---")
for _, r in miss.head(20).iterrows():
    c = r["code"]
    nm = (names.get(c, "") or "")[:8]
    ind = (stocks.industry_name_for_code(c) or "?")[:10]
    rl = float(r.get("ret_lag1") or 0)
    vs = float(r.get("vol_surge_ratio") or 0)
    mom = prediction_ranking.momentum_raw_from_row(r)
    print(
        f"  {c} {nm:8s} {float(r['return_pct'])*100:5.1f}% "
        f"ind={ind:10s} lag1={rl*100:5.1f}% vs={vs:.2f} mom={mom:.2f}"
    )
