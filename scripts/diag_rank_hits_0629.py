"""6/29 풀 내 급등주 순위 분포."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src import config, news, stocks, train_snapshot, trading_calendar
from src.learning import support as theme_support
from src.prediction import ml_move_rank, prediction_ranking

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
fp = train_snapshot.fingerprint() if snap else "none"

bundle = ml_move_rank.fit_or_load_classifier(
    train_events=events,
    returns_ml=returns_ml,
    news_by_calendar=news_by,
    listing_names=names,
    fp=fp,
    label_before_exclusive=T,
    force_retrain=False,
)
tw = theme_support.weights_for_observation_day(
    T,
    train_events=events,
    news_by_calendar=news_by,
    returns_df=returns_ml,
    threshold=config.BIG_MOVE_THRESHOLD,
)
preds = ml_move_rank.rank_predictions_ml(
    target_day=T,
    listing_codes=codes,
    listing_names=names,
    train_events=events,
    news_text_blob=blob,
    returns_ml=returns_ml,
    pipeline=(bundle or {}).get("pipeline"),
    theme_weights=tw,
    ml_bundle=bundle,
    top_n=40,
)

rank_map = {str(p.code).zfill(6): i for i, p in enumerate(preds, 1)}
sl = returns_ml[returns_ml["Date"] == pd.Timestamp(T)]
big = sl[sl["return_pct"] >= 0.20].copy()
big["code"] = big["Code"].astype(str).str.zfill(6)

print("IN POOL — rank of actual 20%+ movers:")
for _, r in big.sort_values("return_pct", ascending=False).iterrows():
    c = r["code"]
    if c not in rank_map:
        continue
    nm = (names.get(c, "") or "")[:8]
    print(
        f"  rank #{rank_map[c]:2d} {c} {nm:8s} "
        f"act={float(r['return_pct'])*100:5.1f}% tier={next(p.confidence_tier for p in preds if str(p.code).zfill(6)==c)}"
    )

print("\nIN POOL but NOT in top40:")
for _, r in big.sort_values("return_pct", ascending=False).iterrows():
    c = r["code"]
    from src.prediction import predict
    ctx = predict.build_scoring_context(blob, events)
    cands = set(ml_move_rank._day_candidate_codes(codes, names, blob, ctx, returns_ml, T))
    if c in cands and c not in rank_map:
        nm = (names.get(c, "") or "")[:8]
        print(f"  MISS top40 {c} {nm} act={float(r['return_pct'])*100:.1f}%")
