"""2026-06-29 예측 순위·Hit@K 빠른 점검(캐시 OHLCV)."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src import config, news, stocks, train_snapshot, trading_calendar
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

from src import predict
from src.learning import support as theme_carryover

cands = ml_move_rank._day_candidate_codes(
    codes, names, blob, predict.build_scoring_context(blob, events),
    returns_ml, T,
)
sl = returns_ml[returns_ml["Date"] == pd.Timestamp(T)]
big = sl[sl["return_pct"] >= 0.20]
in_pool = sum(1 for _, r in big.iterrows() if str(r["Code"]).zfill(6) in set(cands))
print(f"T={T} candidates={len(cands)} actual20+={len(big)} in_pool={in_pool}/{len(big)}")

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
tw = theme_carryover.weights_for_observation_day(
    T, train_events=events, news_by_calendar=news_by, returns_df=returns_ml, threshold=config.BIG_MOVE_THRESHOLD,
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
)
ranked = [str(p.code).zfill(6) for p in preds]
actual = {str(r["Code"]).zfill(6) for _, r in big.iterrows()}
metrics = prediction_ranking.compute_hit_at_k_metrics(
    ranked, actual, universe_size=len(codes)
)
print("Hit@K", {k: metrics[k] for k in metrics if str(k).startswith("hit_at")})
print("Top10:")
for i, p in enumerate(preds[:10], 1):
    c6 = str(p.code).zfill(6)
    act = float(sl[sl["Code"].astype(str).str.zfill(6) == c6].iloc[0]["return_pct"]) * 100 if c6 in actual else None
    ind = stocks.industry_name_for_code(c6) or "?"
    hit = "HIT" if c6 in actual else "miss"
    print(
        f"  #{i} {c6} {names.get(c6,'')[:10]:10s} {ind[:8]:8s} "
        f"tier={p.confidence_tier} act={act:.1f}% {hit}" if act is not None
        else f"  #{i} {c6} {names.get(c6,'')[:10]:10s} {ind[:8]:8s} tier={p.confidence_tier}"
    )
