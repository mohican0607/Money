"""June 2026 Hit@K backtest (cached OHLCV, no freeze)."""
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
from src.prediction import ml_move_rank, predict, prediction_ranking

JUNE_DAYS = [
    d
    for d in (
        date(2026, 6, d)
        for d in (
            1, 2, 4, 5, 8, 9, 10, 11, 12, 15, 16, 17, 18, 19, 22, 23, 24, 25, 26, 29, 30
        )
    )
    if trading_calendar.is_trading_day(d)
]


def main() -> None:
    ohlcv = pd.read_parquet(config.CACHE_DIR / "ohlcv_long_full.parquet")
    returns_ml = stocks.enrich_daily_returns_for_ml(stocks.daily_returns_table(ohlcv))
    listing = stocks.load_listing()
    codes = listing["Code"].astype(str).str.zfill(6).tolist()
    names = {str(r["Code"]).zfill(6): str(r["Name"]) for _, r in listing.iterrows()}
    snap = train_snapshot.load_snapshot(config.TRAIN_SNAPSHOT_PATH)
    events = snap.events if snap else []
    fp = train_snapshot.fingerprint()

    news_by: dict = {}
    d = date(2026, 5, 1)
    while d <= date(2026, 6, 30):
        p = news.day_news_json_path(d)
        if p.is_file():
            news_by[d] = json.loads(p.read_text(encoding="utf-8"))
        d += timedelta(days=1)

    ks = [5, 10, 20, 40]
    sums = {k: 0 for k in ks}
    n_days = 0
    per_day: list[tuple[str, dict]] = []

    for T in JUNE_DAYS:
        prev = trading_calendar.last_trading_day_before(T)
        blob, _ = news.aggregate_early_late_for_target(news_by, prev)
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
            top_n=max(config.PRED_RANK_POOL_N, max(config.PRED_EVAL_HIT_AT_K)),
        )
        ranked_codes = [str(p.code).zfill(6) for p in preds]
        sl = returns_ml[returns_ml["Date"] == pd.Timestamp(T)]
        actual = {
            str(r["Code"]).zfill(6)
            for _, r in sl.iterrows()
            if float(r.get("return_pct") or 0) >= config.BIG_MOVE_THRESHOLD
        }
        if not actual:
            continue
        m = prediction_ranking.compute_hit_at_k_metrics(
            ranked_codes, actual, universe_size=len(codes)
        )
        n_days += 1
        line = {k: m.get(f"hit_at_{k}", 0) for k in ks}
        per_day.append((T.isoformat(), line))
        for k in ks:
            sums[k] += int(line[k])

    print(f"ML v{ml_move_rank.ML_MODEL_VERSION} | days={n_days}")
    for k in ks:
        print(f"  Hit@{k} avg {sums[k] / max(1, n_days):.2f}/day  total {sums[k]}")
    print("worst @10:")
    for iso, line in sorted(per_day, key=lambda x: x[1][10])[:5]:
        print(f"  {iso} hit@10={line[10]}")
    print("best @10:")
    for iso, line in sorted(per_day, key=lambda x: -x[1][10])[:5]:
        print(f"  {iso} hit@10={line[10]}")


if __name__ == "__main__":
    main()
