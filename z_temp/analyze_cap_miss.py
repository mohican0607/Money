"""Analyze actual movers missed by ML score pool cap."""
from __future__ import annotations

from datetime import date

import pandas as pd

from src import config, stocks
from src.diagnostics.prediction import load_news_by_calendar
from src.learning import support as train_snapshot
from src.news import aggregate_early_late_for_target
from src.prediction import ml_move_rank, predict as pred_mod
from src.prediction.candidate_pool import day_candidate_codes, industry_must_keep_codes
from src.prediction.market_features import ohlcv_lookup

DAYS = [
    date(2026, 6, 10),
    date(2026, 6, 11),
    date(2026, 6, 16),
    date(2026, 6, 18),
]


def main() -> None:
    ohlcv = pd.read_parquet(config.CACHE_DIR / "ohlcv_long_full.parquet")
    ohlcv = ohlcv[ohlcv["Date"] >= "2025-11-01"]
    returns_ml = stocks.enrich_daily_returns_for_ml(stocks.daily_returns_table(ohlcv))
    listing = stocks.load_listing()
    names = {str(r["Code"]).zfill(6): str(r["Name"]) for _, r in listing.iterrows()}
    codes = list(names.keys())
    snap = train_snapshot.load_snapshot(config.TRAIN_SNAPSHOT_PATH)
    events = snap.events if snap else []
    news_by = load_news_by_calendar(date(2026, 5, 15), date(2026, 6, 30))
    ohlcv_idx = ohlcv_lookup(returns_ml)

    for T in DAYS:
        blob, _ = aggregate_early_late_for_target(news_by, T)
        ctx = pred_mod.build_scoring_context(blob, events)
        pool = day_candidate_codes(codes, names, blob, ctx, returns_ml, T)
        must = [
            str(c).zfill(6)
            for c in industry_must_keep_codes(returns_ml, T, codes)
        ]
        capped = set(
            ml_move_rank._cap_score_codes_for_ml(
                pool,
                cap=config.PRED_ML_SCORE_POOL_CAP,
                must_keep=must,
                target_day=T,
                ohlcv_idx=ohlcv_idx,
                kw_news=ctx[0],
                profile=ctx[1],
                news_blob=blob,
                listing_names=names,
            )
        )
        must_set = set(must)
        sl = returns_ml[returns_ml["Date"] == pd.Timestamp(T)]
        actual = sl.loc[sl["return_pct"] >= 0.20].copy()
        actual["c"] = actual["Code"].astype(str).str.zfill(6)
        hit = len(set(actual["c"]) & capped)
        print(f"\n=== {T} act={len(actual)} cap_hit={hit} must_n={len(must)} ===")
        for _, r in actual.sort_values("return_pct", ascending=False).iterrows():
            c = r["c"]
            if c in capped:
                continue
            in_pool = c in set(pool)
            in_must = c in must_set
            ps = None
            if in_pool:
                ps = ml_move_rank._cheap_prescore_code(
                    c,
                    target_day=T,
                    ohlcv_idx=ohlcv_idx,
                    kw_news=ctx[0],
                    profile=ctx[1],
                    news_blob=blob,
                    listing_names=names,
                )
            print(
                f"  MISS {c} {(names.get(c) or '')[:8]} "
                f"act={float(r['return_pct']) * 100:.1f}% "
                f"lag1={float(r.get('ret_lag1') or 0) * 100:.1f}% "
                f"vs={float(r.get('vol_surge_ratio') or 0):.2f} "
                f"gap={float(r.get('open_gap') or 0) * 100:.1f}% "
                f"pool={in_pool} must={in_must} ps={ps}"
            )


if __name__ == "__main__":
    main()
