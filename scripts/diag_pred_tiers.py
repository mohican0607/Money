"""Quick tier diagnostic for one trading day."""
from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src import config, ml_move_rank, predict, stocks, news, train_snapshot  # noqa: E402


def main() -> None:
    t = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2026, 6, 19)
    snap = train_snapshot.load_snapshot()
    if snap is None:
        raise SystemExit(f"스냅샷 없음: {config.TRAIN_SNAPSHOT_PATH}")
    events = list(snap.events)
    returns = pd.read_parquet(config.CACHE_DIR / "ohlcv_long_full.parquet")
    returns_ml = stocks.enrich_daily_returns_for_ml(stocks.daily_returns_table(returns))
    codes, names = stocks.listing_codes_and_names()
    fp = ml_move_rank.train_snapshot_fingerprint(events)
    bundle = ml_move_rank.fit_or_load_classifier(
        train_events=events,
        returns_ml=returns_ml,
        news_by_calendar={},
        listing_names=names,
        fp=fp,
        label_before_exclusive=t,
        force_retrain=False,
    )
    news_by: dict = {}
    cal_start, cal_end = news.news_fetch_calendar_span(t)
    d = cal_start
    while d <= cal_end:
        p = config.CACHE_DIR / "news" / f"day_{d.strftime('%Y%m%d')}.json"
        if p.exists():
            news_by[d] = json.loads(p.read_text(encoding="utf-8"))
        d += timedelta(days=1)
    blob = predict.news_window_for_target_trading_day(news_by, t)
    preds = predict.predict_for_trading_day(
        t,
        codes,
        names,
        events,
        blob,
        top_n=config.PRED_RANK_POOL_N,
        min_keyword_hits=config.PRED_ML_POOL_MIN_KEYWORD_HITS,
        ml_bundle=bundle,
        returns_ml=returns_ml,
    )
    print(f"T={t} preds={len(preds)} tiers={dict(Counter(p.confidence_tier for p in preds))}")
    for p in preds[:12]:
        mp = f"{p.ml_prob:.3f}" if p.ml_prob is not None else "—"
        print(
            f"  {p.code} {p.name[:10]:10s} tier={p.confidence_tier:4s} "
            f"ml={mp} nh={p.keyword_hits} mom={p.momentum_score:.2f} rank={p.rank_position}"
        )


if __name__ == "__main__":
    main()
