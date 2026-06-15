"""파이프라인 수준: 6/8~6/11 비교표 25%+ 행 테마 배정 재현·누락 원인."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import config, snapshot_rebuild_learning as srl, stocks, news, trading_calendar  # noqa: E402


def _load_news_range(start: date, end: date) -> dict:
    out: dict = {}
    d = start
    while d <= end:
        p = news.day_news_json_path(d)
        if p.is_file():
            out[d] = json.loads(p.read_text(encoding="utf-8"))
        d += __import__("datetime").timedelta(days=1)
    return out


def main() -> None:
    days = [
        date(2026, 6, 8),
        date(2026, 6, 9),
        date(2026, 6, 10),
        date(2026, 6, 11),
    ]
    cache = config.CACHE_DIR / "ohlcv_long_full.parquet"
    if not cache.is_file():
        cache = config.CACHE_DIR / "ohlcv_long.parquet"
    returns = pd.read_parquet(cache)
    listing = stocks.load_listing()
    names = {
        str(r["Code"]).zfill(6): str(r["Name"])
        for _, r in listing.iterrows()
    }
    ws = days[0] - __import__("datetime").timedelta(days=14)
    we = days[-1] + __import__("datetime").timedelta(days=3)
    news_by = _load_news_range(ws, we)

    theme_rows = srl.build_market_theme_flow(days, news_by, returns, names)
    by_day = {str(r["trading_day"]): r for r in theme_rows}

    # simulate compare rows from OHLCV 25%+ that day
    for t in days:
        row = by_day.get(t.isoformat())
        if not row:
            print(t, "no flow row")
            continue
        day_slice = returns[returns["Date"] == pd.Timestamp(t)]
        ge25 = day_slice[day_slice["return_pct"] + 1e-9 >= srl._STRONG_MOVER_RET_DECIMAL]
        early = srl._early_news_rows_for_trading_day(news_by, t)
        missing = []
        for _, sr in ge25.iterrows():
            code = str(sr["Code"]).zfill(6)
            cr = {
                "code": code,
                "name": str(sr.get("Name") or names.get(code, "")),
                "actual_ret": float(sr["return_pct"]),
                "pred_ret": float(sr["return_pct"]) * 100.0,
                "keywords": [],
                "prediction_signal_html": "x",
            }
            patched = dict(row)
            srl._patch_flow_row_strong_movers_from_compare(
                patched, [cr], names, early
            )
            sectors = srl._theme_labels_for_compare_row(
                patched,
                cr,
                listing_names=names,
                early_rows=early,
                seeds=list(patched.get("theme_seed_hits") or []),
                top_kw=list(patched.get("top_keywords_sample") or []),
            )
            if not sectors:
                missing.append((code, names.get(code, code)))
        leaders = row.get("theme_leaders_ge_25pct") or []
        lim = row.get("theme_limit_up") or []
        print(
            f"{t} OHLCV25+={len(ge25)} flow25+={len(leaders)} "
            f"limit_up={len(lim)} resolver_missing={len(missing)}"
        )
        if missing:
            print("  missing:", missing[:10])


if __name__ == "__main__":
    main()
