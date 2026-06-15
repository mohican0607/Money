"""6/8~11 누락 종목 코드에 대해 현재 테마 resolver 재현."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import config, snapshot_rebuild_learning as srl, stocks, news  # noqa: E402

MISSING_BY_DAY: dict[str, list[str]] = {
    "2026-06-08": [
        "040350", "271830", "291230", "291810", "417860",
        "131760", "181710", "184230", "950250",
    ],
    "2026-06-09": [
        "000430", "002680", "084650", "089970", "115610",
        "219130", "328380", "046970", "181710", "354320",
    ],
    "2026-06-10": ["079650", "083660"],
    "2026-06-11": [
        "027740", "032680", "070300", "073190", "083660",
        "085620", "097800", "131290", "183300", "195500",
        "347700", "465770", "950250",
    ],
}


def _load_news(start: date, end: date) -> dict:
    out: dict = {}
    d = start
    while d <= end:
        p = news.day_news_json_path(d)
        if p.is_file():
            out[d] = json.loads(p.read_text(encoding="utf-8"))
        d += timedelta(days=1)
    return out


def main() -> None:
    cache = config.CACHE_DIR / "ohlcv_long_full.parquet"
    ohlcv = __import__("pandas").read_parquet(cache)
    returns = stocks.daily_returns_table(ohlcv)
    listing = stocks.load_listing()
    names = {str(r["Code"]).zfill(6): str(r["Name"]) for _, r in listing.iterrows()}

    days = [date.fromisoformat(d) for d in MISSING_BY_DAY]
    news_by = _load_news(days[0] - timedelta(days=14), days[-1] + timedelta(days=3))
    theme_rows = srl.build_market_theme_flow(days, news_by, returns, names)
    by_day = {str(r["trading_day"]): r for r in theme_rows}

    still_empty = 0
    total = 0
    for day_iso, codes in MISSING_BY_DAY.items():
        t = date.fromisoformat(day_iso)
        row = by_day.get(day_iso)
        if not row:
            print(day_iso, "NO FLOW ROW")
            continue
        early = srl._early_news_rows_for_trading_day(news_by, t)
        day_slice = returns[returns["Date"] == __import__("pandas").Timestamp(t)]
        for code in codes:
            total += 1
            sr = day_slice[day_slice["Code"].astype(str).str.zfill(6) == code]
            act = float(sr.iloc[0]["return_pct"]) if not sr.empty else 0.27
            cr = {
                "code": code,
                "name": names.get(code, code),
                "actual_ret": act,
                "pred_ret": None,
                "keywords": [],
                "prediction_signal_html": "—",
            }
            patched = dict(row)
            sectors = srl._theme_labels_for_compare_row(
                patched,
                cr,
                listing_names=names,
                early_rows=early,
                seeds=list(patched.get("theme_seed_hits") or []),
                top_kw=list(patched.get("top_keywords_sample") or []),
            )
            if sectors:
                print(f"OK {day_iso} {code} {names.get(code,'')} -> {sectors[0]}")
            else:
                still_empty += 1
                print(f"MISS {day_iso} {code} {names.get(code,'')}")
    print(f"\nresolver: {total - still_empty}/{total} filled, still empty: {still_empty}")


if __name__ == "__main__":
    main()
