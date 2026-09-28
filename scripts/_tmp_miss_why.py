# -*- coding: utf-8 -*-
"""Why actual 20% movers were missing from predictions."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src import config, trading_calendar  # noqa: E402
from src.diagnostics.prediction import load_news_by_calendar  # noqa: E402
from src.features import filter_specific_keywords, keyword_set, name_mention_score  # noqa: E402
from src.news import aggregate_early_late_for_target  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")

freeze = json.loads(config.PREDICTION_FREEZE_PATH.read_text(encoding="utf-8"))
ohlcv = pd.read_parquet(ROOT / "data" / "cache" / "ohlcv_long_full.parquet")
ohlcv["Date"] = pd.to_datetime(ohlcv["Date"]).dt.normalize()
ohlcv["Code"] = ohlcv["Code"].astype(str).str.zfill(6)

days = [date.fromisoformat(x) for x in ["2026-09-21", "2026-09-22", "2026-09-23"]]
cal_start = days[0] - timedelta(days=5)
cal_end = days[-1]
print("loading news...", cal_start, cal_end)
news_by_cal = load_news_by_calendar(cal_start, cal_end)
print("news days", sorted(news_by_cal)[:3], "...", len(news_by_cal))

for T in days:
    day_df = ohlcv[ohlcv["Date"].dt.date == T]
    big = day_df[day_df["Change"] >= 0.20].sort_values("Change", ascending=False)
    pred_codes = {str(r.get("code", "")).zfill(6) for r in freeze.get(T.isoformat(), [])}
    print(f"\n==== {T.isoformat()} actual20={len(big)} pred_n={len(pred_codes)}")

    blob, _ = aggregate_early_late_for_target(news_by_cal, T)
    kw_news = filter_specific_keywords(keyword_set(blob or "", k=150))
    print(" news blob chars", len(blob or ""), "kw", len(kw_news), "sample", list(kw_news)[:15])

    try:
        prev = trading_calendar.last_trading_day_before(T)
    except ValueError:
        prev = None

    mention_gt0 = 0
    for _, row in big.iterrows():
        code = str(row["Code"]).zfill(6)
        name = str(row["Name"])
        mention = name_mention_score(blob or "", name)
        if mention >= 0.35:
            mention_gt0 += 1
        name_toks = filter_specific_keywords(keyword_set(name, k=20))
        inter = list(name_toks & kw_news)[:4]
        prev_ret = None
        if prev is not None:
            sub = ohlcv[(ohlcv["Code"] == code) & (ohlcv["Date"].dt.date == prev)]
            if not sub.empty:
                prev_ret = float(sub.iloc[-1]["Change"]) * 100
        flag = "IN" if code in pred_codes else "OUT"
        print(
            f"  {flag} {code} {name} act={float(row['Change'])*100:.1f}% "
            f"mention={mention:.2f} tok={inter} prev={None if prev_ret is None else round(prev_ret,1)}"
        )
    print(f"  movers with mention>=0.35: {mention_gt0}/{len(big)}")
