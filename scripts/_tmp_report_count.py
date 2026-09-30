# -*- coding: utf-8 -*-
"""Per-day: report table rows vs freeze preds vs actual 20%+ movers."""
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src import config  # noqa: E402

sys.stdout.reconfigure(encoding="utf-8")

html = (config.OUTPUT_DIR / "report_2026.09.html").read_text(encoding="utf-8")
freeze = json.loads(config.PREDICTION_FREEZE_PATH.read_text(encoding="utf-8"))
ohlcv = pd.read_parquet(ROOT / "data" / "cache" / "ohlcv_long_full.parquet")
ohlcv["Date"] = pd.to_datetime(ohlcv["Date"]).dt.normalize()
ohlcv["Code"] = ohlcv["Code"].astype(str).str.zfill(6)

parts = re.split(r"(<h2[^>]*>.*?</h2>)", html, flags=re.S)
table_codes: dict[str, set[str]] = {}
cur = None
for p in parts:
    if "<h2" in p:
        m = re.search(r"2026-09-\d{2}", p)
        cur = m.group(0) if m else None
        continue
    if cur and "<tbody>" in p:
        body = re.search(r"<tbody>(.*?)</tbody>", p, re.S)
        if body:
            table_codes[cur] = set(re.findall(r'data-stock-code="(\d{6})"', body.group(1)))
        cur = None

tot_hit = tot_pred = 0
for k in sorted(table_codes):
    if k not in table_codes:
        continue
    day = date.fromisoformat(k)
    d = ohlcv[ohlcv["Date"].dt.date == day]
    big = d[d["Change"] >= config.BIG_MOVE_THRESHOLD]
    big_codes = set(big["Code"])
    preds = {str(r.get("code", "")).zfill(6) for r in freeze.get(k, [])}
    tc = table_codes[k]
    missing = big_codes - tc
    hits = preds & big_codes
    if big_codes:
        tot_hit += len(hits)
        tot_pred += len(preds)
    print(
        f"{k}: table={len(tc)} preds={len(preds)} preds_in_table={len(preds & tc)} actual20={len(big_codes)} "
        f"actual_in_table={len(big_codes & tc)} missing={sorted(missing)} hit={len(hits)}"
    )
print(f"TOTAL hit {tot_hit}/{tot_pred}")
