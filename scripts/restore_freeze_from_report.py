"""Restore 2026-08-06 prediction freeze from report_2026.0805-2.html backup."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config
from src.pipeline.support import _load_prediction_freeze_payload, _save_prediction_freeze_payload

BACKUP = config.OUTPUT_DIR / "report_2026.0805-2.html"
T_KEY = "2026-08-06"


def _parse_day_predictions(html: str, day: str) -> list[dict]:
    m = re.search(rf'id="day-{day}".*?(?=id="day-|$)', html, re.S)
    if not m:
        raise SystemExit(f"day section not found: {day}")
    chunk = m.group(0)
    rows: list[dict] = []
    for tr in re.finditer(r"<tr[^>]*data-market=[^>]*>(.*?)</tr>", chunk, re.S):
        block = tr.group(1)
        code_m = re.search(r'data-stock-code="(\d{6})"', block)
        pred_m = re.search(r'data-sort-col="pred"[^>]*data-sort-value="([^"]+)"', block)
        name_m = re.search(r'data-sort-value="([^"]+ \d{6})"', block)
        if not code_m or not pred_m:
            continue
        pred_raw = pred_m.group(1).strip()
        if not pred_raw:
            continue
        code = code_m.group(1)
        name = code
        if name_m:
            name = name_m.group(1).rsplit(" ", 1)[0]
        tier = "mid" if "중확신" in block else "none"
        rank_m = re.search(r"#(\d+)", block)
        rank_pos = int(rank_m.group(1)) if rank_m else len(rows) + 1
        reason_m = re.search(
            r'forward-pred-narrative[^>]*><p[^>]*>(.*?)</p>',
            block,
            re.S,
        )
        reason = re.sub(r"<[^>]+>", "", reason_m.group(1)).strip() if reason_m else ""
        rows.append(
            {
                "code": code,
                "name": name,
                "score": 1.0,
                "predicted_return_pct": float(pred_raw),
                "matched_keywords": [],
                "reasons": [reason] if reason else [],
                "ml_prob": None,
                "ml_precision_score": None,
                "keyword_hits": 0,
                "mention_score": 0.0,
                "rank_score": None,
                "rank_position": rank_pos,
                "confidence_tier": tier,
                "investor_flow_score": 0.0,
                "foreign_net_vol_ratio": 0.0,
            }
        )
    return rows


def main() -> None:
    if not BACKUP.is_file():
        raise SystemExit(f"backup missing: {BACKUP}")
    html = BACKUP.read_text(encoding="utf-8", errors="replace")
    items = _parse_day_predictions(html, T_KEY)
    if not items:
        raise SystemExit("no prediction rows parsed")
    payload = _load_prediction_freeze_payload()
    old = payload.get(T_KEY, [])
    payload[T_KEY] = items
    _save_prediction_freeze_payload(payload)
    print(f"restored {T_KEY}: {len(old)} -> {len(items)} items")
    for x in items:
        print(f"  {x['code']} {x['name']} tier={x['confidence_tier']} pred={x['predicted_return_pct']}%")


if __name__ == "__main__":
    main()
