"""report HTML 일자 섹션에서 prediction_freeze_by_t.json 항목을 복원합니다."""
from __future__ import annotations

import json
import re
import sys
from html import unescape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config
from src.pipeline.support import _load_prediction_freeze_payload, _save_prediction_freeze_payload


def _section_html(html: str, day_id: str) -> str:
    marker = f'id="{day_id}"'
    start = html.find(marker)
    if start < 0:
        raise ValueError(f"section not found: {day_id}")
    end = html.find('<section class="day-stack', start + len(marker))
    if end < 0:
        end = html.find("</section>", start)
        while True:
            nxt = html.find("</section>", end + len("</section>"))
            if nxt < 0:
                break
            chunk = html[start:nxt + len("</section>")]
            if chunk.count("<section") <= chunk.count("</section>"):
                end = nxt
                break
            end = nxt
    else:
        end = html.rfind("</section>", start, end)
    return html[start : end + len("</section>")]


def _confidence_tier(row_html: str) -> str:
    if "고확신" in row_html:
        return "high"
    if "중확신" in row_html:
        return "mid"
    return "none"


def _reason_text(row_html: str) -> str:
    m = re.search(
        r'<div class="pred-reason-inline"[^>]*>(.*?)</div>',
        row_html,
        re.S,
    )
    if not m:
        return ""
    text = re.sub(r"<[^>]+>", " ", m.group(1))
    return re.sub(r"\s+", " ", unescape(text)).strip()


def parse_day_section(section_html: str) -> list[dict]:
    tbody_m = re.search(r"<tbody>(.*?)</tbody>", section_html, re.S)
    if not tbody_m:
        return []
    items: list[dict] = []
    for row_html in re.findall(r"<tr[^>]*>.*?</tr>", tbody_m.group(1), re.S):
        code_m = re.search(r'data-stock-code="([^"]+)"', row_html)
        pred_m = re.search(
            r'data-sort-col="pred"\s+data-sort-value="([^"]+)"',
            row_html,
        )
        name_m = re.search(r'<a class="stock"[^>]*>([^<]+)</a>', row_html)
        if not code_m or not pred_m or not name_m:
            continue
        pred = float(pred_m.group(1))
        if not (pred == pred):  # NaN
            continue
        tier = _confidence_tier(row_html)
        reason = _reason_text(row_html)
        items.append(
            {
                "code": str(code_m.group(1)).zfill(6),
                "name": unescape(name_m.group(1)).strip(),
                "score": pred / 100.0,
                "predicted_return_pct": pred,
                "matched_keywords": [],
                "reasons": [reason] if reason else [],
                "ml_prob": None,
                "keyword_hits": 0,
                "mention_score": 0.0,
                "rank_score": None,
                "rank_position": None,
                "confidence_tier": tier,
                "investor_flow_score": 0.0,
                "foreign_net_vol_ratio": 0.0,
            }
        )
    return items


def main() -> None:
    report_path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "output" / "report_2026.0706-1.html"
    day_key = sys.argv[2] if len(sys.argv) > 2 else "2026-07-06"
    day_id = f"day-{day_key}"

    html = report_path.read_text(encoding="utf-8")
    section = _section_html(html, day_id)
    items = parse_day_section(section)
    if not items:
        raise SystemExit(f"no prediction rows parsed from {report_path} / {day_id}")

    payload = _load_prediction_freeze_payload()
    payload[day_key] = items
    _save_prediction_freeze_payload(payload)

    codes = [f"{x['name']}({x['code']})" for x in items]
    print(f"freeze saved: {config.PREDICTION_FREEZE_PATH}")
    print(f"T={day_key} rows={len(items)}")
    for line in codes:
        print(f"  - {line}")


if __name__ == "__main__":
    main()
