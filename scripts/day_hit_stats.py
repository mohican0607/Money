"""리포트 일자별 예측≥20% 적중 통계."""
from __future__ import annotations

import re
import sys
from html import unescape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.import_freeze_from_report import _section_html


def stats(html_path: Path, day_key: str) -> None:
    sec = _section_html(html_path.read_text(encoding="utf-8"), f"day-{day_key}")
    summary = re.search(
        r"당일 예측≥20%.*?임계 적중.*?(\d+)</strong>/(\d+)",
        sec,
        re.S,
    )
    if summary:
        hits, total = summary.group(1), summary.group(2)
        print(f"\n=== {day_key}  적중 {hits}/{total} (예측≥20%) ===")
    else:
        print(f"\n=== {day_key} ===")

    tbody = re.search(r"<tbody>(.*?)</tbody>", sec, re.S)
    if not tbody:
        print("  (no tbody)")
        return

    rows_out: list[tuple] = []
    for row in re.findall(r"<tr[^>]*>.*?</tr>", tbody.group(1), re.S):
        if 'data-sort-col="pred"' not in row:
            continue
        code_m = re.search(r'data-stock-code="([^"]+)"', row)
        pred_m = re.search(r'data-sort-col="pred"\s+data-sort-value="([^"]+)"', row)
        act_m = re.search(r'data-sort-col="actual"\s+data-sort-value="([^"]*)"', row)
        if not code_m or not pred_m:
            continue
        pred = float(pred_m.group(1))
        if pred < 20:
            continue
        act_raw = (act_m.group(1) if act_m else "").strip()
        act = float(act_raw) if act_raw not in ("", "None") else None
        if act is not None and abs(act) <= 1.5:
            act_pct = act * 100.0
        else:
            act_pct = act
        name_m = re.search(r'<a class="stock"[^>]*>([^<]+)</a>', row)
        name = unescape(name_m.group(1)) if name_m else code_m.group(1)
        hit = act_pct is not None and act_pct >= 20.0
        rows_out.append((code_m.group(1), name, pred, act_pct, hit))

    rows_out.sort(key=lambda x: (-x[2], x[0]))
    for code, name, pred, act, hit in rows_out:
        tag = "HIT" if hit else ("MISS" if act is not None else "PEND")
        act_s = f"{act:.1f}%" if act is not None else "—"
        print(f"  [{tag}] {name}({code}) pred={pred:.1f}% actual={act_s}")


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "output" / "report_2026.07.html"
    days = sys.argv[2:] if len(sys.argv) > 2 else [
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-06",
    ]
    for d in days:
        stats(path, d)
