"""Compare prediction rows for one day between two monthly report HTML files."""
from __future__ import annotations

import re
import sys
from datetime import date
from html import unescape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import report  # noqa: E402


def extract_day_rows(section_html: str) -> list[tuple[str, str, str, str]]:
    out: list[tuple[str, str, str, str]] = []
    for tr in re.findall(r"<tr\b[^>]*>.*?</tr>", section_html, re.DOTALL | re.IGNORECASE):
        code_m = re.search(r"<code>(\d{6})</code>", tr)
        if not code_m:
            continue
        code = code_m.group(1)
        name_m = re.search(r'class="stock"[^>]*>([^<]+)<', tr)
        name = unescape(name_m.group(1)).strip() if name_m else ""
        pred_m = re.search(
            r'data-sort-col="pred"[^>]*data-sort-value="([^"]*)"',
            tr,
        )
        pr = pred_m.group(1) if pred_m else ""
        tier = ""
        if "고확신" in tr:
            tier = "high"
        elif "중확신" in tr:
            tier = "mid"
        out.append((code, name, pr, tier))
    return out


def main() -> None:
    day_s = sys.argv[1] if len(sys.argv) > 1 else "2026-06-22"
    t_day = date.fromisoformat(day_s)
    a = ROOT / "output" / "report_2026.06-22-1.html"
    b = ROOT / "output" / "report_2026.06.html"
    sa = report.extract_monthly_report_day_sections(a).get(t_day, "")
    sb = report.extract_monthly_report_day_sections(b).get(t_day, "")
    ra = extract_day_rows(sa)
    rb = extract_day_rows(sb)
    print(f"A {a.name}: {len(ra)} rows")
    print(f"B {b.name}: {len(rb)} rows")
    ha = {c for c, _, _, t in ra if t == "high"}
    hb = {c for c, _, _, t in rb if t == "high"}
    print(f"high A={len(ha)} B={len(hb)}")
    print("only A high:", sorted(ha - hb))
    print("only B high:", sorted(hb - ha))
    ma = {c: (n, pr, t) for c, n, pr, t in ra}
    mb = {c: (n, pr, t) for c, n, pr, t in rb}
    changed = []
    for c in sorted(set(ma) | set(mb)):
        if ma.get(c) != mb.get(c):
            changed.append((c, ma.get(c), mb.get(c)))
    print(f"changed rows: {len(changed)}")
    for c, va, vb in changed:
        print(f"  {c}: A={va} -> B={vb}")


if __name__ == "__main__":
    main()
