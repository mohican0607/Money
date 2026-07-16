"""Deeper week-tab / preserved-section integrity checks."""
from __future__ import annotations

import re
from pathlib import Path

from src.report.render import extract_monthly_report_day_sections

path = Path(r"E:\Git\Money\output\report_2026.07.html")
html = path.read_text(encoding="utf-8")

print("week-tabs-wrap positions:")
for m in re.finditer("week-tabs-wrap", html):
    print(" ", m.start(), repr(html[m.start() - 40 : m.start() + 60]))

secs = extract_monthly_report_day_sections(path)
for d in sorted(secs):
    frag = secs[d]
    # length and whether closes properly; count nested section tags
    opens = len(re.findall(r"<section\b", frag, flags=re.I))
    closes = len(re.findall(r"</section>", frag, flags=re.I))
    # does frag end with </section>?
    end_ok = frag.rstrip().endswith("</section>")
    # table present?
    has_table = "rows-compare" in frag
    print(
        f"{d.isoformat()} len={len(frag)} sections={opens}/{closes} "
        f"end_ok={end_ok} table={has_table}"
    )

# Check CSS for tab-panel near end of head
css = re.findall(r"\.tab-panel[^{]*\{[^}]+\}", html)
print("css rules", css[:5])

# Check if multiple scripts bind tabs
print("show(i) scripts", html.count("function show(i)"))
print("week-tabs-wrap querySelector", html.count('querySelector(".week-tabs-wrap")'))

# Verify panel day membership by slicing between data-tab-panel markers
for i in range(4):
    m = re.search(
        rf'<div class="tab-panel[^"]*" role="tabpanel" data-tab-panel="{i}"[^>]*>(.*?)</div>\s*(?:<div class="tab-panel|</section>\s*<div class="tab-bar)',
        html,
        re.S,
    )
    if not m:
        # fallback: find start and next panel
        starts = [x.start() for x in re.finditer(r'<div class="tab-panel', html)]
        print(f"panel {i} regex fail; starts={starts}")
        continue
    body = m.group(1)
    days = re.findall(r'id="day-(\d{4}-\d{2}-\d{2})"', body)
    print(f"panel {i} days={days} body_len={len(body)}")
