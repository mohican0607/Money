"""Show full market-theme-ref region and locate internal </div> tags."""
from __future__ import annotations

import re
from pathlib import Path

html = Path(r"E:\Git\Money\output\report_2026.07.html").read_text(encoding="utf-8")
i = html.find('id="day-2026-07-01"')
chunk = html[i : i + 25000]
# Find all </div> between market-theme-ref open and hit-at-k-panel
a = chunk.find('class="market-theme-ref"')
b = chunk.find('class="hit-at-k-panel"')
region = chunk[a:b]
print("region len", len(region))
print("div opens", len(re.findall(r"<div\b", region, re.I)))
print("div closes", region.count("</div>"))
# list each </div> with 80 chars before
for m in re.finditer(r"</div>", region):
    print("--- CLOSE @", m.start(), repr(region[max(0, m.start() - 100) : m.start() + 6]))
