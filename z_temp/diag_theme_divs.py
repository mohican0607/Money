"""Locate extra </div> inside market-theme-ref for day 2026-07-01."""
from __future__ import annotations

import re
from pathlib import Path

html = Path(r"E:\Git\Money\output\report_2026.07.html").read_text(encoding="utf-8")
# extract market-theme-ref for day 7/01 area: from first market-theme-ref after day-2026-07-01
i = html.find('id="day-2026-07-01"')
assert i > 0
chunk = html[i : i + 20000]
m = re.search(r'<div class="market-theme-ref"[^>]*>', chunk)
assert m
start = m.start()
# walk and find when depth returns to 0 relative to this div — and dump extra closes
depth = 0
j = start
extras = []
while j < len(chunk):
    if re.match(r"<div\b", chunk[j:], re.I):
        depth += 1
        j = chunk.find(">", j) + 1
        continue
    if chunk.startswith("</div>", j):
        depth -= 1
        if depth < 0:
            extras.append((j, chunk[max(0, j - 150) : j + 20]))
            depth = 0  # resync as browser might
            j += 6
            continue
        if depth == 0:
            print("market-theme-ref closed at", j)
            print("tail", chunk[j - 120 : j + 80])
            break
        j += 6
        continue
    j += 1
print("extras while parsing", len(extras))
for e in extras[:5]:
    print("EXTRA CLOSE ctx:", repr(e[1]))

# Net div balance of market_theme_html alone: between after heading-row close and before final theme-ref close
inner_start = chunk.find(">", start) + 1
# find matching end of market-theme-ref using naive — use the closed-at above
inner = chunk[inner_start:j]
opens = len(re.findall(r"<div\b", inner, re.I))
closes = inner.count("</div>")
print("inner div open/close", opens, closes, "delta", opens - closes)
# show last 500 chars of inner
print("INNER TAIL:\n", inner[-800:])
