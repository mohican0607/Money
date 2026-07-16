"""Locate premature </div> that closes week tab-panel too early."""
from __future__ import annotations

import re
from pathlib import Path

html = Path(r"E:\Git\Money\output\report_2026.07.html").read_text(encoding="utf-8")
m = re.search(
    r'<section class="tabs-wrap week-tabs-wrap">(.*?)</section>\s*<script>',
    html,
    re.S,
)
body = m.group(1)
# first tab-panel open
p0 = re.search(r'<div class="tab-panel\b[^>]*>', body)
assert p0
start = p0.start()
# walk div depth from start, record when depth hits 0
depth = 0
j = start
events = []
while j < len(body):
    if body.startswith("<div", j) and not body.startswith("<div", j):  # noop
        pass
    if re.match(r"<div\b", body[j:], re.I):
        depth += 1
        tag_end = body.find(">", j)
        snippet = body[j : min(j + 80, tag_end + 1)]
        if depth <= 3:
            events.append(("OPEN", depth, j, snippet.replace("\n", " ")))
        j = tag_end + 1
        continue
    if body.startswith("</div>", j):
        depth -= 1
        ctx = body[max(0, j - 60) : j + 6].replace("\n", " ")
        if depth <= 2:
            events.append(("CLOSE", depth, j, ctx))
        j += 6
        if depth == 0:
            events.append(("PANEL_END", 0, j, body[max(0, j - 120) : j + 80].replace("\n", " ")))
            break
        continue
    j += 1

print("first panel ended at", j, "body offset from wrap", j)
print("events near end:")
for ev in events[-25:]:
    print(ev)

# Show what's around panel end
print("\nAROUND PANEL END:")
print(body[j - 200 : j + 200])

# Count div open/close inside first day section only
day1 = re.search(
    r'<section[^>]*id="day-2026-07-01"[^>]*>(.*?)</section>',
    body,
    re.S | re.I,
)
if day1:
    frag = day1.group(0)
    opens = len(re.findall(r"<div\b", frag, re.I))
    closes = frag.count("</div>")
    print("\nday-2026-07-01 div open/close", opens, closes, "delta", opens - closes)
