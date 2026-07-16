"""Check for orphan content outside week tab-panels (browser-visible weirdness)."""
from __future__ import annotations

import re
from pathlib import Path

html = Path(r"E:\Git\Money\output\report_2026.07.html").read_text(encoding="utf-8")

# Isolate week-tabs-wrap inner HTML up to its closing script
m = re.search(
    r'<section class="tabs-wrap week-tabs-wrap">(.*?)</section>\s*<script>',
    html,
    re.S,
)
assert m, "no wrap"
body = m.group(1)

# Split by top-level tab-panel starts
starts = [x.start() for x in re.finditer(r'<div class="tab-panel\b', body)]
print("panel starts in body", starts)

# Content before first panel (should be h2, p, tab-bar only)
pre = body[: starts[0]]
print("PRE len", len(pre))
print("PRE has day-id?", bool(re.search(r'id="day-', pre)))
print("PRE snippet", pre[:500].replace("\n", " | "))

# Between panels: from end of panel i to start of panel i+1
# Approximate panel end: next panel start. Check for day-ids BETWEEN markers outside panels.
# Better: remove all tab-panel blocks with balanced parse.

def extract_div_blocks(s: str, start_pat: str) -> list[tuple[int, int, str]]:
    out = []
    for m in re.finditer(start_pat, s):
        i = m.start()
        # find matching close from opening <div
        depth = 0
        j = i
        while j < len(s):
            if s.startswith("<div", j):
                # check it's a div open (not </div)
                depth += 1
                j = s.find(">", j) + 1
                continue
            if s.startswith("</div>", j):
                depth -= 1
                j += 6
                if depth == 0:
                    out.append((i, j, s[i:j]))
                    break
                continue
            j += 1
        else:
            out.append((i, len(s), s[i:]))
    return out


blocks = extract_div_blocks(body, r'<div class="tab-panel\b')
print("balanced panels", len(blocks))
for i, (a, b, blk) in enumerate(blocks):
    days = re.findall(r'id="day-(\d{4}-\d{2}-\d{2})"', blk)
    active = "active" in blk[:80]
    print(f"  panel{i} active={active} days={days} len={len(blk)}")

# Orphans: remove panels from body, see leftover day sections
stripped = body
for a, b, _ in reversed(blocks):
    stripped = stripped[:a] + stripped[b:]
orphan_days = re.findall(r'id="day-(\d{4}-\d{2}-\d{2})"', stripped)
print("orphan day ids outside panels", orphan_days)
# leftover significant text
textish = re.sub(r"<[^>]+>", " ", stripped)
textish = re.sub(r"\s+", " ", textish).strip()
print("stripped leftover text len", len(textish))
print("stripped leftover sample", textish[:300])
