"""Diagnose week-tab structure in monthly report HTML."""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

html = Path(r"E:\Git\Money\output\report_2026.07.html").read_text(encoding="utf-8")
print("len", len(html))
print("week-tabs-wrap", html.count("week-tabs-wrap"))
print("tab-panel class count", len(re.findall(r'class="tab-panel', html)))
print("tab-btn count", len(re.findall(r'class="tab-btn', html)))
panels = re.findall(r'data-tab-panel="(\d+)"', html)
print("data-tab-panel", panels, "n", len(panels))
labels = re.findall(r'data-tab-idx="\d+"[^>]*>([^<]+)</button>', html)
print("btn labels unique", list(dict.fromkeys(labels)))
days = re.findall(r'id="day-(\d{4}-\d{2}-\d{2})"', html)
print("day ids", days)
print("dup days", [d for d, n in Counter(days).items() if n > 1])

# Check if preserved sections contain nested tab-panel
from src.report.render import extract_monthly_report_day_sections

secs = extract_monthly_report_day_sections(Path(r"E:\Git\Money\output\report_2026.07.html"))
print("extracted sections", len(secs), sorted(secs))
nested = 0
for d, frag in secs.items():
    if "tab-panel" in frag or "week-tabs-wrap" in frag or "tab-btn" in frag:
        nested += 1
        print("NESTED in", d, "tab-panel", "tab-panel" in frag, "week-tabs", "week-tabs-wrap" in frag)
print("nested bad sections", nested)

# Simulate JS: direct children tab-panels under week-tabs-wrap
# Find week-tabs-wrap section and count direct-ish panels
m = re.search(r'<section class="tabs-wrap week-tabs-wrap">(.*?)</section>\s*<script>', html, re.S)
if not m:
    m = re.search(r'<section class="tabs-wrap week-tabs-wrap">(.*)</section>', html, re.S)
if m:
    body = m.group(1)
    # panels at depth: count opening tab-panel
    print("inside wrap tab-panel opens", len(re.findall(r'<div class="tab-panel', body)))
    print("inside wrap day-stack", len(re.findall(r'id="day-', body)))
else:
    print("could not isolate week-tabs-wrap body")
