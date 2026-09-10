"""월간 리포트 탭: 기본은 가장 최근 날짜."""
from __future__ import annotations

import re

from src.report.render import _ensure_report_interaction_script
from src.report.templates import (
    LATEST_DAY_FOCUS_SNIPPET,
    REPORT_TABLE_INTERACTION_SNIPPET,
    _COMPACT_TEMPLATE,
)


def test_compact_week_tabs_mark_last_active() -> None:
    assert "{% if loop.last %} active{% endif %}" in _COMPACT_TEMPLATE
    assert "{% if loop.first %} active{% endif %}" not in _COMPACT_TEMPLATE
    assert "show(panels.length - 1)" in _COMPACT_TEMPLATE


def test_latest_day_focus_script_scrolls_to_day_section() -> None:
    js = LATEST_DAY_FOCUS_SNIPPET
    assert "__moneyFocusLatestDay" in js
    assert "dayIdFromHash" in js
    assert 'section[id^="day-"]' in js
    assert "scrollIntoView" in js
    assert "scrollRestoration" in js
    assert "latestDaySection" in js
    assert "<!-- /money-latest-day-focus -->" in js


def test_interaction_defers_to_latest_day_focus() -> None:
    js = REPORT_TABLE_INTERACTION_SNIPPET
    assert "__moneyFocusLatestDay" in js
    assert "_safeBind" in js


def test_ensure_patches_stale_monthly_html_to_last_tab_and_focus() -> None:
    html = """<!DOCTYPE html>
<html><head><title>t</title></head>
<body>
<section class="tabs-wrap week-tabs-wrap">
<div class="tab-bar" role="tablist">
  <button type="button" class="tab-btn active" role="tab"
          aria-selected="true" data-tab-idx="0">w0</button>
  <button type="button" class="tab-btn" role="tab"
          aria-selected="false" data-tab-idx="1">w1</button>
</div>
<div class="tab-panel active" role="tabpanel" data-tab-panel="0">
  <section class="day-stack" id="day-2026-09-01"><h2>2026-09-01</h2></section>
</div>
<div class="tab-panel" role="tabpanel" data-tab-panel="1">
  <section class="day-stack" id="day-2026-09-10"><h2>2026-09-10</h2></section>
  <section class="day-stack" id="day-2026-09-11"><h2>2026-09-11</h2></section>
</div>
<div class="tab-bar tab-bar-bottom" role="tablist">
  <button type="button" class="tab-btn active" role="tab"
          aria-selected="true" data-tab-idx="0">w0</button>
  <button type="button" class="tab-btn" role="tab"
          aria-selected="false" data-tab-idx="1">w1</button>
</div>
</section>
<script>
(function () {
  var wrap = document.querySelector(".week-tabs-wrap");
  var bars = wrap.querySelectorAll(":scope > .tab-bar");
  var panels = wrap.querySelectorAll(":scope > .tab-panel");
  function show(i) {
    bars.forEach(function (bar) {
      bar.querySelectorAll(":scope > .tab-btn").forEach(function (b, j) {
        b.classList.toggle("active", j === i);
      });
    });
    panels.forEach(function (p, j) { p.classList.toggle("active", j === i); });
  }
  bars.forEach(function (bar) {
    bar.querySelectorAll(":scope > .tab-btn").forEach(function (b, i) {
      b.addEventListener("click", function () { show(i); });
    });
  });
})();
</script>
<!-- money-report-table-interaction -->
<script>window.__old = 1;</script>
</body></html>
"""
    out = _ensure_report_interaction_script(html)
    assert "money-latest-day-focus" in out
    assert "__moneyFocusLatestDay" in out
    assert "show(panels.length - 1)" in out
    assert 'history.scrollRestoration = "manual"' in out.split("</head>", 1)[0]
    assert out.count('class="tab-btn active"') == 2
    assert re.search(
        r'class="tab-btn" role="tab"[\s\S]*?data-tab-idx="0"',
        out,
    )
    assert re.search(
        r'class="tab-btn active" role="tab"[\s\S]*?data-tab-idx="1"',
        out,
    )
    # last panel is active, first is not
    first_panel = out.find('<div class="tab-panel')
    second_panel = out.find('<div class="tab-panel', first_panel + 1)
    assert 'class="tab-panel"' in out[first_panel : first_panel + 40]
    assert 'class="tab-panel active"' in out[second_panel : second_panel + 40]
