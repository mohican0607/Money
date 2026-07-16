"""Unit test: nested div theme inject must not truncate market-theme-ref."""
from __future__ import annotations

from src.report.render import (
    _match_balanced_div,
    build_market_theme_ref_block,
    inject_market_theme_into_day_section,
)


def test_inject_replaces_full_theme_with_nested_heading_div() -> None:
    section = (
        '<section class="day-stack day-market-block" id="day-2026-07-01">'
        '<div class="day-heading-row">'
        "<h2>2026-07-01</h2>"
        '<div class="market-filter-radios">filters</div>'
        '<div class="rise-filter-radios">rise</div>'
        "</div>"
        '<div class="market-theme-ref" style="x">'
        '<div class="market-theme-heading-row"><h3>당일 테마 요약</h3></div>'
        '<p class="sub">OLD THEME</p>'
        "</div>"
        '<div class="hit-at-k-panel">hits</div>'
        "<table class=\"rows-compare\"><tr><td>1</td></tr></table>"
        "</section>"
    )
    out = inject_market_theme_into_day_section(section, '<p class="sub">NEW THEME</p>')
    assert out.count("market-theme-ref") == 1
    assert "NEW THEME" in out
    assert "OLD THEME" not in out
    assert out.count("hit-at-k-panel") == 1
    assert "rows-compare" in out
    # div balance inside section
    opens = out.lower().count("<div")
    closes = out.count("</div>")
    assert opens == closes, (opens, closes, out)


def test_match_balanced_div_skips_nested_close() -> None:
    html = (
        '<div class="market-theme-ref">'
        '<div class="inner">x</div>'
        "<p>body</p>"
        "</div>"
        "<p>after</p>"
    )
    m = _match_balanced_div(html, "market-theme-ref")
    assert m is not None
    assert m.group(0).endswith("</div>")
    assert "after" not in m.group(0)
    assert "body" in m.group(0)


def test_build_theme_block_roundtrip() -> None:
    block = build_market_theme_ref_block("<p>hi</p>")
    assert 'class="market-theme-ref"' in block
    assert block.count("<div") == block.count("</div>")
