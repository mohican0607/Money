"""메일 첨부 HTML이 휴대폰에서 깨지지 않도록 모바일 CSS·테이블 래퍼가 있는지."""

from src.report.templates import (
    REPORT_MOBILE_CSS,
    REPORT_TABLE_INTERACTION_SNIPPET,
    _COMPACT_TEMPLATE,
    _DATED_N_TEMPLATE,
    _TEMPLATE,
    _inject_report_mobile_css,
)


def test_report_mobile_css_injected_into_main_templates() -> None:
    assert "money-report-mobile-css" in _COMPACT_TEMPLATE
    assert "money-report-mobile-css" in _DATED_N_TEMPLATE
    assert "money-report-mobile-css" in _TEMPLATE
    assert "min-width: 920px" in REPORT_MOBILE_CSS
    assert "@media (max-width: 760px)" in REPORT_MOBILE_CSS
    assert ".live-intraday-toggle" in REPORT_MOBILE_CSS
    assert "white-space: nowrap" in REPORT_MOBILE_CSS
    assert "pred-reason-tip-compact" in REPORT_MOBILE_CSS
    assert "td.pred-reason-forward:has(.pred-reason-tip-compact) .pred-reason-inline" in REPORT_MOBILE_CSS


def test_forward_pred_reason_has_compact_mobile_trigger() -> None:
    assert "pred-reason-tip-compact" in _COMPACT_TEMPLATE
    assert "pred-reason-tip-compact" in _DATED_N_TEMPLATE
    assert "pred-reason-tip-compact" in _TEMPLATE
    assert "max-width:28rem" not in _COMPACT_TEMPLATE
    assert "max-width:28rem" not in _DATED_N_TEMPLATE
    assert "max-width:28rem" not in _TEMPLATE


def test_compact_day_table_has_horizontal_scroll_wrap() -> None:
    assert '<div class="table-wrap">' in _COMPACT_TEMPLATE
    assert "table-wrap" in _DATED_N_TEMPLATE


def test_touch_tip_toggle_bound_in_interaction_snippet() -> None:
    assert "bindTouchTipToggles" in REPORT_TABLE_INTERACTION_SNIPPET
    assert "(hover: none), (pointer: coarse)" in REPORT_TABLE_INTERACTION_SNIPPET


def test_inject_report_mobile_css_idempotent() -> None:
    sample = "<html><head><style>\nbody{color:red}\n</style></head></html>"
    once = _inject_report_mobile_css(sample)
    twice = _inject_report_mobile_css(once)
    assert once.count("money-report-mobile-css") == 1
    assert twice == once


def test_inject_report_mobile_css_replaces_stale_block() -> None:
    stale = (
        "<html><head><style>\nbody{color:red}\n"
        "    /* money-report-mobile-css */\n    .stale-mobile { max-width: none; }\n"
        "</style></head></html>"
    )
    updated = _inject_report_mobile_css(stale)
    assert updated.count("money-report-mobile-css") == 1
    assert ".stale-mobile" not in updated
    assert ".live-intraday-toggle" in updated
