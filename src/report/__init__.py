"""
HTML 리포트 생성(Jinja2).

하위: ``types`` · ``templates`` · ``render``
기존 ``from src import report`` 는 ``src.report`` 패키지를 로드합니다.
"""
from .render import (
    backfill_preserved_day_sections_market_theme,
    collect_monthly_report_index_links,
    extract_monthly_report_day_sections,
    merge_dated_n_rollup,
    parse_monthly_report_trading_days,
    preserved_day_section_is_stale_forward_observation,
    preserved_day_section_needs_market_theme_backfill,
    render_compact_tabbed_report,
    render_dated_n_report,
    render_movers_index,
    render_report,
)
from .render import DayReport, format_prediction_signal_cell, format_stock_ret_tooltip_lines

__all__ = [
    "DayReport",
    "format_prediction_signal_cell",
    "format_stock_ret_tooltip_lines",
    "render_report",
    "render_compact_tabbed_report",
    "render_movers_index",
    "render_dated_n_report",
    "merge_dated_n_rollup",
    "extract_monthly_report_day_sections",
    "parse_monthly_report_trading_days",
    "collect_monthly_report_index_links",
    "backfill_preserved_day_sections_market_theme",
    "preserved_day_section_is_stale_forward_observation",
    "preserved_day_section_needs_market_theme_backfill",
]
