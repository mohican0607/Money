"""
학습·테마 사후 병합 — ``market_theme_flow`` · ``rebuild_learning``.

기존 ``from src import snapshot_rebuild_learning`` 는 루트 shim 으로 호환됩니다.
"""
from .market_theme import (
    build_market_theme_flow,
    enrich_compare_rows_theme_for_day,
    enrich_day_reports_market_theme,
    format_market_theme_flow_html,
    market_theme_html_for_trading_day,
    market_theme_html_is_incomplete,
    merge_extended_rebuild_into_snapshot,
    merge_rebuild_learning_dict,
    theme_sector_labels_for_flow_row,
)

__all__ = [
    "build_market_theme_flow",
    "enrich_compare_rows_theme_for_day",
    "enrich_day_reports_market_theme",
    "format_market_theme_flow_html",
    "market_theme_html_for_trading_day",
    "market_theme_html_is_incomplete",
    "merge_extended_rebuild_into_snapshot",
    "merge_rebuild_learning_dict",
    "theme_sector_labels_for_flow_row",
]
