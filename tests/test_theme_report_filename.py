"""테마 리포트 파일명·병합 경로."""
from __future__ import annotations

from datetime import date

from src.report.content import (
    legacy_theme_report_filename_for_month,
    theme_report_filename_for_month,
    theme_report_filename_for_range,
    theme_report_threshold_pct,
)


def test_theme_report_filename_for_month() -> None:
    assert theme_report_filename_for_month(2026, 9) == "report_theme_20pct_2026.09.html"
    assert legacy_theme_report_filename_for_month(2026, 8) == "report_theme_25pct_2026.08.html"


def test_theme_report_filename_for_range_same_month() -> None:
    d = date(2026, 9, 1)
    assert theme_report_filename_for_range(d, date(2026, 9, 30)) == (
        "report_theme_20pct_2026.09.html"
    )


def test_theme_report_threshold_matches_big_move() -> None:
    assert theme_report_threshold_pct() == 20
