"""테마 리포트 파일명·병합 경로."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from src.report import collect_monthly_report_index_links
from src.report.content import (
    legacy_theme_report_filename_for_month,
    theme_report_filename_for_month,
    theme_report_filename_for_range,
    theme_report_source_paths_for_month,
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


def test_theme_source_paths_and_index_links_use_essential(tmp_path: Path) -> None:
    essential = tmp_path / "essential"
    essential.mkdir()
    for name in (
        "report_2026.09.html",
        "report_theme_20pct_2026.09.html",
        "report_theme_25pct_2026.09.html",
    ):
        (essential / name).write_text("<html></html>", encoding="utf-8")
    (tmp_path / "report_2026.0930-1430.html").write_text("snap", encoding="utf-8")

    paths = theme_report_source_paths_for_month(2026, 9, output_dir=tmp_path)
    assert [p.name for p in paths] == [
        "report_theme_20pct_2026.09.html",
        "report_theme_25pct_2026.09.html",
    ]
    assert all(p.parent == essential for p in paths)

    hrefs = [href for href, _ in collect_monthly_report_index_links(tmp_path)]
    assert hrefs == [
        "essential/report_2026.09.html",
        "essential/report_theme_20pct_2026.09.html",
        "essential/report_theme_25pct_2026.09.html",
    ]
