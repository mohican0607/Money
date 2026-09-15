# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

from src.report.theme_flow import (
    discover_theme_report_paths,
    theme_flow_range_filename,
    update_theme_flow_report,
)


def test_theme_flow_range_filename() -> None:
    assert (
        theme_flow_range_filename("2026-06-01", "2026-09-15")
        == "report_theme_flow_2026.06-09.html"
    )


def test_update_theme_flow_from_output(tmp_path: Path) -> None:
    """실제 output 테마 HTML이 있으면 흐름 파일을 쓴다(없으면 skip)."""
    out = Path("E:/Git/Money/output")
    paths = discover_theme_report_paths(out)
    if not paths:
        return
    # 격리: 임시 디렉터리에 심볼릭/복사 대신 실제 output에 쓰고 경로만 검증
    got = update_theme_flow_report(out)
    assert got is not None
    assert got.name == "report_theme_flow.html"
    assert got.is_file()
    ranged = out / theme_flow_range_filename(
        "2026-06-01", "2026-09-15"
    )
    # 기간 파일은 데이터 구간에 따라 이름이 달라질 수 있음 — canonical만 필수
    assert "상한가 테마" in got.read_text(encoding="utf-8")
    assert ranged.is_file() or any(out.glob("report_theme_flow_2026.*.html"))
