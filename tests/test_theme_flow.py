# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path

from src.report.theme_flow import (
    _span_label,
    discover_theme_report_paths,
    theme_flow_range_filename,
    update_theme_flow_report,
)


def test_bucket_for_groups_unlisted_raw_themes() -> None:
    from src.report.theme_flow import bucket_for

    assert bucket_for("섬유·패션") == "섬유·소비재"
    assert bucket_for("우주항공과국방") == "조선·방산"
    assert bucket_for("금융·은행") == "금융"
    assert bucket_for("반도체") == "반도체·전자"
    assert bucket_for("알수없음") == "기타"


def test_stock_issues_uses_disclosures_and_articles() -> None:
    from src.report import theme_issues as ti

    stock = {"name": "테스트전자", "code": "123456", "raw": "반도체", "reason": "관련주 동반 급등"}
    rows = [
        {"title": "테스트전자, 중동 긴장에 방산 수요 부각", "description": "", "stock_code": "123456"},
        {"title": "다른회사 실적 발표", "description": "", "stock_code": "999999"},
    ]
    disc = [{"code": "123456", "title": "단일판매ㆍ공급계약체결"}]
    issues, head = ti.stock_issues(stock, rows, disclosures=disc)
    assert "국제·지정학" in issues and "실적·수주" in issues
    assert head.startswith("[공시]")
    assert ti.stock_issues({"name": "무명", "code": "000001"}, [])[0] == [ti.UNKNOWN]


def test_render_html_has_charts_and_hides_unknown_from_sankey() -> None:
    from src.report import theme_issues as ti
    from src.report.theme_flow import _build_episodes, _build_segments, _render_html

    def stock(name: str, bucket: str, issues: list[str]) -> dict:
        return {"name": name, "code": "", "raw": bucket, "bucket": bucket, "reason": "", "issues": issues}

    day_rows = []
    for i, d in enumerate(["2026-10-07", "2026-10-08", "2026-10-12", "2026-10-13"]):
        stocks = [
            stock(f"A{i}", "반도체·전자", ["실적·수주"]),
            stock(f"B{i}", "반도체·전자", [ti.UNKNOWN]),
            stock(f"C{i}", "바이오·헬스", ["임상·인허가"]),
        ]
        day_rows.append(
            {"d": d, "n": 3, "top": "반도체·전자", "topN": 2, "share": 0.67,
             "by_theme": {"반도체·전자": 2, "바이오·헬스": 1}, "stocks": stocks,
             "news_total": 10, "news_issue": {"실적·수주": 2}, "news_head": {"실적·수주": "h"}}
        )
    eps = _build_episodes(_build_segments(day_rows))
    out = _render_html(eps=eps, day_rows=day_rows, source_names=["x.html"])
    for title in ("테마 흐름", "테마 × 주간 히트맵", "이슈 ↔ 테마 연관", "시장 뉴스 이슈 온도", "테마 전환 행렬"):
        assert title in out
    assert "뉴스 미확인 4종·일 제외" in out
    assert f'data-i="{ti.UNKNOWN}"' not in out
    assert "10-07~10-13" in out


def test_episode_label_shows_end_day_of_ongoing_episode() -> None:
    assert _span_label({"from": "2026-10-07", "to": "2026-10-13"}) == "10-07~10-13"
    assert _span_label({"from": "2026-10-07", "to": "2026-10-07"}) == "10-07"


def test_theme_flow_range_filename() -> None:
    assert (
        theme_flow_range_filename("2026-06-01", "2026-09-15")
        == "report_theme_flow_2026.06-09.html"
    )


def test_discover_theme_report_paths_reads_essential_only(tmp_path: Path) -> None:
    essential = tmp_path / "essential"
    essential.mkdir()
    (essential / "report_theme_20pct_2026.09.html").write_text("x", encoding="utf-8")
    (essential / "report_theme_25pct_2026.09.html").write_text("x", encoding="utf-8")
    (essential / "report_theme_25pct_2026.08.html").write_text("x", encoding="utf-8")
    (tmp_path / "report_theme_20pct_2026.07.html").write_text("x", encoding="utf-8")
    names = [p.name for p in discover_theme_report_paths(tmp_path)]
    assert names == ["report_theme_25pct_2026.08.html", "report_theme_20pct_2026.09.html"]


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
