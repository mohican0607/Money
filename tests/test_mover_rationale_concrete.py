"""「왜 올랐나」가 테마명만 남기지 않고 구체 이슈를 쓰는지."""
from __future__ import annotations

from datetime import date

from src.report import content as rc


def test_sector_summary_catalyst_kept_without_token_overlap() -> None:
    """섹터 요약 촉매(HBM 등)가 별칭과 안 맞아도 버리지 않음."""
    flow = {
        "theme_sector_summaries": [
            {
                "sector": "반도체 관련주",
                "catalyst": "HBM4 양산 기대",
            }
        ]
    }
    theme, catalyst, _ = rc._sector_context(
        flow, code="000660", theme_label="반도체"
    )
    assert theme
    assert "HBM" in catalyst or "양산" in catalyst


def test_theme_news_preferred_over_bare_theme_label() -> None:
    """테마 뉴스 제목이 있으면 「○○ 테마 급등」 대신 제목을 씀."""
    early = [
        (
            date(2026, 8, 4),
            {
                "title": "반도체 HBM 수요 폭증에 장비주 강세",
                "description": "원익 등 반도체 장비",
            },
        )
    ]
    flow = {
        "theme_stock_sectors": {"074600": "반도체"},
        "theme_sector_summaries": [{"sector": "반도체와반도체장비", "catalyst": ""}],
        "theme_limit_up": [
            {"code": "074600", "name": "원익홀딩스", "return_pct": 29.8, "theme_sector": "반도체"}
        ],
        "theme_leaders_ge_25pct": [],
    }
    pick = rc._reason_for_stock(
        None,
        code="074600",
        name="원익홀딩스",
        early_rows=early,
        actual_ctx_rows=[],
        flow_row=flow,
        theme_label="반도체",
        return_pct=29.8,
        is_limit_up=True,
    )
    assert "테마 급등" not in pick.text
    assert "HBM" in pick.text or "수요" in pick.text or "양산" in pick.text or "—" in pick.text


def test_fallback_not_bare_theme_surge() -> None:
    pick = rc._fallback_reason(
        name="비비안",
        theme="섬유",
        catalyst="",
        keywords=[],
        peers=[],
        return_pct=29.5,
        is_limit_up=True,
    )
    assert "테마 급등" not in pick.text
    assert "섬유" in pick.text
    assert "미확인" not in pick.text
    assert "원인 미확인" not in pick.text


def test_fallback_never_unknown_without_theme() -> None:
    pick = rc._fallback_reason(
        name="동화기업",
        theme="",
        catalyst="",
        keywords=[],
        peers=[],
        return_pct=30.0,
        is_limit_up=True,
    )
    assert pick.text.strip()
    assert "미확인" not in pick.text
    assert "원인 미확인" not in pick.text


def test_format_bullet_never_unknown() -> None:
    html = rc._format_bullet_line("혜인", "원인 미확인", is_limit_up=True)
    assert "미확인" not in html
    assert "혜인" in html
    html2 = rc._format_bullet_line(
        "펩트론",
        "생물공학 관련주 동반 강세(종목 직접 뉴스·공시 미확인)",
        is_limit_up=True,
    )
    assert "미확인" not in html2
    assert "생물공학" in html2


def test_useless_date_fragment_rejected() -> None:
    assert rc._is_useless_reason_phrase("일부터")
    assert rc._is_useless_reason_phrase("4일부터")


def test_resolve_sector_aliases_from_token() -> None:
    full, aliases = rc._resolve_sector_aliases("섬유")
    assert aliases
    assert any("섬유" in a or a == "섬유" for a in aliases)


def test_format_bullet_uses_dot_limit_up() -> None:
    html = rc._format_bullet_line("펩트론", "생물공학 — 비만치료제 임상 기대", is_limit_up=True)
    assert "· 상한가" in html
    assert "에 상한가" not in html


def test_synthesize_combines_stock_news_and_theme() -> None:
    """종목 뉴스와 테마 맥락을 한 문장으로 종합."""
    early = [
        (
            date(2026, 8, 4),
            {
                "title": "반도체 HBM 수요 폭증…장비주 동반 강세",
                "description": "원익홀딩스 등",
            },
        )
    ]
    compare = {
        "actual_news_hits": [
            {
                "title": "원익홀딩스, 반도체 장비 수주 증가 소식에 급등",
                "description": "HBM 관련 장비",
                "matched": "원익홀딩스",
            }
        ]
    }
    flow = {
        "theme_stock_sectors": {"074600": "반도체"},
        "theme_sector_summaries": [
            {"sector": "반도체 관련주", "catalyst": "HBM4 양산 기대"}
        ],
        "theme_limit_up": [
            {
                "code": "074600",
                "name": "원익홀딩스",
                "return_pct": 29.8,
                "theme_sector": "반도체",
            }
        ],
        "theme_leaders_ge_25pct": [
            {
                "code": "000660",
                "name": "SK하이닉스",
                "return_pct": 28.0,
                "theme_sector": "반도체",
            }
        ],
    }
    pick = rc._reason_for_stock(
        compare,
        code="074600",
        name="원익홀딩스",
        early_rows=early,
        actual_ctx_rows=[],
        flow_row=flow,
        theme_label="반도체",
        return_pct=29.8,
        is_limit_up=True,
    )
    assert "테마 급등" not in pick.text
    assert len(pick.text) >= 20
    # 종목 이슈 또는 테마 이슈가 드러나야 함
    assert any(
        bit in pick.text for bit in ("수주", "HBM", "반도체", "양산", "장비", "동반")
    )


def test_thin_catalyst_rejected() -> None:
    assert rc._is_thin_reason_text("반도체 — 실적")
    assert rc._is_thin_reason_text("실적")
    assert not rc._is_thin_reason_text("반도체 — HBM4 양산 기대에 장비주 강세")
