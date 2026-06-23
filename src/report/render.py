"""HTML 렌더·월간·dated rollup."""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from jinja2 import Environment, select_autoescape

from .. import config, trading_calendar
from ..features import highlight_terms

from .templates import (
    REPORT_TABLE_INTERACTION_MARKER,
    REPORT_TABLE_INTERACTION_SNIPPET,
    _COMPACT_TEMPLATE,
    _DATED_N_TEMPLATE,
    _INDEX_TEMPLATE,
    _TEMPLATE,
)
from .types import (
    DayReport,
    _DAY_HEADING_ROW_RE,
    _MARKET_THEME_REF_RE,
    backfill_preserved_day_sections_market_theme,
    build_market_theme_ref_block,
    build_mover_rationale_ref_block,
    format_prediction_signal_cell,
    format_stock_ret_tooltip_lines,
    inject_market_theme_into_day_section,
    naver_chart_day_img_url,
    naver_chart_url,
    naver_disclosure_url,
    preserved_day_section_needs_market_theme_backfill,
)

def render_report(
    title: str,
    days: list[DayReport],
    meta: dict,
    out_path: Path,
    *,
    tabbed: bool = False,
    week_note: str | None = None,
) -> None:
    """
    레거시/일반 리포트 HTML 생성. ``_TEMPLATE`` 사용.

    월간 배치 경로에서는 ``render_compact_tabbed_report`` 가 주로 쓰입니다.
    """
    env = Environment(autoescape=select_autoescape(["html", "xml"]))
    tpl = env.from_string(_TEMPLATE)
    html = tpl.render(
        title=title,
        days=days,
        meta=meta,
        naver_chart_url=naver_chart_url,
        naver_chart_day_img_url=naver_chart_day_img_url,
        naver_disclosure_url=naver_disclosure_url,
        highlight_terms=highlight_terms,
        tabbed=tabbed,
        week_note=week_note,
        interaction_snippet=REPORT_TABLE_INTERACTION_SNIPPET,
        format_prediction_signal_cell=format_prediction_signal_cell,
        format_stock_ret_tooltip_lines=format_stock_ret_tooltip_lines,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")


_DAY_SECTION_RE = re.compile(
    r'<section\s[^>]*\bid="day-(\d{4}-\d{2}-\d{2})"[^>]*>.*?</section>',
    re.DOTALL | re.IGNORECASE,
)


def extract_monthly_report_day_sections(source: Path | str) -> dict[date, str]:
    """
    월간 리포트 HTML에서 거래일별 ``<section id="day-…">`` 블록 전체를 추출합니다.
    """
    if isinstance(source, Path):
        try:
            html = source.read_text(encoding="utf-8")
        except OSError:
            return {}
    else:
        html = source
    out: dict[date, str] = {}
    for m in _DAY_SECTION_RE.finditer(html):
        try:
            d = date.fromisoformat(m.group(1))
        except ValueError:
            continue
        out[d] = m.group(0)
    return out


def parse_monthly_report_trading_days(source: Path | str) -> list[date]:
    """
    월간 리포트 HTML에서 ``id="day-YYYY-MM-DD"`` 앵커를 읽어 거래일 목록을 반환합니다.
    """
    if isinstance(source, Path):
        try:
            html = source.read_text(encoding="utf-8")
        except OSError:
            return []
    else:
        html = source
    hits = re.findall(r'id="day-(\d{4}-\d{2}-\d{2})"', html)
    seen: set[date] = set()
    out: list[date] = []
    for s in hits:
        try:
            d = date.fromisoformat(s)
        except ValueError:
            continue
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return sorted(out)


def collect_monthly_report_index_links(output_dir: Path | None = None) -> list[tuple[str, str]]:
    """
    ``output/report_YYYY.MM.html`` 파일을 스캔해 월간 목차용 ``(파일명, 라벨)`` 목록을 만듭니다.
    """
    root = output_dir or config.OUTPUT_DIR
    links: list[tuple[str, str]] = []
    for path in sorted(root.glob("report_*.html")):
        low = path.name.lower()
        if "index" in low or "dated" in low:
            continue
        if not (
            re.fullmatch(r"report_\d{4}\.\d{2}\.html", path.name)
            or re.fullmatch(r"report_theme_25pct_\d{4}\.\d{2}\.html", path.name)
            or re.fullmatch(r"report_theme_25pct_\d{8}_\d{8}\.html", path.name)
        ):
            continue
        days = parse_monthly_report_trading_days(path)
        prefix = "[테마 25%↑] " if "theme_25pct" in path.name else ""
        if days:
            label = (
                f"{prefix}{path.name} · {days[0].isoformat()} ~ "
                f"{days[-1].isoformat()} ({len(days)}일)"
            )
        else:
            label = prefix + path.name
        links.append((path.name, label))
    return links


def render_movers_index(week_links: list[tuple[str, str]], out_path: Path, title: str) -> None:
    """
    여러 월/주 HTML 파일로의 링크 목차 페이지를 생성합니다.

    Args:
        week_links: ``(파일명 또는 상대 경로, 표시 라벨)`` 튜플 리스트.
    """
    env = Environment(autoescape=select_autoescape(["html", "xml"]))
    tpl = env.from_string(_INDEX_TEMPLATE)
    html = tpl.render(title=title, week_links=week_links)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")


def _monday_of_iso_week(d: date) -> date:
    """날짜 ``d`` 가 속한 ISO 주의 월요일 캘린더일."""
    return d - timedelta(days=d.weekday())


def _panel_days_from_reports_and_preserved(
    reports: list[DayReport],
    preserved_day_html: dict[date, str] | None,
) -> list[dict]:
    """렌더용 일자 목록: 이번 실행 ``DayReport`` + 기존 HTML 조각(유지 일자)."""
    preserved = preserved_day_html or {}
    by_day = {dr.trading_day: dr for dr in reports}
    out: list[dict] = []
    for d in sorted(set(by_day.keys()) | set(preserved.keys())):
        if d in by_day:
            out.append(
                {"trading_day": d, "report": by_day[d], "preserved_html": None}
            )
        else:
            out.append(
                {"trading_day": d, "report": None, "preserved_html": preserved[d]}
            )
    return out


def _week_panels_from_panel_days(panel_days: list[dict]) -> list[dict]:
    """일자 패널 목록 → ISO 주(월요일) 단위 그룹 패널."""
    by_week: dict[date, list[dict]] = {}
    for pd in panel_days:
        d = pd["trading_day"]
        by_week.setdefault(_monday_of_iso_week(d), []).append(pd)
    week_panels: list[dict] = []
    for mon in sorted(by_week.keys()):
        wdays = sorted(by_week[mon], key=lambda x: x["trading_day"])
        fd, ld = wdays[0]["trading_day"], wdays[-1]["trading_day"]
        week_panels.append(
            {
                "monday": mon,
                "label": f"{mon.isoformat()} 주 · {fd.isoformat()} ~ {ld.isoformat()}",
                "days": wdays,
            }
        )
    return week_panels


def render_compact_tabbed_report(
    title: str,
    days: list[DayReport],
    meta: dict,
    out_path: Path,
    *,
    week_note: str | None = None,
    stack_days: bool = False,
    week_tabs_stack_days: bool = False,
    preserved_day_html: dict[date, str] | None = None,
) -> None:
    """
    실제 20%↑·예측 후보 비교 표 중심의 컴팩트 리포트를 ``out_path`` 에 씁니다.

    Args:
        week_note: 상단에 표시할 설명(예: 월 범위 안내).
        stack_days: ISO 주 탭 없이 일자별 섹션만 세로 스택.
        week_tabs_stack_days: True이면 월요일 기준 주별 탭 → 탭 내부에서 거래일 오름차순 스택
            (``main`` 월간/구간 배치에서 사용).
        preserved_day_html: 기존 월간 HTML에서 가져온 일자 블록(이번 실행에 없는 날).
    """
    week_panels: list[dict] | None = None
    if week_tabs_stack_days:
        panel_days = _panel_days_from_reports_and_preserved(days, preserved_day_html)
        week_panels = _week_panels_from_panel_days(panel_days)
    env = Environment(autoescape=select_autoescape(["html", "xml"]))
    tpl = env.from_string(_COMPACT_TEMPLATE)
    html = tpl.render(
        title=title,
        days=days,
        meta=meta,
        naver_chart_url=naver_chart_url,
        naver_chart_day_img_url=naver_chart_day_img_url,
        naver_disclosure_url=naver_disclosure_url,
        week_note=week_note,
        stack_days=stack_days and not week_tabs_stack_days,
        week_tabs_stack_days=week_tabs_stack_days,
        week_panels=week_panels or [],
        interaction_snippet=REPORT_TABLE_INTERACTION_SNIPPET,
        format_prediction_signal_cell=format_prediction_signal_cell,
        format_stock_ret_tooltip_lines=format_stock_ret_tooltip_lines,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")

_m_dated_style = re.search(r"<style>\s*(.*?)\s*</style>", _DATED_N_TEMPLATE, re.DOTALL)
_DATED_N_CSS_INNER = _m_dated_style.group(1) if _m_dated_style else ""


def _dated_n_block_markers(n_compact: str) -> tuple[str, str]:
    """누적 HTML에서 기준일 N 블록 경계 HTML 주석 ``(begin, end)``."""
    return (
        f"<!-- MONEY_DATED_N_BEGIN:{n_compact} -->",
        f"<!-- MONEY_DATED_N_END:{n_compact} -->",
    )


def _dated_n_block_pattern(n_compact: str) -> re.Pattern[str]:
    """기준일 ``n_compact`` 블록 전체를 찾는 정규식(교체·삭제용)."""
    beg, end = _dated_n_block_markers(n_compact)
    return re.compile(
        re.escape(beg) + r".*?" + re.escape(end) + r"\s*",
        re.DOTALL,
    )


def _strip_html_body_inner(full_html: str) -> str:
    """전체 HTML에서 ``<body>`` 내부 문자열만 추출."""
    m = re.search(r"<body[^>]*>(.*)</body>", full_html, re.DOTALL | re.IGNORECASE)
    if not m:
        raise ValueError("dated N HTML에 <body>…</body>가 없습니다.")
    return m.group(1).strip()


def _rollup_style_block_content() -> str:
    """누적 리포트 ``<style>`` 안에 넣는 CSS(단일일 템플릿과 동일 + 롤업 전용 보조 규칙)."""
    extra = """
    header.rollup-page-header { margin-bottom: 20px; }
    main#money-dated-rollup article.dated-n-block {
      margin-bottom: 36px; padding-bottom: 28px; border-bottom: 1px solid #243044;
    }
    main#money-dated-rollup article.dated-n-block:last-of-type {
      margin-bottom: 0; padding-bottom: 0; border-bottom: none;
    }
"""
    return f"{_DATED_N_CSS_INNER}\n{extra}"


def _refresh_dated_rollup_style_block(html: str) -> str:
    """
    기존 누적 HTML은 본문 ``<article>`` 만 갱신하고 ``<style>`` 은 옛날에 고정되는 경우가 있어,
    여기서 첫 ``<style>…</style>`` 내용을 현재 템플릿과 맞춥니다(종목 차트 툴팁 등).
    """
    if not _has_money_dated_main(html):
        return html
    m = re.search(r"(<style>\s*)(.*?)(\s*</style>)", html, flags=re.DOTALL | re.IGNORECASE)
    if not m:
        return html
    new_inner = _rollup_style_block_content()
    return html[: m.start(2)] + new_inner + html[m.end(2) :]


def _rollup_html_shell() -> str:
    """첫 누적 파일 생성 시 head·스타일·빈 main."""
    inner = _rollup_style_block_content()
    head = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>기준일 N별 리포트 (누적)</title>
  <style>
{inner}
  </style>
</head>
<body>
  <header class="rollup-page-header">
    <h1 style="margin:0 0 6px 0;font-size:1.45rem">기준일 N별 리포트 (누적)</h1>
    <p class="sub" style="margin:0;line-height:1.5">
      같은 기준일 N(YYYYMMDD)로 다시 실행하면 아래에서 <strong>해당 N 블록만</strong> 갱신됩니다.
      위쪽이 최근에 추가되거나 갱신된 기준일입니다.
    </p>
  </header>
  <main id="money-dated-rollup">
  </main>
"""
    return head + REPORT_TABLE_INTERACTION_SNIPPET + "\n</body>\n</html>\n"


def _has_money_dated_main(html: str) -> bool:
    """누적 리포트용 ``money-dated-rollup`` main 요소가 있는지 여부."""
    return (
        re.search(r'<main\s+id="money-dated-rollup"\s*>', html, re.IGNORECASE)
        is not None
    )


def _insert_into_money_dated_main(html: str, wrapped_block: str) -> str:
    """``money-dated-rollup`` main 시작 직후에 블록 HTML을 삽입."""
    main_open = re.search(
        r'(<main\s+id="money-dated-rollup"\s*>\s*)',
        html,
        re.IGNORECASE,
    )
    if not main_open:
        raise ValueError(
            '<main id="money-dated-rollup"> 를 찾지 못했습니다. '
            "누적 리포트가 손상된 경우 output 의 해당 HTML 을 삭제한 뒤 다시 실행하세요."
        )
    insert_at = main_open.end()
    return html[:insert_at] + wrapped_block + html[insert_at:]


def _ensure_report_interaction_script(html: str) -> str:
    """정렬·시장 필터 등 인터랙션 스크립트가 없으면 마지막 ``</body>`` 앞에 삽입(구버전 누적 HTML 보강)."""
    if REPORT_TABLE_INTERACTION_MARKER in html:
        return html
    lower = html.lower()
    idx = lower.rfind("</body>")
    if idx == -1:
        return html
    return html[:idx] + REPORT_TABLE_INTERACTION_SNIPPET + "\n" + html[idx:]


def merge_dated_n_rollup(*, rollup_path: Path, n_compact: str, body_inner: str) -> None:
    """누적 HTML에 기준일 N 블록을 넣거나, 같은 ``n_compact`` 블록만 교체한다."""
    beg, end = _dated_n_block_markers(n_compact)
    inner = body_inner.strip()
    wrapped = (
        f"{beg}\n"
        f'<article class="dated-n-block" data-n="{n_compact}">\n'
        f"{inner}\n"
        f"</article>\n"
        f"{end}\n"
    )
    rollup_path.parent.mkdir(parents=True, exist_ok=True)

    def _write_out(s: str) -> None:
        """누적 HTML을 디스크에 쓰고 인터랙션 스크립트를 보강."""
        rollup_path.write_text(_ensure_report_interaction_script(s), encoding="utf-8")

    if not rollup_path.exists():
        _write_out(_insert_into_money_dated_main(_rollup_html_shell(), wrapped))
        return

    text = rollup_path.read_text(encoding="utf-8")
    text = _refresh_dated_rollup_style_block(text)
    if not _has_money_dated_main(text):
        # 예전 단일 페이지 HTML(일자별 파일 등)은 마커가 없으므로 통째로 누적 포맷으로 바꿉니다.
        _write_out(_insert_into_money_dated_main(_rollup_html_shell(), wrapped))
        return

    pat = _dated_n_block_pattern(n_compact)
    if pat.search(text):
        text = pat.sub("", text)
        text = _insert_into_money_dated_main(text, wrapped)
        _write_out(text)
        return

    text = _insert_into_money_dated_main(text, wrapped)
    _write_out(text)


def render_dated_n_report(
    *,
    n_day: date,
    t_day: date,
    day: DayReport,
    meta: dict,
    is_live_n: bool,
    before_open_n: bool = False,
    rollup_path: Path | None = None,
    row_id_prefix: str = "",
) -> None:
    """
    ``python main.py YYYYMMDD`` 단일 실행 전용 레이아웃(``_DATED_N_TEMPLATE``).

    기준일 N → 관측일 T 안내, 라이브 여부에 따라 표에 예측 후보만 강조할 수 있습니다.
    내용은 ``rollup_path``(기본 ``config.REPORT_DATED_ROLLUP_HTML``)에 기준일 블록으로 누적·갱신합니다.
    """
    out_rollup = rollup_path if rollup_path is not None else config.REPORT_DATED_ROLLUP_HTML
    n_compact = n_day.strftime("%Y%m%d")
    env = Environment(autoescape=select_autoescape(["html", "xml"]))
    tpl = env.from_string(_DATED_N_TEMPLATE)
    html = tpl.render(
        n_day=n_day,
        t_day=t_day,
        day=day,
        meta=meta,
        is_live_n=is_live_n,
        before_open_n=before_open_n,
        row_id_prefix=row_id_prefix,
        naver_chart_url=naver_chart_url,
        naver_chart_day_img_url=naver_chart_day_img_url,
        naver_disclosure_url=naver_disclosure_url,
        format_prediction_signal_cell=format_prediction_signal_cell,
        format_stock_ret_tooltip_lines=format_stock_ret_tooltip_lines,
    )
    body_inner = _strip_html_body_inner(html)
    merge_dated_n_rollup(rollup_path=out_rollup, n_compact=n_compact, body_inner=body_inner)
