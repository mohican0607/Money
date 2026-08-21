"""HTML 렌더·월간·dated rollup."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from jinja2 import Environment, select_autoescape

from .. import config, trading_calendar
from ..features import highlight_terms


class _SpanMatch:
    """``re.Match`` 대용 — ``group(0)`` / ``start`` / ``end`` 만 제공."""

    __slots__ = ("_s", "_a", "_b")

    def __init__(self, s: str, a: int, b: int) -> None:
        self._s = s
        self._a = a
        self._b = b

    def group(self, n: int = 0) -> str:
        if n != 0:
            raise IndexError(n)
        return self._s[self._a : self._b]

    def start(self, n: int = 0) -> int:
        if n != 0:
            raise IndexError(n)
        return self._a

    def end(self, n: int = 0) -> int:
        if n != 0:
            raise IndexError(n)
        return self._b


def _match_balanced_div(html: str, class_name: str, *, start: int = 0) -> _SpanMatch | None:
    """
    ``class`` 에 ``class_name`` 이 포함된 최상위 ``<div>…</div>`` 한 블록.

    비탐욕 ``.*?`` 는 중첩 ``</div>`` 에서 조기 종료되어 주간 탭을 깨뜨립니다.
    """
    open_re = re.compile(
        rf'<div\b[^>]*\bclass="[^"]*\b{re.escape(class_name)}\b[^"]*"[^>]*>',
        re.IGNORECASE,
    )
    m = open_re.search(html, start)
    if not m:
        return None
    depth = 0
    j = m.start()
    n = len(html)
    lower = None  # lazy lower only for tag sniff if needed
    while j < n:
        if html[j] != "<":
            j += 1
            continue
        # opening <div ...>
        if html.startswith("<div", j) or html.startswith("<DIV", j):
            nxt = html[j + 4] if j + 4 < n else ""
            if nxt not in " \t\r\n>/":
                j += 1
                continue
            # avoid matching </div
            depth += 1
            gt = html.find(">", j)
            if gt < 0:
                return None
            j = gt + 1
            continue
        if html.startswith("</div>", j) or html.startswith("</DIV>", j):
            depth -= 1
            j += 6
            if depth == 0:
                return _SpanMatch(html, m.start(), j)
            continue
        j += 1
    return None


def _replace_balanced_div(html: str, class_name: str, replacement: str) -> str | None:
    """첫 번째 해당 class div 블록을 ``replacement`` 로 교체. 없으면 None."""
    m = _match_balanced_div(html, class_name)
    if m is None:
        return None
    return html[: m.start()] + replacement + html[m.end() :]


@dataclass
class DayReport:
    """거래일 하루치 파이프라인 결과(예측·비교 표·뉴스 샘플·실제 급등 목록)."""

    trading_day: date
    predictions: list  # PredictionRow
    rows_compare: list[dict]
    false_negatives: list[dict]
    news_titles_sample: list[str]
    news_highlight_terms: list[str]
    actual_big_movers: list[dict]
    forward_observation: bool = False
    show_stock_ret_column: bool = False
    market_theme_html: str = ""
    mover_rationale_html: str = ""
    forward_pred_rationale_html: str = ""
    hit_at_k_metrics: dict | None = None
    pred_accuracy_summary: dict | None = None


def format_prediction_signal_cell(row: dict[str, Any]) -> str:
    """리포트 ``예측 신호`` 셀 — 저장 HTML + ``market_theme_sectors`` pill(있을 때)."""
    base = str(row.get("prediction_signal_html") or "—")
    sectors = row.get("market_theme_sectors") or []
    if not sectors:
        return base
    from ..learning import market_theme as snapshot_rebuild_learning

    theme_bit = snapshot_rebuild_learning.format_theme_sectors_signal_html(
        [str(s).strip() for s in sectors if str(s).strip()],
        source=str(row.get("market_theme_source") or "theme"),
    )
    if theme_bit and theme_bit not in base:
        return base + theme_bit
    return base


def _fmt_pct_span_html(pct: float | None) -> str:
    if pct is None:
        return "—"
    try:
        pv = float(pct)
        if not math.isfinite(pv):
            return "—"
        inner = f"{pv:.2f}"
        if pv < 0:
            return f'<span class="bad">{inner}</span>'
        return inner
    except (TypeError, ValueError):
        return "—"


def format_forward_actual_ret_cell(row: dict[str, Any] | None) -> str:
    """예측 전용(장 마감 전) 관측일: 실제 상승률 칸은 항상 미확정(—)."""
    return "—"


def _format_stock_ret_pct(pv: float, *, spaced: bool = False) -> str:
    """일봉 등락률 표시 문자열(``spaced`` 이면 ``12.34 %``)."""
    s = f"{pv:.2f}"
    return f"{s} %" if spaced else f"{s}%"


def _stock_ret_lines_html(
    row: dict[str, Any] | None,
    *,
    wrapper_class: str,
    empty: str,
    spaced_percent: bool = False,
) -> str:
    """N-3·N-2·N-1·N 등락률 HTML 블록( tooltip·표 열 공통 생성 )."""
    if not row:
        return empty
    items = row.get("stock_ret_tooltip") or []
    if not items:
        return empty
    code = str(row.get("code", "")).zfill(6)
    parts = [f'<div class="{wrapper_class}" aria-label="N-3·N-2·N-1·N 일봉 등락률">']
    for it in reversed(items):
        label = str(it.get("label") or "").strip()
        dt = str(it.get("date") or "")
        pct = it.get("pct")
        intraday = bool(it.get("intraday"))
        lbl_base = "N" if label == "N" else label
        mmdd = f"{dt[5:7]}{dt[8:10]}" if len(dt) >= 10 else ""
        if mmdd:
            lbl = f"{lbl_base}({mmdd})"
        else:
            lbl = lbl_base
        cls = ""
        live_attr = (
            f' data-live-intraday="1" data-stock-code="{code}"' if intraday else ""
        )
        if intraday and pct is None:
            pct_s = "…"
        elif pct is not None:
            try:
                pv = float(pct)
                cls = " ok" if pv >= 0 else " bad"
                pct_s = _format_stock_ret_pct(pv, spaced=spaced_percent)
            except (TypeError, ValueError):
                pct_s = "—"
        else:
            pct_s = "—"
        parts.append(
            f'<span class="stock-ret-line{cls}"{live_attr}>'
            f'<span class="stock-ret-lbl">{lbl}</span>: '
            f'<span class="stock-ret-pct">{pct_s}</span>'
            f"</span>"
        )
    parts.append("</div>")
    return "".join(parts)


def format_stock_ret_tooltip_lines(row: dict[str, Any] | None) -> str:
    """종목명 호버 popup 상단 — N-3·N-2·N-1·N 일봉 등락률(%) HTML."""
    return _stock_ret_lines_html(row, wrapper_class="stock-ret-lines", empty="")


def format_stock_ret_column_lines(row: dict[str, Any] | None) -> str:
    """표 전용 N-3·N-2·N-1·N 일봉 등락률(예측 전용·장마감 후 유지)."""
    return _stock_ret_lines_html(
        row,
        wrapper_class="stock-ret-col-lines",
        empty="—",
        spaced_percent=True,
    )


def stock_chart_n_day_iso(row: dict[str, Any] | None) -> str:
    """compare 행의 기준일 N(ISO). 차트 N 봉 마커·캡션용."""
    if not row:
        return ""
    explicit = str(row.get("report_n_day") or "").strip()
    if len(explicit) >= 10:
        return explicit[:10]
    for it in row.get("stock_ret_tooltip") or []:
        if str(it.get("label") or "").strip() == "N":
            dt = str(it.get("date") or "").strip()
            if len(dt) >= 10:
                return dt[:10]
    return ""


def stock_chart_n_bar_offset(row: dict[str, Any] | None) -> int | None:
    """차트 우측 끝 대비 N 봉 offset(0=최우측). 없거나 범위 밖이면 ``None``."""
    if not row:
        return None
    off = row.get("stock_chart_n_bar_offset")
    if off is None:
        return None
    try:
        return int(off)
    except (TypeError, ValueError):
        return None


def build_mover_rationale_ref_block(rationale_inner_html: str) -> str:
    """「왜 올랐나」 패널 HTML."""
    inner = (rationale_inner_html or "").strip()
    if not inner:
        return ""
    return (
        '<div class="mover-rationale-ref" style="margin:12px 0 16px;padding:12px 14px;'
        'background:#1a2838;border:1px solid #3a5a4a;border-radius:8px">'
        '<h3 style="font-size:0.95rem;color:var(--ok);margin:0 0 8px">'
        "왜 올랐나 (상한가·급등)</h3>"
        f"{inner}</div>"
    )


def build_market_theme_ref_block(theme_inner_html: str) -> str:
    """``당일 테마 요약`` 패널 HTML(``market_theme_panel`` 매크로와 동일 구조)."""
    inner = (theme_inner_html or "").strip()
    if not inner:
        return ""
    return (
        '<div class="market-theme-ref" style="margin:12px 0 16px;padding:12px 14px;'
        'background:#152232;border:1px solid #2a4a6a;border-radius:8px">'
        '<h3 style="font-size:0.95rem;color:var(--ok);margin:0 0 8px">당일 테마 요약</h3>'
        f"{inner}</div>"
    )


def inject_market_theme_into_day_section(section_html: str, theme_inner_html: str) -> str:
    """기존 일자 ``<section>`` HTML에 ``당일 테마 요약`` 블록을 삽입하거나 교체합니다."""
    block = build_market_theme_ref_block(theme_inner_html)
    if not block:
        return section_html
    replaced = _replace_balanced_div(section_html, "market-theme-ref", block)
    if replaced is not None:
        return replaced
    heading = _match_balanced_div(section_html, "day-heading-row")
    if heading is not None:
        pos = heading.end()
        return section_html[:pos] + "\n        " + block + section_html[pos:]
    return block + "\n" + section_html


def preserved_day_section_is_stale_intraday_closed_ui(
    section_html: str,
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> bool:
    """
    월간 병합 유지 블록이 **오늘 장중**인데 마감 확정 UI(실제 상승률 등)로 남아 있으면 True.
    """
    if not trading_calendar.is_trading_day(t_day):
        return False
    now = now_kst or datetime.now(trading_calendar.KST)
    if t_day != now.date():
        return False
    if trading_calendar.is_krx_daily_bar_effective_closed(t_day, now_kst=now):
        return False
    html = section_html or ""
    if "day-forward-obs" in html:
        return False
    if re.search(r"실제\s*상승률", html):
        return True
    if re.search(r"실제≥", html):
        return True
    return False


def preserved_day_section_is_stale_forward_observation(
    section_html: str,
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> bool:
    """
    월간 병합으로 유지된 일자 블록이 장 마감 후에도 예측 전용 UI로 남아 있으면 True.

    (예: 6/30 실행으로 미래일 T=7/1 이 예측 전용으로 저장된 뒤, 7/2 에 7/2 만 재실행)

    「예측 근거」 열은 확정 UI에도 있으므로 stale 판별에 쓰지 않습니다.
    """
    if not trading_calendar.is_trading_day(t_day):
        return False
    now = now_kst or datetime.now(trading_calendar.KST)
    if not trading_calendar.trading_day_actuals_finalized(t_day, now_kst=now):
        return False
    html = section_html or ""
    if "day-forward-obs" in html:
        return True
    if re.search(r"예측\s*전용\s*</span>", html):
        return True
    return False


def preserved_day_section_needs_market_theme_backfill(
    section_html: str,
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> bool:
    """
    월간 병합으로 유지된 일자 블록이 장 마감 후 테마 요약 보강 대상인지 판단합니다.

    장중에 만들어져 테마가 비었거나 placeholder 인 **이미 마감된** 거래일만 True.
    """
    if not trading_calendar.is_trading_day(t_day):
        return False
    now = now_kst or datetime.now(trading_calendar.KST)
    if t_day > now.date():
        return False
    if not trading_calendar.is_krx_daily_bar_effective_closed(t_day, now_kst=now):
        return False
    m = _match_balanced_div(section_html or "", "market-theme-ref")
    if not m:
        return True
    from ..learning import market_theme as snapshot_rebuild_learning

    return snapshot_rebuild_learning.market_theme_html_is_incomplete(m.group(0))


def backfill_preserved_day_sections_market_theme(
    preserved_day_html: dict[date, str],
    *,
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: Any,
    listing_names: dict[str, str],
    news_cutoff_label: str,
    now_kst: datetime | None = None,
) -> dict[date, str]:
    """병합 유지 일자 HTML에 장 마감 후 ``당일 테마 요약`` 을 채웁니다."""
    from ..learning import market_theme as snapshot_rebuild_learning

    if not preserved_day_html or returns_df is None:
        return preserved_day_html
    now = now_kst or datetime.now(trading_calendar.KST)
    out = dict(preserved_day_html)
    patched = 0
    for t_day in sorted(preserved_day_html.keys()):
        html = preserved_day_html[t_day]
        if not preserved_day_section_needs_market_theme_backfill(
            html, t_day, now_kst=now
        ):
            continue
        theme_inner = snapshot_rebuild_learning.market_theme_html_for_trading_day(
            t_day,
            news_by_calendar,
            returns_df,
            listing_names,
            news_cutoff_label=news_cutoff_label,
        )
        if snapshot_rebuild_learning.market_theme_html_is_incomplete(theme_inner):
            continue
        out[t_day] = inject_market_theme_into_day_section(html, theme_inner)
        patched += 1
    if patched:
        print(
            f"월간 리포트 병합: 장 마감 후 당일 테마 요약 보강 {patched}일(기존 HTML 유지 일자)",
            flush=True,
        )
    return out


def naver_chart_url(code: str) -> str:
    """네이버 금융 캔들 차트 링크(6자리 종목코드)."""
    c = str(code).zfill(6)
    return f"https://finance.naver.com/item/fchart.naver?code={c}"


def naver_disclosure_url(code: str) -> str:
    """네이버 금융 종목 공시 페이지 링크(6자리 종목코드)."""
    c = str(code).zfill(6)
    return f"https://finance.naver.com/item/news_notice.naver?code={c}"


def naver_chart_day_img_url(code: str) -> str:
    """네이버 금융 일봉 캔들 PNG 베이스 URL(호버 시 JS로 최신 이미지 로드, 약 700×289px)."""
    c = str(code).zfill(6)
    return f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{c}.png"


# ``table.rows-compare``: 헤더 ``th.sortable-col`` 클릭 정렬 + 누적 정확도 hover 툴팁(gap-tip)

from .templates import (
    REPORT_TABLE_INTERACTION_MARKER,
    REPORT_TABLE_INTERACTION_SNIPPET,
    _COMPACT_TEMPLATE,
    _DATED_N_TEMPLATE,
    _INDEX_TEMPLATE,
    _TEMPLATE,
)

_LIVE_QUOTES_SCRIPT = '<script src="live_quotes.js" id="money-live-quotes-js"></script>'


def _inject_live_quotes_script(html: str) -> str:
    """``<head>`` 에 live_quotes.js 를 넣습니다.

    본문 인터랙션 스크립트가 ``live_quotes.js`` 문자열을 포함하므로, 파일명 부분
    일치가 아니라 script id 로 중복 삽입을 막습니다.
    """
    if 'id="money-live-quotes-js"' in html or "id='money-live-quotes-js'" in html:
        return html
    if "</head>" not in html:
        return html
    return html.replace("</head>", f"  {_LIVE_QUOTES_SCRIPT}\n</head>", 1)


def _after_report_written(out_path: Path, days: list[DayReport]) -> None:
    from .live_quotes import (
        collect_stock_codes_from_day_reports,
        collect_stock_codes_from_html,
        ensure_live_quotes_for_report,
    )

    codes = set(collect_stock_codes_from_day_reports(days))
    try:
        if out_path.is_file():
            codes.update(
                collect_stock_codes_from_html(out_path.read_text(encoding="utf-8"))
            )
    except OSError:
        pass
    ensure_live_quotes_for_report(out_path.parent, sorted(codes))


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
        format_stock_ret_column_lines=format_stock_ret_column_lines,
        format_forward_actual_ret_cell=format_forward_actual_ret_cell,
        stock_chart_n_day_iso=stock_chart_n_day_iso,
        stock_chart_n_bar_offset=stock_chart_n_bar_offset,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_ensure_report_interaction_script(html), encoding="utf-8")
    _after_report_written(out_path, days)


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


def iso_week_chart_view_end(
    monday: date, *, now_kst: datetime | None = None
) -> date:
    """
    주간 탭 차트 캡션용 — ISO 주(월~금) 마지막 거래일, 단 오늘 장마감 상한 이내.

    현재 주(예: 6/22~)이면 오늘(또는 장중이면 전 거래일)까지, 지난 주면 그 주 금요일(또는
    그 주 마지막 거래일)까지를 봅니다.
    """
    friday = monday + timedelta(days=4)
    sessions = trading_calendar.trading_sessions_in_range(monday, friday)
    week_end = sessions[-1] if sessions else monday
    cap = trading_calendar.ohlcv_request_end_cap(now_kst=now_kst)
    return week_end if week_end <= cap else cap


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
                "chart_view_end": iso_week_chart_view_end(mon).isoformat(),
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
        format_stock_ret_column_lines=format_stock_ret_column_lines,
        format_forward_actual_ret_cell=format_forward_actual_ret_cell,
        stock_chart_n_day_iso=stock_chart_n_day_iso,
        stock_chart_n_bar_offset=stock_chart_n_bar_offset,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_ensure_report_interaction_script(html), encoding="utf-8")
    _after_report_written(out_path, days)

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
  {_LIVE_QUOTES_SCRIPT}
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


_INTERACTION_BLOCK_RE = re.compile(
    r"<!-- money-report-table-interaction -->.*?</script>",
    re.DOTALL | re.IGNORECASE,
)


def _ensure_report_interaction_script(html: str) -> str:
    """인터랙션 스크립트를 보강·교체(구버전 누적 HTML의 stale JS 갱신)."""
    html = _inject_live_quotes_script(html)
    if _INTERACTION_BLOCK_RE.search(html):
        snippet = REPORT_TABLE_INTERACTION_SNIPPET.strip()
        return _INTERACTION_BLOCK_RE.sub(lambda _m: snippet, html, count=1)
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
        format_stock_ret_column_lines=format_stock_ret_column_lines,
        format_forward_actual_ret_cell=format_forward_actual_ret_cell,
        stock_chart_n_day_iso=stock_chart_n_day_iso,
        stock_chart_n_bar_offset=stock_chart_n_bar_offset,
    )
    body_inner = _strip_html_body_inner(html)
    merge_dated_n_rollup(rollup_path=out_rollup, n_compact=n_compact, body_inner=body_inner)
    _after_report_written(out_rollup, [day])
