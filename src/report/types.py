"""리포트 데이터 타입·셀 포맷·테마 ref 블록."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .. import config, trading_calendar
from ..features import highlight_terms

_MARKET_THEME_REF_RE = re.compile(
    r'<div class="market-theme-ref"[^>]*>.*?</div>',
    re.DOTALL | re.IGNORECASE,
)
_DAY_HEADING_ROW_RE = re.compile(
    r'(<div class="day-heading-row"[^>]*>.*?</div>)',
    re.DOTALL | re.IGNORECASE,
)


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
    market_theme_html: str = ""
    mover_rationale_html: str = ""
    hit_at_k_metrics: dict | None = None


def format_prediction_signal_cell(row: dict[str, Any]) -> str:
    """리포트 ``예측 신호`` 셀 — 저장 HTML + ``market_theme_sectors`` pill(있을 때)."""
    base = str(row.get("prediction_signal_html") or "—")
    sectors = row.get("market_theme_sectors") or []
    if not sectors:
        return base
    from .. import snapshot_rebuild_learning

    theme_bit = snapshot_rebuild_learning.format_theme_sectors_signal_html(
        [str(s).strip() for s in sectors if str(s).strip()],
        source=str(row.get("market_theme_source") or "theme"),
    )
    if theme_bit and theme_bit not in base:
        return base + theme_bit
    return base


def format_stock_ret_tooltip_lines(row: dict[str, Any] | None) -> str:
    """종목명 호버 popup 상단 — N-2·N-1·N 일봉 등락률(%) HTML."""
    if not row:
        return ""
    items = row.get("stock_ret_tooltip") or []
    if not items:
        return ""
    parts = ['<div class="stock-ret-lines" aria-label="N-2·N-1·N 일봉 등락률">']
    for it in reversed(items):
        label = str(it.get("label") or "").strip()
        dt = str(it.get("date") or "")
        pct = it.get("pct")
        intraday = bool(it.get("intraday"))
        lbl = "N&nbsp;&nbsp;" if label == "N" else label
        mmdd = f"{dt[5:7]}{dt[8:10]}" if len(dt) >= 10 else ""
        cls = ""
        if pct is not None:
            try:
                pv = float(pct)
                cls = " ok" if pv >= 0 else " bad"
                pct_s = f"{pv:.2f}%"
            except (TypeError, ValueError):
                pct_s = "—"
        else:
            pct_s = "—"
        dt_suffix = f"({mmdd}·장중)" if intraday and mmdd else (f"({mmdd})" if mmdd else "")
        parts.append(
            f'<span class="stock-ret-line{cls}">'
            f'<span class="stock-ret-lbl">{lbl}</span>: '
            f'<span class="stock-ret-pct">{pct_s}</span> '
            f'<span class="stock-ret-dt">{dt_suffix}</span>'
            f"</span>"
        )
    parts.append("</div>")
    return "".join(parts)


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
    if _MARKET_THEME_REF_RE.search(section_html):
        return _MARKET_THEME_REF_RE.sub(block, section_html, count=1)
    m = _DAY_HEADING_ROW_RE.search(section_html)
    if m:
        pos = m.end()
        return section_html[:pos] + "\n        " + block + section_html[pos:]
    return block + "\n" + section_html


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
    m = _MARKET_THEME_REF_RE.search(section_html or "")
    if not m:
        return True
    from .. import snapshot_rebuild_learning

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
    from .. import snapshot_rebuild_learning

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
    """네이버 금융 일봉 캔들 차트 정적 PNG(가로축 거래일, 약 700×289px)."""
    c = str(code).zfill(6)
    return f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{c}.png"


# ``table.rows-compare``: 헤더 ``th.sortable-col`` 클릭 정렬 + 누적 정확도 hover 툴팁(gap-tip)
