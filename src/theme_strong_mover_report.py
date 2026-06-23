"""
25%↑ 급등 종목 날짜별 테마·상승 배경 전용 리포트(축약형).

``output/report_theme_25pct_YYYY.MM.html`` — 메인 ``report_YYYY.MM.html`` 과 별도 파일.
"""
from __future__ import annotations

import html
import re
from datetime import date
from pathlib import Path
from typing import Any

from . import features, snapshot_rebuild_learning as srl, stocks, trading_calendar

_GENERIC_CATALYSTS = frozenset(
    {"따르면", "테마", "급등", "상한가", "코스피", "코스닥", "증시", "주가", "관련주"}
)
_TITLE_MAX = 72
_REASON_MAX = 200
_KW_MAX = 4
_PEER_MAX = 5


def _esc(s: str) -> str:
    """HTML 이스케이프."""
    return html.escape(str(s or ""), quote=True)


def _news_row_title(row: dict[str, str], *, max_len: int = _TITLE_MAX) -> str:
    """뉴스 row에서 표시용 제목(없으면 description 앞부분)."""
    t = str(row.get("title") or "").strip()
    if not t:
        t = str(row.get("description") or "").strip()[:max_len]
    return t[:max_len]


def _stock_direct_news(
    code: str,
    name: str,
    early_rows: list[tuple[date, dict[str, str]]],
    *,
    limit: int = 1,
) -> list[tuple[date, str]]:
    """종목 직접 매칭 early 뉴스 제목 목록."""
    from .features import _stock_relevant_news_rows

    out: list[tuple[date, str]] = []
    for d, row in _stock_relevant_news_rows(early_rows, code, name):
        title = _news_row_title(row)
        if title:
            out.append((d, title))
        if len(out) >= limit:
            break
    return out


def _news_clip_for_aliases(
    early_rows: list[tuple[date, dict[str, str]]],
    aliases: tuple[str, ...],
) -> tuple[date, str] | None:
    """테마 별칭이 포함된 첫 early 뉴스 (날짜, 제목)."""
    if not aliases:
        return None
    for d, row in early_rows:
        blob = f"{row.get('title', '')} {row.get('description', '')}"
        if not srl._alias_in_blob(blob, aliases):
            continue
        title = _news_row_title(row)
        if title:
            return d, title
    return None


def _sector_summary_for_label(
    flow_row: dict[str, Any], theme_compact: str
) -> dict[str, Any] | None:
    """``theme_sector_summaries`` 에서 축약 라벨과 일치하는 섹터 요약 dict."""
    for summ in flow_row.get("theme_sector_summaries") or []:
        if not isinstance(summ, dict):
            continue
        sec = str(summ.get("sector") or "")
        if srl._compact_sector_label(sec) == theme_compact or sec == theme_compact:
            return summ
    return None


def _usable_catalyst(raw: str) -> str:
    """범용·짧은 촉매 문자열은 빈 문자열로 제거."""
    s = str(raw or "").strip()
    if len(s) < 2 or s.lower() in _GENERIC_CATALYSTS:
        return ""
    return s


def _compact_day_overview(flow_row: dict[str, Any]) -> str:
    """일자 통계 한 줄(전체 market_theme HTML 제외)."""
    n25 = int(flow_row.get("n_movers_ge_25pct") or 0)
    n_lim = int(flow_row.get("n_limit_up") or 0)
    bits = [f"25%↑ {n25}종"]
    if n_lim:
        bits.append(f"상한 {n_lim}종")
    seeds = [str(s) for s in (flow_row.get("theme_seed_hits") or [])[:6] if str(s).strip()]
    line = " · ".join(bits)
    if seeds:
        pills = "".join(f'<span class="pill">{_esc(s)}</span> ' for s in seeds)
        line += f" · 시드 {pills}"
    return f'<p class="day-stats">{line}</p>'


def _sector_rationale_compact(
    flow_row: dict[str, Any],
    sector_label: str,
    early_rows: list[tuple[date, dict[str, str]]],
    stocks: list[dict[str, Any]],
) -> str:
    """테마당 1~2문장."""
    compact = srl._compact_sector_label(sector_label)
    full = srl._full_sector_label(sector_label)
    aliases = srl._sector_aliases_for_label(full)
    summ = _sector_summary_for_label(flow_row, compact)

    parts: list[str] = []
    catalyst = _usable_catalyst(str((summ or {}).get("catalyst") or ""))
    if catalyst:
        parts.append(f"이슈: {_esc(catalyst)}")

    kw = [
        str(a)
        for a in (summ or {}).get("news_aliases") or []
        if str(a).strip() and str(a).lower() not in _GENERIC_CATALYSTS
    ][:3]
    if kw:
        parts.append("키워드: " + ", ".join(_esc(k) for k in kw))

    clip = _news_clip_for_aliases(early_rows, aliases)
    if clip:
        d, title = clip
        parts.append(f'기사({_esc(d.isoformat())}): {_esc(title)}')

    top_names = [
        f"{s.get('name')} {float(s.get('return_pct') or 0):+.0f}%"
        for s in sorted(stocks, key=lambda x: float(x.get("return_pct") or 0), reverse=True)
    ][: _PEER_MAX]
    if len(stocks) > _PEER_MAX:
        top_names.append(f"외 {len(stocks) - _PEER_MAX}종")
    if top_names:
        parts.append("급등: " + _esc(", ".join(top_names)))

    if not parts:
        return '<p class="sector-why muted">당일 early 뉴스·급등 패턴 기반 추정</p>'
    return f'<p class="sector-why">{" · ".join(parts)}</p>'


def _market_label(segment: str) -> str:
    """``market_segment`` 코드 → KOSPI/KOSDAQ 표시."""
    seg = str(segment or "").lower()
    if seg == "kospi":
        return "KOSPI"
    if seg == "kosdaq":
        return "KOSDAQ"
    return "-"


def _plain_from_html(fragment: str, *, max_len: int = _TITLE_MAX) -> str:
    """HTML fragment → 평문(태그 제거·길이 제한)."""
    s = re.sub(r"<[^>]+>", " ", str(fragment or ""))
    s = html.unescape(re.sub(r"\s+", " ", s).strip())
    return s[:max_len]


def _news_hits_from_compare(compare_row: dict[str, Any] | None, *, limit: int = 2) -> list[str]:
    """비교 행의 예측/실제 뉴스 hit·공시에서 표시용 문자열 추출."""
    if not compare_row:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for key in ("actual_news_hits", "pred_news_hits"):
        for h in compare_row.get(key) or []:
            if not isinstance(h, dict):
                continue
            title = _news_row_title(h)
            if not title or title in seen:
                continue
            seen.add(title)
            matched = str(h.get("matched") or "").strip()
            if matched:
                out.append(f"[{matched}] {title}")
            else:
                out.append(title)
            if len(out) >= limit:
                return out
    for h in compare_row.get("disclosure_hits") or []:
        if not isinstance(h, dict):
            continue
        title = str(h.get("title") or h.get("report_nm") or "").strip()
        if title and title not in seen:
            out.append(f"공시: {title[:_TITLE_MAX]}")
            break
    return out


def _rise_reason_bullet(compare_row: dict[str, Any] | None) -> str:
    """메인 리포트 rise_reason_html 에서 뉴스·시세 bullet 1개 추출."""
    if not compare_row:
        return ""
    raw = str(compare_row.get("rise_reason_html") or "")
    if not raw:
        return ""
    skip_bits = (
        "자동 참고",
        "단일 원인",
        "수집을 생략",
        "미확정",
        "인과 단정",
    )
    for m in re.finditer(r"<li[^>]*>(.*?)</li>", raw, flags=re.I | re.S):
        line = _plain_from_html(m.group(1), max_len=_REASON_MAX)
        if len(line) < 12:
            continue
        if any(bit in line for bit in skip_bits):
            continue
        if re.match(r"^\d{4}-\d{2}-\d{2}\s", line):
            continue
        return line
    return ""


def _display_keywords(stock: dict[str, Any]) -> list[str]:
    """표시용 키워드(범용·잡음 제외, 종목명 관련 우선)."""
    raw = list(srl._stock_theme_keywords(stock))
    ov = stock.get("name_keyword_overlap_with_news") or []
    for x in ov:
        t = str(x).strip()
        if t:
            raw.append(t)
    merged = list(features.filter_specific_keywords(raw))
    name = str(stock.get("name") or "").strip().lower()
    if name:
        in_name = [k for k in merged if k.lower() in name or name in k.lower()]
        if in_name:
            return in_name[:_KW_MAX]
    return merged[:_KW_MAX]


def _stock_reason_summary(
    stock: dict[str, Any],
    *,
    theme: str,
    early_rows: list[tuple[date, dict[str, str]]],
    sector_catalyst: str,
    sector_aliases: tuple[str, ...],
    compare_row: dict[str, Any] | None,
    peers: list[dict[str, Any]],
) -> str:
    """종목 상승 배경 1~2문장(뉴스·공시·테마 맥락)."""
    code = str(stock.get("code") or "").zfill(6)
    name = str(stock.get("name") or "").strip()
    parts: list[str] = []

    parts.extend(_news_hits_from_compare(compare_row, limit=2))

    if not parts:
        for d, title in _stock_direct_news(code, name, early_rows, limit=1):
            parts.append(f"early({d.isoformat()}): {title}")

    if not parts:
        bullet = _rise_reason_bullet(compare_row)
        if bullet:
            parts.append(bullet)

    if not parts:
        clip = _news_clip_for_aliases(early_rows, sector_aliases)
        if clip:
            d, title = clip
            parts.append(f"테마 뉴스({d.isoformat()}): {title}")

    kw = _display_keywords(stock)
    if kw and not parts:
        parts.append(f"종목·뉴스 키워드: {', '.join(kw)}")

    if sector_catalyst and not any(sector_catalyst in p for p in parts):
        parts.append(f"{theme}: {sector_catalyst}")

    if not parts:
        peer_names = [
            str(s.get("name") or "")
            for s in peers
            if str(s.get("code") or "").zfill(6) != code and s.get("name")
        ][:3]
        if peer_names:
            parts.append(
                f"{theme} 동반 급등({', '.join(peer_names)} 등), 종목 직접 뉴스 없음"
            )
        else:
            parts.append(f"{theme} 테마 급등, 종목·테마 직접 뉴스 매칭 없음")

    text = " / ".join(parts[:2])
    if len(text) > _REASON_MAX:
        text = text[: _REASON_MAX - 1] + "…"
    return _esc(text)


def build_day_strong_mover_sections(
    dr: Any,
    flow_row: dict[str, Any] | None,
    *,
    news_by_calendar: dict[date, list[dict[str, str]]],
    news_cutoff_label: str,
    market_by_code: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """거래일 하나의 25%↑ 테마 섹션(축약)."""
    if dr.forward_observation or not flow_row:
        return None

    t_day: date = dr.trading_day
    early_rows = srl._early_news_rows_for_trading_day(news_by_calendar, t_day)
    if dr.rows_compare:
        srl._patch_flow_row_strong_movers_from_compare(
            flow_row,
            dr.rows_compare,
            flow_row.get("_listing_names") or {},
            early_rows,
        )

    leaders = list(flow_row.get("theme_leaders_ge_25pct") or [])
    if not leaders:
        return None

    mkt_map = market_by_code or {}

    compare_by_code = {
        str(r.get("code", "")).zfill(6): r
        for r in (dr.rows_compare or [])
        if isinstance(r, dict) and r.get("code")
    }

    for stock in leaders:
        if not isinstance(stock, dict):
            continue
        code = str(stock.get("code") or "").zfill(6)
        if str(stock.get("theme_sector") or "").strip():
            continue
        cr = compare_by_code.get(code)
        labels, _src = srl._theme_labels_for_compare_row(
            flow_row,
            cr or {
                "code": code,
                "name": stock.get("name"),
                "actual_ret": float(stock.get("return_pct") or 0) / 100.0,
                "pred_ret": None,
                "keywords": [],
            },
            listing_names=flow_row.get("_listing_names") or {},
            early_rows=early_rows,
            seeds=list(flow_row.get("theme_seed_hits") or []),
            top_kw=list(flow_row.get("top_keywords_sample") or []),
        )
        if labels:
            stock["theme_sector"] = labels[0]

    seeds = list(flow_row.get("theme_seed_hits") or [])
    top_kw = list(flow_row.get("top_keywords_sample") or [])
    for stock in leaders:
        if not isinstance(stock, dict):
            continue
        if str(stock.get("theme_sector") or "").strip():
            continue
        label = srl._best_effort_theme_label(
            stock, flow_row, seeds=seeds, top_kw=top_kw
        )
        if label:
            stock["theme_sector"] = label

    by_sector: dict[str, list[dict[str, Any]]] = {}
    for stock in leaders:
        if not isinstance(stock, dict):
            continue
        theme = srl._compact_sector_label(str(stock.get("theme_sector") or ""))
        if not theme:
            theme = srl._best_effort_theme_label(
                stock, flow_row, seeds=seeds, top_kw=top_kw
            )
            if theme:
                stock["theme_sector"] = theme
        if not theme:
            continue
        by_sector.setdefault(theme, []).append(stock)

    sector_blocks: list[dict[str, Any]] = []
    for theme, stocks in sorted(
        by_sector.items(),
        key=lambda x: (
            -max(float(s.get("return_pct") or 0) for s in x[1]),
            -len(x[1]),
        ),
    ):
        stocks.sort(key=lambda s: float(s.get("return_pct") or 0), reverse=True)
        full_label = srl._full_sector_label(theme) or theme
        summ = _sector_summary_for_label(flow_row, theme)
        cat = _usable_catalyst(str((summ or {}).get("catalyst") or ""))
        sector_aliases = srl._sector_aliases_for_label(full_label)
        sector_blocks.append(
            {
                "theme": theme,
                "count": len(stocks),
                "rationale_html": _sector_rationale_compact(
                    flow_row, full_label, early_rows, stocks
                ),
                "stocks": [
                    {
                        "code": str(s.get("code") or "").zfill(6),
                        "name": str(s.get("name") or ""),
                        "theme": srl._compact_sector_label(
                            str(s.get("theme_sector") or theme)
                        )
                        or theme,
                        "market": _market_label(
                            str(
                                compare_by_code.get(
                                    str(s.get("code") or "").zfill(6), {}
                                ).get("market_segment")
                                or mkt_map.get(str(s.get("code") or "").zfill(6), "")
                            )
                        ),
                        "return_pct": float(s.get("return_pct") or 0),
                        "is_limit_up": bool(
                            not s.get("is_intraday_limit_down")
                            and (
                                s.get("is_limit_up")
                                or srl._is_near_limit_up_return_pct(
                                    float(s.get("return_pct") or 0)
                                )
                            )
                        ),
                        "reason": _stock_reason_summary(
                            s,
                            theme=theme,
                            early_rows=early_rows,
                            sector_catalyst=cat,
                            sector_aliases=sector_aliases,
                            compare_row=compare_by_code.get(
                                str(s.get("code") or "").zfill(6)
                            ),
                            peers=stocks,
                        ),
                    }
                    for s in stocks
                ],
            }
        )

    return {
        "trading_day": t_day,
        "n_strong": len(leaders),
        "n_limit_up": int(flow_row.get("n_limit_up") or 0),
        "news_cutoff_label": news_cutoff_label,
        "news_prev_day": trading_calendar.last_trading_day_before(t_day).isoformat(),
        "overview_html": _compact_day_overview(flow_row),
        "sectors": sector_blocks,
    }


def build_strong_mover_report_days(
    day_reports: list[Any],
    theme_flow_rows: list[dict[str, Any]],
    *,
    news_by_calendar: dict[date, list[dict[str, str]]],
    listing_names: dict[str, str],
    news_cutoff_label: str,
) -> list[dict[str, Any]]:
    """여러 ``DayReport`` → 25%↑ 테마 리포트용 일자별 섹션 dict 목록."""
    by_day = {str(r.get("trading_day")): dict(r) for r in theme_flow_rows if r.get("trading_day")}
    market_by_code = stocks.market_segment_by_code()
    out: list[dict[str, Any]] = []
    for dr in sorted(day_reports, key=lambda x: x.trading_day):
        if getattr(dr, "forward_observation", False):
            continue
        row = by_day.get(dr.trading_day.isoformat())
        if row is not None:
            row["_listing_names"] = listing_names
        section = build_day_strong_mover_sections(
            dr,
            row,
            news_by_calendar=news_by_calendar,
            news_cutoff_label=news_cutoff_label,
            market_by_code=market_by_code,
        )
        if section:
            out.append(section)
    return out


_THEME_DAY_SECTION_TEMPLATE = r"""
  <section class="day" id="day-{{ day.trading_day.isoformat() }}">
    <h2>{{ day.trading_day.isoformat() }}</h2>
    <p class="day-meta">early {{ day.news_prev_day }} {{ day.news_cutoff_label }}</p>
    {% if day.overview_html %}{{ day.overview_html | safe }}{% endif %}

    {% for sec in day.sectors %}
    <h3>{{ sec.theme }} ({{ sec.count }})</h3>
    {{ sec.rationale_html | safe }}
    <table class="stocks">
      <thead><tr><th>종목</th><th>테마</th><th>시장</th><th>등락</th><th>근거 요약</th></tr></thead>
      <tbody>
      {% for st in sec.stocks %}
        <tr>
          <td><strong>{{ st.name }}</strong> <code>{{ st.code }}</code></td>
          <td class="theme">{{ st.theme }}</td>
          <td class="market">{{ st.market }}</td>
          <td class="ok">{{ "%+.1f"|format(st.return_pct) }}%{% if st.is_limit_up %} <span class="warn">상한</span>{% endif %}</td>
          <td class="reason muted">{{ st.reason | safe }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% endfor %}
  </section>
"""

_THEME_REPORT_SHELL_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root {
      --bg: #0f1419; --card: #1a2332; --text: #e7ecf3; --muted: #8b9cb3;
      --accent: #3d9cf5; --ok: #3ecf8e; --warn: #f0a84a;
    }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 16px 20px 40px;
           line-height: 1.5; max-width: 1100px; font-size: 0.88rem; }
    h1 { font-size: 1.3rem; margin-bottom: 0.25rem; }
    h2 { font-size: 1.05rem; color: var(--ok); margin: 20px 0 6px; }
    h3 { font-size: 0.95rem; color: var(--accent); margin: 14px 0 4px; }
    .sub { color: var(--muted); font-size: 0.82rem; margin-bottom: 0.8rem; }
    .pill { display: inline-block; padding: 1px 6px; border-radius: 999px;
            background: #243044; font-size: 0.68rem; margin-right: 3px; }
    .muted { color: var(--muted); }
    .ok { color: var(--ok); font-weight: 600; }
    .warn { color: var(--warn); font-size: 0.72rem; }
    section.day { background: var(--card); border: 1px solid #243044; border-radius: 8px;
                   padding: 12px 14px; margin-bottom: 16px; }
    .day-meta { font-size: 0.78rem; color: var(--muted); margin: 0 0 6px; }
    .day-stats { margin: 0 0 8px; font-size: 0.82rem; }
    .sector-why { margin: 0 0 6px; font-size: 0.8rem; color: #c8d8e8; }
    table.stocks { width: 100%; border-collapse: collapse; font-size: 0.8rem; margin-bottom: 10px; }
    table.stocks th { text-align: left; color: var(--muted); font-weight: 600;
                      border-bottom: 1px solid #2a3f5c; padding: 4px 6px; }
    table.stocks td { padding: 4px 6px; border-bottom: 1px solid #1e2a3a; vertical-align: top; }
    table.stocks td.reason { font-size: 0.78rem; line-height: 1.45; max-width: 420px; }
    table.stocks td.theme { color: var(--accent); white-space: nowrap; }
    table.stocks td.market { white-space: nowrap; font-size: 0.75rem; color: var(--muted); }
    table.stocks tr:hover td { background: #121c28; }
    .disclaimer { font-size: 0.72rem; color: var(--muted); margin-top: 20px; }
    code { font-size: 0.72rem; color: var(--muted); }
  </style>
</head>
<body>
  <h1>{{ title }}</h1>
  <p class="sub">{{ subtitle }}</p>
  <p class="sub">종가 {{ strong_pct }}%↑ · early {{ cutoff }}(KST) · 상세 뉴스·공시는 메인 리포트 참고</p>

  {% for block in day_section_blocks %}
  {{ block | safe }}
  {% endfor %}

  <p class="disclaimer">자동 추정·참고용. 인과 단정이 아닙니다.</p>
</body>
</html>
"""


def extract_theme_report_day_sections(source: Path | str) -> dict[date, str]:
    """기존 ``report_theme_25pct_*.html`` 에서 ``id=\"day-YYYY-MM-DD\"`` 일자 블록을 추출합니다."""
    from . import report

    return report.extract_monthly_report_day_sections(source)


def render_theme_day_section_html(day: dict[str, Any]) -> str:
    """단일 거래일 테마 25%↑ 섹션 HTML."""
    from jinja2 import Environment, select_autoescape

    env = Environment(autoescape=select_autoescape(["html", "xml"]))
    tpl = env.from_string(_THEME_DAY_SECTION_TEMPLATE)
    return tpl.render(day=day).strip()


def format_strong_mover_theme_report_html(
    days: list[dict[str, Any]],
    *,
    title: str,
    subtitle: str,
    meta_note: str,
    news_cutoff_label: str,
    preserved_day_html: dict[date, str] | None = None,
) -> str:
    """일자 섹션 블록을 합쳐 완전한 테마 리포트 HTML 문자열 생성."""
    from jinja2 import Environment, select_autoescape

    preserved = preserved_day_html or {}
    new_by_day = {d["trading_day"]: d for d in days}
    all_days = sorted(set(new_by_day.keys()) | set(preserved.keys()))
    blocks: list[str] = []
    for d in all_days:
        if d in new_by_day:
            blocks.append(render_theme_day_section_html(new_by_day[d]))
        else:
            blocks.append(preserved[d].strip())
    env = Environment(autoescape=select_autoescape(["html", "xml"]))
    tpl = env.from_string(_THEME_REPORT_SHELL_TEMPLATE)
    return tpl.render(
        title=title,
        subtitle=subtitle,
        meta_note=meta_note,
        strong_pct=int(srl._STRONG_MOVER_PCT),
        cutoff=news_cutoff_label,
        day_section_blocks=blocks,
    )


def render_strong_mover_theme_report(
    out_path: Path,
    *,
    day_reports: list[Any],
    theme_flow_rows: list[dict[str, Any]],
    news_by_calendar: dict[date, list[dict[str, str]]],
    listing_names: dict[str, str],
    news_cutoff_label: str,
    title: str,
    subtitle: str,
    meta_note: str = "",
    preserved_day_html: dict[date, str] | None = None,
) -> Path | None:
    """25%↑ 테마 리포트 HTML 파일을 ``out_path`` 에 저장. 섹션 없으면 ``None``."""
    days = build_strong_mover_report_days(
        day_reports,
        theme_flow_rows,
        news_by_calendar=news_by_calendar,
        listing_names=listing_names,
        news_cutoff_label=news_cutoff_label,
    )
    preserved = preserved_day_html or {}
    if not days and not preserved:
        return None
    html_out = format_strong_mover_theme_report_html(
        days,
        title=title,
        subtitle=subtitle,
        meta_note=meta_note,
        news_cutoff_label=news_cutoff_label,
        preserved_day_html=preserved,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_out, encoding="utf-8")
    return out_path


def theme_report_filename_for_month(year: int, month: int) -> str:
    """월간 테마 리포트 파일명 (예: ``report_theme_25pct_2026.06.html``)."""
    return f"report_theme_25pct_{year}.{month:02d}.html"


def theme_report_filename_for_range(d_from: date, d_to: date) -> str:
    """구간 테마 리포트 파일명(동월이면 월간, 아니면 From_To)."""
    if d_from.year == d_to.year and d_from.month == d_to.month:
        return theme_report_filename_for_month(d_from.year, d_from.month)
    return (
        f"report_theme_25pct_{d_from.strftime('%Y%m%d')}_{d_to.strftime('%Y%m%d')}.html"
    )
