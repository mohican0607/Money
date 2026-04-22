"""
HTML 리포트 생성(Jinja2 템플릿 문자열 내장).

- 월간/구간 배치: ``render_compact_tabbed_report`` + ``render_movers_index``
- 단일 ``main.py N``: ``render_dated_n_report`` → ``output/report_dated_by_n.html`` 에 기준일(N) 블록 누적, 같은 N 재실행 시 해당 블록만 교체
스타일은 다크 테마 위주의 단일 HTML 파일로 ``output/`` 에 저장합니다.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from jinja2 import Environment, select_autoescape

from . import config
from .features import highlight_terms


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
REPORT_TABLE_INTERACTION_MARKER = "money-report-table-interaction"
REPORT_TABLE_INTERACTION_SNIPPET = r"""<!-- money-report-table-interaction -->
<script>
(function () {
  function numKey(v) {
    if (v == null || v === "") return null;
    var n = parseFloat(String(v), 10);
    return isNaN(n) ? null : n;
  }
  function textKey(v) {
    if (v == null) return "";
    return String(v).toLowerCase();
  }
  function cellSortValue(tr, col) {
    var td = tr.querySelector('td[data-sort-col="' + col + '"]');
    if (!td) return null;
    return td.getAttribute("data-sort-value");
  }
  function bindSortTable(table) {
    var tbody = table.querySelector("tbody");
    if (!tbody) return;
    var ths = table.querySelectorAll("th.sortable-col");
    if (!ths.length) return;
    ths.forEach(function (th) {
      th.addEventListener("click", function () {
        var col = th.getAttribute("data-sort");
        if (!col) return;
        var descending = th.getAttribute("data-sort-dir") !== "desc";
        table.querySelectorAll("th.sortable-col").forEach(function (h) {
          h.removeAttribute("data-sort-dir");
          h.classList.remove("sort-asc", "sort-desc");
        });
        th.setAttribute("data-sort-dir", descending ? "desc" : "asc");
        if (col === "cumulative") {
          th.classList.add(descending ? "sort-asc" : "sort-desc");
        } else {
          th.classList.add(descending ? "sort-desc" : "sort-asc");
        }
        var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));
        rows.sort(function (a, b) {
          var va = cellSortValue(a, col);
          var vb = cellSortValue(b, col);
          if (va == null && vb == null) return 0;
          if (va == null) return 1;
          if (vb == null) return -1;
          if (col === "stock") {
            var sa = textKey(va);
            var sb = textKey(vb);
            return descending ? sb.localeCompare(sa) : sa.localeCompare(sb);
          }
          var na = numKey(va);
          var nb = numKey(vb);
          if (na == null && nb == null) return 0;
          if (na == null) return 1;
          if (nb == null) return -1;
          if (col === "cumulative") {
            var da = Math.abs(na - 1);
            var db = Math.abs(nb - 1);
            return descending ? da - db : db - da;
          }
          return descending ? nb - na : na - nb;
        });
        rows.forEach(function (r) { tbody.appendChild(r); });
      });
    });
  }
  function bindMarketRowFilters(root) {
    root.querySelectorAll(".day-market-block").forEach(function (block) {
      var table = block.querySelector("table.rows-compare");
      if (!table) return;
      var tbody = table.querySelector("tbody");
      if (!tbody) return;
      var radios = block.querySelectorAll(".market-filter-radios input[type=radio]");
      if (!radios.length) return;
      function apply() {
        var sel = "all";
        for (var i = 0; i < radios.length; i++) {
          if (radios[i].checked) sel = radios[i].value;
        }
        tbody.querySelectorAll("tr").forEach(function (tr) {
          var m = tr.getAttribute("data-market") || "other";
          if (sel === "all") {
            tr.style.removeProperty("display");
            return;
          }
          if (m === "kospi" || m === "kosdaq") {
            tr.style.display = sel === m ? "" : "none";
          } else {
            tr.style.display = "none";
          }
        });
      }
      radios.forEach(function (r) { r.addEventListener("change", apply); });
      apply();
    });
  }
  document.querySelectorAll("table.rows-compare").forEach(bindSortTable);
  bindMarketRowFilters(document);
})();
</script>"""


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
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")


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


def render_compact_tabbed_report(
    title: str,
    days: list[DayReport],
    meta: dict,
    out_path: Path,
    *,
    week_note: str | None = None,
    stack_days: bool = False,
    week_tabs_stack_days: bool = False,
) -> None:
    """
    실제 20%↑·예측 후보 비교 표 중심의 컴팩트 리포트를 ``out_path`` 에 씁니다.

    Args:
        week_note: 상단에 표시할 설명(예: 월 범위 안내).
        stack_days: ISO 주 탭 없이 일자별 섹션만 세로 스택.
        week_tabs_stack_days: True이면 월요일 기준 주별 탭 → 탭 내부에서 거래일 오름차순 스택
            (``main`` 월간/구간 배치에서 사용).
    """
    week_panels: list[dict] | None = None
    if week_tabs_stack_days:
        by_week: dict[date, list[DayReport]] = {}
        for dr in days:
            by_week.setdefault(_monday_of_iso_week(dr.trading_day), []).append(dr)
        week_panels = []
        for mon in sorted(by_week.keys()):
            wdays = sorted(by_week[mon], key=lambda x: x.trading_day)
            fd, ld = wdays[0].trading_day, wdays[-1].trading_day
            week_panels.append(
                {
                    "monday": mon,
                    "label": f"{mon.isoformat()} 주 · {fd.isoformat()} ~ {ld.isoformat()}",
                    "days": wdays,
                }
            )
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
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")


_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root {
      --bg: #0f1419;
      --card: #1a2332;
      --text: #e7ecf3;
      --muted: #8b9cb3;
      --accent: #3d9cf5;
      --ok: #3ecf8e;
      --bad: #f07178;
      --warn: #e6c07b;
    }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.55; }
    h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
    .sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 1.5rem; }
    section { background: var(--card); border-radius: 12px; padding: 20px; margin-bottom: 20px;
              border: 1px solid #243044; }
    h2 { font-size: 1.15rem; margin: 0 0 12px 0; color: var(--accent); }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    th, td { padding: 10px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    tr:hover td { background: #1e2a3d; }
    a.stock { color: var(--accent); text-decoration: none; font-weight: 600; }
    a.stock:hover { text-decoration: underline; }
    mark { background: #e6c07b; color: #1a1a1a; padding: 0 2px; border-radius: 2px; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 0.75rem;
            margin: 2px 4px 2px 0; background: #243044; color: var(--muted); }
    .ok { color: var(--ok); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .reasons { font-size: 0.82rem; color: var(--muted); }
    .fn-block { margin-top: 14px; padding: 12px; background: #131b28; border-radius: 8px;
                border-left: 3px solid var(--bad); }
    .gap-tip { position: relative; display: inline-block; margin-top: 6px; max-width: 100%; }
    .gap-tip.gap-tip-inline { margin-top: 0; margin-left: 10px; vertical-align: baseline; }
    .gap-tip.gap-tip-end .gap-tip-popup { left: auto; right: 0; }
    .gap-tip-trigger {
      cursor: help; border-bottom: 1px dotted var(--accent); color: var(--accent);
      font-size: 0.78rem; font-weight: 600; outline: none;
    }
    .gap-tip-trigger:hover, .gap-tip-trigger:focus { color: #7ec4ff; }
    .gap-tip-popup {
      display: none; position: absolute; z-index: 500; left: 0; top: calc(100% + 6px);
      min-width: 380px; max-width: min(920px, 96vw); max-height: 92vh;
      overflow: auto; padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e;
      border-radius: 8px; box-shadow: 0 8px 24px rgba(0,0,0,0.45);
      font-size: 0.82rem; line-height: 1.55; color: #d0dce8; text-align: left;
    }
    .gap-tip-popup p { margin: 0 0 6px 0; }
    .gap-tip-popup ul { margin: 4px 0 0 16px; padding: 0; }
    .gap-tip-popup li { margin-bottom: 4px; }
    .gap-tip:hover .gap-tip-popup, .gap-tip:focus-within .gap-tip-popup { display: block; }
    .pred-reason-plain { font-size: 0.82rem; color: var(--muted); line-height: 1.45; max-width: 28em; display: inline-block; vertical-align: middle; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
    .combo-tip { margin-left: 8px; vertical-align: middle; white-space: nowrap; }
    .combo-tip-popup {
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      left: 0 !important;
      right: auto !important;
      transform: none !important;
      top: calc(100% + 8px) !important;
      box-sizing: border-box;
      max-height: 90vh;
      overflow: auto;
      padding: 14px 16px !important;
    }
    .combo-tip-inner {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0 20px;
      align-items: start;
      width: 100%;
    }
    .combo-tip-col { min-width: 0; border-left: 1px solid #2d4a6a; padding-left: 16px; }
    .combo-tip-col:first-child { border-left: none; padding-left: 0; padding-right: 4px; }
    .combo-tip-h {
      margin: 0 0 8px 0;
      font-size: 0.78rem;
      font-weight: 700;
      color: #8ec5f6;
      text-transform: none;
    }
    .combo-tip-body {
      font-size: 0.82rem; line-height: 1.6; color: #d0dce8;
      overflow-wrap: anywhere; word-wrap: break-word; word-break: break-word;
      white-space: normal;
    }
    .combo-tip-body p { margin: 0 0 6px 0; }
    .combo-tip-empty { margin: 0; color: var(--muted); font-style: italic; font-size: 0.82rem; }
    .combo-tip-rise {
      grid-column: 1 / -1;
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid #2d4a6a;
      width: 100%;
    }
    ul.nl { margin: 4px 0 0 0; padding-left: 18px; }
    ul.nl li { margin-bottom: 6px; }
    @media (max-width: 760px) {
      .combo-tip-inner { grid-template-columns: 1fr; }
      .combo-tip-col { border-left: none; padding-left: 0; border-top: 1px solid #2d4a6a; padding-top: 12px; margin-top: 10px; }
      .combo-tip-col:first-child { border-top: none; padding-top: 0; margin-top: 0; }
    }
    .kw-pills { display: inline-flex; flex-wrap: wrap; gap: 4px 4px; align-items: center; vertical-align: middle; }
    ul.news { margin: 0; padding-left: 18px; color: var(--muted); font-size: 0.85rem; }
    .day-heading-row { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-start; gap: 8px 14px; margin-bottom: 6px; }
    .day-heading-row h2 { margin: 0; }
    .market-filter-radios { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; font-size: 0.82rem; color: var(--muted); }
    .market-filter-title { font-weight: 600; color: var(--muted); margin-right: 2px; }
    .market-filter-label { cursor: pointer; display: inline-flex; align-items: center; gap: 5px; margin: 0; font-weight: 500; }
    .market-filter-label input { accent-color: var(--accent); vertical-align: middle; }
    .tab-bar { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
    .tab-btn { font: inherit; cursor: pointer; padding: 8px 14px; border-radius: 8px; border: 1px solid #2a3548;
              background: #131b28; color: var(--muted); }
    .tab-btn.active { background: var(--accent); color: #0f1419; border-color: var(--accent); font-weight: 600; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    .tabs-wrap section { margin-bottom: 16px; }
    table.rows-compare { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    table.rows-compare th, table.rows-compare td { padding: 10px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top; }
    table.rows-compare th.sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th.sortable-col:hover { text-decoration: underline; }
    table.rows-compare th.sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th.sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    .gap-tip.cumulative-hist-tip { margin-top: 0; vertical-align: middle; }
    .gap-tip.cumulative-hist-tip .gap-tip-trigger {
      font-size: inherit;
      font-variant-numeric: tabular-nums;
    }
    .gap-tip.cumulative-hist-tip .gap-tip-popup.cumulative-hist-popup {
      min-width: 280px;
      max-width: min(440px, 94vw);
      z-index: 600;
    }
    .stock-chart-tip { position: relative; display: inline-block; vertical-align: baseline; max-width: 100%; }
    .stock-chart-tip .stock { position: relative; z-index: 1; }
    .stock-chart-popup {
      display: none; position: absolute; left: 0; top: calc(100% + 6px); z-index: 850;
      padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e; border-radius: 10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.55);
      min-width: min(720px, calc(100vw - 32px)); max-width: min(720px, calc(100vw - 24px));
      box-sizing: border-box;
    }
    .stock-chart-tip:hover .stock-chart-popup,
    .stock-chart-tip:focus-within .stock-chart-popup { display: block; }
    .stock-chart-img {
      display: block; width: 100%; max-width: 700px; height: auto;
      background: #0f1419; border-radius: 6px;
    }
    .stock-chart-caption { display: block; margin-top: 8px; font-size: 0.74rem; color: var(--muted); text-align: center; line-height: 1.4; }
  </style>
</head>
<body>
{% macro stock_name_link(code, name) -%}
<span class="stock-chart-tip" tabindex="0">
  <a class="stock" target="_blank" rel="noopener" href="{{ naver_chart_url(code) }}" title="클릭: 네이버 차트 · 호버: 일봉 캔들">{{ name }}</a>
  <span class="stock-chart-popup" role="tooltip">
    <img class="stock-chart-img" src="{{ naver_chart_day_img_url(code) }}" alt="{{ name }} 일봉 캔들 차트" width="700" height="289" loading="lazy" decoding="async" referrerpolicy="no-referrer-when-downgrade"/>
    <span class="stock-chart-caption">일봉 캔들 · 가로축은 거래일(네이버 금융 이미지)</span>
  </span>
</span>
{%- endmacro %}
{% macro actual_ret_cell(r) -%}
{% if r.actual_cell_pre_close_snapshot | default(false) %}{% if r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ "%.2f"|format(r.actual_ret_intraday_pct) }}%){% elif r.actual_ret is not none %}— ({{ "%.2f"|format(r.actual_ret * 100) }}%){% else %}—{% endif %}{% elif r.actual_ret is not none %}{{ "%.2f"|format(r.actual_ret * 100) }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ "%.2f"|format(r.actual_ret_intraday_pct) }}%){% else %}—{% endif %}
{%- endmacro %}
{% macro cumulative_accuracy_td(r, meta) -%}
<td style="white-space:nowrap;font-variant-numeric:tabular-nums" data-sort-col="cumulative" data-sort-value="{% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}{{ r.cumulative_accuracy_avg }}{% endif %}">
  {% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
  <span class="gap-tip cumulative-hist-tip">
    <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="누적 정확도·{{ meta.threshold }} 이상 예측 이력">{{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} ({{ r.cumulative_hit_x }} {% if r.cumulative_hit_z is defined and r.cumulative_hit_z is not none %}{{ r.cumulative_hit_z }}{% else %}0{% endif %} <span class="bad">{% if r.cumulative_hit_neg is defined and r.cumulative_hit_neg is not none %}{{ r.cumulative_hit_neg }}{% else %}0{% endif %}</span> / {{ r.cumulative_hit_y }}){% endif %}</span>
    <div class="gap-tip-popup cumulative-hist-popup" role="tooltip">
      <div class="combo-tip-body">
        <strong>관측일 T별 · 예측 ≥ {{ meta.threshold }}</strong>
        {% if r.pred_high_history|default([])|length > 0 %}
        <ul style="margin:8px 0 0 0;padding-left:18px">
        {% for h in r.pred_high_history %}
          <li><span class="pill">{{ h.t }}</span> 예측 {{ "%.2f"|format(h.pred_pct) }}%
            {% if h.actual_pct is not none %} · 실제 {{ "%.2f"|format(h.actual_pct) }}%{% else %} · 실적 미확정{% endif %}
          </li>
        {% endfor %}
        </ul>
        {% else %}
        <p class="combo-tip-empty" style="margin:8px 0 0 0">저장된 {{ meta.threshold }} 이상 예측 이력이 없습니다.</p>
        {% endif %}
        <p style="font-size:0.76rem;color:var(--muted);margin:8px 0 0 0;line-height:1.35">앞 {{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%: 관측일별 min(|실제%|,|예측%|) / max(|실제%|,|예측%|) 평균(정확히 일치할 때만 100%).{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} 괄호 (a b c / d): 예측≥{{ meta.threshold }}·실적 확정 건 중 a=실제≥{{ meta.threshold }}, b=0&lt;실제&lt;{{ meta.threshold }}, c=실제&lt;0(빨간색), d=전체.{% endif %}</p>
      </div>
    </div>
  </span>
  {% else %}—{% endif %}
</td>
{%- endmacro %}
{% macro market_filter_radios(suffix) -%}
<div class="market-filter-radios" role="radiogroup" aria-label="표 시장 구분">
  <span class="market-filter-title">시장 -</span>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="all" checked="checked"/> 전체</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kospi"/> KOSPI</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kosdaq"/> KOSDAQ</label>
</div>
{%- endmacro %}
{% macro day_panel(d, meta) -%}
  <section id="day-{{ d.trading_day.isoformat() }}" class="day-market-block">
    <div class="day-heading-row">
      <h2>{{ d.trading_day.isoformat() }} (거래일)</h2>
      {{ market_filter_radios(d.trading_day.isoformat()) }}
    </div>
    <p class="sub">{% if meta.use_decision_cutoff %}N-1 거래일 {{ meta.cutoff_kst }}(KST)까지 반영한 {% endif %}예측 입력 뉴스 하이라이트 키워드 예시:
      {% for t in d.news_highlight_terms[:20] %}
      <span class="pill">{{ t }}</span>
      {% endfor %}
    </p>

    <h3 style="font-size:1rem;color:var(--ok);margin:16px 0 8px;">실제·예측 20% 이상 포함 종목</h3>
    <p class="sub" style="margin-top:0">당일 <strong>실제</strong> {{ meta.threshold }} 이상 급등 종목과, 모델 <strong>예측 상승률</strong>이 {{ meta.threshold }} 이상인 상위 후보(중복 제거)를 함께 표시합니다.</p>
    {% if d.rows_compare %}
    <table class="rows-compare">
      <thead>
        <tr>
          <th class="sortable-col" data-sort="group" scope="col" title="구분 우선순위 정렬: 실제+예측 > 실제만 > 예측만">구분</th>
          <th class="sortable-col" data-sort="stock" scope="col" title="종목명/코드 오름차순·내림차순 정렬">종목</th>
          <th class="sortable-col" data-sort="actual" scope="col" title="종가 확정 후 일봉 기준. 금일 장 마감 전(15:30 KST 전)에는 일봉 확정 전이므로 — 뒤 괄호에 pykrx·네이버 실시간 등락률(리포트 생성 시점)을 둡니다.">실제 상승률(%)<br/><span style="font-size:0.68rem;font-weight:500;color:var(--muted)">(장중·참고)</span></th>
          <th class="sortable-col" data-sort="pred" scope="col">예측 상승률(%)</th>
          <th>보정(%)</th>
          <th class="sortable-col" data-sort="cumulative" scope="col" title="예측≥임계 후보만. 앞: 관측일별 min(|실제%|,|예측%|)/max(|실제%|,|예측%|) 평균(정확히 일치할 때만 100%). 괄호: a=실제≥임계, b=0&lt;실제&lt;임계, c=실제&lt;0, d=예측≥임계·실적 확정 전체 (a b c / d)">누적 정확도<br/><span style="font-size:0.68rem;font-weight:500;color:var(--muted)">(달성%·a b c / d)</span></th>
          <th>이유/차이</th>
          <th>일치 키워드</th>
        </tr>
      </thead>
      <tbody>
        {% for r in d.rows_compare %}
        <tr data-market="{{ r.market_segment|default('other') }}">
          <td style="white-space:nowrap;vertical-align:top" data-sort-col="group" data-sort-value="{% if r.actual_big and (r.pred_high | default(false)) %}3{% elif r.actual_big %}2{% elif r.pred_high | default(false) %}1{% else %}0{% endif %}">
            {% if r.actual_big %}<span class="pill" style="background:#1e3d2f;color:var(--ok)">실제≥{{ meta.threshold }}</span>{% endif %}
            {% if r.pred_high | default(false) %}<span class="pill" style="margin-top:4px;display:inline-block">예측≥{{ meta.threshold }}</span>{% endif %}
          </td>
          <td data-sort-col="stock" data-sort-value="{{ r.name }} {{ r.code }}">
            {{ stock_name_link(r.code, r.name) }}
            <div class="pill">{{ r.code }}</div>
          </td>
          <td class="{% if r.actual_big %}ok{% elif r.actual_ret is not none and r.actual_ret < 0 %}bad{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none and r.actual_ret_intraday_pct < 0 %}bad{% endif %}" data-sort-col="actual" data-sort-value="{% if r.actual_cell_pre_close_snapshot | default(false) and r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% elif r.actual_ret is not none %}{{ r.actual_ret }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% endif %}">
            {{ actual_ret_cell(r) }}
          </td>
          <td class="{% if r.pred_high | default(false) %}warn{% endif %}" style="vertical-align:top;{% if r.pred_high | default(false) %}color:var(--warn);font-weight:600{% endif %}" data-sort-col="pred" data-sort-value="{% if r.pred_ret is not none %}{{ r.pred_ret }}{% endif %}">
            {% if r.pred_ret is not none %}{{ "%.2f"|format(r.pred_ret) }}{% else %}—{% endif %}
          </td>
          <td style="vertical-align:top">
            {% if r.pred_ret is not none and r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
            {% if r.cumulative_accuracy_from_hist | default(false) %}—{% else %}{{ "%.2f"|format(r.pred_ret * r.cumulative_accuracy_avg) }}{% endif %}
            {% else %}—{% endif %}
          </td>
          {{ cumulative_accuracy_td(r, meta) | safe }}
          <td class="pred-reason">
            <span class="gap-tip combo-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 이유, 예측·실제 차이, 상승 이유(참고)를 함께 보기">통합 보기</span>
              <div class="gap-tip-popup combo-tip-popup" role="tooltip">
                <div class="combo-tip-inner">
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측 이유</h4>
                    <div class="combo-tip-body">{{ r.pred_reason_detail_html | default('') | safe }}</div>
                  </div>
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측·실제 차이</h4>
                    <div class="combo-tip-body">
                      {% if r.gap_analysis_html %}
                      {{ r.gap_analysis_html | safe }}
                      {% else %}
                      <p class="combo-tip-empty">해당 설명이 없습니다.</p>
                      {% endif %}
                    </div>
                  </div>
                  <div class="combo-tip-rise">
                    <h4 class="combo-tip-h">상승 이유 (참고)</h4>
                    <div class="combo-tip-body">{{ r.rise_reason_html | default('') | safe }}</div>
                  </div>
                </div>
              </div>
            </span>
            <span class="combo-tip" style="margin-left:10px">
              <a class="gap-tip-trigger" target="_blank" rel="noopener" href="{{ naver_disclosure_url(r.code) }}">공시</a>
            </span>
            <span class="pred-reason-plain" style="margin-left:10px">{{ r.pred_reason_hit_line | default(r.pred_reason_summary) | default('—') }}</span>
          </td>
          <td>
            <span class="kw-pills">
              {% for k in r.keywords[:12] %}<span class="pill">{{ k }}</span>{% endfor %}
            </span>
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% else %}
    <p class="sub">당일 실제·예측 {{ meta.threshold }} 이상 해당 종목 없음.</p>
    {% endif %}

    {% if d.false_negatives %}
    <h3 style="font-size:1rem;color:var(--bad);margin:20px 0 8px;">예측했으나 실제 음수 수익 — 집중 점검</h3>
    {% for fn in d.false_negatives %}
    <div class="fn-block">
      <strong>{{ stock_name_link(fn.code, fn.name) }}</strong>
      ({{ fn.code }}) · 예측 {{ "%.2f"|format(fn.pred_ret) }}% · 실제
      <span class="bad">{{ "%.2f"|format(fn.actual_ret * 100) }}%</span>
      <p class="reasons" style="margin:8px 0;">{{ fn.analysis }}</p>
      <p class="reasons"><em>예측 시 참고한 키워드:</em>
        {% for k in fn.keywords[:15] %}<span class="pill">{{ k }}</span>{% endfor %}
      </p>
    </div>
    {% endfor %}
    {% endif %}

    <h3 style="font-size:1rem;color:var(--muted);margin:20px 0 8px;">참고 뉴스 제목 (일부)</h3>
    <ul class="news">
      {% for t in d.news_titles_sample[:12] %}
      <li>{{ highlight_terms(t, d.news_highlight_terms) | safe }}</li>
      {% endfor %}
    </ul>
  </section>
{%- endmacro %}

  <h1>{{ title }}</h1>
  <p class="sub">
    생성 기준: 훈련 구간 {{ meta.train_range }} · 테스트 구간 {{ meta.test_range }} ·
    급등 기준 {{ meta.threshold }} · 뉴스 출처: {{ meta.news_source }}
  </p>

  <section>
    <h2>주의사항 · 매수 시나리오와 뉴스 시각</h2>
    {% if meta.use_decision_cutoff %}
    <ul class="news" style="margin-top:8px;line-height:1.6">
      <li>한국 현물시장은 <strong>15:00</strong>에 장이 마감됩니다. 본 리포트는 <strong>N 거래일 장 마감 직전(약 14:00~14:50)</strong>에 매수 주문을 넣어 <strong>N+1 거래일</strong>에 20% 이상 상승할 종목을 고르는 전제에 맞춥니다.</li>
      <li>그에 따라 예측·훈련에 쓰는 뉴스는 <strong>N-1 거래일 {{ meta.cutoff_kst }}(KST)까지</strong>로 제한합니다. (N = 익일 T의 직전 거래일, N-1은 그 이전 거래일.) <strong>N-1일 {{ meta.cutoff_kst }} 이후</strong> 기사와 <strong>N일·주말</strong> 등 그 다음 캘린더 구간 뉴스는 예측 입력에서 빠집니다.</li>
      <li><strong>N-1일 {{ meta.cutoff_kst }} 이후</strong> 뉴스와, 예측 종목이 <strong>실제로 20% 미만</strong>으로 마감한 경우의 겹침(간단 문자열 매칭)은 아래 &quot;탐색&quot; 표로 따로 집계합니다. 인과 검증이 아니라 후속 분석용입니다.</li>
      <li>기사에 시각이 없거나 옛 캐시면 해당 캘린더일은 <strong>09:00 KST</strong>로 간주해 early/late를 나눕니다. 시각이 중요하면 <code>data/cache/news/naver</code> 또는 <code>…/google</code> 아래 해당 월·일 JSON 삭제 후 재수집을 권장합니다.</li>
    </ul>
    {% else %}
    <p class="sub" style="margin:0">
      일자 단위 전통 뉴스 윈도우만 사용 중입니다. N-1 거래일 {{ meta.cutoff_kst }}(KST) 컷오프·지연 뉴스 탐색을 쓰려면 <code>USE_DECISION_NEWS_INTRADAY_CUTOFF=1</code>로 실행하세요.
    </p>
    {% endif %}
  </section>

  <section>
    <h2>요약</h2>
    <p class="sub" style="margin:0">
      테스트 일수 {{ meta.n_days }} · 예측 종목 총건수 {{ meta.total_preds }} ·
      실제 20% 이상 급등 {{ meta.total_actual_big }}건 ·
      <span class="bad">예측했으나 실제 음수 수익</span> {{ meta.n_false_neg }}건
    </p>
  </section>

  {% if meta.correlation_rows %}
  <section>
    <h2>훈련 구간: 전일 뉴스 키워드와 당일 20% 급등의 공출현 요약</h2>
    <p class="sub" style="margin-top:0">
      각 급등일에 수집된 뉴스에서 추출한 키워드가 몇 번의 급등 사건에서 등장했는지 집계했습니다.
      (인과가 아닌 공출현·패턴 탐색용 지표입니다.)
    </p>
    <table>
      <thead><tr><th>키워드</th><th>급등 사건 수(종목·일)</th></tr></thead>
      <tbody>
        {% for word, cnt in meta.correlation_rows %}
        <tr><td>{{ word }}</td><td>{{ cnt }}</td></tr>
        {% endfor %}
      </tbody>
    </table>
  </section>
  {% endif %}

  {% if meta.late_news_probe %}
  <section>
    <h2>N-1일 {{ meta.cutoff_kst }} 이후 뉴스 vs 실제 20% 미만 (탐색)</h2>
    <p class="sub" style="margin-top:0">
      예측 상위 종목마다 &quot;지연 구간&quot; 뉴스(위 주의사항 정의)에 <em>예측 시 일치한 키워드</em>가 등장했는지 단순 포함 여부로 집계했습니다.
    </p>
    <table>
      <thead><tr><th>구분</th><th>표본 수</th><th>지연 뉴스에 키워드 겹침</th><th>비율</th></tr></thead>
      <tbody>
        <tr>
          <td>실제 상승률 20% 미만</td>
          <td>{{ meta.late_news_probe.below_n }}</td>
          <td>{{ meta.late_news_probe.below_kw }}</td>
          <td>{{ meta.late_news_probe.below_pct }}</td>
        </tr>
        <tr>
          <td>실제 상승률 20% 이상</td>
          <td>{{ meta.late_news_probe.gte_n }}</td>
          <td>{{ meta.late_news_probe.gte_kw }}</td>
          <td>{{ meta.late_news_probe.gte_pct }}</td>
        </tr>
      </tbody>
    </table>
  </section>
  {% endif %}

  {% if tabbed and days %}
  <section class="tabs-wrap">
    <h2>거래일별 보기 (탭)</h2>
    {% if week_note %}
    <p class="sub" style="margin-top:0">{{ week_note }}</p>
    {% endif %}
    <div class="tab-bar" role="tablist">
      {% for d in days %}
      <button type="button" class="tab-btn{% if loop.first %} active{% endif %}" role="tab"
              aria-selected="{{ 'true' if loop.first else 'false' }}" data-tab-idx="{{ loop.index0 }}">{{ d.trading_day.isoformat() }}</button>
      {% endfor %}
    </div>
    {% for d in days %}
    <div class="tab-panel{% if loop.first %} active{% endif %}" role="tabpanel" data-tab-panel="{{ loop.index0 }}">
      {{ day_panel(d, meta) }}
    </div>
    {% endfor %}
  </section>
  <script>
  (function () {
    var bar = document.querySelector(".tabs-wrap .tab-bar");
    if (!bar) return;
    var wrap = bar.closest(".tabs-wrap");
    var btns = bar.querySelectorAll(".tab-btn");
    var panels = wrap.querySelectorAll(".tab-panel");
    function show(i) {
      btns.forEach(function (b, j) {
        b.classList.toggle("active", j === i);
        b.setAttribute("aria-selected", j === i ? "true" : "false");
      });
      panels.forEach(function (p, j) { p.classList.toggle("active", j === i); });
    }
    btns.forEach(function (b, i) { b.addEventListener("click", function () { show(i); }); });
  })();
  </script>
  {% else %}
  {% for d in days %}
  {{ day_panel(d, meta) }}
  {% endfor %}
  {% endif %}
{{ interaction_snippet | safe }}
</body>
</html>
"""

_COMPACT_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root {
      --bg: #0f1419; --card: #1a2332; --text: #e7ecf3; --muted: #8b9cb3;
      --accent: #3d9cf5; --ok: #3ecf8e; --bad: #f07178; --warn: #e6c07b;
    }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.5; }
    h1 { font-size: 1.45rem; margin-bottom: 0.35rem; }
    .sub { color: var(--muted); font-size: 0.88rem; margin-bottom: 1rem; }
    section { background: var(--card); border-radius: 12px; padding: 18px 20px; margin-bottom: 18px;
              border: 1px solid #243044; }
    h2 { font-size: 1.05rem; margin: 0 0 10px 0; color: var(--accent); }
    table { width: 100%; border-collapse: collapse; font-size: 0.86rem; }
    th, td { padding: 9px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    tr:hover td { background: #1e2a3d; }
    a.stock { color: var(--accent); text-decoration: none; font-weight: 600; }
    a.stock:hover { text-decoration: underline; }
    .ok { color: var(--ok); font-weight: 600; }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); font-weight: 600; }
    .pill { display: inline-block; padding: 1px 6px; border-radius: 6px; font-size: 0.72rem;
             margin: 1px 3px 1px 0; background: #243044; color: var(--muted); }
    .tab-bar { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }
    .tab-btn { font: inherit; cursor: pointer; padding: 8px 12px; border-radius: 8px; border: 1px solid #2a3548;
              background: #131b28; color: var(--muted); }
    .tab-btn.active { background: var(--accent); color: #0f1419; border-color: var(--accent); font-weight: 600; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    .note { font-size: 0.82rem; color: var(--muted); margin-top: 10px; line-height: 1.45; }
    .day-stack { margin-bottom: 28px; padding-bottom: 4px; border-bottom: 1px solid #2a3f5c; }
    .day-stack:last-of-type { border-bottom: none; margin-bottom: 8px; }
    .day-stack > h2 { font-size: 1.08rem; margin: 0 0 12px 0; color: #8ec5f6; }
    .gap-tip { position: relative; display: inline-block; margin-top: 4px; max-width: 100%; }
    .gap-tip.gap-tip-inline { margin-top: 0; margin-left: 8px; vertical-align: baseline; }
    .gap-tip.gap-tip-end .gap-tip-popup { left: auto; right: 0; }
    .gap-tip-trigger {
      cursor: help; border-bottom: 1px dotted var(--accent); color: var(--accent);
      font-size: 0.72rem; font-weight: 600; outline: none;
    }
    .gap-tip-trigger:hover, .gap-tip-trigger:focus { color: #7ec4ff; }
    .gap-tip-popup {
      display: none; position: absolute; z-index: 500; left: 0; top: calc(100% + 6px);
      min-width: 360px; max-width: min(900px, 96vw); max-height: 92vh;
      overflow: auto; padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e;
      border-radius: 8px; box-shadow: 0 8px 24px rgba(0,0,0,0.45);
      font-size: 0.8rem; line-height: 1.55; color: #d0dce8; text-align: left;
    }
    .gap-tip-popup p { margin: 0 0 6px 0; }
    .gap-tip-popup ul { margin: 4px 0 0 14px; padding: 0; }
    .gap-tip-popup li { margin-bottom: 4px; }
    .gap-tip:hover .gap-tip-popup, .gap-tip:focus-within .gap-tip-popup { display: block; }
    .pred-reason-plain { font-size: 0.82rem; color: var(--muted); line-height: 1.45; max-width: 26em; display: inline-block; vertical-align: middle; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
    .combo-tip { margin-left: 6px; vertical-align: middle; white-space: nowrap; }
    .combo-tip-popup {
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      left: 0 !important;
      right: auto !important;
      transform: none !important;
      top: calc(100% + 8px) !important;
      box-sizing: border-box;
      max-height: 90vh;
      overflow: auto;
      padding: 14px 16px !important;
    }
    .combo-tip-inner {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0 20px;
      align-items: start;
      width: 100%;
    }
    .combo-tip-col { min-width: 0; border-left: 1px solid #2d4a6a; padding-left: 16px; }
    .combo-tip-col:first-child { border-left: none; padding-left: 0; padding-right: 4px; }
    .combo-tip-h {
      margin: 0 0 8px 0;
      font-size: 0.76rem;
      font-weight: 700;
      color: #8ec5f6;
    }
    .combo-tip-body {
      font-size: 0.8rem; line-height: 1.6; color: #d0dce8;
      overflow-wrap: anywhere; word-wrap: break-word; word-break: break-word;
      white-space: normal;
    }
    .combo-tip-body p { margin: 0 0 6px 0; }
    .combo-tip-empty { margin: 0; color: var(--muted); font-style: italic; font-size: 0.8rem; }
    .combo-tip-rise {
      grid-column: 1 / -1;
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid #2d4a6a;
      width: 100%;
    }
    ul.nl { margin: 4px 0 0 0; padding-left: 18px; }
    ul.nl li { margin-bottom: 6px; }
    @media (max-width: 760px) {
      .combo-tip-inner { grid-template-columns: 1fr; }
      .combo-tip-col { border-left: none; padding-left: 0; border-top: 1px solid #2d4a6a; padding-top: 12px; margin-top: 10px; }
      .combo-tip-col:first-child { border-top: none; padding-top: 0; margin-top: 0; }
    }
    .kw-pills { display: inline-flex; flex-wrap: wrap; gap: 4px 4px; align-items: center; vertical-align: middle; }
    .movers-data-note { background: #2a1f18; border: 1px solid #8b5a2b; border-radius: 10px;
                        padding: 12px 14px; margin-bottom: 14px; font-size: 0.86rem; line-height: 1.55;
                        color: #e8c9a8; }
    .day-heading-row { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-start; gap: 8px 14px; margin-bottom: 6px; }
    .day-heading-row h2 { margin: 0; }
    .market-filter-radios { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; font-size: 0.82rem; color: var(--muted); }
    .market-filter-title { font-weight: 600; color: var(--muted); margin-right: 2px; }
    .market-filter-label { cursor: pointer; display: inline-flex; align-items: center; gap: 5px; margin: 0; font-weight: 500; }
    .market-filter-label input { accent-color: var(--accent); vertical-align: middle; }
    table.rows-compare th.sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th.sortable-col:hover { text-decoration: underline; }
    table.rows-compare th.sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th.sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    .gap-tip.cumulative-hist-tip { margin-top: 0; vertical-align: middle; }
    .gap-tip.cumulative-hist-tip .gap-tip-trigger {
      font-size: inherit;
      font-variant-numeric: tabular-nums;
    }
    .gap-tip.cumulative-hist-tip .gap-tip-popup.cumulative-hist-popup {
      min-width: 280px;
      max-width: min(440px, 94vw);
      z-index: 600;
    }
    .stock-chart-tip { position: relative; display: inline-block; vertical-align: baseline; max-width: 100%; }
    .stock-chart-tip .stock { position: relative; z-index: 1; }
    .stock-chart-popup {
      display: none; position: absolute; left: 0; top: calc(100% + 6px); z-index: 850;
      padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e; border-radius: 10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.55);
      min-width: min(720px, calc(100vw - 32px)); max-width: min(720px, calc(100vw - 24px));
      box-sizing: border-box;
    }
    .stock-chart-tip:hover .stock-chart-popup,
    .stock-chart-tip:focus-within .stock-chart-popup { display: block; }
    .stock-chart-img {
      display: block; width: 100%; max-width: 700px; height: auto;
      background: #0f1419; border-radius: 6px;
    }
    .stock-chart-caption { display: block; margin-top: 8px; font-size: 0.74rem; color: var(--muted); text-align: center; line-height: 1.4; }
  </style>
</head>
<body>
{% macro stock_name_link(code, name) -%}
<span class="stock-chart-tip" tabindex="0">
  <a class="stock" target="_blank" rel="noopener" href="{{ naver_chart_url(code) }}" title="클릭: 네이버 차트 · 호버: 일봉 캔들">{{ name }}</a>
  <span class="stock-chart-popup" role="tooltip">
    <img class="stock-chart-img" src="{{ naver_chart_day_img_url(code) }}" alt="{{ name }} 일봉 캔들 차트" width="700" height="289" loading="lazy" decoding="async" referrerpolicy="no-referrer-when-downgrade"/>
    <span class="stock-chart-caption">일봉 캔들 · 가로축은 거래일(네이버 금융 이미지)</span>
  </span>
</span>
{%- endmacro %}
{% macro actual_ret_cell_monthly(r) -%}
{% if r.actual_cell_pre_close_snapshot | default(false) %}{% if r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ "%.2f"|format(r.actual_ret_intraday_pct) }}%){% elif r.actual_ret is not none %}— ({{ "%.2f"|format(r.actual_ret * 100) }}%){% else %}—{% endif %}{% elif r.actual_ret is not none %}{{ "%.2f"|format(r.actual_ret * 100) }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ "%.2f"|format(r.actual_ret_intraday_pct) }}%){% else %}—{% endif %}
{%- endmacro %}
{% macro compact_cumulative_td(r, meta) -%}
<td style="white-space:nowrap;font-variant-numeric:tabular-nums" data-sort-col="cumulative" data-sort-value="{% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}{{ r.cumulative_accuracy_avg }}{% endif %}">
  {% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
  <span class="gap-tip cumulative-hist-tip">
    <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="누적 정확도·{{ meta.threshold }} 이상 예측 이력">{{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} ({{ r.cumulative_hit_x }} {% if r.cumulative_hit_z is defined and r.cumulative_hit_z is not none %}{{ r.cumulative_hit_z }}{% else %}0{% endif %} <span class="bad">{% if r.cumulative_hit_neg is defined and r.cumulative_hit_neg is not none %}{{ r.cumulative_hit_neg }}{% else %}0{% endif %}</span> / {{ r.cumulative_hit_y }}){% endif %}</span>
    <div class="gap-tip-popup cumulative-hist-popup" role="tooltip">
      <div class="combo-tip-body">
        <strong>관측일 T별 · 예측 ≥ {{ meta.threshold }}</strong>
        {% if r.pred_high_history|default([])|length > 0 %}
        <ul style="margin:8px 0 0 0;padding-left:18px">
        {% for h in r.pred_high_history %}
          <li><span class="pill">{{ h.t }}</span> 예측 {{ "%.2f"|format(h.pred_pct) }}%
            {% if h.actual_pct is not none %} · 실제 {{ "%.2f"|format(h.actual_pct) }}%{% else %} · 실적 미확정{% endif %}
          </li>
        {% endfor %}
        </ul>
        {% else %}
        <p class="combo-tip-empty" style="margin:8px 0 0 0">저장된 {{ meta.threshold }} 이상 예측 이력이 없습니다.</p>
        {% endif %}
        <p style="font-size:0.76rem;color:var(--muted);margin:8px 0 0 0;line-height:1.35">앞 {{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%: 관측일별 min(|실제%|,|예측%|) / max(|실제%|,|예측%|) 평균(정확히 일치할 때만 100%).{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} 괄호 (a b c / d): 예측≥{{ meta.threshold }}·실적 확정 건 중 a=실제≥{{ meta.threshold }}, b=0&lt;실제&lt;{{ meta.threshold }}, c=실제&lt;0(빨간색), d=전체.{% endif %}</p>
      </div>
    </div>
  </span>
  {% else %}—{% endif %}
</td>
{%- endmacro %}
{% macro market_filter_radios(suffix) -%}
<div class="market-filter-radios" role="radiogroup" aria-label="표 시장 구분">
  <span class="market-filter-title">시장 -</span>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="all" checked="checked"/> 전체</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kospi"/> KOSPI</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kosdaq"/> KOSDAQ</label>
</div>
{%- endmacro %}
{% macro compact_day_table(d, empty_extra='') -%}
{% if d.rows_compare %}
<table class="rows-compare">
  <thead>
    <tr>
      <th class="sortable-col" data-sort="group" scope="col" title="구분 우선순위 정렬: 실제+예측 > 실제만 > 예측만">구분</th>
      <th class="sortable-col" data-sort="stock" scope="col" title="종목명/코드 오름차순·내림차순 정렬">종목</th>
      <th class="sortable-col" data-sort="actual" scope="col" title="종가 확정 후 일봉 기준. 금일 장 마감 전(15:30 KST 전)에는 — 뒤 괄호에 pykrx·네이버 실시간 등락률(리포트 생성 시점)을 둡니다.">실제 상승률(%)<br/><span style="font-size:0.65rem;font-weight:500;color:var(--muted)">(장중·참고)</span></th>
      <th class="sortable-col" data-sort="pred" scope="col">예측 상승률(%)</th>
      <th>보정(%)</th>
      <th class="sortable-col" data-sort="cumulative" scope="col" title="예측≥임계 후보만. 앞: 관측일별 min(|실제%|,|예측%|)/max(|실제%|,|예측%|) 평균(정확히 일치할 때만 100%). 괄호: a=실제≥임계, b=0&lt;실제&lt;임계, c=실제&lt;0, d=예측≥임계·실적 확정 전체 (a b c / d)">누적 정확도<br/><span style="font-size:0.65rem;font-weight:500;color:var(--muted)">(달성%·a b c / d)</span></th>
      <th>이유/차이</th>
      <th>일치 키워드</th>
    </tr>
  </thead>
  <tbody>
    {% for r in d.rows_compare %}
    <tr data-market="{{ r.market_segment|default('other') }}">
      <td style="white-space:nowrap;vertical-align:top" data-sort-col="group" data-sort-value="{% if r.actual_big and (r.pred_high | default(false)) %}3{% elif r.actual_big %}2{% elif r.pred_high | default(false) %}1{% else %}0{% endif %}">
        {% if r.actual_big %}<span class="pill" style="background:#1e3d2f;color:var(--ok)">실제</span>{% endif %}
        {% if r.pred_high | default(false) %}<span class="pill" style="margin-top:4px;display:inline-block">예측</span>{% endif %}
      </td>
      <td data-sort-col="stock" data-sort-value="{{ r.name }} {{ r.code }}">
        {{ stock_name_link(r.code, r.name) }}
        <div class="pill">{{ r.code }}</div>
      </td>
      <td class="{% if r.actual_big %}ok{% elif r.actual_ret is not none and r.actual_ret < 0 %}bad{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none and r.actual_ret_intraday_pct < 0 %}bad{% endif %}" data-sort-col="actual" data-sort-value="{% if r.actual_cell_pre_close_snapshot | default(false) and r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% elif r.actual_ret is not none %}{{ r.actual_ret }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% endif %}">
        {{ actual_ret_cell_monthly(r) }}
      </td>
      <td class="{% if r.pred_high | default(false) %}warn{% endif %}" style="vertical-align:top" data-sort-col="pred" data-sort-value="{% if r.pred_ret is not none %}{{ r.pred_ret }}{% endif %}">
        {% if r.pred_ret is not none %}{{ "%.2f"|format(r.pred_ret) }}{% else %}—{% endif %}
      </td>
      <td style="vertical-align:top">
        {% if r.pred_ret is not none and r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
        {% if r.cumulative_accuracy_from_hist | default(false) %}—{% else %}{{ "%.2f"|format(r.pred_ret * r.cumulative_accuracy_avg) }}{% endif %}
        {% else %}—{% endif %}
      </td>
      {{ compact_cumulative_td(r, meta) | safe }}
      <td class="pred-reason">
        <span class="gap-tip combo-tip">
          <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 이유, 예측·실제 차이, 상승 이유(참고)를 함께 보기">통합 보기</span>
          <div class="gap-tip-popup combo-tip-popup" role="tooltip">
            <div class="combo-tip-inner">
              <div class="combo-tip-col">
                <h4 class="combo-tip-h">예측 이유</h4>
                <div class="combo-tip-body">{{ r.pred_reason_detail_html | default('') | safe }}</div>
              </div>
              <div class="combo-tip-col">
                <h4 class="combo-tip-h">예측·실제 차이</h4>
                <div class="combo-tip-body">
                  {% if r.gap_analysis_html %}
                  {{ r.gap_analysis_html | safe }}
                  {% else %}
                  <p class="combo-tip-empty">해당 설명이 없습니다.</p>
                  {% endif %}
                </div>
              </div>
              <div class="combo-tip-rise">
                <h4 class="combo-tip-h">상승 이유 (참고)</h4>
                <div class="combo-tip-body">{{ r.rise_reason_html | default('') | safe }}</div>
              </div>
            </div>
          </div>
        </span>
        <span class="combo-tip" style="margin-left:10px">
          <a class="gap-tip-trigger" target="_blank" rel="noopener" href="{{ naver_disclosure_url(r.code) }}">공시</a>
        </span>
        <span class="pred-reason-plain" style="margin-left:10px">{{ r.pred_reason_hit_line | default(r.pred_reason_summary) | default('—') }}</span>
      </td>
      <td>
        <span class="kw-pills">
          {% for k in r.keywords[:8] %}<span class="pill">{{ k }}</span>{% endfor %}
        </span>
      </td>
    </tr>
    {% endfor %}
  </tbody>
</table>
{% else %}
<p class="sub">해당일 실제·예측 {{ meta.threshold }} 이상 해당 종목 없음{% if empty_extra %} ({{ empty_extra }}){% endif %}.</p>
{% endif %}
{%- endmacro %}

  <h1>{{ title }}</h1>
  <p class="sub">
    {% if meta.run_subtitle %}{{ meta.run_subtitle }} · {% endif %}
    급등 기준 {{ meta.threshold }} · 뉴스: {{ meta.news_source }}
    {% if meta.use_decision_cutoff %} · 예측 입력 뉴스 N-1일 {{ meta.cutoff_kst }}(KST)까지{% endif %}
  </p>

  {% if meta.movers_data_note %}
  <p class="movers-data-note">{{ meta.movers_data_note }}</p>
  {% endif %}

  {% if week_note %}
  <p class="sub" style="margin-top:-6px">{{ week_note }}</p>
  {% endif %}

  {% if week_tabs_stack_days and week_panels %}
  <section class="tabs-wrap">
    <h2>주간별 (탭 · ISO 주 월요일 기준)</h2>
    <p class="sub" style="margin-top:0">각 탭은 한 주(월~금)를 <strong>월요일 날짜</strong>로 묶었습니다. 탭 안에서는 해당 주의 거래일을 <strong>일자 순</strong>으로 위에서 아래에 표시합니다. 앵커: <code>#day-YYYY-MM-DD</code></p>
    <div class="tab-bar" role="tablist">
      {% for w in week_panels %}
      <button type="button" class="tab-btn{% if loop.first %} active{% endif %}" role="tab"
              aria-selected="{{ 'true' if loop.first else 'false' }}" data-tab-idx="{{ loop.index0 }}">{{ w.label }}</button>
      {% endfor %}
    </div>
    {% for w in week_panels %}
    <div class="tab-panel{% if loop.first %} active{% endif %}" role="tabpanel" data-tab-panel="{{ loop.index0 }}">
      {% for d in w.days %}
      <section class="day-stack day-market-block" id="day-{{ d.trading_day.isoformat() }}">
        <div class="day-heading-row">
          <h2>{{ d.trading_day.isoformat() }}</h2>
          {{ market_filter_radios(d.trading_day.isoformat() ~ "-" ~ w.monday.isoformat()) }}
        </div>
        {{ compact_day_table(d) }}
      </section>
      {% endfor %}
    </div>
    {% endfor %}
  </section>
  <script>
  (function () {
    var bar = document.querySelector(".tabs-wrap .tab-bar");
    if (!bar) return;
    var wrap = bar.closest(".tabs-wrap");
    var btns = bar.querySelectorAll(".tab-btn");
    var panels = wrap.querySelectorAll(".tab-panel");
    function show(i) {
      btns.forEach(function (b, j) {
        b.classList.toggle("active", j === i);
        b.setAttribute("aria-selected", j === i ? "true" : "false");
      });
      panels.forEach(function (p, j) { p.classList.toggle("active", j === i); });
    }
    btns.forEach(function (b, i) { b.addEventListener("click", function () { show(i); }); });
  })();
  </script>
  {% elif stack_days %}
  <p class="sub" style="margin-top:0">거래일을 <strong>일자 순</strong>으로 위에서 아래에 이어 붙였습니다. 앵커: <code>#day-YYYY-MM-DD</code></p>
  {% for d in days %}
  <section class="day-stack day-market-block" id="day-{{ d.trading_day.isoformat() }}">
    <div class="day-heading-row">
      <h2>{{ d.trading_day.isoformat() }}</h2>
      {{ market_filter_radios(d.trading_day.isoformat()) }}
    </div>
    {{ compact_day_table(d) }}
  </section>
  {% endfor %}
  {% elif days|length > 1 %}
  <section class="tabs-wrap">
    <h2>거래일별 (탭)</h2>
    <p class="sub" style="margin-top:0">각 탭: <strong>실제</strong> {{ meta.threshold }} 이상 급등 종목 + 모델 <strong>예측</strong> {{ meta.threshold }} 이상 후보(상위 예측·중복 제외).</p>
    <div class="tab-bar" role="tablist">
      {% for d in days %}
      <button type="button" class="tab-btn{% if loop.first %} active{% endif %}" role="tab"
              aria-selected="{{ 'true' if loop.first else 'false' }}" data-tab-idx="{{ loop.index0 }}">{{ d.trading_day.isoformat() }}</button>
      {% endfor %}
    </div>
    {% for d in days %}
    <div class="tab-panel{% if loop.first %} active{% endif %}" role="tabpanel" data-tab-panel="{{ loop.index0 }}">
      <div class="day-market-block">
        <div class="day-heading-row">
          <h2>{{ d.trading_day.isoformat() }}</h2>
          {{ market_filter_radios(d.trading_day.isoformat() ~ "-daytab-" ~ loop.index0|string) }}
        </div>
        {{ compact_day_table(d) }}
      </div>
    </div>
    {% endfor %}
  </section>
  <script>
  (function () {
    var bar = document.querySelector(".tabs-wrap .tab-bar");
    if (!bar) return;
    var wrap = bar.closest(".tabs-wrap");
    var btns = bar.querySelectorAll(".tab-btn");
    var panels = wrap.querySelectorAll(".tab-panel");
    function show(i) {
      btns.forEach(function (b, j) {
        b.classList.toggle("active", j === i);
        b.setAttribute("aria-selected", j === i ? "true" : "false");
      });
      panels.forEach(function (p, j) { p.classList.toggle("active", j === i); });
    }
    btns.forEach(function (b, i) { b.addEventListener("click", function () { show(i); }); });
  })();
  </script>
  {% else %}
  {% for d in days %}
  <section class="day-market-block">
    <div class="day-heading-row">
      <h2>{{ d.trading_day.isoformat() }} · 실제·예측 {{ meta.threshold }} 이상</h2>
      {{ market_filter_radios(d.trading_day.isoformat() ~ "-single") }}
    </div>
    {{ compact_day_table(d, '장 전 실행 시 데이터 없음') }}
  </section>
  {% endfor %}
  {% endif %}

  <p class="note">
    실제≥{{ meta.threshold }}·예측≥{{ meta.threshold }} 뱃지로 행 구분. 종목명 클릭 시 네이버 차트. 예측만 해당인 종목은 상위 예측(top_n) 중 예측 상승률 기준이며, 실제 급등과 겹치면 한 행으로 합칩니다.
  </p>
{{ interaction_snippet | safe }}
</body>
</html>
"""

_INDEX_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root { --bg: #0f1419; --card: #1a2332; --text: #e7ecf3; --muted: #8b9cb3; --accent: #3d9cf5; }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.55; }
    h1 { font-size: 1.5rem; }
    .sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 1.25rem; }
    ul { list-style: none; padding: 0; margin: 0; }
    li { margin-bottom: 10px; }
    a { color: var(--accent); text-decoration: none; font-weight: 600; }
    a:hover { text-decoration: underline; }
    section { background: var(--card); border-radius: 12px; padding: 20px; border: 1px solid #243044; }
  </style>
</head>
<body>
  <h1>{{ title }}</h1>
  <p class="sub">월별 파일을 열면 <strong>ISO 주(월요일 기준)</strong> 단위 탭으로 구분되고, 탭 안에서는 거래일이 일자 순으로 나열됩니다.</p>
  <section>
    <ul>
      {% for href, label in week_links %}
      <li><a href="{{ href }}">{{ label }}</a></li>
      {% endfor %}
    </ul>
  </section>
</body>
</html>
"""

_DATED_N_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>N={{ n_day.isoformat() }} · T={{ t_day.isoformat() }} 리포트</title>
  <style>
    :root {
      --bg: #0c1017; --card: #151d2a; --text: #e8edf5; --muted: #8b9cb3;
      --accent: #4da3ff; --ok: #3ecf8e; --bad: #f07178; --warn: #e6c07b;
      --banner: #1a2740;
    }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.55; }
    h1 { font-size: 1.5rem; margin-bottom: 0.4rem; }
    h2 { font-size: 1.12rem; color: var(--accent); margin: 0 0 12px 0; }
    h3 { font-size: 1.05rem; margin: 0 0 8px 0; }
    h4 { font-size: 0.92rem; color: #a8c7ef; margin: 0 0 6px 0; }
    .sub, .hint, .muted { color: var(--muted); font-size: 0.86rem; }
    .hint { margin: 0 0 8px 0; line-height: 1.45; }
    .muted { font-style: italic; }
    section { background: var(--card); border-radius: 14px; padding: 20px 22px; margin-bottom: 20px;
              border: 1px solid #243044; }
    .banner { background: var(--banner); border-left: 4px solid var(--accent); padding: 14px 16px;
              border-radius: 10px; margin-bottom: 18px; font-size: 0.92rem; line-height: 1.55; }
    .banner.hist { border-left-color: var(--ok); }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 8px; font-size: 0.72rem;
            margin: 2px 4px 2px 0; background: #243044; color: var(--muted); }
    .table-wrap { overflow-x: auto; margin-top: 10px; -webkit-overflow-scrolling: touch; }
    table.rows-compare { width: 100%; border-collapse: collapse; font-size: 0.86rem; min-width: 720px; }
    table.rows-compare th, table.rows-compare td {
      padding: 10px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top;
    }
    table.rows-compare th { color: var(--muted); font-weight: 600; white-space: nowrap; }
    table.rows-compare tr:hover td { background: #1a2434; }
    table.rows-compare td.td-center { text-align: center; vertical-align: middle; }
    table.rows-compare td.num { font-variant-numeric: tabular-nums; white-space: nowrap; }
    table.rows-compare td.ok { color: var(--ok); font-weight: 600; }
    table.rows-compare td.bad { color: var(--bad); }
    table.rows-compare td.warn { color: var(--warn); font-weight: 600; }
    ul.nl { margin: 0; padding-left: 18px; font-size: 0.84rem; }
    ul.nl li { margin-bottom: 8px; }
    a { color: var(--accent); text-decoration: none; font-weight: 600; }
    a:hover { text-decoration: underline; }
    .reasons { margin-top: 12px; padding-top: 12px; border-top: 1px solid #2a3548; font-size: 0.84rem;
               color: var(--muted); line-height: 1.5; }
    .fn-block { margin-top: 12px; padding: 12px; background: #131b28; border-radius: 8px;
                border-left: 3px solid var(--bad); font-size: 0.86rem; }
    .gap-tip { position: relative; display: inline-block; vertical-align: middle; }
    .gap-tip.gap-tip-inline { margin-left: 6px; }
    .gap-tip.gap-tip-end .gap-tip-popup { left: auto; right: 0; }
    .gap-tip-trigger {
      cursor: help; border-bottom: 1px dotted var(--accent); color: var(--accent);
      font-size: 0.85rem; font-weight: 600; outline: none; margin-left: 0;
    }
    .gap-tip-trigger:hover, .gap-tip-trigger:focus { color: #7ec4ff; }
    .gap-tip-popup {
      display: none; position: absolute; z-index: 500; left: 0; top: calc(100% + 8px);
      min-width: 380px; max-width: min(920px, 96vw); max-height: 92vh;
      overflow: auto; padding: 14px 16px; background: #1a2838; border: 1px solid #3d6a9e;
      border-radius: 10px; box-shadow: 0 10px 28px rgba(0,0,0,0.5);
      font-size: 0.86rem; line-height: 1.55; color: #d0dce8; text-align: left;
    }
    .gap-tip-popup p { margin: 0 0 8px 0; }
    .gap-tip-popup ul { margin: 6px 0 0 16px; padding: 0; }
    .gap-tip-popup li { margin-bottom: 6px; }
    .gap-tip:hover .gap-tip-popup, .gap-tip:focus-within .gap-tip-popup { display: block; }
    .pred-reason-plain { font-size: 0.86rem; color: var(--muted); line-height: 1.5; display: inline-block; max-width: 100%; vertical-align: middle; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
    .combo-tip { margin-left: 0; vertical-align: middle; white-space: nowrap; }
    .combo-tip-popup {
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      left: 0 !important;
      right: auto !important;
      transform: none !important;
      top: calc(100% + 8px) !important;
      box-sizing: border-box;
      max-height: 90vh;
      overflow: auto;
      padding: 14px 16px !important;
    }
    .integrate-tip-popup { text-align: center; }
    .integrate-tip-popup .combo-tip-inner { text-align: left; margin: 0 auto; }
    .integrate-tip-popup .combo-tip-h { text-align: center; }
    .combo-tip-inner {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0 20px;
      align-items: start;
      width: 100%;
    }
    .combo-tip-col { min-width: 0; border-left: 1px solid #2d4a6a; padding-left: 16px; }
    .combo-tip-col:first-child { border-left: none; padding-left: 0; padding-right: 4px; }
    .combo-tip-h {
      margin: 0 0 8px 0;
      font-size: 0.8rem;
      font-weight: 700;
      color: #8ec5f6;
    }
    .combo-tip-body {
      font-size: 0.84rem; line-height: 1.6; color: #d0dce8;
      overflow-wrap: anywhere; word-wrap: break-word; word-break: break-word;
      white-space: normal;
    }
    .combo-tip-body p { margin: 0 0 8px 0; }
    .combo-tip-empty { margin: 0; color: var(--muted); font-style: italic; font-size: 0.84rem; }
    .combo-tip-rise {
      grid-column: 1 / -1;
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid #2d4a6a;
      width: 100%;
    }
    .integrate-tip-popup .combo-tip-rise .combo-tip-h { text-align: left; }
    @media (max-width: 760px) {
      .combo-tip-inner { grid-template-columns: 1fr; }
      .combo-tip-col { border-left: none; padding-left: 0; border-top: 1px solid #2d4a6a; padding-top: 12px; margin-top: 12px; }
      .combo-tip-col:first-child { border-top: none; padding-top: 0; margin-top: 0; }
    }
    .pred-reason-cell { max-width: 22em; font-size: 0.84rem; color: var(--muted); line-height: 1.45;
                        overflow-wrap: anywhere; word-break: break-word; }
    .kw-pills { display: inline-flex; flex-wrap: wrap; gap: 4px 6px; align-items: center; vertical-align: middle; }
    .news-tip-hint { font-size: 0.76rem; color: var(--muted); line-height: 1.4; margin: 0 0 8px 0; font-weight: 500; }
    .movers-data-note { background: #2a1f18; border: 1px solid #8b5a2b; border-radius: 10px;
                        padding: 12px 14px; margin-bottom: 16px; font-size: 0.86rem; line-height: 1.55;
                        color: #e8c9a8; }
    .day-heading-row { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-start; gap: 8px 14px; margin-bottom: 8px; }
    .day-heading-row h2 { margin: 0; font-size: 1.12rem; }
    .market-filter-radios { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; font-size: 0.82rem; color: var(--muted); }
    .market-filter-title { font-weight: 600; color: var(--muted); margin-right: 2px; }
    .market-filter-label { cursor: pointer; display: inline-flex; align-items: center; gap: 5px; margin: 0; font-weight: 500; }
    .market-filter-label input { accent-color: var(--accent); vertical-align: middle; }
    table.rows-compare th.sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th.sortable-col:hover { text-decoration: underline; }
    table.rows-compare th.sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th.sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    .gap-tip.cumulative-hist-tip { margin-top: 0; vertical-align: middle; }
    .gap-tip.cumulative-hist-tip .gap-tip-trigger {
      font-size: inherit;
      font-variant-numeric: tabular-nums;
    }
    .gap-tip.cumulative-hist-tip .gap-tip-popup.cumulative-hist-popup {
      min-width: 280px;
      max-width: min(440px, 94vw);
      z-index: 600;
    }
    .stock-chart-tip { position: relative; display: inline-block; vertical-align: baseline; max-width: 100%; }
    .stock-chart-tip .stock { position: relative; z-index: 1; }
    .stock-chart-popup {
      display: none; position: absolute; left: 0; top: calc(100% + 6px); z-index: 850;
      padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e; border-radius: 10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.55);
      min-width: min(720px, calc(100vw - 32px)); max-width: min(720px, calc(100vw - 24px));
      box-sizing: border-box;
    }
    .stock-chart-tip:hover .stock-chart-popup,
    .stock-chart-tip:focus-within .stock-chart-popup { display: block; }
    .stock-chart-img {
      display: block; width: 100%; max-width: 700px; height: auto;
      background: #0f1419; border-radius: 6px;
    }
    .stock-chart-caption { display: block; margin-top: 8px; font-size: 0.74rem; color: var(--muted); text-align: center; line-height: 1.4; }
  </style>
</head>
<body>
{% macro market_filter_radios(suffix) -%}
<div class="market-filter-radios" role="radiogroup" aria-label="표 시장 구분">
  <span class="market-filter-title">시장 -</span>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="all" checked="checked"/> 전체</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kospi"/> KOSPI</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kosdaq"/> KOSDAQ</label>
</div>
{%- endmacro %}
{% macro stock_name_link(code, name) -%}
<span class="stock-chart-tip" tabindex="0">
  <a class="stock" target="_blank" rel="noopener" href="{{ naver_chart_url(code) }}" title="클릭: 네이버 차트 · 호버: 일봉 캔들">{{ name }}</a>
  <span class="stock-chart-popup" role="tooltip">
    <img class="stock-chart-img" src="{{ naver_chart_day_img_url(code) }}" alt="{{ name }} 일봉 캔들 차트" width="700" height="289" loading="lazy" decoding="async" referrerpolicy="no-referrer-when-downgrade"/>
    <span class="stock-chart-caption">일봉 캔들 · 가로축은 거래일(네이버 금융 이미지)</span>
  </span>
</span>
{%- endmacro %}
{% macro actual_ret_cell_dated(r) -%}
{% if r.actual_cell_pre_close_snapshot | default(false) %}{% if r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ "%.2f"|format(r.actual_ret_intraday_pct) }}%){% elif r.actual_ret is not none %}— ({{ "%.2f"|format(r.actual_ret * 100) }}%){% else %}—{% endif %}{% elif r.actual_ret is not none %}{{ "%.2f"|format(r.actual_ret * 100) }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ "%.2f"|format(r.actual_ret_intraday_pct) }}%){% else %}—{% endif %}
{%- endmacro %}
  <h1>기준일 N={{ n_day.isoformat() }} → 관측일 T={{ t_day.isoformat() }}</h1>
  <p class="sub">
    {{ meta.run_subtitle }} · 급등 기준 {{ meta.threshold }} · 뉴스: {{ meta.news_source }}
    {% if meta.use_decision_cutoff %} · 예측 입력: T 직전 KRX 거래일 {{ meta.cutoff_kst }}(KST)까지{% endif %}
    · 예측 후보 수 {{ meta.total_preds }}
  </p>

  {% if meta.movers_data_note %}
  <p class="movers-data-note">{{ meta.movers_data_note }}</p>
  {% endif %}

  {% if meta.prediction_only and is_live_n and before_open_n %}
  <div class="banner">
    <strong>당일(N) · 장 시작 전 실행.</strong> 예측 입력 뉴스는 <strong>전 거래일까지</strong> 반영합니다(당일 장중 뉴스는 포함하지 않음).
    <strong>T={{ t_day.isoformat() }}</strong> 관측일 전이거나 장이 끝나기 전이면 <strong>실제 상승률·누적 정확도</strong>는 표에서 <strong>빈 칸</strong>으로 둡니다. 아래는 <strong>예측 상승률 {{ meta.threshold }} 이상 후보</strong> 위주입니다.
  </div>
  {% elif meta.prediction_only and is_live_n %}
  <div class="banner">
    <strong>당일(N) 실행 모드.</strong> 예측에는 위 시각까지 반영된 뉴스가 쓰였습니다.
    <strong>T={{ t_day.isoformat() }}</strong> 가 예측 전용이거나 일봉이 아직 확정되지 않았으면 <strong>실제 상승률</strong>은 빈 칸이거나, 당일 장 마감 전에는 pykrx·네이버 실시간 등락률을 <strong>— (xx%)</strong> 형태로만 참고합니다. <strong>누적 정확도</strong>는 실적이 없으면 빈 칸일 수 있습니다. <strong>예측 상승률 {{ meta.threshold }} 이상 후보만</strong> 표에 올립니다(당일 시장 전체 급등 종목은 포함하지 않음).
    과거 기준일로 다시 실행하면 시장 20%↑ 종목과 예측을 함께 비교할 수 있습니다.
  </div>
  {% elif meta.prediction_only %}
  <div class="banner">
    <strong>예측 전용 리포트.</strong> 비거래일·미래 N·당일 실행 등으로 <strong>실제 상승률·누적 정확도</strong>를 알 수 없으면 표에서 <strong>빈 칸</strong>으로 둡니다.
  </div>
  {% else %}
  <div class="banner hist">
    <strong>과거 N일 기준 리포트.</strong> T일 종가 기준 실제 상승률과 모델 예측을 함께 표시합니다.
    예측 근거는 <em>예측에 사용한 뉴스 구간</em>에서 종목명·키워드가 들어간 기사만 골랐고,
    오른쪽은 <em>컷오프 이후·T일</em> 보도를 참고용으로 묶었습니다(인과 단정 아님).
  </div>
  {% endif %}

  <section class="day-market-block">
    <div class="day-heading-row">
      <h2>종목별 상세 <span style="font-size:0.82rem;font-weight:500;color:var(--muted)">(관측일 {{ t_day.isoformat() }})</span></h2>
      {{ market_filter_radios(n_day.strftime("%Y%m%d")) }}
    </div>
    <p class="sub" style="margin-top:0">한 줄이 한 종목입니다. <strong>통합 보기</strong>·<strong>공시</strong>·<strong>이유/차이</strong>·<strong>뉴스</strong> 순으로 보시면 됩니다. 키워드는 과거 20%↑ 사례와의 문자열 일치입니다.</p>
    {% if day.rows_compare|length > 0 %}
    <div class="table-wrap">
    <table class="rows-compare">
      <thead>
        <tr>
          <th class="sortable-col" data-sort="group" scope="col" title="구분 우선순위 정렬: 실제+예측 > 실제만 > 예측만">구분</th>
          <th class="sortable-col" data-sort="stock" scope="col" title="종목명/코드 오름차순·내림차순 정렬">종목</th>
          <th class="sortable-col" data-sort="actual" scope="col" title="종가 확정 후 일봉 기준. 금일 장 마감 전(15:30 KST 전)에는 — 뒤 괄호에 pykrx·네이버 실시간 등락률(리포트 생성 시점)을 둡니다.">실제 상승률(%)<br/><span style="font-size:0.68rem;font-weight:500;color:var(--muted)">(장중·참고)</span></th>
          <th class="sortable-col" data-sort="pred" scope="col">예측 상승률(%)</th>
          <th>보정(%)</th>
          <th class="sortable-col" data-sort="cumulative" scope="col" title="예측≥임계 후보만. 앞: 관측일별 min(|실제%|,|예측%|)/max(|실제%|,|예측%|) 평균(정확히 일치할 때만 100%). 괄호: a=실제≥임계, b=0&lt;실제&lt;임계, c=실제&lt;0, d=예측≥임계·실적 확정 전체 (a b c / d)">누적 정확도<br/><span style="font-size:0.68rem;font-weight:500;color:var(--muted)">(달성%·a b c / d)</span></th>
          <th>통합 보기</th>
          <th>공시</th>
          <th>이유/차이</th>
          <th>뉴스</th>
          <th>일치 키워드</th>
        </tr>
      </thead>
      <tbody>
        {% for r in day.rows_compare %}
        <tr id="code-{{ row_id_prefix }}{{ r.code }}" data-market="{{ r.market_segment|default('other') }}">
          <td style="white-space:nowrap" data-sort-col="group" data-sort-value="{% if (not meta.prediction_only) and r.actual_big and (r.pred_high | default(false)) %}3{% elif (not meta.prediction_only) and r.actual_big %}2{% elif r.pred_high | default(false) %}1{% else %}0{% endif %}">
            {% if not meta.prediction_only and r.actual_big %}<span class="pill" style="background:#1e3d2f;color:var(--ok)">실제≥{{ meta.threshold }}</span>{% endif %}
            {% if r.pred_high | default(false) %}<span class="pill" style="margin-top:4px;display:inline-block;color:var(--warn)">예측≥{{ meta.threshold }}</span>{% endif %}
          </td>
          <td data-sort-col="stock" data-sort-value="{{ r.name }} {{ r.code }}">
            {{ stock_name_link(r.code, r.name) }}
            <div class="pill">{{ r.code }}</div>
          </td>
          <td class="num {% if not meta.prediction_only and r.actual_big %}ok{% elif r.actual_ret is not none and r.actual_ret < 0 %}bad{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none and r.actual_ret_intraday_pct < 0 %}bad{% endif %}" data-sort-col="actual" data-sort-value="{% if r.actual_cell_pre_close_snapshot | default(false) and r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% elif r.actual_ret is not none %}{{ r.actual_ret }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% endif %}">
            {{ actual_ret_cell_dated(r) }}
          </td>
          <td class="num {% if r.pred_high | default(false) %}warn{% endif %}" data-sort-col="pred" data-sort-value="{% if r.pred_ret is not none %}{{ r.pred_ret }}{% endif %}">
            {% if r.pred_ret is none %}—{% else %}{{ "%.2f"|format(r.pred_ret) }}{% endif %}
          </td>
          <td class="num">
            {% if r.pred_ret is not none and r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
            {% if r.cumulative_accuracy_from_hist | default(false) %}—{% else %}{{ "%.2f"|format(r.pred_ret * r.cumulative_accuracy_avg) }}{% endif %}
            {% else %}—{% endif %}
          </td>
          <td class="num" style="white-space:nowrap;font-variant-numeric:tabular-nums" data-sort-col="cumulative" data-sort-value="{% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}{{ r.cumulative_accuracy_avg }}{% endif %}">
            {% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
            <span class="gap-tip cumulative-hist-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="누적 정확도·{{ meta.threshold }} 이상 예측 이력"{% if meta.cumulative_track_hint is defined %} title="{{ meta.cumulative_track_hint | e }}"{% endif %}>{{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} : ({{ r.cumulative_hit_x }} {% if r.cumulative_hit_z is defined and r.cumulative_hit_z is not none %}{{ r.cumulative_hit_z }}{% else %}0{% endif %} <span class="bad">{% if r.cumulative_hit_neg is defined and r.cumulative_hit_neg is not none %}{{ r.cumulative_hit_neg }}{% else %}0{% endif %}</span> / {{ r.cumulative_hit_y }}){% endif %}</span>
              <div class="gap-tip-popup cumulative-hist-popup" role="tooltip">
                <div class="combo-tip-body">
                  <strong>관측일 T별 · 예측 ≥ {{ meta.threshold }}</strong>
                  {% if r.pred_high_history|default([])|length > 0 %}
                  <ul class="nl" style="margin:8px 0 0 0">
                  {% for h in r.pred_high_history %}
                    <li><span class="pill">{{ h.t }}</span> 예측 {{ "%.2f"|format(h.pred_pct) }}%
                      {% if h.actual_pct is not none %} · 실제 {{ "%.2f"|format(h.actual_pct) }}%{% else %} · 실적 미확정{% endif %}
                    </li>
                  {% endfor %}
                  </ul>
                  {% else %}
                  <p class="combo-tip-empty" style="margin:8px 0 0 0">저장된 {{ meta.threshold }} 이상 예측 이력이 없습니다.</p>
                  {% endif %}
                  <p style="font-size:0.76rem;color:var(--muted);margin:8px 0 0 0;line-height:1.35">앞 {{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%: 관측일별 min(|실제%|,|예측%|) / max(|실제%|,|예측%|) 평균(정확히 일치할 때만 100%).{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} 괄호 (a b c / d): 예측≥{{ meta.threshold }}·실적 확정 건 중 a=실제≥{{ meta.threshold }}, b=0&lt;실제&lt;{{ meta.threshold }}, c=실제&lt;0(빨간색), d=전체.{% endif %}</p>
                </div>
              </div>
            </span>
            {% endif %}
          </td>
          <td class="td-center">
            <span class="gap-tip combo-tip integrate-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 이유, 예측·실제 차이, 상승 이유(참고)를 함께 보기">통합 보기</span>
              <div class="gap-tip-popup combo-tip-popup integrate-tip-popup" role="tooltip">
                <div class="combo-tip-inner">
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측 이유</h4>
                    <div class="combo-tip-body">{{ r.pred_reason_detail_html | default('') | safe }}</div>
                  </div>
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">{% if meta.prediction_only %}참고{% else %}예측·실제 차이{% endif %}</h4>
                    <div class="combo-tip-body">
                      {% if r.gap_analysis_html %}
                      {{ r.gap_analysis_html | safe }}
                      {% else %}
                      <p class="combo-tip-empty">해당 설명이 없습니다.</p>
                      {% endif %}
                    </div>
                  </div>
                  <div class="combo-tip-rise">
                    <h4 class="combo-tip-h">상승 이유 (참고)</h4>
                    <div class="combo-tip-body">{{ r.rise_reason_html | default('') | safe }}</div>
                  </div>
                </div>
              </div>
            </span>
          </td>
          <td class="td-center">
            <a class="gap-tip-trigger" target="_blank" rel="noopener" href="{{ naver_disclosure_url(r.code) }}">공시</a>
          </td>
          <td class="pred-reason-cell">{{ r.pred_reason_hit_line | default(r.pred_reason_summary) | default('—') }}</td>
          <td class="td-center">
            <span class="gap-tip combo-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 입력 구간 뉴스와 참고 뉴스를 함께 보기">뉴스</span>
              <div class="gap-tip-popup combo-tip-popup" role="tooltip">
                <div class="combo-tip-inner">
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측 상승률이 높게 나온 데 기여한 뉴스(예측 입력 구간)</h4>
                    <p class="news-tip-hint">종목명 또는 예측 시 맞춘 키워드가 제목·요약에 포함된 기사입니다.</p>
                    <ul class="nl">
                    {% for h in r.pred_news_hits|default([]) %}
                      <li><span class="pill">{{ h.day.isoformat() }}</span> <code style="font-size:0.75rem;color:var(--warn)">{{ h.matched }}</code>
                        {% if h.link %}
                        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
                        {% else %}
                        {{ h.title }}
                        {% endif %}
                      </li>
                    {% else %}
                      <li class="muted">매칭된 기사 없음(키워드·종목명이 뉴스 제목·요약에 직접 나타난 경우만 표시).</li>
                    {% endfor %}
                    </ul>
                  </div>
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">실제 등락과 시기가 겹치는 뉴스(참고)</h4>
                    <p class="news-tip-hint">예측 컷오프 이후 구간 + T({{ t_day.isoformat() }}) 당일 기사 중 같은 방식으로 매칭한 목록입니다. 상승 원인으로 단정하지 않습니다.</p>
                    <ul class="nl">
                    {% for h in r.actual_news_hits|default([]) %}
                      <li><span class="pill">{{ h.day.isoformat() }}</span> <code style="font-size:0.75rem;color:var(--ok)">{{ h.matched }}</code>
                        {% if h.link %}
                        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
                        {% else %}
                        {{ h.title }}
                        {% endif %}
                      </li>
                    {% else %}
                      <li class="muted">매칭된 기사 없음.</li>
                    {% endfor %}
                    </ul>
                    <h4 class="combo-tip-h" style="margin-top:12px">네이버 종목 공시(당일)</h4>
                    <ul class="nl">
                    {% for h in r.disclosure_hits|default([]) %}
                      <li>
                        <code style="font-size:0.75rem;color:#9fd3ff">{{ h.kind }}</code>
                        {% if h.link %}
                        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
                        {% else %}
                        {{ h.title }}
                        {% endif %}
                      </li>
                    {% else %}
                      <li class="muted">당일 공시 매칭 없음.</li>
                    {% endfor %}
                    </ul>
                  </div>
                </div>
              </div>
            </span>
          </td>
          <td>
            <span class="kw-pills">
              {% for k in r.keywords[:16] %}<span class="pill">{{ k }}</span>{% else %}<span class="muted">—</span>{% endfor %}
            </span>
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    </div>
    {% else %}
    <p class="sub">실제·예측 {{ meta.threshold }} 이상으로 표에 올린 종목이 없습니다.</p>
    {% endif %}
  </section>

  {% if (not meta.prediction_only) and day.false_negatives and day.false_negatives|length > 0 %}
  <section>
    <h2>예측했으나 실제 음수 수익</h2>
    {% for fn in day.false_negatives %}
    <div class="fn-block">
      <strong>{{ stock_name_link(fn.code, fn.name) }}</strong>
      ({{ fn.code }}) · 예측 {{ "%.2f"|format(fn.pred_ret) }}% · 실제
      <span class="bad">{{ "%.2f"|format(fn.actual_ret * 100) }}%</span>
      <p style="margin:8px 0 0 0;color:var(--muted);">{{ fn.analysis }}</p>
    </div>
    {% endfor %}
  </section>
  {% endif %}

  <section>
    <h2>예측 입력 뉴스 제목 샘플</h2>
    <ul class="nl">
      {% for t in day.news_titles_sample[:16] %}
      <li>{{ t }}</li>
      {% else %}
      <li class="muted">없음</li>
      {% endfor %}
    </ul>
  </section>
</body>
</html>
"""

_m_dated_style = re.search(r"<style>\s*(.*?)\s*</style>", _DATED_N_TEMPLATE, re.DOTALL)
_DATED_N_CSS_INNER = _m_dated_style.group(1) if _m_dated_style else ""


def _dated_n_block_markers(n_compact: str) -> tuple[str, str]:
    return (
        f"<!-- MONEY_DATED_N_BEGIN:{n_compact} -->",
        f"<!-- MONEY_DATED_N_END:{n_compact} -->",
    )


def _dated_n_block_pattern(n_compact: str) -> re.Pattern[str]:
    beg, end = _dated_n_block_markers(n_compact)
    return re.compile(
        re.escape(beg) + r".*?" + re.escape(end) + r"\s*",
        re.DOTALL,
    )


def _strip_html_body_inner(full_html: str) -> str:
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
    return (
        re.search(r'<main\s+id="money-dated-rollup"\s*>', html, re.IGNORECASE)
        is not None
    )


def _insert_into_money_dated_main(html: str, wrapped_block: str) -> str:
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
    )
    body_inner = _strip_html_body_inner(html)
    merge_dated_n_rollup(rollup_path=out_rollup, n_compact=n_compact, body_inner=body_inner)
