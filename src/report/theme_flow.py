# -*- coding: utf-8 -*-
"""월간 테마 리포트(상한) → 테마 흐름·이슈 연관 HTML.

``output/essential/report_theme_20pct_*.html`` / ``report_theme_25pct_*.html`` 을 읽어
``output/report_theme_flow.html`` (및 기간 파일명)을 갱신한다.
테마 월간 리포트가 쓰인 직후(14:30·15:30 파이프라인)에 호출한다.
외부 스크립트 없이 SVG·소량 JS 만 쓰는 단일 파일이다.
"""
from __future__ import annotations

import html as html_mod
import math
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from src import config, trading_calendar
from src.report import theme_issues as ti

_THEME_FILE_RE = re.compile(
    r"^report_theme_(?:20pct|25pct)_(\d{4})\.(\d{2})\.html$",
    re.IGNORECASE,
)

BUCKET: dict[str, str] = {
    "반도체": "반도체·전자",
    "전자장비와기기": "반도체·전자",
    "디스플레이장비및부품": "반도체·전자",
    "디스플레이": "반도체·전자",
    "핸드셋": "반도체·전자",
    "전기제품": "반도체·전자",
    "컴퓨터와주변기기": "반도체·전자",
    "통신": "반도체·전자",
    "IT서비스": "IT·SW",
    "소프트웨어": "IT·SW",
    "AI·데이터": "IT·SW",
    "게임": "게임·엔터",
    "게임·엔터": "게임·엔터",
    "방송과엔터테인먼트": "게임·엔터",
    "바이오·제약": "바이오·헬스",
    "제약": "바이오·헬스",
    "생물공학": "바이오·헬스",
    "생명과학도구및서비스": "바이오·헬스",
    "건강관리장비와용품": "바이오·헬스",
    "건강관리업체및서비스": "바이오·헬스",
    "건설": "건설·부동산",
    "건설·부동산": "건설·부동산",
    "건축자재": "건설·부동산",
    "건축제품": "건설·부동산",
    "부동산": "건설·부동산",
    "자동차부품": "자동차·모빌리티",
    "자동차·부품": "자동차·모빌리티",
    "자동차": "자동차·모빌리티",
    "전기차·모빌리티": "자동차·모빌리티",
    "섬유": "섬유·소비재",
    "화장품": "섬유·소비재",
    "화장품·뷰티": "섬유·소비재",
    "식품": "섬유·소비재",
    "농업·식품": "섬유·소비재",
    "로봇·자동화": "로봇·기계",
    "기계": "로봇·기계",
    "에너지": "에너지·전력",
    "신재생·에너지": "에너지·전력",
    "전기장비": "에너지·전력",
    "원전·SMR": "에너지·전력",
    "철강": "소재·산업",
    "화학": "소재·산업",
    "화학·소재": "소재·산업",
    "비철금속": "소재·산업",
}

# 표에 없는 원 테마 이름은 부분 문자열로 묶는다(위에서부터 먼저 걸린 것).
_BUCKET_RULES: list[tuple[tuple[str, ...], str]] = [
    (("조선", "해운", "국방", "우주", "방산", "해양"), "조선·방산"),
    (("반도체", "디스플레이", "전자", "핸드셋", "컴퓨터"), "반도체·전자"),
    (("바이오", "제약", "생물", "생명", "건강관리"), "바이오·헬스"),
    (("건설", "건축", "부동산"), "건설·부동산"),
    (("자동차", "모빌리티"), "자동차·모빌리티"),
    (("에너지", "전력", "전기유틸", "원전", "석유", "가스"), "에너지·전력"),
    (("화학", "소재", "철강", "비철"), "소재·산업"),
    (("로봇", "기계"), "로봇·기계"),
    (("게임", "엔터", "방송", "미디어", "광고", "출판"), "게임·엔터"),
    (("소프트웨어", "IT", "AI", "데이터", "플랫폼", "통신"), "IT·SW"),
    (("금융", "은행", "증권", "창업투자", "보험"), "금융"),
    (("섬유", "패션", "화장품", "뷰티", "식품", "식료품", "가구", "가정용", "문구", "소비재"), "섬유·소비재"),
    (("레저", "여행", "항공", "호텔", "레스토랑", "백화점", "유통", "판매", "교통", "여객", "교육"), "레저·서비스"),
]

SHORT: dict[str, str] = {
    "건설·부동산": "건설",
    "반도체·전자": "반도체",
    "바이오·헬스": "바이오",
    "섬유·소비재": "소비재",
    "자동차·모빌리티": "자동차",
    "에너지·전력": "에너지",
    "소재·산업": "소재",
    "로봇·기계": "로봇",
    "게임·엔터": "엔터",
    "IT·SW": "IT",
    "조선·방산": "조선방산",
    "금융": "금융",
    "레저·서비스": "레저",
    "분산": "분산",
    "기타": "기타",
}

BLURB: dict[str, str] = {
    "건설·부동산": "수주·동반급등형 쏠림",
    "반도체·전자": "대형·중형 전자 동반",
    "바이오·헬스": "그룹·테마 동반 스윕",
    "섬유·소비재": "소비재 동반 이동",
    "IT·SW": "소프트웨어·IT 순환",
    "자동차·모빌리티": "부품주 동반",
    "에너지·전력": "에너지 테마 단발",
    "소재·산업": "소재·화학 단발",
    "로봇·기계": "로봇·기계 이벤트",
    "게임·엔터": "엔터·게임 산발",
    "조선·방산": "조선·방산 동반",
    "금융": "금융주 동반",
    "레저·서비스": "레저·유통 동반",
    "분산": "주도 없이 산발",
    "기타": "소테마 혼합",
}

THEME_COLOR: dict[str, str] = {
    "반도체·전자": "#3d9cf5",
    "바이오·헬스": "#3ecf8e",
    "건설·부동산": "#f0a84a",
    "섬유·소비재": "#e88bd0",
    "자동차·모빌리티": "#9b8cf5",
    "IT·SW": "#4fd1d9",
    "에너지·전력": "#f5d74a",
    "소재·산업": "#c98b5a",
    "로봇·기계": "#8fb3c9",
    "게임·엔터": "#f57a7a",
    "조선·방산": "#6f8ff0",
    "금융": "#b0c46a",
    "레저·서비스": "#d9a3f5",
    "분산": "#3a4a5c",
    "기타": "#5a6a80",
}


def _esc(s: object) -> str:
    return html_mod.escape(str(s))


def _short(theme: str) -> str:
    return SHORT.get(theme, theme)


def _color(theme: str) -> str:
    return THEME_COLOR.get(theme, "#5a6a80")


def bucket_for(raw_theme: str) -> str:
    """원 테마 이름 → 묶음 테마."""
    raw = (raw_theme or "").strip()
    if raw in BUCKET:
        return BUCKET[raw]
    for kws, bucket in _BUCKET_RULES:
        if any(k in raw for k in kws):
            return bucket
    return "기타"


def _span_label(e: dict[str, Any]) -> str:
    """에피소드 기간 ``MM-DD`` 또는 ``MM-DD~MM-DD`` (진행 중 에피소드도 끝 날짜가 보이게)."""
    a, b = e["from"][5:], e["to"][5:]
    return a if a == b else f"{a}~{b}"


def discover_theme_report_paths(output_dir: Path | None = None) -> list[Path]:
    """``essential`` 안 월별 테마 HTML. 같은 달이면 20pct 우선, 없으면 25pct."""
    out = config.output_essential_dir(output_dir)
    by_ym: dict[tuple[int, int], dict[str, Path]] = defaultdict(dict)
    for p in out.glob("report_theme_*pct_*.html"):
        m = _THEME_FILE_RE.match(p.name)
        if not m:
            continue
        y, mo = int(m.group(1)), int(m.group(2))
        kind = "20" if "20pct" in p.name.lower() else "25"
        by_ym[(y, mo)][kind] = p
    paths: list[Path] = []
    for ym in sorted(by_ym):
        d = by_ym[ym]
        paths.append(d.get("20") or d["25"])
    return paths


def _parse_limit_up_days(paths: list[Path]) -> list[dict[str, Any]]:
    """날짜별 상한 종목(이름·코드·원 테마·묶음 테마·근거)과 테마 집계."""
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in paths:
        soup = BeautifulSoup(p.read_text(encoding="utf-8"), "html.parser")
        for sec in soup.select("section.day[id^=day-]"):
            day = (sec.get("id") or "").replace("day-", "")
            if not day:
                continue
            seen: set[str] = set()
            for tr in sec.select("table.stocks tbody tr"):
                chg = tr.select_one(".col-chg") or tr.select_one("td.ok")
                if not chg or "상한" not in chg.get_text():
                    continue
                theme_el = tr.select_one(".col-theme") or tr.select_one("td.theme")
                raw = theme_el.get_text(strip=True) if theme_el else ""
                stock_el = tr.select_one(".col-stock")
                name_el = stock_el.select_one("strong") if stock_el else None
                code_el = stock_el.select_one("code") if stock_el else None
                name = name_el.get_text(strip=True) if name_el else ""
                code = code_el.get_text(strip=True) if code_el else ""
                key = code or name
                if key and key in seen:
                    continue
                seen.add(key)
                reason_el = tr.select_one(".col-reason")
                by_day[day].append(
                    {
                        "name": name,
                        "code": code,
                        "raw": raw,
                        "bucket": bucket_for(raw),
                        "reason": reason_el.get_text(" ", strip=True) if reason_el else "",
                    }
                )
    day_rows: list[dict[str, Any]] = []
    for d in sorted(by_day):
        stocks = by_day[d]
        c = Counter(s["bucket"] for s in stocks)
        top, tn = c.most_common(1)[0]
        total = len(stocks)
        day_rows.append(
            {
                "d": d,
                "n": total,
                "top": top,
                "topN": tn,
                "share": round(tn / total, 2),
                "by_theme": dict(c),
                "stocks": stocks,
            }
        )
    return day_rows


def _build_segments(day_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    days = [r["d"] for r in day_rows]
    segs: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None
    for i, r in enumerate(day_rows):
        label = r["top"] if (r["topN"] >= 2 and r["share"] >= 0.25) else "분산"
        if cur and cur["theme"] == label and i > 0 and days[i - 1] == cur["to"]:
            cur["to"] = r["d"]
            cur["days"] += 1
            cur["hits"] += r["n"]
            if r["n"] > cur["peak"]:
                cur["peak"] = r["n"]
                cur["peakDay"] = r["d"]
            cur["avgN"] = round(cur["hits"] / cur["days"], 1)
        else:
            if cur:
                segs.append(cur)
            cur = {
                "theme": label,
                "from": r["d"],
                "to": r["d"],
                "days": 1,
                "hits": r["n"],
                "peak": r["n"],
                "peakDay": r["d"],
                "avgN": float(r["n"]),
            }
    if cur:
        segs.append(cur)
    return segs


def _build_episodes(segs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    eps: list[dict[str, Any]] = []
    buf: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal buf
        if not buf:
            return
        themes_c: Counter[str] = Counter()
        hits = 0
        peak = 0
        peak_day = buf[0]["peakDay"]
        dn = 0
        for s in buf:
            themes_c[s["theme"]] += s["hits"]
            hits += s["hits"]
            dn += s["days"]
            if s["peak"] > peak:
                peak = s["peak"]
                peak_day = s["peakDay"]
        top = themes_c.most_common(1)[0][0]
        title = top if len(themes_c) == 1 else f"{top} 중심 혼합"
        blurb = BLURB.get(top, "테마 전환")
        if "혼합" in title:
            blurb = "짧은 테마가 교차하는 구간"
        eps.append(
            {
                "id": f"e{len(eps) + 1:02d}",
                "from": buf[0]["from"],
                "to": buf[-1]["to"],
                "days": dn,
                "hits": hits,
                "peak": peak,
                "peakDay": peak_day,
                "theme": top,
                "title": title,
                "avgN": round(hits / dn, 1),
                "blurb": blurb,
                "mix": [{"b": b, "n": n} for b, n in themes_c.most_common(4)],
            }
        )
        buf = []

    for s in segs:
        strong = (
            s["days"] >= 3
            or s["peak"] >= 18
            or (
                s["days"] >= 2
                and s["hits"] >= 18
                and s["theme"] not in ("분산", "기타")
            )
        )
        if strong:
            flush()
            eps.append(
                {
                    "id": f"e{len(eps) + 1:02d}",
                    "from": s["from"],
                    "to": s["to"],
                    "days": s["days"],
                    "hits": s["hits"],
                    "peak": s["peak"],
                    "peakDay": s["peakDay"],
                    "theme": s["theme"],
                    "title": s["theme"],
                    "avgN": s["avgN"],
                    "blurb": BLURB.get(s["theme"], "테마 전환"),
                    "mix": [{"b": s["theme"], "n": s["hits"]}],
                }
            )
        else:
            buf.append(s)
            if (
                sum(x["days"] for x in buf) >= 3
                or sum(x["hits"] for x in buf) >= 22
                or len(buf) >= 3
            ):
                flush()
    flush()
    return eps


def _stocks_in(day_rows: list[dict[str, Any]], d0: str, d1: str) -> list[dict[str, Any]]:
    return [s for r in day_rows if d0 <= r["d"] <= d1 for s in r.get("stocks", [])]


def _top_issues(stocks: list[dict[str, Any]], k: int = 3) -> list[tuple[str, int]]:
    c: Counter[str] = Counter()
    for s in stocks:
        for i in s.get("issues", []):
            if i != ti.UNKNOWN:
                c[i] += 1
    return c.most_common(k)


def _week_key(d: str) -> str:
    x = date.fromisoformat(d)
    return (x - timedelta(days=x.weekday())).isoformat()


def _themes_by_total(day_rows: list[dict[str, Any]]) -> list[str]:
    c: Counter[str] = Counter()
    for r in day_rows:
        c.update(r.get("by_theme", {}))
    return [t for t, _ in c.most_common()]


def _issue_tag_html(issues: list[str]) -> str:
    return "".join(
        f'<span class="tag" style="border-color:{ti.ISSUE_COLOR.get(i, "#5a6a80")};'
        f'color:{ti.ISSUE_COLOR.get(i, "#8b9cb3")}">{_esc(i)}</span>'
        for i in issues
    )


def _day_tip(r: dict[str, Any]) -> str:
    lines = [f"{r['d']} · 상한 {r['n']}종"]
    by = sorted(r.get("by_theme", {}).items(), key=lambda kv: -kv[1])
    lines.append(" · ".join(f"{_short(t)} {n}" for t, n in by))
    for s in r.get("stocks", [])[:12]:
        iss = ", ".join(i for i in s.get("issues", []) if i != ti.UNKNOWN) or "뉴스 미확인"
        lines.append(f"- {s['name']} ({_short(s['bucket'])}) [{iss}]")
    if len(r.get("stocks", [])) > 12:
        lines.append(f"… 외 {len(r['stocks']) - 12}종")
    return "\n".join(lines)


def _svg_stream(day_rows: list[dict[str, Any]], eps: list[dict[str, Any]], themes: list[str]) -> str:
    """일별 상한 종목 수를 테마별로 쌓은 막대 + 위쪽 에피소드 띠."""
    n = len(day_rows)
    step = 13 if n <= 90 else 10
    left, right, ribbon_h, top_gap, plot_h, axis_h = 34, 12, 22, 8, 230, 34
    width = left + n * step + right
    y0 = ribbon_h + top_gap
    base = y0 + plot_h
    height = base + axis_h
    max_n = max((r["n"] for r in day_rows), default=1) or 1
    scale = plot_h / max_n
    idx = {r["d"]: i for i, r in enumerate(day_rows)}

    def x_of(i: int) -> float:
        return left + i * step + step / 2

    out = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        'xmlns="http://www.w3.org/2000/svg">'
    ]
    for frac in (0.25, 0.5, 0.75, 1.0):
        v = max_n * frac
        y = base - v * scale
        out.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#243044"/>'
            f'<text x="{left - 4}" y="{y + 3:.1f}" text-anchor="end" font-size="9" fill="#8b9cb3">{v:.0f}</text>'
        )
    bar_w = step - 3
    for i, r in enumerate(day_rows):
        y = base
        for t in themes:
            v = r.get("by_theme", {}).get(t, 0)
            if not v:
                continue
            h = v * scale
            y -= h
            out.append(
                f'<rect x="{left + i * step + 1.5}" y="{y:.1f}" width="{bar_w}" height="{h:.1f}" '
                f'fill="{_color(t)}" stroke="#0f1419" stroke-width="0.5"/>'
            )
    for e in eps:
        i0, i1 = idx.get(e["from"]), idx.get(e["to"])
        if i0 is None or i1 is None:
            continue
        x = left + i0 * step
        w = (i1 - i0 + 1) * step - 1
        tip = f"{e['id']} {e['from']} ~ {e['to']} · {e['title']} · 일평균 {e['avgN']}종"
        out.append(
            f'<a href="#ep-{e["id"]}"><rect x="{x}" y="2" width="{w}" height="{ribbon_h - 4}" rx="3" '
            f'fill="{_color(e["theme"])}" data-tip="{_esc(tip)}"/>'
        )
        if w >= 30:
            out.append(
                f'<text x="{x + w / 2:.1f}" y="{ribbon_h / 2 + 4}" text-anchor="middle" font-size="10" '
                f'font-weight="700" fill="#0f1419" pointer-events="none">{_esc(_short(e["theme"]))}</text>'
            )
        out.append("</a>")
    every = max(1, math.ceil(n / 14))
    for i, r in enumerate(day_rows):
        if i % every == 0 or i == n - 1:
            out.append(
                f'<text x="{x_of(i):.1f}" y="{base + 14}" text-anchor="middle" font-size="9" '
                f'fill="#8b9cb3">{r["d"][5:]}</text>'
            )
        out.append(
            f'<rect class="hov" x="{left + i * step}" y="{y0}" width="{step}" height="{plot_h}" '
            f'data-tip="{_esc(_day_tip(r))}"/>'
        )
    out.append("</svg>")
    return "".join(out)


def _legend(themes: list[str]) -> str:
    return '<div class="legend">' + "".join(
        f'<span><i style="background:{_color(t)}"></i>{_esc(_short(t))}</span>' for t in themes
    ) + "</div>"


def _svg_week_heat(day_rows: list[dict[str, Any]], themes: list[str]) -> str:
    weeks = sorted({_week_key(r["d"]) for r in day_rows})
    cell: dict[tuple[str, str], list[str]] = defaultdict(list)
    for r in day_rows:
        wk = _week_key(r["d"])
        for s in r.get("stocks", []):
            cell[(s["bucket"], wk)].append(s["name"])
    max_c = max((len(v) for v in cell.values()), default=1) or 1
    cw, ch, lw, th = 30, 22, 70, 34
    width = lw + len(weeks) * cw + 6
    height = th + len(themes) * ch + 4
    out = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        'xmlns="http://www.w3.org/2000/svg">'
    ]
    for j, wk in enumerate(weeks):
        if j % 2 == 0 or j == len(weeks) - 1:
            out.append(
                f'<text x="{lw + j * cw + cw / 2}" y="{th - 8}" text-anchor="middle" font-size="9" '
                f'fill="#8b9cb3">{wk[5:].replace("-", "/")}</text>'
            )
    for i, t in enumerate(themes):
        y = th + i * ch
        out.append(
            f'<text x="{lw - 6}" y="{y + ch / 2 + 4}" text-anchor="end" font-size="11" '
            f'fill="{_color(t)}">{_esc(_short(t))}</text>'
        )
        for j, wk in enumerate(weeks):
            names = cell.get((t, wk), [])
            k = len(names)
            op = 0.06 if k == 0 else 0.25 + 0.75 * k / max_c
            fill = "#1a2332" if k == 0 else _color(t)
            tip = f"{wk} 주 · {t} · 상한 {k}종" + (
                "\n" + ", ".join(names[:10]) + (" …" if k > 10 else "") if k else ""
            )
            out.append(
                f'<rect x="{lw + j * cw + 1}" y="{y + 1}" width="{cw - 2}" height="{ch - 2}" rx="3" '
                f'fill="{fill}" fill-opacity="{op:.2f}" data-tip="{_esc(tip)}"/>'
            )
            if k:
                out.append(
                    f'<text x="{lw + j * cw + cw / 2}" y="{y + ch / 2 + 4}" text-anchor="middle" '
                    f'font-size="10" fill="#e7ecf3" pointer-events="none">{k}</text>'
                )
    out.append("</svg>")
    return "".join(out)


def _svg_sankey(day_rows: list[dict[str, Any]], themes: list[str]) -> str:
    """이슈(왼쪽) ↔ 테마(오른쪽). 띠 굵기 = 그 이슈가 걸린 상한 종목·일 수."""
    links: Counter[tuple[str, str]] = Counter()
    examples: dict[tuple[str, str], list[str]] = defaultdict(list)
    n_unknown = n_all = 0
    for r in day_rows:
        for s in r.get("stocks", []):
            n_all += 1
            known = [i for i in s.get("issues", []) if i != ti.UNKNOWN]
            if not known:
                n_unknown += 1
            for i in known:
                links[(i, s["bucket"])] += 1
                ex = examples[(i, s["bucket"])]
                if len(ex) < 6 and s["name"] not in ex:
                    ex.append(s["name"])
    note = (
        f'<p class="sub">상한 {n_all}종·일 중 기사·공시에서 이슈를 찾은 {n_all - n_unknown}종·일만 표시 '
        f"(뉴스 미확인 {n_unknown}종·일 제외 — 소형주는 수집 기사에 안 잡히는 경우가 많음)</p>"
    )
    if not links:
        return note + '<p class="muted">이슈 분류할 뉴스가 없습니다.</p>'
    issues = [i for i in ti.ISSUE_NAMES if any(k[0] == i for k in links)]
    right = [t for t in themes if any(k[1] == t for k in links)]
    tot_i = {i: sum(v for (a, _), v in links.items() if a == i) for i in issues}
    tot_t = {t: sum(v for (_, b), v in links.items() if b == t) for t in right}
    total = sum(links.values())
    width, height, gap, nw, min_slot = 760, 520, 6, 14, 16
    lx, rx = 160, width - 150
    avail = height - 20 - (gap + min_slot) * max(len(issues), len(right))
    sc = max(0.5, avail / total)

    def layout(names: list[str], tot: dict[str, int]) -> dict[str, float]:
        y = 10.0
        pos: dict[str, float] = {}
        for n in names:
            pos[n] = y
            y += max(tot[n] * sc, min_slot) + gap
        return pos

    py_i, py_t = layout(issues, tot_i), layout(right, tot_t)
    height = int(
        max(
            [py_i[n] + max(tot_i[n] * sc, min_slot) + 10 for n in issues]
            + [py_t[n] + max(tot_t[n] * sc, min_slot) + 10 for n in right]
        )
    )
    off_i = dict(py_i)
    off_t = dict(py_t)
    out = [
        note,
        f'<svg class="chart sankey fit" viewBox="0 0 {width} {height}" '
        'xmlns="http://www.w3.org/2000/svg">',
    ]
    mid = (lx + nw + rx) / 2
    for i in issues:
        for t in right:
            v = links.get((i, t), 0)
            if not v:
                continue
            h = v * sc
            y1, y2 = off_i[i], off_t[t]
            off_i[i] += h
            off_t[t] += h
            tip = f"{i} → {t} · 상한 {v}종·일\n예: " + ", ".join(examples[(i, t)])
            out.append(
                f'<path class="band" data-i="{_esc(i)}" data-t="{_esc(t)}" '
                f'd="M {lx + nw} {y1:.1f} C {mid:.1f} {y1:.1f} {mid:.1f} {y2:.1f} {rx} {y2:.1f} '
                f'L {rx} {y2 + h:.1f} C {mid:.1f} {y2 + h:.1f} {mid:.1f} {y1 + h:.1f} {lx + nw} {y1 + h:.1f} Z" '
                f'fill="{ti.ISSUE_COLOR.get(i, "#5a6a80")}" fill-opacity="0.42" data-tip="{_esc(tip)}"/>'
            )
    for i in issues:
        h = max(2.0, tot_i[i] * sc)
        out.append(
            f'<rect class="node" data-i="{_esc(i)}" x="{lx}" y="{py_i[i]:.1f}" width="{nw}" height="{h:.1f}" '
            f'fill="{ti.ISSUE_COLOR.get(i, "#5a6a80")}" data-tip="{_esc(f"{i} · 상한 {tot_i[i]}종·일")}"/>'
            f'<text x="{lx - 6}" y="{py_i[i] + h / 2 + 4:.1f}" text-anchor="end" font-size="11" '
            f'fill="#e7ecf3">{_esc(i)} <tspan fill="#8b9cb3">{tot_i[i]}</tspan></text>'
        )
    for t in right:
        h = max(2.0, tot_t[t] * sc)
        out.append(
            f'<rect class="node" data-t="{_esc(t)}" x="{rx}" y="{py_t[t]:.1f}" width="{nw}" height="{h:.1f}" '
            f'fill="{_color(t)}" data-tip="{_esc(f"{t} · 상한 {tot_t[t]}종·일(이슈 중복 포함)")}"/>'
            f'<text x="{rx + nw + 6}" y="{py_t[t] + h / 2 + 4:.1f}" font-size="11" '
            f'fill="#e7ecf3">{_esc(t)} <tspan fill="#8b9cb3">{tot_t[t]}</tspan></text>'
        )
    out.append("</svg>")
    return "".join(out)


def _svg_issue_heat(day_rows: list[dict[str, Any]]) -> str:
    """주별 시장 뉴스 이슈 비중(평소 대비 배수로 색) + 아래 주별 상한 종목 수."""
    weeks = sorted({_week_key(r["d"]) for r in day_rows})
    tot_w: Counter[str] = Counter()
    cnt_w: dict[tuple[str, str], int] = defaultdict(int)
    head_w: dict[tuple[str, str], str] = {}
    lu_w: Counter[str] = Counter()
    for r in day_rows:
        wk = _week_key(r["d"])
        tot_w[wk] += int(r.get("news_total", 0))
        lu_w[wk] += r["n"]
        for i, c in (r.get("news_issue") or {}).items():
            cnt_w[(i, wk)] += c
            if c and (i, wk) not in head_w:
                head_w[(i, wk)] = (r.get("news_head") or {}).get(i, "")
    if not any(tot_w.values()):
        return '<p class="muted">뉴스 캐시가 없어 이슈 온도를 계산하지 못했습니다.</p>'
    share: dict[tuple[str, str], float] = {}
    mean: dict[str, float] = {}
    for i in ti.ISSUE_NAMES:
        vals = []
        for wk in weeks:
            s = cnt_w[(i, wk)] / tot_w[wk] if tot_w[wk] else 0.0
            share[(i, wk)] = s
            if tot_w[wk]:
                vals.append(s)
        mean[i] = (sum(vals) / len(vals)) if vals else 0.0
    cw, ch, lw, th = 30, 22, 120, 34
    width = lw + len(weeks) * cw + 6
    rows_n = len(ti.ISSUE_NAMES) + 1
    height = th + rows_n * ch + 10
    max_lu = max(lu_w.values(), default=1) or 1
    out = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        'xmlns="http://www.w3.org/2000/svg">'
    ]
    for j, wk in enumerate(weeks):
        if j % 2 == 0 or j == len(weeks) - 1:
            out.append(
                f'<text x="{lw + j * cw + cw / 2}" y="{th - 8}" text-anchor="middle" font-size="9" '
                f'fill="#8b9cb3">{wk[5:].replace("-", "/")}</text>'
            )
    for r_i, issue in enumerate(ti.ISSUE_NAMES):
        y = th + r_i * ch
        col = ti.ISSUE_COLOR[issue]
        out.append(
            f'<text x="{lw - 6}" y="{y + ch / 2 + 4}" text-anchor="end" font-size="11" '
            f'fill="{col}">{_esc(issue)}</text>'
        )
        for j, wk in enumerate(weeks):
            s = share[(issue, wk)]
            ratio = s / mean[issue] if mean[issue] > 0 else 0.0
            op = min(1.0, max(0.05, (ratio - 0.5) / 1.5))
            tip = (
                f"{wk} 주 · {issue}\n기사 {cnt_w[(issue, wk)]}/{tot_w[wk]}건 ({s:.0%}) · 평소의 {ratio:.1f}배"
                + (f"\n대표: {head_w[(issue, wk)]}" if head_w.get((issue, wk)) else "")
            )
            out.append(
                f'<rect x="{lw + j * cw + 1}" y="{y + 1}" width="{cw - 2}" height="{ch - 2}" rx="3" '
                f'fill="{col}" fill-opacity="{op:.2f}" data-tip="{_esc(tip)}"/>'
            )
            if ratio >= 1.5:
                out.append(
                    f'<text x="{lw + j * cw + cw / 2}" y="{y + ch / 2 + 4}" text-anchor="middle" '
                    f'font-size="9" font-weight="700" fill="#0f1419" pointer-events="none">↑</text>'
                )
    y = th + len(ti.ISSUE_NAMES) * ch + 4
    out.append(
        f'<text x="{lw - 6}" y="{y + ch / 2 + 4}" text-anchor="end" font-size="11" fill="#e7ecf3">상한 종목 수</text>'
    )
    for j, wk in enumerate(weeks):
        h = (ch - 4) * lu_w[wk] / max_lu
        out.append(
            f'<rect x="{lw + j * cw + 4}" y="{y + ch - 2 - h:.1f}" width="{cw - 8}" height="{h:.1f}" '
            f'fill="#f0a84a" data-tip="{_esc(f"{wk} 주 · 상한 {lu_w[wk]}종")}"/>'
        )
    out.append("</svg>")
    return "".join(out)


def _svg_transitions(eps: list[dict[str, Any]]) -> str:
    tc: Counter[tuple[str, str]] = Counter()
    for a, b in zip(eps, eps[1:]):
        if a["theme"] != b["theme"]:
            tc[(a["theme"], b["theme"])] += 1
    if not tc:
        return '<p class="muted">전환이 아직 없습니다.</p>'
    names = sorted({t for k in tc for t in k}, key=lambda t: -sum(v for k, v in tc.items() if t in k))
    cw, lw, th = 44, 74, 50
    width = lw + len(names) * cw + 6
    height = th + len(names) * 26 + 6
    max_v = max(tc.values())
    out = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        'xmlns="http://www.w3.org/2000/svg">',
        f'<text x="{lw - 6}" y="14" text-anchor="end" font-size="9" fill="#8b9cb3">이전 ↓ / 다음 →</text>',
    ]
    for j, t in enumerate(names):
        out.append(
            f'<text x="{lw + j * cw + cw / 2}" y="{th - 8}" text-anchor="middle" font-size="10" '
            f'fill="{_color(t)}">{_esc(_short(t))}</text>'
        )
    for i, a in enumerate(names):
        y = th + i * 26
        out.append(
            f'<text x="{lw - 6}" y="{y + 17}" text-anchor="end" font-size="10" fill="{_color(a)}">{_esc(_short(a))}</text>'
        )
        for j, b in enumerate(names):
            v = tc.get((a, b), 0)
            op = 0.06 if not v else 0.3 + 0.7 * v / max_v
            out.append(
                f'<rect x="{lw + j * cw + 1}" y="{y + 1}" width="{cw - 2}" height="24" rx="3" '
                f'fill="{"#3d9cf5" if v else "#1a2332"}" fill-opacity="{op:.2f}" '
                f'data-tip="{_esc(f"{a} → {b} · {v}회")}"/>'
            )
            if v:
                out.append(
                    f'<text x="{lw + j * cw + cw / 2}" y="{y + 17}" text-anchor="middle" font-size="11" '
                    f'fill="#e7ecf3" pointer-events="none">{v}</text>'
                )
    out.append("</svg>")
    return "".join(out)


_CSS = """
:root{--bg:#0f1419;--card:#1a2332;--text:#e7ecf3;--muted:#8b9cb3;--accent:#3d9cf5;--ok:#3ecf8e;--warn:#f0a84a;--line:#243044}
body{font-family:"Malgun Gothic","Apple SD Gothic Neo",sans-serif;background:var(--bg);color:var(--text);
  margin:0;padding:16px 20px 48px;line-height:1.5;max-width:1240px;font-size:0.9rem}
h1{font-size:1.35rem;margin:0 0 6px}
h2{font-size:1.08rem;color:var(--ok);margin:30px 0 6px}
.sub{color:var(--muted);font-size:0.8rem;margin:0 0 10px}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px 14px;margin:10px 0}
.scroll{overflow-x:auto}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin:12px 0}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
.kpi b{display:block;font-size:1.15rem;color:var(--warn);margin:2px 0}
.kpi span{font-size:0.74rem;color:var(--muted)}
.legend{display:flex;flex-wrap:wrap;gap:6px 14px;font-size:0.78rem;margin:6px 0}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px;vertical-align:-1px}
.tag{display:inline-block;border:1px solid;border-radius:999px;padding:0 7px;font-size:0.7rem;margin:1px 3px 1px 0}
.chips{display:flex;flex-wrap:wrap;gap:8px}
.chip{background:#121c28;border:1px solid var(--line);border-left:4px solid;border-radius:6px;padding:6px 9px;font-size:0.8rem}
.chip .hl{color:var(--muted);font-size:0.72rem;display:block;max-width:330px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
table{width:100%;border-collapse:collapse;font-size:0.8rem;margin-top:8px}
th{text-align:left;color:var(--muted);border-bottom:1px solid #2a3f5c;padding:5px 6px}
td{padding:5px 6px;border-bottom:1px solid #1e2a3a;vertical-align:top}
tr.active td{background:#152232}
.muted{color:var(--muted)}
svg text{font-family:inherit}
svg.fit{width:100%;max-width:900px;height:auto;display:block}
svg .hov{fill:transparent}
svg .hov:hover{fill:#ffffff;fill-opacity:0.07}
svg [data-tip]{cursor:pointer}
.sankey .band{transition:fill-opacity .15s}
.sankey.focus .band{fill-opacity:0.06}
.sankey.focus .band.on{fill-opacity:0.75}
#tip{position:fixed;z-index:9;display:none;max-width:380px;background:#0b1118;border:1px solid #3a5a7a;border-radius:6px;
  padding:7px 9px;font-size:0.76rem;line-height:1.45;white-space:pre-line;pointer-events:none;box-shadow:0 4px 14px #0008}
.disclaimer{font-size:0.72rem;color:var(--muted);margin-top:28px}
a{color:var(--accent)}
@media (max-width:760px){body{padding:10px 8px 32px;font-size:0.86rem}h1{font-size:1.15rem}}
"""

_JS = """
const tip=document.getElementById('tip');
function showTip(ev){const t=ev.target.closest('[data-tip]');if(!t){tip.style.display='none';return;}
  tip.textContent=t.getAttribute('data-tip');tip.style.display='block';
  const w=tip.offsetWidth,h=tip.offsetHeight;let x=ev.clientX+14,y=ev.clientY+14;
  if(x+w>innerWidth-8)x=ev.clientX-w-14;if(y+h>innerHeight-8)y=ev.clientY-h-14;
  tip.style.left=Math.max(4,x)+'px';tip.style.top=Math.max(4,y)+'px';}
document.addEventListener('mousemove',showTip);document.addEventListener('click',showTip);
document.querySelectorAll('svg.sankey').forEach(svg=>{
  svg.addEventListener('mouseover',ev=>{const n=ev.target.closest('[data-i],[data-t]');if(!n)return;
    const i=n.getAttribute('data-i'),t=n.getAttribute('data-t');svg.classList.add('focus');
    svg.querySelectorAll('.band').forEach(b=>{const ok=(i===null||b.getAttribute('data-i')===i)&&(t===null||b.getAttribute('data-t')===t);
      b.classList.toggle('on',ok);});});
  svg.addEventListener('mouseleave',()=>{svg.classList.remove('focus');svg.querySelectorAll('.band.on').forEach(b=>b.classList.remove('on'));});
});
function sync(){const id=(location.hash||'').slice(1);
  document.querySelectorAll('tr.active').forEach(el=>el.classList.remove('active'));
  if(id){const el=document.getElementById(id);if(el)el.classList.add('active');}}
window.addEventListener('hashchange',sync);sync();
document.querySelectorAll('.scroll.latest').forEach(el=>{el.scrollLeft=el.scrollWidth;});
"""


def _render_html(
    *,
    eps: list[dict[str, Any]],
    day_rows: list[dict[str, Any]],
    source_names: list[str],
    has_news: bool = True,
) -> str:
    themes = _themes_by_total(day_rows)
    d0, d1 = day_rows[0]["d"], day_rows[-1]["d"]
    last = day_rows[-1]
    recent = day_rows[-5:]
    rc: Counter[str] = Counter()
    for r in recent:
        rc.update(r.get("by_theme", {}))
    r_theme, r_n = rc.most_common(1)[0] if rc else ("-", 0)
    r_iss = _top_issues([s for r in recent for s in r.get("stocks", [])], 2)
    cur_ep = eps[-1] if eps else None
    now = datetime.now(trading_calendar.KST)

    parts: list[str] = [
        '<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8"/>'
        '<meta name="viewport" content="width=device-width, initial-scale=1"/>'
        "<title>상한가 테마 흐름·이슈 연관</title><style>",
        _CSS,
        "</style></head><body><div id=\"tip\"></div>",
        "<h1>상한가 테마 흐름·이슈 연관</h1>",
        f'<p class="sub">{d0} ~ {d1} · 거래일 {len(day_rows)}일 · 에피소드 {len(eps)}개 · '
        f"최종 갱신 {now:%Y-%m-%d %H:%M} KST · 출처: {_esc(' · '.join(source_names))} + 뉴스 캐시</p>",
        '<div class="kpis">',
        f'<div class="kpi"><span>최근 거래일 {last["d"][5:]}</span><b>상한 {last["n"]}종</b>'
        f'<span>주도 {_esc(_short(last["top"]))} {last["topN"]}종</span></div>',
        f'<div class="kpi"><span>최근 5거래일 주도 테마</span><b style="color:{_color(r_theme)}">'
        f'{_esc(r_theme)}</b><span>상한 {r_n}종 / 전체 {sum(rc.values())}종</span></div>',
        '<div class="kpi"><span>최근 5거래일 상한 종목 주요 이슈</span><b>'
        + (_esc(r_iss[0][0]) if r_iss else "뉴스 미확인")
        + "</b><span>"
        + (" · ".join(f"{_esc(i)} {n}" for i, n in r_iss) if r_iss else "-")
        + "</span></div>",
    ]
    if cur_ep:
        parts.append(
            f'<div class="kpi"><span>진행 중 에피소드 {cur_ep["id"]}</span>'
            f'<b style="color:{_color(cur_ep["theme"])}">{_esc(_short(cur_ep["theme"]))} '
            f'{_span_label(cur_ep)}</b><span>일평균 {cur_ep["avgN"]}종 · 피크 {cur_ep["peak"]}종</span></div>'
        )
    parts.append("</div>")

    parts.append(f"<h2>오늘의 상한 종목 ({last['d']})</h2>")
    parts.append('<p class="sub">종목별 묶음 테마와, 그 종목을 다룬 기사·공시(직전 장 마감 ~ 다음날 아침)에서 찾은 이슈</p>')
    parts.append('<div class="chips">')
    for s in last.get("stocks", []):
        hl = f'<span class="hl">{_esc(s.get("headline") or s.get("reason") or "")}</span>'
        parts.append(
            f'<div class="chip" style="border-left-color:{_color(s["bucket"])}"><b>{_esc(s["name"])}</b> '
            f'<span class="muted">{_esc(s["raw"] or _short(s["bucket"]))}</span><br/>'
            f'{_issue_tag_html(s.get("issues", []))}{hl}</div>'
        )
    parts.append("</div>")

    parts.append("<h2>1. 테마 흐름</h2>")
    parts.append(
        '<p class="sub">일별 상한 종목 수를 테마별로 쌓은 막대 · 맨 위 띠 = 에피소드(클릭 시 상세) · '
        "날짜 위에 마우스를 올리면 그날 종목·이슈</p>"
    )
    parts.append(_legend(themes))
    parts.append(f'<div class="card scroll latest">{_svg_stream(day_rows, eps, themes)}</div>')

    parts.append("<h2>2. 테마 × 주간 히트맵</h2>")
    parts.append('<p class="sub">진할수록 그 주 상한 종목이 많음 · 칸에 마우스를 올리면 종목</p>')
    parts.append(f'<div class="card scroll latest">{_svg_week_heat(day_rows, themes)}</div>')

    parts.append("<h2>3. 이슈 ↔ 테마 연관</h2>")
    parts.append(
        '<p class="sub">왼쪽 = 상한 종목 기사·공시에 걸린 이슈, 오른쪽 = 묶음 테마, 띠 굵기 = 상한 종목·일 수 '
        "(한 종목이 여러 이슈면 각각 셈). 노드에 마우스를 올리면 그 연결만 강조</p>"
    )
    if not has_news:
        parts.append('<p class="muted">뉴스 캐시가 없어 이슈는 근거 요약 문구로만 분류했습니다.</p>')
    parts.append(f'<div class="card scroll">{_svg_sankey(day_rows, themes)}</div>')

    parts.append("<h2>4. 시장 뉴스 이슈 온도 (주간)</h2>")
    parts.append(
        '<p class="sub">전체 수집 기사 중 이슈별 비중을 그 이슈의 평소 비중과 비교 · 진할수록 평소보다 많이 언급 '
        "(↑ = 1.5배 이상) · 맨 아래 = 그 주 상한 종목 수</p>"
    )
    parts.append(f'<div class="card scroll latest">{_svg_issue_heat(day_rows)}</div>')

    parts.append("<h2>5. 테마 전환 행렬</h2>")
    parts.append('<p class="sub">행 = 이전 에피소드 주도 테마, 열 = 다음 에피소드 주도 테마, 숫자 = 전환 횟수</p>')
    parts.append(f'<div class="card scroll">{_svg_transitions(eps)}</div>')

    parts.append("<h2>6. 에피소드 상세</h2>")
    parts.append(
        "<table><thead><tr><th>ID</th><th>기간</th><th>일수</th><th>주도</th><th>일평균</th>"
        "<th>피크</th><th>주요 이슈</th><th>대표 종목</th></tr></thead><tbody>"
    )
    for e in reversed(eps):
        stocks = _stocks_in(day_rows, e["from"], e["to"])
        iss = _top_issues(stocks)
        names: list[str] = []
        for s in stocks:
            if s["name"] not in names:
                names.append(s["name"])
        parts.append(
            f'<tr id="ep-{e["id"]}"><td><a href="#ep-{e["id"]}">{e["id"]}</a></td>'
            f'<td>{e["from"]} ~ {e["to"]}</td><td>{e["days"]}</td>'
            f'<td><span style="color:{_color(e["theme"])}">■</span> {_esc(e["title"])}'
            f'<br/><span class="muted">{_esc(e["blurb"])}</span></td><td>{e["avgN"]}종</td>'
            f'<td>{e["peak"]}종 @{e["peakDay"][5:]}</td>'
            f'<td>{_issue_tag_html([i for i, _ in iss]) or "<span class=muted>미확인</span>"}</td>'
            f'<td class="muted">{_esc(", ".join(names[:6]))}{" …" if len(names) > 6 else ""}</td></tr>'
        )
    parts.append("</tbody></table>")
    parts.append(
        '<p class="disclaimer">자동 집계 · 테마 묶음은 리포트 표 분류를 합친 것 · 이슈는 기사 제목·요약 키워드 '
        "매칭이라 인과가 아니라 동시 언급입니다 · 투자 권유가 아닙니다.</p>"
    )
    parts.append(f"<script>{_JS}</script></body></html>")
    return "".join(parts)


def theme_flow_range_filename(day_from: str, day_to: str) -> str:
    """예: 2026-06-01, 2026-09-15 → report_theme_flow_2026.06-09.html"""
    a = day_from[:7].replace("-", ".")
    b = day_to[5:7]
    if day_from[:4] == day_to[:4]:
        return f"report_theme_flow_{a[:4]}.{a[5:]}-{b}.html"
    return f"report_theme_flow_{a}-{day_to[:7].replace('-', '.')}.html"


def update_theme_flow_report(output_dir: Path | None = None) -> Path | None:
    """테마 월간 HTML을 모아 흐름 리포트를 갱신. 데이터 없으면 ``None``."""
    out_dir = Path(output_dir or config.OUTPUT_DIR)
    paths = discover_theme_report_paths(out_dir)
    if not paths:
        return None
    day_rows = _parse_limit_up_days(paths)
    if not day_rows:
        return None
    eps = _build_episodes(_build_segments(day_rows))
    if not eps:
        return None
    has_news = ti.attach_issues(day_rows)
    html_out = _render_html(
        eps=eps,
        day_rows=day_rows,
        source_names=[p.name for p in paths],
        has_news=has_news,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    canonical = out_dir / "report_theme_flow.html"
    ranged = out_dir / theme_flow_range_filename(day_rows[0]["d"], day_rows[-1]["d"])
    canonical.write_text(html_out, encoding="utf-8")
    if ranged.resolve() != canonical.resolve():
        ranged.write_text(html_out, encoding="utf-8")
    return canonical
