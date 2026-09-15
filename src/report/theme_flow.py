# -*- coding: utf-8 -*-
"""월간 테마 리포트(상한) → 연결·변화 흐름 HTML.

``report_theme_20pct_*.html`` / ``report_theme_25pct_*.html`` 을 읽어
``output/report_theme_flow.html`` (및 기간 파일명)을 갱신한다.
테마 월간 리포트가 쓰인 직후(14:30·15:30 파이프라인)에 호출한다.
"""
from __future__ import annotations

import html as html_mod
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from src import config

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

SHORT: dict[str, str] = {
    "건설·부동산": "건설",
    "반도체·전자": "반도체",
    "바이오·헬스": "바이오",
    "섬유·소비재": "섬유",
    "자동차·모빌리티": "자동차",
    "에너지·전력": "에너지",
    "소재·산업": "소재",
    "로봇·기계": "로봇",
    "게임·엔터": "엔터",
    "IT·SW": "IT",
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
    "분산": "주도 없이 산발",
    "기타": "소테마 혼합",
}


def _esc(s: object) -> str:
    return html_mod.escape(str(s))


def _fill_for(n: float) -> str:
    if n >= 18:
        return "#3d9cf5"
    if n >= 11:
        return "#2a4a6a"
    if n >= 8:
        return "#243044"
    return "#1a2332"


def discover_theme_report_paths(output_dir: Path | None = None) -> list[Path]:
    """월별 테마 HTML. 같은 달이면 20pct 우선, 없으면 25pct."""
    out = Path(output_dir or config.OUTPUT_DIR)
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
    by_day: dict[str, list[str]] = defaultdict(list)
    for p in paths:
        soup = BeautifulSoup(p.read_text(encoding="utf-8"), "html.parser")
        for sec in soup.select("section.day[id^=day-]"):
            day = (sec.get("id") or "").replace("day-", "")
            if not day:
                continue
            for tr in sec.select("table.stocks tbody tr"):
                chg = tr.select_one(".col-chg") or tr.select_one("td.ok")
                if not chg or "상한" not in chg.get_text():
                    continue
                theme_el = tr.select_one(".col-theme") or tr.select_one("td.theme")
                theme = theme_el.get_text(strip=True) if theme_el else ""
                by_day[day].append(BUCKET.get(theme, "기타"))
    day_rows: list[dict[str, Any]] = []
    for d in sorted(by_day):
        c = Counter(by_day[d])
        top, tn = c.most_common(1)[0]
        total = len(by_day[d])
        day_rows.append(
            {
                "d": d,
                "n": total,
                "top": top,
                "topN": tn,
                "share": round(tn / total, 2),
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


def _render_html(
    *,
    eps: list[dict[str, Any]],
    day_rows: list[dict[str, Any]],
    source_names: list[str],
) -> str:
    tcount: Counter[tuple[str, str]] = Counter()
    for a, b in zip(eps, eps[1:]):
        if a["theme"] != b["theme"]:
            tcount[(a["theme"], b["theme"])] += 1
    theme_edges = [{"from": a, "to": b, "n": n} for (a, b), n in tcount.most_common()]
    themes = sorted({e["theme"] for e in eps})

    theme_pos_sum: dict[str, float] = defaultdict(float)
    theme_pos_n: dict[str, int] = defaultdict(int)
    for i, e in enumerate(eps):
        theme_pos_sum[e["theme"]] += i
        theme_pos_n[e["theme"]] += 1
    order = sorted(themes, key=lambda t: theme_pos_sum[t] / theme_pos_n[t])
    ranks = [order[i : i + 3] for i in range(0, len(order), 3)]
    node_w, node_h, rank_gap, node_gap, pad = 110, 48, 90, 24, 20
    pos: dict[str, tuple[float, float]] = {}
    max_h = 0.0
    for ri, rank in enumerate(ranks):
        for oi, tid in enumerate(rank):
            x = pad + ri * (node_w + rank_gap)
            y = pad + oi * (node_h + node_gap)
            pos[tid] = (x, y)
            max_h = max(max_h, y + node_h)
    theme_svg_w = pad * 2 + max(1, len(ranks)) * (node_w + rank_gap) - rank_gap
    theme_svg_h = max_h + pad

    cols = 8
    ep_w, ep_h, egap = 88, 58, 18
    ep_rows = (len(eps) + cols - 1) // cols
    chain_svg_w = pad * 2 + cols * (ep_w + egap) - egap
    chain_svg_h = pad * 2 + ep_rows * (ep_h + egap + 20) - egap

    d0, d1 = day_rows[0]["d"], day_rows[-1]["d"]
    src_note = " · ".join(source_names)

    parts: list[str] = [
        """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>상한가 테마 연결·변화 그래프</title>
<style>
:root {
  --bg:#0f1419; --card:#1a2332; --text:#e7ecf3; --muted:#8b9cb3;
  --accent:#3d9cf5; --ok:#3ecf8e; --warn:#f0a84a; --line:#243044;
}
body{font-family:"Malgun Gothic","Apple SD Gothic Neo",sans-serif;background:var(--bg);color:var(--text);
  margin:0;padding:16px 20px 48px;line-height:1.5;max-width:1200px;font-size:0.9rem}
h1{font-size:1.35rem;margin:0 0 6px}
h2{font-size:1.08rem;color:var(--ok);margin:28px 0 10px}
h3{font-size:0.95rem;color:var(--accent);margin:16px 0 8px}
.sub{color:var(--muted);font-size:0.82rem;margin:0 0 14px}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:14px 16px;margin:12px 0}
.pill{display:inline-block;padding:2px 8px;border-radius:999px;background:#243044;font-size:0.72rem;margin:2px 3px 2px 0;color:var(--text);text-decoration:none;border:1px solid transparent}
.pill:hover{border-color:var(--accent);color:var(--accent)}
.stats{display:flex;flex-wrap:wrap;gap:14px;margin:10px 0 6px}
.stat{min-width:100px}.stat b{display:block;font-size:1.25rem;color:var(--warn)}.stat span{font-size:0.75rem;color:var(--muted)}
table{width:100%;border-collapse:collapse;font-size:0.82rem;margin-top:8px}
th{text-align:left;color:var(--muted);border-bottom:1px solid #2a3f5c;padding:5px 6px}
td{padding:5px 6px;border-bottom:1px solid #1e2a3a;vertical-align:top}
tr:hover td{background:#121c28}
tr.active td{background:#152232}
.muted{color:var(--muted)}.warn{color:var(--warn)}
svg text{font-family:inherit}
.scroll{overflow-x:auto}
.bar-row{display:flex;align-items:center;gap:8px;margin:4px 0;font-size:0.8rem}
.bar-label{width:120px;flex-shrink:0;color:var(--muted)}
.bar-track{flex:1;background:#121c28;height:16px;border-radius:3px;overflow:hidden}
.bar-fill{height:100%;background:var(--warn);border-radius:3px}
.bar-val{width:48px;text-align:right;font-size:0.75rem}
.callout{background:#152232;border:1px solid #2a4a6a;border-radius:8px;padding:10px 12px;margin:12px 0;font-size:0.84rem}
.line-chart{display:flex;align-items:flex-end;gap:3px;height:120px;padding:8px 0;border-bottom:1px solid var(--line)}
.line-col{flex:1;min-width:10px;display:flex;flex-direction:column;justify-content:flex-end;align-items:center;height:100%;cursor:pointer;text-decoration:none}
.line-col .stem{width:100%;max-width:14px;border-radius:2px 2px 0 0;background:#2a4a6a}
.line-col:hover .stem{background:var(--accent)}
.line-col .cap{font-size:0.58rem;color:var(--muted);margin-top:3px;transform:rotate(-50deg);transform-origin:top left;white-space:nowrap;height:28px}
.disclaimer{font-size:0.72rem;color:var(--muted);margin-top:28px}
a{color:var(--accent)}
</style>
</head>
<body>
""",
        "<h1>상한가 테마 연결·변화 그래프</h1>",
        f'<p class="sub">{d0} ~ {d1} · 에피소드 {len(eps)}개 · 출처: {_esc(src_note)}</p>',
        '<div class="callout"><strong>자동 갱신</strong> — '
        "14:30·15:30 리포트 파이프라인이 테마 HTML을 쓴 직후 이 파일도 같이 갱신됩니다. "
        "보름이 아니라 <b>1~6거래일 에피소드</b> 단위입니다.</div>",
        '<div class="stats">',
        f'<div class="stat"><b>{len(eps)}</b><span>에피소드</span></div>',
        f'<div class="stat"><b>{len(themes)}</b><span>테마 노드</span></div>',
        f'<div class="stat"><b>{len(theme_edges)}</b><span>전환 종류</span></div>',
        f'<div class="stat"><b>{sum(r["n"] for r in day_rows)}</b><span>상한 종목·일</span></div>',
        "</div>",
        "<h2>1. 시간축 연결 그래프</h2>",
        '<p class="sub">노드 = 에피소드 · 파랑/진할수록 강도↑ · 클릭하면 아래 상세</p>',
        f'<div class="scroll card"><svg width="{chain_svg_w}" height="{chain_svg_h}" '
        'xmlns="http://www.w3.org/2000/svg">',
    ]

    coords: list[tuple[float, float, dict[str, Any]]] = []
    for i, e in enumerate(eps):
        r, c = divmod(i, cols)
        x = pad + c * (ep_w + egap)
        y = pad + r * (ep_h + egap + 20)
        coords.append((x, y, e))
    for i in range(len(coords) - 1):
        x1, y1, _ = coords[i]
        x2, y2, _ = coords[i + 1]
        if i % cols == cols - 1:
            sx, sy = x1 + ep_w / 2, y1 + ep_h
            tx, ty = x2 + ep_w / 2, y2
            parts.append(
                f'<path d="M {sx} {sy} L {sx} {sy + 10} L {tx} {sy + 10} L {tx} {ty}" '
                'fill="none" stroke="#5a6a80" stroke-width="1.5"/>'
            )
        else:
            parts.append(
                f'<line x1="{x1 + ep_w}" y1="{y1 + ep_h / 2}" x2="{x2}" '
                f'y2="{y2 + ep_h / 2}" stroke="#5a6a80" stroke-width="1.5"/>'
            )
    for x, y, e in coords:
        fill = _fill_for(e["avgN"])
        tc = "#0f1419" if e["avgN"] >= 18 else "#e7ecf3"
        parts.append(f'<a href="#ep-{e["id"]}"><g>')
        parts.append(
            f'<rect x="{x}" y="{y}" width="{ep_w}" height="{ep_h}" rx="5" fill="{fill}" '
            'stroke="#3a4a5c" stroke-width="1"/>'
        )
        parts.append(
            f'<text x="{x + ep_w / 2}" y="{y + 16}" text-anchor="middle" font-size="10" '
            f'fill="{tc}">{_esc(e["from"][5:])}</text>'
        )
        parts.append(
            f'<text x="{x + ep_w / 2}" y="{y + 34}" text-anchor="middle" font-size="12" '
            f'font-weight="700" fill="{tc}">{_esc(SHORT.get(e["theme"], e["theme"]))}</text>'
        )
        parts.append(
            f'<text x="{x + ep_w / 2}" y="{y + 50}" text-anchor="middle" font-size="10" '
            f'fill="{tc}">{e["avgN"]}종</text>'
        )
        parts.append("</g></a>")
    parts.append("</svg></div>")

    parts.append('<div style="margin:8px 0 4px">')
    for e in eps:
        parts.append(
            f'<a class="pill" href="#ep-{e["id"]}">{e["id"]} '
            f'{_esc(SHORT.get(e["theme"], e["theme"]))}</a>'
        )
    parts.append("</div>")

    parts.append("<h2>2. 테마 연결 그래프</h2>")
    parts.append(
        '<p class="sub">노드 = 테마 · 화살표 = 에피소드 간 전환 횟수 · 굵을수록 잦음</p>'
    )
    parts.append(
        f'<div class="scroll card"><svg width="{theme_svg_w}" height="{theme_svg_h}" '
        'xmlns="http://www.w3.org/2000/svg">'
    )
    maxw = max((e["n"] for e in theme_edges), default=1)
    for te in theme_edges:
        if te["from"] not in pos or te["to"] not in pos:
            continue
        x1, y1 = pos[te["from"]]
        x2, y2 = pos[te["to"]]
        sx, sy = x1 + node_w, y1 + node_h / 2
        tx, ty = x2, y2 + node_h / 2
        mid_x = (sx + tx) / 2
        mid_y = (sy + ty) / 2 - (20 if x2 <= x1 else 0)
        sw = 1 + (te["n"] / maxw) * 4
        parts.append(
            f'<path d="M {sx} {sy} Q {mid_x} {mid_y} {tx} {ty}" fill="none" '
            f'stroke="#3d9cf5" stroke-width="{sw:.1f}" opacity="0.85"/>'
        )
        parts.append(
            f'<text x="{mid_x}" y="{mid_y - 4}" text-anchor="middle" font-size="10" '
            f'fill="#8b9cb3">{te["n"]}</text>'
        )
    for tid, (x, y) in pos.items():
        cnt = sum(1 for e in eps if e["theme"] == tid)
        parts.append(
            f'<rect x="{x}" y="{y}" width="{node_w}" height="{node_h}" rx="5" '
            'fill="#243044" stroke="#3a5a7a"/>'
        )
        parts.append(
            f'<text x="{x + node_w / 2}" y="{y + 20}" text-anchor="middle" font-size="12" '
            f'font-weight="700" fill="#e7ecf3">{_esc(SHORT.get(tid, tid))}</text>'
        )
        parts.append(
            f'<text x="{x + node_w / 2}" y="{y + 36}" text-anchor="middle" font-size="10" '
            f'fill="#8b9cb3">{cnt}회 등장</text>'
        )
    parts.append("</svg></div>")

    parts.append('<h3>자주 나타난 테마 전환</h3><div class="card">')
    tmax = theme_edges[0]["n"] if theme_edges else 1
    for te in theme_edges[:12]:
        pct = 100 * te["n"] / tmax
        label = f"{SHORT.get(te['from'], te['from'])} → {SHORT.get(te['to'], te['to'])}"
        parts.append(
            f'<div class="bar-row"><div class="bar-label">{_esc(label)}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{pct:.0f}%"></div></div>'
            f'<div class="bar-val">{te["n"]}회</div></div>'
        )
    parts.append("</div>")

    parts.append("<h2>3. 변화 그래프</h2>")
    parts.append("<h3>에피소드 강도 (일평균 상한 종목 수)</h3>")
    parts.append(
        '<p class="sub">막대 높이 = 그 에피소드 일평균 상한 · 클릭 시 상세</p>'
        '<div class="card scroll"><div class="line-chart" style="min-width:700px">'
    )
    maxn = max((e["avgN"] for e in eps), default=1) or 1
    for e in eps:
        h = max(6, int(100 * e["avgN"] / maxn))
        parts.append(
            f'<a class="line-col" href="#ep-{e["id"]}" title="{e["id"]} {e["avgN"]}종">'
            f'<div class="stem" style="height:{h}px;background:{_fill_for(e["avgN"])}"></div>'
            f'<div class="cap">{e["from"][5:]}</div></a>'
        )
    parts.append("</div></div>")

    parts.append('<h3>직전 에피소드 대비 증감 (Δ)</h3><div class="card">')
    if len(eps) >= 2:
        dmax = (
            max(abs(eps[i]["avgN"] - eps[i - 1]["avgN"]) for i in range(1, len(eps)))
            or 1
        )
        for i, e in enumerate(eps):
            if i == 0:
                continue
            delta = round(e["avgN"] - eps[i - 1]["avgN"], 1)
            pct = min(100, abs(delta) / dmax * 100)
            color = "#3ecf8e" if delta >= 0 else "#e07070"
            label = f"{e['from'][5:]} {SHORT.get(e['theme'], e['theme'])}"
            sign = "+" if delta >= 0 else ""
            parts.append(
                f'<div class="bar-row"><div class="bar-label">{_esc(label)}</div>'
                f'<div class="bar-track"><div class="bar-fill" '
                f'style="width:{pct:.0f}%;background:{color}"></div></div>'
                f'<div class="bar-val">{sign}{delta}</div></div>'
            )
    parts.append("</div>")

    parts.append(
        '<h3>일별 상한 종목 수</h3><div class="card scroll">'
        '<div class="line-chart" style="min-width:900px;height:100px">'
    )
    dmaxn = max((r["n"] for r in day_rows), default=1) or 1
    for r in day_rows:
        h = max(4, int(80 * r["n"] / dmaxn))
        parts.append(
            f'<div class="line-col" title="{r["d"]} {r["n"]}종 {r["top"]}">'
            f'<div class="stem" style="height:{h}px"></div>'
            f'<div class="cap">{r["d"][5:]}</div></div>'
        )
    parts.append("</div></div>")

    parts.append("<h2>4. 에피소드 상세</h2>")
    parts.append(
        "<table><thead><tr><th>ID</th><th>기간</th><th>일수</th><th>주도</th>"
        "<th>일평균</th><th>피크</th><th>설명</th></tr></thead><tbody>"
    )
    for e in eps:
        parts.append(
            f'<tr id="ep-{e["id"]}"><td><a href="#ep-{e["id"]}">{e["id"]}</a></td>'
            f'<td>{e["from"]} ~ {e["to"]}</td><td>{e["days"]}</td>'
            f'<td>{_esc(e["title"])}</td><td>{e["avgN"]}종</td>'
            f'<td class="warn">{e["peak"]}종 @{e["peakDay"][5:]}</td>'
            f'<td class="muted">{_esc(e["blurb"])}</td></tr>'
        )
    parts.append("</tbody></table>")
    parts.append(
        '<p class="disclaimer">자동 집계 · 테마 버킷은 리포트 표 분류를 묶은 것 · '
        "투자 권유가 아닙니다.</p>"
    )
    parts.append(
        """<script>
function sync(){
  const id=(location.hash||'').slice(1);
  document.querySelectorAll('tr.active').forEach(el=>el.classList.remove('active'));
  if(id){const el=document.getElementById(id); if(el){el.classList.add('active');}}
}
window.addEventListener('hashchange',sync); sync();
</script>
</body></html>
"""
    )
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
    html_out = _render_html(
        eps=eps,
        day_rows=day_rows,
        source_names=[p.name for p in paths],
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    canonical = out_dir / "report_theme_flow.html"
    ranged = out_dir / theme_flow_range_filename(day_rows[0]["d"], day_rows[-1]["d"])
    canonical.write_text(html_out, encoding="utf-8")
    if ranged.resolve() != canonical.resolve():
        ranged.write_text(html_out, encoding="utf-8")
    return canonical
