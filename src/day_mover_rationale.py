"""
장 마감 확정 거래일 — 상한가·20%↑ 종목별 「왜 올랐나」 한 줄 요약 HTML.

메인 리포트 ``DayReport.mover_rationale_html`` 에 삽입됩니다.
종목마다 뉴스·공시·테마·키워드를 단계적으로 탐색해 한 줄을 채웁니다.
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from . import config, features, news, trading_calendar
from . import snapshot_rebuild_learning as srl
from .features import _stock_relevant_news_rows

_REASON_MAX = 88

_GENERIC_MATCH_KW = frozenset(
    {
        "기관",
        "외국인",
        "개인",
        "순매수",
        "순매도",
        "수급",
        "공매도",
        "프로그램",
        "연기금",
        "증시",
        "코스피",
        "코스닥",
        "장중",
        "마감",
        "특징주",
        "관련주",
        "테마",
        "급등",
        "상한가",
        "하락",
        "보합",
    }
)

_GENERIC_CATALYSTS = frozenset(
    {"따르면", "테마", "급등", "상한가", "코스피", "코스닥", "증시", "주가", "관련주"}
)

_CATALYST_EVENTS = (
    "인수",
    "인수설",
    "매수",
    "M&A",
    "MOU",
    "국책",
    "과제",
    "선정",
    "유상증자",
    "무상증자",
    "실적",
    "영업이익",
    "흑자",
    "적자",
    "수주",
    "계약",
    "공급",
    "승인",
    "허가",
    "FDA",
    "임상",
    "목표가",
    "투자",
    "합병",
    "지분",
    "양산",
    "출시",
    "특허",
    "납품",
    "수출",
    "관세",
    "정책",
    "보조금",
    "상장",
    "공모",
    "CB",
    "BW",
    "전환사채",
    "배당",
    "자사주",
    "조회공시",
    "개발",
    "수혜",
    "기대",
    "호재",
    "공략",
    "인프라",
    "환원",
)

_NOISE_TITLE_BITS = (
    "맛보기",
    "경영인증원",
    "글로벌 투자 정보",
    "오늘의 증시",
    "증시 마감",
    "장 마감",
    "코스피·코스닥",
    "특징주 정리",
)

_TECH_RISE_BITS = (
    "종가 기준",
    "거래량",
    "이평",
    "KOSPI",
    "누적",
    "변동성",
    "직전 거래일",
    "표준편차",
)


@dataclass(frozen=True)
class _ReasonPick:
    """급등 이유 후보 1건(문구·신뢰 점수·출처 태그)."""

    text: str
    confidence: int
    source: str


def _esc(s: str) -> str:
    """HTML 이스케이프."""
    return html.escape(str(s or ""), quote=True)


def _shorten(text: str, *, max_len: int = _REASON_MAX) -> str:
    """공백 정리 후 최대 길이로 자름(말줄임)."""
    t = re.sub(r"\s+", " ", str(text or "").strip())
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def _clean_phrase(text: str) -> str:
    """제목·이슈 문구에서 괄호·상한가/급등 꼬리표 등 노이즈 제거."""
    t = re.sub(r"\s+", " ", str(text or "").strip())
    t = re.sub(r"^\[.*?\]\s*", "", t)
    for tail in (
        "에 상한가",
        " 상한가",
        " 급등",
        " 주가",
        " 증시",
        " 관련주",
        " 테마",
        " 따르면",
    ):
        if t.endswith(tail):
            t = t[: -len(tail)].rstrip(" ,·")
    return t.strip()


def _usable_catalyst(raw: str) -> str:
    """범용·짧은 촉매 문자열 필터."""
    s = str(raw or "").strip()
    if len(s) < 2 or s.lower() in _GENERIC_CATALYSTS:
        return ""
    return s


def _has_catalyst(blob: str) -> bool:
    """``_CATALYST_EVENTS`` 키워드가 텍스트에 포함되는지."""
    return any(ev in str(blob or "") for ev in _CATALYST_EVENTS)


def _is_noise_title(title: str) -> bool:
    """시황 요약·맛보기 등 노이즈 제목 여부."""
    return any(bit in str(title or "") for bit in _NOISE_TITLE_BITS)


def _is_generic_match(matched: str, name: str) -> bool:
    """매칭 토큰이 외국인·수급 등 범용 키워드인지."""
    m = str(matched or "").strip()
    if not m or m == name:
        return False
    return m in _GENERIC_MATCH_KW


def _name_in_title(title: str, name: str) -> bool:
    """종목명이 제목에 그대로 포함되는지."""
    n = (name or "").strip()
    return bool(n and n in str(title or ""))


def _extract_issue_from_title(title: str, name: str) -> str:
    """뉴스 제목에서 종목명 전후·구분자로 이슈 구절 추출."""
    raw = re.sub(r"\s+", " ", str(title or "").strip())
    raw = re.sub(r"^\[.*?\]\s*", "", raw)
    if not raw:
        return ""
    if name and name in raw:
        idx = raw.find(name)
        before = raw[:idx].strip(" ,·-|—")
        after = raw[idx + len(name) :].strip(" ,·-|—")
        candidates = [c for c in (after, before) if c]
        for c in candidates:
            if _has_catalyst(c) or re.search(r"\d+(?:\.\d+)?(?:조|억|천억)", c):
                return _clean_phrase(c)
        if candidates:
            return _clean_phrase(candidates[0])
    parts = [p.strip() for p in re.split(r"[·|—,]", raw) if p.strip()]
    for p in parts:
        if _has_catalyst(p) or re.search(r"\d+(?:\.\d+)?(?:조|억|천억)", p):
            return _clean_phrase(p)
    return _clean_phrase(raw)


def _score_news_hit(
    hit: dict[str, Any],
    *,
    name: str,
    keywords: list[str],
    relaxed: bool = False,
) -> int:
    """
    뉴스 hit 1건의 급등 이유 적합도 점수(-1=제외, 높을수록 우선).

    종목명·매칭 토큰·촉매·금액·타이밍(early/late)을 가중합니다.
    """
    title = str(hit.get("title") or "").strip()
    desc = str(hit.get("description") or "").strip()
    matched = str(hit.get("matched") or "").strip()
    blob = f"{title} {desc}"

    if not title or _is_noise_title(title):
        return -1

    name_title = _name_in_title(title, name)
    name_blob = bool(name and name in blob)

    if _is_generic_match(matched, name):
        if not relaxed or not (_has_catalyst(blob) and name_blob):
            return -1
        score = 38
    elif name_title:
        score = 85
    elif matched == name and name_blob:
        score = 72 if name in title else 58
    elif relaxed and matched and matched not in _GENERIC_MATCH_KW:
        kw_set = {str(k) for k in keywords}
        if matched in kw_set or len(matched) >= 3:
            if _has_catalyst(blob) or name_blob:
                score = 48
            else:
                score = 32
        else:
            return -1
    else:
        return -1

    if _has_catalyst(blob):
        score += 14
    if re.search(r"\d+(?:\.\d+)?(?:조|억|천억)", blob):
        score += 8
    bucket = str(hit.get("timing_bucket") or "")
    if bucket == "t_day":
        score += 8
    elif bucket == "late":
        score += 4
    if name_title and not _has_catalyst(blob):
        if any(w in title for w in ("급등", "상한가", "강세", "↑")):
            score += 5
    return score


def _pick_from_news_hits(
    hits: list[dict[str, Any]] | None,
    *,
    name: str,
    keywords: list[str],
    source: str,
    relaxed: bool = False,
    min_score: int = 30,
) -> _ReasonPick | None:
    """뉴스 hit 목록에서 최고 점수 이유 1건 선택."""
    best: _ReasonPick | None = None
    for h in hits or []:
        if not isinstance(h, dict):
            continue
        score = _score_news_hit(h, name=name, keywords=keywords, relaxed=relaxed)
        if score < min_score:
            continue
        title = str(h.get("title") or "").strip()
        phrase = _extract_issue_from_title(title, name)
        if not phrase:
            phrase = _clean_phrase(title)
        if len(phrase) < 3:
            continue
        pick = _ReasonPick(text=phrase, confidence=score, source=source)
        if best is None or pick.confidence > best.confidence:
            best = pick
    return best


def _pick_from_row_list(
    rows: list[tuple[date, dict[str, str]]],
    *,
    name: str,
    source: str,
    require_name_in_title: bool = False,
) -> _ReasonPick | None:
    """early/당일 뉴스 row 목록에서 종목 관련 최고 이유 1건."""
    best: _ReasonPick | None = None
    for _, row in rows:
        title = str(row.get("title") or "").strip()
        desc = str(row.get("description") or "").strip()
        blob = f"{title} {desc}"
        if not title or _is_noise_title(title):
            continue
        if require_name_in_title and not _name_in_title(title, name):
            continue
        if name and name not in blob:
            continue
        phrase = _extract_issue_from_title(title, name) or _clean_phrase(title)
        if len(phrase) < 3:
            continue
        score = 78 if _name_in_title(title, name) else 55
        if _has_catalyst(blob):
            score += 12
        pick = _ReasonPick(text=phrase, confidence=score, source=source)
        if best is None or pick.confidence > best.confidence:
            best = pick
    return best


def _pick_from_matched_rows(
    rows: list[tuple[date, dict[str, str]]],
    *,
    name: str,
    keywords: list[str],
    aliases: tuple[str, ...],
    source: str,
) -> _ReasonPick | None:
    """종목명·키워드·테마 별칭으로 뉴스 윈도우를 추가 검색."""
    kws = list(keywords or [])
    for a in aliases:
        if a and a not in kws:
            kws.append(a)
    hits = news.match_stock_news_rows(rows, name, kws, limit=12)
    return _pick_from_news_hits(
        hits, name=name, keywords=kws, source=source, relaxed=True, min_score=28
    )


def _disclosure_phrase(kind: str, title: str) -> str:
    """공시 제목에서 유상증자·계약 등 핵심 구절 추출."""
    t = _clean_phrase(title)
    for pat, fmt in (
        (r"(\d+(?:\.\d+)?억)\s*유상증자", r"\1 유상증자 결정"),
        (r"유상증자\s*결정", "유상증자 결정"),
        (r"(\d+(?:\.\d+)?억).*?무상증자", r"\1 규모 무상증자"),
        (r"무상증자", "무상증자"),
        (r"단일판매.*?계약", "단일판매·공급 계약"),
        (r"영업이익", "실적 발표"),
        (r"조회공시", "조회공시"),
    ):
        m = re.search(pat, t)
        if m:
            try:
                return m.expand(fmt)
            except re.error:
                return m.group(0)
    if kind and kind != "기타":
        return kind
    return t


def _pick_from_disclosure(hits: list[dict[str, Any]] | None) -> _ReasonPick | None:
    """공시 hit에서 최고 신뢰 이유 1건."""
    best: _ReasonPick | None = None
    for h in hits or []:
        if not isinstance(h, dict):
            continue
        kind = str(h.get("kind") or "").strip()
        title = str(h.get("title") or h.get("report_nm") or "").strip()
        if not title:
            continue
        phrase = _shorten(_disclosure_phrase(kind, title))
        if len(phrase) < 3:
            continue
        conf = 80 if kind and kind != "기타" else 62
        pick = _ReasonPick(text=phrase, confidence=conf, source="disclosure")
        if best is None or pick.confidence > best.confidence:
            best = pick
    return best


def _pick_from_rise_reason_html(compare_row: dict[str, Any] | None) -> _ReasonPick | None:
    """메인 리포트 ``rise_reason_html`` 의 ``<li>`` 에서 기술·시황 노이즈 제외 후 1건."""
    if not compare_row:
        return None
    raw = str(compare_row.get("rise_reason_html") or "")
    if not raw:
        return None
    skip_bits = (
        "자동 참고",
        "단일 원인",
        "수집을 생략",
        "미확정",
        "인과 단정",
        "종목 특징",
        "시장·국제",
    )
    best: _ReasonPick | None = None
    for m in re.finditer(r"<li[^>]*>(.*?)</li>", raw, flags=re.I | re.S):
        inner = m.group(1)
        line = re.sub(r"<[^>]+>", " ", inner)
        line = re.sub(r"\s+", " ", line).strip()
        if len(line) < 10:
            continue
        if any(bit in line for bit in skip_bits + _TECH_RISE_BITS):
            continue
        if re.match(r"^\d{4}-\d{2}-\d{2}\s", line):
            continue
        conf = 64 if ("<a " in inner or "http" in inner) else 42
        if _has_catalyst(line):
            conf += 10
        pick = _ReasonPick(text=_shorten(_clean_phrase(line)), confidence=conf, source="rise_reason")
        if best is None or pick.confidence > best.confidence:
            best = pick
    return best


def _pick_from_theme_news(
    early_rows: list[tuple[date, dict[str, str]]],
    actual_ctx_rows: list[tuple[date, dict[str, str]]],
    *,
    aliases: tuple[str, ...],
) -> _ReasonPick | None:
    """테마 별칭이 매칭된 섹터 뉴스에서 이슈 1건(종목 직접 뉴스 없을 때)."""
    for rows, src in ((actual_ctx_rows, "theme_actual"), (early_rows, "theme_early")):
        for _, row in rows:
            blob = f"{row.get('title', '')} {row.get('description', '')}"
            if not srl._alias_in_blob(blob, aliases):
                continue
            title = str(row.get("title") or "").strip()
            if not title or _is_noise_title(title):
                continue
            phrase = _extract_issue_from_title(title, "") or _clean_phrase(title)
            if len(phrase) < 4:
                continue
            return _ReasonPick(text=phrase, confidence=46, source=src)
    return None


def _sector_context(
    flow_row: dict[str, Any] | None,
    *,
    code: str,
    theme_label: str,
) -> tuple[str, str, tuple[str, ...]]:
    """종목의 축약 테마·섹터 촉매·뉴스 검색 별칭."""
    theme = srl._compact_sector_label(theme_label)
    if not theme and flow_row:
        stock_map = flow_row.get("theme_stock_sectors") or {}
        if isinstance(stock_map, dict):
            theme = srl._compact_sector_label(str(stock_map.get(code) or ""))
    full = srl._full_sector_label(theme) if theme else ""
    aliases = srl._sector_aliases_for_label(full) if full else ()
    catalyst = ""
    if flow_row and theme:
        for summ in flow_row.get("theme_sector_summaries") or []:
            if not isinstance(summ, dict):
                continue
            sec = str(summ.get("sector") or "")
            if srl._compact_sector_label(sec) != theme:
                continue
            catalyst = _usable_catalyst(str(summ.get("catalyst") or ""))
            break
    return theme, catalyst, aliases


def _stock_specific_keywords(
    code: str,
    name: str,
    compare_row: dict[str, Any] | None,
    early_rows: list[tuple[date, dict[str, str]]],
) -> list[str]:
    """비교 행·급등 이력에서 종목별 구체 키워드(최대 8)."""
    raw: list[str] = list((compare_row or {}).get("keywords") or [])
    raw.extend(list((compare_row or {}).get("name_keyword_overlap_with_news") or []))
    bk, _ = features.breakout_keywords_for_stock(code, name, early_rows)
    raw.extend(list(bk))
    return list(features.filter_specific_keywords(raw))[:8]


def _peer_names(
    flow_row: dict[str, Any] | None,
    *,
    code: str,
    theme: str,
) -> list[str]:
    """같은 테마 상한·25%↑ 동반 급등 종목명(최대 3)."""
    if not flow_row or not theme:
        return []
    compact = srl._compact_sector_label(theme)
    out: list[str] = []
    for key in ("theme_limit_up", "theme_leaders_ge_25pct"):
        for s in flow_row.get(key) or []:
            if not isinstance(s, dict):
                continue
            c = str(s.get("code") or "").zfill(6)
            if c == code:
                continue
            ts = srl._compact_sector_label(str(s.get("theme_sector") or ""))
            if ts != compact:
                continue
            nm = str(s.get("name") or "").strip()
            if nm and nm not in out:
                out.append(nm)
    return out[:3]


def _fallback_reason(
    *,
    theme: str,
    catalyst: str,
    keywords: list[str],
    peers: list[str],
    return_pct: float,
    is_limit_up: bool,
) -> _ReasonPick:
    """뉴스·공시 매칭 실패 시 테마·키워드·동반 급등·등락률 순 폴백."""
    if catalyst:
        text = catalyst if not theme else f"{catalyst}"
        return _ReasonPick(text=text, confidence=35, source="sector_catalyst")
    if keywords:
        kw = ", ".join(keywords[:3])
        label = theme or "테마"
        return _ReasonPick(
            text=f"{label} ({kw}) 이슈",
            confidence=28,
            source="keywords",
        )
    if theme and peers:
        return _ReasonPick(
            text=f"{theme} 동반 급등({', '.join(peers)} 등)",
            confidence=22,
            source="peers",
        )
    if theme:
        return _ReasonPick(text=f"{theme} 테마 급등", confidence=18, source="theme_only")
    if is_limit_up:
        return _ReasonPick(text="당일 상한가(직접 뉴스·공시 미매칭)", confidence=10, source="bare")
    return _ReasonPick(
        text=f"당일 +{return_pct:.1f}% 급등",
        confidence=8,
        source="bare",
    )


def _reason_for_stock(
    compare_row: dict[str, Any] | None,
    *,
    code: str,
    name: str,
    early_rows: list[tuple[date, dict[str, str]]],
    actual_ctx_rows: list[tuple[date, dict[str, str]]],
    flow_row: dict[str, Any] | None,
    theme_label: str,
    return_pct: float,
    is_limit_up: bool,
) -> _ReasonPick:
    """
    한 종목의 급등 이유를 다단계 탐색해 최종 1건 반환.

    actual/pred 뉴스 → 공시 → rise_reason HTML → 직접/매칭 뉴스 → 테마 뉴스 → 폴백.
    """
    keywords = _stock_specific_keywords(code, name, compare_row, early_rows)
    theme, catalyst, aliases = _sector_context(flow_row, code=code, theme_label=theme_label)
    peers = _peer_names(flow_row, code=code, theme=theme)
    picks: list[_ReasonPick] = []

    if compare_row:
        for hits, src, relaxed in (
            (compare_row.get("actual_news_hits"), "actual_news", False),
            (compare_row.get("actual_news_hits"), "actual_news_relaxed", True),
            (compare_row.get("pred_news_hits"), "pred_news", False),
            (compare_row.get("pred_news_hits"), "pred_news_relaxed", True),
        ):
            p = _pick_from_news_hits(
                hits, name=name, keywords=keywords, source=src, relaxed=relaxed
            )
            if p:
                picks.append(p)
        p = _pick_from_disclosure(compare_row.get("disclosure_hits"))
        if p:
            picks.append(p)
        p = _pick_from_rise_reason_html(compare_row)
        if p:
            picks.append(p)

    for rows, src, req_title in (
        (_stock_relevant_news_rows(actual_ctx_rows, code, name), "ctx_direct", False),
        (_stock_relevant_news_rows(early_rows, code, name), "early_direct", False),
        (_stock_relevant_news_rows(actual_ctx_rows, code, name), "ctx_title", True),
    ):
        p = _pick_from_row_list(rows, name=name, source=src, require_name_in_title=req_title)
        if p:
            picks.append(p)

    for rows, src in ((actual_ctx_rows, "ctx_match"), (early_rows, "early_match")):
        p = _pick_from_matched_rows(
            rows, name=name, keywords=keywords, aliases=aliases, source=src
        )
        if p:
            picks.append(p)

    p = _pick_from_theme_news(early_rows, actual_ctx_rows, aliases=aliases)
    if p:
        picks.append(p)

    if picks:
        return max(picks, key=lambda p: p.confidence)
    return _fallback_reason(
        theme=theme,
        catalyst=catalyst,
        keywords=keywords,
        peers=peers,
        return_pct=return_pct,
        is_limit_up=is_limit_up,
    )


def _format_bullet_line(name: str, reason: str, *, is_limit_up: bool) -> str:
    """종목명 + 이유 한 줄 ``<li>`` HTML."""
    r = _shorten(reason.strip())
    if is_limit_up and r and "상한" not in r:
        r = f"{r}에 상한가"
    if not r:
        r = "상한가" if is_limit_up else "급등"
    return (
        f'<li style="margin-bottom:6px;line-height:1.5">'
        f"<strong>{_esc(name)}</strong>: {_esc(r)}</li>"
    )


def _collect_stock_entries(
    flow_row: dict[str, Any] | None,
    compare_rows: list[dict[str, Any]],
    listing_names: dict[str, str],
) -> tuple[list[dict[str, Any]], int]:
    """상한·20%↑ 종목 엔트리 목록과 상한가 개수."""
    compare_by_code = {
        str(r.get("code", "")).zfill(6): r
        for r in compare_rows
        if isinstance(r, dict) and r.get("code")
    }
    seen: set[str] = set()
    entries: list[dict[str, Any]] = []

    def _add(code: str, name: str, ret_pts: float, is_lim: bool) -> None:
        """상한·급등 엔트리 목록에 중복 없이 추가."""
        c = str(code).zfill(6)
        if not c or c in seen:
            return
        seen.add(c)
        entries.append(
            {
                "code": c,
                "name": name or listing_names.get(c, c),
                "return_pct": float(ret_pts),
                "is_limit_up": bool(is_lim),
                "compare_row": compare_by_code.get(c),
            }
        )

    if flow_row:
        for s in flow_row.get("theme_limit_up") or []:
            if not isinstance(s, dict):
                continue
            code = str(s.get("code") or "").zfill(6)
            _add(code, str(s.get("name") or ""), float(s.get("return_pct") or 0), True)
        for s in flow_row.get("theme_leaders_ge_25pct") or []:
            if not isinstance(s, dict):
                continue
            code = str(s.get("code") or "").zfill(6)
            rp = float(s.get("return_pct") or 0)
            is_lim = bool(
                s.get("is_limit_up") or srl._is_near_limit_up_return_pct(rp)
            )
            _add(code, str(s.get("name") or ""), rp, is_lim)

    thr20 = float(config.BIG_MOVE_THRESHOLD) * 100.0
    for r in compare_rows:
        if not isinstance(r, dict):
            continue
        code = str(r.get("code", "")).zfill(6)
        if code in seen:
            continue
        ret_pts = srl._actual_return_pct_points(r)
        if ret_pts is None or ret_pts + 1e-9 < thr20:
            continue
        is_lim = srl._is_near_limit_up_return_pct(ret_pts)
        _add(code, str(r.get("name") or ""), float(ret_pts), is_lim)

    entries.sort(
        key=lambda e: (not e["is_limit_up"], -float(e["return_pct"]), e["code"])
    )
    max_lim = int(config.REPORT_MOVER_RATIONALE_MAX_LIMIT_UP)
    max_big = int(config.REPORT_MOVER_RATIONALE_MAX_BIG)
    lim_n = sum(1 for e in entries if e["is_limit_up"])
    big_n = sum(1 for e in entries if not e["is_limit_up"])
    cap_lim = max_lim if max_lim > 0 else lim_n
    cap_big = max_big if max_big > 0 else big_n
    out: list[dict[str, Any]] = []
    l = b = 0
    for e in entries:
        if e["is_limit_up"]:
            if l >= cap_lim:
                continue
            l += 1
        else:
            if b >= cap_big:
                continue
            b += 1
        out.append(e)
    return out, len(entries)


def format_day_mover_rationale_html(
    *,
    flow_row: dict[str, Any] | None,
    compare_rows: list[dict[str, Any]],
    listing_names: dict[str, str],
    early_rows: list[tuple[date, dict[str, str]]],
    actual_ctx_rows: list[tuple[date, dict[str, str]]] | None = None,
    forward_observation: bool = False,
    trading_day: date | None = None,
    now_kst: datetime | None = None,
) -> str:
    """장 마감 확정일 — 상한가·20%↑ 종목마다 한 줄 요약(내부 HTML)."""
    if not config.REPORT_MOVER_RATIONALE_ENABLED:
        return ""
    if forward_observation or not trading_day:
        return ""
    now = now_kst or datetime.now(trading_calendar.KST)
    if not trading_calendar.is_krx_daily_bar_effective_closed(trading_day, now_kst=now):
        return ""
    entries, total_n = _collect_stock_entries(flow_row, compare_rows, listing_names)
    if not entries:
        return ""

    ctx_rows = list(actual_ctx_rows or [])
    stock_map = (
        flow_row.get("theme_stock_sectors")
        if isinstance(flow_row, dict) and isinstance(flow_row.get("theme_stock_sectors"), dict)
        else {}
    )
    bullets: list[str] = []
    for e in entries:
        code = e["code"]
        name = str(e["name"])
        cr = e.get("compare_row")
        theme_label = str(stock_map.get(code) or "")
        if not theme_label and isinstance(cr, dict):
            sectors = srl._theme_labels_for_compare_row(
                flow_row or {},
                cr,
                listing_names=listing_names,
                early_rows=early_rows,
                seeds=list((flow_row or {}).get("theme_seed_hits") or []),
                top_kw=list((flow_row or {}).get("top_keywords_sample") or []),
            )
            theme_label = sectors[0] if sectors else ""
        pick = _reason_for_stock(
            cr if isinstance(cr, dict) else None,
            code=code,
            name=name,
            early_rows=early_rows,
            actual_ctx_rows=ctx_rows,
            flow_row=flow_row,
            theme_label=theme_label,
            return_pct=float(e["return_pct"]),
            is_limit_up=bool(e["is_limit_up"]),
        )
        bullets.append(
            _format_bullet_line(name, pick.text, is_limit_up=bool(e["is_limit_up"]))
        )

    hidden = max(0, total_n - len(entries))
    parts = [
        '<ul class="nl mover-rationale-list" style="margin:0;padding-left:18px">',
        *bullets,
    ]
    if hidden:
        parts.append(
            f'<li style="color:var(--muted);font-size:0.8rem">'
            f"… 외 {hidden}종 (표·테마 리포트 참고)</li>"
        )
    parts.append("</ul>")
    parts.append(
        '<p class="combo-tip-empty" style="margin:8px 0 0;font-size:0.72em;color:var(--muted)">'
        "뉴스·공시·테마·키워드 순으로 자동 탐색. 직접 기사가 없으면 테마·동반 급등 맥락으로 보완.</p>"
    )
    return "".join(parts)


def enrich_day_report_mover_rationale(
    dr: Any,
    flow_row: dict[str, Any] | None,
    *,
    listing_names: dict[str, str],
    early_rows: list[tuple[date, dict[str, str]]],
    news_by_calendar: dict[date, list[dict[str, str]]] | None = None,
    now_kst: datetime | None = None,
) -> None:
    """``DayReport.mover_rationale_html`` 채우기."""
    t_day = getattr(dr, "trading_day", None)
    actual_ctx_rows: list[tuple[date, dict[str, str]]] = []
    if isinstance(t_day, date) and news_by_calendar:
        actual_ctx_rows = news.rows_for_actual_context(news_by_calendar, t_day)
    inner = format_day_mover_rationale_html(
        flow_row=flow_row,
        compare_rows=list(getattr(dr, "rows_compare", None) or []),
        listing_names=listing_names,
        early_rows=early_rows,
        actual_ctx_rows=actual_ctx_rows,
        forward_observation=bool(getattr(dr, "forward_observation", False)),
        trading_day=t_day,
        now_kst=now_kst,
    )
    setattr(dr, "mover_rationale_html", inner or "")
