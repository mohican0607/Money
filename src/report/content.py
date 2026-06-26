"""급등 근거·테마 리포트 HTML (rationale + theme_movers)."""
from __future__ import annotations

import html
import math
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .. import config, features, trading_calendar
from ..features import _stock_relevant_news_rows
from .. import news, stocks
from ..learning import market_theme as srl


# --- rationale (from rationale.py) ---

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
    "띠별 운세",
    "오늘의 띠",
    "운세",
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


def _catalyst_relevant_to_sector(
    catalyst: str,
    *,
    compact: str,
    full: str,
    aliases: tuple[str, ...],
) -> bool:
    """당일 공통 키워드(예: HBM4)가 섹터와 무관하면 섹터 이슈로 표시하지 않음."""
    cat = str(catalyst or "").strip()
    if not cat:
        return False
    blob = f"{compact} {full} {' '.join(aliases)}".lower()
    cat_l = cat.lower()
    if compact and compact.lower() in cat_l:
        return True
    for a in aliases:
        al = str(a).strip().lower()
        if len(al) >= 2 and al in cat_l:
            return True
    if any(al in blob for al in cat_l.split() if len(al) >= 2):
        return True
    return False


def _is_generic_day_theme_phrase(phrase: str, *, aliases: tuple[str, ...]) -> bool:
    """당일 시장 공통 테마 문구(예: HBM4의 전망) — 종목별 이유로 쓰지 않음."""
    p = str(phrase or "").strip()
    if not p:
        return True
    if re.search(r"의\s*(전망|기대|수혜|관련|흐름)", p):
        if not _catalyst_relevant_to_sector(
            p, compact="", full="", aliases=aliases
        ):
            return True
    m = re.search(r"([A-Za-z0-9&][A-Za-z0-9&.\-]{1,20})", p)
    if m and re.search(r"의\s*전망", p):
        token = m.group(1).lower()
        blob = f"{' '.join(aliases)}".lower()
        if token not in blob and not any(token in str(a).lower() for a in aliases):
            return True
    return False


def _stock_reason_context_rows(
    news_by_calendar: dict[date, list[dict[str, str]]] | None,
    t_day: date,
    early_rows: list[tuple[date, dict[str, str]]],
    actual_ctx_rows: list[tuple[date, dict[str, str]]],
    *,
    lookback_sessions: int = 4,
) -> list[tuple[date, dict[str, str]]]:
    """당일·late·직전 거래일 뉴스를 합쳐 종목 이유 탐색용 row 목록."""
    rows: list[tuple[date, dict[str, str]]] = list(actual_ctx_rows)
    seen = {str(r.get("title") or "") for _, r in rows if str(r.get("title") or "")}
    if news_by_calendar and lookback_sessions > 0:
        try:
            end_prev = trading_calendar.last_trading_day_before(t_day)
            start_prev = end_prev
            for _ in range(max(0, lookback_sessions - 1)):
                start_prev = trading_calendar.last_trading_day_before(start_prev)
            for d in trading_calendar.trading_sessions_in_range(start_prev, end_prev):
                for r in news_by_calendar.get(d, []):
                    title = str(r.get("title") or "")
                    if title and title not in seen:
                        seen.add(title)
                        rows.append((d, r))
        except ValueError:
            pass
    for d, r in early_rows:
        title = str(r.get("title") or "")
        if title and title not in seen:
            seen.add(title)
            rows.append((d, r))
    return rows


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
            if _is_generic_day_theme_phrase(phrase, aliases=aliases):
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
            raw_cat = _usable_catalyst(str(summ.get("catalyst") or ""))
            if raw_cat and _catalyst_relevant_to_sector(
                raw_cat,
                compact=theme,
                full=full,
                aliases=aliases,
            ):
                catalyst = raw_cat
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
    name: str,
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
    if theme and peers:
        return _ReasonPick(
            text=f"{theme} 동반 급등({', '.join(peers)} 등)",
            confidence=28,
            source="peers",
        )
    usable_kw = [
        k
        for k in keywords
        if str(k).strip()
        and str(k).lower() not in (name or "").strip().lower()
        and (name or "").strip().lower() not in str(k).lower()
    ]
    if usable_kw:
        kw = ", ".join(usable_kw[:3])
        label = theme or "테마"
        return _ReasonPick(
            text=f"{label} ({kw}) 이슈",
            confidence=24,
            source="keywords",
        )
    if theme:
        return _ReasonPick(text=f"{theme} 테마 급등", confidence=18, source="theme_only")
    if is_limit_up:
        return _ReasonPick(text="당일 상한가(종목 직접 뉴스·공시 미확인)", confidence=10, source="bare")
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
        filtered: list[_ReasonPick] = []
        for p in picks:
            if _is_generic_day_theme_phrase(p.text, aliases=aliases):
                if p.source in ("theme_early", "theme_actual", "sector_catalyst", "ctx_match", "early_match"):
                    continue
                if not _name_in_title(p.text, name) and name not in p.text:
                    continue
            filtered.append(p)
        if filtered:
            return max(filtered, key=lambda p: p.confidence)
    return _fallback_reason(
        name=name,
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


def _stock_reason_text(
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
) -> str:
    """종목별 급등 이유 평문(테마 리포트 표·불릿 공통)."""
    pick = _reason_for_stock(
        compare_row if isinstance(compare_row, dict) else None,
        code=code,
        name=name,
        early_rows=early_rows,
        actual_ctx_rows=actual_ctx_rows,
        flow_row=flow_row,
        theme_label=theme_label,
        return_pct=return_pct,
        is_limit_up=is_limit_up,
    )
    r = _shorten(pick.text.strip())
    if is_limit_up and r and "상한" not in r:
        r = f"{r}에 상한가"
    if not r:
        r = "상한가" if is_limit_up else f"당일 +{return_pct:.1f}% 급등"
    return _esc(r)


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
    news_by_calendar: dict[date, list[dict[str, str]]] | None = None,
    trading_day: date | None = None,
    forward_observation: bool = False,
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

    ctx_rows = _stock_reason_context_rows(
        news_by_calendar,
        trading_day,
        early_rows,
        list(actual_ctx_rows or []),
    )
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
        news_by_calendar=news_by_calendar,
        trading_day=t_day,
        forward_observation=bool(getattr(dr, "forward_observation", False)),
        now_kst=now_kst,
    )
    setattr(dr, "mover_rationale_html", inner or "")

# --- move reference ---

@dataclass
class _NewsClip:
    """리포트 참고 패널용 뉴스 한 건(분류·타이밍 메타 포함)."""

    day: date | None
    title: str
    description: str
    link: str
    matched: str
    category: str
    timing_bucket: str = ""
    timing_label: str = ""


_INVESTOR_KW = (
    "외국인",
    "기관",
    "순매수",
    "순매도",
    "수급",
    "공매도",
    "프로그램 매수",
    "프로그램 매도",
    "개인",
    "기관투자자",
    "연기금",
)
_EXPERT_KW = (
    "목표가",
    "목표주가",
    "투자의견",
    "컨센서스",
    "리포트",
    "증권가",
    "증권사",
    "전망",
    "상향",
    "하향",
    "매수",
    "매도",
    "목표",
    "OP",
    "TP",
)
_THEME_KW = (
    "테마",
    "섹터",
    "반도체",
    "2차전지",
    "바이오",
    "AI",
    "로봇",
    "조선",
    "방산",
    "원전",
    "전력",
    "실적",
    "수주",
    "공급",
    "수출",
)
_RISK_KW = (
    "우려",
    "리스크",
    "경고",
    "악재",
    "조정",
    "하락",
    "급락",
    "적자",
    "부진",
    "소송",
    "규제",
    "상폐",
    "거래정지",
)


def _esc_nq(s: str) -> str:
    """HTML 이스케이프(속성 따옴표 없음)."""
    return html.escape(s, quote=False)


def _clip_li(c: _NewsClip) -> str:
    """``_NewsClip`` → 리포트 ``<li>`` HTML."""
    day_s = c.day.isoformat() if isinstance(c.day, date) else ""
    me = _esc_nq(c.matched or c.category)
    te = _esc_nq(c.title)
    desc = (c.description or "").strip()
    desc_html = f" <span style=\"color:var(--muted)\">{_esc_nq(desc[:160])}</span>" if desc else ""
    timing_html = ""
    if c.timing_label:
        tone = "#9fd3ff" if c.timing_bucket == "early" else ("#e6c07b" if c.timing_bucket == "late" else "#3ecf8e")
        timing_html = (
            f'<code style="font-size:0.7rem;color:{tone};margin-right:6px">'
            f"{_esc_nq(c.timing_label)}</code>"
        )
    if c.link:
        le = html.escape(c.link, quote=True)
        body = f'<a href="{le}" target="_blank" rel="noopener">{te}</a>{desc_html}'
    else:
        body = f"{te}{desc_html}"
    return (
        f'<li><span class="pill">{day_s}</span> {timing_html}'
        f'<code style="font-size:0.72rem;color:#9fd3ff">{me}</code> {body}</li>'
    )


def _classify_blob(blob: str) -> str:
    """기사 제목·요약을 수급/전문가·테마·일반/리스크 톤으로 분류."""
    for k in _RISK_KW:
        if k in blob:
            return "리스크·우려"
    for k in _INVESTOR_KW:
        if k in blob:
            return "수급·투자자"
    for k in _EXPERT_KW:
        if k in blob:
            return "전망·의견"
    for k in _THEME_KW:
        if k in blob:
            return "테마·업종"
    return "종목·이슈"


def _news_clips_from_hits(hits: list[dict] | None, *, limit: int = 12) -> list[_NewsClip]:
    """뉴스 hit dict 목록 → 분류된 ``_NewsClip`` 리스트."""
    out: list[_NewsClip] = []
    for h in hits or []:
        if len(out) >= limit:
            break
        title = str(h.get("title") or "").strip()
        if not title:
            continue
        day_o = h.get("day")
        day_v = day_o if isinstance(day_o, date) else None
        blob = f"{title} {h.get('description') or ''}"
        out.append(
            _NewsClip(
                day=day_v,
                title=title,
                description=str(h.get("description") or "")[:220],
                link=str(h.get("link") or "").strip(),
                matched=str(h.get("matched") or ""),
                category=_classify_blob(blob),
                timing_bucket=str(h.get("timing_bucket") or ""),
                timing_label=str(h.get("timing_label") or ""),
            )
        )
    return out


def _merge_rise_news_hits(
    pred_news_hits: list[dict] | None,
    actual_news_hits: list[dict] | None,
    *,
    limit: int = 14,
) -> list[dict]:
    """early(14:30까지) + late/당일 뉴스 hit을 제목 기준 중복 제거 후 합칩니다."""
    merged: list[dict] = []
    seen: set[str] = set()
    for h in list(pred_news_hits or []) + list(actual_news_hits or []):
        title = str(h.get("title") or "").strip()
        if not title:
            continue
        key = str(h.get("link") or title)
        if key in seen:
            continue
        seen.add(key)
        merged.append(h)
        if len(merged) >= limit:
            break
    return merged


def _news_timing_summary_html(hits: list[dict]) -> str:
    """상승 참고 뉴스가 14:30까지/이후/당일 중 어디에 주로 있는지 요약."""
    if not hits:
        return ""
    counts = {"early": 0, "late": 0, "t_day": 0}
    for h in hits:
        b = str(h.get("timing_bucket") or "")
        if b in counts:
            counts[b] += 1
    total = sum(counts.values())
    if total <= 0:
        return ""
    n_early, n_late, n_t = counts["early"], counts["late"], counts["t_day"]
    parts = []
    if n_early:
        parts.append(f"14:30까지 <strong>{n_early}</strong>건")
    if n_late:
        parts.append(f"14:30이후 <strong>{n_late}</strong>건")
    if n_t:
        parts.append(f"관측일 당일 <strong>{n_t}</strong>건")
    detail = " · ".join(parts)
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    top_bucket, top_n = ranked[0]
    if top_n <= 0:
        attribution = ""
    elif top_n == total:
        label = {"early": "14:30까지 뉴스", "late": "14:30이후 뉴스", "t_day": "관측일 당일 뉴스"}.get(
            top_bucket, ""
        )
        attribution = f" → 상승 참고는 <strong>{label}</strong> 쪽에 모여 있습니다."
    else:
        second_bucket, second_n = ranked[1] if len(ranked) > 1 else ("", 0)
        if second_n > 0 and top_n == second_n:
            attribution = " → 14:30 전·후·당일 뉴스가 고르게 겹칩니다."
        else:
            label = {"early": "14:30까지 뉴스", "late": "14:30이후 뉴스", "t_day": "관측일 당일 뉴스"}.get(
                top_bucket, ""
            )
            attribution = f" → 상승 참고는 주로 <strong>{label}</strong> 때문으로 보입니다."
    return (
        "<p style=\"margin:0 0 10px;padding:8px 10px;background:#152232;"
        "border-left:3px solid var(--accent);border-radius:6px;font-size:0.84rem\">"
        f"<strong>뉴스 시점</strong> — {detail}{attribution}</p>"
    )


def _scan_context_news(
    actual_ctx_rows: list[tuple[date, dict]] | None,
    *,
    stock_name: str,
    keywords: list[str],
    limit: int = 16,
) -> list[_NewsClip]:
    """관측일 late+당일 뉴스에서 종목·키워드·수급/전망 키워드 기사를 추가 수집."""
    if not actual_ctx_rows:
        return []
    name = (stock_name or "").strip()
    kws = list(keywords or [])
    for extra in _INVESTOR_KW[:6] + _EXPERT_KW[:6] + _THEME_KW[:4]:
        if extra not in kws:
            kws.append(extra)
    hits = news.match_stock_news_rows(actual_ctx_rows, name, kws, limit=limit)
    broad: list[_NewsClip] = []
    seen: set[str] = set()
    for cal_d, row in actual_ctx_rows:
        title = (row.get("title") or "").strip()
        desc = (row.get("description") or "").strip()
        blob = f"{title} {desc}"
        if not blob.strip():
            continue
        key = row.get("link") or (title + desc[:80])
        if key in seen:
            continue
        cat = _classify_blob(blob)
        if cat in ("수급·투자자", "전망·의견", "테마·업종", "리스크·우려"):
            if name and name in blob:
                matched = name
            else:
                matched = cat
            seen.add(key)
            broad.append(
                _NewsClip(
                    day=cal_d,
                    title=title,
                    description=desc[:220],
                    link=(row.get("link") or "").strip(),
                    matched=matched,
                    category=cat,
                )
            )
        if len(broad) >= limit:
            break
    by_title = {c.title: c for c in _news_clips_from_hits(hits, limit=limit)}
    for c in broad:
        if c.title not in by_title:
            by_title[c.title] = c
    return list(by_title.values())[:limit]


def _technical_bullets(
    code: str,
    t_day: date,
    returns_df: pd.DataFrame | None,
    returns_ml: pd.DataFrame | None,
    *,
    returns_sub: pd.DataFrame | None = None,
    returns_ml_sub: pd.DataFrame | None = None,
) -> list[str]:
    """당일·직전 거래일 시세·거래량 특징(일봉 캐시 기준)."""
    code = str(code).zfill(6)
    bullets: list[str] = []
    sub = returns_sub
    if sub is None:
        if returns_df is None or returns_df.empty:
            return ["일봉(OHLCV) 데이터가 없어 추세·거래량 요약을 생략했습니다."]
        sub = returns_df[returns_df["Code"].astype(str).str.zfill(6) == code].sort_values("Date")
    if sub.empty:
        return ["캐시에 해당 종목 일봉이 없습니다."]

    ts = pd.Timestamp(t_day)
    row = sub[sub["Date"] == ts]
    if row.empty:
        return [f"{t_day.isoformat()} 거래일 일봉 행이 없습니다(상장·거래정지·휴장 등)."]
    r0 = row.iloc[-1]
    ret_t = float(r0.get("return_pct", 0) or 0) * 100.0
    vol = float(r0.get("Volume", 0) or 0)

    prior = sub[sub["Date"] < ts].tail(5)
    if len(prior) >= 1:
        ret_prev = float(prior.iloc[-1].get("return_pct", 0) or 0) * 100.0
        bullets.append(f"직전 거래일 수익률 약 <strong>{ret_prev:+.2f}%</strong>.")
    if len(prior) >= 3:
        cum3 = (float(prior.tail(3)["return_pct"].sum()) * 100.0) if "return_pct" in prior.columns else 0.0
        bullets.append(f"직전 3거래일 누적 약 <strong>{cum3:+.2f}%</strong> (단기 추세 참고).")

    if len(prior) >= 5 and "Volume" in prior.columns:
        vmean = float(prior["Volume"].mean())
        if vmean > 0 and vol > 0:
            ratio = vol / vmean
            if ratio >= 2.5:
                bullets.append(
                    f"당일 거래량이 직전 5일 평균 대비 약 <strong>{ratio:.1f}배</strong> (거래 급증 참고)."
                )
            elif ratio <= 0.45:
                bullets.append(
                    f"당일 거래량이 직전 5일 평균 대비 약 <strong>{ratio:.1f}배</strong> (거래 위축)."
                )

    ml_slice = returns_ml_sub
    if ml_slice is None and returns_ml is not None and not returns_ml.empty:
        ml_slice = returns_ml[
            (returns_ml["Code"].astype(str).str.zfill(6) == code)
            & (returns_ml["Date"] == ts)
        ]
    if ml_slice is not None and not ml_slice.empty:
        ml = ml_slice[ml_slice["Date"] == ts] if "Date" in ml_slice.columns else ml_slice
        if not ml.empty:
            m = ml.iloc[-1]
            ma_r = float(m.get("close_ma20_ratio", 0) or 0)
            if ma_r > 0.08:
                bullets.append(
                    "20일 이평 대비 직전 종가가 <strong>위쪽</strong>에 있음(중기 추세 상단 근처)."
                )
            elif ma_r < -0.08:
                bullets.append(
                    "20일 이평 대비 직전 종가가 <strong>아래쪽</strong>에 있음(중기 추세 하단 근처)."
                )
            std5 = float(m.get("ret_roll_std5", 0) or 0)
            if std5 >= 0.06:
                bullets.append(
                    f"최근 5일 수익률 변동성(표준편차)이 <strong>큼</strong> ({std5*100:.1f}%p 수준)."
                )

    direction = "상승" if ret_t > 0.5 else ("하락" if ret_t < -0.5 else "보합")
    bullets.insert(
        0,
        f"{t_day.isoformat()} 종가 기준 일간 <strong>{ret_t:+.2f}%</strong> ({direction}).",
    )
    return bullets[:8]


def _market_bullet(kospi_return: float | None, t_day: date) -> str | None:
    """KOSPI 전일 대비 수익률 한 줄 bullet(없으면 ``None``)."""
    if kospi_return is None or not math.isfinite(float(kospi_return)):
        return None
    pct = float(kospi_return) * 100.0
    tone = "상승" if pct > 0.15 else ("하락" if pct < -0.15 else "보합")
    return f"같은 날 KOSPI 지수 전일 대비 약 <strong>{pct:+.2f}%</strong> ({tone}) — 시장 흐름 참고."


def _section(title: str, bullets: list[str]) -> str:
    """제목 + bullet ``<ul>`` HTML 섹션."""
    if not bullets:
        return ""
    items = "".join(f"<li>{b}</li>" for b in bullets)
    return f"<p><strong>{_esc_nq(title)}</strong></p><ul class=\"nl\">{items}</ul>"


def _section_news(title: str, clips: list[_NewsClip], *, empty: str) -> str:
    """카테고리별로 묶은 뉴스 클립 섹션 HTML."""
    if not clips:
        return f"<p><strong>{_esc_nq(title)}</strong> — {empty}</p>"
    by_cat: dict[str, list[_NewsClip]] = {}
    for c in clips:
        by_cat.setdefault(c.category, []).append(c)
    parts = [f"<p><strong>{_esc_nq(title)}</strong> (제목·요약 키워드 분류, 인과 아님)</p>"]
    for cat in ("수급·투자자", "전망·의견", "테마·업종", "종목·이슈", "리스크·우려"):
        group = by_cat.get(cat)
        if not group:
            continue
        parts.append(f"<p style=\"margin:8px 0 4px;font-size:0.8rem;color:var(--muted)\">▸ {_esc_nq(cat)}</p>")
        parts.append("<ul class=\"nl\">" + "".join(_clip_li(c) for c in group[:5]) + "</ul>")
    return "".join(parts)


def theme_early_overlap_html(
    overlap: list[str],
    *,
    news_cutoff_label: str,
) -> str:
    """예측 입력(early) 뉴스·종목명 키워드 겹침 — ``rise_reason_html`` 말미용."""
    if not overlap:
        return ""
    kw = ", ".join(_esc_nq(k) for k in overlap[:10])
    return (
        "<p style=\"margin-top:10px;padding-top:8px;border-top:1px solid #2a3f5c\">"
        f"<strong>테마·early 뉴스 상관</strong> — 직전 거래일 "
        f"<strong>{_esc_nq(news_cutoff_label)}</strong>(KST)까지 뉴스 키워드와 "
        f"종목명 토큰 겹침: <span class=\"pill\" style=\"margin-left:4px\">{kw}</span>"
        " (당일 시장 테마 표와 동일 규칙)</p>"
    )


def build_move_reference_html(
    *,
    code: str,
    name: str,
    t_trading_day: date | None,
    actual_ret: float | None,
    actual_intraday_pct: float | None = None,
    pred_news_hits: list[dict] | None = None,
    actual_news_hits: list[dict] | None = None,
    actual_ctx_rows: list[tuple[date, dict]] | None = None,
    disclosure_hits: list[dict] | None = None,
    keywords: list[str] | None = None,
    kospi_return: float | None = None,
    news_evidence_collected: bool = False,
    returns_df: Any = None,
    returns_ml: Any = None,
    returns_sub: pd.DataFrame | None = None,
    returns_ml_sub: pd.DataFrame | None = None,
) -> str:
    """
    리포트 「상·하락 참고」 HTML 블록 전체를 생성합니다.

    ``news_evidence_collected`` 가 False이면 뉴스·공시 상세는 수집 생략 안내만 넣습니다.
    """
    tlabel = t_trading_day.isoformat() if t_trading_day is not None else "관측일"
    chunks: list[str] = []

    if actual_ret is None:
        if actual_intraday_pct is not None and math.isfinite(float(actual_intraday_pct)):
            ip = float(actual_intraday_pct)
            chunks.append(
                "<p><strong>참고</strong> — "
                f"{tlabel} <strong>장중·실시간 등락률 약 {ip:+.2f}%</strong> (종가 확정 전). "
                "아래 시세·뉴스 해석은 장 마감 후 갱신될 수 있습니다.</p>"
            )
        else:
            chunks.append(
                "<p class=\"combo-tip-empty\"><strong>참고</strong> — "
                f"{tlabel} 실제 등락률이 없거나 미확정입니다.</p>"
            )
        return "".join(chunks)

    act_pct = float(actual_ret) * 100.0
    move_word = "상승" if act_pct > 0.5 else ("하락" if act_pct < -0.5 else "보합")
    chunks.append(
        f"<p><strong>{tlabel} 종가 {act_pct:+.2f}%</strong> ({move_word}) 기준 자동 참고입니다. "
        "단일 원인이 아닐 수 있으며, 아래는 공개 뉴스·시세·공시 매칭 결과입니다.</p>"
    )

    if t_trading_day is not None and trading_calendar.is_trading_day(t_trading_day):
        tech = _technical_bullets(
            code,
            t_trading_day,
            returns_df if isinstance(returns_df, pd.DataFrame) else None,
            returns_ml if isinstance(returns_ml, pd.DataFrame) else None,
            returns_sub=returns_sub,
            returns_ml_sub=returns_ml_sub,
        )
        chunks.append(_section("종목 특징·추세·거래량 (일봉)", tech))

    mkt = _market_bullet(kospi_return, t_trading_day or date.today())
    if mkt:
        chunks.append(_section("시장·국제 흐름", [mkt]))

    if not news_evidence_collected:
        chunks.append(
            "<p><em>이번 배치 실행</em>에서는 관측일 뉴스·공시 매칭을 생략했습니다. "
            "단일일·구간 실행 시 <code>include_target_calendar_news</code> 가 켜지면 "
            "수급·전망·테마 뉴스 분류가 채워집니다.</p>"
        )
    else:
        merged_hits = _merge_rise_news_hits(pred_news_hits, actual_news_hits, limit=14)
        timing_html = _news_timing_summary_html(merged_hits)
        if timing_html:
            chunks.append(timing_html)
        clips = _news_clips_from_hits(merged_hits, limit=14)
        extra = _scan_context_news(
            actual_ctx_rows,
            stock_name=name,
            keywords=list(keywords or []),
            limit=8,
        )
        seen_t = {c.title for c in clips}
        for c in extra:
            if c.title not in seen_t:
                clips.append(c)
                seen_t.add(c.title)
        chunks.append(
            _section_news(
                "뉴스 참고 (수급·테마·전문가 의견·리스크)",
                clips[:14],
                empty="관측일·전후 구간에서 종목명·키워드·수급/전망 키워드와 겹친 기사를 찾지 못했습니다.",
            )
        )

        dh = list(disclosure_hits or [])
        if dh:
            disc_parts = ["<p><strong>공시·IR</strong> (네이버 증권 종목 공시)</p><ul class=\"nl\">"]
            for d in dh[:6]:
                kind = _esc_nq(str(d.get("kind") or ""))
                title = _esc_nq(str(d.get("title") or ""))
                link = (d.get("link") or "").strip()
                if link:
                    le = html.escape(link, quote=True)
                    disc_parts.append(
                        f'<li><code style="font-size:0.72rem;color:#9fd3ff">{kind}</code> '
                        f'<a href="{le}" target="_blank" rel="noopener">{title}</a></li>'
                    )
                else:
                    disc_parts.append(
                        f'<li><code style="font-size:0.72rem;color:#9fd3ff">{kind}</code> {title}</li>'
                    )
            disc_parts.append("</ul>")
            chunks.append("".join(disc_parts))
        else:
            chunks.append("<p><strong>공시·IR</strong> — 당일 표시할 공시 항목이 없거나 매칭되지 않았습니다.</p>")

    if keywords:
        kw_show = ", ".join(_esc_nq(k) for k in list(keywords)[:10])
        chunks.append(
            f"<p style=\"font-size:0.82rem;color:var(--muted)\">예측 시 참고 키워드: {kw_show}</p>"
        )

    chunks.append(
        "<p class=\"combo-tip-empty\" style=\"margin-top:8px;font-size:0.78em\">"
        "자동 요약이며 투자 조언이 아닙니다. 수급·전문가 의견은 뉴스 제목·요약 키워드 분류이고, "
        "거래소 공식 투자자별 순매수 통계와 다를 수 있습니다.</p>"
    )
    return "".join(chunks)

# --- theme_movers (from theme_movers.py) ---

_GENERIC_CATALYSTS = frozenset(
    {"따르면", "테마", "급등", "상한가", "코스피", "코스닥", "증시", "주가", "관련주"}
)
_TITLE_MAX = 72
_THEME_REASON_MAX = 200
_KW_MAX = 4
_PEER_MAX = 5


def _esc_theme(s: str) -> str:
    """HTML 이스케이프(테마 리포트)."""
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
    from ..features import _stock_relevant_news_rows

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
    if catalyst and _catalyst_relevant_to_sector(
        catalyst, compact=compact, full=full, aliases=aliases
    ):
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
        "종목 특징",
        "시장·국제",
    )
    for m in re.finditer(r"<li[^>]*>(.*?)</li>", raw, flags=re.I | re.S):
        line = _plain_from_html(m.group(1), max_len=_THEME_REASON_MAX)
        if len(line) < 12:
            continue
        if any(bit in line for bit in skip_bits + _TECH_RISE_BITS):
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
    if len(text) > _THEME_REASON_MAX:
        text = text[: _THEME_REASON_MAX - 1] + "…"
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
    actual_ctx_rows = _stock_reason_context_rows(
        news_by_calendar,
        t_day,
        early_rows,
        news.rows_for_actual_context(news_by_calendar, t_day),
    )
    listing_names = flow_row.get("_listing_names") or {}
    mover_rationale_html = format_day_mover_rationale_html(
        flow_row=flow_row,
        compare_rows=list(dr.rows_compare or []),
        listing_names=listing_names,
        early_rows=early_rows,
        actual_ctx_rows=actual_ctx_rows,
        news_by_calendar=news_by_calendar,
        trading_day=t_day,
    )
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
                        "reason": _stock_reason_text(
                            compare_by_code.get(str(s.get("code") or "").zfill(6)),
                            code=str(s.get("code") or "").zfill(6),
                            name=str(s.get("name") or ""),
                            early_rows=early_rows,
                            actual_ctx_rows=actual_ctx_rows,
                            flow_row=flow_row,
                            theme_label=theme,
                            return_pct=float(s.get("return_pct") or 0),
                            is_limit_up=bool(
                                not s.get("is_intraday_limit_down")
                                and (
                                    s.get("is_limit_up")
                                    or srl._is_near_limit_up_return_pct(
                                        float(s.get("return_pct") or 0)
                                    )
                                )
                            ),
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
        "mover_rationale_html": mover_rationale_html,
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
    {% if day.mover_rationale_html %}
    <div class="mover-rationale-block">
      <h3 class="mover-rationale-h">왜 올랐나 (상한가·급등)</h3>
      {{ day.mover_rationale_html | safe }}
    </div>
    {% endif %}

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
    .mover-rationale-block { margin: 10px 0 14px; padding: 10px 12px;
      background: #1a2838; border: 1px solid #3a5a4a; border-radius: 8px; }
    .mover-rationale-h { font-size: 0.92rem; color: var(--ok); margin: 0 0 8px; }
    .mover-rationale-block ul { margin: 0; padding-left: 18px; }
    .mover-rationale-block li { margin-bottom: 6px; line-height: 1.5; }
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
    from .. import report

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