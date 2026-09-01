"""급등 근거·테마 리포트 HTML (rationale + theme_movers)."""
from __future__ import annotations

import html
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .. import config, features, trading_calendar
from ..features import _stock_relevant_news_rows
from .. import news, stocks
from ..learning import market_theme as srl

# 월간 테마 리포트 파일명: ``report_theme_20pct_YYYY.MM.html``
THEME_REPORT_FILENAME_PCT_TAG = "20pct"
_LEGACY_THEME_REPORT_FILENAME_PCT_TAG = "25pct"


def theme_report_threshold_pct() -> int:
    """테마 리포트 제목·파일명에 쓰는 급등 임계(퍼센트 포인트)."""
    return int(round(float(config.BIG_MOVE_THRESHOLD) * 100.0))


def theme_report_filename_for_month(year: int, month: int) -> str:
    """월간 테마 리포트 파일명 (예: ``report_theme_20pct_2026.09.html``)."""
    return (
        f"report_theme_{THEME_REPORT_FILENAME_PCT_TAG}_{year}.{month:02d}.html"
    )


def legacy_theme_report_filename_for_month(year: int, month: int) -> str:
    """구 파일명 (``report_theme_25pct_*.html``) — 병합·목차 호환용."""
    return (
        f"report_theme_{_LEGACY_THEME_REPORT_FILENAME_PCT_TAG}_{year}.{month:02d}.html"
    )


def theme_report_source_paths_for_month(
    year: int, month: int, *, output_dir: Path | None = None
) -> list[Path]:
    """해당 월 테마 리포트를 읽을 경로(신규 → 구 파일명 순, 존재하는 것만)."""
    root = output_dir or config.OUTPUT_DIR
    paths: list[Path] = []
    for fname in (
        theme_report_filename_for_month(year, month),
        legacy_theme_report_filename_for_month(year, month),
    ):
        p = root / fname
        if p.is_file() and p not in paths:
            paths.append(p)
    return paths


# --- rationale (from rationale.py) ---

# 종목별 「왜 올랐나」·근거 요약 — 뉴스·공시 종합 문장이 잘리지 않도록
_REASON_MAX = 220

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

# 단독으로는 근거가 되지 않는 키워드(「테마 — 실적」 등)
_THIN_REASON_TOKENS = frozenset(
    {
        "실적",
        "계약",
        "수주",
        "공급",
        "승인",
        "허가",
        "투자",
        "기대",
        "전망",
        "강세",
        "급등",
        "테마",
        "관련",
        "이슈",
        "호재",
        "재료",
        "수혜",
        "부각",
        "관심",
        "상승",
        "반등",
        "기준",
        "규모",
    }
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

_RET_ONLY_REASON_RE = re.compile(r"^당일 \+\d+(?:\.\d+)?% 급등$")

# 공시 괄호 안 흔한 메타(실질 내용 아님)
_DISCLOSURE_PAREN_NOISE = frozenset(
    {
        "답변",
        "답변변경",
        "기타",
        "정정",
        "공시",
        "해당없음",
        "없음",
        "해당사항없음",
        "해당사항 없음",
        "상승",
        "하락",
        "급등",
        "급락",
        "풍문보도",
        "보도",
        "언론",
        "미확정",
        "확인중",
    }
)

# 공시 제목에서 먼저 잡을 고가치 촉매 패턴
_DISCLOSURE_SUBSTANCE_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"단일판매\s*[·ㆍ.\-]?\s*공급\s*계약\s*체결", "단일판매·공급 계약"),
    (r"(\d+(?:\.\d+)?(?:억|조|천억))\s*(?:규모\s*)?(?:단일판매|공급|수주|계약)", r"\1 규모 계약·수주"),
    (r"(\d+(?:\.\d+)?(?:억|조|천억))\s*유상증자", r"\1 유상증자"),
    (r"유상증자\s*결정", "유상증자 결정"),
    (r"(\d+(?:\.\d+)?(?:억|조|천억)).*?무상증자", r"\1 규모 무상증자"),
    (r"무상증자", "무상증자"),
    (r"(?:영업|분기|반기|연간).*?(?:실적|잠정)", "실적 발표"),
    (r"(?:M&A|인수|합병|지분\s*취득)", "M&A·인수"),
    (r"(?:FDA|임상\s*\d+상)", "FDA·임상"),
    (r"매매거래정지\s*해제", "거래정지 해제"),
    (r"거래정지\s*해제", "거래정지 해제"),
)

# 이유 문구와 종목 테마가 명백히 다를 때 걸러낼 업종 토큰
_MAJOR_SECTOR_TOKENS = (
    "반도체",
    "2차전지",
    "바이오",
    "조선",
    "방산",
    "원전",
    "건설",
    "부동산",
    "게임",
    "엔터",
    "화장품",
    "자동차",
    "철강",
    "해운",
    "방위",
    "의료",
    "항공",
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
    if _is_useless_reason_phrase(s):
        return ""
    if s in _THIN_REASON_TOKENS or len(re.sub(r"\s+", "", s)) < 4:
        return ""
    return s


def _is_thin_reason_text(text: str) -> bool:
    """「테마 — 실적」처럼 정보량이 부족한 근거 문구."""
    t = re.sub(r"\s+", " ", str(text or "").strip())
    if not t:
        return True
    # 동반·테마 폴백 문구는 짧아도 표시용으로 인정
    if any(
        bit in t
        for bit in (
            "동반 급등",
            "관련주 동반",
            "이슈 급등",
            "테마·수급",
            "상한가권",
        )
    ):
        return False
    if " — " in t:
        _head, tail = t.rsplit(" — ", 1)
        tail = tail.strip()
        if tail in _THIN_REASON_TOKENS or _is_useless_reason_phrase(tail):
            return True
        if len(re.sub(r"\s+", "", tail)) < 6:
            return True
    core = re.sub(r"\s+", "", t)
    if t in _THIN_REASON_TOKENS or core in _THIN_REASON_TOKENS:
        return True
    if len(core) < 8:
        return True
    return False


def _reason_texts_overlap(a: str, b: str) -> bool:
    """두 근거 문구가 실질적으로 같은 내용인지(중복 합성 방지)."""
    ca = re.sub(r"\s+", "", str(a or ""))
    cb = re.sub(r"\s+", "", str(b or ""))
    if not ca or not cb:
        return False
    if ca in cb or cb in ca:
        return True
    # 앞 18자  overlapping window
    return ca[:18] == cb[:18] and len(ca) >= 12 and len(cb) >= 12


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
    if relaxed and not name_title and not name_blob:
        score = min(score, 24)
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
        if len(phrase) < 3 or _is_useless_reason_phrase(phrase, name=name):
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
        if len(phrase) < 3 or _is_useless_reason_phrase(phrase, name=name):
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
    # 간접 매칭은 점수를 높여 오탐(무관 기사)을 줄임
    return _pick_from_news_hits(
        hits, name=name, keywords=kws, source=source, relaxed=True, min_score=40
    )


def _norm_disclosure_text(text: str) -> str:
    """공시 제목 정규화(공백·특수 구분)."""
    s = str(text or "").replace("ㆍ", "·")
    return re.sub(r"\s+", " ", s).strip()


def _is_disclosure_label_only(phrase: str) -> bool:
    """조회공시·거래정지 등 라벨만 있고 실질 내용이 없을 때 True."""
    p = re.sub(r"\s+", "", _norm_disclosure_text(phrase))
    if len(p) < 2:
        return True
    if p in {
        "조회공시",
        "조회공시답변",
        "거래정지",
        "매매거래정지",
        "공시",
        "상한가",
        "급등",
    }:
        return True
    if re.fullmatch(
        r"(?:주가(?:및|와)거래량등의급변에관한)?조회공시(?:답변(?:변경)?)?(?:\(.*?\))*",
        p,
        flags=re.I,
    ):
        return True
    if re.fullmatch(r"(?:매매)?거래정지(?:\(.*?\))?(?:의건)?", p, flags=re.I):
        return True
    if p.endswith("조회공시") and len(p) <= 32 and not _has_catalyst(phrase):
        return True
    return False


def _disclosure_paren_substance(title: str) -> str:
    """괄호 안 실질 내용(조회공시 답변 등) 추출."""
    for m in re.finditer(r"\(([^)]{4,})\)", str(title or "")):
        inner = m.group(1).strip()
        key = re.sub(r"\s+", "", inner)
        if key in _DISCLOSURE_PAREN_NOISE or key.endswith("없음"):
            continue
        if len(inner) >= 4 and (_has_catalyst(inner) or len(inner) >= 6):
            phrase = _clean_phrase(inner)
            if phrase and not _is_disclosure_label_only(phrase):
                return phrase
    return ""


def _strip_disclosure_boilerplate_one(title: str) -> str:
    """공시 제목 한 덩어리에서 boilerplate 제거 후 잔여 구절."""
    s = _norm_disclosure_text(title)
    for pat in (
        r"^주가\s*(?:및|와)\s*거래량\s*등?\s*의\s*급(?:변|등)[^\-·|:]*",
        r"^주가(?:및|와)거래량등의급(?:변|등)[^\-·|:]*",
        r"^조회\s*공시(?:\s*답변(?:변경)?)?(?:\s*\([^)]*\))?\s*",
        r"^조회공시(?:답변(?:변경)?)?(?:\([^)]*\))?\s*",
        r"^매매?\s*거래\s*정지(?:\s*\([^)]*\))?(?:\s*의\s*건)?\s*",
        r"^매매거래정지(?:\([^)]*\))?(?:의건)?\s*",
        r"^공\s*시\s*",
    ):
        s = re.sub(pat, "", s, flags=re.I).strip(" -·|:,")
    paren = _disclosure_paren_substance(s)
    if paren:
        return paren
    for m in re.finditer(r"\(([^)]{3,})\)", s):
        inner = m.group(1).strip()
        key = re.sub(r"\s+", "", inner)
        if key in _DISCLOSURE_PAREN_NOISE or key.endswith("없음"):
            continue
        if len(inner) >= 4 and (_has_catalyst(inner) or len(inner) >= 6):
            return _clean_phrase(inner)
    s = re.sub(r"\([^)]*\)", "", s).strip(" -·|:,")
    return _clean_phrase(s)


def _strip_disclosure_boilerplate(title: str) -> str:
    """구분자로 나뉜 공시 제목에서 실질 구절(뒤쪽 우선) 추출."""
    s = _norm_disclosure_text(title)
    if not s:
        return ""
    for sep in (" - ", " · ", " | ", " / ", "-", "·", "|", "/"):
        if sep in s:
            for chunk in reversed([c.strip() for c in s.split(sep) if c.strip()]):
                cleaned = _strip_disclosure_boilerplate_one(chunk)
                if cleaned and not _is_disclosure_label_only(cleaned):
                    return cleaned
    cleaned = _strip_disclosure_boilerplate_one(s)
    if cleaned and not _is_disclosure_label_only(cleaned):
        return cleaned
    return ""


def _disclosure_phrase(kind: str, title: str) -> str:
    """공시 제목에서 상한·급등 원인으로 쓸 **실질 문구** 추출(라벨만이면 '')."""
    t = _norm_disclosure_text(title)
    if not t:
        return ""
    paren = _disclosure_paren_substance(t)
    if paren:
        return paren
    for pat, fmt in _DISCLOSURE_SUBSTANCE_PATTERNS:
        m = re.search(pat, t, flags=re.I)
        if not m:
            continue
        try:
            phrase = m.expand(fmt)
        except re.error:
            phrase = m.group(0)
        phrase = _clean_phrase(phrase)
        if phrase and not _is_disclosure_label_only(phrase):
            return phrase
    stripped = _strip_disclosure_boilerplate(t)
    if stripped:
        return stripped
    return ""


def _score_disclosure_reason(phrase: str, kind: str) -> int:
    """공시에서 추출한 이유의 신뢰 점수."""
    if not phrase or _is_disclosure_label_only(phrase):
        return -1
    score = 62
    if _has_catalyst(phrase):
        score += 20
    if re.search(r"\d+(?:\.\d+)?(?:억|조|천억)", phrase):
        score += 8
    if kind in ("단일판매", "실적", "유상증자", "무상증자", "BW/CB"):
        score += 8
    if kind == "조회공시" and len(phrase) >= 8:
        score += 6
    if "해제" in phrase and "정지" in phrase:
        score += 4
    return min(score, 92)


def _pick_from_disclosure(hits: list[dict[str, Any]] | None) -> _ReasonPick | None:
    """공시 hit에서 실질 내용이 있는 최고 이유 1건."""
    best: _ReasonPick | None = None
    for h in hits or []:
        if not isinstance(h, dict):
            continue
        kind = str(h.get("kind") or "").strip()
        title = str(h.get("title") or h.get("report_nm") or "").strip()
        if not title:
            continue
        phrase = _shorten(_disclosure_phrase(kind, title))
        if len(phrase) < 3 or _is_useless_reason_phrase(phrase):
            continue
        conf = _score_disclosure_reason(phrase, kind)
        if conf < 0:
            continue
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


def _is_useless_reason_phrase(phrase: str, *, name: str = "") -> bool:
    """등락률 반복·주가 재인용·공시 라벨만 등 정보 없는 문구."""
    p = str(phrase or "").strip()
    if not p:
        return True
    if _RET_ONLY_REASON_RE.match(p):
        return True
    if _is_disclosure_label_only(p):
        return True
    if p in ("상한가", "급등"):
        return True
    if p.startswith("주가,") or p.startswith("주가 "):
        return True
    if "장중" in p and re.search(r"\d+(?:\.\d+)?%", p) and name and name not in p:
        return True
    # 날짜 조각·조사만 남은 잔해 (예: 「일부터」, 「월부터」)
    core = re.sub(r"\s+", "", p)
    if len(core) < 6 and re.fullmatch(
        r"(?:\d{1,2})?(?:년|월|일|주)?(?:부터|까지|이후|이전|현재)?",
        core,
    ):
        return True
    if re.fullmatch(r".{0,4}일부터", core):
        return True
    if core in {"관련", "이슈", "테마", "강세", "급등세", "관련주", "기준", "규모"}:
        return True
    # 「핸드셋 관련 cbsi」처럼 테마+잡영문 토큰
    if re.fullmatch(r".{2,40}관련[A-Za-z]{2,8}", core):
        return True
    if re.fullmatch(r"[A-Za-z]{2,6}", core) and not core.isupper():
        return True
    return False


def _phrase_conflicts_sector(
    phrase: str,
    *,
    compact: str,
    aliases: tuple[str, ...],
) -> bool:
    """이유에 다른 업종 토큰이 있으면 True(종목 테마와 불일치)."""
    p = str(phrase or "")
    if not p:
        return False
    alias_blob = f"{compact} {' '.join(aliases)}".lower()
    for tok in _MAJOR_SECTOR_TOKENS:
        if tok not in p:
            continue
        tl = tok.lower()
        if tl in alias_blob:
            continue
        if compact and tl in compact.lower():
            continue
        if any(tl in str(a).lower() for a in aliases):
            continue
        return True
    return False


def _reason_pick_usable(
    pick: _ReasonPick,
    *,
    name: str,
    theme: str,
    full: str,
    aliases: tuple[str, ...],
) -> bool:
    """최종 채택 가능한 이유인지(인과·업종·정보 가치)."""
    if _is_useless_reason_phrase(pick.text, name=name):
        return False
    name_linked = bool(name and (name in pick.text or _name_in_title(pick.text, name)))
    if _phrase_conflicts_sector(pick.text, compact=theme, aliases=aliases) and not name_linked:
        return False
    indirect_sources = (
        "actual_news_relaxed",
        "pred_news_relaxed",
        "ctx_match",
        "early_match",
        "theme_early",
        "theme_actual",
    )
    if pick.source in indirect_sources and not name_linked:
        if not _catalyst_relevant_to_sector(
            pick.text, compact=theme, full=full, aliases=aliases
        ):
            return False
    return True


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


_OFFTOPIC_THEME_TITLE_BITS = (
    "개방형 직위",
    "공무원",
    "원서 접수",
    "채용 공고",
    "교육부",
    "행안부",
    "띠별 운세",
    "오늘의 운세",
    "날씨",
    "프로야구",
    "프리미어리그",
    "연예가",
)


def _is_offtopic_theme_title(title: str, *, aliases: tuple[str, ...]) -> bool:
    """테마와 무관한 채용·운세·스포츠 등 제목인지."""
    t = str(title or "")
    if not t:
        return True
    if any(bit in t for bit in _OFFTOPIC_THEME_TITLE_BITS):
        # 별칭이 제목에 직접 있으면 허용(예: 교육 테마 + 교육부)
        if not any(len(str(a)) >= 2 and str(a) in t for a in aliases):
            return True
    return False


def _pick_from_theme_news(
    early_rows: list[tuple[date, dict[str, str]]],
    actual_ctx_rows: list[tuple[date, dict[str, str]]],
    *,
    aliases: tuple[str, ...],
    theme: str = "",
) -> _ReasonPick | None:
    """테마 별칭이 매칭된 섹터 뉴스에서 이슈 1건(종목 직접 뉴스 없을 때)."""
    if not aliases:
        return None
    best: _ReasonPick | None = None
    for rows, src in ((actual_ctx_rows, "theme_actual"), (early_rows, "theme_early")):
        for _, row in rows:
            title = str(row.get("title") or "").strip()
            desc = str(row.get("description") or "").strip()
            blob = f"{title} {desc}"
            # 제목에 별칭이 있어야 테마 뉴스로 인정(본문만 매칭은 오탐 많음)
            if not srl._alias_in_blob(title, aliases):
                continue
            if not title or _is_noise_title(title) or _is_offtopic_theme_title(
                title, aliases=aliases
            ):
                continue
            phrase = _extract_issue_from_title(title, "") or _clean_phrase(title)
            if len(phrase) < 10 or _is_useless_reason_phrase(phrase) or _is_thin_reason_text(
                phrase
            ):
                continue
            generic = _is_generic_day_theme_phrase(phrase, aliases=aliases)
            has_cat = _has_catalyst(blob)
            conf = 54 if has_cat else 42
            if generic:
                if not has_cat:
                    continue
                conf = 28
            elif not has_cat and len(re.sub(r"\s+", "", phrase)) < 14:
                continue
            text = phrase
            if theme and theme not in phrase:
                text = f"{theme} — {phrase}"
            pick = _ReasonPick(text=text, confidence=conf, source=src)
            if best is None or pick.confidence > best.confidence:
                best = pick
    return best


def _resolve_sector_aliases(theme: str) -> tuple[str, tuple[str, ...]]:
    """축약 테마 → 풀 라벨·뉴스 별칭(별칭 토큰으로도 환원)."""
    if not theme:
        return "", ()
    full = srl._full_sector_label(theme)
    aliases = srl._sector_aliases_for_label(full) if full else ()
    if aliases:
        return full, aliases
    theme_l = theme.lower()
    for sector_label, als in srl._THEME_SECTORS:
        compact = srl._compact_sector_label(sector_label)
        if theme == compact or theme == sector_label:
            return sector_label, als
        if any(theme_l == str(a).lower() for a in als):
            return sector_label, als
    return full or theme, ()


def _sector_labels_match(a: str, b: str) -> bool:
    """축약·풀 라벨·부분 포함으로 같은 테마인지."""
    ca = srl._compact_sector_label(a)
    cb = srl._compact_sector_label(b)
    if not ca or not cb:
        return False
    if ca == cb:
        return True
    fa = srl._full_sector_label(a)
    fb = srl._full_sector_label(b)
    if fa and fb and fa == fb:
        return True
    if ca in cb or cb in ca:
        return True
    return False


def _sector_seed_catalyst(
    flow_row: dict[str, Any] | None,
    *,
    theme: str,
    full: str,
    aliases: tuple[str, ...],
) -> str:
    """theme_seed_details / 상위 키워드에서 섹터에 맞는 구체 촉매."""
    if not flow_row or not theme:
        return ""
    for sd in flow_row.get("theme_seed_details") or []:
        if not isinstance(sd, dict):
            continue
        seed = str(sd.get("seed") or "").strip()
        cat = _usable_catalyst(str(sd.get("catalyst") or ""))
        sectors = [str(s) for s in (sd.get("sectors") or [])]
        seed_hit = bool(seed) and (
            seed.lower() in theme.lower()
            or theme.lower() in seed.lower()
            or any(
                seed.lower() in str(a).lower() or str(a).lower() in seed.lower()
                for a in aliases
            )
        )
        sector_hit = any(_sector_labels_match(s, theme) for s in sectors if s)
        if not (seed_hit or sector_hit):
            continue
        if cat and cat.lower() not in (theme.lower(), seed.lower()):
            return cat
        if (
            seed
            and seed.lower() != theme.lower()
            and not _is_useless_reason_phrase(seed)
            and seed.lower() not in _GENERIC_CATALYSTS
        ):
            return seed
    top_kw = [
        str(k).strip()
        for k in (flow_row.get("top_keywords_sample") or [])
        if str(k).strip()
    ]
    related = [
        k
        for k in top_kw
        if k.lower() not in _GENERIC_CATALYSTS
        and not _is_useless_reason_phrase(k)
        and len(k) >= 2
        and (
            any(len(str(a)) >= 2 and str(a).lower() in k.lower() for a in aliases)
            or (theme and theme.lower() in k.lower())
            or _has_catalyst(k)
            # 티커·약어(HBM, FDA) — 소문자 잡토큰(cbsi 등) 제외
            or re.fullmatch(r"[A-Z][A-Z0-9&.\-]{1,24}", k)
        )
    ]
    if related:
        lead = related[0]
        if theme and theme not in lead:
            return f"{theme} 관련 {lead}"
        return lead
    return ""


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
    full, aliases = _resolve_sector_aliases(theme)
    catalyst = ""
    if flow_row and theme:
        for summ in flow_row.get("theme_sector_summaries") or []:
            if not isinstance(summ, dict):
                continue
            sec = str(summ.get("sector") or "")
            if not _sector_labels_match(sec, theme) and not _sector_labels_match(
                sec, full
            ):
                continue
            # 해당 섹터 요약에 붙은 촉매는 이미 섹터 귀속 — 토큰 불일치로 버리지 않음
            raw_cat = _usable_catalyst(str(summ.get("catalyst") or ""))
            if raw_cat and raw_cat.lower() not in (
                theme.lower(),
                (full or "").lower(),
                srl._compact_sector_label(full).lower(),
            ):
                catalyst = raw_cat
            elif raw_cat and _catalyst_relevant_to_sector(
                raw_cat, compact=theme, full=full, aliases=aliases
            ):
                catalyst = raw_cat
            break
        if not catalyst:
            catalyst = _sector_seed_catalyst(
                flow_row, theme=theme, full=full, aliases=aliases
            )
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


def _compose_always_reason(
    *,
    name: str = "",
    theme: str = "",
    catalyst: str = "",
    keywords: list[str] | None = None,
    peers: list[str] | None = None,
    return_pct: float = 0.0,
    is_limit_up: bool = False,
) -> str:
    """
    「원인 미확인」 없이 항상 표시용 근거 문구를 만듭니다.

    뉴스·공시가 없어도 테마·동반·키워드·등락으로 채웁니다.
    """
    peers = [p for p in (peers or []) if str(p).strip()]
    usable_kw = [
        str(k).strip()
        for k in (keywords or [])
        if str(k).strip()
        and str(k).lower() not in (name or "").strip().lower()
        and (name or "").strip().lower() not in str(k).lower()
        and str(k).lower() not in _GENERIC_CATALYSTS
        and str(k) not in _THIN_REASON_TOKENS
        and not _is_useless_reason_phrase(str(k))
    ]
    cat = str(catalyst or "").strip()
    if cat and not _is_thin_reason_text(cat) and not _is_useless_reason_phrase(cat):
        if theme and theme not in cat:
            return f"{theme} — {cat}"
        return cat
    if theme and peers:
        return f"{theme} 동반 급등({', '.join(peers[:3])})"
    if theme and usable_kw:
        return f"{theme} ({', '.join(usable_kw[:3])}) 관련 급등"
    if peers:
        return f"동반 급등({', '.join(peers[:3])})"
    if theme:
        return f"{theme} 관련주 동반 급등"
    if usable_kw:
        return f"{', '.join(usable_kw[:3])} 이슈 급등"
    if is_limit_up or return_pct >= 29:
        return "테마·수급 동반 상한가권 급등"
    if return_pct > 0:
        return f"테마·수급 동반 +{return_pct:.1f}% 급등"
    return "테마·수급 동반 급등"


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
    """뉴스·공시 매칭 실패 시에도 「원인 미확인」 없이 테마·동반·키워드로 채움."""
    text = _compose_always_reason(
        name=name,
        theme=theme,
        catalyst=catalyst,
        keywords=keywords,
        peers=peers,
        return_pct=return_pct,
        is_limit_up=is_limit_up,
    )
    if catalyst and catalyst in text:
        conf, src = 35, "sector_catalyst"
    elif theme and peers and any(p in text for p in peers):
        conf, src = 28, "peers"
    elif keywords and any(str(k) in text for k in keywords):
        conf, src = 24, "keywords"
    elif theme:
        conf, src = 18, "theme_only"
    else:
        conf, src = 12, "market_flow"
    return _ReasonPick(text=text, confidence=conf, source=src)


_DIRECT_REASON_SOURCES = frozenset(
    {
        "actual_news",
        "actual_news_relaxed",
        "pred_news",
        "pred_news_relaxed",
        "disclosure",
        "rise_reason",
        "ctx_direct",
        "early_direct",
        "ctx_title",
    }
)
_THEME_CONTEXT_SOURCES = frozenset(
    {
        "theme_early",
        "theme_actual",
        "sector_catalyst",
        "ctx_match",
        "early_match",
        "keywords",
    }
)
_CONCRETE_REASON_SOURCES = _DIRECT_REASON_SOURCES | _THEME_CONTEXT_SOURCES


def _dedupe_reason_picks(picks: list[_ReasonPick]) -> list[_ReasonPick]:
    """신뢰도 순으로 중복·빈약한 근거를 걸러 유지."""
    ranked = sorted(picks, key=lambda p: p.confidence, reverse=True)
    out: list[_ReasonPick] = []
    for p in ranked:
        if _is_thin_reason_text(p.text) or _is_useless_reason_phrase(p.text):
            continue
        if any(_reason_texts_overlap(p.text, q.text) for q in out):
            continue
        out.append(p)
    return out


def _synthesize_reason_pick(
    pool: list[_ReasonPick],
    *,
    name: str,
    theme: str,
    peers: list[str],
    fallback: _ReasonPick,
) -> _ReasonPick:
    """
    뉴스·공시·테마 맥락을 1~3조각으로 묶어 종합 근거 문구를 만듭니다.

    예: 「종목 뉴스. 반도체 테마: HBM 수요 관련 제목. 동반: A·B」
    """
    usable = _dedupe_reason_picks(pool)
    directs = [p for p in usable if p.source in _DIRECT_REASON_SOURCES]
    themes = [p for p in usable if p.source in _THEME_CONTEXT_SOURCES]
    parts: list[str] = []
    sources: list[str] = []

    if directs:
        best_d = directs[0]
        parts.append(best_d.text.strip())
        sources.append(best_d.source)
        for p in directs[1:]:
            if p.source == "disclosure" and not any(
                _reason_texts_overlap(p.text, x) for x in parts
            ):
                parts.append(f"공시 {p.text.strip()}")
                sources.append(p.source)
                break

    if themes and len(parts) < 3:
        for p in themes:
            if any(_reason_texts_overlap(p.text, x) for x in parts):
                continue
            bit = p.text.strip()
            # 테마 제목에 종목명이 없고 테마 라벨도 없으면 맥락 붙임
            if (
                theme
                and theme not in bit
                and name not in bit
                and p.source in ("theme_early", "theme_actual", "sector_catalyst")
            ):
                bit = f"{theme} 테마: {bit}" if "—" not in bit else bit
            parts.append(bit)
            sources.append(p.source)
            break

    if len(parts) < 2 and theme and peers:
        peer_bit = f"동반 급등 {', '.join(peers[:2])}"
        if not any(peer_bit in x or peers[0] in x for x in parts):
            parts.append(peer_bit)
            sources.append("peers")

    if not parts:
        if fallback.text and "원인 미확인" not in fallback.text and fallback.text.strip():
            if not _is_thin_reason_text(fallback.text):
                return fallback
            # thin 이어도 테마명만 있으면 동반 급등으로 확장
            return _ReasonPick(
                text=_compose_always_reason(
                    name=name,
                    theme=theme or fallback.text,
                    peers=peers,
                    is_limit_up=True,
                ),
                confidence=max(fallback.confidence, 14),
                source=fallback.source or "theme_only",
            )
        return _ReasonPick(
            text=_compose_always_reason(name=name, theme=theme, peers=peers, is_limit_up=True),
            confidence=16,
            source="theme_only",
        )

    text = ". ".join(parts[:3])
    conf = max(p.confidence for p in usable[:3]) if usable else fallback.confidence
    src = "+".join(sources[:3]) if sources else "synth"
    return _ReasonPick(text=text, confidence=min(conf + 8, 95), source=src)


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
    한 종목의 급등 이유를 다단계 탐색한 뒤 뉴스·공시·테마를 종합해 반환.

    actual/pred 뉴스 → 공시 → rise_reason HTML → 직접/매칭 뉴스 → 테마 뉴스 → 폴백.
    """
    keywords = _stock_specific_keywords(code, name, compare_row, early_rows)
    theme, catalyst, aliases = _sector_context(flow_row, code=code, theme_label=theme_label)
    full, resolved_aliases = _resolve_sector_aliases(theme)
    if resolved_aliases:
        aliases = resolved_aliases
    peers = _peer_names(flow_row, code=code, theme=theme)
    fallback = _fallback_reason(
        name=name,
        theme=theme,
        catalyst=catalyst,
        keywords=keywords,
        peers=peers,
        return_pct=return_pct,
        is_limit_up=is_limit_up,
    )
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
        # 두 번째 뉴스 제목도 후보에 넣어 종합 요약에 활용
        for key, src in (
            ("actual_news_hits", "actual_news"),
            ("pred_news_hits", "pred_news"),
        ):
            scored: list[_ReasonPick] = []
            for h in compare_row.get(key) or []:
                if not isinstance(h, dict):
                    continue
                sc = _score_news_hit(h, name=name, keywords=keywords, relaxed=False)
                if sc < 40:
                    continue
                title = str(h.get("title") or "").strip()
                phrase = _extract_issue_from_title(title, name) or _clean_phrase(title)
                if len(phrase) < 8 or _is_thin_reason_text(phrase):
                    continue
                if _is_useless_reason_phrase(phrase, name=name):
                    continue
                scored.append(_ReasonPick(text=phrase, confidence=sc, source=src))
            scored.sort(key=lambda x: x.confidence, reverse=True)
            for p in scored[:2]:
                if not any(_reason_texts_overlap(p.text, x.text) for x in picks):
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

    p = _pick_from_theme_news(
        early_rows, actual_ctx_rows, aliases=aliases, theme=theme
    )
    if p:
        picks.append(p)

    if catalyst and not _is_thin_reason_text(catalyst):
        cat_text = (
            f"{theme} — {catalyst}" if theme and theme not in catalyst else catalyst
        )
        if not _is_thin_reason_text(cat_text):
            picks.append(
                _ReasonPick(text=cat_text, confidence=35, source="sector_catalyst")
            )
    if theme and peers:
        peer_text = f"{theme} 동반 급등({', '.join(peers)} 등)"
        if keywords:
            usable = [
                k
                for k in keywords
                if str(k).strip()
                and str(k).lower() not in name.lower()
                and str(k).lower() not in _GENERIC_CATALYSTS
                and str(k) not in _THIN_REASON_TOKENS
            ][:2]
            if usable:
                peer_text = f"{peer_text}, {', '.join(usable)}"
        picks.append(
            _ReasonPick(text=peer_text, confidence=28, source="peers")
        )

    if not picks:
        return fallback

    filtered: list[_ReasonPick] = []
    weak: list[_ReasonPick] = []
    for p in picks:
        if _is_thin_reason_text(p.text):
            continue
        generic = _is_generic_day_theme_phrase(p.text, aliases=aliases)
        if generic:
            if p.source in (
                "theme_early",
                "theme_actual",
                "sector_catalyst",
                "ctx_match",
                "early_match",
            ):
                weak.append(
                    _ReasonPick(
                        text=p.text,
                        confidence=min(p.confidence, 28),
                        source=p.source,
                    )
                )
                continue
            if not _name_in_title(p.text, name) and name not in p.text:
                continue
        if not _reason_pick_usable(
            p, name=name, theme=theme, full=full, aliases=aliases
        ):
            continue
        filtered.append(p)

    pool = filtered or weak
    if not pool:
        return fallback
    return _synthesize_reason_pick(
        pool, name=name, theme=theme, peers=peers, fallback=fallback
    )


def _ensure_display_reason(
    reason: str,
    *,
    name: str = "",
    is_limit_up: bool = False,
) -> str:
    """표시 직전에 「원인 미확인」·빈 문구를 금지하고 짧은 테마명은 동반 급등으로 확장."""
    r = re.sub(r"\s+", " ", str(reason or "").strip())
    if " — " in r:
        head, tail = r.rsplit(" — ", 1)
        if _is_useless_reason_phrase(tail, name=name) or _is_thin_reason_text(tail):
            r = head.strip()
    # 「미확인」 꼬리만 제거하고 테마·동반 맥락은 유지
    r = re.sub(r"\(?\s*종목\s*직접\s*뉴스[·⋅･]?공시\s*미확인\s*\)?", "", r)
    r = re.sub(r"원인\s*미확인", "", r)
    r = re.sub(r"미확인", "", r)
    r = re.sub(r"\s+", " ", r).strip(" ·,.-")
    r = r.replace("관련주 동반 강세", "관련주 동반 급등")
    if not r or _is_useless_reason_phrase(r, name=name):
        return _compose_always_reason(name=name, is_limit_up=is_limit_up)
    if _is_thin_reason_text(r):
        # 「건설」「핸드셋」처럼 테마명만 남은 경우
        if "동반" not in r and "급등" not in r and "이슈" not in r and "테마" not in r:
            return f"{r} 관련주 동반 급등"
        return r
    return r


def _format_bullet_line(name: str, reason: str, *, is_limit_up: bool) -> str:
    """종목명 + 이유 한 줄 ``<li>`` HTML."""
    r = _ensure_display_reason(
        _shorten(reason.strip(), max_len=_REASON_MAX),
        name=name,
        is_limit_up=is_limit_up,
    )
    if is_limit_up and r and "상한" not in r:
        r = f"{r} · 상한가"
    return (
        f'<li style="margin-bottom:6px;line-height:1.5">'
        f"<strong>{_esc(name)}</strong>: {_esc(r)}</li>"
    )


def _korean_conjunction(word: str) -> str:
    """앞 구절 뒤에 붙일 '과/와' 선택."""
    if not word:
        return "와"
    last = word.rstrip()[-1]
    if ord(last) < 0xAC00 or ord(last) > 0xD7A3:
        return "와"
    jong = (ord(last) - 0xAC00) % 28
    return "과" if jong != 0 else "와"


def _theme_catalyst_for_forward_pred(
    row: dict[str, Any],
    pr: Any,
    *,
    flow_row: dict[str, Any] | None = None,
) -> str:
    """예측 근거 tooltip용 테마·업종 촉매 문구."""
    code = str(row.get("code", "")).zfill(6)
    sectors = [str(s).strip() for s in (row.get("market_theme_sectors") or []) if str(s).strip()]
    theme_label = sectors[0] if sectors else ""
    if not theme_label and flow_row:
        stock_map = flow_row.get("theme_stock_sectors") or {}
        if isinstance(stock_map, dict):
            theme_label = srl._compact_sector_label(str(stock_map.get(code) or ""))

    _, catalyst, _aliases = _sector_context(flow_row, code=code, theme_label=theme_label)
    if catalyst:
        return _shorten(catalyst, max_len=56)
    if len(sectors) >= 2:
        return _shorten(f"{sectors[0]}·{sectors[1]}", max_len=56)
    if sectors:
        return _shorten(sectors[0], max_len=56)
    if theme_label:
        return _shorten(theme_label, max_len=56)

    kws = features.filter_keywords(
        list(getattr(pr, "matched_keywords", None) or row.get("keywords") or [])
    )
    thematic = [k for k in kws if len(k) >= 2 and k not in _GENERIC_MATCH_KW][:3]
    if len(thematic) >= 2:
        return _shorten(f"{thematic[0]}·{thematic[1]}", max_len=56)
    if thematic:
        return thematic[0]
    return ""


def _strip_forward_news_tail(phrase: str, *, pred_pct: float) -> str:
    """예측 전용 문구에서 과거형·등락 꼬리표 제거."""
    t = _shorten(_clean_phrase(phrase), max_len=80)
    for tail in (
        "에 상한가",
        " 상한가",
        " 급등",
        "에 강세",
        " 강세",
        f" 당일 +{pred_pct:.1f}% 급등",
        " 동반 급등",
    ):
        if t.endswith(tail):
            t = t[: -len(tail)].rstrip(" ,·")
    return t


def _forward_news_clause(
    name: str,
    row: dict[str, Any],
    pick: _ReasonPick,
    *,
    pred_pct: float,
) -> str:
    """예측 입력 뉴스에서 '○○○ 소식' 형태 절 추출."""
    hits = list(row.get("pred_news_hits") or [])
    phrase = ""
    if hits:
        kws = list(row.get("keywords") or [])
        best_hit = hits[0]
        best_score = -1
        for h in hits:
            sc = _score_news_hit(h, name=name, keywords=kws)
            if sc > best_score:
                best_score = sc
                best_hit = h
        title = str(best_hit.get("title") or "").strip()
        phrase = _extract_issue_from_title(title, name) or _clean_phrase(title)
    if len(phrase) < 3:
        phrase = pick.text
    phrase = _strip_forward_news_tail(phrase, pred_pct=pred_pct)
    phrase = re.sub(r"\(주\)", "", phrase).strip()
    phrase = re.sub(r"\s*소식\s*$", "", phrase).strip()
    if not phrase:
        return ""
    return f"{phrase} 소식"


def format_forward_pred_reason_narrative_html(
    pr: Any,
    row: dict[str, Any],
    *,
    early_rows: list[tuple[date, dict[str, str]]] | None = None,
    actual_ctx_rows: list[tuple[date, dict[str, str]]] | None = None,
    flow_row: dict[str, Any] | None = None,
) -> str:
    """
    장 미개장 예측 후보 ``예측 근거`` tooltip.

    ``금호전기: ○○○ 소식과, AI인프라·전력수급에 따른 효과로 인한 상승 예상`` 형식.
    급등 확정일 ``_reason_for_stock`` 과 동일한 뉴스·공시 탐색을 재사용합니다.
    """
    name = str(row.get("name") or getattr(pr, "name", "") or "").strip()
    code = str(row.get("code") or getattr(pr, "code", "")).zfill(6)
    sectors = [str(s).strip() for s in (row.get("market_theme_sectors") or []) if str(s).strip()]
    theme_label = sectors[0] if sectors else ""

    pred_pct = float(
        getattr(pr, "predicted_return_pct", 0.0) or row.get("pred_ret") or 0.0
    )
    pick = _reason_for_stock(
        row,
        code=code,
        name=name,
        early_rows=list(early_rows or []),
        actual_ctx_rows=list(actual_ctx_rows or []),
        flow_row=flow_row,
        theme_label=theme_label,
        return_pct=pred_pct,
        is_limit_up=False,
    )
    news_phrase = _forward_news_clause(name, row, pick, pred_pct=pred_pct)
    theme_phrase = _theme_catalyst_for_forward_pred(row, pr, flow_row=flow_row)

    if news_phrase and theme_phrase:
        if theme_phrase in news_phrase or news_phrase in theme_phrase:
            main = (
                f"<strong>{_esc(name)}</strong>: {_esc(news_phrase)}에 따른 상승 예상"
            )
        else:
            conj = _korean_conjunction(news_phrase)
            main = (
                f"<strong>{_esc(name)}</strong>: {_esc(news_phrase)}{conj}, "
                f"{_esc(theme_phrase)}에 따른 효과로 인한 상승 예상"
            )
    elif news_phrase:
        main = f"<strong>{_esc(name)}</strong>: {_esc(news_phrase)}에 따른 상승 예상"
    elif theme_phrase:
        main = (
            f"<strong>{_esc(name)}</strong>: "
            f"{_esc(theme_phrase)} 테마·업종 흐름에 따른 상승 예상"
        )
    else:
        main = (
            f"<strong>{_esc(name)}</strong>: "
            "예측 입력 뉴스·시세에서 뚜렷한 단서는 적으나 모델이 익일 급등 후보로 선정"
        )

    parts = [
        '<div class="forward-pred-narrative" style="font-size:0.86rem;line-height:1.5">',
        f"<p style=\"margin:0\">{main}</p>",
    ]

    hits = list(row.get("pred_news_hits") or [])
    if hits:
        best_hit = hits[0]
        best_score = -1
        kws = list(row.get("keywords") or [])
        for h in hits:
            sc = _score_news_hit(h, name=name, keywords=kws)
            if sc > best_score:
                best_score = sc
                best_hit = h
        title = str(best_hit.get("title") or "").strip()
        if title and title not in news_phrase and _name_in_title(title, name):
            parts.append(
                f'<p class="muted" style="margin:6px 0 0;font-size:0.78rem;line-height:1.4">'
                f"참고 기사: {_esc(_shorten(title, max_len=100))}</p>"
            )

    parts.append("</div>")
    return "".join(parts)


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
    """종목별 급등 이유 평문(테마 리포트 표·불릿 공통) — 뉴스·공시·테마 종합."""
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
    r = _ensure_display_reason(
        _shorten(pick.text.strip(), max_len=_REASON_MAX),
        name=name,
        is_limit_up=is_limit_up,
    )
    # 테마 라벨이 있으면 thin 폴백을 테마 기준으로 다시 채움
    if (
        theme_label
        and (
            r.startswith("테마·수급")
            or "원인 미확인" in r
            or _is_thin_reason_text(pick.text)
        )
    ):
        theme, catalyst, _ = _sector_context(
            flow_row, code=code, theme_label=theme_label
        )
        peers = _peer_names(flow_row, code=code, theme=theme)
        r = _compose_always_reason(
            name=name,
            theme=theme or theme_label,
            catalyst=catalyst,
            keywords=_stock_specific_keywords(code, name, compare_row, early_rows),
            peers=peers,
            return_pct=return_pct,
            is_limit_up=is_limit_up,
        )
        r = _shorten(r, max_len=_REASON_MAX)
    if is_limit_up and r and "상한" not in r:
        r = f"{r} · 상한가"
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

    bullets = [b for b in bullets if b]
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
        "종목 뉴스·공시와 테마 뉴스·섹터 촉매·동반 급등을 묶어 종합 요약합니다.</p>"
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


_GLOBAL_INDEX_SYMBOLS: tuple[tuple[str, str], ...] = (
    ("KS11", "KOSPI"),
    ("IXIC", "나스닥"),
    ("DJI", "다우"),
    ("US500", "S&P500"),
)

_GLOBAL_NEWS_KW: tuple[str, ...] = (
    "나스닥",
    "다우",
    "S&P",
    "미국증시",
    "뉴욕증시",
    "연준",
    "FOMC",
    "글로벌",
    "유가",
    "WTI",
    "금값",
    "환율",
    "달러",
    "중국증시",
    "일본증시",
    "반도체",
    "엔비디아",
    "테슬라",
    "애플",
)

_UNUSUAL_NEWS_KW: tuple[str, ...] = (
    "상한가",
    "급등",
    "수주",
    "계약",
    "유상증자",
    "무상증자",
    "FDA",
    "임상",
    "승인",
    "인수",
    "합병",
    "실적",
    "흑자",
    "적자",
    "공급",
    "수출",
    "관세",
    "제재",
    "소송",
    "거래정지",
    "상장",
)


def _last_index_return_bullet(symbol: str, label: str, asof: date) -> str | None:
    """``asof`` 이전 마지막 거래일 지수 등락률 한 줄."""
    try:
        start = asof - timedelta(days=21)
        df = stocks.load_index_frame(symbol, start, asof)
        if df.empty:
            return None
        s = df.sort_values("Date").reset_index(drop=True)
        s = s[s["Date"] <= pd.Timestamp(asof)]
        if s.empty:
            return None
        last_row = s.iloc[-1]
        last_d = last_row["Date"].date() if hasattr(last_row["Date"], "date") else asof
        r = stocks.index_daily_return_pct(s, last_d)
        if r is None or not math.isfinite(float(r)):
            return None
        pct = float(r) * 100.0
        tone = "상승" if pct > 0.15 else ("하락" if pct < -0.15 else "보합")
        return (
            f"{_esc_nq(label)} ({last_d.isoformat()}) 전일 대비 "
            f"<strong>{pct:+.2f}%</strong> ({tone})"
        )
    except (KeyError, TypeError, ValueError, OSError):
        return None


def _global_headlines_from_rows(
    rows: list[tuple[date, dict[str, str]]] | None,
    *,
    limit: int = 5,
) -> list[str]:
    """예측 입력 구간 뉴스에서 글로벌·거시 이슈 헤드라인."""
    if not rows:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for cal_d, row in reversed(rows):
        title = str(row.get("title") or "").strip()
        if not title or title in seen:
            continue
        blob = f"{title} {row.get('description') or ''}"
        if not any(k in blob for k in _GLOBAL_NEWS_KW):
            continue
        seen.add(title)
        link = str(row.get("link") or "").strip()
        te = _esc_nq(title[:120])
        if link:
            le = html.escape(link, quote=True)
            out.append(
                f'<span class="pill" style="font-size:0.68rem">{cal_d.isoformat()}</span> '
                f'<a href="{le}" target="_blank" rel="noopener">{te}</a>'
            )
        else:
            out.append(
                f'<span class="pill" style="font-size:0.68rem">{cal_d.isoformat()}</span> {te}'
            )
        if len(out) >= limit:
            break
    return out


def build_forward_day_market_context_html(
    t_trading_day: date,
    *,
    kospi_return: float | None = None,
    early_rows: list[tuple[date, dict[str, str]]] | None = None,
) -> str:
    """
    예측 전용 관측일: 장 시작 전 시장·글로벌 흐름 요약(일자 패널 상단).
    """
    asof = trading_calendar.last_trading_day_before(t_trading_day)
    bullets: list[str] = []
    if kospi_return is not None and math.isfinite(float(kospi_return)):
        pct = float(kospi_return) * 100.0
        tone = "상승" if pct > 0.15 else ("하락" if pct < -0.15 else "보합")
        bullets.append(
            f"예측 입력 기준 직전 거래일({asof.isoformat()}) KOSPI 전일 대비 "
            f"<strong>{pct:+.2f}%</strong> ({tone})"
        )
    for sym, lbl in _GLOBAL_INDEX_SYMBOLS:
        if sym == "KS11":
            continue
        b = _last_index_return_bullet(sym, lbl, asof)
        if b:
            bullets.append(b)
    chunks: list[str] = []
    if bullets:
        chunks.append(_section("시장·세계 지수 (예측 입력 시점)", bullets))
    gnews = _global_headlines_from_rows(early_rows, limit=5)
    if gnews:
        chunks.append(
            "<p><strong>글로벌·거시 뉴스 흐름</strong> "
            "(예측 입력 구간 헤드라인)</p>"
            f'<ul class="nl">{"".join(f"<li>{x}</li>" for x in gnews)}</ul>'
        )
    if not chunks:
        return ""
    return (
        '<div class="forward-day-market-ctx" style="margin:0 0 14px;padding:10px 12px;'
        'background:#152232;border:1px solid #2a4a6a;border-radius:8px;font-size:0.86rem">'
        + "".join(chunks)
        + "</div>"
    )


def build_forward_pred_context_html(
    *,
    code: str,
    name: str,
    t_trading_day: date,
    kospi_return: float | None = None,
    pred_news_hits: list[dict] | None = None,
    actual_ctx_rows: list[tuple[date, dict[str, str]]] | None = None,
    early_rows: list[tuple[date, dict[str, str]]] | None = None,
    disclosure_hits: list[dict] | None = None,
    keywords: list[str] | None = None,
    returns_sub: pd.DataFrame | None = None,
    returns_ml_sub: pd.DataFrame | None = None,
    market_theme_sectors: list[str] | None = None,
    market_segment: str | None = None,
) -> str:
    """
    장 시작 전 예측 후보: 종목 시세·업종·수급 뉴스·특이 이슈·공시 맥락 HTML.
    """
    code = str(code).zfill(6)
    asof = trading_calendar.last_trading_day_before(t_trading_day)
    chunks: list[str] = []

    trait_bits: list[str] = []
    if market_segment and market_segment not in ("other", ""):
        seg = "KOSPI" if market_segment == "kospi" else (
            "KOSDAQ" if market_segment == "kosdaq" else market_segment.upper()
        )
        trait_bits.append(f"시장 구분: <strong>{_esc_nq(seg)}</strong>")
    try:
        ind = stocks.industry_name_for_code(code)
        if ind:
            trait_bits.append(f"업종: <strong>{_esc_nq(ind)}</strong>")
    except (OSError, ValueError, TypeError):
        pass
    sectors = [str(s).strip() for s in (market_theme_sectors or []) if str(s).strip()]
    if sectors:
        trait_bits.append(
            "당일 early 뉴스 테마 연관: "
            + " · ".join(f'<span class="pill" style="font-size:0.72rem">{_esc_nq(s)}</span>' for s in sectors[:4])
        )
    if trait_bits:
        chunks.append(_section("종목·섹터 특성", trait_bits))

    tech = _technical_bullets(
        code,
        asof,
        None,
        None,
        returns_sub=returns_sub,
        returns_ml_sub=returns_ml_sub,
    )
    if tech and tech[0].startswith(f"{asof.isoformat()}"):
        tech[0] = (
            f"예측 기준 마지막 거래일({asof.isoformat()}) 종가 기준 "
            + tech[0].split("종가 기준 ", 1)[-1]
            if "종가 기준" in tech[0]
            else tech[0]
        )
    chunks.append(_section(f"종목 시세·추세 (마지막 확정 일봉 {asof.isoformat()})", tech))

    mkt = _market_bullet(kospi_return, asof)
    if mkt:
        mkt = mkt.replace("같은 날", f"직전 거래일({asof.isoformat()})")
        chunks.append(_section("국내 시장", [mkt]))

    merged_hits = _merge_rise_news_hits(pred_news_hits, None, limit=12)
    ctx_rows = list(actual_ctx_rows or []) or list(early_rows or [])
    clips = _news_clips_from_hits(merged_hits, limit=10)
    extra = _scan_context_news(
        ctx_rows,
        stock_name=name,
        keywords=list(keywords or []),
        limit=8,
    )
    seen_t = {c.title for c in clips}
    for c in extra:
        if c.title not in seen_t:
            clips.append(c)
            seen_t.add(c.title)
    unusual: list[str] = []
    for c in clips:
        blob = f"{c.title} {c.description}"
        if any(k in blob for k in _UNUSUAL_NEWS_KW) or (name and name in blob):
            unusual.append(_clip_li(c))
    if unusual:
        chunks.append(
            "<p><strong>종목 특이 뉴스·이슈</strong></p>"
            f'<ul class="nl">{"".join(unusual[:6])}</ul>'
        )
    if clips:
        chunks.append(
            _section_news(
                "종목 관련 뉴스 (예측 입력 구간)",
                clips[:8],
                empty="종목명·키워드가 겹친 기사를 찾지 못했습니다.",
            )
        )

    dh = list(disclosure_hits or [])
    if dh:
        disc_parts = ["<p><strong>공시·IR</strong></p><ul class=\"nl\">"]
        for d in dh[:5]:
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

    if not chunks:
        return ""
    return (
        '<div class="forward-pred-context" style="margin-top:10px;padding-top:10px;'
        'border-top:1px solid #2a3f5c;font-size:0.84rem;line-height:1.5">'
        + "".join(chunks)
        + "</div>"
    )


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
        elif t_trading_day is not None and trading_calendar.is_trading_day(t_trading_day):
            if trading_calendar.trading_day_actuals_finalized(t_trading_day):
                chunks.append(
                    f"<p class=\"combo-tip-empty\"><strong>참고</strong> — "
                    f"{tlabel} 종가 실적은 비교표·일봉에 반영됩니다. "
                    "이 패널은 종목 맥락 참고용입니다.</p>"
                )
            else:
                asof = trading_calendar.last_trading_day_before(t_trading_day)
                chunks.append(
                    "<p><strong>예측 전용</strong> — "
                    f"{tlabel} 장 마감 전·실적 미확정. "
                    f"시세·뉴스는 <strong>{asof.isoformat()}</strong>까지 확정된 일봉·"
                    "예측 입력 뉴스 구간 기준입니다.</p>"
                )
                chunks.append(
                    build_forward_pred_context_html(
                        code=code,
                        name=name,
                        t_trading_day=t_trading_day,
                        kospi_return=kospi_return,
                        pred_news_hits=pred_news_hits,
                        actual_ctx_rows=actual_ctx_rows,
                        disclosure_hits=disclosure_hits,
                        keywords=keywords,
                        returns_sub=returns_sub,
                        returns_ml_sub=returns_ml_sub,
                    )
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
_THEME_REASON_MAX = 240
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
    market_theme_html = srl.format_market_theme_flow_html(
        flow_row,
        news_cutoff_label=news_cutoff_label,
        threshold_pct=float(config.BIG_MOVE_THRESHOLD) * 100.0,
    )
    if dr.rows_compare:
        srl._patch_flow_row_strong_movers_from_compare(
            flow_row,
            dr.rows_compare,
            flow_row.get("_listing_names") or {},
            early_rows,
        )

    leaders = list(
        flow_row.get("theme_leaders_ge_20pct")
        or flow_row.get("theme_leaders_ge_25pct")
        or []
    )
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
        stocks.sort(
            key=lambda s: (
                -float(s.get("return_pct") or 0),
                str(s.get("name") or ""),
                str(s.get("code") or "").zfill(6),
            )
        )
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
        "market_theme_html": market_theme_html,
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


_THEME_DAY_META_RE = re.compile(
    r'(<p class="day-meta"[^>]*>.*?</p>)',
    re.DOTALL | re.IGNORECASE,
)


def inject_market_theme_into_theme_day_section(
    section_html: str, theme_inner_html: str
) -> str:
    """테마 25%↑ 일자 ``<section>`` 에 ``당일 테마 요약`` 블록을 삽입하거나 교체합니다."""
    from . import render as report_render

    block = report_render.build_market_theme_ref_block(theme_inner_html)
    if not block:
        return section_html
    replaced = report_render._replace_balanced_div(
        section_html, "market-theme-ref", block
    )
    if replaced is not None:
        return replaced
    m = _THEME_DAY_META_RE.search(section_html)
    if m:
        pos = m.end()
        return section_html[:pos] + "\n    " + block + section_html[pos:]
    m2 = re.search(r"</h2>", section_html, re.IGNORECASE)
    if m2:
        pos = m2.end()
        return section_html[:pos] + "\n    " + block + section_html[pos:]
    return section_html


def backfill_preserved_theme_day_sections_market_theme(
    preserved_day_html: dict[date, str],
    *,
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: Any,
    listing_names: dict[str, str],
    news_cutoff_label: str,
    now_kst: datetime | None = None,
) -> dict[date, str]:
    """병합 유지 테마 리포트 일자 HTML에 ``당일 테마 요약`` 을 채웁니다."""
    from . import render as report_render

    if not preserved_day_html or returns_df is None:
        return preserved_day_html
    now = now_kst or datetime.now(trading_calendar.KST)
    out = dict(preserved_day_html)
    patched = 0
    for t_day in sorted(preserved_day_html.keys()):
        html = preserved_day_html[t_day]
        if not report_render.preserved_day_section_needs_market_theme_backfill(
            html, t_day, now_kst=now
        ):
            continue
        theme_inner = srl.market_theme_html_for_trading_day(
            t_day,
            news_by_calendar,
            returns_df,
            listing_names,
            news_cutoff_label=news_cutoff_label,
        )
        if srl.market_theme_html_is_incomplete(theme_inner):
            continue
        out[t_day] = inject_market_theme_into_theme_day_section(html, theme_inner)
        patched += 1
    if patched:
        print(
            f"테마 리포트 병합: 장 마감 후 당일 테마 요약 보강 {patched}일(기존 HTML 유지 일자)",
            flush=True,
        )
    return out


_THEME_DAY_SECTION_TEMPLATE = r"""
  <section class="day" id="day-{{ day.trading_day.isoformat() }}">
    <h2>{{ day.trading_day.isoformat() }}</h2>
    <p class="day-meta">early {{ day.news_prev_day }} {{ day.news_cutoff_label }}</p>
    {% if day.overview_html %}{{ day.overview_html | safe }}{% endif %}
    {% if day.market_theme_html %}
    <div class="market-theme-ref" style="margin:12px 0 16px;padding:12px 14px;background:#152232;border:1px solid #2a4a6a;border-radius:8px">
      <h3 class="market-theme-h" style="font-size:0.95rem;color:var(--ok);margin:0 0 8px">당일 테마 요약</h3>
      {{ day.market_theme_html | safe }}
    </div>
    {% endif %}
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
      <colgroup>
        <col class="col-stock"/><col class="col-theme"/><col class="col-market"/>
        <col class="col-chg"/><col class="col-reason"/>
      </colgroup>
      <thead><tr>
        <th class="col-stock">종목</th><th class="col-theme">테마</th><th class="col-market">시장</th>
        <th class="col-chg">등락</th><th class="col-reason">근거 요약</th>
      </tr></thead>
      <tbody>
      {% for st in sec.stocks %}
        <tr>
          <td class="col-stock"><strong>{{ st.name }}</strong> <code>{{ st.code }}</code></td>
          <td class="col-theme theme">{{ st.theme }}</td>
          <td class="col-market market">{{ st.market }}</td>
          <td class="col-chg ok">{{ "%+.1f"|format(st.return_pct) }}%{% if st.is_limit_up %} <span class="warn">상한</span>{% endif %}</td>
          <td class="col-reason reason muted">{{ st.reason | safe }}</td>
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
    .market-theme-ref { margin: 12px 0 16px; padding: 12px 14px;
      background: #152232; border: 1px solid #2a4a6a; border-radius: 8px; }
    .market-theme-h { font-size: 0.95rem; color: var(--ok); margin: 0 0 8px; }
    .mover-rationale-h { font-size: 0.92rem; color: var(--ok); margin: 0 0 8px; }
    .mover-rationale-block ul { margin: 0; padding-left: 18px; }
    .mover-rationale-block li { margin-bottom: 6px; line-height: 1.5; }
    table.stocks { width: 100%; table-layout: fixed; border-collapse: collapse;
      font-size: 0.8rem; margin-bottom: 10px; }
    table.stocks th { text-align: left; color: var(--muted); font-weight: 600;
                      border-bottom: 1px solid #2a3f5c; padding: 6px 8px; vertical-align: bottom; }
    table.stocks td { padding: 6px 8px; border-bottom: 1px solid #1e2a3a; vertical-align: top; }
    table.stocks th.col-stock, table.stocks td.col-stock { width: 19%; }
    table.stocks th.col-theme, table.stocks td.col-theme { width: 14%; }
    table.stocks th.col-market, table.stocks td.col-market { width: 8%; }
    table.stocks th.col-chg, table.stocks td.col-chg { width: 11%; text-align: right;
      white-space: nowrap; font-variant-numeric: tabular-nums; }
    table.stocks th.col-reason, table.stocks td.col-reason { width: 48%; }
  /* 병합·구버전 일자 블록(클래스 없음) 호환 */
    table.stocks th:nth-child(1), table.stocks td:nth-child(1) { width: 19%; }
    table.stocks th:nth-child(2), table.stocks td:nth-child(2) { width: 14%; }
    table.stocks th:nth-child(3), table.stocks td:nth-child(3) { width: 8%; }
    table.stocks th:nth-child(4), table.stocks td:nth-child(4) { width: 11%;
      text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }
    table.stocks th:nth-child(5), table.stocks td:nth-child(5) { width: 48%; }
    table.stocks td.col-stock { line-height: 1.35; }
    table.stocks td.col-stock code { display: inline-block; margin-left: 4px;
      font-variant-numeric: tabular-nums; }
    table.stocks td.reason, table.stocks td.col-reason { font-size: 0.78rem; line-height: 1.45;
      word-break: keep-all; overflow-wrap: break-word; }
    table.stocks td.theme, table.stocks td.col-theme { color: var(--accent); white-space: nowrap;
      overflow: hidden; text-overflow: ellipsis; }
    table.stocks td.market, table.stocks td.col-market { white-space: nowrap; font-size: 0.75rem;
      color: var(--muted); }
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
    """기존 ``report_theme_*pct_*.html`` 에서 ``id=\"day-YYYY-MM-DD\"`` 일자 블록을 추출합니다."""
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
    allow_empty_shell: bool = False,
) -> Path | None:
    """급등 테마 리포트 HTML 파일을 ``out_path`` 에 저장. 섹션 없으면 ``None``."""
    days = build_strong_mover_report_days(
        day_reports,
        theme_flow_rows,
        news_by_calendar=news_by_calendar,
        listing_names=listing_names,
        news_cutoff_label=news_cutoff_label,
    )
    preserved = preserved_day_html or {}
    if not days and not preserved and not allow_empty_shell:
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


def theme_report_filename_for_range(d_from: date, d_to: date) -> str:
    """구간 테마 리포트 파일명(동월이면 월간, 아니면 From_To)."""
    tag = THEME_REPORT_FILENAME_PCT_TAG
    if d_from.year == d_to.year and d_from.month == d_to.month:
        return theme_report_filename_for_month(d_from.year, d_from.month)
    return (
        f"report_theme_{tag}_{d_from.strftime('%Y%m%d')}_{d_to.strftime('%Y%m%d')}.html"
    )