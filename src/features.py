"""
뉴스 텍스트·종목명 기반 특징과 과거 급등–뉴스 이벤트 구축.

훈련 구간에서 「당일 수익률이 임계 이상인 날」마다, **해당 종목이 직접 언급된 뉴스**에서
키워드 집합을 뽑아 ``BreakoutEvent`` 로 저장합니다. 예측 단계에서는 비범용 키워드 교집합과
종목명 언급으로 스코어를 냅니다.
"""
from __future__ import annotations

import html
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd

from . import config
from . import news

# 한글 2글자 이상, 영단어 3글자 이상
_TOKEN = re.compile(r"[가-힣]{2,}|[A-Za-z][A-Za-z0-9]{2,}")

# HTML 엔티티(&quot; 등)가 디코딩·토큰화되며 생기는 잡음
_HTML_ENTITY_WORD_STOP = frozenset(
    {
        "quot",
        "amp",
        "nbsp",
        "lt",
        "gt",
        "apos",
        "hellip",
        "copy",
        "reg",
        "deg",
        "yen",
        "euro",
        "cent",
        "para",
        "middot",
        "bull",
        "frasl",
        "ndash",
        "mdash",
    }
)

_STRIP_EDGE_PUNCT = re.compile(r'^["\'“”‘’`]+|["\'“”‘’`]+$')

_KEYWORD_STOP = frozenset(
    {
        "있다",
        "없다",
        "오늘",
        "내일",
        "기자",
        "뉴스",
        "이날",
        "당일",
        "관련",
        "대한",
        "통해",
        "위해",
        "이번",
        "지난",
        "것으로",
        "합니다",
        "있습니다",
    }
)

# 시장 전체 뉴스에 매일 등장하는 범용어 — 교집합·프로필 점수에서 제외
_GENERIC_MARKET_KEYWORDS = frozenset(
    {
        "코스피",
        "코스닥",
        "증시",
        "주식",
        "시장",
        "상승",
        "하락",
        "반등",
        "조정",
        "외국인",
        "기관",
        "개인",
        "순매수",
        "순매도",
        "금리",
        "환율",
        "달러",
        "원화",
        "나스닥",
        "다우",
        "연준",
        "지수",
        "종합",
        "상장",
        "거래",
        "거래량",
        "시총",
        "투자",
        "매수",
        "매도",
        "급등",
        "급락",
        "상한가",
        "하한가",
        "테마",
        "섹터",
        "장중",
        "장마감",
        "마감",
        "개장",
        "etf",
        "fed",
        "fomc",
        "cpi",
        "gdp",
        "실적",
        "전망",
        "분석",
        "리포트",
        "목표가",
        "추천",
        "관심",
        "주목",
        "강세",
        "약세",
        "보합",
        "출발",
        "마무리",
        "고속",
        "여행",
        "관광",
        "휴가",
        "연휴",
        "면세",
        "유가",
        "국제유가",
        "버스",
        "철도",
        "항공",
    }
)

# 종목명만으로 프로필을 만들 때 오탐이 잦은 짧은·모호 토큰
_AMBIGUOUS_NAME_TOKENS = frozenset({"동양", "대우", "한국", "삼화", "동방", "서부", "남부", "북부"})

# 종목명 단독 프로필 허용 최소 글자 수(한글 기준)
_MIN_NAME_ONLY_PROFILE_LEN = 4


def is_generic_market_keyword(k: str) -> bool:
    """증시·시장 전체 뉴스에 흔한 범용 키워드인지."""
    t = clean_keyword_token(k).lower()
    return t in _GENERIC_MARKET_KEYWORDS


def filter_specific_keywords(keywords: list[str] | frozenset[str] | set[str]) -> frozenset[str]:
    """범용 시장 키워드를 제외한 교집합·프로필용 집합."""
    out: set[str] = set()
    for k in keywords:
        t = clean_keyword_token(k)
        if not is_valid_keyword(t):
            continue
        if is_generic_market_keyword(t):
            continue
        out.add(t)
    return frozenset(out)


def normalize_text_for_keywords(text: str) -> str:
    """
    뉴스·키워드 추출 전 HTML 태그·엔티티를 정리합니다.

    RSS/네이버 요약에 ``&quot;`` 등이 남으면 토큰 ``quot`` 가 생기므로 먼저 풀어 씁니다.
    """
    if not text:
        return ""
    s = html.unescape(str(text))
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&(?:#x?[0-9a-fA-F]+|[a-zA-Z]+);", " ", s)
    s = re.sub(r"&[a-zA-Z0-9#]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_keyword_token(tok: str) -> str:
    """토큰 양끝 따옴표·백틱 제거."""
    return _STRIP_EDGE_PUNCT.sub("", str(tok).strip())


def is_valid_keyword(k: str) -> bool:
    """리포트·교집합에 노출할 키워드인지(HTML 잡음·불용어 제외)."""
    t = clean_keyword_token(k).lower()
    if len(t) < 2:
        return False
    if t in _KEYWORD_STOP or t in _HTML_ENTITY_WORD_STOP:
        return False
    return True


def filter_keywords(keywords: list[str] | frozenset[str]) -> list[str]:
    """표시·매칭용 키워드 목록에서 잡음을 제거합니다."""
    out: list[str] = []
    seen: set[str] = set()
    for k in keywords:
        t = clean_keyword_token(k)
        if not is_valid_keyword(t):
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


def tokenize(text: str) -> list[str]:
    """
    한글 2글자 이상·영문 3글자 이상 토큰만 정규식으로 추출합니다(소문자화).

    뉴스 본문에서 키워드 후보를 만드는 1차 단계입니다.
    """
    text = normalize_text_for_keywords(text)
    out: list[str] = []
    for raw in _TOKEN.findall(text.lower()):
        t = clean_keyword_token(raw)
        if len(t) < 2:
            continue
        if t in _KEYWORD_STOP or t in _HTML_ENTITY_WORD_STOP:
            continue
        out.append(t)
    return out


def top_keywords(text: str, k: int = 80) -> list[str]:
    """
    불용어를 제거한 뒤 빈도 상위 ``k`` 개 키워드를 문자열 리스트로 반환합니다.

    리포트 하이라이트·요약용으로 쓰입니다.
    """
    toks = tokenize(text)
    toks = [t for t in toks if is_valid_keyword(t)]
    ctr = Counter(toks)
    return [w for w, _ in ctr.most_common(k)]


def keyword_set(text: str, k: int = 80) -> frozenset[str]:
    """
    ``top_keywords`` 결과를 ``frozenset`` 으로 고정해 집합 연산(교집합·유니온)에 씁니다.
    """
    return frozenset(top_keywords(text, k))


@dataclass
class BreakoutEvent:
    """과거 거래일 급등 라벨 1건: 해당일 수익률·종목 관련 뉴스에서 뽑은 키워드."""

    trading_day: date
    code: str
    name: str
    return_pct: float
    news_keywords: frozenset[str]
    news_snippets: list[str] = field(default_factory=list)


def _news_row_text(row: dict[str, str]) -> str:
    """뉴스 row 제목+본문 한 줄 blob."""
    return f"{row.get('title', '')} {row.get('description', '')}".strip()


def _collect_early_news_rows(
    news_by_calendar: dict[date, list[dict[str, str]]],
    trading_day: date,
    news_window_fn,
) -> list[tuple[date, dict[str, str]]]:
    """거래일 ``trading_day`` 예측 입력(early) 뉴스 row 목록."""
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        early_rows, _ = news.classified_rows_for_target(news_by_calendar, trading_day)
        return list(early_rows)
    cal_start, cal_end = news_window_fn(trading_day)
    out: list[tuple[date, dict[str, str]]] = []
    cd = cal_start
    while cd <= cal_end:
        for row in news_by_calendar.get(cd, []):
            out.append((cd, row))
        cd += timedelta(days=1)
    return out


def _stock_relevant_news_rows(
    early_rows: list[tuple[date, dict[str, str]]],
    code: str,
    name: str,
) -> list[tuple[date, dict[str, str]]]:
    """종목명·코드가 본문에 등장하는 뉴스만 남깁니다."""
    code_z = str(code).zfill(6)
    norm_name = normalize_text_for_keywords(name) if name else ""
    out: list[tuple[date, dict[str, str]]] = []
    for d, row in early_rows:
        text = normalize_text_for_keywords(_news_row_text(row))
        if norm_name and norm_name in text:
            out.append((d, row))
            continue
        compact = text.replace(" ", "")
        if code_z in compact:
            out.append((d, row))
    return out


def breakout_keywords_for_stock(
    code: str,
    name: str,
    early_rows: list[tuple[date, dict[str, str]]],
) -> tuple[frozenset[str], list[str]]:
    """
    급등 이벤트용 종목별 키워드 프로필.

    - 우선: 해당 종목이 직접 언급된 뉴스에서만 키워드 추출
    - 없으면: 4글자 이상 종목명 토큰만(시장 전체 blob 사용 금지)
    - 범용 시장 키워드는 항상 제거
    """
    stock_rows = _stock_relevant_news_rows(early_rows, code, name)
    if stock_rows:
        blob = "\n".join(_news_row_text(r) for _, r in stock_rows)
        kw = filter_specific_keywords(keyword_set(blob, k=60))
        snippets = [_news_row_text(r)[:120] for _, r in stock_rows[:15]]
        return kw, snippets

    name_clean = (name or "").strip()
    if len(name_clean) >= _MIN_NAME_ONLY_PROFILE_LEN:
        kw = filter_specific_keywords(
            frozenset(
                t
                for t in tokenize(name_clean)
                if is_valid_keyword(t) and t not in _AMBIGUOUS_NAME_TOKENS
            )
        )
        if kw:
            return kw, []
    return frozenset(), []


def build_breakout_events(
    returns_df: pd.DataFrame,
    news_by_calendar: dict[date, list[dict[str, str]]],
    news_window_fn,
    threshold: float,
) -> list[BreakoutEvent]:
    """
    수익률 표와 일자별 뉴스로부터 급등 이벤트 목록을 만듭니다.

    각 거래일 ``d`` 의 급등 종목마다, **그 종목이 직접 언급된 뉴스**에서만 키워드 프로필을 만듭니다.
    (시장 전체 뉴스 blob을 모든 급등 종목에 공유하던 이전 방식은 오탐 원인이어서 폐기.)

    Args:
        returns_df: ``daily_returns_table`` 결과(``Date``, ``Code``, ``return_pct`` 등).
        news_by_calendar: 캘린더일 → 뉴스 row dict 리스트.
        news_window_fn: 컷오프가 꺼져 있을 때 ``(start_cal, end_cal)`` 뉴스 윈도우를 주는 함수.
            ``trading_calendar.news_window_for_target_trading_day`` 가 일반적.
        threshold: 급등 정의(소수, 예 ``0.2``).

    Returns:
        ``BreakoutEvent`` 리스트(거래일 오름차순이 아닐 수 있음; 호출 측에서 필터).

    Note:
        ``USE_DECISION_NEWS_INTRADAY_CUTOFF`` 가 켜져 있으면 ``news.aggregate_early_late_for_target``
        와 동일한 early blob만 사용합니다.
    """
    events: list[BreakoutEvent] = []
    r = returns_df
    for d in sorted(r["Date"].dt.date.unique()):
        movers = r[(r["Date"] == pd.Timestamp(d)) & (r["return_pct"] >= threshold)]
        if movers.empty:
            continue
        early_rows = _collect_early_news_rows(news_by_calendar, d, news_window_fn)
        for _, row in movers.iterrows():
            code = str(row["Code"]).zfill(6)
            name = str(row["Name"])
            kw, snippets = breakout_keywords_for_stock(code, name, early_rows)
            events.append(
                BreakoutEvent(
                    trading_day=d,
                    code=code,
                    name=name,
                    return_pct=float(row["return_pct"]),
                    news_keywords=kw,
                    news_snippets=snippets,
                )
            )
    return events


def build_breakout_events_for_trading_days(
    returns_df: pd.DataFrame,
    news_by_calendar: dict[date, list[dict[str, str]]],
    news_window_fn,
    threshold: float,
    trading_days: set[date],
) -> list[BreakoutEvent]:
    """
    ``build_breakout_events`` 와 동일 규칙이되, 지정한 **거래일** 집합에 대해서만 이벤트를 만듭니다.

    학습 스냅샷 증분 갱신(뉴스 캘린더 일자 일부만 새로 반영) 시 사용합니다.
    """
    events: list[BreakoutEvent] = []
    r = returns_df
    for d in sorted(trading_days):
        movers = r[(r["Date"] == pd.Timestamp(d)) & (r["return_pct"] >= threshold)]
        if movers.empty:
            continue
        early_rows = _collect_early_news_rows(news_by_calendar, d, news_window_fn)
        for _, row in movers.iterrows():
            code = str(row["Code"]).zfill(6)
            name = str(row["Name"])
            kw, snippets = breakout_keywords_for_stock(code, name, early_rows)
            events.append(
                BreakoutEvent(
                    trading_day=d,
                    code=code,
                    name=name,
                    return_pct=float(row["return_pct"]),
                    news_keywords=kw,
                    news_snippets=snippets,
                )
            )
    return events


def keyword_overlap_score(kw_news: frozenset[str], events: list[BreakoutEvent], code: str) -> float:
    """
    현재 뉴스 키워드 집합과, 과거 해당 ``code`` 의 급등 이벤트들에 붙은 키워드 집합의
    Jaccard 유사도를 이벤트별로 평균한 점수(0~1 근방)를 반환합니다.

    (현재 파이프라인에서는 ``predict`` 쪽 규칙이 우선이며, 확장·실험용으로 남겨 둘 수 있습니다.)
    """
    rel = [e for e in events if e.code == code]
    if not rel:
        return 0.0
    score = 0.0
    spec_news = filter_specific_keywords(kw_news)
    for e in rel:
        inter = len(spec_news & filter_specific_keywords(e.news_keywords))
        union = len(spec_news | filter_specific_keywords(e.news_keywords)) or 1
        score += inter / union
    return score / len(rel)


def name_mention_score(text_blob: str, name: str) -> float:
    """
    ``text_blob`` 안에 상장 종목명 ``name`` 이 몇 번 나오는지 세어 0~1로 캡합니다.

    최대 5회 이상이면 1.0. 예측 시 종목명 직접 거론을 가중치로 반영합니다.
    """
    if not name:
        return 0.0
    blob = normalize_text_for_keywords(text_blob)
    if name not in blob:
        return 0.0
    return min(1.0, blob.count(name) / 5.0)


def highlight_terms(text: str, terms: list[str]) -> str:
    """
    텍스트를 HTML 이스케이프한 뒤, ``terms`` 를 긴 순으로 ``<mark>`` 로 감쌉니다.

    Jinja 리포트에서 뉴스 스니펫 강조에 사용합니다(대소문자 무시 매칭).
    """
    import html

    s = html.escape(text)
    for t in sorted(set(terms), key=len, reverse=True):
        if len(t) < 2:
            continue
        pat = re.compile(re.escape(html.escape(t)), re.IGNORECASE)
        s = pat.sub(lambda m: f"<mark>{m.group(0)}</mark>", s)
    return s
