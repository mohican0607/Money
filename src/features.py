"""
뉴스 텍스트·종목명 기반 특징과 과거 급등–뉴스 이벤트 구축.

훈련 구간에서 「당일 수익률이 임계 이상인 날」마다, 그날(또는 컷오프 반영) 뉴스 blob에서
키워드 집합을 뽑아 ``BreakoutEvent`` 로 저장합니다. 예측 단계에서는 이 이벤트와
당일 뉴스 키워드의 겹침으로 스코어를 냅니다.
"""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd

from . import config, news

# 한글 2글자 이상, 영단어 3글자 이상
_TOKEN = re.compile(r"[가-힣]{2,}|[A-Za-z][A-Za-z0-9]{2,}")


def tokenize(text: str) -> list[str]:
    """
    한글 2글자 이상·영문 3글자 이상 토큰만 정규식으로 추출합니다(소문자화).

    뉴스 본문에서 키워드 후보를 만드는 1차 단계입니다.
    """
    return _TOKEN.findall(text.lower())


def top_keywords(text: str, k: int = 80) -> list[str]:
    """
    불용어를 제거한 뒤 빈도 상위 ``k`` 개 키워드를 문자열 리스트로 반환합니다.

    리포트 하이라이트·요약용으로 쓰입니다.
    """
    toks = tokenize(text)
    stop = {
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
    toks = [t for t in toks if t not in stop and len(t) > 1]
    ctr = Counter(toks)
    return [w for w, _ in ctr.most_common(k)]


def keyword_set(text: str, k: int = 80) -> frozenset[str]:
    """
    ``top_keywords`` 결과를 ``frozenset`` 으로 고정해 집합 연산(교집합·유니온)에 씁니다.
    """
    return frozenset(top_keywords(text, k))


@dataclass
class BreakoutEvent:
    """훈련 구간의 한 건: 특정 거래일에 임계 이상 급등한 종목과 그날 뉴스 키워드 프로필."""

    trading_day: date
    code: str
    name: str
    return_pct: float
    news_keywords: frozenset[str]
    news_snippets: list[str] = field(default_factory=list)


def build_breakout_events(
    returns_df: pd.DataFrame,
    news_by_calendar: dict[date, list[dict[str, str]]],
    news_window_fn,
    threshold: float,
) -> list[BreakoutEvent]:
    """
    수익률 표와 일자별 뉴스로부터 급등 이벤트 목록을 만듭니다.

    각 캘린더상의 거래일 ``d`` 에 대해 ``return_pct >= threshold`` 인 모든 종목을 찾고,
    그날 예측에 쓸 뉴스 텍스트 blob을 만든 뒤 ``keyword_set(blob)`` 을 이벤트에 붙입니다.

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
        if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
            blob, _ = news.aggregate_early_late_for_target(news_by_calendar, d)
            snippets = []
            for line in blob.split("\n")[:15]:
                snippets.append(line[:120])
        else:
            cal_start, cal_end = news_window_fn(d)
            texts: list[str] = []
            snippets = []
            cd = cal_start
            while cd <= cal_end:
                rows = news_by_calendar.get(cd, [])
                for row in rows:
                    t = f"{row.get('title', '')} {row.get('description', '')}"
                    texts.append(t)
                    snippets.append(row.get("title", "")[:120])
                cd += timedelta(days=1)
            blob = "\n".join(texts)
        kw = keyword_set(blob)
        for _, row in movers.iterrows():
            events.append(
                BreakoutEvent(
                    trading_day=d,
                    code=str(row["Code"]).zfill(6),
                    name=str(row["Name"]),
                    return_pct=float(row["return_pct"]),
                    news_keywords=kw,
                    news_snippets=snippets[:15],
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
        if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
            blob, _ = news.aggregate_early_late_for_target(news_by_calendar, d)
            snippets = []
            for line in blob.split("\n")[:15]:
                snippets.append(line[:120])
        else:
            cal_start, cal_end = news_window_fn(d)
            texts: list[str] = []
            snippets = []
            cd = cal_start
            while cd <= cal_end:
                rows = news_by_calendar.get(cd, [])
                for row in rows:
                    t = f"{row.get('title', '')} {row.get('description', '')}"
                    texts.append(t)
                    snippets.append(row.get("title", "")[:120])
                cd += timedelta(days=1)
            blob = "\n".join(texts)
        kw = keyword_set(blob)
        for _, row in movers.iterrows():
            events.append(
                BreakoutEvent(
                    trading_day=d,
                    code=str(row["Code"]).zfill(6),
                    name=str(row["Name"]),
                    return_pct=float(row["return_pct"]),
                    news_keywords=kw,
                    news_snippets=snippets[:15],
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
    for e in rel:
        inter = len(kw_news & e.news_keywords)
        union = len(kw_news | e.news_keywords) or 1
        score += inter / union
    return score / len(rel)


def name_mention_score(text_blob: str, name: str) -> float:
    """
    ``text_blob`` 안에 상장 종목명 ``name`` 이 몇 번 나오는지 세어 0~1로 캡합니다.

    최대 5회 이상이면 1.0. 예측 시 종목명 직접 거론을 가중치로 반영합니다.
    """
    if not name or name not in text_blob:
        return 0.0
    return min(1.0, text_blob.count(name) / 5.0)


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
