# -*- coding: utf-8 -*-
"""상한가 종목·시장 뉴스를 이슈 분류(국제·정책·사회 등)로 묶는다.

테마 흐름 리포트용. 뉴스는 **캐시 파일만** 읽고 네트워크 조회는 하지 않는다.
분류는 제목·요약 키워드 매칭이라 인과가 아니라 동시 언급 정도로 읽어야 한다.
"""
from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from src import news, trading_calendar

UNKNOWN = "개별·수급(뉴스 미확인)"

# (이슈, 색, 키워드) — 위에서부터 표시 순서.
ISSUES: list[tuple[str, str, tuple[str, ...]]] = [
    ("국제·지정학", "#f07178", (
        "중동", "이스라엘", "이란", "가자", "우크라이나", "러시아", "전쟁", "휴전", "미사일",
        "북한", "대만", "지정학", "안보", "나토", "NATO", "제재", "분쟁", "테러",
    )),
    ("미국·글로벌 증시", "#5aa9f5", (
        "뉴욕증시", "미국증시", "미 증시", "나스닥", "다우", "S&P", "엔비디아", "테슬라",
        "빅테크", "월가", "닛케이", "중국 증시", "글로벌 증시",
    )),
    ("금리·환율·원자재", "#f5d74a", (
        "금리", "연준", "FOMC", "파월", "국채", "CPI", "물가", "인플레", "환율", "달러",
        "엔화", "위안", "유가", "원유", "금값", "구리", "리튬", "니켈", "원자재",
    )),
    ("무역·관세", "#f0a84a", ("관세", "무역", "수출규제", "보호무역", "트럼프", "통상", "FTA")),
    ("정책·정치", "#c792ea", (
        "정부", "대통령", "국회", "법안", "개정안", "규제", "정책", "예산", "추경", "지원책",
        "세제", "공정위", "금융위", "국토부", "산업부", "과기정통부", "선거", "여당", "야당",
        "특검", "탄핵",
    )),
    ("사회 이슈", "#ff9e64", (
        "저출산", "출산", "고령화", "감염", "독감", "코로나", "팬데믹", "백신", "폭염", "한파",
        "태풍", "지진", "재난", "화재", "사고", "마약", "파업", "의대", "의료대란", "전세사기",
        "식중독", "미세먼지", "산불", "홍수",
    )),
    ("실적·수주", "#3ecf8e", (
        "실적", "영업이익", "매출", "흑자", "어닝", "수주", "공급계약", "단일판매", "납품",
        "계약 체결",
    )),
    ("임상·인허가", "#7fdbca", (
        "임상", "FDA", "품목허가", "허가", "기술이전", "라이선스", "신약", "식약처", "특허",
    )),
    ("M&A·지배구조", "#e88bd0", (
        "인수", "합병", "M&A", "최대주주", "경영권", "지분", "매각", "공개매수", "지배구조",
        "유상증자", "무상증자", "IPO", "신규상장",
    )),
    ("기술·신산업", "#82aaff", (
        "AI", "인공지능", "HBM", "로봇", "휴머노이드", "양자", "우주", "위성", "드론",
        "자율주행", "2차전지", "배터리", "원전", "SMR", "수소", "데이터센터", "전고체", "6G",
    )),
    ("엔터·이벤트", "#ffcb6b", (
        "콘서트", "공연", "드라마", "영화", "아이돌", "컴백", "출시", "올림픽", "월드컵",
        "박람회", "콘테스트", "후원", "축제", "흥행",
    )),
]
ISSUE_NAMES: list[str] = [n for n, _, _ in ISSUES]
ISSUE_COLOR: dict[str, str] = {n: c for n, c, _ in ISSUES}
ISSUE_COLOR[UNKNOWN] = "#5a6a80"

_SESSION_CUTOFF = time(15, 30)
_NEXT_MORNING = time(9, 0)


def classify(text: str) -> set[str]:
    """제목·요약 문자열에 키워드가 걸린 이슈 집합."""
    if not text:
        return set()
    return {name for name, _, kws in ISSUES if any(k in text for k in kws)}


def _cached_news_paths(d: date) -> list[Path]:
    return [
        news.day_news_json_path(d),
        news._legacy_provider_monthly_news_json_path(d),
        news._legacy_day_news_json_path(d),
        news._legacy_monthly_news_json_path(d),
    ]


def load_cached_news(d: date, _memo: dict[date, list[dict]] | None = None) -> list[dict]:
    """캘린더일 ``d`` 뉴스 캐시(없으면 빈 목록). 네트워크 조회 없음."""
    if _memo is not None and d in _memo:
        return _memo[d]
    rows: list[dict] = []
    for p in _cached_news_paths(d):
        if not p.is_file():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            data = data.get("items") or data.get("rows") or []
        if isinstance(data, list):
            rows = [r for r in data if isinstance(r, dict)]
            break
    if _memo is not None:
        _memo[d] = rows
    return rows


def load_cached_disclosures(d: date, memo: dict[date, list[dict]]) -> list[dict]:
    """캘린더일 ``d`` 공시 캐시(종목코드·제목). 네트워크 조회 없음."""
    if d in memo:
        return memo[d]
    rows: list[dict] = []
    p = news._day_file(d)
    if p.is_file():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            rows = [r for r in data if isinstance(r, dict)] if isinstance(data, list) else []
        except (OSError, json.JSONDecodeError):
            rows = []
    memo[d] = rows
    return rows


def session_news(trading_day: date, memo: dict[date, list[dict]]) -> list[dict]:
    """직전 거래일 15:30 초과 ~ 다음날 09:00 이하 기사(사후 정리용 — 장 마감 뒤 '왜 올랐나' 기사 포함)."""
    try:
        prev = trading_calendar.last_trading_day_before(trading_day)
    except ValueError:
        prev = trading_day - timedelta(days=1)
    lo = datetime.combine(prev, _SESSION_CUTOFF, tzinfo=trading_calendar.KST)
    nxt = trading_day + timedelta(days=1)
    hi = datetime.combine(nxt, _NEXT_MORNING, tzinfo=trading_calendar.KST)
    out: list[dict] = []
    d = prev
    while d <= nxt:
        for r in load_cached_news(d, memo):
            try:
                pub = news.row_published_kst(r, d)
            except (TypeError, ValueError):
                continue
            if lo < pub <= hi:
                out.append(r)
        d += timedelta(days=1)
    return out


def _row_text(r: dict) -> str:
    return f"{r.get('title') or ''} {r.get('description') or ''}"


def _mentions_stock(r: dict, code: str, name: str) -> bool:
    if code and (str(r.get("stock_code") or "") == code or code in (r.get("stock_codes") or [])):
        return True
    return len(name) >= 3 and name in _row_text(r)


def stock_issues(
    stock: dict[str, Any],
    rows: list[dict],
    *,
    disclosures: list[dict] | None = None,
    max_articles: int = 12,
) -> tuple[list[str], str]:
    """상한 종목 하나의 이슈 목록과 대표 기사(또는 공시) 제목.

    종목 근거 요약·원 테마 이름·그 종목 공시·그 종목을 언급한 기사를 함께 분류한다.
    """
    code = str(stock.get("code") or "")
    name = str(stock.get("name") or "")
    hits = classify(f"{stock.get('reason') or ''} {stock.get('raw') or ''}")
    headline = ""
    for dr in disclosures or []:
        if str(dr.get("code") or "").zfill(6) != code.zfill(6):
            continue
        title = str(dr.get("title") or "").strip()
        tags = classify(title)
        if tags and not headline:
            headline = f"[공시] {title}"
        hits |= tags
    n = 0
    for r in rows:
        if not _mentions_stock(r, code, name):
            continue
        tags = classify(_row_text(r))
        if tags and not headline:
            headline = str(r.get("title") or "").strip()
        hits |= tags
        n += 1
        if n >= max_articles:
            break
    ordered = [i for i in ISSUE_NAMES if i in hits]
    return (ordered or [UNKNOWN]), headline


def market_issue_counts(rows: list[dict]) -> tuple[int, dict[str, int], dict[str, str]]:
    """세션 전체 기사 수, 이슈별 기사 수, 이슈별 대표 제목."""
    counts: dict[str, int] = {}
    heads: dict[str, str] = {}
    seen: set[str] = set()
    total = 0
    for r in rows:
        key = str(r.get("link") or r.get("title") or "")
        if key in seen:
            continue
        seen.add(key)
        total += 1
        for tag in classify(_row_text(r)):
            counts[tag] = counts.get(tag, 0) + 1
            heads.setdefault(tag, str(r.get("title") or "").strip())
    return total, counts, heads


def attach_issues(day_rows: list[dict[str, Any]]) -> bool:
    """``day_rows`` 각 날짜에 종목 이슈·시장 이슈를 채운다. 뉴스 캐시가 하나도 없으면 False."""
    memo: dict[date, list[dict]] = {}
    disc_memo: dict[date, list[dict]] = {}
    any_news = False
    for r in day_rows:
        try:
            d = date.fromisoformat(r["d"])
        except ValueError:
            continue
        rows = session_news(d, memo)
        any_news = any_news or bool(rows)
        try:
            prev = trading_calendar.last_trading_day_before(d)
        except ValueError:
            prev = d - timedelta(days=1)
        disc = load_cached_disclosures(prev, disc_memo) + load_cached_disclosures(d, disc_memo)
        for s in r.get("stocks", []):
            s["issues"], s["headline"] = stock_issues(s, rows, disclosures=disc)
        total, counts, heads = market_issue_counts(rows)
        r["news_total"] = total
        r["news_issue"] = counts
        r["news_head"] = heads
    return any_news
