"""
일자별 뉴스 수집·캐시·예측용 early/late 분류.

- 캐시: ``data/cache/news/{naver|google|mock|none}/YYYY/day_YYYYMMDD.json``.
  네이버 키 사용 시 ``naver/``, Google RSS 시 ``google/``, ``MOCK_NEWS`` 는 ``mock/``.
  구버전 평면·구 ``news/YYYYMM/``·구 ``<provider>/YYYYMM/`` 는 읽은 뒤 연도 폴더로 이전(루트 월별은 naver 일 때만).
- 네이버: ``NEWS_NAVER_QUERY_MODE=market``(시장 키워드), ``ticker``(종목명+일자 쿼리 후 pubDate 필터·기사 링크 기준 중복 제거),
  ``both``(시장+종목 동시 수집 후 병합).
  API에 날짜 From/To 파라미터 없음.
- 일자 JSON을 **최대한 채우려면**: (1) 네이버 Client ID/Secret 설정,
  (2) ``NEWS_NAVER_QUERY_MODE=both``,
  (3) ``NEWS_NAVER_MARKET_MAX_PAGES=10``, ``NEWS_TICKER_NAVER_MAX_PAGES=10`` (429 나오면 낮춤),
  (4) ``SAMPLE_TICKERS`` 비움(전 종목),
  (5) RSS만 쓸 때 ``GOOGLE_NEWS_RSS_DUAL_LOCALE=1`` 및 ``NEWS_QUERY_SEEDS`` / ``GOOGLE_NEWS_RSS_QUERY_SEEDS_EXTRA`` 확장,
  (6) 기존 ``day_*.json`` 삭제 후 재수집.
- 네이버 키 없으면 RSS 시장 쿼리(``ticker``/``both`` 설정 시 자동 대체).
- ``MOCK_NEWS`` 로 API 없이 파이프라인 테스트 가능.

매수 시나리오(장 마감 전 주문)에 맞춰 관측일 ``T`` 의 **직전 KRX 거래일** ``NEWS_CUTOFF_*``(KST)까지를 early로 둡니다.
"""
from __future__ import annotations

import calendar
import json
import re
import threading
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as time_cls, timedelta, timezone
from email.utils import parsedate_to_datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests
from tqdm import tqdm

from . import config, trading_calendar

KST = ZoneInfo("Asia/Seoul")

_CACHE_NEWS = config.CACHE_DIR / "news"


class _NewsHttpCountSink:
    """하루치 뉴스 수집 구간에서 네이버·RSS HTTP 호출 수를 스레드 안전하게 집계."""

    __slots__ = ("_lock", "naver", "rss")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.naver = 0
        self.rss = 0

    def add_naver(self, n: int = 1) -> None:
        with self._lock:
            self.naver += n

    def add_rss(self, n: int = 1) -> None:
        with self._lock:
            self.rss += n


def _print_news_http_usage(
    target: date, sink: _NewsHttpCountSink, stocks_with_news: int
) -> None:
    """
    네트워크 수집 직후 한 줄 요약.

    ``YYYYMMDD - (시도한 HTTP 호출 수) : (해당 일 뉴스 row를 1건이라도 받은 종목 수)`` 형식.
    시장 전용·RSS 등 종목 단위가 없으면 둘째 값은 ``1``(수집 결과 있음) / ``0``(없음).
    """
    attempts = sink.naver + sink.rss
    print(f"{target.strftime('%Y%m%d')} - {attempts} : {stocks_with_news}", flush=True)


def _day_key(d: date) -> str:
    """캐시 파일명용 ``YYYYMMDD`` 문자열."""
    return d.strftime("%Y%m%d")


def news_cache_provider_subdir() -> str:
    """
    뉴스 캐시 최상위 하위 폴더 이름.

    ``USE_GOOGLE_NEWS_RSS_FALLBACK`` 및 네이버 키 유무와 동일한 분기로
    ``fetch_news_for_calendar_day`` 가 실제 조회하는 출처와 맞춥니다.
    """
    if config.MOCK_NEWS:
        return "mock"
    if config.NAVER_CLIENT_ID and config.NAVER_CLIENT_SECRET:
        if config.NEWS_NAVER_QUERY_MODE == "ticker":
            return "naver_ticker"
        if config.NEWS_NAVER_QUERY_MODE == "both":
            return "naver_both"
        return "naver"
    if config.NEWS_NAVER_QUERY_MODE in ("ticker", "both") or config.USE_GOOGLE_NEWS_RSS_FALLBACK:
        return "google"
    return "none"


def _legacy_day_news_json_path(target: date) -> Path:
    """구버전 평면 캐시: ``news/day_YYYYMMDD.json``."""
    return _CACHE_NEWS / f"day_{_day_key(target)}.json"


def _legacy_monthly_news_json_path(target: date) -> Path:
    """구버전 월 폴더(출처 미구분): ``news/YYYYMM/day_YYYYMMDD.json``."""
    return _CACHE_NEWS / target.strftime("%Y%m") / f"day_{_day_key(target)}.json"


def _legacy_provider_monthly_news_json_path(target: date) -> Path:
    """이전 출처별 월 폴더: ``news/<provider>/YYYYMM/day_YYYYMMDD.json``."""
    return (
        _CACHE_NEWS
        / news_cache_provider_subdir()
        / target.strftime("%Y%m")
        / f"day_{_day_key(target)}.json"
    )


def day_news_json_path(target: date) -> Path:
    """뉴스 일별 JSON: ``data/cache/news/<출처>/YYYY/day_YYYYMMDD.json``."""
    return (
        _CACHE_NEWS
        / news_cache_provider_subdir()
        / target.strftime("%Y")
        / f"day_{_day_key(target)}.json"
    )


def day_news_cache_hit(target: date) -> bool:
    """현재 출처 연도 경로·구 출처 월별·구 평면·(naver일 때) 루트 월별 경로에 캐시가 있으면 True."""
    if day_news_json_path(target).is_file():
        return True
    if _legacy_provider_monthly_news_json_path(target).is_file():
        return True
    if _legacy_day_news_json_path(target).is_file():
        return True
    if news_cache_provider_subdir() == "naver" and _legacy_monthly_news_json_path(target).is_file():
        return True
    return False


def naver_day_cache_paths_for_purge(target: date) -> tuple[Path, ...]:
    """``fetch_news_naver_day --force`` 등에서 제거할 네이버·구 레거시 경로."""
    return (
        _CACHE_NEWS / "naver" / target.strftime("%Y") / f"day_{_day_key(target)}.json",
        _CACHE_NEWS / "naver_ticker" / target.strftime("%Y") / f"day_{_day_key(target)}.json",
        _CACHE_NEWS / "naver_both" / target.strftime("%Y") / f"day_{_day_key(target)}.json",
        _CACHE_NEWS / "naver" / target.strftime("%Y%m") / f"day_{_day_key(target)}.json",
        _legacy_day_news_json_path(target),
        _legacy_monthly_news_json_path(target),
    )


def _parse_pub_date(s: str) -> date | None:
    """
    네이버 API ``pubDate``(RFC822) → **KST 기준 캘린더 날짜**.

    로컬 타임존으로 바꾸던 기존 방식은 OS 설정에 따라 하루 어긋날 수 있어,
    ``row_published_kst`` 와 동일하게 KST로 정규화합니다.
    """
    raw = (s or "").strip()
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo:
            return dt.astimezone(KST).date()
        return date(dt.year, dt.month, dt.day)
    except Exception:
        return None


def _rfc822_kst(d: date, hour: int = 9, minute: int = 0) -> str:
    """모의 뉴스 등에 쓰는 RFC822 형식 타임스탬프 문자열(+0900)."""
    wd = calendar.day_abbr[d.weekday()]
    mon = calendar.month_abbr[d.month]
    return f"{wd}, {d.day:02d} {mon} {d.year} {hour:02d}:{minute:02d}:00 +0900"


def row_published_kst(row: dict[str, str], calendar_day: date) -> datetime:
    """
    뉴스 row의 ``pub`` / ``pubDate`` 로부터 게시 시각을 KST ``datetime`` 으로 파싱합니다.

    값이 없으면 해당 ``calendar_day`` 09:00 KST로 간주(구 캐시·RSS 누락 호환).
    """
    raw = (row.get("pub") or row.get("pubDate") or "").strip()
    if not raw:
        return datetime.combine(calendar_day, time_cls(9, 0), tzinfo=KST)
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo:
            return dt.astimezone(KST)
        return dt.replace(tzinfo=KST)
    except Exception:
        return datetime.combine(calendar_day, time_cls(9, 0), tzinfo=KST)


def news_fetch_calendar_span(target_trading_day: date) -> tuple[date, date]:
    """
    관측 거래일 ``T`` 에 필요한 뉴스를 **어느 캘린더 구간까지** 가져올지 [시작, 끝]을 반환합니다.

    ``n_buy`` = ``T`` 직전 거래일, ``n_prev`` = 그 직전 거래일일 때,
    ``n_prev`` 캘린더일부터 ``T`` 전일(캘린더)까지(양 끝 포함)를 다운로드 대상으로 삼습니다.
    """
    n_buy = trading_calendar.last_trading_day_before(target_trading_day)
    n_prev = trading_calendar.last_trading_day_before(n_buy)
    end = target_trading_day - timedelta(days=1)
    return n_prev, end


def _decision_news_bounds(target_trading_day: date) -> tuple[date, date, datetime, date]:
    """
    ``T`` 기준 early/late 분류에 쓰이는 경계값.

    Returns:
        (n_prev, t_cal_end, cutoff_kst, n_buy) — ``n_buy``=T 직전 KRX 거래일(직전 장),
        ``n_prev``=그 이전 거래일(뉴스 캘린더 스캔 구간 시작에 사용),
        ``cutoff``=``n_buy`` 일 ``config.NEWS_CUTOFF_*`` KST(직전 장 마감 시각까지 early),
        ``t_cal_end``=T 전일(캘린더).
    """
    n_buy = trading_calendar.last_trading_day_before(target_trading_day)
    n_prev = trading_calendar.last_trading_day_before(n_buy)
    t_cal_end = target_trading_day - timedelta(days=1)
    cutoff = datetime.combine(
        n_buy,
        time_cls(config.NEWS_CUTOFF_KST_HOUR, config.NEWS_CUTOFF_KST_MINUTE),
        tzinfo=KST,
    )
    return n_prev, t_cal_end, cutoff, n_buy


def calendar_days_for_breakout_training(target_trading_day: date) -> frozenset[date]:
    """
    ``features.build_breakout_events`` 가 해당 **거래일** 급등 blob을 만들 때 스캔하는 캘린더 일자 집합.

    학습 스냅샷 증분 갱신 시 ``누락된 캘린더 일`` 이 어떤 거래일 이벤트에 영향을 주는지 판별할 때 씁니다.
    """
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        n_prev, t_cal_end, _, _ = _decision_news_bounds(target_trading_day)
        s: list[date] = []
        d = n_prev
        while d <= t_cal_end:
            s.append(d)
            d += timedelta(days=1)
        return frozenset(s)
    ws, we = trading_calendar.news_window_for_target_trading_day(target_trading_day)
    s = []
    d = ws
    while d <= we:
        s.append(d)
        d += timedelta(days=1)
    return frozenset(s)


def _news_row_bucket(
    d: date,
    pub: datetime,
    n_buy: date,
    cutoff: datetime,
) -> str:
    """
    단일 뉴스 row를 ``early``(예측 입력에 포함) vs ``late``(그 이후)로 분류합니다.

    - ``d < n_buy``: 해당 캘린더일 전체 early(직전 장 이전·휴일 포함).
    - ``d == n_buy``: ``pub <= cutoff``(직전 장 ``NEWS_CUTOFF_*`` KST) 이면 early.
    - ``d > n_buy`` 이고 ``T`` 전일까지: late.
    """
    if d < n_buy:
        return "early"
    if d == n_buy:
        return "early" if pub <= cutoff else "late"
    return "late"


def classified_rows_for_target(
    news_by_calendar: dict[date, list[dict[str, str]]],
    target_trading_day: date,
) -> tuple[list[tuple[date, dict]], list[tuple[date, dict]]]:
    """
    관측 거래일 ``T`` 에 대해 뉴스 row를 early / late 두 리스트로 나눕니다.

    Returns:
        ``(early_rows, late_rows)`` — 각 원소는 ``(캘린더일, 뉴스 row dict)``.

    Note:
        ``USE_DECISION_NEWS_INTRADAY_CUTOFF=0`` 이면 ``news_window_for_target_trading_day`` 구간
        전체를 early에 넣고 late는 빈 리스트입니다.
    """
    early_out: list[tuple[date, dict]] = []
    late_out: list[tuple[date, dict]] = []
    if not config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        ws, we = trading_calendar.news_window_for_target_trading_day(target_trading_day)
        d = ws
        while d <= we:
            for row in news_by_calendar.get(d, []):
                if f"{row.get('title', '')}{row.get('description', '')}".strip():
                    early_out.append((d, row))
            d += timedelta(days=1)
        return early_out, late_out

    n_prev, t_cal_end, cutoff, n_buy = _decision_news_bounds(target_trading_day)
    d = n_prev
    while d <= t_cal_end:
        for row in news_by_calendar.get(d, []):
            text = f"{row.get('title', '')} {row.get('description', '')}".strip()
            if not text:
                continue
            pub = row_published_kst(row, d)
            bucket = _news_row_bucket(d, pub, n_buy, cutoff)
            (early_out if bucket == "early" else late_out).append((d, row))
        d += timedelta(days=1)
    return early_out, late_out


def aggregate_early_late_for_target(
    news_by_calendar: dict[date, list[dict[str, str]]],
    target_trading_day: date,
) -> tuple[str, str]:
    """
    관측 거래일 ``T`` 에 대해 예측에 쓸 텍스트(early)와 그 이후(late)를 각각 한 덩어리 문자열로 만듭니다.

    - **Early**: 장 마감 전 주문 시점까지 반영 가능한 뉴스(설명은 ``classified_rows_for_target`` 참고).
    - **Late**: 그 이후~T 전일 — 예측 입력에는 넣지 않고,
      갭 분석·「늦은 뉴스에 키워드가 있었는지」 프로브에 사용.

    Returns:
        ``(early_blob, late_blob)`` — 제목·요약을 줄바꿈으로 이은 문자열.
    """
    early_rows, late_rows = classified_rows_for_target(news_by_calendar, target_trading_day)
    early_parts = [
        f"{r.get('title', '')} {r.get('description', '')}".strip() for _, r in early_rows
    ]
    late_parts = [f"{r.get('title', '')} {r.get('description', '')}".strip() for _, r in late_rows]
    return "\n".join(early_parts), "\n".join(late_parts)


def sample_titles_early_for_target(
    news_by_calendar: dict[date, list[dict[str, str]]],
    target_trading_day: date,
    limit: int = 12,
) -> list[str]:
    """early 구간 뉴스에서 제목만 최대 ``limit`` 개 추려 리포트 요약에 표시합니다."""
    early_rows, _ = classified_rows_for_target(news_by_calendar, target_trading_day)
    out: list[str] = []
    for d, row in early_rows:
        t = row.get("title")
        if t:
            out.append(t)
        if len(out) >= limit:
            break
    return out[:limit]


def rows_for_actual_context(
    news_by_calendar: dict[date, list[dict[str, str]]],
    target_trading_day: date,
) -> list[tuple[date, dict]]:
    """
    '실제 급등일' 맥락 참고용: late 구간 기사 + 관측일 T 캘린더일 기사(당일 장중·종가 이슈).
    인과 단정 아님(키워드·종목명 문자열 매칭).
    """
    _, late_rows = classified_rows_for_target(news_by_calendar, target_trading_day)
    t_day = target_trading_day
    t_rows = [(t_day, r) for r in news_by_calendar.get(t_day, [])]
    return late_rows + t_rows


def match_stock_news_rows(
    rows: list[tuple[date, dict]],
    stock_name: str,
    keywords: list[str],
    *,
    limit: int = 8,
) -> list[dict]:
    """종목명 또는 키워드(2글자 이상)가 제목·요약에 들어가는 기사만."""
    name = (stock_name or "").strip()
    kws = [k.strip() for k in keywords if k and len(k.strip()) >= 2]
    seen: set[str] = set()
    hits: list[dict] = []
    for cal_d, row in rows:
        title = (row.get("title") or "").strip()
        desc = (row.get("description") or "").strip()
        blob = f"{title} {desc}"
        if not blob.strip():
            continue
        key = row.get("link") or (title + desc[:80])
        if key in seen:
            continue
        matched = ""
        if name and name in blob:
            matched = name
        else:
            for k in kws:
                if k in blob:
                    matched = k
                    break
        if not matched:
            continue
        seen.add(key)
        hits.append(
            {
                "day": cal_d,
                "title": title,
                "description": desc[:220],
                "link": (row.get("link") or "").strip(),
                "matched": matched,
            }
        )
        if len(hits) >= limit:
            break
    return hits


def late_blob_covers_keywords(late_blob: str, keywords: list[str]) -> bool:
    """
    late 텍스트에 예측 키워드(2글자 이상)가 소문자 기준 부분 문자열로 하나라도 있으면 True.

    「장 마감 후 뉴스에 같은 키워드가 있었는지」 휴리스틱용.
    """
    if not late_blob or not keywords:
        return False
    low = late_blob.lower()
    return any(len(k) >= 2 and k.lower() in low for k in keywords)


def _fetch_naver_page(
    session: requests.Session,
    query: str,
    start: int,
    display: int = 100,
    *,
    http_sink: _NewsHttpCountSink | None = None,
) -> dict[str, Any]:
    """네이버 뉴스 검색 API 한 페이지(JSON). ``start`` 는 1-based 오프셋."""
    if http_sink is not None:
        http_sink.add_naver(1)
    headers = {
        "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
    }
    params = {"query": query, "display": display, "start": start, "sort": "date"}
    r = session.get(config.NAVER_NEWS_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # HTTP 200 이면서 본문에 오류 코드가 오는 경우(미사용 API·쿼터 등)
    err = data.get("errorCode") or data.get("errorMessage")
    if err:
        msg = data.get("errorMessage", str(err))
        raise RuntimeError(f"네이버 뉴스 API 오류: {msg} (code={data.get('errorCode')!r})")
    return data


def describe_news_fetch_source() -> str:
    """리포트·로그용 출처 문구."""
    if config.MOCK_NEWS:
        return "모의 뉴스(MOCK_NEWS=1)"
    if config.NAVER_CLIENT_ID and config.NAVER_CLIENT_SECRET:
        if config.NEWS_NAVER_QUERY_MODE == "ticker":
            return "네이버 검색 API (종목별 쿼리·pubDate 일자 필터)"
        if config.NEWS_NAVER_QUERY_MODE == "both":
            return "네이버 검색 API (시장+종목 동시 쿼리·병합)"
        return "네이버 검색 API (시장 키워드·일자)"
    if config.USE_GOOGLE_NEWS_RSS_FALLBACK:
        if config.NEWS_NAVER_QUERY_MODE in ("ticker", "both"):
            return "Google News RSS - 네이버 키 없음, 종목별 API 불가 → 시장 일자 쿼리로 대체"
        return "Google News RSS (네이버 API 키 없음)"
    return "뉴스 수집 생략 (키 없음·RSS 폴백 끔)"


def _published_date_kst_from_rfc822(pub_header: str) -> date | None:
    """Google RSS ``pubDate`` 등 RFC822 → KST 날짜. 파싱 실패 시 None."""
    try:
        dt = parsedate_to_datetime(pub_header.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(KST).date()
    except Exception:
        return None


def _local_xml_tag(tag: str) -> str:
    """ElementTree 네임스페이스 제거 후 로컬 태그명만 반환."""
    return tag.split("}", 1)[1] if "}" in tag else tag


def _google_rss_item_fields(item: ET.Element) -> dict[str, str]:
    """RSS ``item`` 원소에서 title/link/pubDate/description 추출."""
    title = link = pub_raw = desc = ""
    for child in item:
        ln = _local_xml_tag(child.tag)
        t = (child.text or "").strip()
        if ln == "title":
            title = t
        elif ln == "link":
            link = t
        elif ln == "pubDate":
            pub_raw = t
        elif ln == "description":
            desc = re.sub(r"<[^>]+>", "", t).strip()
    return {"title": title, "link": link, "pub": pub_raw, "description": desc}


def _fetch_google_news_rss_once(
    session: requests.Session,
    query: str,
    *,
    hl: str = "ko",
    gl: str = "KR",
    ceid: str = "KR:ko",
    http_sink: _NewsHttpCountSink | None = None,
) -> list[dict[str, str]]:
    """Google News RSS 검색 한 번 호출해 item 리스트를 dict로 반환."""
    if http_sink is not None:
        http_sink.add_rss(1)
    url = "https://news.google.com/rss/search?" + urlencode(
        {"q": query, "hl": hl, "gl": gl, "ceid": ceid}
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }
    r = session.get(url, headers=headers, timeout=45)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    channel = root.find("channel")
    if channel is None:
        return []
    out: list[dict[str, str]] = []
    for el in channel.findall("item"):
        raw = _google_rss_item_fields(el)
        if raw.get("title"):
            out.append(raw)
    return out


def _dedupe_query_strings(queries: list[str]) -> list[str]:
    """앞뒤 공백 제거 후 순서 유지 중복 제거."""
    seen: set[str] = set()
    out: list[str] = []
    for q in queries:
        s = (q or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


@lru_cache(maxsize=1)
def _stock_name_code_pairs() -> tuple[tuple[str, str], ...]:
    """
    종목명-종목코드 사전(메모리 캐시).

    종목명 문자열 매칭으로 ``stock_code`` 가 비어 있는 뉴스 row를 보강할 때 사용합니다.
    """
    from . import stocks

    listing = stocks.load_listing()
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for _, r in listing.iterrows():
        code = str(r.get("Code", "")).zfill(6)
        name = str(r.get("Name", "")).strip()
        if not code or not name:
            continue
        # 1글자 이름은 오탐이 많아 제외.
        if len(name) < 2:
            continue
        key = (name, code)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)
    # 긴 이름 우선으로 매칭해 "삼성전자우" vs "삼성전자" 충돌을 줄입니다.
    pairs.sort(key=lambda x: len(x[0]), reverse=True)
    return tuple(pairs)


def _enrich_rows_with_stock_codes(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """
    ``stock_code`` 가 없는 뉴스 row에 종목명 부분 문자열 매칭으로 코드·이름을 보강합니다.

    - 기존 ``stock_code`` 가 있으면 그대로 유지합니다.
    - 여러 종목이 동시에 매칭되면 첫 매칭을 대표로 두고, 전체는 ``stock_codes``/``stock_names`` 에 보관합니다.
    """
    if not rows:
        return rows
    pairs = _stock_name_code_pairs()
    if not pairs:
        return rows

    for row in rows:
        if str((row or {}).get("stock_code") or "").strip():
            continue
        title = str((row or {}).get("title") or "").strip()
        desc = str((row or {}).get("description") or "").strip()
        blob = f"{title} {desc}".strip()
        if not blob:
            continue

        hit_codes: list[str] = []
        hit_names: list[str] = []
        for name, code in pairs:
            if name in blob:
                if code not in hit_codes:
                    hit_codes.append(code)
                    hit_names.append(name)
                # 과도한 다중 매칭은 노이즈가 커 상위 일부만 유지
                if len(hit_codes) >= 5:
                    break

        if not hit_codes:
            continue
        row["stock_code"] = hit_codes[0]
        row["stock_name"] = hit_names[0]
        if len(hit_codes) > 1:
            row["stock_codes"] = hit_codes
            row["stock_names"] = hit_names
    return rows


def _google_news_rss_queries_for_calendar_day(target: date) -> list[str]:
    """
    하루치 Google News RSS용 검색어 목록.

    한글 날짜·ISO 날짜·날짜 범위(after/before) 등 서로 다른 인덱스를 노려 건수를 늘립니다.
    최종 필터는 여전히 ``pubDate`` → KST 캘린더일 == ``target`` 입니다.
    """
    date_ko = f"{target.year}년 {target.month}월 {target.day}일"
    iso = target.isoformat()
    next_cal = target + timedelta(days=1)
    iso_next = next_cal.isoformat()
    parts: list[str] = []

    for tail in (
        "증시",
        "코스피",
        "코스닥",
        "주식",
        "한국증시",
        "국내증시",
        "코스피지수",
        "코스닥지수",
        "증권",
        "금융",
        "주식시장",
        "선물옵션",
    ):
        parts.append(f"{date_ko} {tail}")

    for tail in ("증시", "코스피", "코스닥", "주식", "KOSPI", "stock market"):
        parts.append(f"{iso} {tail}")

    parts += [f"{date_ko} {s}" for s in config.NEWS_QUERY_SEEDS]
    parts += [f"{date_ko} {s}" for s in config.GOOGLE_NEWS_RSS_QUERY_SEEDS_EXTRA]

    for kw in (
        "코스피",
        "KOSPI",
        "코스닥",
        "KOSDAQ",
        "증시",
        "주식",
        "한국거래소",
        "상장사",
    ):
        parts.append(f"{kw} after:{iso} before:{iso_next}")

    return _dedupe_query_strings(parts)


def _google_news_rss_locale_variants() -> tuple[tuple[str, str, str], ...]:
    """(hl, gl, ceid) 튜플. DUAL_LOCALE 이면 영문 에디션을 추가."""
    base: tuple[tuple[str, str, str], ...] = (("ko", "KR", "KR:ko"),)
    if config.GOOGLE_NEWS_RSS_DUAL_LOCALE:
        return base + (("en", "KR", "KR:en"),)
    return base


def _naver_rows_for_ticker_on_day(
    session: requests.Session,
    code: str,
    name: str,
    target: date,
    date_ko: str,
    *,
    http_sink: _NewsHttpCountSink | None = None,
) -> list[dict[str, str]]:
    """
    한 종목·한 캘린더일: ``YYYY년 M월 D일 {종목명}`` 로 네이버 뉴스 검색 후 ``pubDate==target`` 만 유지.

    네이버 API는 기간 파라미터가 없어, **조회 구간**은 페이지를 넘기며 ``pubDate`` 로만 자릅니다.
    """
    name = (name or "").strip()
    if len(name) < 2:
        return []
    # 시장 모드 쿼리와 같이 「YYYY년 M월 D일 …」 형을 앞에 두어 검색 일관성 유지
    query = f"{date_ko} {name}"
    out: list[dict[str, str]] = []
    for i in range(config.NEWS_TICKER_NAVER_MAX_PAGES):
        page_start = 1 + i * 100
        try:
            data = _fetch_naver_page(session, query, page_start, http_sink=http_sink)
        except (requests.HTTPError, RuntimeError):
            break
        batch = data.get("items", [])
        if not batch:
            break
        stop_q = False
        for it in batch:
            pub = _parse_pub_date(it.get("pubDate", ""))
            if pub is None:
                continue
            if pub < target:
                stop_q = True
                continue
            if pub != target:
                continue
            title = re.sub(r"<[^>]+>", "", it.get("title", ""))
            desc = re.sub(r"<[^>]+>", "", it.get("description", ""))
            link = it.get("link", "")
            pub_raw = (it.get("pubDate") or "").strip()
            out.append(
                {
                    "title": title,
                    "description": desc,
                    "link": link,
                    "pub": pub_raw,
                    "stock_code": str(code).zfill(6),
                    "stock_name": name,
                }
            )
        if stop_q:
            break
        time.sleep(0.04)
    return out


def _ticker_day_fetch_worker(payload: tuple[_NewsHttpCountSink, str, str, date, str]) -> list[dict[str, str]]:
    http_sink, code, name, target, date_ko = payload
    sess = requests.Session()
    try:
        return _naver_rows_for_ticker_on_day(
            sess, code, name, target, date_ko, http_sink=http_sink
        )
    finally:
        sess.close()


def _fetch_news_day_naver_per_ticker(
    target: date, *, http_sink: _NewsHttpCountSink | None = None
) -> tuple[list[dict[str, str]], int]:
    """
    상장(또는 ``SAMPLE_TICKERS_N`` 표본) 각 종목에 대해 해당 일 뉴스를 조회해 하나의 일자 풀으로 합칩니다.

    여러 종목 쿼리에 같은 기사가 잡히므로, 링크(없으면 제목+요약) 기준으로 ``_merge_news_rows`` 와
    동일하게 **기사 단위 중복 제거**를 합니다. 캐시 파일 형식은 ``YYYY/day_YYYYMMDD.json`` 1개/일.

    Returns:
        ``(merged_rows, n_stocks_with_any_row)`` — 둘째 값은 워커가 **그날 pubDate 필터 후**
        기사를 1건이라도 돌려준 종목 수(병합·중복 제거 전 기준).
    """
    from . import stocks

    listing = stocks.load_listing()
    if config.SAMPLE_TICKERS_N:
        listing = listing.head(config.SAMPLE_TICKERS_N)
    date_ko = f"{target.year}년 {target.month}월 {target.day}일"
    codes = listing["Code"].astype(str).str.zfill(6).tolist()
    names = listing["Name"].astype(str).tolist()
    payloads = [
        (http_sink, c, n, target, date_ko) for c, n in zip(codes, names)
    ]
    raw_rows: list[dict[str, str]] = []
    stocks_with_any_row = 0
    with ThreadPoolExecutor(max_workers=config.NEWS_TICKER_NAVER_MAX_WORKERS) as ex:
        futures = [ex.submit(_ticker_day_fetch_worker, p) for p in payloads]
        use_pbar = len(payloads) >= 40
        it = as_completed(futures)
        if use_pbar:
            it = tqdm(it, total=len(payloads), desc=f"네이버 종목뉴스 {target}", leave=False)
        for fut in it:
            try:
                rows = fut.result()
            except Exception:
                continue
            if rows:
                stocks_with_any_row += 1
            raw_rows.extend(rows)
    return _merge_news_rows(raw_rows, []), stocks_with_any_row


def _fetch_news_day_naver(
    target: date,
    session: requests.Session,
    *,
    http_sink: _NewsHttpCountSink | None = None,
) -> list[dict[str, str]]:
    """
    네이버 검색으로 ``target`` 일자에 게재된 기사만 모읍니다.

    날짜가 들어간 쿼리 여러 개 × 페이지네이션; ``pubDate`` 가 ``target`` 과 다른 건 제외.
    """
    seen: set[str] = set()
    items: list[dict[str, str]] = []
    date_ko = f"{target.year}년 {target.month}월 {target.day}일"
    queries = [f"{date_ko} 증시", f"{date_ko} 코스피", f"{date_ko} 코스닥", f"{date_ko} 주식"]
    queries += [f"{date_ko} {s}" for s in config.NEWS_QUERY_SEEDS if s not in ("KOSPI", "KOSDAQ")]

    for q in queries:
        for i in range(config.NEWS_NAVER_MARKET_MAX_PAGES):
            page_start = 1 + i * 100
            try:
                data = _fetch_naver_page(session, q, page_start, http_sink=http_sink)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code in (401, 403):
                    print(
                        f"네이버 뉴스 API HTTP {e.response.status_code} (쿼리 일부: {q[:40]}…). "
                        "Client ID/Secret·앱에서 '검색' API 사용 설정을 확인하세요.",
                        flush=True,
                    )
                break
            except RuntimeError as e:
                print(f"{e}", flush=True)
                break
            batch = data.get("items", [])
            if not batch:
                break
            stop_q = False
            for it in batch:
                pub = _parse_pub_date(it.get("pubDate", ""))
                if pub is None:
                    continue
                if pub < target:
                    stop_q = True
                    continue
                if pub != target:
                    continue
                title = re.sub(r"<[^>]+>", "", it.get("title", ""))
                desc = re.sub(r"<[^>]+>", "", it.get("description", ""))
                link = it.get("link", "")
                pub_raw = (it.get("pubDate") or "").strip()
                key = link or (title + desc)
                if key in seen:
                    continue
                seen.add(key)
                items.append(
                    {"title": title, "description": desc, "link": link, "pub": pub_raw}
                )
            if stop_q:
                break
            time.sleep(0.05)
    return items


def _merge_news_rows(rows_a: list[dict[str, str]], rows_b: list[dict[str, str]]) -> list[dict[str, str]]:
    """market/ticker 두 소스 결과를 기사 단위로 병합(중복 제거)."""
    merged: dict[str, dict[str, str]] = {}
    for row in rows_a + rows_b:
        link = (row.get("link") or "").strip()
        key = link or (
            f"{(row.get('title') or '').strip()}|{(row.get('description') or '').strip()}"
        )
        if not key:
            continue
        if key not in merged:
            merged[key] = dict(row)
            continue
        cur = merged[key]
        # ticker에서만 있는 종목 메타를 잃지 않도록 보강
        for k in ("stock_code", "stock_name", "pub", "title", "description", "link"):
            if not cur.get(k) and row.get(k):
                cur[k] = row[k]
    return list(merged.values())


def _fetch_news_day_google_rss(
    target: date,
    session: requests.Session,
    *,
    http_sink: _NewsHttpCountSink | None = None,
) -> list[dict[str, str]]:
    """Google RSS로 ``target`` 일자 필터. 다중 쿼리·선택 이중 로케일로 건수 확대."""
    seen: set[str] = set()
    items: list[dict[str, str]] = []
    queries = _google_news_rss_queries_for_calendar_day(target)
    locales = _google_news_rss_locale_variants()

    for q in queries:
        for hl, gl, ceid in locales:
            try:
                batch = _fetch_google_news_rss_once(
                    session, q, hl=hl, gl=gl, ceid=ceid, http_sink=http_sink
                )
            except (requests.RequestException, ET.ParseError):
                continue
            for it in batch:
                pub_d = _published_date_kst_from_rfc822(it.get("pub", ""))
                if pub_d is None or pub_d != target:
                    continue
                title = it.get("title", "")
                desc = it.get("description", "")
                link = it.get("link", "")
                pub_raw = (it.get("pub") or "").strip()
                key = link or (title + desc)
                if key in seen:
                    continue
                seen.add(key)
                items.append(
                    {"title": title, "description": desc, "link": link, "pub": pub_raw}
                )
        time.sleep(config.GOOGLE_NEWS_RSS_QUERY_SLEEP_SEC)
    return items


def fetch_news_for_calendar_day(
    target: date,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    """
    특정 캘린더 일자 ``target`` 의 뉴스 row 리스트를 반환합니다.

    1) ``<naver|google|mock|none>/YYYY/day_*.json`` 캐시가 있으면 읽기만 함.
       구 ``<provider>/YYYYMM/``·구 평면·구 ``news/YYYYMM/``(출처 미표시)는 조건에 따라 읽은 뒤 이전.
    2) ``MOCK_NEWS`` 이면 고정 모의 데이터 저장 후 반환.
    3) 아니면 네이버 또는 Google RSS로 수집 후 캐시에 JSON 저장.

    네이버는 최신순 정렬이라 쿼리에 한국어 날짜 문자열을 넣고 ``pubDate`` 로 재필터합니다.
    ``NEWS_NAVER_QUERY_MODE=ticker`` 이면 **종목마다** ``YYYY년 M월 D일 {종목명}`` 검색 후 같은 일자만 합칩니다.
    ``NEWS_NAVER_QUERY_MODE=both`` 이면 시장 쿼리 + 종목 쿼리를 모두 수행해 결과를 병합합니다.
    (네이버 API에 dateFrom/dateTo 없음 → 기간은 pubDate 필터로만 구현.)

    Args:
        session: 재사용 시 연결 풀 이점. ``None`` 이면 함수 내부에서 열고 닫음.
            ``ticker``/``both``+네이버일 때 종목 워커는 별도 Session 을 씁니다.
    """
    _CACHE_NEWS.mkdir(parents=True, exist_ok=True)
    provider = news_cache_provider_subdir()
    day_file = day_news_json_path(target)
    legacy_file = _legacy_day_news_json_path(target)
    legacy_monthly = _legacy_monthly_news_json_path(target)
    legacy_provider_monthly = _legacy_provider_monthly_news_json_path(target)

    def _should_refetch_empty_cache(cached: object) -> bool:
        # 이전에 실패 후 [] 만 저장된 캐시는 무시하고 재수집
        if not isinstance(cached, list) or len(cached) != 0:
            return False
        if provider == "mock":
            return False
        if provider == "naver":
            return bool(
                config.NAVER_CLIENT_ID
                and config.NAVER_CLIENT_SECRET
                and not config.MOCK_NEWS
            )
        if provider == "google":
            return not config.MOCK_NEWS and (
                config.USE_GOOGLE_NEWS_RSS_FALLBACK
                or config.NEWS_NAVER_QUERY_MODE in ("ticker", "both")
            )
        return False

    def _migrate_from(path: Path) -> list | None:
        if not path.is_file():
            return None
        cached = json.loads(path.read_text(encoding="utf-8"))
        if _should_refetch_empty_cache(cached):
            try:
                path.unlink()
            except OSError:
                pass
            return None
        day_file.parent.mkdir(parents=True, exist_ok=True)
        day_file.write_text(json.dumps(cached, ensure_ascii=False), encoding="utf-8")
        try:
            path.unlink()
        except OSError:
            pass
        return cached

    if day_file.is_file():
        cached = json.loads(day_file.read_text(encoding="utf-8"))
        if _should_refetch_empty_cache(cached):
            try:
                day_file.unlink()
            except OSError:
                pass
        else:
            # 과거 버전은 종목코드|링크 단위로 쌓아 동일 기사가 수백 번 들어갈 수 있음 → 읽을 때 한 번 정리.
            if (
                provider == "naver_ticker"
                and isinstance(cached, list)
                and cached
                and isinstance(cached[0], dict)
            ):
                fixed = _merge_news_rows(cached, [])
                if len(fixed) < len(cached):
                    try:
                        day_file.write_text(
                            json.dumps(fixed, ensure_ascii=False), encoding="utf-8"
                        )
                    except OSError:
                        pass
                    return fixed
            return cached
    else:
        migrated_pm = _migrate_from(legacy_provider_monthly)
        if migrated_pm is not None:
            return migrated_pm
        migrated = _migrate_from(legacy_file)
        if migrated is not None:
            return migrated
        if provider == "naver":
            migrated_m = _migrate_from(legacy_monthly)
            if migrated_m is not None:
                return migrated_m

    if config.MOCK_NEWS:
        rows = _mock_news(target)
        day_file.parent.mkdir(parents=True, exist_ok=True)
        day_file.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        return rows

    own_session = session is None
    if session is None:
        session = requests.Session()

    http_sink = _NewsHttpCountSink()
    stocks_with_news = 0
    try:
        if config.NAVER_CLIENT_ID and config.NAVER_CLIENT_SECRET:
            if config.NEWS_NAVER_QUERY_MODE == "ticker":
                # 종목별 워커가 각자 Session 사용. 전달된 session은 사용하지 않음.
                items, stocks_with_news = _fetch_news_day_naver_per_ticker(
                    target, http_sink=http_sink
                )
            elif config.NEWS_NAVER_QUERY_MODE == "both":
                market_items = _fetch_news_day_naver(target, session, http_sink=http_sink)
                ticker_items, stocks_with_news = _fetch_news_day_naver_per_ticker(
                    target, http_sink=http_sink
                )
                items = _merge_news_rows(market_items, ticker_items)
            else:
                items = _fetch_news_day_naver(target, session, http_sink=http_sink)
                stocks_with_news = 1 if items else 0
        elif config.NEWS_NAVER_QUERY_MODE in ("ticker", "both"):
            # 종목별 모드는 네이버 API 전용(RSS는 시장 쿼리만 지원)
            items = _fetch_news_day_google_rss(target, session, http_sink=http_sink)
            stocks_with_news = 1 if items else 0
        elif config.USE_GOOGLE_NEWS_RSS_FALLBACK:
            items = _fetch_news_day_google_rss(target, session, http_sink=http_sink)
            stocks_with_news = 1 if items else 0
        else:
            items = []
    finally:
        if own_session:
            session.close()

    _print_news_http_usage(target, http_sink, stocks_with_news)

    day_file.parent.mkdir(parents=True, exist_ok=True)
    items = _enrich_rows_with_stock_codes(items)
    day_file.write_text(json.dumps(items, ensure_ascii=False), encoding="utf-8")
    return items


def aggregate_news_text(news_rows: list[dict[str, str]]) -> str:
    """row 리스트의 title·description을 줄바꿈으로 이은 단일 문자열."""
    parts = []
    for n in news_rows:
        parts.append(n.get("title", ""))
        parts.append(n.get("description", ""))
    return "\n".join(parts)


def fetch_news_for_date_range(start: date, end: date) -> dict[date, list[dict[str, str]]]:
    """캘린더 구간의 각 일자별 뉴스(일자별 파일 캐시)."""
    out: dict[date, list[dict[str, str]]] = {}
    session = requests.Session()
    try:
        for d in _iter_days(start, end):
            out[d] = fetch_news_for_calendar_day(d, session=session)
    finally:
        session.close()
    return out


def _iter_days(start: date, end: date):
    """``start``~``end`` 캘린더일을 하루씩 yield."""
    from datetime import timedelta

    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _mock_news(target: date) -> list[dict[str, str]]:
    """API 없이 파이프라인 검증용."""
    return [
        {
            "title": f"[모의] {target} 코스피 상승 전망",
            "description": "반도체 바이오 이차전지 테마 주목",
            "link": "https://example.com/mock",
            "pub": _rfc822_kst(target, 9, 0),
        }
    ]
