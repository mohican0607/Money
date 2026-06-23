"""캘린더 일자별 뉴스 수집."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests
from tqdm import tqdm

from src import config, news

def _collect_calendar_days_for_trading_range(
    trading_days: list[date],
    *,
    include_target_calendar_days: bool = False,
    target_calendar_trading_days: frozenset[date] | None = None,
    omit_target_calendar_days: frozenset[date] | None = None,
) -> list[date]:
    """
    뉴스를 내려받아야 하는 **캘린더 일자** 집합을 만듭니다.

    각 거래일 ``t`` 에 대해 ``news.news_fetch_calendar_span(t)`` 의 연속 구간을 합치고,
    ``include_target_calendar_days`` 가 True이면 **모든** ``t`` 의 캘린더일(관측일 당일)도 포함합니다.
    ``target_calendar_trading_days`` 가 있으면, 그 집합에 들어 있는 ``t`` 만 당일 캘린더를 추가합니다
    (배치 리포트에서 비교 표·상승 이유용으로 **테스트 관측일만** 받을 때 사용).
    ``omit_target_calendar_days`` 에 있는 관측일 ``t`` 는 당일 추가를 생략합니다.
    """
    omit = omit_target_calendar_days or frozenset()
    s: set[date] = set()
    for t in trading_days:
        a, b = news.news_fetch_calendar_span(t)
        d = a
        while d <= b:
            s.add(d)
            d += timedelta(days=1)
        if include_target_calendar_days and t not in omit:
            s.add(t)
        elif (
            target_calendar_trading_days is not None
            and t in target_calendar_trading_days
            and t not in omit
        ):
            s.add(t)
    return sorted(s)


def _fetch_news_for_calendar_days(cal_days: list[date], *, fetch_until: date | None = None) -> dict:
    """
    ``<naver|google|mock>/YYYY/day_YYYYMMDD.json`` 단위로 뉴스를 채워 ``{날짜: rows}`` 딕셔너리를 만듭니다.

    종목코드별 조회가 아니라 **캘린더 일자별** 시장 뉴스입니다(네이버/RSS 쿼리는 날짜+증시 키워드).
    ``NEWS_FETCH_MAX_WORKERS`` > 1 이면 일자를 병렬로 처리(일자마다 별도 ``Session``).
    ``fetch_until`` 이 있으면 그 날짜를 초과한 미래 캘린더일은 조회하지 않고 빈 리스트로 둡니다.
    """
    if fetch_until is not None:
        allowed_days = [d for d in cal_days if d <= fetch_until]
        skipped_days = [d for d in cal_days if d > fetch_until]
    else:
        allowed_days = list(cal_days)
        skipped_days = []

    if not allowed_days:
        return {d: [] for d in skipped_days}

    if len(allowed_days) <= 1 or config.NEWS_FETCH_MAX_WORKERS <= 1:
        out: dict = {}
        sess = requests.Session()
        try:
            for d in tqdm(allowed_days, desc="뉴스(일자별 캐시)"):
                out[d] = news.fetch_news_for_calendar_day(d, session=sess)
        finally:
            sess.close()
        for d in skipped_days:
            out[d] = []
        return out

    def _one_day(day: date) -> tuple[date, list]:
        """단일 캘린더 일 뉴스를 세션 재사용으로 수집해 (일자, rows) 를 반환."""
        sess = requests.Session()
        try:
            return day, news.fetch_news_for_calendar_day(day, session=sess)
        finally:
            sess.close()

    out = {}
    with ThreadPoolExecutor(max_workers=config.NEWS_FETCH_MAX_WORKERS) as ex:
        futures = [ex.submit(_one_day, d) for d in allowed_days]
        for fut in tqdm(
            as_completed(futures),
            total=len(allowed_days),
            desc=f"뉴스(일자별·워커 {config.NEWS_FETCH_MAX_WORKERS})",
        ):
            d, rows = fut.result()
            out[d] = rows
    for d in skipped_days:
        out[d] = []
    return out
