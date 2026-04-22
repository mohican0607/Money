"""
캘린더 일자별 네이버 뉴스를 data/cache/news/<naver>/YYYY/day_YYYYMMDD.json 에 백필합니다.

  python scripts/backfill_news.py
  python scripts/backfill_news.py --start 20060101 --end 20261231

- 이미 있는 day_*.json 은 건너뜁니다(기존 fetch_news_for_calendar_day 동작).
- NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 필수(MOCK_NEWS=1 이면 모의 데이터만 저장).
- 기간이 길면 네이버 API 일일 한도·실행 시간 제약으로 며칠에 나눠 돌려야 할 수 있습니다.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import requests
from tqdm import tqdm

from src import config, news


def _parse_yyyymmdd(s: str) -> date:
    """CLI 인자 ``YYYYMMDD`` 를 ``date`` 로 변환. 형식 오류 시 ``ValueError``."""
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        raise ValueError(f"날짜는 YYYYMMDD 8자리: {s}")
    y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
    return date(y, m, d)


def main() -> None:
    """
    ``--start``~``--end`` 캘린더 구간의 연도별 ``day_YYYYMMDD.json`` 을 채웁니다.

    이미 있는 날짜는 건너뜁니다. ``main.py`` 와 동일한 ``fetch_news_for_calendar_day`` 경로를 사용합니다.
    """
    p = argparse.ArgumentParser(description="뉴스 일자 캐시 백필")
    p.add_argument("--start", default="20060101", help="시작일 YYYYMMDD (기본 20060101)")
    p.add_argument("--end", default=None, help="종료일 YYYYMMDD (기본 오늘)")
    p.add_argument(
        "--sleep-day",
        type=float,
        default=0.15,
        help="일자마다 추가 대기(초), API 부담 완화 (기본 0.15)",
    )
    args = p.parse_args()

    start = _parse_yyyymmdd(args.start)
    end = _parse_yyyymmdd(args.end) if args.end else date.today()
    if start > end:
        start, end = end, start

    if config.MOCK_NEWS:
        print("MOCK_NEWS=1 — 모의 뉴스만 월별 day_*.json 에 씁니다.", file=sys.stderr)
    elif not (config.NAVER_CLIENT_ID and config.NAVER_CLIENT_SECRET) and not config.USE_GOOGLE_NEWS_RSS_FALLBACK:
        print(
            "NAVER 키도 없고 USE_GOOGLE_NEWS_RSS_FALLBACK=0 입니다. "
            ".env 에 네이버 키를 넣거나 USE_GOOGLE_NEWS_RSS_FALLBACK=1(기본) 또는 MOCK_NEWS=1 로 실행하세요.",
            file=sys.stderr,
        )
        sys.exit(1)

    news._CACHE_NEWS.mkdir(parents=True, exist_ok=True)
    total = (end - start).days + 1
    sess = requests.Session()
    n_skip = n_fetch = 0
    try:
        d = start
        for _ in tqdm(range(total), desc="뉴스 백필"):
            if news.day_news_cache_hit(d):
                n_skip += 1
            else:
                news.fetch_news_for_calendar_day(d, session=sess)
                n_fetch += 1
                if args.sleep_day > 0:
                    time.sleep(args.sleep_day)
            d += timedelta(days=1)
    finally:
        sess.close()

    print(f"완료: 스킵(기존 캐시) {n_skip}일 · 신규 수집 시도 {n_fetch}일")


if __name__ == "__main__":
    main()
