"""
지정 캘린더 일(또는 구간)의 뉴스를 **네이버 검색 API만** 사용해 받습니다 (RSS 폴백 없음).

  python scripts/fetch_news_naver_day.py 20260410
  python scripts/fetch_news_naver_day.py 20260401 20260410
  python scripts/fetch_news_naver_day.py 20260410 --force   # 해당일 연도·구 월별·구 평면 캐시 삭제 후 재조회
  python scripts/fetch_news_naver_day.py 20260401 20260410 --force  # 구간 전체 동일

시작일·종료일 순서가 뒤바뀌어 있으면 자동으로 맞바꿉니다.

사전 조건: 프로젝트 루트에 ``.env`` 에 ``NAVER_CLIENT_ID`` / ``NAVER_CLIENT_SECRET`` 설정.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

# config 로드 전에 RSS 폴백 끔 → 네이버만 사용
os.environ["MOCK_NEWS"] = "0"
os.environ["USE_GOOGLE_NEWS_RSS_FALLBACK"] = "0"

from src import config, news  # noqa: E402

if not config.NAVER_CLIENT_ID or not config.NAVER_CLIENT_SECRET:
    print(
        "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 가 없습니다.\n"
        "  1) 프로젝트 루트 ``Money/.env`` 에 키를 넣으세요 (프로젝트 루트 고정 로드).\n"
        "  2) 값 앞뒤 공백·UTF-8 BOM·따옴표는 자동 제거됩니다.\n"
        "  3) https://developers.naver.com 앱에서 **검색(뉴스)** API 사용을 켜세요.",
        file=sys.stderr,
    )
    sys.exit(1)


def _probe_naver_credentials() -> None:
    """키가 유효한지 한 번 호출로 확인. 401이면 종료."""
    headers = {
        "X-Naver-Client-Id": config.NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": config.NAVER_CLIENT_SECRET,
    }
    r = requests.get(
        config.NAVER_NEWS_URL,
        headers=headers,
        params={"query": "증시", "display": 1, "start": 1, "sort": "date"},
        timeout=20,
    )
    if r.status_code == 401:
        print(
            "네이버 API 401 Unauthorized — NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 이\n"
            "  잘못되었거나, 앱이 비활성화·만료되었습니다.\n"
            "  https://developers.naver.com/apps 에서 애플리케이션·비밀키를 확인하세요.",
            file=sys.stderr,
        )
        sys.exit(3)
    r.raise_for_status()


def _parse_yyyymmdd(s: str) -> date:
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        raise ValueError("날짜는 YYYYMMDD 8자리")
    y, m, dd = int(s[:4]), int(s[4:6]), int(s[6:8])
    return date(y, m, dd)


def _unlink_cache_range(d0: date, d1: date) -> None:
    d = d0
    while d <= d1:
        for p in news.naver_day_cache_paths_for_purge(d):
            if p.is_file():
                p.unlink()
                print(f"캐시 삭제: {p}")
        d += timedelta(days=1)


def main() -> int:
    argv = [a for a in sys.argv[1:] if a]
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    force = "--force" in argv
    argv = [a for a in argv if a != "--force"]

    if len(argv) == 1:
        d_start = d_end = _parse_yyyymmdd(argv[0])
    elif len(argv) == 2:
        d_start = _parse_yyyymmdd(argv[0])
        d_end = _parse_yyyymmdd(argv[1])
        if d_start > d_end:
            d_start, d_end = d_end, d_start
    else:
        print(
            "사용법: fetch_news_naver_day.py YYYYMMDD [YYYYMMDD] [--force]",
            file=sys.stderr,
        )
        return 2

    _probe_naver_credentials()

    if force:
        _unlink_cache_range(d_start, d_end)

    print("출처:", news.describe_news_fetch_source())
    print("NEWS_NAVER_QUERY_MODE:", config.NEWS_NAVER_QUERY_MODE)
    n_days = (d_end - d_start).days + 1
    print(f"구간: {d_start} ~ {d_end} ({n_days}일)")

    if d_start == d_end:
        d = d_start
        day_file = news.day_news_json_path(d)
        rows = news.fetch_news_for_calendar_day(d)
        print("건수:", len(rows))
        for i, r in enumerate(rows[:20]):
            t = (r.get("title") or "")[:90]
            print(f"  {i + 1}. {t}")
        if len(rows) > 20:
            print(f"  ... 외 {len(rows) - 20}건")
        print("저장:", day_file.resolve())
        return 0

    by_day = news.fetch_news_for_date_range(d_start, d_end)
    for d in sorted(by_day.keys()):
        rows = by_day[d]
        day_file = news.day_news_json_path(d)
        print(f"{d}: {len(rows)}건 -> {day_file.resolve()}")
    total_rows = sum(len(v) for v in by_day.values())
    print(f"일수 {len(by_day)}, 기사 행 합계 {total_rows}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as e:
        print(e, file=sys.stderr)
        raise SystemExit(2)
