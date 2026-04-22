"""
구 뉴스 캐시를 연도 폴더 레이아웃(``<provider>/YYYY/day_*.json``)으로 옮깁니다.

  python scripts/migrate_news_cache_to_monthly.py

(파일명은 과거 월별 마이그레이션 스크립트에서 이어졌습니다.)

- ``news/day_YYYYMMDD.json`` (평면) → ``news/naver/YYYY/day_*.json``
- ``news/YYYYMM/*.json`` (출처 미구분 월 폴더) → ``news/naver/YYYY/``
- ``news/<naver|google|mock>/YYYYMM/*.json`` → ``news/<provider>/YYYY/``
- 대상에 동일 파일이 있으면 건너뜀(충돌 카운트).
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src import news

_DAY_RE = re.compile(r"^day_(\d{8})(?:_.+)?\.json$")
_PROVIDER_DIRS = frozenset({"naver", "google", "mock", "none"})


def main() -> int:
    news._CACHE_NEWS.mkdir(parents=True, exist_ok=True)
    moved = conflict = 0

    def try_move_to_provider_year(src: Path, provider: str, year_key: str, fname: str) -> None:
        nonlocal moved, conflict
        dest_dir = news._CACHE_NEWS / provider / year_key
        dest = dest_dir / fname
        if dest.is_file():
            conflict += 1
            print(f"건너뜀(대상 존재): {src} → {dest}")
            return
        dest_dir.mkdir(parents=True, exist_ok=True)
        src.replace(dest)
        moved += 1

    for f in sorted(news._CACHE_NEWS.glob("day_*.json")):
        if not f.is_file():
            continue
        m = _DAY_RE.match(f.name)
        if not m:
            continue
        ymd = m.group(1)
        try_move_to_provider_year(f, "naver", ymd[:4], f.name)

    for sub in sorted(news._CACHE_NEWS.iterdir()):
        if not sub.is_dir() or sub.name in _PROVIDER_DIRS:
            continue
        if len(sub.name) != 6 or not sub.name.isdigit():
            continue
        year_key = sub.name[:4]
        for f in sorted(sub.glob("*.json")):
            if not f.is_file():
                continue
            try_move_to_provider_year(f, "naver", year_key, f.name)
        try:
            sub.rmdir()
        except OSError:
            pass

    for provider in ("naver", "google", "mock"):
        pbase = news._CACHE_NEWS / provider
        if not pbase.is_dir():
            continue
        for sub in sorted(pbase.iterdir()):
            if not sub.is_dir():
                continue
            if len(sub.name) != 6 or not sub.name.isdigit():
                continue
            year_key = sub.name[:4]
            for f in sorted(sub.glob("*.json")):
                if not f.is_file():
                    continue
                try_move_to_provider_year(f, provider, year_key, f.name)
            try:
                sub.rmdir()
            except OSError:
                pass

    print(f"완료: 이동 {moved}개 · 충돌(수동 확인) {conflict}개")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
