"""
전종목 OHLCV parquet 캐시만 생성합니다 (main 파이프라인 없이).

  python scripts/build_ohlcv_full_cache.py
      → 구간: TRAIN_START_DEFAULT - 10일 ~ 오늘 (config 와 메인 파이프라인과 동일한 시작)

  python scripts/build_ohlcv_full_cache.py 20260101 20260410
      → 지정 YYYYMMDD ~ YYYYMMDD (역순이면 자동 교환)

- 결과: data/cache/ohlcv_long_full.parquet (USE_KRX_OHLCV=1 이면 ohlcv_long_krx_full.parquet)
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta


def _ensure_utf8_console() -> None:
    """Windows 콘솔 기본 코드 페이지와 UTF-8 출력 불일치로 한글이 깨지는 것을 완화합니다."""
    if sys.platform != "win32":
        return
    try:
        import ctypes

        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        ctypes.windll.kernel32.SetConsoleCP(65001)
    except Exception:
        pass
    for stream in (sys.stdout, sys.stderr):
        reconf = getattr(stream, "reconfigure", None)
        if reconf is not None:
            try:
                reconf(encoding="utf-8", errors="replace")
            except Exception:
                pass


_ensure_utf8_console()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src import config, stocks


def _parse_yyyymmdd(s: str) -> date:
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        raise ValueError("날짜는 YYYYMMDD 8자리 두 개 또는 인자 생략")
    y, m, d = int(s[:4]), int(s[4:6]), int(s[6:8])
    return date(y, m, d)


def main() -> None:
    """
    전종목 OHLCV만 받아 Parquet 캐시를 만듭니다. 인자 없으면 config 기본 구간, 두 개면 그 구간.
    """
    argv = [a for a in sys.argv[1:] if a]
    if len(argv) >= 2:
        start = _parse_yyyymmdd(argv[0])
        end = _parse_yyyymmdd(argv[1])
        if start > end:
            start, end = end, start
    elif len(argv) == 1:
        print(
            "인자가 하나만 있습니다. 구간을 쓰려면 YYYYMMDD YYYYMMDD 두 개를 주세요.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    else:
        end = date.today()
        start = config.TRAIN_START_DEFAULT - timedelta(days=10)

    print(f"구간 {start} ~ {end}, force_full_listing=True", flush=True)
    print(
        "진행 로그: 상장목록/디스크는 stdout, tqdm 진행률은 stderr에 출력됩니다.",
        flush=True,
    )
    df = stocks.build_ohlcv_long(start, end, force_full_listing=True)
    print(f"완료: {len(df)}행 -> {stocks.ohlcv_parquet_path(full_universe=True)}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except ValueError as e:
        print(e, file=sys.stderr)
        raise SystemExit(2)
