"""run_daily_1500.ps1 용: 오늘이 거래일이고 T=다음 거래일도 거래일이면 0."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import trading_calendar  # noqa: E402


def main() -> int:
    """
    일일 스케줄 스크립트용: 오늘이 거래일이고 ``next_trading_day_after(today)`` 도 거래일이면 0.

    비거래일 2, T 비거래일 3, T 없음 4 등 — 호출 측(예: PowerShell)에서 종료 코드로 분기.
    """
    today = date.today()
    if not trading_calendar.is_trading_day(today):
        return 2
    try:
        T = trading_calendar.next_trading_day_after(today)
    except ValueError:
        return 4
    if not trading_calendar.is_trading_day(T):
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
