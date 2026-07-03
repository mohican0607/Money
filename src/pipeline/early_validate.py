"""CLI 날짜·시각 검증 — ``exchange_calendars`` 등 무거운 의존 없이 즉시 실패."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from src import config

KST = ZoneInfo("Asia/Seoul")


def validate_range_n_plus_one(
    n_day: date,
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """
    ``python main.py N T`` 구간 실행 전 검증.

    - ``T`` 는 캘린더 기준 ``N+1`` 일이어야 함.
    - KST ``N`` 일 ``NEWS_CUTOFF_*``(기본 14:30) 시각부터 실행 가능.
    """
    expected_t = n_day + timedelta(days=1)
    if t_day != expected_t:
        return f"N+1일은 {expected_t.strftime('%Y%m%d')}일 입니다."
    now = now_kst or datetime.now(KST)
    cutoff = datetime.combine(
        n_day,
        time(config.NEWS_CUTOFF_KST_HOUR, config.NEWS_CUTOFF_KST_MINUTE),
        tzinfo=KST,
    )
    if now < cutoff:
        return f"N일({n_day.isoformat()}) 14:30분부터 실행 가능합니다."
    return None


def validate_dated_n_day(
    n_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """``python main.py N`` (N 지정) 단일 인자 검증."""
    return validate_range_n_plus_one(n_day, n_day + timedelta(days=1), now_kst=now_kst)


def validate_dated_t_day(
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """``python main.py T`` (관측일 T 지정) 단일 인자 검증."""
    return validate_range_n_plus_one(t_day - timedelta(days=1), t_day, now_kst=now_kst)


def validate_cli_or_none(
    mode: str,
    arg_date: date | None,
    range_end: date | None,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """range·dated 모드에서 검증 실패 시 오류 메시지, 통과 시 ``None``."""
    if mode == "range" and arg_date is not None and range_end is not None:
        return validate_range_n_plus_one(arg_date, range_end, now_kst=now_kst)
    if mode == "dated" and arg_date is not None:
        return validate_dated_t_day(arg_date, now_kst=now_kst)
    return None
