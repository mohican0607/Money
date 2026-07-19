"""``python main.py From To`` · ``python main.py T`` CLI 검증."""
from __future__ import annotations

from datetime import date, datetime

from src import trading_calendar
from src.pipeline.early_validate import (
    current_n_and_n_plus_one,
    validate_dated_t_day,
    validate_range_from_to,
)


def test_current_n_plus_one_on_weekend() -> None:
    # 2026-07-04 토 → N=07-03(금), N+1=07-06(월)
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    n, t = current_n_and_n_plus_one(now_kst=now)
    assert n == date(2026, 7, 3)
    assert t == date(2026, 7, 6)


def test_range_ok_through_current_n_plus_one() -> None:
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    assert validate_range_from_to(
        date(2026, 7, 1), date(2026, 7, 6), now_kst=now
    ) is None


def test_range_ok_historical_only() -> None:
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    assert validate_range_from_to(
        date(2026, 7, 1), date(2026, 7, 3), now_kst=now
    ) is None


def test_range_rejects_beyond_current_n_plus_one() -> None:
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    assert validate_range_from_to(
        date(2026, 7, 1), date(2026, 7, 7), now_kst=now
    ) == "현재 기준으로 N+1일은 20260706 입니다."


def test_range_rejects_before_n_cutoff_when_includes_n_plus_one() -> None:
    # N=07-03, N+1=07-06, 장 마감 뉴스 컷오프 전
    now = datetime(2026, 7, 3, 15, 29, tzinfo=trading_calendar.KST)
    err = validate_range_from_to(date(2026, 7, 1), date(2026, 7, 6), now_kst=now)
    assert err is not None
    assert "15:30" in err
    assert "2026-07-03" in err


def test_range_ok_historical_before_cutoff() -> None:
    # 미래 N+1 미포함이면 장 마감 전에도 과거 구간 허용
    now = datetime(2026, 7, 3, 10, 0, tzinfo=trading_calendar.KST)
    assert validate_range_from_to(
        date(2026, 7, 1), date(2026, 7, 2), now_kst=now
    ) is None


def test_dated_t_ok_past_trading_day() -> None:
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    assert validate_dated_t_day(date(2026, 7, 3), now_kst=now) is None


def test_dated_t_ok_current_n_plus_one() -> None:
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    assert validate_dated_t_day(date(2026, 7, 6), now_kst=now) is None


def test_dated_t_rejects_beyond_n_plus_one() -> None:
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    assert validate_dated_t_day(date(2026, 7, 7), now_kst=now) == (
        "현재 기준으로 N+1일은 20260706 입니다."
    )


def test_dated_t_rejects_non_trading() -> None:
    now = datetime(2026, 7, 4, 16, 0, tzinfo=trading_calendar.KST)
    assert validate_dated_t_day(date(2026, 7, 4), now_kst=now) == (
        "관측일 T=2026-07-04 은(는) 거래일이 아닙니다."
    )


def test_dated_t_rejects_before_cutoff_for_forward() -> None:
    now = datetime(2026, 7, 3, 15, 29, tzinfo=trading_calendar.KST)
    err = validate_dated_t_day(date(2026, 7, 6), now_kst=now)
    assert err is not None
    assert "15:30" in err
