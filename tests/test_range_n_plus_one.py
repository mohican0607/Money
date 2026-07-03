"""``python main.py N N+1`` · ``python main.py T`` 구간·단일 인자 검증."""
from __future__ import annotations

from datetime import date, datetime

from src import trading_calendar
from src.pipeline.early_validate import validate_dated_n_day, validate_dated_t_day, validate_range_n_plus_one


def test_validate_ok_at_n_cutoff() -> None:
    n, t = date(2026, 7, 3), date(2026, 7, 4)
    now = datetime(2026, 7, 3, 14, 30, tzinfo=trading_calendar.KST)
    assert validate_range_n_plus_one(n, t, now_kst=now) is None


def test_validate_ok_after_n_cutoff() -> None:
    n, t = date(2026, 7, 3), date(2026, 7, 4)
    now = datetime(2026, 7, 3, 14, 31, tzinfo=trading_calendar.KST)
    assert validate_range_n_plus_one(n, t, now_kst=now) is None


def test_validate_ok_on_later_calendar_day() -> None:
    n, t = date(2026, 7, 3), date(2026, 7, 4)
    now = datetime(2026, 7, 4, 9, 0, tzinfo=trading_calendar.KST)
    assert validate_range_n_plus_one(n, t, now_kst=now) is None


def test_validate_rejects_wrong_t() -> None:
    n = date(2026, 7, 3)
    now = datetime(2026, 7, 3, 15, 0, tzinfo=trading_calendar.KST)
    assert validate_range_n_plus_one(n, date(2026, 7, 6), now_kst=now) == (
        "N+1일은 20260704일 입니다."
    )


def test_validate_dated_t_ok() -> None:
    t = date(2026, 7, 4)
    now = datetime(2026, 7, 3, 14, 30, tzinfo=trading_calendar.KST)
    assert validate_dated_t_day(t, now_kst=now) is None


def test_validate_dated_n_ok() -> None:
    n = date(2026, 7, 3)
    now = datetime(2026, 7, 3, 14, 30, tzinfo=trading_calendar.KST)
    assert validate_dated_n_day(n, now_kst=now) is None


def test_validate_dated_t_rejects_before_cutoff() -> None:
    t = date(2026, 7, 4)
    now = datetime(2026, 7, 3, 14, 29, tzinfo=trading_calendar.KST)
    err = validate_dated_t_day(t, now_kst=now)
    assert err is not None
    assert "14:30" in err


def test_validate_dated_n_rejects_before_cutoff() -> None:
    n = date(2026, 7, 3)
    now = datetime(2026, 7, 3, 14, 29, tzinfo=trading_calendar.KST)
    err = validate_dated_n_day(n, now_kst=now)
    assert err is not None
    assert "14:30" in err


def test_validate_rejects_before_n_cutoff() -> None:
    n, t = date(2026, 7, 3), date(2026, 7, 4)
    now = datetime(2026, 7, 3, 14, 29, tzinfo=trading_calendar.KST)
    err = validate_range_n_plus_one(n, t, now_kst=now)
    assert err is not None
    assert "14:30" in err
    assert "2026-07-03" in err
