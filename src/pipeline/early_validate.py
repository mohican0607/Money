"""CLI 날짜·시각 검증 — 가벼운 검사는 즉시, 거래일 판별만 캘린더 로드."""
from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from src import config

KST = ZoneInfo("Asia/Seoul")


def current_n_and_n_plus_one(
    *,
    now_kst: datetime | None = None,
) -> tuple[date, date]:
    """
    KST 현재 기준 매수일 ``N`` 과 관측일 ``N+1``(다음 거래일).

    - ``N``: 오늘이 거래일이면 오늘, 아니면 오늘 이전 마지막 거래일.
    - ``N+1``: ``N`` 다음 KRX 거래일.
    """
    from src import trading_calendar

    now = now_kst or datetime.now(KST)
    today = now.date()
    if trading_calendar.is_trading_day(today):
        n_day = today
    else:
        n_day = trading_calendar.last_trading_day_on_or_before(today)
    return n_day, trading_calendar.next_trading_day_after(n_day)


def _cutoff_error(n_day: date, *, now_kst: datetime | None = None) -> str | None:
    now = now_kst or datetime.now(KST)
    cutoff = datetime.combine(
        n_day,
        time(config.NEWS_CUTOFF_KST_HOUR, config.NEWS_CUTOFF_KST_MINUTE),
        tzinfo=KST,
    )
    if now < cutoff:
        return (
            f"N일({n_day.isoformat()}) "
            f"{config.NEWS_CUTOFF_KST_HOUR:02d}:"
            f"{config.NEWS_CUTOFF_KST_MINUTE:02d}부터 실행 가능합니다."
        )
    return None


def validate_range_from_to(
    d_from: date,
    d_to: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """
    ``python main.py From To`` 구간 실행 전 검증.

    - ``From``~``To`` 캘린더 구간(양 끝 포함).
    - ``To`` 는 현재 기준 ``N+1`` 거래일을 넘을 수 없음.
    - 구간에 현재 ``N+1`` 이 포함되면 N일 뉴스 컷오프 이후만 실행 가능.
    """
    if d_from > d_to:
        return f"구간 시작일({d_from.isoformat()})이 종료일({d_to.isoformat()})보다 늦습니다."
    n_day, n_plus_one = current_n_and_n_plus_one(now_kst=now_kst)
    if d_to > n_plus_one:
        return f"현재 기준으로 N+1일은 {n_plus_one.strftime('%Y%m%d')} 입니다."
    if d_to >= n_plus_one:
        return _cutoff_error(n_day, now_kst=now_kst)
    return None


def validate_range_n_plus_one(
    n_day: date,
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """호환 별칭 — 구간 ``From``~``To`` 검증."""
    return validate_range_from_to(n_day, t_day, now_kst=now_kst)


def validate_dated_t_day(
    t_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """
    ``python main.py T`` 단일 인자(관측일 T) 검증.

    - ``T`` 는 KRX 거래일이어야 함.
    - ``T`` 는 현재 기준 ``N+1`` 거래일을 넘을 수 없음.
    - ``T`` 가 현재 ``N+1`` 이면 N일 뉴스 컷오프 이후만 실행 가능.
    """
    from src import trading_calendar

    if not trading_calendar.is_trading_day(t_day):
        return f"관측일 T={t_day.isoformat()} 은(는) 거래일이 아닙니다."
    n_day, n_plus_one = current_n_and_n_plus_one(now_kst=now_kst)
    if t_day > n_plus_one:
        return f"현재 기준으로 N+1일은 {n_plus_one.strftime('%Y%m%d')} 입니다."
    if t_day == n_plus_one:
        return _cutoff_error(n_day, now_kst=now_kst)
    return None


def validate_dated_n_day(
    n_day: date,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """``python main.py N`` (N 지정) — 관측일 ``T=N`` 다음 거래일로 검증."""
    from src import trading_calendar

    if not trading_calendar.is_trading_day(n_day):
        return f"기준일 N={n_day.isoformat()} 은(는) 거래일이 아닙니다."
    t_day = trading_calendar.next_trading_day_after(n_day)
    return validate_dated_t_day(t_day, now_kst=now_kst)


def validate_cli_or_none(
    mode: str,
    arg_date: date | None,
    range_end: date | None,
    *,
    now_kst: datetime | None = None,
) -> str | None:
    """range·dated 모드에서 검증 실패 시 오류 메시지, 통과 시 ``None``."""
    if mode == "range" and arg_date is not None and range_end is not None:
        return validate_range_from_to(arg_date, range_end, now_kst=now_kst)
    if mode == "dated" and arg_date is not None:
        return validate_dated_t_day(arg_date, now_kst=now_kst)
    return None
