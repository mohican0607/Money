"""
한국 거래소(XKRX) 영업일 판별·세션 나열·뉴스 윈도우 경계.

``exchange_calendars`` 의 ``XKRX`` 캘린더를 사용합니다. 장전 스케줄·휴장 반영 목적입니다.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as ecals
import pandas as pd

KST = ZoneInfo("Asia/Seoul")

_CAL = ecals.get_calendar("XKRX")


def is_trading_day(d: date) -> bool:
    """주어진 캘린더일이 KRX 정규 세션(거래일)이면 True."""
    ts = pd.Timestamp(d)
    return bool(_CAL.is_session(ts))


def last_trading_day_before(day_n: date) -> date:
    """
    캘린더 일자 ``day_n`` **시점 이전**의 마지막 거래일을 반환합니다(당일 장은 포함하지 않음).

    ``day_n`` 이 거래일이어도 그 전날 세션이 아니라 **그 이전** 마지막 세션입니다.
    """
    ts = pd.Timestamp(day_n)
    sessions = _CAL.sessions
    prior = sessions[sessions < ts]
    if len(prior) == 0:
        raise ValueError(f"No XKRX session before {day_n}")
    return prior[-1].date()


def trading_sessions_in_range(start: date, end: date) -> list[date]:
    """``start``~``end``(포함) 사이의 KRX 거래일을 날짜 리스트로 반환합니다."""
    s, e = pd.Timestamp(start), pd.Timestamp(end)
    sess = _CAL.sessions_in_range(s, e)
    return [x.date() for x in sess]


def news_window_for_target_trading_day(target: date) -> tuple[date, date]:
    """
    target 영업일 T의 전일 뉴스 윈도우 [start, end] (양 끝 포함, 캘린더일).

    - end = T - 1일
    - start = T 직전(장 전) 마지막 영업일

    장이 끊긴 뒤 첫 영업일이면, start~end 사이에 비거래일 뉴스도 포함.

    (참고) 장중 매수 시나리오용 직전 거래일 컷오프(KST, ``config.NEWS_CUTOFF_*``)는 news.aggregate_early_late_for_target 사용.
    """
    end = target - timedelta(days=1)
    start = last_trading_day_before(target)
    if start > end:
        start = end
    return start, end


def calendar_days_inclusive(start: date, end: date) -> list[date]:
    """캘린더 연속 일자 ``start``~``end`` 양 끝 포함(휴장 포함). 거래일 필터 없음."""
    out = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=1)
    return out


def next_trading_day_after(d: date) -> date:
    """
    캘린더일 ``d`` 의 **다음 날부터** 스캔하여 첫 거래일을 반환합니다.

    CLI에서 기준일 N에 대해 관측일 T = N+1 거래일을 정할 때 사용합니다.
    """
    x = d + timedelta(days=1)
    for _ in range(400):
        if is_trading_day(x):
            return x
        x += timedelta(days=1)
    raise ValueError(f"No XKRX session found after {d}")


def last_trading_day_on_or_before(d: date) -> date:
    """캘린더일 ``d``(포함) 이전으로 스캔해 가장 가까운 KRX 거래일."""
    x = d
    for _ in range(400):
        if is_trading_day(x):
            return x
        x -= timedelta(days=1)
    raise ValueError(f"No XKRX session on or before {d}")


def ohlcv_request_end_cap_today() -> date:
    """
    일봉 OHLCV 요청 시 ``end`` 상한으로 쓸 날짜.

    거래일 당일이 **아직 정규장 종가(15:30 KST) 전**이면 당일 봉은 소스에 없거나 불완전한 경우가
    많아 **전 거래일**까지만 요청합니다. (개장 전·장중 모두 포함.)

    그 외(전일 이전 캘린더일, 또는 당일 장 마감 후)에는 KST 기준 오늘(포함) 이전의
    가장 가까운 거래일입니다.
    """
    now_kst = datetime.now(KST)
    today_ = now_kst.date()
    if is_trading_day(today_) and is_before_krx_regular_close_kst(today_, now_kst=now_kst):
        return last_trading_day_before(today_)
    return last_trading_day_on_or_before(today_)


def is_before_krx_regular_open_kst(cal_day: date, *, now_kst: datetime | None = None) -> bool:
    """
    한국 현물 정규장 개장(09:00 KST) 전인지 여부.

    ``cal_day`` 가 오늘(KST)과 같고 거래일일 때만 의미가 있습니다.
    (장전·휴장 등 세부 스케줄은 단순화해 09:00 기준.)
    """
    if now_kst is None:
        now_kst = datetime.now(KST)
    if now_kst.date() != cal_day:
        return False
    if not is_trading_day(cal_day):
        return False
    open_today = datetime.combine(cal_day, time(9, 0), tzinfo=KST)
    return now_kst < open_today


def is_before_krx_regular_close_kst(cal_day: date, *, now_kst: datetime | None = None) -> bool:
    """
    한국 현물 정규장 종가(15:30 KST) 전인지 여부.

    ``cal_day`` 가 오늘(KST)과 같고 거래일일 때만 의미가 있습니다.
    (일봉 OHLCV는 장 마감 후에야 확정되는 경우가 많아, 마감 전에는 당일 봉 보강을 생략하는 데 씁니다.)
    """
    if now_kst is None:
        now_kst = datetime.now(KST)
    if now_kst.date() != cal_day:
        return False
    if not is_trading_day(cal_day):
        return False
    close_today = datetime.combine(cal_day, time(15, 30), tzinfo=KST)
    return now_kst < close_today


def is_krx_daily_bar_effective_closed(session_day: date, *, now_kst: datetime | None = None) -> bool:
    """
    해당 캘린더일 기준으로 **당일 현물 정규장 일봉이 통상적으로 확정된 뒤**로 볼 수 있으면 True.

    - ``session_day`` 가 KST 오늘보다 이전이면 True(과거 거래일·휴일 모두, 이미 지난 날짜).
    - 오늘과 같으면: 비거래일이면 True, 거래일이면 정규장 15:30 KST 이후만 True.
    - 미래 날짜는 False.
    """
    if now_kst is None:
        now_kst = datetime.now(KST)
    today = now_kst.date()
    if session_day > today:
        return False
    if session_day < today:
        return True
    if not is_trading_day(session_day):
        return True
    return not is_before_krx_regular_close_kst(session_day, now_kst=now_kst)
