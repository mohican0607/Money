"""거래일·OHLCV 상한·관측 모드 판단."""
from __future__ import annotations

from datetime import date, datetime, timedelta

from src import config, report, snapshot_rebuild_learning, trading_calendar

def _resolve_pipeline_ohlcv_end(
    end_date: date,
    test_days: Sequence[date],
    *,
    now_kst: datetime,
) -> date:
    """
    일봉 OHLCV 요청 ``end`` 날짜.

    ``ohlcv_request_end_cap_today()`` 상한에 더해, 이미 마감·확정된 관측 거래일(``test_days``)은
    구간 종료일이 더 뒤여도 해당 일까지 요청합니다. (캐시가 전 거래일까지만 있어도 5/18 실제값 조회)
    """
    cap = trading_calendar.ohlcv_request_end_cap_today()
    today = now_kst.date()
    ohlcv_end = min(end_date, cap)
    for t in test_days:
        if t > end_date or not trading_calendar.is_trading_day(t):
            continue
        if t > today:
            continue
        if t < today or trading_calendar.is_krx_daily_bar_effective_closed(
            t, now_kst=now_kst
        ):
            ohlcv_end = max(ohlcv_end, min(t, end_date))
    return min(ohlcv_end, end_date)


def _observation_day_forward_mode(
    t: date,
    *,
    today: date,
    pipeline_forward_only: bool,
    now_kst: datetime | None = None,
) -> bool:
    """
    관측일 ``T`` 를 예측 전용(실적·당일 테마 미확정)으로 볼지.

    - ``T > today`` (미래 거래일)
    - ``T == today`` 이고 **정규장 개장(09:00 KST) 전** — 장 시작 전 실행
    - ``pipeline_forward_only`` (레거시 일괄 플래그; dated/daily 경로는 아래 규칙 우선)
    """
    if now_kst is None:
        now_kst = datetime.now(trading_calendar.KST)
    if t > today:
        return True
    if (
        t == today
        and trading_calendar.is_trading_day(t)
        and trading_calendar.is_before_krx_regular_open_kst(t, now_kst=now_kst)
    ):
        return True
    if pipeline_forward_only:
        return True
    return False


def _omit_target_calendar_before_close(
    test_days: Sequence[date],
    *,
    now_kst: datetime,
) -> frozenset[date]:
    """
    관측 거래일 ``T`` 가 KST ``오늘`` 이고 14:30 전이면, 해당 ``T`` 캘린더 뉴스는 받지 않습니다.

    예측 입력은 직전 거래일 ``NEWS_CUTOFF_*`` 까지이므로 당일 ``T`` 본장 전 종목·시장 뉴스는 불필요합니다.
    """
    today = now_kst.date()
    out: list[date] = []
    for t in test_days:
        if (
            t == today
            and trading_calendar.is_trading_day(t)
            and (now_kst.hour, now_kst.minute) < (14, 30)
        ):
            out.append(t)
    return frozenset(out)


def _should_skip_ohlcv_right_gap(n_day: date, *, now_kst: datetime) -> bool:
    """
    N일 기준일 실행에서, 아직 존재하지 않는 일봉을 채우려는 우측 OHLCV 보강을 건너뛸지.

    - N이 KST 오늘보다 미래이거나
    - N이 KST 오늘인 거래일이면서 정규장 마감(15:30 KST) 전이면 True.
    """
    tk = now_kst.date()
    if n_day > tk:
        return True
    if n_day == tk and trading_calendar.is_trading_day(n_day):
        return trading_calendar.is_before_krx_regular_close_kst(n_day, now_kst=now_kst)
    return False


def _refresh_incomplete_market_theme_on_day_reports(
    day_reports: list[report.DayReport],
    *,
    news_by_calendar: dict,
    returns_df,
    listing_names: dict[str, str],
    news_cutoff_label: str,
    now_kst: datetime | None = None,
) -> None:
    """장 마감 후에도 테마 요약이 placeholder 인 ``DayReport`` 를 종가·뉴스 기준으로 다시 채웁니다."""
    now = now_kst or datetime.now(trading_calendar.KST)
    refreshed = 0
    for dr in day_reports:
        if dr.forward_observation:
            continue
        if not trading_calendar.is_krx_daily_bar_effective_closed(
            dr.trading_day, now_kst=now
        ):
            continue
        if not snapshot_rebuild_learning.market_theme_html_is_incomplete(
            dr.market_theme_html
        ):
            continue
        theme_inner = snapshot_rebuild_learning.market_theme_html_for_trading_day(
            dr.trading_day,
            news_by_calendar,
            returns_df,
            listing_names,
            news_cutoff_label=news_cutoff_label,
        )
        if snapshot_rebuild_learning.market_theme_html_is_incomplete(theme_inner):
            continue
        flow_rows = snapshot_rebuild_learning.build_market_theme_flow(
            [dr.trading_day],
            news_by_calendar,
            returns_df,
            listing_names,
        )
        flow_row = flow_rows[0] if flow_rows else None
        early_rows = snapshot_rebuild_learning._early_news_rows_for_trading_day(
            news_by_calendar, dr.trading_day
        )
        if flow_row and dr.rows_compare:
            snapshot_rebuild_learning._patch_flow_row_strong_movers_from_compare(
                flow_row, dr.rows_compare, listing_names, early_rows
            )
        dr.market_theme_html = (
            snapshot_rebuild_learning.format_market_theme_flow_html(
                flow_row,
                news_cutoff_label=news_cutoff_label,
                threshold_pct=float(config.BIG_MOVE_THRESHOLD) * 100.0,
            )
            if flow_row
            else theme_inner
        )
        snapshot_rebuild_learning.enrich_compare_rows_theme_for_day(
            dr,
            flow_row,
            listing_names=listing_names,
            early_rows=early_rows,
            news_cutoff_label=news_cutoff_label,
        )
        refreshed += 1
    if refreshed:
        print(
            f"장 마감 반영: 당일 테마 요약 재생성 {refreshed}일(이번 실행 거래일)",
            flush=True,
        )
