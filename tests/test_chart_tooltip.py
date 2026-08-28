"""리포트 차트 N 마커·tooltip 등락률 보강."""
from __future__ import annotations

from datetime import date, datetime

from src import trading_calendar
from src.pipeline import observation_day_forward_mode
from src.pipeline.rows import (
    _enrich_forward_day_actual_display_rows,
    _enrich_rows_stock_ret_tooltip,
)
from src.report.render import (
    format_forward_actual_ret_cell,
    format_stock_ret_column_lines,
    format_stock_ret_tooltip_lines,
    stock_chart_n_bar_offset,
)


def test_forward_mode_true_during_regular_session_before_close() -> None:
    now = datetime(2026, 7, 3, 10, 30, tzinfo=trading_calendar.KST)
    assert observation_day_forward_mode(
        date(2026, 7, 3),
        today=date(2026, 7, 3),
        pipeline_forward_only=False,
        now_kst=now,
    )


def test_forward_mode_false_after_close() -> None:
    now = datetime(2026, 7, 3, 16, 0, tzinfo=trading_calendar.KST)
    assert not observation_day_forward_mode(
        date(2026, 7, 3),
        today=date(2026, 7, 3),
        pipeline_forward_only=False,
        now_kst=now,
    )


def test_stale_intraday_preserved_section_detects_actual_column() -> None:
    from src.report.render import preserved_day_section_is_stale_intraday_closed_ui

    now = datetime(2026, 7, 3, 10, 0, tzinfo=trading_calendar.KST)
    html = '<section id="day-2026-07-03"><th>실제 상승률(%)</th></section>'
    assert preserved_day_section_is_stale_intraday_closed_ui(
        html, date(2026, 7, 3), now_kst=now
    )
    forward_html = '<section class="day-forward-obs"><th>예측</th></section>'
    assert not preserved_day_section_is_stale_intraday_closed_ui(
        forward_html, date(2026, 7, 3), now_kst=now
    )


def test_naver_chart_axis_end_is_today_during_regular_session() -> None:
    now = datetime(2026, 7, 3, 10, 30, tzinfo=trading_calendar.KST)
    assert trading_calendar.naver_chart_axis_end_kst(now_kst=now) == date(2026, 7, 3)
    assert trading_calendar.ohlcv_request_end_cap(now_kst=now) == date(2026, 7, 2)


def test_forward_today_n_line_is_live_intraday_without_baked_pct(monkeypatch) -> None:
    t = date(2026, 7, 3)
    now = datetime(2026, 7, 3, 10, 0, tzinfo=trading_calendar.KST)
    rows = [{"code": "017000"}]

    def _fake_return(returns, code, td):
        if td == date(2026, 6, 30):
            return 0.0
        if td == date(2026, 7, 1):
            return 0.2869
        if td == date(2026, 7, 2):
            return -0.0682
        return None

    monkeypatch.setattr(
        "src.pipeline.rows.stocks.actual_return_on_date",
        _fake_return,
    )

    _enrich_rows_stock_ret_tooltip(
        rows,
        None,
        t,
        forward_observation=True,
        now_kst=now,
    )
    tips = rows[0]["stock_ret_tooltip"]
    assert len(tips) == 4
    labels = [x["label"] for x in tips]
    assert labels == ["N", "N-1", "N-2", "N-3"]
    assert rows[0]["report_n_day"] == "2026-07-03"
    assert rows[0]["stock_chart_n_bar_offset"] == 0
    n_item = next(x for x in tips if x["label"] == "N")
    assert n_item["intraday"] is True
    assert n_item["pct"] is None
    html = format_stock_ret_tooltip_lines(rows[0])
    assert "0630 (N-3)" in html
    assert "0703 (N  )" in html
    assert "0703·장중" not in html
    assert "data-live-intraday" in html
    assert "…" in html


def test_forward_stock_ret_column_lines() -> None:
    row = {
        "code": "017000",
        "stock_ret_tooltip": [
            {"label": "N", "date": "2026-07-03", "intraday": True, "pct": None},
            {"label": "N-1", "date": "2026-07-02", "pct": -6.82},
        ],
    }
    html = format_stock_ret_column_lines(row)
    assert "stock-ret-col-lines" in html
    assert "0702 (N-1)" in html
    assert "-6.82 %" in html
    assert 'data-stock-code="017000"' in html
    assert format_stock_ret_column_lines({}) == "—"


def test_forward_future_t_n_chain_uses_observation_day_as_n(monkeypatch) -> None:
    """T=관측일(캘린더)이 미래일 때도 N=T, N-1.. 은 직전 거래일."""
    t = date(2026, 7, 4)  # 토요일(비거래일) 관측일
    now = datetime(2026, 7, 3, 14, 30, tzinfo=trading_calendar.KST)
    rows = [{"code": "017000"}]

    def _fake_return(returns, code, td):
        m = {
            date(2026, 7, 1): 0.01,
            date(2026, 7, 2): 0.02,
            date(2026, 7, 3): 0.03,
        }
        return m.get(td)

    monkeypatch.setattr(
        "src.pipeline.rows.stocks.actual_return_on_date",
        _fake_return,
    )

    _enrich_rows_stock_ret_tooltip(
        rows,
        None,
        t,
        forward_observation=True,
        now_kst=now,
    )
    tips = {x["label"]: x for x in rows[0]["stock_ret_tooltip"]}
    assert tips["N"]["date"] == "2026-07-04"
    assert tips["N-1"]["date"] == "2026-07-03"
    assert tips["N-2"]["date"] == "2026-07-02"
    assert tips["N-3"]["date"] == "2026-07-01"
    assert tips["N"]["intraday"] is False
    assert rows[0]["report_n_day"] == "2026-07-04"


def test_trading_sessions_in_range_skips_weekend() -> None:
    days = trading_calendar.trading_sessions_in_range(date(2026, 7, 1), date(2026, 7, 6))
    assert days == [
        date(2026, 7, 1),
        date(2026, 7, 2),
        date(2026, 7, 3),
        date(2026, 7, 6),
    ]


def test_calendar_range_includes_non_trading_end() -> None:
    days = trading_calendar.calendar_days_inclusive(date(2026, 7, 3), date(2026, 7, 4))
    assert days == [date(2026, 7, 3), date(2026, 7, 4)]


def test_forward_day_actual_ret_cell_is_dash() -> None:
    row = {
        "forward_observation": True,
        "actual_ret_intraday_pct": 4.11,
        "stock_ret_tooltip": [{"label": "N", "pct": 4.11}],
    }
    assert format_forward_actual_ret_cell(row) == "—"


def test_forward_day_actual_display_rows_clear_actual(monkeypatch) -> None:
    t = date(2026, 7, 3)
    now = datetime(2026, 7, 3, 10, 0, tzinfo=trading_calendar.KST)
    rows = [{"code": "017000", "actual_ret": 0.2, "actual_big": True}]
    _enrich_forward_day_actual_display_rows(rows, None, t, now_kst=now)
    assert rows[0]["forward_observation"] is True
    assert rows[0].get("actual_ret") is None
    assert rows[0]["actual_big"] is False


def test_chart_offset_none_when_n_after_axis_end() -> None:
    row = {"report_n_day": "2026-07-03", "stock_chart_n_bar_offset": None}
    assert stock_chart_n_bar_offset(row) is None
