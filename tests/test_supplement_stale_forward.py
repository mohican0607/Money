"""장후 미갱신 예측전용 일자 자동 보강."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.pipeline import batch
from src.pipeline.types import PipelineOut
from src.report.render import DayReport, preserved_day_section_is_stale_forward_observation

KST = ZoneInfo("Asia/Seoul")


def _empty_po(*days: date, forward: bool = False) -> PipelineOut:
    reports = [
        DayReport(
            trading_day=d,
            predictions=[],
            rows_compare=[],
            false_negatives=[],
            news_titles_sample=[],
            news_highlight_terms=[],
            actual_big_movers=[],
            forward_observation=forward,
        )
        for d in days
    ]
    return PipelineOut(
        day_reports=reports,
        news_source="test",
        correlation_rows=[],
        train_start=date(2025, 1, 1),
        test_start=date(2026, 1, 1),
        end_date=max(days) if days else date(2026, 8, 21),
        late_below_n=0,
        late_below_kw=0,
        late_gte_n=0,
        late_gte_kw=0,
        movers_data_note="",
        returns_df=None,
        news_by_calendar={},
        listing_names={},
    )


def test_supplement_skips_before_close(monkeypatch) -> None:
    po = _empty_po(date(2026, 8, 24), forward=True)
    monkeypatch.setattr(
        "src.pipeline.batch.config.PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD", True
    )
    monkeypatch.setattr(
        "src.pipeline.batch.trading_calendar.is_trading_day", lambda _d: True
    )
    monkeypatch.setattr(
        "src.pipeline.batch.trading_calendar.is_before_krx_regular_close_kst",
        lambda _d, now_kst=None: True,
    )
    monkeypatch.setattr(
        "src.pipeline.batch.datetime",
        type(
            "D",
            (),
            {
                "now": staticmethod(
                    lambda tz=None: datetime(2026, 8, 21, 14, 35, tzinfo=KST)
                ),
                "combine": datetime.combine,
            },
        ),
    )
    out = batch._supplement_stale_forward_preserved_reports(
        po, merge_existing_monthly_days=True
    )
    assert out is po


def test_stale_forward_detection_ignores_settled_pred_reason_header() -> None:
    now = datetime(2026, 8, 21, 16, 0, tzinfo=KST)
    settled = (
        '<section class="day-stack day-market-block" id="day-2026-08-20">'
        "<h2>2026-08-20</h2>"
        "<th>예측 근거</th><th>실제 상승률</th></section>"
    )
    forward = (
        '<section class="day-stack day-market-block day-forward-obs" '
        'id="day-2026-08-21"><h2>2026-08-21 '
        '<span class="pill">예측 전용</span></h2>'
        "<th>예측 근거</th></section>"
    )
    assert (
        preserved_day_section_is_stale_forward_observation(
            settled, date(2026, 8, 20), now_kst=now
        )
        is False
    )
    assert (
        preserved_day_section_is_stale_forward_observation(
            forward, date(2026, 8, 21), now_kst=now
        )
        is True
    )


def test_supplement_runs_after_close_even_with_forward_batch(
    monkeypatch, tmp_path: Path
) -> None:
    """장후 + 배치에 미래 예측전용 T 가 있어도 과거 stale 일자를 보강."""
    po = _empty_po(date(2026, 8, 24), forward=True)
    stale_day = date(2026, 8, 21)
    monthly = tmp_path / "report_2026.08.html"
    monthly.write_text(
        f'<section class="day-stack day-market-block day-forward-obs" '
        f'id="day-{stale_day.isoformat()}">'
        f"<h2>{stale_day.isoformat()} "
        f'<span class="pill">예측 전용</span></h2></section>',
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "src.pipeline.batch.config.PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD", True
    )
    monkeypatch.setattr(
        "src.pipeline.batch.trading_calendar.is_trading_day", lambda _d: True
    )
    monkeypatch.setattr(
        "src.pipeline.batch.trading_calendar.is_before_krx_regular_close_kst",
        lambda _d, now_kst=None: False,
    )
    monkeypatch.setattr(
        "src.pipeline.batch.trading_calendar.trading_day_actuals_finalized",
        lambda d, now_kst=None: d <= date(2026, 8, 21),
    )
    monkeypatch.setattr(
        "src.pipeline.batch._monthly_report_paths_for_po", lambda _po: [monthly]
    )
    monkeypatch.setattr(
        "src.pipeline.batch.datetime",
        type(
            "D",
            (),
            {
                "now": staticmethod(
                    lambda tz=None: datetime(2026, 8, 21, 15, 40, tzinfo=KST)
                ),
                "combine": datetime.combine,
            },
        ),
    )

    called: dict = {}

    def fake_run(test_days, end_date, **kwargs):
        called["days"] = list(test_days)
        called["kwargs"] = kwargs
        settled = _empty_po(stale_day, forward=False)
        settled.news_by_calendar = {}
        settled.listing_names = {}
        return settled

    monkeypatch.setattr("src.pipeline.run._run_pipeline", fake_run)

    out = batch._supplement_stale_forward_preserved_reports(
        po, merge_existing_monthly_days=True
    )
    assert called["days"] == [stale_day]
    assert called["kwargs"].get("respect_prediction_freeze") is True
    assert called["kwargs"].get("train_snapshot_mode") == "use"
    days = {dr.trading_day: dr for dr in out.day_reports}
    assert date(2026, 8, 24) in days
    assert date(2026, 8, 21) in days
    assert days[date(2026, 8, 21)].forward_observation is False
