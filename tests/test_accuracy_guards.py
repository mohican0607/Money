"""전일 급등 추격 차단·연휴 freeze 재랭킹·슬레이트 Hit·당일 재료 태그."""
from __future__ import annotations

from datetime import date, datetime

from src import config, trading_calendar
from src.learning import support as miss_diag
from src.pipeline import support as ps
from src.prediction import prediction_ranking as prk
from src.prediction.predict import PredictionRow
from src.report.render import DayReport


def test_session_gap_weekend_vs_holiday() -> None:
    # 2026-10-02(목) → 2026-10-06(화): 개천절 연휴 갭
    assert trading_calendar.session_gap_calendar_days(date(2026, 10, 6)) >= 4
    assert trading_calendar.is_long_session_gap(date(2026, 10, 6))
    # 일반 주말 금→월 (2026-01-02→01-05) = 3일
    assert trading_calendar.session_gap_calendar_days(date(2026, 1, 5)) == 3
    assert not trading_calendar.is_long_session_gap(date(2026, 1, 5))


def test_long_gap_freeze_not_reused_on_observation_day() -> None:
    items = [
        {
            "code": "263600",
            "name": "덕우전자",
            "predicted_return_pct": 30.0,
            "confidence_tier": "high",
            "saved_on": "2026-10-02",
            "n_date": "2026-10-02",
        },
        {
            "code": "070300",
            "name": "퀀텀레일",
            "predicted_return_pct": 10.0,
            "confidence_tier": "high",
            "saved_on": "2026-10-02",
        },
    ]
    now = datetime(2026, 10, 6, 14, 30, tzinfo=trading_calendar.KST)
    assert ps._freeze_is_stale_long_gap(date(2026, 10, 6), items, now_kst=now)
    assert not ps._should_reuse_prediction_freeze(
        ignore_freeze_for_trading_day=False,
        frozen_items=items,
        observation_day=date(2026, 10, 6),
        now_kst=now,
    )
    # 연휴 전에 미리 계산해 둔 T=10/6 freeze 는 10/2에는 유지
    before = datetime(2026, 10, 2, 15, 40, tzinfo=trading_calendar.KST)
    assert not ps._freeze_is_stale_long_gap(date(2026, 10, 6), items, now_kst=before)


def test_long_gap_freeze_reused_after_same_day_refresh() -> None:
    items = [
        {
            "code": "000001",
            "predicted_return_pct": 12.0,
            "confidence_tier": "high",
            "saved_on": "2026-10-06",
        },
        {
            "code": "000002",
            "predicted_return_pct": 11.0,
            "confidence_tier": "high",
            "saved_on": "2026-10-06",
        },
    ]
    now = datetime(2026, 10, 6, 15, 40, tzinfo=trading_calendar.KST)
    assert ps._should_reuse_prediction_freeze(
        ignore_freeze_for_trading_day=False,
        frozen_items=items,
        observation_day=date(2026, 10, 6),
        now_kst=now,
    )


def test_hit_at_slate_matches_shown_count() -> None:
    m = prk.compute_hit_at_k_metrics(
        ["000001", "000002", "000003", "000004", "000005"],
        {"000001", "099999"},
        universe_size=1000,
        same_day_codes={"099999"},
        slate_k=5,
    )
    assert m["slate_k"] == 5
    assert m["hit_at_slate"] == 1
    assert m["hit_at_5"] == 1
    assert m["same_day_catalyst_count"] == 1
    assert m["predictable_big_count"] == 1
    assert m["actual_big_count"] == 2


def test_same_day_catalyst_tag_and_skip_pos_boost(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(config, "ML_INCREMENTAL_MISS_BOOST_ENABLED", True)
    from src.prediction import feedback_loop

    monkeypatch.setattr(feedback_loop, "INCREMENTAL_MISS_PATH", tmp_path / "miss.json")
    dr = DayReport(
        trading_day=date(2026, 10, 6),
        predictions=[
            PredictionRow("070300", "퀀텀레일", 1.0, 10.0, [], [], confidence_tier="high")
        ],
        rows_compare=[],
        false_negatives=[],
        news_titles_sample=[],
        news_highlight_terms=[],
        actual_big_movers=[
            {"code": "411080", "name": "샌즈랩", "ret_pct": 30.0},
        ],
    )
    missed, _ = miss_diag.build_miss_rows_for_day(dr)
    assert any(
        m["code"] == "411080" and "same_day_catalyst" in m["tags"] for m in missed
    )
    feedback_loop.append_incremental_miss_from_day_report(dr)
    pos, _neg = feedback_loop.load_incremental_miss_boost()
    assert ("2026-10-06", "411080") not in pos


def test_exhausted_name_not_in_display_slate() -> None:
    rows = [
        PredictionRow(
            "263600",
            "덕우전자",
            1.0,
            30.0,
            [],
            [],
            ret_lag1=0.2018,
            confidence_tier="high",
        ),
        PredictionRow(
            "011080",
            "형지I&C",
            0.9,
            7.0,
            [],
            [],
            ret_lag1=0.05,
            confidence_tier="high",
        ),
    ]
    out = ps._display_prediction_rows_for_freeze(rows)
    codes = {r.code for r in out}
    assert "263600" not in codes
    assert "011080" in codes
