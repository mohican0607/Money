from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from src import config, stocks
from src.prediction import ml_move_rank, prediction_ranking


def _price_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2026-06-01", periods=8)
    close = [100, 102, 101, 105, 108, 110, 112, 115]
    rows = []
    for i, (day, value) in enumerate(zip(dates, close)):
        rows.append(
            {
                "Date": day,
                "Code": "000001",
                "Name": "테스트",
                "Open": value - 1,
                "High": value + 2,
                "Low": value - 2,
                "Close": value,
                "Volume": 1000 + i * 100,
                "return_pct": 0.0 if i == 0 else value / close[i - 1] - 1.0,
            }
        )
    return pd.DataFrame(rows)


def test_forecast_alignment_synthesizes_same_lag_features_as_historical_row() -> None:
    base = _price_frame()
    enriched = stocks.enrich_daily_returns_for_ml(base)
    target = enriched["Date"].iloc[-1].date()

    expected = enriched.loc[enriched["Date"] == pd.Timestamp(target)].iloc[0]
    history_only = enriched.loc[enriched["Date"] < pd.Timestamp(target)]
    aligned = stocks.align_returns_ml_for_forecast(history_only, target)
    actual = aligned.loc[aligned["Date"] == pd.Timestamp(target)].iloc[0]

    for column in (
        "ret_lag1",
        "ret_lag3",
        "log_vol_lag1",
        "ret_roll_std5",
        "log_vol_roll_mean5",
        "close_ma20_ratio",
        "close_ma5_ratio",
        "ret_roll_mean5",
        "ret_max5",
        "ret_min5",
        "vol_surge_ratio",
        "hl_range_lag1",
    ):
        assert float(actual[column]) == pytest.approx(float(expected[column]))
    assert float(actual["open_gap"]) == 0.0


def test_forecast_alignment_removes_historical_target_open_gap() -> None:
    enriched = stocks.enrich_daily_returns_for_ml(_price_frame())
    target = enriched["Date"].iloc[-1].date()
    assert float(enriched.loc[enriched["Date"] == pd.Timestamp(target), "open_gap"].iloc[0]) != 0.0

    aligned = stocks.align_returns_ml_for_forecast(enriched, target)

    assert float(aligned.loc[aligned["Date"] == pd.Timestamp(target), "open_gap"].iloc[0]) == 0.0


def _row(
    *,
    probability: float,
    keyword_hits: int,
    mention_score: float = 0.5,
) -> SimpleNamespace:
    return SimpleNamespace(
        code="000001",
        confidence_tier="none",
        ml_prob=probability,
        ml_rank_score=0.55,
        keyword_hits=keyword_hits,
        mention_score=mention_score,
        news_context_score=0.5,
        momentum_score=0.55,
        investor_flow_score=0.5,
        foreign_net_vol_ratio=0.02,
        relative_strength_score=0.55,
        industry_momentum=0.4,
        industry_theme_overlap=0.4,
        industry_limit_up_heat=0.1,
        prior_industry_hot=0.1,
        sector_breadth_hot=0.2,
        ks11_ret_lag1=0.0,
        ret_lag1=0.05,
        open_gap=0.0,
        rank_position=None,
        rank_score=0.0,
    )


def test_miss_streak_does_not_zero_high_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    """연속 오판이 길어도 고확신 상한(10)을 0으로 닫지 않는다."""
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", False)
    monkeypatch.setattr(config, "PRED_PRECISION_MAX_HIGH", 10)
    monkeypatch.setattr(config, "PRED_FORWARD_HIGH_MAX_RANK", 15)
    monkeypatch.setattr(config, "PRED_PRECISION_CODE_FAIL_CLOSED", False)
    monkeypatch.setattr(config, "PRED_FEEDBACK_ADAPTIVE_ENABLED", True)
    rows = []
    for i in range(11):
        r = _row(probability=0.22 - i * 0.004, keyword_hits=2, mention_score=0.6)
        r.code = f"{i + 1:06d}"
        r.ml_precision_score = 0.5
        rows.append(r)

    prediction_ranking.assign_forward_confidence_tiers(
        rows,
        regime_scale=1.0,
        feedback_ctx={"recent_miss_streak_days": 8, "adaptive_tightness": 0.50},
    )
    high_n = sum(1 for r in rows if r.confidence_tier == "high")
    assert high_n == 10


def test_tight_regime_keeps_configured_slot_caps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PRED_PRECISION_MAX_HIGH", 10)
    monkeypatch.setattr(config, "PRED_MID_OUTPUT_MAX", 5)
    monkeypatch.setattr(config, "PRED_FORWARD_MIN_HIGH", 3)
    monkeypatch.setattr(config, "PRED_FORWARD_MIN_MID", 5)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", True)
    high, mid = prediction_ranking._forward_slot_caps(0.286)
    assert high == 10
    assert mid == 5


def test_tight_regime_pads_review_slate_instead_of_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_SHOW_MAX", 15)
    monkeypatch.setattr(config, "PRED_MID_OUTPUT_MAX", 5)
    monkeypatch.setattr(config, "PRED_FORWARD_MIN_MID", 5)
    monkeypatch.setattr(config, "PRED_FORWARD_MIN_SLATE", 8)
    monkeypatch.setattr(config, "PRED_FORWARD_SLATE_PAD_MAX_MISS_STREAK", 8)
    monkeypatch.setattr(config, "PRED_FORWARD_SLATE_PAD_MIN_TIGHTNESS", 0.50)
    monkeypatch.setattr(config, "PRED_FEEDBACK_ADAPTIVE_ENABLED", True)
    monkeypatch.setattr(config, "PRED_REQUIRE_NEWS_EVIDENCE", False)
    from src.prediction.predict import PredictionRow

    rows = [
        PredictionRow(
            f"{i:06d}",
            f"N{i}",
            1.0,
            22.0,
            ["kw"],
            [],
            confidence_tier="none",
            keyword_hits=2,
            rank_position=i + 1,
            rank_score=1.0 - i * 0.05,
            mention_score=0.4,
        )
        for i in range(12)
    ]
    prediction_ranking.fill_forward_review_slate(
        rows,
        regime_scale=0.286,
        feedback_ctx={"recent_miss_streak_days": 8, "adaptive_tightness": 0.52},
    )
    tiered = [
        r
        for r in rows
        if str(getattr(r, "confidence_tier", "") or "") in ("high", "mid")
    ]
    assert len(tiered) >= 8


def test_slate_pad_still_runs_on_long_miss_streak(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_SHOW_MAX", 12)
    monkeypatch.setattr(config, "PRED_MID_OUTPUT_MAX", 5)
    monkeypatch.setattr(config, "PRED_FORWARD_SLATE_PAD_MAX_MISS_STREAK", 8)
    monkeypatch.setattr(config, "PRED_FORWARD_SLATE_PAD_MIN_TIGHTNESS", 0.50)
    monkeypatch.setattr(config, "PRED_FEEDBACK_ADAPTIVE_ENABLED", True)
    monkeypatch.setattr(config, "PRED_REQUIRE_NEWS_EVIDENCE", False)
    from src.prediction.predict import PredictionRow

    rows = [
        PredictionRow(
            f"{i:06d}",
            f"N{i}",
            1.0,
            22.0,
            ["kw"],
            [],
            confidence_tier="high" if i < 2 else "none",
            keyword_hits=2,
            rank_position=i + 1,
            rank_score=1.0 - i * 0.05,
            mention_score=0.4,
        )
        for i in range(12)
    ]
    prediction_ranking.fill_forward_review_slate(
        rows,
        regime_scale=1.0,
        feedback_ctx={"recent_miss_streak_days": 9, "adaptive_tightness": 0.40},
    )
    tiered = [
        r
        for r in rows
        if str(getattr(r, "confidence_tier", "") or "") in ("high", "mid")
    ]
    assert len(tiered) >= 7


def test_forward_confidence_abstains_instead_of_forcing_slots(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", False)
    weak = _row(probability=0.10, keyword_hits=0)

    prediction_ranking.assign_forward_confidence_tiers([weak])

    assert weak.confidence_tier == "none"


def test_forward_high_requires_calibrated_probability_and_news_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", False)
    monkeypatch.setattr(config, "PRED_PRECISION_CODE_FAIL_CLOSED", False)
    strong = _row(probability=0.08, keyword_hits=2, mention_score=0.5)
    weak_news = _row(probability=0.08, keyword_hits=0)
    weak_news.code = "000002"
    no_evidence = _row(probability=0.08, keyword_hits=1, mention_score=0.0)
    no_evidence.code = "000003"

    prediction_ranking.assign_forward_confidence_tiers(
        [strong, weak_news, no_evidence]
    )

    assert strong.confidence_tier == "high"
    assert weak_news.confidence_tier == "none"
    assert no_evidence.confidence_tier == "none"


def test_relative_high_assigns_when_all_calibrated_probs_are_low(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """상위권이 전부 ~2.3%여도 상대 하한+뉴스면 고확신을 붙인다."""
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", False)
    monkeypatch.setattr(config, "PRED_PRECISION_GATE_ENABLED", True)
    monkeypatch.setattr(config, "PRED_PRECISION_CODE_FAIL_CLOSED", False)
    monkeypatch.setattr(config, "PRED_PRECISION_MAX_HIGH", 10)
    monkeypatch.setattr(config, "PRED_FORWARD_HIGH_MAX_RANK", 15)
    monkeypatch.setattr(config, "PRED_HIGH_CALIBRATED_RELATIVE", 0.72)
    monkeypatch.setattr(config, "PRED_PRECISION_MIN_PILLARS", 1)
    monkeypatch.setattr(config, "PRED_FORWARD_HIGH_MIN_KEYWORD_HITS", 1)
    monkeypatch.setattr(config, "PRED_HIGH_NEWS_EVIDENCE_MIN", 0.55)
    monkeypatch.setattr(config, "PRED_ML_HIGH_CONFIDENCE_PROB", 0.10)
    monkeypatch.setattr(config, "PRED_HIGH_SELECT_FLOOR", 0.18)
    rows = []
    for i in range(6):
        r = _row(probability=0.023 - i * 0.0004, keyword_hits=2, mention_score=0.6)
        r.code = f"{i + 1:06d}"
        r.ml_precision_score = 0.023
        rows.append(r)
    weak = _row(probability=0.010, keyword_hits=2, mention_score=0.6)
    weak.code = "000099"
    rows.append(weak)
    no_news = _row(probability=0.023, keyword_hits=0, mention_score=0.0)
    no_news.code = "000098"
    rows.append(no_news)

    prediction_ranking.assign_forward_confidence_tiers(rows, regime_scale=0.286)
    prediction_ranking.refine_confidence_tiers(rows)

    high_codes = {r.code for r in rows if r.confidence_tier == "high"}
    assert "000001" in high_codes
    assert "000002" in high_codes
    assert weak.code not in high_codes
    assert no_news.code not in high_codes
    assert len(high_codes) >= 3


def test_refine_does_not_promote_zero_keyword_raw_ml100(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``refine_confidence_tiers`` 가 ml_rank_score 만으로 high 승격하지 않음."""
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    monkeypatch.setattr(config, "PRED_PRECISION_GATE_ENABLED", True)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", False)
    carryover = _row(probability=0.03, keyword_hits=0, mention_score=0.0)
    carryover.industry_theme_overlap = 0.85
    carryover.industry_momentum = 0.75
    carryover.industry_limit_up_heat = 0.55
    carryover.prior_industry_hot = 0.35
    carryover.sector_breadth_hot = 0.40
    carryover.ml_rank_score = 0.92
    carryover.confidence_tier = "none"

    prediction_ranking.assign_forward_confidence_tiers([carryover])
    assert carryover.confidence_tier == "none"

    carryover.confidence_tier = "high"
    prediction_ranking.refine_confidence_tiers([carryover])
    assert carryover.confidence_tier == "none"


def test_case_control_calibration_is_rebased_to_population_prevalence() -> None:
    raw = pd.Series([0.1 + i * 0.01 for i in range(40)]).to_numpy()
    labels = pd.Series(([0] * 32) + ([1] * 8)).to_numpy()

    sampled = ml_move_rank._build_prob_calibration_table(raw, labels)
    rebased = ml_move_rank._build_prob_calibration_table(
        raw,
        labels,
        population_base_rate=0.005,
    )

    assert sampled
    assert rebased
    assert max(row["rate"] for row in rebased) < max(
        row["rate"] for row in sampled
    )


def test_legacy_contaminated_cal_table_is_sanitized_at_inference() -> None:
    cal_table = [{"lo": 0.0, "hi": 1.0, "rate": 1.0}]
    p = ml_move_rank.calibrate_ml_probability(
        0.9,
        cal_table=cal_table,
        base_rate=0.18,
    )
    assert p <= 0.12
