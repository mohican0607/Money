"""예측 표·freeze·전일 과열 게이트 회귀 방지."""
from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from src import config
from src.pipeline.rows import (
    _compare_row_belongs_in_closed_day_table,
    _compare_row_is_prediction_candidate,
    _merge_actual_big_movers_into_rows_compare,
)
from src.pipeline.support import (
    _display_prediction_rows_for_freeze,
    _freeze_entry_usable,
    _prediction_rows_from_frozen_items,
    _prediction_rows_to_frozen_items,
    _should_reuse_prediction_freeze,
    effective_train_snapshot_cal_scope,
)
from src.prediction.predict import PredictionRow
from src.prediction import prediction_ranking as prk


def test_compare_table_excludes_actual_only_rows_on_forward_days() -> None:
    assert not _compare_row_is_prediction_candidate({"actual_big": True, "pred_high": False})
    assert _compare_row_is_prediction_candidate({"pred_high": True, "actual_big": False})
    assert _compare_row_is_prediction_candidate(
        {"pred_high": False, "pred_mid": False, "pred_ret": 20.0, "confidence_tier": "none"}
    )
    assert _compare_row_is_prediction_candidate(
        {
            "pred_high": False,
            "pred_mid": False,
            "pred_ret": 8.0,
            "confidence_tier": "none",
            "rank_position": 1,
        }
    )


def test_closed_day_table_keeps_actual_big_movers() -> None:
    assert _compare_row_belongs_in_closed_day_table({"actual_big": True, "pred_high": False})
    assert not _compare_row_belongs_in_closed_day_table({"actual_big": False, "pred_high": False})


def test_closed_day_table_keeps_frozen_pred_at_threshold() -> None:
    assert _compare_row_belongs_in_closed_day_table(
        {
            "actual_big": False,
            "pred_high": False,
            "pred_mid": False,
            "pred_ret": 21.81,
            "confidence_tier": "none",
        }
    )


def test_merge_actual_big_movers_adds_missing_rows() -> None:
    rows: list[dict] = [{"code": "000001", "actual_big": False, "pred_high": True}]
    _merge_actual_big_movers_into_rows_compare(
        rows,
        [{"code": "000002", "name": "B", "ret_pct": 25.0}],
        market_by_code={},
    )
    codes = {str(r["code"]).zfill(6) for r in rows}
    assert codes == {"000001", "000002"}
    extra = next(r for r in rows if r["code"] == "000002")
    assert extra["actual_big"] is True
    assert extra["pred_ret"] is None


def test_reuse_prediction_freeze_after_market_close() -> None:
    items = [
        {
            "code": "000001",
            "name": "A",
            "predicted_return_pct": 22.0,
            "confidence_tier": "high",
        }
    ]
    assert _should_reuse_prediction_freeze(
        ignore_freeze_for_trading_day=False,
        frozen_items=items,
    )
    assert not _should_reuse_prediction_freeze(
        ignore_freeze_for_trading_day=True,
        frozen_items=items,
    )
    assert not _should_reuse_prediction_freeze(
        ignore_freeze_for_trading_day=False,
        frozen_items=None,
    )


def test_freeze_roundtrip_preserves_prediction_codes_and_tiers() -> None:
    rows = [
        PredictionRow(
            "000001",
            "A",
            1.0,
            25.0,
            ["테마"],
            [],
            confidence_tier="high",
            rank_position=1,
            keyword_hits=2,
        ),
        PredictionRow(
            "000002",
            "B",
            1.0,
            22.0,
            ["테마"],
            [],
            confidence_tier="mid",
            rank_position=2,
            keyword_hits=1,
        ),
        PredictionRow(
            "000003",
            "C",
            1.0,
            8.0,
            [],
            [],
            confidence_tier="none",
            rank_position=3,
        ),
    ]
    frozen = _prediction_rows_to_frozen_items(_display_prediction_rows_for_freeze(rows))
    restored = _prediction_rows_from_frozen_items(frozen)
    assert [r.code for r in restored] == ["000001", "000002"]
    assert [r.confidence_tier for r in restored] == ["high", "mid"]
    assert [round(r.predicted_return_pct, 1) for r in restored] == [25.0, 22.0]


def test_freeze_pads_none_tier_when_no_high_mid(monkeypatch) -> None:
    monkeypatch.setattr(config, "PRED_FORWARD_SHOW_MAX", 10)
    rows = [
        PredictionRow(
            f"{i:06d}",
            f"N{i}",
            1.0,
            21.81,
            ["테마"],
            [],
            confidence_tier="none",
            rank_position=i,
            keyword_hits=1,
        )
        for i in range(1, 11)
    ]
    display = _display_prediction_rows_for_freeze(rows)
    assert [r.code for r in display] == [f"{i:06d}" for i in range(1, 11)]
    frozen = _prediction_rows_to_frozen_items(display)
    assert len(frozen) == 10
    assert _freeze_entry_usable(frozen)
    assert [r.code for r in _prediction_rows_from_frozen_items(frozen)] == [
        f"{i:06d}" for i in range(1, 11)
    ]


def test_empty_slate_marker_is_not_reused() -> None:
    assert not _freeze_entry_usable([{"_empty_slate": True}])
    assert _prediction_rows_from_frozen_items([{"_empty_slate": True}]) == []
    assert _prediction_rows_to_frozen_items([]) == []


def test_freeze_reuses_historical_none_tier() -> None:
    # 과거 freeze(none-tier)도 재사용 가능 — 신규 더미 채움만 저장 경로에서 막음
    junk = [
        {
            "code": "007810",
            "name": "X",
            "predicted_return_pct": 21.81,
            "confidence_tier": "none",
        }
        for _ in range(10)
    ]
    assert _freeze_entry_usable(junk)


def test_display_freeze_drops_rows_without_news(monkeypatch) -> None:
    monkeypatch.setattr(config, "PRED_FORWARD_SHOW_MAX", 10)
    monkeypatch.setattr(config, "PRED_REQUIRE_NEWS_EVIDENCE", True)
    rows = [
        PredictionRow(
            "000001",
            "뉴스있음",
            1.0,
            22.0,
            ["테마"],
            [],
            confidence_tier="none",
            rank_position=2,
            keyword_hits=1,
        ),
        PredictionRow(
            "000002",
            "뉴스없음",
            1.0,
            25.0,
            [],
            [],
            confidence_tier="high",
            rank_position=1,
            keyword_hits=0,
            mention_score=0.0,
            news_context_score=0.0,
        ),
    ]
    display = _display_prediction_rows_for_freeze(rows)
    assert [r.code for r in display] == ["000001"]


def test_freeze_does_not_pad_when_few_tiered(monkeypatch) -> None:
    monkeypatch.setattr(config, "PRED_FORWARD_SHOW_MAX", 10)
    rows = [
        PredictionRow(
            f"{i:06d}",
            f"N{i}",
            1.0,
            20.0,
            ["테마"] if i == 1 else [],
            [],
            confidence_tier="mid" if i == 1 else "none",
            rank_position=i,
            keyword_hits=1 if i == 1 else 0,
        )
        for i in range(1, 8)
    ]
    frozen = _display_prediction_rows_for_freeze(rows)
    assert len(frozen) == 1
    assert frozen[0].code == "000001"
    assert frozen[0].confidence_tier == "mid"


def test_prior_day_exhaustion_blocks_after_limit_up() -> None:
    row = PredictionRow(
        "091590",
        "남화토건",
        1.0,
        25.0,
        [],
        [],
        ret_lag1=0.2996,
        confidence_tier="high",
    )
    assert prk.prior_day_exhaustion_blocks_confidence(row)
    assert prk.prior_day_exhaustion_penalty(row) >= 0.99


def test_theme_rotation_prefers_moderate_lag_in_hot_sector() -> None:
    hot = PredictionRow(
        "023410",
        "유진기업",
        1.0,
        22.0,
        [],
        [],
        ret_lag1=0.10,
        prior_industry_hot=0.35,
        industry_momentum=0.30,
    )
    exhausted = PredictionRow(
        "091590",
        "남화토건",
        1.0,
        25.0,
        [],
        [],
        ret_lag1=0.30,
        prior_industry_hot=0.40,
        industry_momentum=0.35,
    )
    assert prk.theme_rotation_score(hot) > prk.theme_rotation_score(exhausted)
    assert prk.theme_rotation_score(exhausted) == 0.0


def test_carryover_auto_on_heat(monkeypatch) -> None:
    monkeypatch.setattr(config, "PRED_CARRYOVER_INDUSTRY_RERANK_ENABLED", False)
    monkeypatch.setattr(config, "PRED_CARRYOVER_AUTO_ON_HEAT", True)
    import pandas as pd

    t = date(2026, 8, 28)
    prev = date(2026, 8, 27)
    df = pd.DataFrame(
        {
            "Date": [pd.Timestamp(prev)],
            "Code": ["005930"],
            "return_pct": [0.25],
        }
    )
    assert prk._carryover_rerank_enabled(df, t) is True
    monkeypatch.setattr(config, "PRED_CARRYOVER_AUTO_ON_HEAT", False)
    assert prk._carryover_rerank_enabled(df, t) is False


def test_append_learning_single_day_infers_cal_scope_from_day_reports() -> None:
    t = date(2026, 7, 7)
    dr = SimpleNamespace(trading_day=t, forward_observation=False)
    scope = effective_train_snapshot_cal_scope(
        train_snapshot_mode="append_learning",
        train_snapshot_cal_scope=None,
        day_reports=[dr],
        forward_prediction_only=False,
    )
    assert scope == (t, t)


def test_observation_day_trading_halt_excludes_candidate() -> None:
    import pandas as pd

    from src import stocks

    t = date(2026, 7, 29)
    df = pd.DataFrame(
        [
            {
                "Date": pd.Timestamp(t),
                "Code": "123456",
                "Name": "HALT",
                "return_pct": 0.0,
                "Volume": 0.0,
            }
        ]
    )
    assert stocks.is_observation_day_trading_halted(df, "123456", t)
    df.iloc[0, df.columns.get_loc("Volume")] = 1200.0
    assert not stocks.is_observation_day_trading_halted(df, "123456", t)


def test_future_observation_day_without_bar_is_not_halted() -> None:
    import pandas as pd

    from src import stocks

    last = date(2026, 7, 30)
    future = date(2026, 8, 3)
    df = pd.DataFrame(
        [
            {
                "Date": pd.Timestamp(last),
                "Code": "123456",
                "Name": "A",
                "return_pct": 0.01,
                "Volume": 1000.0,
            }
        ]
    )
    assert not stocks.is_observation_day_trading_halted(df, "123456", future)


def test_forecast_synthetic_row_without_volume_is_not_halted() -> None:
    import pandas as pd

    from src import stocks

    t = date(2026, 8, 3)
    df = pd.DataFrame(
        [
            {
                "Date": pd.Timestamp(t),
                "Code": "123456",
                "Name": "A",
                "return_pct": 0.0,
                "open_gap": 0.0,
            }
        ]
    )
    assert not stocks.is_observation_day_trading_halted(df, "123456", t)


def test_tier_none_with_high_display_pct_is_not_pred_high_when_confidence_gate_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    row = PredictionRow(
        "000001",
        "A",
        1.0,
        25.0,
        [],
        [],
        confidence_tier="none",
    )
    assert not prk.is_high_confidence_prediction(row)
    assert not prk.is_mid_confidence_prediction(row)
    assert not prk.row_pred_high_from_dict(
        {"confidence_tier": "none", "pred_ret": 25.0}
    )


def test_tier_high_is_pred_high_when_confidence_gate_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", True)
    row = PredictionRow(
        "000001",
        "A",
        1.0,
        25.0,
        [],
        [],
        confidence_tier="high",
    )
    assert prk.is_high_confidence_prediction(row)


def test_display_pct_fallback_when_confidence_gate_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_RANKING_MODE", False)
    monkeypatch.setattr(config, "PRED_CONFIDENCE_OUTPUT_ENABLED", False)
    row = PredictionRow(
        "000001",
        "A",
        1.0,
        25.0,
        [],
        [],
        confidence_tier="none",
    )
    assert prk.is_high_confidence_prediction(row)

    t = date(2026, 7, 8)
    dr = SimpleNamespace(trading_day=t, forward_observation=True)
    scope = effective_train_snapshot_cal_scope(
        train_snapshot_mode="append_learning",
        train_snapshot_cal_scope=None,
        day_reports=[dr],
        forward_prediction_only=True,
    )
    assert scope is None


def test_ranking_mode_never_pred_high_on_tier_none_even_with_high_display(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_RANKING_MODE", True)
    row = PredictionRow(
        "000001",
        "A",
        1.0,
        28.0,
        [],
        [],
        confidence_tier="none",
    )
    assert not prk.is_high_confidence_prediction(row)
    assert not prk.row_pred_high_from_dict(
        {"confidence_tier": "none", "pred_ret": 28.0}
    )


def test_freeze_rejects_short_high_only_slate() -> None:
    items = [
        {"code": "000001", "predicted_return_pct": 25.0, "confidence_tier": "high"},
        {"code": "000002", "predicted_return_pct": 24.0, "confidence_tier": "high"},
        {"code": "000003", "predicted_return_pct": 23.0, "confidence_tier": "high"},
    ]
    assert not _should_reuse_prediction_freeze(
        ignore_freeze_for_trading_day=False,
        frozen_items=items,
    )


def test_fill_forward_review_slate_adds_mid_to_reach_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_FORWARD_SHOW_MAX", 8)
    monkeypatch.setattr(config, "PRED_MID_OUTPUT_MAX", 5)
    monkeypatch.setattr(config, "PRED_FORWARD_MID_ENABLED", True)
    rows = [
        PredictionRow(
            f"{i:06d}",
            f"N{i}",
            1.0,
            25.0,
            ["kw"],
            [],
            confidence_tier="high" if i < 3 else "none",
            keyword_hits=2,
            rank_position=i + 1,
            rank_score=1.0 - i * 0.05,
        )
        for i in range(12)
    ]
    prk.fill_forward_review_slate(
        rows,
        feedback_ctx={"recent_miss_streak_days": 0, "adaptive_tightness": 1.0},
    )
    tiered = [
        r
        for r in rows
        if str(getattr(r, "confidence_tier", "") or "") in ("high", "mid")
    ]
    assert len(tiered) >= 8


def test_soft_move_label_roundtrip_to_return() -> None:
    from src.prediction.ml_move_rank import (
        _soft_move_label,
        _soft_move_label_to_return,
    )

    thr = 0.20
    for ret in (0.0, 0.05, 0.10, 0.20, 0.28, 0.35):
        soft = _soft_move_label(ret, threshold=thr)
        back = _soft_move_label_to_return(soft, threshold=thr)
        assert abs(back - min(ret, thr + 0.15)) < 1e-9


def test_feedback_without_global_fallback_keeps_stock_estimate() -> None:
    from src.prediction.predict import _feedback_calibrated_return

    ctx = {
        "by_code_mean_ratio": {},
        "by_code_count": {},
        "global_mean_ratio": 0.50,
        "global_count": 200,
    }
    out = _feedback_calibrated_return(
        0.12,
        code="323280",
        n_hit=2,
        mention=0.1,
        feedback_ctx=ctx,
        clamp_lo=0.0,
        clamp_hi=0.35,
        use_global_fallback=False,
    )
    assert abs(out - 0.12) < 1e-9
