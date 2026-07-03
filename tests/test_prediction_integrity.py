"""예측 표·freeze·전일 과열 게이트 회귀 방지."""
from __future__ import annotations

from src import config
from src.pipeline.rows import (
    _compare_row_belongs_in_closed_day_table,
    _compare_row_is_prediction_candidate,
    _merge_actual_big_movers_into_rows_compare,
)
from src.pipeline.support import _display_prediction_rows_for_freeze
from src.prediction.predict import PredictionRow
from src.prediction import prediction_ranking as prk


def test_compare_table_excludes_actual_only_rows_on_forward_days() -> None:
    assert not _compare_row_is_prediction_candidate({"actual_big": True, "pred_high": False})
    assert _compare_row_is_prediction_candidate({"pred_high": True, "actual_big": False})


def test_closed_day_table_keeps_actual_big_movers() -> None:
    assert _compare_row_belongs_in_closed_day_table({"actual_big": True, "pred_high": False})
    assert not _compare_row_belongs_in_closed_day_table({"actual_big": False, "pred_high": False})


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


def test_freeze_keeps_display_candidates_only() -> None:
    rows = [
        PredictionRow("000001", "A", 1.0, 25.0, [], [], confidence_tier="high"),
        PredictionRow("000002", "B", 1.0, 22.0, [], [], confidence_tier="none"),
        PredictionRow("000003", "C", 1.0, 21.0, [], [], confidence_tier="mid"),
    ]
    frozen = _display_prediction_rows_for_freeze(rows)
    codes = {r.code for r in frozen}
    assert codes == {"000001", "000003"}


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


def test_carryover_rerank_disabled_by_default() -> None:
    assert not config.PRED_CARRYOVER_INDUSTRY_RERANK_ENABLED
