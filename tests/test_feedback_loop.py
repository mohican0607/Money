"""적응형 오판 반성 루프 단위 테스트."""
from __future__ import annotations

from datetime import date

import pytest

from src import config
from src.prediction import feedback_loop
from src.prediction.predict import PredictionRow


def test_adaptive_tightness_low_on_poor_recent_hit_rate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_FEEDBACK_ADAPTIVE_ENABLED", True)
    payload = {
        "hit_at_k_by_day": {
            "2026-07-27": {"hit_at_10": {"hits": 0, "k": 10}},
            "2026-07-28": {"hit_at_10": {"hits": 0, "k": 10}},
            "2026-07-29": {"hit_at_10": {"hits": 1, "k": 10}},
        },
        "t_code_ratio": {
            "2026-07-27:000001": 0.0,
            "2026-07-28:000002": 0.0,
            "2026-07-29:000003": 0.0,
        },
    }
    t = feedback_loop.compute_adaptive_tightness(payload, as_of=date(2026, 7, 30))
    assert t < 0.75


def test_should_block_code_fast_on_recent_misses() -> None:
    ctx = {
        "by_code_recent_mean_ratio": {"123456": 0.05},
        "by_code_recent_count": {"123456": 3},
        "by_code_mean_ratio": {},
        "by_code_count": {},
    }
    assert feedback_loop.should_block_code_fast("123456", mention=0.0, feedback_ctx=ctx)
    assert not feedback_loop.should_block_code_fast("123456", mention=0.50, feedback_ctx=ctx)


def test_feedback_rank_penalty_reduces_bad_code() -> None:
    row = PredictionRow(
        "999999",
        "X",
        0.8,
        24.0,
        ["kw"],
        [],
        confidence_tier="none",
        keyword_hits=1,
        mention_score=0.1,
    )
    row.rank_score = 0.9
    ctx = {
        "adaptive_tightness": 0.6,
        "by_code_recent_mean_ratio": {"999999": 0.08},
        "by_code_recent_count": {"999999": 4},
        "signal_bucket_stats": {},
    }
    p = feedback_loop.feedback_rank_penalty(row, ctx)
    assert p > 0.15
    feedback_loop.apply_feedback_rank_penalties([row], ctx)
    assert row.rank_score < 0.75


def test_build_enriched_feedback_context_has_adaptive_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_FEEDBACK_ADAPTIVE_ENABLED", True)
    ctx = feedback_loop.build_enriched_feedback_context(as_of=date(2026, 7, 31))
    assert "adaptive_tightness" in ctx
    assert "by_code_recent_mean_ratio" in ctx
    assert "recent_miss_streak_days" in ctx
