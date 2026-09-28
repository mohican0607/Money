"""적응형 오판 반성 루프 단위 테스트."""
from __future__ import annotations

import json
from datetime import date

import pytest

from src import config
from src.learning.support import build_miss_rows_for_day
from src.prediction import feedback_loop
from src.prediction.predict import PredictionRow
from src.report.render import DayReport


def test_hit_at_10_int_format_parsed() -> None:
    assert feedback_loop._hit_at_10_rate({"hit_at_10": 2}) == 0.2
    assert feedback_loop._hit_at_10_rate({"hit_at_10": {"hits": 3, "k": 10}}) == 0.3
    assert feedback_loop._hit_at_10_rate({}) is None


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
    assert "fn_industry_weights" in ctx


def test_miss_streak_counts_int_hit_at_10(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PRED_FEEDBACK_ADAPTIVE_ENABLED", True)
    payload = {
        "hit_at_k_by_day": {
            "2026-07-29": {"hit_at_10": 0},
            "2026-07-30": {"hit_at_10": 0},
            "2026-07-31": {"hit_at_10": 1},
        },
        "t_code_ratio": {},
    }
    monkeypatch.setattr(
        feedback_loop.prediction_accuracy_cache,
        "_load_payload",
        lambda: payload,
    )
    ctx = feedback_loop.build_enriched_feedback_context(as_of=date(2026, 7, 31))
    assert int(ctx.get("recent_miss_streak_days") or 0) >= 1


def test_build_miss_rows_uses_actual_big_movers_not_only_table() -> None:
    """비교 표가 예측만 담아도 actual_big_movers 로 FN을 잡는다."""
    preds = [
        PredictionRow(
            "111111",
            "예측종목",
            0.5,
            25.0,
            [],
            [],
            confidence_tier="mid",
        )
    ]
    dr = DayReport(
        trading_day=date(2026, 9, 22),
        predictions=preds,
        rows_compare=[
            {
                "code": "111111",
                "name": "예측종목",
                "pred_ret": 25.0,
                "actual_ret": -0.01,
                "pred_high": True,
                "actual_big": False,
                "keywords": [],
            }
        ],
        false_negatives=[],
        news_titles_sample=[],
        news_highlight_terms=[],
        actual_big_movers=[
            {"code": "002630", "name": "오리엔트바이오", "ret_pct": 103.2},
            {"code": "393210", "name": "토마토시스템", "ret_pct": 30.0},
        ],
    )
    missed, pred_misses = build_miss_rows_for_day(dr)
    codes = {m["code"] for m in missed}
    assert "002630" in codes
    assert "393210" in codes
    assert "111111" not in codes
    assert len(pred_misses) == 1
    assert pred_misses[0]["code"] == "111111"


def test_miss_reflection_industry_boost(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PRED_MISS_REFLECTION_ENABLED", True)
    monkeypatch.setattr(config, "PRED_MISS_REFLECTION_INDUSTRY_BOOST_MAX", 0.28)
    ctx = {
        "fn_industry_weights": {"999": 0.28},
    }
    row = PredictionRow("123456", "X", 0.5, 22.0, [], [])
    monkeypatch.setattr(
        "src.stocks.industry_code_for_stock",
        lambda code, refresh=False: "999",
    )
    b = feedback_loop.miss_reflection_industry_boost(row, ctx)
    assert b == pytest.approx(0.28)
    feedback_loop.stamp_miss_reflection_boosts([row], ctx)
    assert row.miss_reflection_boost == pytest.approx(0.28)


def test_append_miss_reflection_lessons_writes_fn_industries(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setattr(config, "PRED_MISS_REFLECTION_ENABLED", True)
    path = tmp_path / "miss_reflection_lessons.json"
    monkeypatch.setattr(feedback_loop, "MISS_REFLECTION_PATH", path)
    monkeypatch.setattr(
        "src.stocks.industry_code_for_stock",
        lambda code, refresh=False: "777" if str(code).endswith("630") else "888",
    )
    monkeypatch.setattr(
        "src.stocks.industry_name_for_code",
        lambda code, refresh=False: "바이오",
    )
    dr = DayReport(
        trading_day=date(2026, 9, 22),
        predictions=[],
        rows_compare=[],
        false_negatives=[],
        news_titles_sample=[],
        news_highlight_terms=[],
        actual_big_movers=[
            {"code": "002630", "name": "오리엔트바이오", "ret_pct": 103.2},
        ],
    )
    feedback_loop.append_miss_reflection_lessons(dr)
    data = json.loads(path.read_text(encoding="utf-8"))
    day = data["days"]["2026-09-22"]
    assert day["fn_count"] == 1
    assert "777" in day["fn_industries"]
    assert day["fn_industries"]["777"] > 1.0
