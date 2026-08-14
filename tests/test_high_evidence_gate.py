"""뉴스·보정 ML 통합 고확신 게이트."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src import config
from src.prediction import prediction_ranking as prk


def _row(**kwargs) -> SimpleNamespace:
    base = dict(
        code="000001",
        confidence_tier="none",
        ml_prob=0.08,
        ml_rank_score=0.55,
        keyword_hits=2,
        mention_score=0.4,
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
        ml_precision_score=0.08,
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_news_evidence_zero_for_no_keywords() -> None:
    row = _row(keyword_hits=0, mention_score=0.0, news_context_score=0.0)
    assert prk._news_evidence_strength(row) < 0.55
    assert not prk.row_has_news_evidence(row)


def test_row_has_news_evidence_for_keyword_or_mention() -> None:
    assert prk.row_has_news_evidence(_row(keyword_hits=1, mention_score=0.0, news_context_score=0.0))
    assert prk.row_has_news_evidence(
        _row(keyword_hits=0, mention_score=0.35, news_context_score=0.0)
    )


def test_carryover_without_news_detects_sector_only() -> None:
    row = _row(
        keyword_hits=0,
        mention_score=0.0,
        industry_theme_overlap=0.9,
        industry_momentum=0.8,
        industry_limit_up_heat=0.6,
        prior_industry_hot=0.4,
        sector_breadth_hot=0.5,
        ml_rank_score=0.95,
        ml_prob=0.04,
    )
    assert prk._carryover_without_news(row)


def test_passes_precision_gate_rejects_low_calibrated_despite_high_raw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "PRED_HIGH_NEWS_EVIDENCE_MIN", 0.55)
    row = _row(ml_prob=0.03, ml_rank_score=0.95, keyword_hits=0, mention_score=0.0)
    assert not prk.passes_precision_gate(row, top_ml=0.10, rank_position=1)


def test_passes_precision_gate_accepts_strong_news_and_calibrated() -> None:
    row = _row(ml_prob=0.12, keyword_hits=2, mention_score=0.4)
    assert prk.passes_precision_gate(row, top_ml=0.12, rank_position=1)
