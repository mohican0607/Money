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


def test_stopwords_block_filler_keywords() -> None:
    from src.features import filter_keywords, is_valid_keyword

    assert not is_valid_keyword("기준")
    assert not is_valid_keyword("대비")
    assert not is_valid_keyword("기업")
    assert not is_valid_keyword("일부터")
    cleaned = filter_keywords(["기준", "대비", "상한가", "로봇"])
    assert "기준" not in cleaned
    assert "대비" not in cleaned
    assert "로봇" in cleaned


def test_substantive_keywords_reject_filler_and_media() -> None:
    from src.features import filter_specific_keywords, is_substantive_keyword

    assert not is_substantive_keyword("기준")
    assert not is_substantive_keyword("더벨")
    assert not is_substantive_keyword("원을")
    assert not is_substantive_keyword("기사입니다")
    assert not is_substantive_keyword("개월")
    assert not is_substantive_keyword("개최")
    assert not is_substantive_keyword("기존")
    assert is_substantive_keyword("반도체")
    assert is_substantive_keyword("로봇")
    cleaned = filter_specific_keywords(
        ["기준", "대비", "더벨", "반도체", "로봇", "원을", "상한가", "개월", "개최"]
    )
    assert cleaned == frozenset({"반도체", "로봇"})


def test_news_pillar_prefers_mention_and_theme_over_keyword_count() -> None:
    junk = _row(keyword_hits=4, mention_score=0.0, news_context_score=0.0)
    junk.industry_theme_overlap = 0.0
    junk.theme_carryover_score = 0.0
    mention = _row(keyword_hits=0, mention_score=0.6, news_context_score=0.2)
    mention.industry_theme_overlap = 0.3
    mention.theme_carryover_score = 2.0
    pj = prk.compute_pillar_scores(junk)
    pm = prk.compute_pillar_scores(mention)
    assert pm["news"] > pj["news"]


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


def test_passes_precision_gate_rejects_low_abs_calibrated_even_if_top_of_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """풀 1등이어도 절대 보정 ML 하한 미만이면 고확신 불가."""
    monkeypatch.setattr(config, "PRED_HIGH_NEWS_EVIDENCE_MIN", 0.55)
    monkeypatch.setattr(config, "PRED_HIGH_CALIBRATED_ABS_MIN", 0.08)
    monkeypatch.setattr(config, "PRED_FORWARD_HIGH_CALIBRATED_MIN", 0.08)
    monkeypatch.setattr(config, "PRED_HIGH_CALIBRATED_RELATIVE", 0.72)
    monkeypatch.setattr(config, "PRED_PRECISION_MIN_PILLARS", 1)
    monkeypatch.setattr(config, "PRED_HIGH_SELECT_FLOOR", 0.18)
    monkeypatch.setattr(config, "PRED_ML_HIGH_CONFIDENCE_PROB", 0.10)
    row = _row(ml_prob=0.023, keyword_hits=2, mention_score=0.6)
    assert not prk.passes_precision_gate(row, top_ml=0.023, rank_position=1)
    ok = _row(ml_prob=0.12, keyword_hits=2, mention_score=0.6)
    assert prk.passes_precision_gate(ok, top_ml=0.12, rank_position=1)


def test_adaptive_high_floor_never_below_abs_min(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "PRED_HIGH_CALIBRATED_ABS_MIN", 0.08)
    monkeypatch.setattr(config, "PRED_FORWARD_HIGH_CALIBRATED_MIN", 0.08)
    monkeypatch.setattr(config, "PRED_HIGH_CALIBRATED_RELATIVE", 0.72)
    pool = [_row(ml_prob=0.02), _row(ml_prob=0.018)]
    floor = prk._adaptive_calibrated_high_floor(pool, regime_scale=1.0)
    assert floor == pytest.approx(0.08)


def test_clamp_display_pct_respects_report_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ML 캡을 켜도 리포트 하한(20%) 미만으로 깎지 않는다."""
    monkeypatch.setattr(config, "PRED_DISPLAY_PCT_ML_CAP_ENABLED", True)
    monkeypatch.setattr(config, "PRED_REPORT_MIN_PCT", 20.0)
    row = _row(ml_prob=0.018)
    row.predicted_return_pct = 25.0
    prk.clamp_display_pct_to_ml_prob(row)
    assert float(row.predicted_return_pct) >= 20.0


def test_report_slate_fills_even_when_soft_pct_below_20(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """소프트 라벨이 5%대여도 리포트 슬레이트는 20%↑로 매핑해 비우지 않는다."""
    from src.pipeline import support as pipe_support

    monkeypatch.setattr(config, "PRED_FORWARD_SHOW_MAX", 8)
    monkeypatch.setattr(config, "PRED_FORWARD_MIN_SLATE", 5)
    monkeypatch.setattr(config, "PRED_REQUIRE_NEWS_EVIDENCE", True)
    rows = []
    for i in range(12):
        r = _row(
            code=f"{i + 1:06d}",
            keyword_hits=2,
            mention_score=0.5,
            ml_prob=0.02,
            confidence_tier="none",
        )
        r.name = f"N{i}"
        r.predicted_return_pct = 5.0
        r.rank_score = 1.0 - i * 0.04
        r.rank_position = i + 1
        r.score = float(r.rank_score)
        r.reasons = []
        r.matched_keywords = ["반도체"]
        rows.append(r)

    slate = pipe_support._display_prediction_rows_for_freeze(rows)
    assert len(slate) >= 5
    assert all(float(r.predicted_return_pct) + 1e-9 >= 20.0 for r in slate)


def test_news_evidence_one_keyword_alone_not_high() -> None:
    row = _row(keyword_hits=1, mention_score=0.0, news_context_score=0.0)
    assert prk._news_evidence_strength(row) < 0.72
    assert not prk._high_tier_news_ok(row)
