"""뉴스 필터·must_keep bypass."""
from __future__ import annotations

from src import config
from src.prediction import prediction_ranking as pr
from src.prediction.predict import PredictionRow


def test_news_backed_rows_bypass_must_keep(monkeypatch) -> None:
    monkeypatch.setattr(config, "PRED_REQUIRE_NEWS_EVIDENCE", True)
    with_news = PredictionRow(
        "000001", "A", 0.5, 20.0, ["kw"], [], keyword_hits=2, mention_score=0.0
    )
    no_news = PredictionRow(
        "000002", "B", 0.5, 20.0, [], [], keyword_hits=0, mention_score=0.0
    )
    out = pr._news_backed_rows(
        [with_news, no_news], bypass_codes=frozenset({"000002"})
    )
    codes = {r.code for r in out}
    assert codes == {"000001", "000002"}


def test_news_backed_rows_without_bypass_filters(monkeypatch) -> None:
    monkeypatch.setattr(config, "PRED_REQUIRE_NEWS_EVIDENCE", True)
    with_news = PredictionRow(
        "000001", "A", 0.5, 20.0, ["kw"], [], keyword_hits=1, mention_score=0.0
    )
    no_news = PredictionRow(
        "000002", "B", 0.5, 20.0, [], [], keyword_hits=0, mention_score=0.0
    )
    out = pr._news_backed_rows([with_news, no_news])
    assert [r.code for r in out] == ["000001"]
