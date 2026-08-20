"""빈 뉴스 day_*.json 이 재수집을 막지 않는지."""
from __future__ import annotations

from datetime import date

from src import news


def test_empty_naver_both_cache_is_refetched(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(news, "_CACHE_NEWS", tmp_path)
    monkeypatch.setattr(news.config, "MOCK_NEWS", False)
    monkeypatch.setattr(news.config, "NEWS_NAVER_QUERY_MODE", "both")
    day = date(2026, 8, 13)
    path = news.day_news_json_path(day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[]", encoding="utf-8")

    called = {"n": 0}

    def _fake_naver(_target, _session, http_sink=None):
        called["n"] += 1
        return [{"title": "테스트 뉴스", "description": "본문", "link": "http://x"}]

    monkeypatch.setattr(news, "_fetch_news_day_naver", _fake_naver)
    monkeypatch.setattr(
        news, "_fetch_news_day_naver_per_ticker", lambda *a, **k: ([], 0)
    )
    monkeypatch.setattr(news, "_enrich_rows_with_stock_codes", lambda rows: rows)
    monkeypatch.setattr(news.config, "NAVER_CLIENT_ID", "id")
    monkeypatch.setattr(news.config, "NAVER_CLIENT_SECRET", "secret")

    rows = news.fetch_news_for_calendar_day(day)
    assert called["n"] == 1
    assert rows and rows[0]["title"] == "테스트 뉴스"


def test_future_calendar_day_is_not_cached(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(news, "_CACHE_NEWS", tmp_path)
    monkeypatch.setattr(news.config, "MOCK_NEWS", False)
    future = date(2099, 1, 1)
    path = news.day_news_json_path(future)
    rows = news.fetch_news_for_calendar_day(future)
    assert rows == []
    assert not path.is_file()
