"""OHLCV 캐시 종목 보강이 비어도 날짜가 채워져 있으면 파이프라인을 죽이지 않는다."""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src import stocks


def _cache_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-09-17", "2026-09-18"]),
            "Code": ["005930", "005930"],
            "Name": ["삼성전자", "삼성전자"],
            "Open": [1.0, 1.0],
            "High": [1.0, 1.0],
            "Low": [1.0, 1.0],
            "Close": [100.0, 101.0],
            "Volume": [1, 1],
        }
    )


def test_missing_ticker_empty_boost_keeps_date_complete_cache(monkeypatch, tmp_path) -> None:
    cache = tmp_path / "ohlcv_long_full.parquet"
    _cache_df().to_parquet(cache, index=False)
    listing = pd.DataFrame(
        {
            "Code": ["005930", "999999"],
            "Name": ["삼성전자", "신규상장"],
        }
    )
    monkeypatch.setattr(stocks, "load_listing", lambda: listing)
    monkeypatch.setattr(stocks, "ohlcv_parquet_path", lambda **k: cache)
    monkeypatch.setattr(stocks, "_download_ohlcv_tasks", lambda *a, **k: [])
    monkeypatch.setattr(stocks.config, "SAMPLE_TICKERS_N", None)

    df = stocks.build_ohlcv_long(
        date(2026, 9, 17),
        date(2026, 9, 18),
        skip_gap_download=True,
        download_timeout_sec=10,
        max_workers=1,
    )
    assert not df.empty
    assert set(df["Code"].astype(str).str.zfill(6)) == {"005930"}


def test_empty_boost_raises_when_date_gap_unfilled(monkeypatch, tmp_path) -> None:
    cache = tmp_path / "ohlcv_long_full.parquet"
    _cache_df().to_parquet(cache, index=False)
    listing = pd.DataFrame({"Code": ["005930"], "Name": ["삼성전자"]})
    monkeypatch.setattr(stocks, "load_listing", lambda: listing)
    monkeypatch.setattr(stocks, "ohlcv_parquet_path", lambda **k: cache)
    monkeypatch.setattr(stocks, "_download_ohlcv_tasks", lambda *a, **k: [])
    monkeypatch.setattr(stocks.config, "SAMPLE_TICKERS_N", None)

    with pytest.raises(RuntimeError, match="가격 데이터를 가져오지 못했습니다"):
        stocks.build_ohlcv_long(
            date(2026, 9, 17),
            date(2026, 9, 21),
            skip_gap_download=False,
            download_timeout_sec=10,
            max_workers=1,
        )
