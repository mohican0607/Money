"""투자자 순매수·호가 잔량 수집."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src import config, investor_flow as inv
from src import stocks


def test_collect_hot_orderbook_saves_only_hot_names(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path)
    snap = pd.DataFrame(
        {
            "Code": ["000001", "000002"],
            "Name": ["상한", "보합"],
            "Open": [100.0, 100.0],
            "High": [130.0, 101.0],
            "Low": [100.0, 99.0],
            "Close": [130.0, 100.0],
            "Volume": [1000.0, 500.0],
            "Change": [0.30, 0.0],
        }
    )
    monkeypatch.setattr(stocks, "try_naver_realtime_ohlcv_by_codes", lambda codes: snap)
    monkeypatch.setattr(
        inv, "fetch_asking", lambda c: {"total_buy": 5e6, "total_sell": 0.0, "best_bid_qty": 4.9e6}
    )
    monkeypatch.setattr(inv, "fetch_listed_shares", lambda codes: {"000001": 1e6})
    when = datetime(2026, 10, 8, 15, 30)
    path = inv.collect_hot_orderbook(["000001", "000002"], when=when)
    assert path is not None and path.name == "20261008_1530.json"
    rows = json.loads(path.read_text(encoding="utf-8"))["rows"]
    assert [r["code"] for r in rows] == ["000001"]
    assert rows[0]["at_limit"] is True
    assert rows[0]["mcap"] == 1.3e8
    assert rows[0]["total_buy"] == 5e6


def test_ensure_trend_skips_cached_and_dedups(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(config, "CACHE_DIR", tmp_path)
    calls: list[tuple[str, date]] = []

    def fake_fetch(code, *, before=None, page_size=5):
        calls.append((code, before))
        return pd.DataFrame(
            {
                "Code": [code, code],
                "Date": [pd.Timestamp("2026-10-07"), pd.Timestamp("2026-10-06")],
                "foreign_net": [1.0, 2.0],
                "organ_net": [0.0, 0.0],
                "indiv_net": [-1.0, -2.0],
                "foreign_hold": [0.5, 0.5],
                "volume": [10.0, 20.0],
            }
        )

    monkeypatch.setattr(inv, "fetch_trend", fake_fetch)
    out = inv.ensure_trend([("1", date(2026, 10, 8)), ("1", date(2026, 10, 8))], workers=1)
    assert len(out) == 2
    out2 = inv.ensure_trend([("000001", date(2026, 10, 8))], workers=1)
    assert len(out2) == 2
    assert len(calls) == 2
