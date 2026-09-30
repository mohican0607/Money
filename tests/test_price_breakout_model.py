"""가격·거래량 랭커: 관측일 당일 이후 데이터를 쓰지 않는지."""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from src import config
from src.prediction import price_breakout_model as pbm


def _synthetic_returns(n_codes: int = 60, n_days: int = 160) -> pd.DataFrame:
    rng = np.random.default_rng(1)
    days = pd.bdate_range("2026-01-02", periods=n_days)
    rows = []
    for i in range(n_codes):
        code = f"{i:06d}"
        close = 1000.0
        for d in days:
            r = float(rng.normal(0, 0.03))
            if rng.random() < 0.02:
                r = 0.29
            prev = close
            close = max(10.0, close * (1 + r))
            rows.append(
                {
                    "Date": d,
                    "Code": code,
                    "Name": f"N{i}",
                    "Open": prev,
                    "High": max(prev, close) * 1.01,
                    "Low": min(prev, close) * 0.99,
                    "Close": close,
                    "Volume": float(rng.integers(1000, 100000)),
                    "return_pct": close / prev - 1.0,
                }
            )
    return pd.DataFrame(rows)


def test_predict_top_ignores_target_day_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    top_a = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=5)
    assert len(top_a) == 5

    tampered = ret.copy()
    m = tampered["Date"] == pd.Timestamp(target)
    tampered.loc[m, "return_pct"] = 0.29
    tampered.loc[m, "Close"] = tampered.loc[m, "Close"] * 1.29
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    top_b = pbm.predict_top(tampered, target, listing_codes=None, listing_names={}, top_n=5)
    assert [r.code for r in top_a] == [r.code for r in top_b]


def test_predict_top_rows_are_high_tier_slate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=3)
    assert [r.rank_position for r in rows] == [1, 2, 3]
    assert all(r.confidence_tier == "high" for r in rows)
    assert all(-31.0 <= r.predicted_return_pct <= 31.0 for r in rows)
    assert len({round(r.predicted_return_pct, 6) for r in rows}) > 1
    assert all(r.pred_source == "price_model" for r in rows)
    assert all(0.0 <= float(r.ml_prob) <= 1.0 for r in rows)


def test_price_model_rows_survive_freeze_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.pipeline import support as ps

    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=3)
    back = ps._prediction_rows_from_frozen_items(ps._prediction_rows_to_frozen_items(rows))
    assert [r.pred_source for r in back] == ["price_model"] * 3
    assert [round(float(r.ml_prob), 6) for r in back] == [round(float(r.ml_prob), 6) for r in rows]


def test_predict_top_excludes_prior_day_decliners(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    asof = ret.loc[ret["Date"] < pd.Timestamp(target), "Date"].max()
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=20)
    assert rows
    prior = ret[ret["Date"] == asof].set_index("Code")["return_pct"]
    assert all(float(prior[r.code]) >= 0.0 for r in rows)
