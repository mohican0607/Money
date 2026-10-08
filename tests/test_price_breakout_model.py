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


def _snapshot_for(ret: pd.DataFrame, day: pd.Timestamp, change: float = 0.0) -> pd.DataFrame:
    last = ret[ret["Date"] == ret["Date"].max()]
    return pd.DataFrame(
        {
            "Code": last["Code"].values,
            "Name": last["Name"].values,
            "Open": last["Close"].values,
            "High": last["Close"].values * (1 + max(change, 0) + 0.01),
            "Low": last["Close"].values * 0.99,
            "Close": last["Close"].values * (1 + change),
            "Volume": last["Volume"].values,
            "Change": change,
        }
    )


def test_price_model_inputs_fills_today_from_intraday_snapshot() -> None:
    from datetime import datetime

    from src import trading_calendar
    from src.pipeline import support as ps

    ret = _synthetic_returns(n_codes=10, n_days=30)
    last = ret["Date"].max().date()
    n_day = trading_calendar.next_trading_day_after(last)
    target = trading_calendar.next_trading_day_after(n_day)
    now = datetime(n_day.year, n_day.month, n_day.day, 14, 30, tzinfo=trading_calendar.KST)
    calls: list[list[str]] = []

    def fetch(codes: list[str]) -> pd.DataFrame:
        calls.append(codes)
        return _snapshot_for(ret, pd.Timestamp(n_day), change=0.05)

    out, partial, note = ps._price_model_inputs(ret, target, None, now_kst=now, fetch_snapshot=fetch)
    assert partial is True and calls
    assert pd.to_datetime(out["Date"]).max().date() == n_day
    assert "장중 시세" in note
    day_rows = out[pd.to_datetime(out["Date"]).dt.date == n_day]
    assert len(day_rows) == 10 and (day_rows["return_pct"] == 0.05).all()


def test_price_model_inputs_low_coverage_falls_back_with_note() -> None:
    from datetime import datetime

    from src import trading_calendar
    from src.pipeline import support as ps

    ret = _synthetic_returns(n_codes=10, n_days=30)
    last = ret["Date"].max().date()
    n_day = trading_calendar.next_trading_day_after(last)
    target = trading_calendar.next_trading_day_after(n_day)
    now = datetime(n_day.year, n_day.month, n_day.day, 14, 30, tzinfo=trading_calendar.KST)
    out, partial, note = ps._price_model_inputs(
        ret, target, None, now_kst=now, fetch_snapshot=lambda codes: _snapshot_for(ret, n_day).head(2)
    )
    assert out is ret and partial is False
    assert "종가 기준" in note


def test_price_model_inputs_untouched_when_prior_day_present() -> None:
    from src import trading_calendar
    from src.pipeline import support as ps

    ret = _synthetic_returns(n_codes=10, n_days=30)
    target = trading_calendar.next_trading_day_after(ret["Date"].max().date())
    out, partial, note = ps._price_model_inputs(
        ret, target, None, fetch_snapshot=lambda codes: pytest.fail("fetch must not run")
    )
    assert out is ret and partial is False and note == ""


def test_predict_top_uses_intraday_session_day(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import trading_calendar

    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    ret = _synthetic_returns()
    n_day = trading_calendar.next_trading_day_after(ret["Date"].max().date())
    target = trading_calendar.next_trading_day_after(n_day)
    snap = _snapshot_for(ret, pd.Timestamp(n_day), change=0.0)
    snap.loc[snap.index[:3], ["Change"]] = 0.29
    snap.loc[snap.index[:3], "Close"] = snap.loc[snap.index[:3], "Close"] * 1.29
    snap.loc[snap.index[:3], "High"] = snap.loc[snap.index[:3], "Close"]
    merged = pbm.with_intraday_session(ret, n_day, snap)
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(
        merged, target, listing_codes=None, listing_names={}, top_n=5,
        partial_last_day=True, basis_note="장중 시세 기준",
    )
    assert rows
    assert all("장중 시세 기준" in r.reasons[0] for r in rows)
    by_code = snap.set_index("Code")["Change"]
    assert all(abs(float(r.ret_lag1) - float(by_code[r.code])) < 1e-9 for r in rows)


def test_predict_top_keeps_prior_day_limit_up_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    monkeypatch.setattr(config, "PRED_PRIOR_DAY_EXHAUSTION_BLOCK_SLATE", False)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    asof = ret.loc[ret["Date"] < pd.Timestamp(target), "Date"].max()
    m = (ret["Date"] == asof) & ret["Code"].isin(["000001", "000002"])
    ret.loc[m, "return_pct"] = 0.299
    lu = ret.loc[m, "Code"]
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=200)
    assert set(lu) <= {r.code for r in rows}
    probs = [float(r.ml_prob) for r in rows]
    assert probs == sorted(probs, reverse=True)


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
