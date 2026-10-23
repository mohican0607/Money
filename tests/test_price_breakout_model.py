"""가격·거래량 랭커: 관측일 당일 이후 데이터를 쓰지 않는지."""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from src import config
from src.prediction import price_breakout_model as pbm


@pytest.fixture(autouse=True)
def _stub_regime_and_news(monkeypatch: pytest.MonkeyPatch) -> None:
    """테스트는 FDR·뉴스 캐시 없이 추세 피처만 검증(레짐·뉴스는 0)."""
    monkeypatch.setattr(
        pbm,
        "_index_ret_frame",
        lambda *_a, **_k: pd.DataFrame(columns=["Date", "idx_ret", "idx_vol5"]),
    )
    monkeypatch.setattr(
        pbm,
        "_load_news_counts",
        lambda: pd.DataFrame(columns=["Code", "NewsDay", "news_n"]),
    )
    monkeypatch.setattr(
        pbm,
        "_load_ind_theme_counts",
        lambda: pd.DataFrame(columns=["NewsDay", "ind", "ind_theme"]),
    )
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    pbm._news_cache.clear()
    pbm._index_cache.clear()


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


def test_build_raw_features_has_trend_and_regime_cols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    ret = _synthetic_returns(n_codes=10, n_days=80)
    raw = pbm.build_raw_features(ret)
    for col in (
        "r10", "r60", "ret_cum3", "close_ma20_ratio", "ks11_ret", "ixic_ret",
        "ret_max4_prev", "ret_min5", "n_crash5", "prefix_big", "prefix_ret", "prefix_lu",
    ):
        assert col in raw.columns
    pairs = pbm._training_pairs(raw)
    for col in pbm.CONTEXT_FEATURE_COLS:
        assert col in pairs.columns


def test_predict_top_ignores_target_day_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    top_a = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=5)
    assert sum(1 for r in top_a if not r.prior_limit_up) == 5

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
    assert [r.rank_position for r in rows] == list(range(1, len(rows) + 1))
    main = [r for r in rows if not r.prior_limit_up]
    assert len(main) == 3 and rows[:3] == main
    # 확률 절대값이 낮으면 mid, 충분하면 high.
    for r in main:
        if float(r.ml_prob) + 1e-12 >= pbm._HIGH_TIER_MIN_PROB:
            assert r.confidence_tier == "high"
        else:
            assert r.confidence_tier == "mid"
    assert all(-31.0 <= r.predicted_return_pct <= 31.0 for r in rows)
    assert len({round(r.predicted_return_pct, 6) for r in rows}) > 1
    assert all(r.pred_source == "price_model" for r in rows)
    assert all(0.0 <= float(r.ml_prob) <= 1.0 for r in rows)


def test_sample_weights_upweight_hard_negatives_and_quiet_hits() -> None:
    before = pd.Timestamp("2026-10-22")
    tr = pd.DataFrame(
        {
            "Date": [before - pd.Timedelta(days=5)] * 4,
            "big": [0, 0, 1, 1],
            "r1": [-0.16, 0.01, 0.01, 0.18],
            "ret_max5": [0.05, 0.05, 0.05, 0.22],
            "ret_min5": [-0.16, -0.02, -0.02, -0.01],
        }
    )
    w = pbm._sample_weights(tr, before)
    # 급락 오탐 > 평범한 음수, 잔잔한 급등 적중 > 잔열 급등 적중.
    assert w[0] > w[1]
    assert w[2] > w[3]


def test_sample_weights_upweight_name_prefix_cluster_hits() -> None:
    before = pd.Timestamp("2026-10-23")
    d = before - pd.Timedelta(days=2)
    tr = pd.DataFrame(
        {
            "Date": [d, d, d],
            "big": [1, 1, 1],
            "r1": [0.01, 0.02, 0.01],
            "ret_max5": [0.05, 0.04, 0.05],
            "ret_min5": [-0.02, -0.01, -0.02],
            "prefix": ["진양", "진양", "단독"],
            "ind": ["272", "272", "261"],
        }
    )
    w = pbm._sample_weights(tr, before)
    assert w[0] == w[1]
    assert w[0] > w[2]


def test_fit_return_predicts_expected_return_not_optimistic_quantile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    ret = _synthetic_returns(n_codes=20, n_days=80)
    raw = pbm.build_raw_features(ret)
    pairs = pbm._training_pairs(raw)
    before = pd.to_datetime(pairs["Date"]).max()
    reg = pbm._fit_return(pairs, before=before)
    assert reg.get_params()["loss"] == "squared_error"


def test_model_ranks_quiet_breakout_above_crash_rebound(monkeypatch: pytest.MonkeyPatch) -> None:
    """급락 다음날은 거의 실패, 잔잔한 다음날은 20%↑가 반복되면 모델이 잔잔한 쪽을 위로 올린다."""
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    monkeypatch.setattr(config, "PRED_LIMIT_UP_EXTRA_MAX", 0)
    days = pd.bdate_range("2025-06-02", periods=162)
    rng = np.random.default_rng(7)
    rows = []
    for i in range(40):
        close = 4000.0
        crash = i < 20
        for di, d in enumerate(days):
            r = float(rng.normal(0.0, 0.012))
            if crash and di % 9 == 3:
                r = -0.18
            elif crash and di % 9 == 4:
                r = -0.05
            elif (not crash) and di % 9 == 5:
                r = 0.01
            elif (not crash) and di % 9 == 6:
                r = 0.22
            prev = close
            close = max(50.0, close * (1.0 + r))
            rows.append(
                {
                    "Date": d,
                    "Code": f"{i:06d}",
                    "Name": f"N{i}",
                    "Open": prev,
                    "High": max(prev, close) * 1.01,
                    "Low": min(prev, close) * 0.99,
                    "Close": close,
                    "Volume": 20000.0,
                    "return_pct": float(r),
                }
            )
    ret = pd.DataFrame(rows)
    asof = days[-1]
    for i in range(20):
        _set_day_return(ret, asof, [f"{i:06d}"], -0.18)
    for i in range(20, 40):
        _set_day_return(ret, asof, [f"{i:06d}"], 0.01)
    target = (asof + pd.Timedelta(days=1)).date()
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    ranked = pbm.predict_top(
        ret, target, listing_codes=None, listing_names={}, top_n=40
    )
    main = [r for r in ranked if not r.prior_limit_up]
    by = {r.code: float(r.ml_prob) for r in main}
    crash_p = [by[f"{i:06d}"] for i in range(20) if f"{i:06d}" in by]
    quiet_p = [by[f"{i:06d}"] for i in range(20, 40) if f"{i:06d}" in by]
    assert crash_p and quiet_p
    assert float(np.mean(quiet_p)) > float(np.mean(crash_p))


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
    assert [r.pred_source for r in back] == ["price_model"] * len(rows)
    assert [r.prior_limit_up for r in back] == [r.prior_limit_up for r in rows]
    assert [round(float(r.ml_prob), 6) for r in back] == [round(float(r.ml_prob), 6) for r in rows]


def _quiet_recent(ret: pd.DataFrame, asof: pd.Timestamp, codes: list[str], n: int = 6) -> None:
    """합성 데이터의 무작위 상한가가 잔열 필터에 걸리지 않게 직전 n일을 조용히 둔다."""
    days = sorted(ret.loc[ret["Date"] < asof, "Date"].unique())[-n:]
    for d in days:
        _set_day_return(ret, d, codes, 0.01)


def _set_day_return(ret: pd.DataFrame, day: pd.Timestamp, codes: list[str], r: float) -> None:
    """해당일 ``return_pct`` 와 OHLC를 직전 종가 기준으로 맞춘다(``r2``·연속 상한 필터용)."""
    for code in codes:
        m = (ret["Date"] == day) & (ret["Code"] == code)
        if not m.any():
            continue
        idx = ret.index[m][0]
        prev = ret[(ret["Code"] == code) & (ret["Date"] < day)]
        if prev.empty:
            continue
        prev_close = float(prev.iloc[-1]["Close"])
        new_close = max(10.0, prev_close * (1.0 + r))
        ret.loc[idx, "return_pct"] = float(r)
        ret.loc[idx, "Open"] = prev_close
        ret.loc[idx, "Close"] = new_close
        ret.loc[idx, "High"] = max(prev_close, new_close) * 1.01
        ret.loc[idx, "Low"] = min(prev_close, new_close) * 0.99


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


def test_prior_day_limit_up_listed_separately_after_main(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    monkeypatch.setattr(config, "PRED_PRIOR_DAY_EXHAUSTION_BLOCK_SLATE", False)
    monkeypatch.setattr(config, "PRED_LIMIT_UP_EXTRA_MAX", 100)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    asof = ret.loc[ret["Date"] < pd.Timestamp(target), "Date"].max()
    prev = ret.loc[ret["Date"] < asof, "Date"].max()
    # 단일 상한가만 남도록 직전 구간은 소폭, asof만 상한가.
    _quiet_recent(ret, asof, ["000001", "000002"])
    _set_day_return(ret, prev, ["000001", "000002"], 0.01)
    _set_day_return(ret, asof, ["000001", "000002"], 0.299)
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=200)
    main = [r for r in rows if not r.prior_limit_up]
    extra = [r for r in rows if r.prior_limit_up]
    assert {"000001", "000002"} <= {r.code for r in extra}
    assert not {"000001", "000002"} & {r.code for r in main}
    assert rows == main + extra
    assert all(r.confidence_tier == "mid" and "전일 상한가" in r.reasons[0] for r in extra)
    assert all(r.predicted_return_pct <= pbm._LU_EXTRA_DISPLAY_PCT_CAP + 1e-9 for r in extra)
    assert all("반락 위험" in r.reasons[0] for r in extra)
    for grp in (main, extra):
        probs = [float(r.ml_prob) for r in grp]
        assert probs == sorted(probs, reverse=True)


def test_consecutive_limit_up_can_appear_in_extras(monkeypatch: pytest.MonkeyPatch) -> None:
    """연속 상한은 규칙으로 숨기지 않고, 모델 점수가 되면 별도 표시에 남긴다."""
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    monkeypatch.setattr(config, "PRED_PRIOR_DAY_EXHAUSTION_BLOCK_SLATE", False)
    monkeypatch.setattr(config, "PRED_LIMIT_UP_EXTRA_MAX", 100)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    asof = ret.loc[ret["Date"] < pd.Timestamp(target), "Date"].max()
    prev = ret.loc[ret["Date"] < asof, "Date"].max()
    _quiet_recent(ret, asof, ["000001", "000002"])
    _set_day_return(ret, prev, ["000001"], 0.299)
    _set_day_return(ret, asof, ["000001"], 0.299)
    _set_day_return(ret, prev, ["000002"], 0.01)
    _set_day_return(ret, asof, ["000002"], 0.299)
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=200)
    extra_codes = {r.code for r in rows if r.prior_limit_up}
    assert "000001" in extra_codes
    assert "000002" in extra_codes


def test_limit_up_extra_only_when_in_overall_top(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    monkeypatch.setattr(config, "PRED_PRIOR_DAY_EXHAUSTION_BLOCK_SLATE", False)
    monkeypatch.setattr(config, "PRED_LIMIT_UP_EXTRA_MAX", 0)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    asof = ret.loc[ret["Date"] < pd.Timestamp(target), "Date"].max()
    ret.loc[(ret["Date"] == asof) & ret["Code"].isin(["000001"]), "return_pct"] = 0.299
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=5)
    assert len(rows) == 5 and not any(r.prior_limit_up for r in rows)


def test_main_slate_may_include_prior_day_decliners(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    ret = _synthetic_returns()
    target = ret["Date"].max().date()
    asof = ret.loc[ret["Date"] < pd.Timestamp(target), "Date"].max()
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    rows = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=200)
    prior = ret[ret["Date"] == asof].set_index("Code")["return_pct"]
    main = [r for r in rows if not r.prior_limit_up]
    assert any(float(prior[r.code]) < 0.0 for r in main)
    assert all(float(prior[r.code]) < 0.29 for r in main)


def test_issuer_prefix_strips_preferred_suffix() -> None:
    assert pbm._issuer_prefix("진양산업") == "진양"
    assert pbm._issuer_prefix("진양화학") == "진양"
    assert pbm._issuer_prefix("소프트센우") == "소프"


def test_issuer_prefix_peer_features_exclude_self(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    days = pd.bdate_range("2026-09-01", periods=30)
    rows = []
    names = {"000001": "진양산업", "000002": "진양화학", "000003": "단독제약"}
    for code, name in names.items():
        close = 5000.0
        for d in days:
            r = 0.22 if (code == "000002" and d == days[-1]) else 0.01
            prev = close
            close = max(50.0, close * (1.0 + r))
            rows.append(
                {
                    "Date": d,
                    "Code": code,
                    "Name": name,
                    "Open": prev,
                    "High": max(prev, close) * 1.01,
                    "Low": min(prev, close) * 0.99,
                    "Close": close,
                    "Volume": 20000.0,
                    "return_pct": float(r),
                }
            )
    raw = pbm.build_raw_features(pd.DataFrame(rows))
    last = raw[raw["Date"] == days[-1]].set_index("Code")
    assert float(last.loc["000001", "prefix_big"]) == 1.0
    assert float(last.loc["000002", "prefix_big"]) == 0.0
    assert float(last.loc["000003", "prefix_big"]) == 0.0


def test_article_codes_include_name_in_title(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pbm,
        "_listing_name_pairs",
        lambda: (("아이패밀리에스씨", "114840"), ("NH투자증권", "005940")),
    )
    codes = pbm._article_codes(
        {
            "stock_code": "005940",
            "title": "日 마루베니, 아이패밀리에스씨 지분 공개매수",
            "description": "",
        }
    )
    assert "114840" in codes
    assert "005940" in codes
    assert pbm._article_codes({"stock_code": "000000", "title": "시황", "description": ""}) == []


def test_news_5d_uses_prior_days_on_single_row_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pbm,
        "_load_news_counts",
        lambda: pd.DataFrame(
            {
                "Code": ["000001", "000001"],
                "NewsDay": [pd.Timestamp("2026-10-21"), pd.Timestamp("2026-10-23")],
                "news_n": [4.0, 1.0],
            }
        ),
    )
    monkeypatch.setattr(
        pbm,
        "_load_ind_theme_counts",
        lambda: pd.DataFrame(columns=["NewsDay", "ind", "ind_theme"]),
    )
    df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2026-10-23")],
            "Code": ["000001"],
            "ind": ["272"],
            "prefix": ["진양"],
            "Name": ["진양산업"],
        }
    )
    out = pbm._attach_news_context(df)
    assert float(out.loc[0, "news_n"]) == 1.0
    assert float(out.loc[0, "news_5d"]) == 4.0


def test_model_ranks_quiet_prefix_peer_after_group_hits(monkeypatch: pytest.MonkeyPatch) -> None:
    """한쪽이 20%↑한 다음날 같은 상호의 잔잔한 형제가 반복 급등하면 그 형제를 위로 올린다."""
    monkeypatch.setattr(pbm, "_industry_map", lambda: {})
    monkeypatch.setattr(pbm, "_MIN_POSITIVES", 20)
    monkeypatch.setattr(config, "PRED_LIMIT_UP_EXTRA_MAX", 0)
    days = pd.bdate_range("2025-06-02", periods=162)
    rng = np.random.default_rng(11)
    rows = []
    for i in range(30):
        close = 4000.0
        grouped = i < 8
        name = f"진양{i}" if grouped else f"단독{i}"
        for di, d in enumerate(days):
            r = float(rng.normal(0.0, 0.006))
            if grouped and di % 8 == 3 and i == 0:
                r = 0.22
            elif grouped and di % 8 == 4 and i != 0:
                r = 0.22
            prev = close
            close = max(50.0, close * (1.0 + r))
            rows.append(
                {
                    "Date": d,
                    "Code": f"{i:06d}",
                    "Name": name,
                    "Open": prev,
                    "High": max(prev, close) * 1.01,
                    "Low": min(prev, close) * 0.99,
                    "Close": close,
                    "Volume": 20000.0,
                    "return_pct": float(r),
                }
            )
    ret = pd.DataFrame(rows)
    asof = days[-1]
    _set_day_return(ret, asof, ["000000"], 0.22)
    for i in range(1, 30):
        _set_day_return(ret, asof, [f"{i:06d}"], 0.01)
    target = (asof + pd.Timedelta(days=1)).date()
    pbm._raw_cache.clear()
    pbm._model_cache.clear()
    ranked = pbm.predict_top(ret, target, listing_codes=None, listing_names={}, top_n=30)
    main = [r for r in ranked if not r.prior_limit_up]
    by = {r.code: float(r.ml_prob) for r in main}
    peer_p = [by[f"{i:06d}"] for i in range(1, 8) if f"{i:06d}" in by]
    other_p = [by[f"{i:06d}"] for i in range(8, 30) if f"{i:06d}" in by]
    assert peer_p and other_p
    assert float(np.mean(peer_p)) > float(np.mean(other_p))
