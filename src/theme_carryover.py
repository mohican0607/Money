"""
전일(직전 영업일) 20%↑ 급등·표 키워드·뉴스 하이라이트로 **테마 가중치**를 만들고 JSON에 누적합니다.

관측일 ``T`` 예측 시에는 ``last_trading_day_before(T)`` 의 스냅샷(없으면 수익률표·뉴스로 합성)을 써서
휴리스틱 점수·ML 피처에 반영합니다(B안: 휴리스틱 + 단순 ML 피처 1개).
"""
from __future__ import annotations

import json
import math
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from . import config, news, predict, trading_calendar
from .features import BreakoutEvent, keyword_set

_SNAPSHOT_PATH = config.CACHE_DIR / "train" / "daily_theme_snapshots.json"
_CACHE_MTIME: float | None = None
_CACHE_PAYLOAD: dict[str, Any] | None = None


def snapshot_path() -> Path:
    return _SNAPSHOT_PATH


def _default_payload() -> dict[str, Any]:
    return {"version": 1, "by_day": {}}


def _load_payload() -> dict[str, Any]:
    global _CACHE_MTIME, _CACHE_PAYLOAD
    p = _SNAPSHOT_PATH
    if not p.is_file():
        _CACHE_MTIME, _CACHE_PAYLOAD = None, _default_payload()
        return _CACHE_PAYLOAD
    try:
        m = p.stat().st_mtime
    except OSError:
        _CACHE_MTIME, _CACHE_PAYLOAD = None, _default_payload()
        return _CACHE_PAYLOAD
    if _CACHE_MTIME == m and _CACHE_PAYLOAD is not None:
        return _CACHE_PAYLOAD
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _CACHE_MTIME, _CACHE_PAYLOAD = m, _default_payload()
        return _CACHE_PAYLOAD
    if not isinstance(raw, dict):
        raw = {}
    out = _default_payload()
    bd = raw.get("by_day")
    if isinstance(bd, dict):
        out["by_day"] = bd
    _CACHE_MTIME, _CACHE_PAYLOAD = m, out
    return out


def _invalidate_cache() -> None:
    global _CACHE_MTIME, _CACHE_PAYLOAD
    _CACHE_MTIME, _CACHE_PAYLOAD = None, None


def _save_payload(payload: dict[str, Any]) -> None:
    _SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "by_day": dict(payload.get("by_day") or {})}
    _SNAPSHOT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _invalidate_cache()


def _normalize_weights(c: Counter, *, top_n: int = 96) -> dict[str, float]:
    if not c:
        return {}
    mx = max(c.values())
    if mx <= 0:
        return {}
    out: dict[str, float] = {}
    for k, v in c.most_common(top_n):
        if not isinstance(k, str) or len(k.strip()) < 2:
            continue
        fv = float(v) / float(mx)
        if math.isfinite(fv) and fv > 0:
            out[k.strip()] = fv
    return out


def _early_blob_for_trading_day(news_by_calendar: dict[date, list[dict[str, str]]], d: date) -> str:
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by_calendar, d)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(d)
    return predict.aggregate_news_for_window(news_by_calendar, ws, we)


def synthesize_weights_for_day(
    session_day: date,
    *,
    train_events: list[BreakoutEvent],
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: pd.DataFrame,
    threshold: float,
) -> dict[str, float]:
    """
    ``session_day`` 에 20%↑였던 종목의 훈련 키워드와, 그날 early 뉴스 키워드 교집합으로 테마 가중치를 만듭니다.

    스냅샷이 없을 때 예측·ML 학습 보조용.
    """
    blob = _early_blob_for_trading_day(news_by_calendar, session_day)
    if not blob.strip():
        return {}
    kw_news = keyword_set(blob, k=120)
    ts = pd.Timestamp(session_day)
    if returns_df is None or returns_df.empty or "Date" not in returns_df.columns:
        return {}
    m = (returns_df["Date"] == ts) & (
        pd.to_numeric(returns_df["return_pct"], errors="coerce") >= float(threshold) - 1e-9
    )
    day_rows = returns_df.loc[m]
    if day_rows.empty:
        return {}
    c: Counter[str] = Counter()
    thr = float(threshold)
    for _, row in day_rows.iterrows():
        code = str(row["Code"]).zfill(6)
        rp = float(row["return_pct"])
        if not math.isfinite(rp):
            continue
        w = 1.0 + max(0.0, rp - thr) * 3.0
        hist: set[str] = set()
        for e in train_events:
            if e.code == code and e.trading_day < session_day:
                hist |= set(e.news_keywords)
        for k in hist & set(kw_news):
            c[k] += w
    return _normalize_weights(c)


def build_rich_weights_from_pipeline(
    *,
    actual_big_movers: list[dict[str, Any]],
    rows_compare: list[dict[str, Any]],
    highlight_terms: list[str],
    threshold: float,
) -> dict[str, float]:
    """당일 파이프라인 표·급등 목록·하이라이트 용어로 테마 가중치."""
    c: Counter[str] = Counter()
    mover_codes = {str(m.get("code", "")).zfill(6) for m in actual_big_movers if m.get("code")}
    thr = float(threshold)
    for r in rows_compare:
        code = str(r.get("code", "")).zfill(6)
        if code not in mover_codes:
            continue
        ar = r.get("actual_ret")
        if ar is None or not math.isfinite(float(ar)) or float(ar) < thr - 1e-9:
            continue
        w = 1.0 + max(0.0, float(ar) - thr) * 3.0
        for kw in r.get("keywords") or []:
            if isinstance(kw, str):
                kk = kw.strip()
                if len(kk) >= 2:
                    c[kk] += w
    for t in highlight_terms or []:
        if isinstance(t, str):
            tt = t.strip()
            if len(tt) >= 2:
                c[tt] += 0.35
    return _normalize_weights(c)


def load_weights_for_calendar_day(day: date) -> dict[str, float]:
    """저장된 스냅샷에서 ``day``(거래일) 테마 가중치만 읽습니다."""
    p = _load_payload()
    row = (p.get("by_day") or {}).get(day.isoformat())
    if not isinstance(row, dict):
        return {}
    w = row.get("weights")
    if not isinstance(w, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in w.items():
        if isinstance(k, str) and isinstance(v, (int, float)) and math.isfinite(float(v)):
            out[k.strip()] = float(v)
    return out


def persist_rich_snapshot(
    trading_day: date,
    *,
    actual_big_movers: list[dict[str, Any]],
    rows_compare: list[dict[str, Any]],
    highlight_terms: list[str],
    threshold: float,
) -> None:
    """관측일 ``trading_day`` 처리 후, 그날 급등·표 기반 테마를 JSON에 병합합니다."""
    if not config.THEME_CARRYOVER_ENABLED:
        return
    weights = build_rich_weights_from_pipeline(
        actual_big_movers=actual_big_movers,
        rows_compare=rows_compare,
        highlight_terms=highlight_terms,
        threshold=threshold,
    )
    p = _load_payload()
    by_day = dict(p.get("by_day") or {})
    t_iso = trading_day.isoformat()
    by_day[t_iso] = {
        "weights": dict(weights),
        "n_movers": len(actual_big_movers),
        "source": "pipeline",
    }
    p["by_day"] = by_day
    _save_payload(p)


def weights_for_observation_day(
    target_day: date,
    *,
    train_events: list[BreakoutEvent],
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: pd.DataFrame,
    threshold: float,
) -> dict[str, float]:
    """
    관측일 ``target_day`` 예측에 쓸 테마 가중치 = **직전 영업일** 스냅샷(또는 합성).
    """
    if not config.THEME_CARRYOVER_ENABLED:
        return {}
    try:
        prev_td = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        return {}
    snap = load_weights_for_calendar_day(prev_td)
    if snap:
        return snap
    return synthesize_weights_for_day(
        prev_td,
        train_events=train_events,
        news_by_calendar=news_by_calendar,
        returns_df=returns_df,
        threshold=threshold,
    )


def theme_kw_overlap_score(kw_news: frozenset[str], theme_weights: dict[str, float] | None) -> float:
    """뉴스 키워드와 테마 가중치의 가중합(상한 캡). ML·로그용."""
    if not theme_weights:
        return 0.0
    s = 0.0
    for k in kw_news:
        v = theme_weights.get(k)
        if v is not None and math.isfinite(float(v)):
            s += float(v)
    return float(min(s, 14.0))
