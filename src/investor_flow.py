"""네이버 모바일 증권 API: 종목별 일별 외국인·기관·개인 순매수, 상장주식수, 호가 잔량.

- ``fetch_trend(code, before=T)``: T 이전 거래일들의 투자자별 순매수(주).
- 결과는 ``data/cache/investor_trend.parquet`` 에 (Code, Date) 단위로 누적한다.
- ``collect_hot_orderbook``: 14:30·15:30 실행 직전 급등 종목 호가 잔량을 ``data/cache/orderbook/`` 에 쌓는다.
"""
from __future__ import annotations

import json
import math
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from . import config

_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
_TREND_URL = "https://m.stock.naver.com/api/stock/{code}/trend"
_ASK_URL = "https://m.stock.naver.com/api/stock/{code}/askingPrice"
_POLL_URL = "https://polling.finance.naver.com/api/realtime/domestic/stock/"
TREND_COLS = ["Code", "Date", "foreign_net", "organ_net", "indiv_net", "foreign_hold", "volume"]


def trend_cache_path() -> Path:
    return config.CACHE_DIR / "investor_trend.parquet"


def _num(v: Any) -> float:
    try:
        return float(str(v).replace(",", "").replace("+", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return float("nan")


def fetch_trend(code: str, *, before: date | None = None, page_size: int = 20) -> pd.DataFrame:
    """``before`` 직전 거래일부터 과거로 ``page_size`` 일. ``before`` 없으면 최근."""
    code = str(code).zfill(6)
    params: dict[str, Any] = {"pageSize": int(page_size)}
    if before is not None:
        params["bizdate"] = before.strftime("%Y%m%d")
    try:
        r = requests.get(_TREND_URL.format(code=code), params=params, headers=_HEADERS, timeout=12)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError):
        return pd.DataFrame(columns=TREND_COLS)
    rows = []
    for it in data if isinstance(data, list) else []:
        try:
            d = datetime.strptime(str(it.get("bizdate")), "%Y%m%d").date()
        except ValueError:
            continue
        rows.append(
            {
                "Code": code,
                "Date": pd.Timestamp(d),
                "foreign_net": _num(it.get("foreignerPureBuyQuant")),
                "organ_net": _num(it.get("organPureBuyQuant")),
                "indiv_net": _num(it.get("individualPureBuyQuant")),
                "foreign_hold": _num(it.get("foreignerHoldRatio")),
                "volume": _num(it.get("accumulatedTradingVolume")),
            }
        )
    return pd.DataFrame(rows, columns=TREND_COLS)


def load_trend_cache() -> pd.DataFrame:
    p = trend_cache_path()
    if not p.is_file():
        return pd.DataFrame(columns=TREND_COLS)
    return pd.read_parquet(p)


def save_trend_cache(df: pd.DataFrame) -> None:
    p = trend_cache_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    df = df.drop_duplicates(["Code", "Date"], keep="last").sort_values(["Code", "Date"])
    df.reset_index(drop=True).to_parquet(p, index=False)


def ensure_trend(requests_: list[tuple[str, date]], *, workers: int = 4, page_size: int = 5) -> pd.DataFrame:
    """(code, before) 목록 중 캐시에 없는 (code, before 직전 거래일) 만 받아 캐시에 합친다."""
    cache = load_trend_cache()
    have: dict[str, set[date]] = {}
    for c, d in zip(cache["Code"].astype(str), pd.to_datetime(cache["Date"]).dt.date):
        have.setdefault(c, set()).add(d)
    todo = []
    for code, before in requests_:
        code = str(code).zfill(6)
        known = [d for d in have.get(code, ()) if d < before]
        if known and (before - max(known)).days <= 4:
            continue
        todo.append((code, before))
    if todo:
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            parts = list(ex.map(lambda a: fetch_trend(a[0], before=a[1], page_size=page_size), todo))
        parts = [p for p in parts if not p.empty]
        if parts:
            cache = pd.concat([cache, *parts] if not cache.empty else parts, ignore_index=True)
            save_trend_cache(cache)
    return cache.drop_duplicates(["Code", "Date"], keep="last").reset_index(drop=True)


def fetch_listed_shares(codes: list[str]) -> dict[str, float]:
    """현재 시가총액 / 현재가 = 상장주식수(근사)."""
    out: dict[str, float] = {}
    uniq = sorted({str(c).zfill(6) for c in codes})
    for i in range(0, len(uniq), 80):
        chunk = uniq[i : i + 80]
        try:
            r = requests.get(_POLL_URL + ",".join(chunk), headers=_HEADERS, timeout=12)
            datas = r.json().get("datas") or []
        except (requests.RequestException, ValueError):
            continue
        for it in datas:
            mv = _num(it.get("marketValueFullRaw"))
            px = _num(it.get("closePriceRaw"))
            if math.isfinite(mv) and math.isfinite(px) and px > 0:
                out[str(it.get("itemCode")).zfill(6)] = mv / px
    return out


def orderbook_snapshot_path(when: datetime) -> Path:
    return config.CACHE_DIR / "orderbook" / f"{when:%Y%m%d_%H%M}.json"


def collect_hot_orderbook(
    codes: list[str],
    *,
    when: datetime,
    min_change: float = 0.10,
    workers: int = 6,
) -> Path | None:
    """장중·종가 직후 등락률 ``min_change`` 이상 종목의 호가 잔량·시총을 저장(향후 피처 검증용).

    과거 호가는 어디서도 받을 수 없어 실행 때마다 쌓아 둔다.
    """
    from . import stocks

    snap = stocks.try_naver_realtime_ohlcv_by_codes(codes)
    if snap.empty:
        return None
    hot = snap[snap["Change"] >= float(min_change)].copy()
    if hot.empty:
        return None
    hot_codes = hot["Code"].astype(str).str.zfill(6).tolist()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        asks = dict(zip(hot_codes, ex.map(fetch_asking, hot_codes)))
    shares = fetch_listed_shares(hot_codes)
    rows = []
    for r in hot.itertuples():
        c = str(r.Code).zfill(6)
        a = asks.get(c) or {}
        rows.append(
            {
                "code": c,
                "name": str(r.Name),
                "change": float(r.Change),
                "open": float(r.Open),
                "high": float(r.High),
                "low": float(r.Low),
                "close": float(r.Close),
                "volume": float(r.Volume),
                "mcap": float(shares.get(c, float("nan")) * float(r.Close)),
                "at_limit": bool(float(r.Change) >= 0.29 and float(r.Close) >= float(r.High)),
                **{k: float(v) for k, v in a.items()},
            }
        )
    path = orderbook_snapshot_path(when)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"when": when.isoformat(), "min_change": float(min_change), "rows": rows}
    path.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=True), encoding="utf-8")
    return path


def fetch_asking(code: str) -> dict[str, float]:
    """현재 호가 잔량 합계(매수·매도)와 최우선 매수 잔량."""
    try:
        r = requests.get(_ASK_URL.format(code=str(code).zfill(6)), headers=_HEADERS, timeout=10)
        d = r.json()
    except (requests.RequestException, ValueError):
        return {}
    buys = d.get("buyInfos") or []
    return {
        "total_buy": _num(d.get("totalBuy")),
        "total_sell": _num(d.get("totalSell")),
        "best_bid_qty": _num(buys[0].get("count")) if buys else float("nan"),
    }
