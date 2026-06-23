"""
네이버 증권 투자자별 순매수(외국인·기관·개인) 수집·캐시·예측 피처.

- 출처: ``https://finance.naver.com/item/frgn.naver?code=XXXXXX``
- 캐시: ``data/cache/investor_flow/naver/{code}.json``
- 예측 시 **직전 거래일(lag1)** 까지 값만 사용(장 시작 전 알 수 있는 정보).
"""
from __future__ import annotations

import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

from . import config, trading_calendar

_CACHE_ROOT = config.CACHE_DIR / "investor_flow" / "naver"
_FLOW_COLS = (
    "foreign_net_lag1",
    "inst_net_lag1",
    "individual_net_lag1",
    "foreign_net_vol_ratio_lag1",
    "inst_net_vol_ratio_lag1",
    "foreign_holding_pct_lag1",
    "foreign_net_sum3_ratio",
    "investor_flow_score",
)


def _code_path(code: str) -> Path:
    """종목별 수급 캐시 JSON 경로."""
    return _CACHE_ROOT / f"{str(code).zfill(6)}.json"


def _parse_int(s: str) -> int:
    """네이버 표 셀 문자열 → 정수(쉼표·기호 제거)."""
    t = re.sub(r"[^\d\-+]", "", str(s or ""))
    if not t or t in ("+", "-"):
        return 0
    try:
        return int(t)
    except ValueError:
        return 0


def _parse_pct(s: str) -> float:
    """``12.34%`` 형태 → float."""
    t = str(s or "").replace("%", "").strip()
    try:
        return float(t)
    except ValueError:
        return 0.0


def _parse_date_dot(s: str) -> date | None:
    """``YYYY.MM.DD`` → ``date``."""
    m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", str(s or ""))
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def naver_investor_url(code: str) -> str:
    """네이버 증권 투자자별 매매동향 페이지 URL."""
    return f"https://finance.naver.com/item/frgn.naver?code={str(code).zfill(6)}"


def fetch_investor_history(code: str, *, session: requests.Session | None = None) -> list[dict[str, Any]]:
    """종목별 최근 투자자 순매수 표(약 20거래일)를 파싱."""
    code = str(code).zfill(6)
    sess = session or requests.Session()
    try:
        r = sess.get(
            naver_investor_url(code),
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        r.raise_for_status()
        r.encoding = "euc-kr"
    except requests.RequestException:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.select("table.type2")
    target = None
    for table in tables:
        txt = table.get_text(" ", strip=True)
        if "202" in txt and ("%" in txt or "거래량" in txt or "외국인" in txt):
            target = table
            break
    if target is None and tables:
        for table in tables:
            for tr in table.select("tr")[:25]:
                tds = [td.get_text(strip=True) for td in tr.select("td")]
                if tds and _parse_date_dot(tds[0]):
                    target = table
                    break
            if target is not None:
                break
    if target is None and tables:
        target = tables[-1]
    if target is None:
        return []

    out: list[dict[str, Any]] = []
    for tr in target.select("tr"):
        tds = [td.get_text(strip=True) for td in tr.select("td")]
        if len(tds) < 7:
            continue
        d = _parse_date_dot(tds[0])
        if d is None:
            continue
        vol = _parse_int(tds[4])
        inst_net = _parse_int(tds[5])
        foreign_net = _parse_int(tds[6])
        foreign_pct = _parse_pct(tds[8] if len(tds) > 8 else "0")
        individual_net = -(inst_net + foreign_net)
        out.append(
            {
                "date": d.isoformat(),
                "volume": vol,
                "inst_net": inst_net,
                "foreign_net": foreign_net,
                "individual_net": individual_net,
                "foreign_holding_pct": foreign_pct,
            }
        )
    return out


def load_cached_history(code: str) -> list[dict[str, Any]]:
    """``data/cache/investor_flow/naver/{code}.json`` 의 ``rows`` 목록."""
    path = _code_path(code)
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = raw.get("rows") if isinstance(raw, dict) else raw
    return list(rows) if isinstance(rows, list) else []


def save_cached_history(code: str, rows: list[dict[str, Any]]) -> None:
    """수급 이력을 날짜 키로 병합·정렬해 캐시에 저장."""
    path = _code_path(code)
    path.parent.mkdir(parents=True, exist_ok=True)
    by_date: dict[str, dict[str, Any]] = {}
    for row in load_cached_history(code):
        if isinstance(row, dict) and row.get("date"):
            by_date[str(row["date"])] = row
    for row in rows:
        if isinstance(row, dict) and row.get("date"):
            by_date[str(row["date"])] = row
    merged = sorted(by_date.values(), key=lambda x: str(x.get("date", "")))
    path.write_text(
        json.dumps({"code": str(code).zfill(6), "rows": merged}, ensure_ascii=False),
        encoding="utf-8",
    )


def warm_cache_for_codes(
    codes: list[str],
    *,
    max_codes: int | None = None,
    workers: int | None = None,
    sleep_sec: float = 0.05,
) -> int:
    """누락 종목만 네이버에서 순매수 이력을 받아 캐시합니다."""
    if not config.PRED_INVESTOR_FLOW_ENABLED:
        return 0
    cap = max_codes if max_codes is not None else int(config.INVESTOR_FLOW_PREFETCH_MAX_CODES)
    todo = [str(c).zfill(6) for c in codes if c][: max(1, cap)]
    missing = [c for c in todo if not _code_path(c).is_file()]
    if not missing:
        return 0
    n_workers = workers if workers is not None else int(config.INVESTOR_FLOW_FETCH_WORKERS)
    n_workers = max(1, min(n_workers, 8))
    fetched = 0

    def _one(c: str) -> bool:
        """단일 종목 fetch → 캐시 저장 성공 여부."""
        sess = requests.Session()
        rows = fetch_investor_history(c, session=sess)
        if rows:
            save_cached_history(c, rows)
            return True
        return False

    with ThreadPoolExecutor(max_workers=n_workers) as ex:
        futs = {ex.submit(_one, c): c for c in missing}
        for fut in as_completed(futs):
            try:
                if fut.result():
                    fetched += 1
            except Exception:
                pass
            if sleep_sec > 0:
                time.sleep(sleep_sec)
    return fetched


def priority_codes_for_prefetch(
    returns_df: pd.DataFrame,
    listing_codes: list[str],
    *,
    train_event_codes: list[str] | None = None,
    move_threshold: float | None = None,
) -> list[str]:
    """급등·학습 이력 종목 우선으로 수급 캐시 대상 코드를 고릅니다."""
    thr = float(move_threshold if move_threshold is not None else config.BIG_MOVE_THRESHOLD)
    seen: set[str] = set()
    out: list[str] = []

    def _add(c: str) -> None:
        """우선순위 목록에 중복 없이 추가."""
        z = str(c).zfill(6)
        if z and z not in seen:
            seen.add(z)
            out.append(z)

    for c in train_event_codes or []:
        _add(c)
    if returns_df is not None and not returns_df.empty and "return_pct" in returns_df.columns:
        hot = returns_df.loc[returns_df["return_pct"].astype(float) >= thr * 0.5, "Code"]
        for c in hot.astype(str).str.zfill(6).unique().tolist():
            _add(c)
    for c in listing_codes:
        _add(c)
        if len(out) >= int(config.INVESTOR_FLOW_PREFETCH_MAX_CODES):
            break
    return out


def _history_by_code(code: str) -> dict[date, dict[str, Any]]:
    """캐시 rows → ``date`` 키 dict."""
    rows = load_cached_history(code)
    out: dict[date, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        d = row.get("date")
        if isinstance(d, date):
            out[d] = row
            continue
        if isinstance(d, str):
            try:
                out[date.fromisoformat(d[:10])] = row
            except ValueError:
                continue
    return out


def _flow_score(
    foreign_ratio: float,
    inst_ratio: float,
    foreign_pct: float,
    foreign_sum3_ratio: float,
) -> float:
    """0~1 수급 점수(외국인 비중·순매수 강조)."""
    def _sig(x: float) -> float:
        """로지스틱 squashing (±6 클램프)."""
        return 1.0 / (1.0 + math.exp(-max(-6.0, min(6.0, x))))

    f_buy = _sig(float(foreign_ratio) * 6.0)
    i_buy = _sig(float(inst_ratio) * 4.0)
    hold = max(0.0, min(1.0, float(foreign_pct) / 55.0))
    streak = _sig(float(foreign_sum3_ratio) * 4.0)
    return max(
        0.0,
        min(
            1.0,
            0.46 * f_buy + 0.18 * i_buy + 0.20 * hold + 0.16 * streak,
        ),
    )


def flow_features_for_day(code: str, trading_day: date) -> dict[str, float]:
    """관측일 ``trading_day`` 장 시작 전에 알 수 있는 수급 피처(lag1·3일 누적)."""
    hist = _history_by_code(code)
    if not hist:
        return {c: 0.0 for c in _FLOW_COLS}

    try:
        prev = trading_calendar.last_trading_day_before(trading_day)
    except ValueError:
        return {c: 0.0 for c in _FLOW_COLS}

    row = hist.get(prev)
    if not row:
        return {c: 0.0 for c in _FLOW_COLS}

    vol = max(1, int(row.get("volume") or 0))
    fn = int(row.get("foreign_net") or 0)
    inn = int(row.get("inst_net") or 0)
    ind = int(row.get("individual_net") or -(fn + inn))
    f_ratio = max(-1.5, min(1.5, fn / vol))
    i_ratio = max(-1.5, min(1.5, inn / vol))
    f_pct = float(row.get("foreign_holding_pct") or 0.0)

    days_back = [prev]
    d = prev
    for _ in range(2):
        try:
            d = trading_calendar.last_trading_day_before(d)
            days_back.append(d)
        except ValueError:
            break
    f_sum = 0
    v_sum = 0
    for d0 in days_back:
        r0 = hist.get(d0)
        if not r0:
            continue
        f_sum += int(r0.get("foreign_net") or 0)
        v_sum += max(1, int(r0.get("volume") or 0))
    f_sum3 = f_sum / max(1, v_sum)

    score = _flow_score(f_ratio, i_ratio, f_pct, f_sum3)
    return {
        "foreign_net_lag1": float(fn),
        "inst_net_lag1": float(inn),
        "individual_net_lag1": float(ind),
        "foreign_net_vol_ratio_lag1": float(f_ratio),
        "inst_net_vol_ratio_lag1": float(i_ratio),
        "foreign_holding_pct_lag1": float(f_pct),
        "foreign_net_sum3_ratio": float(f_sum3),
        "investor_flow_score": float(score),
    }


def merge_flow_features(returns_ml: pd.DataFrame) -> pd.DataFrame:
    """``returns_ml`` 각 행에 투자자별 수급 lag 피처를 붙입니다."""
    if not config.PRED_INVESTOR_FLOW_ENABLED or returns_ml is None or returns_ml.empty:
        return returns_ml
    df = returns_ml.copy()
    n = len(df)
    arrays: dict[str, list[float]] = {col: [0.0] * n for col in _FLOW_COLS}

    for pos, (_, row) in enumerate(df.iterrows()):
        code = str(row.get("Code", "")).zfill(6)
        d_raw = row.get("Date")
        if isinstance(d_raw, pd.Timestamp):
            t_day = d_raw.date()
        elif isinstance(d_raw, date):
            t_day = d_raw
        else:
            continue
        feats = flow_features_for_day(code, t_day)
        for k in _FLOW_COLS:
            arrays[k][pos] = float(feats.get(k, 0.0))

    for k in _FLOW_COLS:
        df[k] = arrays[k]
    return df


def investor_flow_candidate_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    top_k: int = 80,
) -> list[str]:
    """외국인·기관 순매수가 강한 종목(뉴스 없이도 후보 확장)."""
    if not config.PRED_INVESTOR_FLOW_ENABLED:
        return []
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    if sl.empty:
        return []
    allowed = {str(c).zfill(6) for c in listing_codes}
    scored: list[tuple[float, str]] = []
    for _, r in sl.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        fs = float(r.get("investor_flow_score") or 0.0)
        fr = float(r.get("foreign_net_vol_ratio_lag1") or 0.0)
        if fs + 1e-12 < 0.42 and fr + 1e-12 < 0.02:
            continue
        scored.append((fs + 0.25 * max(0.0, fr), code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [c for _, c in scored[: max(10, int(top_k))]]
