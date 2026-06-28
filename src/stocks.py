"""
FinanceDataReader 기반 KOSPI·KOSDAQ 일별 OHLCV·수익률·급등 종목 조회.

- 상장 리스트는 ``krx_listing.parquet`` 에 캐시합니다.
- 일봉은 ``ohlcv_long[_krx]_full.parquet`` 또는 ``SAMPLE_TICKERS`` 용 별도 파일에 씁니다.
- ``pykrx`` 가 있으면 당일 전종목 등락률로 「실제 20%↑」를 보강합니다.
"""
from __future__ import annotations

import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import FinanceDataReader as fdr
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from . import config, trading_calendar

LISTING_CACHE = config.CACHE_DIR / "krx_listing.parquet"
_sfx         = f"_{config.SAMPLE_TICKERS_N}tickers" if config.SAMPLE_TICKERS_N else "_full"
_ohlcv_base  = "ohlcv_long_krx" if config.USE_KRX_OHLCV else "ohlcv_long"
PRICES_CACHE = config.CACHE_DIR / f"{_ohlcv_base}{_sfx}.parquet"


def ohlcv_parquet_path(*, full_universe: bool) -> Path:
    """
    OHLCV Parquet 파일 경로를 반환합니다.

    Args:
        full_universe: True이면 전종목용 ``*_full.parquet``. False이고 ``SAMPLE_TICKERS_N`` 이
            설정되어 있으면 ``_*Ntickers.parquet`` (표본 전용).

    Returns:
        ``config.CACHE_DIR`` 아래 ``ohlcv_long`` 또는 ``ohlcv_long_krx`` (``USE_KRX_OHLCV``) 파일 경로.
    """
    base = "ohlcv_long_krx" if config.USE_KRX_OHLCV else "ohlcv_long"
    if full_universe or not config.SAMPLE_TICKERS_N:
        sfx = "_full"
    else:
        sfx = f"_{config.SAMPLE_TICKERS_N}tickers"
    return config.CACHE_DIR / f"{base}{sfx}.parquet"


def _ensure_cache_dir() -> None:
    """``data/cache`` 디렉터리가 없으면 생성합니다."""
    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_listing() -> pd.DataFrame:
    """
    KRX 상장 종목 목록을 로드합니다.

    캐시 ``krx_listing.parquet`` 가 있으면 읽고, 없으면 FinanceDataReader로
    ``StockListing("KRX")`` 를 받아 KOSPI/KOSDAQ/KOSDAQ GLOBAL 만 남긴 뒤 저장합니다.

    Returns:
        최소 ``Code``, ``Name``, ``Market`` 컬럼을 가진 DataFrame.
    """
    _ensure_cache_dir()
    if LISTING_CACHE.exists():
        return pd.read_parquet(LISTING_CACHE)
    print(
        "네트워크: KRX 상장 목록 다운로드 중 (FinanceDataReader StockListing)...",
        flush=True,
    )
    df = fdr.StockListing("KRX")
    df = df[df["Market"].isin(["KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"])].copy()
    df.to_parquet(LISTING_CACHE, index=False)
    print(f"상장 목록 완료: {len(df)}종 -> {LISTING_CACHE.name}", flush=True)
    return df


def market_segment_by_code() -> dict[str, str]:
    """
    상장 목록 기준 6자리 종목코드 → ``kospi`` | ``kosdaq`` | ``other``.

    ``KOSDAQ``·``KOSDAQ GLOBAL`` 은 ``kosdaq`` 로 묶습니다. 리포트 시장 필터용.
    """
    df = load_listing()
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        c = str(row["Code"]).zfill(6)
        m = str(row.get("Market", "") or "")
        if m == "KOSPI":
            out[c] = "kospi"
        elif m in ("KOSDAQ", "KOSDAQ GLOBAL"):
            out[c] = "kosdaq"
        else:
            out[c] = "other"
    return out


def _download_one_ticker(args: tuple[str, str, date, date]) -> pd.DataFrame | None:
    """
    단일 종목·구간에 대해 FinanceDataReader로 OHLCV를 가져옵니다.

    ``USE_KRX_OHLCV`` 이면 ``KRX:코드`` 후 실패 시 ``코드`` 순으로 시도합니다.
    데이터가 없거나 예외면 ``None`` 을 반환합니다(스레드 풀에서 호출됨).
    """
    code, name, start, end = args
    symbols = (f"KRX:{code}", code) if config.USE_KRX_OHLCV else (code,)
    for sym in symbols:
        try:
            ohlcv = fdr.DataReader(sym, start, end)
            if ohlcv is None or ohlcv.empty:
                continue
            ohlcv = ohlcv.reset_index()
            ohlcv["Code"] = code
            ohlcv["Name"] = name
            return ohlcv
        except Exception:
            continue
    return None


def _clamp_calendar_gap_to_trading_span(gs: date, ge: date) -> tuple[date, date] | None:
    """
    캘린더 구간 [gs, ge] 안에서 첫·마지막 **실거래일**만 남깁니다.

    임시 휴장(예: 지방선거)은 ``trading_calendar.is_trading_day`` 가 False 이므로 보강 대상에서 제외됩니다.
    """
    from . import trading_calendar

    if gs > ge:
        return None
    d = gs
    while d <= ge and not trading_calendar.is_trading_day(d):
        d += timedelta(days=1)
    if d > ge:
        return None
    first = d
    d = ge
    while d >= first and not trading_calendar.is_trading_day(d):
        d -= timedelta(days=1)
    if d < first:
        return None
    return first, d


def _ohlcv_calendar_gaps(start: date, end: date, dmin: date, dmax: date) -> list[tuple[date, date]]:
    """
    캐시가 [dmin, dmax], 요청이 [start, end] 일 때 API로 받아야 할 부분 구간들.

    겹치지 않으면 전체 [start, end] 한 덩어리. 겹치면 왼쪽(요청 시작~캐시 전날)·오른쪽(캐시 다음날~요청 끝)만.
    각 구간은 **실거래일** 기준으로 잘라 임시 휴장 캘린더일은 건너뜁니다.
    """
    if start > end:
        return []
    if end < dmin or start > dmax:
        raw = [(start, end)]
    else:
        raw = []
        if start < dmin:
            ge = min(end, dmin - timedelta(days=1))
            if start <= ge:
                raw.append((start, ge))
        if end > dmax:
            gs = max(start, dmax + timedelta(days=1))
            if gs <= end:
                raw.append((gs, end))
    gaps: list[tuple[date, date]] = []
    for gs, ge in raw:
        span = _clamp_calendar_gap_to_trading_span(gs, ge)
        if span is not None:
            gaps.append(span)
    return gaps


def _download_ohlcv_tasks(
    tasks: list[tuple[str, str, date, date]],
    max_workers: int,
    deadline: float,
    *,
    desc: str,
) -> list[pd.DataFrame]:
    """종목×구간 작업 목록을 실행해 비어 있지 않은 DataFrame 청크 리스트를 반환."""
    chunks: list[pd.DataFrame] = []
    if not tasks:
        return chunks
    print(
        f"네트워크: OHLCV 수신 시작: {len(tasks)}건, 동시 {max_workers}워커 ({desc})",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        pending = {ex.submit(_download_one_ticker, t): t for t in tasks}
        with tqdm(
            total=len(tasks),
            desc=desc,
            file=sys.stderr,
            dynamic_ncols=True,
            mininterval=0.25,
        ) as pbar:
            while pending and time.time() < deadline:
                wait_timeout = min(8.0, max(0.5, deadline - time.time()))
                if wait_timeout <= 0:
                    break
                done, pending = wait(pending, timeout=wait_timeout, return_when=FIRST_COMPLETED)
                for fut in done:
                    try:
                        r = fut.result()
                        if r is not None and not r.empty:
                            chunks.append(r)
                    except Exception:
                        pass
                    pbar.update(1)
            for fut in pending:
                fut.cancel()
                pbar.update(1)
    print(
        f"네트워크: 다운로드 단계 종료: 유효 OHLCV 청크 {len(chunks)}개 (요청 {len(tasks)}건)",
        flush=True,
    )
    return chunks


def _normalize_ohlcv_df(df: pd.DataFrame) -> pd.DataFrame:
    """OHLCV DataFrame의 ``Date``·``Code`` 컬럼 형식을 파이프라인 공통 스키마로 맞춤."""
    if "Date" not in df.columns and "index" in df.columns:
        df = df.rename(columns={"index": "Date"})
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    df["Code"] = df["Code"].astype(str).str.zfill(6)
    return df


def build_ohlcv_long(
    start: date,
    end: date,
    max_workers: int | None = None,
    download_timeout_sec: int | None = None,
    *,
    force_full_listing: bool = False,
    skip_gap_download: bool = False,
    refresh_tail_days: int = 0,
) -> pd.DataFrame:
    """
    전 종목(또는 표본) 일별 OHLCV를 세로로 쌓은 long-form DataFrame을 만듭니다.

    동작 요약:
        1. 캐시가 ``[start, end]`` 를 완전히 덮으면 슬라이스만 읽어 반환.
        2. 캐시가 일부만 덮으면 **비는 캘린더 구간만** 종목별 다운로드 후 기존 Parquet과 ``concat``,
           ``Date``+``Code`` 기준 중복 제거 후 저장(날짜가 늘어날 때마다 전량 재다운로드하지 않음).
        3. 캐시가 없으면 ``[start, end]`` 전체를 받아 저장.

    Args:
        start, end: 조회 캘린더 구간(일봉 인덱스와 맞춤).
        max_workers: 동시 다운로드 스레드 수. ``None`` 이면 ``config.OHLCV_MAX_WORKERS``.
        download_timeout_sec: 전체 다운로드 상한(초). ``None`` 이면 표본 360초,
            전종목은 ``max(7200, min(28800, 종목수*12))`` 로 자동 설정.
        force_full_listing: ``SAMPLE_TICKERS_N`` 이 있어도 무시하고 전상장·``_full`` 캐시 사용.
            과거일 리포트에서 pykrx 실패 시 OHLCV 범위만으로 「실제 20%↑」를 넓히기 위함.
        skip_gap_download: True이면 캐시의 마지막 일자(``dmax``)보다 뒤를 채우려는 **우측 보강** 다운로드를
            하지 않고, ``end`` 를 ``dmax`` 로 줄여 캐시 구간만 반환합니다(예: 당일 장 마감 전·N이 미래일 때).
        refresh_tail_days: 1 이상이면 캐시가 요청 구간을 덮더라도 마지막 N일은 다시 받아 최신 종가로 갱신합니다.

    Returns:
        ``Date``, ``Code``, ``Name`` 및 시세 컬럼(Close 등). KRX 모드면 ``Change``(등락률) 포함 가능.

    Raises:
        RuntimeError: 타임아웃 내 성공한 청크가 하나도 없을 때.

    Note:
        ``USE_KRX_OHLCV=1`` 이면 KRX 일봉 우선(거래소 등락률), 실패 시 일반 심볼로 폴백.
    """
    _ensure_cache_dir()
    use_sample = bool(config.SAMPLE_TICKERS_N) and not force_full_listing
    cache_file = ohlcv_parquet_path(full_universe=not use_sample)

    listing = load_listing()
    if use_sample:
        print(
            f"경고: SAMPLE_TICKERS={config.SAMPLE_TICKERS_N} - "
            "상위 N종만 OHLCV에 있어, 그 밖의 시장 20% 급등 종목은 리포트에 나오지 않습니다.",
            flush=True,
        )
        listing = listing.head(config.SAMPLE_TICKERS_N)
    elif force_full_listing and config.SAMPLE_TICKERS_N:
        print(
            f"과거 분석 모드: SAMPLE_TICKERS={config.SAMPLE_TICKERS_N} 을 무시하고 전종목 OHLCV(캐시 {cache_file.name})를 사용합니다.",
            flush=True,
        )

    codes = listing["Code"].astype(str).str.zfill(6).tolist()
    names = listing["Name"].tolist()
    name_by_code = {c: n for c, n in zip(codes, names)}

    if max_workers is None:
        max_workers = config.OHLCV_MAX_WORKERS

    if download_timeout_sec is None:
        # 샘플만 쓸 때는 짧게; 전종목은 360초면 대부분 취소되어 캐시가 안 생길 수 있음
        download_timeout_sec = (
            360
            if use_sample
            else max(7200, min(28_800, len(codes) * 12))
        )
    if not use_sample:
        print(
            f"전종목 OHLCV 다운로드 타임아웃 {download_timeout_sec}s, "
            f"워커 {max_workers}, ({len(codes)}종목) -> 캐시 {cache_file.name}",
            flush=True,
        )

    deadline = time.time() + download_timeout_sec

    if cache_file.exists():
        print(f"디스크: 기존 OHLCV 캐시 읽는 중... ({cache_file.name})", flush=True)
        old = pd.read_parquet(cache_file)
        old = _normalize_ohlcv_df(old)
        dmin, dmax = old["Date"].min().date(), old["Date"].max().date()
        print(f"캐시에 있는 날짜 범위: {dmin} ~ {dmax}", flush=True)
        if skip_gap_download and dmax < end:
            print(
                f"OHLCV: 요청 끝 {end} 는 캐시 끝 {dmax} 보다 뒤입니다. "
                "당일 장 마감 전·미래 관측일 등으로 우측 보강 다운로드를 생략하고 캐시까지만 사용합니다.",
                flush=True,
            )
            end = dmax
        cached_codes = set(old["Code"].astype(str).str.zfill(6).unique().tolist())
        expected_codes = set(codes)
        missing_codes = sorted(expected_codes - cached_codes)
        if not use_sample and missing_codes:
            print(
                f"OHLCV 캐시 종목 보강 필요: {len(missing_codes)}종 누락 "
                f"(캐시 {len(cached_codes)} / 기대 {len(expected_codes)}).",
                flush=True,
            )
        if dmin <= start and dmax >= end and refresh_tail_days <= 0 and (
            use_sample or not missing_codes
        ):
            print(
                f"캐시 히트: 요청 구간 {start} ~ {end} 는 캐시로 충족됩니다.",
                flush=True,
            )
            m = (old["Date"] >= pd.Timestamp(start)) & (old["Date"] <= pd.Timestamp(end))
            return old.loc[m].copy()

        gaps = _ohlcv_calendar_gaps(start, end, dmin, dmax)
        if refresh_tail_days > 0:
            tail_start = max(start, end - timedelta(days=max(0, refresh_tail_days - 1)))
            if tail_start <= end:
                # 장중 캐시/정정 반영 등으로 같은 날짜 값이 달라질 수 있어 꼬리 구간은 재조회 후 덮어쓴다.
                gaps.append((tail_start, end))

        # 중복 구간 정리
        uniq: list[tuple[date, date]] = []
        seen_gaps: set[tuple[date, date]] = set()
        for g in gaps:
            if g in seen_gaps:
                continue
            seen_gaps.add(g)
            uniq.append(g)
        gaps = uniq

        gap_tasks: list[tuple[str, str, date, date]] = []
        for gs, ge in gaps:
            gap_tasks.extend([(c, n, gs, ge) for c, n in zip(codes, names)])
        if not use_sample and missing_codes:
            # 과거 캐시가 부분 다운로드로 저장된 경우, 누락 종목을 요청 구간 전체로 보강.
            gap_tasks.extend(
                [(c, name_by_code.get(c, c), start, end) for c in missing_codes]
            )
        # 중복 task 제거
        gap_tasks = list(dict.fromkeys(gap_tasks))
        if gap_tasks:
            gap_label = ", ".join(f"{a}~{b}" for a, b in gaps)
            print(
                f"OHLCV 캐시 확장: 기존 {dmin}~{dmax}, 추가 구간 {gap_label}",
                flush=True,
            )
            chunks = _download_ohlcv_tasks(
                gap_tasks,
                max_workers,
                deadline,
                desc="가격 다운로드(캐시 보강)",
            )
            if not chunks:
                raise RuntimeError(
                    f"가격 데이터를 가져오지 못했습니다. 보강 구간 {gap_label}. 네트워크/기간을 확인하세요."
                )
            new_df = pd.concat(chunks, ignore_index=True)
            new_df = _normalize_ohlcv_df(new_df)
            df = pd.concat([old, new_df], ignore_index=True)
            df = df.drop_duplicates(subset=["Date", "Code"], keep="last")
            df = df.sort_values(["Date", "Code"]).reset_index(drop=True)
        else:
            df = old
    else:
        print(
            f"캐시 없음: 전 구간 {start} ~ {end} 를 네트워크에서 받습니다.",
            flush=True,
        )
        tasks = [(c, n, start, end) for c, n in zip(codes, names)]
        chunks = _download_ohlcv_tasks(
            tasks,
            max_workers,
            deadline,
            desc="가격 다운로드",
        )
        if not chunks:
            raise RuntimeError("가격 데이터를 가져오지 못했습니다. 네트워크/기간을 확인하세요.")
        df = pd.concat(chunks, ignore_index=True)
        df = _normalize_ohlcv_df(df)

    print(
        f"디스크: Parquet 저장 중... ({len(df):,}행 -> {cache_file.name})",
        flush=True,
    )
    df.to_parquet(cache_file, index=False)
    print(f"OHLCV 캐시 저장: {cache_file.resolve()}", flush=True)
    m = (df["Date"] >= pd.Timestamp(start)) & (df["Date"] <= pd.Timestamp(end))
    return df.loc[m].copy()


def daily_returns_table(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    종목·일자별 일간 수익률 ``return_pct``(소수, 예: 0.2 = 20%)를 붙인 표를 만듭니다.

    기본은 전일 종가 대비 당일 종가입니다.
    직전 거래일이 거래정지(거래량 0)였다가 재개된 날은 시가·종가로 1차 계산한 뒤,
    거래소 ``Change``(전일 종가 대비 등락률)가 있으면 **그 값을 우선**합니다.
    (재개일 상한가는 시가대비가 아니라 전일가대비 29%대인 경우가 많음 — 서산 등)
    단, ``Change`` 가 없을 때만 시가 기준 폴백을 씁니다.
    """
    df = ohlcv.sort_values(["Code", "Date"])
    g = df.groupby("Code", group_keys=False)
    df = df.copy()
    vol = (
        pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0)
        if "Volume" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    opn = pd.to_numeric(df["Open"], errors="coerce") if "Open" in df.columns else pd.Series(np.nan, index=df.index)
    cls = pd.to_numeric(df["Close"], errors="coerce")
    prev_close = g["Close"].shift(1)
    prev_vol = g["Volume"].shift(1) if "Volume" in df.columns else pd.Series(0.0, index=df.index)
    prev_vol = pd.to_numeric(prev_vol, errors="coerce").fillna(0.0)

    # 거래정지 후 재개일: pykrx 캐시는 정지 중 종가(38900)를 유지해 +41% 오류가 남.
    # 네이버/KRX 전일가=시가인 경우가 많아 시가 대비 종가로 맞춤.
    halt_resume = (vol > 0) & (prev_vol <= 0) & opn.notna() & (opn > 0)
    ref_close = prev_close.where(~halt_resume, opn)
    df["prev_close"] = ref_close
    df["return_pct"] = (cls / ref_close) - 1.0
    if "Change" in df.columns:
        ch = pd.to_numeric(df["Change"], errors="coerce")
        trust_change = ch.notna() & (vol > 0)
        # 거래정지 재개일: pykrx Change는 정지 중 종가(예: 391) 기준이라 +169% 등 오류 → 시가 기준 유지
        trust_change = trust_change & ~halt_resume
        df.loc[trust_change, "return_pct"] = ch.loc[trust_change]
    return df


def merge_returns_pct_into_krx_map(
    krx_map: dict[str, float],
    returns_df: pd.DataFrame,
    d: date,
    *,
    min_gap_pp: float = 5.0,
) -> dict[str, float]:
    """
    pykrx 전종목 등락률이 거래정지 재개일 등에서 시가대비로만 나올 때 보정합니다.

    ``daily_returns_table`` 의 ``Change``(전일가대비)가 pykrx 값보다 ``min_gap_pp`` 이상
  크면 후자로 덮어씁니다(상한가·20%↑ 누락 방지).
    """
    if not krx_map or returns_df is None or returns_df.empty:
        return krx_map
    from_returns = change_pct_by_code_from_returns(returns_df, d)
    if not from_returns:
        return krx_map
    out = dict(krx_map)
    for code, rp_ret in from_returns.items():
        krx_v = out.get(code)
        if krx_v is None:
            if abs(float(rp_ret)) >= 10.0:
                out[code] = float(rp_ret)
            continue
        gap = float(rp_ret) - float(krx_v)
        if gap > float(min_gap_pp):
            out[code] = float(rp_ret)
    return out


def enrich_daily_returns_for_ml(returns_df: pd.DataFrame) -> pd.DataFrame:
    """
    ML 랭커용 시세 피처를 ``daily_returns_table`` 결과에 붙입니다.

    각 (종목, 거래일) 행에 대해 **당일 장 시작 시점**까지 알 수 있는 값만 사용합니다.

    - ``ret_lag1``: 직전 영업일 종가 기준 일간 수익률
    - ``log_vol_lag1``: 직전 영업일 거래량 ``log1p``
    - ``ret_roll_std5``: 직전 영업일까지 5영업일 수익률 표준편차
    - ``log_vol_roll_mean5``: 직전 영업일까지 5영업일 ``log1p(Volume)`` 평균
    - ``close_ma20_ratio``: 직전 종가가 20일 이평(직전일까지) 대비 얼마나 떨어져 있는지 ``(C-MA)/MA``
    - ``ret_roll_mean5``: 직전 5영업일 수익률 평균(단기 모멘텀)
    - ``vol_surge_ratio``: 직전 거래량 log 대비 5일 평균 log 차이(거래량 급증)
    """
    df = returns_df.sort_values(["Code", "Date"]).copy()
    if "Volume" not in df.columns:
        df["Volume"] = 0.0
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0.0).clip(lower=0.0)
    if "Close" not in df.columns:
        df["Close"] = np.nan
    df["Close"] = pd.to_numeric(df["Close"], errors="coerce")

    g = df.groupby("Code", group_keys=False)
    df["ret_lag1"] = g["return_pct"].shift(1)
    df["log_vol_lag1"] = np.log1p(g["Volume"].shift(1).fillna(0.0))
    df["ret_roll_std5"] = g["return_pct"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=1).std()
    )
    df["log_vol_roll_mean5"] = g["Volume"].transform(
        lambda s: np.log1p(s).shift(1).rolling(5, min_periods=1).mean()
    )
    ma20 = g["Close"].transform(lambda s: s.shift(1).rolling(20, min_periods=1).mean())
    prev_c = g["Close"].shift(1)
    df["close_ma20_ratio"] = (prev_c - ma20) / ma20.replace(0, np.nan)
    df["ret_roll_mean5"] = g["return_pct"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=1).mean()
    )
    df["vol_surge_ratio"] = df["log_vol_lag1"] - df["log_vol_roll_mean5"]

    for c in ("ret_lag1", "ret_roll_std5", "close_ma20_ratio", "ret_roll_mean5"):
        df[c] = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["log_vol_lag1"] = pd.to_numeric(df["log_vol_lag1"], errors="coerce").fillna(0.0)
    df["log_vol_roll_mean5"] = pd.to_numeric(df["log_vol_roll_mean5"], errors="coerce").fillna(0.0)
    df["close_ma20_ratio"] = df["close_ma20_ratio"].clip(-1.0, 1.0)
    df["ret_roll_std5"] = df["ret_roll_std5"].clip(0.0, 0.6)
    df["ret_roll_mean5"] = df["ret_roll_mean5"].clip(-0.25, 0.25)
    df["vol_surge_ratio"] = pd.to_numeric(df["vol_surge_ratio"], errors="coerce").fillna(0.0)
    df["vol_surge_ratio"] = df["vol_surge_ratio"].clip(-4.0, 4.0)
    return df


def returns_by_code_index(
    returns_df: pd.DataFrame,
    returns_ml: pd.DataFrame | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    종목코드(6자리) → 해당 종목만 담은 일봉·ML 시세 DataFrame.

    리포트 ``move_reference`` 등에서 전체 OHLCV(수십만 행)를 행마다 다시 거르지 않도록 합니다.
    """
    by_ret: dict[str, pd.DataFrame] = {}
    by_ml: dict[str, pd.DataFrame] = {}
    if returns_df is None or returns_df.empty:
        return by_ret, by_ml
    r = returns_df.copy()
    r["Code"] = r["Code"].astype(str).str.zfill(6)
    for code, sub in r.groupby("Code", sort=False):
        by_ret[str(code)] = sub.sort_values("Date")
    if returns_ml is not None and not returns_ml.empty:
        m = returns_ml.copy()
        m["Code"] = m["Code"].astype(str).str.zfill(6)
        for code, sub in m.groupby("Code", sort=False):
            by_ml[str(code)] = sub.sort_values("Date")
    return by_ret, by_ml


def change_pct_by_code_from_returns(returns_df: pd.DataFrame, d: date) -> dict[str, float]:
    """
    ``daily_returns_table`` 결과에서 거래일 ``d`` 의 종목별 일간 수익률을 **퍼센트 포인트**로 돌려줍니다.

    pykrx ``get_market_ohlcv_by_ticker`` 가 KRX 스크래핑 실패할 때, 동일 의미의 전종목 맵으로 씁니다.
    (``return_pct`` 가 소수 0.2 → ``20.0``.)
    """
    if returns_df is None or returns_df.empty or "return_pct" not in returns_df.columns:
        return {}
    ts = pd.Timestamp(d)
    m = returns_df["Date"] == ts
    sub = returns_df.loc[m, ["Code", "return_pct"]]
    if sub.empty:
        return {}
    out: dict[str, float] = {}
    for _, row in sub.iterrows():
        code = str(row["Code"]).zfill(6)
        rp = row["return_pct"]
        if pd.isna(rp):
            continue
        v = float(rp) * 100.0
        if math.isfinite(v):
            out[code] = v
    return out


def big_movers_on_date(
    returns_df: pd.DataFrame,
    d: date,
    threshold: float = config.BIG_MOVE_THRESHOLD,
) -> pd.DataFrame:
    """
    ``returns_df`` 안에서 주어진 거래일 ``d`` 에 ``threshold`` 이상 상승한 종목만 추립니다.

    pykrx 전종목 등락률을 쓸 수 없을 때 「실제 급등」 폴백으로 사용됩니다(데이터에 있는 종목만).
    """
    ts = pd.Timestamp(d)
    m = returns_df["Date"] == ts
    sub = returns_df.loc[m & (returns_df["return_pct"] >= threshold)]
    return sub[["Date", "Code", "Name", "return_pct", "Volume"]].sort_values(
        "return_pct", ascending=False
    )


def try_krx_change_pct_by_code(
    d: date,
    *,
    returns_df: pd.DataFrame | None = None,
) -> dict[str, float] | None:
    """
    pykrx로 KOSPI·KOSDAQ 전 종목의 당일 **등락률**을 종목코드(6자리) → 퍼센트 포인트로 조회합니다.

    예: 실제 20% 상승이면 값 ``20.0`` 근처. 리포트의 「실제 20%↑」와 OHLCV 표본 한계를 완화합니다.

    pykrx 전종목 스크래핑이 실패하는 경우( KRX 페이지 변경 등 ), ``returns_df`` 가 있으면
    그날의 OHLCV 기반 ``return_pct`` 로 동일 형식의 맵을 돌려 **조회 실패를 완화**합니다.

    Returns:
        비어 있지 않은 맵이면 그대로 반환. 데이터가 전혀 없으면 ``None``,
        스크래핑은 됐으나 등락률만 비면 ``{}`` (호출부에서 OHLCV 폴백 구분용).
    """
    try:
        from pykrx import stock
    except ImportError:
        return None

    ds = d.strftime("%Y%m%d")
    by_code: dict[str, float] = {}

    # 1) 우선 pykrx의 "등락률" 컬럼을 그대로 사용(장 마감 후/소스 제공 시 가장 직접적).
    got_any_frame = False
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_ohlcv_by_ticker(ds, market=mkt, alternative=True)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        got_any_frame = True
        if "등락률" not in df.columns:
            continue
        chg = pd.to_numeric(df["등락률"], errors="coerce")
        for code_raw, pct in chg.items():
            if pd.isna(pct):
                continue
            by_code[str(code_raw).zfill(6)] = float(pct)

    if by_code:
        if returns_df is not None:
            by_code = merge_returns_pct_into_krx_map(by_code, returns_df, d)
        return by_code

    # 2) 장중 등락률이 비어 있을 수 있어, 전일종가 대비 현재(당일 종가 컬럼)로 직접 계산.
    try:
        from . import trading_calendar

        prev_d = trading_calendar.last_trading_day_before(d)
    except Exception:
        prev_d = d - timedelta(days=1)
    prev_ds = prev_d.strftime("%Y%m%d")

    any_today = False
    any_prev = False
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            df_today = stock.get_market_ohlcv_by_ticker(ds, market=mkt, alternative=True)
            df_prev = stock.get_market_ohlcv_by_ticker(prev_ds, market=mkt, alternative=True)
        except Exception:
            continue
        if (
            df_today is None
            or df_prev is None
            or df_today.empty
            or df_prev.empty
            or "종가" not in df_today.columns
            or "종가" not in df_prev.columns
        ):
            continue
        any_today = any_today or bool(not df_today.empty)
        any_prev = any_prev or bool(not df_prev.empty)
        close_today = pd.to_numeric(df_today["종가"], errors="coerce")
        close_prev = pd.to_numeric(df_prev["종가"], errors="coerce")
        idx_common = close_today.index.intersection(close_prev.index)
        if len(idx_common) == 0:
            continue
        base = close_prev.loc[idx_common].replace(0, np.nan)
        pct = ((close_today.loc[idx_common] / base) - 1.0) * 100.0
        for code_raw, v in pct.items():
            if pd.isna(v):
                continue
            by_code[str(code_raw).zfill(6)] = float(v)

    if by_code:
        if returns_df is not None:
            by_code = merge_returns_pct_into_krx_map(by_code, returns_df, d)
        return by_code
    if returns_df is not None and not returns_df.empty:
        fb = change_pct_by_code_from_returns(returns_df, d)
        if fb:
            return fb
    if got_any_frame or any_today or any_prev:
        return {}
    return None


def try_krx_change_pct_for_codes_direct(
    d: date,
    codes: list[str],
) -> dict[str, float]:
    """
    장중 bulk 등락률이 비는 경우, 종목별 pykrx 조회로 등락률(%)을 직접 계산합니다.

    반환값은 6자리 코드 -> 퍼센트 포인트.
    """
    if not codes:
        return {}
    try:
        from pykrx import stock
    except ImportError:
        return {}

    try:
        from . import trading_calendar

        prev_d = trading_calendar.last_trading_day_before(d)
    except Exception:
        prev_d = d - timedelta(days=1)
    ds = d.strftime("%Y%m%d")
    prev_ds = prev_d.strftime("%Y%m%d")

    out: dict[str, float] = {}
    uniq_codes = sorted({str(c).zfill(6) for c in codes if str(c).strip()})
    for code in uniq_codes:
        try:
            df = stock.get_market_ohlcv_by_date(prev_ds, ds, code, adjusted=False)
        except Exception:
            continue
        if df is None or df.empty or "종가" not in df.columns:
            continue
        close = pd.to_numeric(df["종가"], errors="coerce").dropna()
        if len(close) < 2:
            continue
        prev_close = float(close.iloc[-2])
        now_close = float(close.iloc[-1])
        if prev_close == 0.0:
            continue
        out[code] = ((now_close / prev_close) - 1.0) * 100.0
    return out


_NAVER_POLL_CHUNK = 80


def try_naver_realtime_fluctuations_pct_by_codes(codes: list[str]) -> dict[str, float]:
    """
    네이버 금융 실시간 polling API로 종목별 등락률(퍼센트 포인트)을 조회합니다.

    ``pykrx`` 전종목·개별 조회가 비거나 누락될 때 장중 ``— (xx%)`` 보조용으로 사용합니다.
    요청은 코드 목록을 쪼개 여러 번 보냅니다(한 URL에 다수 코드, 콤마 구분).
    """
    uniq = sorted({str(c).zfill(6) for c in codes if str(c).strip()})
    if not uniq:
        return {}
    out: dict[str, float] = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
    }
    base = "https://polling.finance.naver.com/api/realtime/domestic/stock/"
    for i in range(0, len(uniq), _NAVER_POLL_CHUNK):
        chunk = uniq[i : i + _NAVER_POLL_CHUNK]
        url = base + ",".join(chunk)
        try:
            resp = requests.get(url, headers=headers, timeout=12)
            resp.raise_for_status()
            obj = resp.json()
        except Exception:
            continue
        datas = obj.get("datas")
        if not isinstance(datas, list):
            continue
        for it in datas:
            if not isinstance(it, dict):
                continue
            code = str(it.get("itemCode") or it.get("symbolCode") or "").zfill(6)
            if len(code) != 6 or not code.isdigit():
                continue
            raw = it.get("fluctuationsRatioRaw")
            if raw is None:
                raw = it.get("fluctuationsRatio")
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(v):
                out[code] = v
    return out


def best_effort_intraday_pct_by_code(
    trading_day: date,
    codes: list[str],
    *,
    returns_df: pd.DataFrame | None = None,
    krx_bulk_attempts: int = 3,
    krx_bulk_sleep_sec: float = 0.65,
) -> dict[str, float]:
    """
    장중·당일 봉 미확정 시점에 표시할 등락률(퍼센트 포인트)을 종목별로 최대한 채웁니다.

    1) pykrx 전종목 스냅샷(짧게 재시도, ``returns_df`` 있으면 동일 실패 시 OHLCV 맵 폴백)
    → 2) pykrx 일별 OHLCV로 종목별 계산
    → 3) 네이버 실시간 polling(묶음) → 4) 아직 비는 코드만 단건 네이버 재시도.
    """
    uniq = sorted({str(c).zfill(6) for c in codes if str(c).strip()})
    if not uniq:
        return {}
    merged: dict[str, float] = {}
    for _ in range(max(1, int(krx_bulk_attempts))):
        bulk = try_krx_change_pct_by_code(trading_day, returns_df=returns_df)
        if bulk:
            for c in uniq:
                if c in bulk and math.isfinite(float(bulk[c])):
                    merged[c] = float(bulk[c])
            break
        if krx_bulk_attempts > 1:
            time.sleep(max(0.0, float(krx_bulk_sleep_sec)))
    need_direct = [c for c in uniq if c not in merged or not math.isfinite(float(merged.get(c, float("nan"))))]
    if need_direct:
        merged.update(try_krx_change_pct_for_codes_direct(trading_day, need_direct))
    missing = [c for c in uniq if c not in merged or not math.isfinite(float(merged.get(c, float("nan"))))]
    if missing:
        naver_map = try_naver_realtime_fluctuations_pct_by_codes(missing)
        for c, p in naver_map.items():
            if math.isfinite(float(p)):
                merged[str(c).zfill(6)] = float(p)
    missing2 = [c for c in uniq if c not in merged or not math.isfinite(float(merged.get(c, float("nan"))))]
    for c in missing2:
        one = try_naver_realtime_fluctuations_pct_by_codes([c])
        if c in one and math.isfinite(float(one[c])):
            merged[c] = float(one[c])
    return merged


def big_movers_from_krx_pct_map(
    pct_by_code: dict[str, float],
    threshold: float,
    listing_names: dict[str, str],
    *,
    direction: str = "up",
) -> list[dict]:
    """
    ``try_krx_change_pct_by_code`` 결과 맵에서 ``threshold``(소수, 예 ``0.2`` = 20%) 이상인 종목만 골라
    ``ret_pct``(퍼센트 포인트) 내림차순 리스트로 만듭니다.

    각 원소는 ``code``, ``name``, ``ret_pct`` 키를 가집니다.
    """
    # ``threshold`` 는 소수(0.1=10%). pykrx 등락률은 퍼센트 포인트(15=15%).
    # 하락 쪽에 음수 threshold(-0.1)를 넘겨도 크기만 씁니다(예: -thr_pct 로 -10% 이하).
    mag_pct = abs(float(threshold)) * 100.0
    direction = (direction or "up").strip().lower()
    if direction not in ("up", "down"):
        direction = "up"
    if direction == "up":
        rows = [
            {"code": c, "name": listing_names.get(c, c), "ret_pct": pct}
            for c, pct in pct_by_code.items()
            if pct is not None and math.isfinite(float(pct)) and float(pct) >= mag_pct
        ]
        rows.sort(key=lambda r: -r["ret_pct"])
        return rows
    rows = [
        {"code": c, "name": listing_names.get(c, c), "ret_pct": pct}
        for c, pct in pct_by_code.items()
        if pct is not None and math.isfinite(float(pct)) and float(pct) <= -mag_pct
    ]
    rows.sort(key=lambda r: r["ret_pct"])
    return rows


def actual_return_on_date(returns_df: pd.DataFrame, code: str, d: date) -> float | None:
    """
    ``returns_df`` 에서 종목 ``code`` 의 거래일 ``d`` 일간 수익률(소수)을 반환합니다.

    해당 행이 없으면 ``None`` (신규 상장·거래정지·데이터 구멍 등).
    """
    ts = pd.Timestamp(d)
    row = returns_df.loc[(returns_df["Code"] == code) & (returns_df["Date"] == ts)]
    if row.empty:
        return None
    return float(row.iloc[0]["return_pct"])


# --- market index (merged from market_index.py) ---

def load_index_frame(symbol: str, start: date, end: date) -> pd.DataFrame:
    """
    FinanceDataReader로 지수 심볼(예: ``KS11``) 일봉을 읽고 ``Date`` 를 정규화합니다.

    실패·빈 데이터면 빈 DataFrame.
    """
    try:
        df = fdr.DataReader(symbol, start, end)
    except (OSError, ValueError, TypeError, KeyError):
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    date_col: str | None = None
    for c in ("Date", "date", "index", "Index", "datetime"):
        if c in df.columns:
            date_col = c
            break
    if date_col is None and len(df.columns) > 0:
        first = df.columns[0]
        if pd.api.types.is_datetime64_any_dtype(df[first]):
            date_col = str(first)
    if date_col is None:
        return pd.DataFrame()
    if date_col != "Date":
        df = df.rename(columns={date_col: "Date"})
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()
    return df


def index_daily_return_pct(df: pd.DataFrame, d: date) -> float | None:
    """
    일봉 ``df`` 에서 날짜 ``d`` 의 전일 대비 종가 수익률을 소수로 반환합니다.

    전일 행이 없으면 ``None`` (첫 거래일 등).
    """
    if df.empty or "Close" not in df.columns:
        return None
    ts = pd.Timestamp(d)
    s = df.sort_values("Date").reset_index(drop=True)
    hit = s.index[s["Date"] == ts]
    if len(hit) == 0:
        return None
    i = int(hit[0])
    if i == 0:
        return None
    prev = float(s.loc[i - 1, "Close"])
    cl = float(s.loc[i, "Close"])
    if prev == 0:
        return None
    return (cl / prev) - 1.0

# --- listing sector (merged from stock_listing_sector.py) ---

_CACHE_DIR = config.LISTING_META_CACHE_DIR
_CODE_TO_INDUSTRY = _CACHE_DIR / "stock_industry_code.json"
_INDUSTRY_NAMES = _CACHE_DIR / "industry_name_by_code.json"
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Money/1.0)"}
_FETCH_SLEEP = 0.12

# 네이버 업종명 → 리포트 짧은 라벨
_INDUSTRY_LABEL: tuple[tuple[str, str], ...] = (
    ("자동차부품", "자동차부품"),
    ("운송장비", "운송장비"),
    ("반도체", "반도체"),
    ("반도체와 반도체장비", "반도체"),
    ("전기전자", "전자"),
    ("전기·전자", "전자"),
    ("화학", "화학"),
    ("제약", "제약"),
    ("바이오", "바이오"),
    ("의료정밀", "의료기기"),
    ("기계", "기계"),
    ("조선", "조선"),
    ("철강", "철강"),
    ("건설", "건설"),
    ("은행", "금융"),
    ("증권", "금융"),
    ("보험", "금융"),
    ("소프트웨어", "소프트웨어"),
    ("게임", "게임"),
    ("통신", "통신"),
    ("유통", "유통"),
    ("음식료", "음식료"),
    ("섬유", "섬유"),
    ("운송", "운송"),
    ("에너지", "에너지"),
    ("2차전지", "2차전지"),
    ("배터리", "2차전지"),
)


def _ensure_dir() -> None:
    """업종 메타 캐시 디렉터리 생성."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default: dict) -> dict:
    """JSON 파일 읽기(실패 시 ``default`` 복사본)."""
    if not path.is_file():
        return dict(default)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else dict(default)
    except (OSError, json.JSONDecodeError, TypeError):
        return dict(default)


def _write_json(path: Path, data: dict) -> None:
    """업종 메타 JSON 저장."""
    _ensure_dir()
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _fetch_industry_code(code: str) -> str:
    """네이버 integration API → ``industryCode``."""
    c = str(code).zfill(6)
    url = f"https://m.stock.naver.com/api/stock/{c}/integration"
    r = requests.get(url, headers=_HEADERS, timeout=14)
    r.raise_for_status()
    payload = r.json()
    return str(payload.get("industryCode") or "").strip()


def _fetch_industry_name(industry_code: str) -> str:
    """업종 상세 페이지 제목에서 업종명 파싱."""
    ic = str(industry_code).strip()
    if not ic:
        return ""
    url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={ic}"
    r = requests.get(url, headers=_HEADERS, timeout=14)
    r.encoding = "euc-kr"
    m = re.search(r"<title>\s*([^<:]+)", r.text)
    return m.group(1).strip() if m else ""


def industry_code_for_stock(code: str, *, refresh: bool = False) -> str:
    """종목코드 → 네이버 업종코드(캐시)."""
    c = str(code).zfill(6)
    mapping = _read_json(_CODE_TO_INDUSTRY, {})
    if not refresh and c in mapping:
        return str(mapping[c] or "")
    try:
        ic = _fetch_industry_code(c)
    except (requests.RequestException, ValueError, TypeError):
        return str(mapping.get(c, "") or "")
    mapping[c] = ic
    _write_json(_CODE_TO_INDUSTRY, mapping)
    time.sleep(_FETCH_SLEEP)
    return ic


def industry_name_for_code(code: str, *, refresh: bool = False) -> str:
    """종목코드 → 업종명(예: 자동차부품)."""
    ic = industry_code_for_stock(code, refresh=refresh)
    if not ic:
        return ""
    names = _read_json(_INDUSTRY_NAMES, {})
    if not refresh and ic in names and str(names[ic]).strip():
        return str(names[ic])
    try:
        name = _fetch_industry_name(ic)
    except (requests.RequestException, ValueError, TypeError):
        name = str(names.get(ic, "") or "")
    if name:
        names[ic] = name
        _write_json(_INDUSTRY_NAMES, names)
        time.sleep(_FETCH_SLEEP)
    return str(name or "")


def compact_industry_label(raw: str) -> str:
    """업종명을 리포트 pill용 짧은 라벨로."""
    s = str(raw or "").strip()
    if not s:
        return ""
    for key, label in _INDUSTRY_LABEL:
        if key in s:
            return label
    return s.replace(" 관련주", "").strip()


def compact_label_for_code(code: str, *, name: str = "") -> str:
    """종목 업종 짧은 라벨. 실패 시 빈 문자열."""
    lab = compact_industry_label(industry_name_for_code(code))
    return lab


def prefetch_industry_codes(codes: list[str]) -> None:
    """여러 종목 업종코드·업종명을 미리 캐시(리포트·학습 전 호출)."""
    seen_ic: set[str] = set()
    for raw in codes:
        c = str(raw).zfill(6)
        ic = industry_code_for_stock(c)
        if ic and ic not in seen_ic:
            industry_name_for_code(c)
            seen_ic.add(ic)


@lru_cache(maxsize=1)
def _code_to_industry_map() -> dict[str, str]:
    """종목→업종코드 캐시 전체(프로세스 내 1회 로드)."""
    return _read_json(_CODE_TO_INDUSTRY, {})


def peers_for_industry(industry_code: str) -> list[str]:
    """동일 업종코드 종목코드 목록(캐시 기준)."""
    ic = str(industry_code).strip()
    if not ic:
        return []
    m = _code_to_industry_map()
    return [c for c, v in m.items() if str(v) == ic]


def industry_momentum_for_code(
    code: str,
    returns_ml: pd.DataFrame,
    target_day: date,
) -> float:
    """
    동일 업종 전일 평균 수익률(``ret_lag1``) → 0~1 정규화.

    ML·하이브리드 랭킹 보조 피처.
    """
    ic = industry_code_for_stock(code)
    if not ic:
        return 0.0
    peers = peers_for_industry(ic)
    if not peers:
        return 0.0
    peer_set = {str(p).zfill(6) for p in peers}
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    if sl.empty:
        return 0.0
    rets: list[float] = []
    for _, row in sl.iterrows():
        c = str(row["Code"]).zfill(6)
        if c not in peer_set:
            continue
        try:
            v = float(row.get("ret_lag1") or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        if v == v:
            rets.append(v)
    if not rets:
        return 0.0
    avg = sum(rets) / len(rets)
    return max(0.0, min(1.0, avg / 0.12))


def industry_theme_overlap(code: str, kw_blob: str | frozenset[str]) -> float:
    """당일 뉴스 키워드와 업종명 겹침(0~1)."""
    ind = industry_name_for_code(code).lower()
    if not ind or len(ind) < 2:
        return 0.0
    if isinstance(kw_blob, frozenset):
        kws = [str(k).lower() for k in kw_blob if str(k).strip()]
    else:
        kws = [str(k).lower() for k in str(kw_blob).split() if k.strip()]
    best = 0.0
    for kw in kws:
        if len(kw) < 2:
            continue
        if kw in ind or ind in kw:
            best = max(best, 1.0)
        elif any(tok in ind for tok in (kw[:4], kw[:3]) if len(tok) >= 2):
            best = max(best, 0.55)
    return best

# --- investor_flow (from investor_flow.py) ---

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