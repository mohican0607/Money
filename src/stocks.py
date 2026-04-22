"""
FinanceDataReader 기반 KOSPI·KOSDAQ 일별 OHLCV·수익률·급등 종목 조회.

- 상장 리스트는 ``krx_listing.parquet`` 에 캐시합니다.
- 일봉은 ``ohlcv_long[_krx]_full.parquet`` 또는 ``SAMPLE_TICKERS`` 용 별도 파일에 씁니다.
- ``pykrx`` 가 있으면 당일 전종목 등락률로 「실제 20%↑」를 보강합니다.
"""
from __future__ import annotations

import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import date, timedelta
from pathlib import Path

import FinanceDataReader as fdr
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from . import config

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


def _ohlcv_calendar_gaps(start: date, end: date, dmin: date, dmax: date) -> list[tuple[date, date]]:
    """
    캐시가 [dmin, dmax], 요청이 [start, end] 일 때 API로 받아야 할 캘린더 부분 구간들.

    겹치지 않으면 전체 [start, end] 한 덩어리. 겹치면 왼쪽(요청 시작~캐시 전날)·오른쪽(캐시 다음날~요청 끝)만.
    """
    if start > end:
        return []
    if end < dmin or start > dmax:
        return [(start, end)]
    gaps: list[tuple[date, date]] = []
    if start < dmin:
        ge = min(end, dmin - timedelta(days=1))
        if start <= ge:
            gaps.append((start, ge))
    if end > dmax:
        gs = max(start, dmax + timedelta(days=1))
        if gs <= end:
            gaps.append((gs, end))
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
    단, ``Change`` 컬럼이 있으면 소스와 무관하게 이를 우선 사용합니다.
    (액면분할/기준가 보정 등으로 ``Close/prev_close`` 와 괴리될 때 거래소 등락률을 따르기 위함)
    """
    df = ohlcv.sort_values(["Code", "Date"])
    g = df.groupby("Code", group_keys=False)
    df = df.copy()
    df["prev_close"] = g["Close"].shift(1)
    df["return_pct"] = (df["Close"] / df["prev_close"]) - 1.0
    if "Change" in df.columns:
        ch = pd.to_numeric(df["Change"], errors="coerce")
        m = ch.notna()
        df.loc[m, "return_pct"] = ch.loc[m]
    return df


def enrich_daily_returns_for_ml(returns_df: pd.DataFrame) -> pd.DataFrame:
    """
    ML 랭커용 시세 피처를 ``daily_returns_table`` 결과에 붙입니다.

    각 (종목, 거래일) 행에 대해 **당일 장 시작 시점**까지 알 수 있는 값만 사용합니다.

    - ``ret_lag1``: 직전 영업일 종가 기준 일간 수익률
    - ``log_vol_lag1``: 직전 영업일 거래량 ``log1p``
    - ``ret_roll_std5``: 직전 영업일까지 5영업일 수익률 표준편차
    - ``log_vol_roll_mean5``: 직전 영업일까지 5영업일 ``log1p(Volume)`` 평균
    - ``close_ma20_ratio``: 직전 종가가 20일 이평(직전일까지) 대비 얼마나 떨어져 있는지 ``(C-MA)/MA``
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

    for c in ("ret_lag1", "ret_roll_std5", "close_ma20_ratio"):
        df[c] = pd.to_numeric(df[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["log_vol_lag1"] = pd.to_numeric(df["log_vol_lag1"], errors="coerce").fillna(0.0)
    df["log_vol_roll_mean5"] = pd.to_numeric(df["log_vol_roll_mean5"], errors="coerce").fillna(0.0)
    df["close_ma20_ratio"] = df["close_ma20_ratio"].clip(-1.0, 1.0)
    df["ret_roll_std5"] = df["ret_roll_std5"].clip(0.0, 0.6)
    return df


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
) -> list[dict]:
    """
    ``try_krx_change_pct_by_code`` 결과 맵에서 ``threshold``(소수, 예 ``0.2`` = 20%) 이상인 종목만 골라
    ``ret_pct``(퍼센트 포인트) 내림차순 리스트로 만듭니다.

    각 원소는 ``code``, ``name``, ``ret_pct`` 키를 가집니다.
    """
    thr_pct = threshold * 100.0
    rows = [
        {"code": c, "name": listing_names.get(c, c), "ret_pct": pct}
        for c, pct in pct_by_code.items()
        if pct >= thr_pct
    ]
    rows.sort(key=lambda r: -r["ret_pct"])
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
