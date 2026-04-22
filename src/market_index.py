"""
시장 지수(KS11 등) 일봉 로드·당일 수익률(코멘트용).

리포트의 예측–실제 갭 설명에 KOSPI 당일 등락 힌트를 붙이는 데 쓰입니다.
"""
from __future__ import annotations

from datetime import date

import FinanceDataReader as fdr
import pandas as pd


def load_index_frame(symbol: str, start: date, end: date) -> pd.DataFrame:
    """
    FinanceDataReader로 지수 심볼(예: ``KS11``) 일봉을 읽고 ``Date`` 를 정규화합니다.

    실패·빈 데이터면 빈 DataFrame.
    """
    df = fdr.DataReader(symbol, start, end)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
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
