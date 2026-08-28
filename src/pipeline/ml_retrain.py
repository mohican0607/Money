"""관측일 T용 ML joblib 만 강제 재학습(파이프라인·리포트 없음)."""
from __future__ import annotations

import time
from datetime import date, timedelta
from typing import Any

import pandas as pd

from src import config, stocks, train_snapshot, trading_calendar
from src.diagnostics.prediction import load_news_by_calendar
from src.learning.support import filter_train_events_before


def forward_ml_retrain_t(*, n_day: date) -> date:
    """기준일 N의 14:30·15:30 예측과 동일한 관측일 T (= N 다음 거래일)."""
    return trading_calendar.next_trading_day_after(n_day)


def force_ml_retrain_for_observation_day(t_day: date) -> dict[str, Any] | None:
    """
    스냅샷 ``train_events``·``rebuild_learning`` 을 반영해 관측일 T용 ML joblib 만 재학습합니다.

  ``label_before_exclusive=T`` 워크포워드 컷오프와 동일합니다.
    """
    from src import ml_move_rank

    if not config.PRED_USE_ML_RANKER:
        print("ML 랭커 비활성(PRED_USE_ML_RANKER=0) — 재학습 생략.", flush=True)
        return None

    snap = train_snapshot.load_snapshot(config.TRAIN_SNAPSHOT_PATH)
    if snap is None or not snap.events:
        print(
            f"학습 스냅샷 없음 또는 이벤트 비어 있음 — "
            f"T={t_day.isoformat()} ML 재학습 생략.",
            flush=True,
        )
        return None

    train_start = config.TRAIN_START_DEFAULT
    train_events_t = filter_train_events_before(
        snap.events, t_day, train_start=train_start
    )
    if not train_events_t:
        print(
            f"T={t_day.isoformat()} 직전 학습 이벤트 없음 — ML 재학습 생략.",
            flush=True,
        )
        return None

    ohlcv_path = config.CACHE_DIR / "ohlcv_long_full.parquet"
    if not ohlcv_path.is_file():
        print(f"OHLCV 캐시 없음: {ohlcv_path} — ML 재학습 생략.", flush=True)
        return None

    print(
        f"ML 강제 재학습: T={t_day.isoformat()} "
        f"(이벤트 {len(train_events_t)}건, label_before_exclusive=T)",
        flush=True,
    )
    ohlcv = pd.read_parquet(ohlcv_path)
    returns_ml = stocks.enrich_daily_returns_for_ml(stocks.daily_returns_table(ohlcv))
    listing = stocks.load_listing()
    names = {
        str(r["Code"]).zfill(6): str(r["Name"])
        for _, r in listing.iterrows()
        if stocks.is_common_equity_code(str(r["Code"]).zfill(6))
    }

    lookback = max(config.ML_TRAIN_LOOKBACK_DAYS, 30)
    news_start = t_day - timedelta(days=lookback + 14)
    if news_start < train_start:
        news_start = train_start
    news_by = load_news_by_calendar(news_start, t_day)
    print(
        f"뉴스 캐시 로드: {news_start.isoformat()} ~ {t_day.isoformat()} "
        f"({len(news_by)}일)",
        flush=True,
    )

    fp = train_snapshot.fingerprint()
    t0 = time.perf_counter()
    bundle = ml_move_rank.fit_or_load_classifier(
        train_events=train_events_t,
        returns_ml=returns_ml,
        news_by_calendar=news_by,
        listing_names=names,
        fp=fp,
        label_before_exclusive=t_day,
        force_retrain=True,
        prefer_prior_reuse=False,
    )
    elapsed = time.perf_counter() - t0
    if bundle is None:
        print("ML 재학습 실패(번들 None).", flush=True)
        return None
    print(
        f"ML 재학습 완료: T={t_day.isoformat()} "
        f"elapsed={elapsed:.1f}s version={bundle.get('version')}",
        flush=True,
    )
    return bundle
