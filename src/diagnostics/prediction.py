"""예측·Hit@K 오프라인 진단 공통 로더."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd

from .. import config, news, stocks
from ..learning import support as train_snapshot
from ..prediction import ml_move_rank, predict, prediction_ranking
from ..prediction.candidate_pool import day_candidate_codes


@dataclass
class PredictionEvalContext:
    """단일 관측일 T 예측 평가에 필요한 데이터 묶음."""

    target_day: date
    returns_ml: pd.DataFrame
    listing_codes: list[str]
    listing_names: dict[str, str]
    train_events: list
    news_by_calendar: dict[date, list[dict[str, str]]]
    news_text_blob: str
    fp: str

    def actual_big_movers(self, threshold: float | None = None) -> pd.DataFrame:
        thr = float(config.BIG_MOVE_THRESHOLD if threshold is None else threshold)
        sl = self.returns_ml[self.returns_ml["Date"] == pd.Timestamp(self.target_day)]
        return sl[sl["return_pct"] >= thr].copy()

    def candidate_codes(self) -> list[str]:
        ctx = predict.build_scoring_context(self.news_text_blob, self.train_events)
        return day_candidate_codes(
            self.listing_codes,
            self.listing_names,
            self.news_text_blob,
            ctx,
            self.returns_ml,
            self.target_day,
        )

    def fit_ml_bundle(self, *, force_retrain: bool = False) -> dict[str, Any] | None:
        return ml_move_rank.fit_or_load_classifier(
            train_events=self.train_events,
            returns_ml=self.returns_ml,
            news_by_calendar=self.news_by_calendar,
            listing_names=self.listing_names,
            fp=self.fp,
            label_before_exclusive=self.target_day,
            force_retrain=force_retrain,
        )

    def theme_weights(self) -> dict[str, float]:
        from ..learning import support as theme_support

        return theme_support.weights_for_observation_day(
            self.target_day,
            train_events=self.train_events,
            news_by_calendar=self.news_by_calendar,
            returns_df=self.returns_ml,
            threshold=config.BIG_MOVE_THRESHOLD,
        )

    def rank_predictions(
        self,
        *,
        top_n: int | None = None,
        ml_bundle: dict[str, Any] | None = None,
        feedback_ctx: dict[str, object] | None = None,
    ) -> list[predict.PredictionRow]:
        bundle = ml_bundle if ml_bundle is not None else self.fit_ml_bundle()
        if bundle is None or bundle.get("pipeline") is None:
            return []
        n = int(top_n or config.PRED_RANK_POOL_N)
        eval_k = max(config.PRED_EVAL_HIT_AT_K) if config.PRED_EVAL_HIT_AT_K else 40
        n = max(n, eval_k)
        if feedback_ctx is None:
            from ..prediction import feedback_loop

            feedback_ctx = feedback_loop.build_enriched_feedback_context(
                as_of=self.target_day
            )
        return ml_move_rank.rank_predictions_ml(
            target_day=self.target_day,
            listing_codes=self.listing_codes,
            listing_names=self.listing_names,
            train_events=self.train_events,
            news_text_blob=self.news_text_blob,
            returns_ml=self.returns_ml,
            pipeline=bundle["pipeline"],
            theme_weights=self.theme_weights(),
            ml_bundle=bundle,
            feedback_ctx=feedback_ctx,
            top_n=n,
        )

    def hit_at_k_metrics(
        self,
        preds: list[predict.PredictionRow],
        *,
        threshold: float | None = None,
    ) -> dict[str, float | int]:
        big = self.actual_big_movers(threshold=threshold)
        actual = {str(r["Code"]).zfill(6) for _, r in big.iterrows()}
        ranked = [str(p.code).zfill(6) for p in preds]
        return prediction_ranking.compute_hit_at_k_metrics(
            ranked, actual, universe_size=len(self.listing_codes)
        )


def load_news_by_calendar(
    start: date,
    end: date,
) -> dict[date, list[dict[str, str]]]:
    """캘린더 구간 뉴스 JSON 로드."""
    out: dict[date, list[dict[str, str]]] = {}
    d = start
    while d <= end:
        p = news.day_news_json_path(d)
        if p.is_file():
            out[d] = json.loads(p.read_text(encoding="utf-8"))
        d += timedelta(days=1)
    return out


def load_prediction_eval_context(
    target_day: date,
    *,
    news_start: date | None = None,
    ohlcv_path: str | None = None,
) -> PredictionEvalContext:
    """캐시 OHLCV·뉴스·스냅샷으로 ``PredictionEvalContext`` 를 구성합니다."""
    path = ohlcv_path or str(config.CACHE_DIR / "ohlcv_long_full.parquet")
    ohlcv = pd.read_parquet(path)
    returns_ml = stocks.enrich_daily_returns_for_ml(stocks.daily_returns_table(ohlcv))
    listing = stocks.load_listing()
    codes = [
        c
        for c in listing["Code"].astype(str).str.zfill(6).tolist()
        if stocks.is_common_equity_code(c)
    ]
    names = {
        str(r["Code"]).zfill(6): str(r["Name"])
        for _, r in listing.iterrows()
        if stocks.is_common_equity_code(str(r["Code"]).zfill(6))
    }

    ns = news_start or date(target_day.year, max(1, target_day.month - 1), 1)
    news_by = load_news_by_calendar(ns, target_day)
    blob, _ = news.aggregate_early_late_for_target(news_by, target_day)

    snap = train_snapshot.load_snapshot(config.TRAIN_SNAPSHOT_PATH)
    events = snap.events if snap else []
    fp = train_snapshot.fingerprint() if snap else "none"

    return PredictionEvalContext(
        target_day=target_day,
        returns_ml=returns_ml,
        listing_codes=codes,
        listing_names=names,
        train_events=events,
        news_by_calendar=news_by,
        news_text_blob=blob,
        fp=fp,
    )
