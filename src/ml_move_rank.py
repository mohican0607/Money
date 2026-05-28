"""
훈련 구간 라벨(당일 급등 여부)로 **감독학습 랭커**를 학습해, 관측일 후보 종목을 확률 순으로 정렬합니다.

피처: 뉴스·과거 급등 이력(키워드 교집합, 종목명, 과거 평균 수익률, 이벤트 건수, 휴리스틱 점수)과
``stocks.enrich_daily_returns_for_ml`` 로 만든 **전일 수익·거래량·단기 변동성·이평 대비 위치** 등 시세 요약.
학습 행은 거래일 ``d`` 기준으로 ``d`` **이전**의 급등 이벤트만 써서 시간 누수를 줄입니다.
"""
from __future__ import annotations

import math
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import (
    config,
    news,
    predict,
    prediction_accuracy_cache,
    snapshot_miss_diagnosis,
    theme_carryover,
    trading_calendar,
    market_index,
)
from .features import BreakoutEvent, keyword_set, name_mention_score

try:
    import joblib
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    _SKLEARN_OK = True
except ImportError:  # pragma: no cover
    joblib = None  # type: ignore[misc, assignment]
    Pipeline = None  # type: ignore[misc, assignment]
    StandardScaler = None  # type: ignore[misc, assignment]
    HistGradientBoostingClassifier = None  # type: ignore[misc, assignment]
    _SKLEARN_OK = False

FEATURE_NAMES = (
    "n_hit",
    "mention",
    "sqrt_n_hit",
    "base_ret",
    "log1p_events",
    "heuristic_score",
    "mention_x_hit",
    "news_kw_count",
    "news_blob_len_log",
    "theme_kw_overlap",
    "ks11_ret_lag1",
    "ks11_ret_std5",
    "ret_lag1",
    "log_vol_lag1",
    "ret_roll_std5",
    "log_vol_roll_mean5",
    "close_ma20_ratio",
)

ML_MODEL_VERSION = 5
MAX_NEG_PER_DAY = 360
MIN_TOTAL_SAMPLES = 200
MIN_POS_SAMPLES = 25


def _early_blob_for_trading_day(
    news_by_calendar: dict[date, list[dict[str, str]]], d: date
) -> str:
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by_calendar, d)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(d)
    return predict.aggregate_news_for_window(news_by_calendar, ws, we)


def _ohlcv_lookup(returns_ml: pd.DataFrame) -> pd.DataFrame:
    """(거래일 date, 6자리 Code) → 행 조회용 인덱스."""
    t = returns_ml.copy()
    t["_d"] = pd.to_datetime(t["Date"]).dt.normalize().dt.date
    t["_c"] = t["Code"].astype(str).str.zfill(6)
    return t.set_index(["_d", "_c"])


def _ks11_market_feats(trading_day: date) -> tuple[float, float]:
    """
    시장흐름 피처(지수):
    - ks11_ret_lag1: trading_day 직전 거래일의 KOSPI 수익률
    - ks11_ret_std5: 최근 5개 값 표준편차(변동성)
    """
    try:
        prev = trading_calendar.last_trading_day_before(trading_day)
    except ValueError:
        return 0.0, 0.0
    # 충분한 과거를 확보하기 위해 넉넉히 60일 범위로 로드(캐시됨)
    start = prev - pd.Timedelta(days=90)
    end = prev + pd.Timedelta(days=2)
    df = _load_ks11_cached(start.date(), end.date())
    if df is None or df.empty:
        return 0.0, 0.0
    r1 = market_index.index_daily_return_pct(df, prev)
    if r1 is None:
        r1v = 0.0
    else:
        r1v = float(r1)
    # 최근 5개(전일 포함) 지수 수익률 표준편차
    try:
        s = df.sort_values("Date").reset_index(drop=True)
        s["ret"] = s["Close"].pct_change()
        ts = pd.Timestamp(prev)
        hit = s.index[s["Date"] == ts]
        if len(hit) == 0:
            return r1v, 0.0
        i = int(hit[0])
        w = s.loc[max(0, i - 6) : i, "ret"].dropna().astype(float).tolist()
        if len(w) < 2:
            return r1v, 0.0
        mean = sum(w) / len(w)
        var = sum((x - mean) ** 2 for x in w) / max(1.0, float(len(w) - 1))
        std = float(math.sqrt(var))
        return r1v, std
    except Exception:
        return r1v, 0.0


@lru_cache(maxsize=4)
def _load_ks11_cached(start: date, end: date) -> pd.DataFrame:
    try:
        return market_index.load_index_frame("KS11", start, end)
    except Exception:
        return pd.DataFrame()

def _price_feats_row(idx: pd.DataFrame | None, code: str, trading_day: date) -> list[float]:
    cols = (
        "ret_lag1",
        "log_vol_lag1",
        "ret_roll_std5",
        "log_vol_roll_mean5",
        "close_ma20_ratio",
    )
    if idx is None:
        return [0.0] * len(cols)
    try:
        row = idx.loc[(trading_day, code)]
    except KeyError:
        return [0.0] * len(cols)
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    out: list[float] = []
    for c in cols:
        try:
            v = float(row.get(c, 0.0) or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        if math.isnan(v) or math.isinf(v):
            v = 0.0
        out.append(v)
    return out


def _feat_vector(
    train_events: list[BreakoutEvent],
    code: str,
    name: str,
    news_blob: str,
    kw_news: frozenset[str],
    before_exclusive: date,
    *,
    ohlcv_idx: pd.DataFrame | None = None,
    theme_weights: dict[str, float] | None = None,
) -> list[float]:
    """뉴스·이력 피터 + (선택) 시세 피처. ``ohlcv_idx`` 가 있으면 ``before_exclusive`` 거래일 행을 붙입니다."""
    sub = [e for e in train_events if e.trading_day < before_exclusive]
    hist_kw: set[str] = set()
    for e in sub:
        if e.code == code:
            hist_kw |= set(e.news_keywords)
    inter = hist_kw & kw_news
    n_hit = len(inter)
    mention = name_mention_score(news_blob, name)
    score = n_hit * 1.0 + mention * 5.0
    base_ret = predict._historical_mean_return(sub, code)
    ce = sum(1 for e in sub if e.code == code)
    overlap = theme_carryover.theme_kw_overlap_score(kw_news, theme_weights)
    # 시장흐름(지수) 피처: KOSPI(KS11) 전일 수익률 및 최근 변동성
    ks11_ret_lag1, ks11_std5 = _ks11_market_feats(before_exclusive)
    base = [
        float(n_hit),
        float(mention),
        float(math.sqrt(n_hit)),
        float(base_ret),
        float(math.log1p(ce)),
        float(score),
        float(mention * math.sqrt(max(0, n_hit))),
        float(len(kw_news)),
        float(math.log1p(max(0, len(news_blob)))),
        float(overlap),
        float(ks11_ret_lag1),
        float(ks11_std5),
    ]
    return base + _price_feats_row(ohlcv_idx, code, before_exclusive)


def _build_training_arrays(
    returns_ml: pd.DataFrame,
    train_events: list[BreakoutEvent],
    news_by_calendar: dict[date, list[dict[str, str]]],
    names: dict[str, str],
    threshold: float,
    train_start: date,
    test_start: date,
    *,
    pos_boost_keys: set[tuple[str, str]] | None = None,
    neg_boost_keys: set[tuple[str, str]] | None = None,
) -> tuple[np.ndarray, np.ndarray] | None:
    rng = np.random.default_rng(42)
    rows_x: list[list[float]] = []
    rows_y: list[int] = []
    ohlcv_idx = _ohlcv_lookup(returns_ml)
    pos_boost = pos_boost_keys or set()
    neg_boost = neg_boost_keys or set()

    days = sorted(
        d for d in returns_ml["Date"].dt.date.unique() if train_start <= d < test_start
    )
    for d in days:
        blob = _early_blob_for_trading_day(news_by_calendar, d)
        if not blob.strip():
            continue
        kw_news = keyword_set(blob, k=100)
        tw = theme_carryover.weights_for_observation_day(
            d,
            train_events=train_events,
            news_by_calendar=news_by_calendar,
            returns_df=returns_ml,
            threshold=threshold,
        )
        day_slice = returns_ml[returns_ml["Date"] == pd.Timestamp(d)]
        if day_slice.empty:
            continue
        pos_mask = day_slice["return_pct"] >= threshold
        pos_df = day_slice.loc[pos_mask]
        neg_df = day_slice.loc[~pos_mask]
        if pos_df.empty:
            continue

        d_iso = d.isoformat()
        for _, r in pos_df.iterrows():
            code = str(r["Code"]).zfill(6)
            name = str(names.get(code, r.get("Name", "")))
            fv = _feat_vector(
                train_events,
                code,
                name,
                blob,
                kw_news,
                before_exclusive=d,
                ohlcv_idx=ohlcv_idx,
                theme_weights=tw,
            )
            rows_x.append(fv)
            rows_y.append(1)
            if (d_iso, code) in pos_boost:
                for _ in range(2):
                    rows_x.append(fv)
                    rows_y.append(1)

        neg_codes = neg_df["Code"].astype(str).str.zfill(6).tolist()
        n_pos = int(pos_df.shape[0])
        n_neg = min(MAX_NEG_PER_DAY, max(30, 6 * n_pos))
        if len(neg_codes) > n_neg:
            neg_codes = list(rng.choice(np.array(neg_codes), size=n_neg, replace=False))
        neg_seen: set[str] = set()
        for code in neg_codes:
            name = str(names.get(code, ""))
            fv = _feat_vector(
                train_events,
                code,
                name,
                blob,
                kw_news,
                before_exclusive=d,
                ohlcv_idx=ohlcv_idx,
                theme_weights=tw,
            )
            rows_x.append(fv)
            rows_y.append(0)
            neg_seen.add(code)
            if (d_iso, code) in neg_boost:
                for _ in range(2):
                    rows_x.append(fv)
                    rows_y.append(0)

        for t_iso, code6 in neg_boost:
            if t_iso != d_iso or code6 in neg_seen:
                continue
            sub = neg_df[neg_df["Code"].astype(str).str.zfill(6) == code6]
            if sub.empty:
                continue
            rr = sub.iloc[0]
            name = str(names.get(code6, rr.get("Name", "")))
            fv = _feat_vector(
                train_events,
                code6,
                name,
                blob,
                kw_news,
                before_exclusive=d,
                ohlcv_idx=ohlcv_idx,
                theme_weights=tw,
            )
            rows_x.append(fv)
            rows_y.append(0)
            rows_x.append(fv)
            rows_y.append(0)
            neg_seen.add(code6)

    if len(rows_y) < MIN_TOTAL_SAMPLES or sum(rows_y) < MIN_POS_SAMPLES:
        return None
    return np.asarray(rows_x, dtype=np.float64), np.asarray(rows_y, dtype=np.int32)


def _model_path(fp: str) -> Path:
    return config.CACHE_DIR / "train" / f"move_ranker_v{ML_MODEL_VERSION}_{fp}.joblib"


def fit_or_load_classifier(
    *,
    train_events: list[BreakoutEvent],
    returns_ml: pd.DataFrame,
    news_by_calendar: dict[date, list[dict[str, str]]],
    listing_names: dict[str, str],
    fp: str,
    force_retrain: bool = False,
) -> dict[str, Any] | None:
    """
    스냅샷 지문 ``fp`` 단위로 모델을 캐시에 두고, 없으면 학습 후 저장합니다.

    ``force_retrain=True`` 이면 기존 joblib 을 읽지 않고 항상 재학습해 덮어씁니다
    (``--rebuild-train-snapshot`` 과 함께 최신 ``BreakoutEvent`` 로 랭커를 맞출 때).

    Returns:
        ``{"pipeline": sklearn Pipeline | None, "fp": str, "feature_names": tuple}`` 또는
        ``None`` (ML 비활성·sklearn 없음).
    """
    if not config.PRED_USE_ML_RANKER:
        return None
    if not _SKLEARN_OK:
        print("ML 랭커: scikit-learn 미설치 → 휴리스틱만 사용합니다. (pip install scikit-learn)", flush=True)
        return None

    path = _model_path(fp)
    bundle: dict[str, Any] = {
        "pipeline": None,
        "fp": fp,
        "feature_names": FEATURE_NAMES,
        "version": ML_MODEL_VERSION,
    }
    if path.is_file() and not force_retrain:
        try:
            loaded = joblib.load(path)
            if (
                isinstance(loaded, dict)
                and loaded.get("fp") == fp
                and loaded.get("version") == ML_MODEL_VERSION
                and loaded.get("pipeline") is not None
            ):
                bundle["pipeline"] = loaded["pipeline"]
                print(f"ML 랭커: 캐시 로드 {path.name}", flush=True)
                return bundle
        except Exception:
            pass

    pos_boost, neg_boost = snapshot_miss_diagnosis.load_ml_boost_sets_from_snapshot()
    if pos_boost or neg_boost:
        print(
            f"ML 랭커: 스냅샷 miss_diagnosis 가중 — 급등 보강 {len(pos_boost)}건·오판 보강 {len(neg_boost)}건",
            flush=True,
        )

    xy = _build_training_arrays(
        returns_ml,
        train_events,
        news_by_calendar,
        listing_names,
        config.BIG_MOVE_THRESHOLD,
        config.TRAIN_START_DEFAULT,
        config.TEST_START,
        pos_boost_keys=pos_boost,
        neg_boost_keys=neg_boost,
    )
    if xy is None:
        print(
            "ML 랭커: 학습 표본 부족으로 비활성(휴리스틱만). "
            f"최소 {MIN_TOTAL_SAMPLES}행·급등 라벨 {MIN_POS_SAMPLES}건 이상 필요.",
            flush=True,
        )
        return bundle

    X, y = xy
    clf = HistGradientBoostingClassifier(
        max_depth=6,
        max_iter=180,
        learning_rate=0.07,
        random_state=42,
        class_weight="balanced",
        early_stopping=True,
        validation_fraction=0.12,
        n_iter_no_change=12,
    )
    pipe: Pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("clf", clf),
        ]
    )
    try:
        pipe.fit(X, y)
    except Exception as e:
        print(f"ML 랭커: 학습 실패(휴리스틱만) — {e}", flush=True)
        return bundle
    bundle["pipeline"] = pipe
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, path)
    print(
        f"ML 랭커: 학습 완료 표본 {len(y)} (급등 {int(y.sum())}건) → 저장 {path.name}",
        flush=True,
    )
    return bundle


def rank_predictions_ml(
    *,
    target_day: date,
    listing_codes: list[str],
    listing_names: dict[str, str],
    train_events: list[BreakoutEvent],
    news_text_blob: str,
    returns_ml: pd.DataFrame,
    pipeline: Any,
    top_n: int = 40,
    min_keyword_hits: int = 0,
    theme_weights: dict[str, float] | None = None,
) -> list[predict.PredictionRow]:
    """전 종목에 대해 급등 확률을 매기고 상위 ``top_n`` ``PredictionRow`` 를 만듭니다."""
    ctx = predict.build_scoring_context(news_text_blob, train_events)
    feedback_ctx = prediction_accuracy_cache.build_feedback_context()
    kw_news, _ = ctx
    tw = theme_weights or {}
    ohlcv_idx = _ohlcv_lookup(returns_ml)
    feats: list[list[float]] = []
    for code in listing_codes:
        name = listing_names.get(code, "")
        feats.append(
            _feat_vector(
                train_events,
                code,
                name,
                news_text_blob,
                kw_news,
                before_exclusive=target_day,
                ohlcv_idx=ohlcv_idx,
                theme_weights=tw,
            )
        )
    X = np.asarray(feats, dtype=np.float64)
    proba = pipeline.predict_proba(X)[:, 1]
    order = np.argsort(-proba)

    out: list[predict.PredictionRow] = []
    for ix in order:
        if len(out) >= top_n:
            break
        i = int(ix)
        code = listing_codes[i]
        pr = predict.prediction_row_for_code(
            code,
            listing_names,
            train_events,
            news_text_blob,
            ctx,
            min_keyword_hits,
            feedback_ctx=feedback_ctx,
            theme_weights=tw,
        )
        if pr is None:
            continue
        p = float(proba[i])
        pr.ml_prob = p
        pr.reasons = [
            f"감독학습 랭커(HistGradientBoosting)가 당일 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) "
            f"추정 확률 {p * 100:.1f}% (뉴스·급등이력·시세·전일테마 피처 {len(FEATURE_NAMES)}개)."
        ] + list(pr.reasons)
        out.append(pr)

    predict.apply_display_return_pct_ranking(
        out,
        rank_value=lambda r: float(r.ml_prob or 0.0),
        reason_prefix="표시 예측 상승률은 ML 급등 확률 순위 기준으로",
    )
    for pr in out:
        n_hit = int(getattr(pr, "keyword_hits", 0) or 0)
        mention = float(getattr(pr, "mention_score", 0.0) or 0.0)
        pr.predicted_return_pct = (
            predict._feedback_calibrated_return(
                pr.predicted_return_pct / 100.0,
                code=pr.code,
                n_hit=n_hit,
                mention=mention,
                feedback_ctx=feedback_ctx,
                clamp_lo=float(config.PRED_RETURN_MIN),
                clamp_hi=float(config.PRED_RETURN_MAX),
            )
            * 100.0
        )
    return out
