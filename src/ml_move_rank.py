"""
훈련 구간 라벨(당일 급등 여부)로 **감독학습 랭커**를 학습해, 관측일 후보 종목을 확률 순으로 정렬합니다.

피처: 뉴스·과거 급등 이력(키워드 교집합, 종목명, 과거 평균 수익률, 이벤트 건수, 휴리스틱 점수)과
``stocks.enrich_daily_returns_for_ml`` 로 만든 **전일 수익·거래량·단기 변동성·이평 대비 위치** 등 시세 요약.
학습 행은 거래일 ``d`` 기준으로 ``d`` **이전**의 급등 이벤트만 써서 시간 누수를 줄입니다.
"""
from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from . import config, news, predict, prediction_accuracy_cache, trading_calendar
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
    "ret_lag1",
    "log_vol_lag1",
    "ret_roll_std5",
    "log_vol_roll_mean5",
    "close_ma20_ratio",
)

ML_MODEL_VERSION = 3
MAX_NEG_PER_DAY = 360
MIN_TOTAL_SAMPLES = 200
MIN_POS_SAMPLES = 25

# ML 확률 → 표시 예측 상승률(%) 단조 매핑. 하한은 임계(20%)보다 낮게 두어
# 저확률 후보는 pred≥임계 표에서 빠지게 해 적중률(실제도≥임계) 분모를 정렬한다.
_ML_RETURN_MAP_FLOOR_PCT = 11.0


def _display_return_pct_from_ml_prob(p: float) -> float:
    """급등(≥임계) 추정 확률을 [하한, PRED_RETURN_MAX] 구간의 표시 %%로 변환."""
    lo = _ML_RETURN_MAP_FLOOR_PCT
    hi = float(config.PRED_RETURN_MAX) * 100.0
    t = float(min(1.0, max(0.0, p)))
    return lo + (hi - lo) * t


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
) -> tuple[np.ndarray, np.ndarray] | None:
    rng = np.random.default_rng(42)
    rows_x: list[list[float]] = []
    rows_y: list[int] = []
    ohlcv_idx = _ohlcv_lookup(returns_ml)

    days = sorted(
        d for d in returns_ml["Date"].dt.date.unique() if train_start <= d < test_start
    )
    for d in days:
        blob = _early_blob_for_trading_day(news_by_calendar, d)
        if not blob.strip():
            continue
        kw_news = keyword_set(blob, k=100)
        day_slice = returns_ml[returns_ml["Date"] == pd.Timestamp(d)]
        if day_slice.empty:
            continue
        pos_mask = day_slice["return_pct"] >= threshold
        pos_df = day_slice.loc[pos_mask]
        neg_df = day_slice.loc[~pos_mask]
        if pos_df.empty:
            continue

        for _, r in pos_df.iterrows():
            code = str(r["Code"]).zfill(6)
            name = str(names.get(code, r.get("Name", "")))
            rows_x.append(
                _feat_vector(
                    train_events,
                    code,
                    name,
                    blob,
                    kw_news,
                    before_exclusive=d,
                    ohlcv_idx=ohlcv_idx,
                )
            )
            rows_y.append(1)

        neg_codes = neg_df["Code"].astype(str).str.zfill(6).tolist()
        n_pos = int(pos_df.shape[0])
        n_neg = min(MAX_NEG_PER_DAY, max(30, 6 * n_pos))
        if len(neg_codes) > n_neg:
            neg_codes = list(rng.choice(np.array(neg_codes), size=n_neg, replace=False))
        for code in neg_codes:
            name = str(names.get(code, ""))
            rows_x.append(
                _feat_vector(
                    train_events,
                    code,
                    name,
                    blob,
                    kw_news,
                    before_exclusive=d,
                    ohlcv_idx=ohlcv_idx,
                )
            )
            rows_y.append(0)

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

    xy = _build_training_arrays(
        returns_ml,
        train_events,
        news_by_calendar,
        listing_names,
        config.BIG_MOVE_THRESHOLD,
        config.TRAIN_START_DEFAULT,
        config.TEST_START,
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
) -> list[predict.PredictionRow]:
    """전 종목에 대해 급등 확률을 매기고 상위 ``top_n`` ``PredictionRow`` 를 만듭니다."""
    ctx = predict.build_scoring_context(news_text_blob, train_events)
    feedback_ctx = prediction_accuracy_cache.build_feedback_context()
    kw_news, _ = ctx
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
        )
        if pr is None:
            continue
        p = float(proba[i])
        pr.ml_prob = p
        pred_pct = _display_return_pct_from_ml_prob(p)
        n_hit = int(max(0.0, feats[i][0])) if i < len(feats) and len(feats[i]) > 0 else 0
        mention = float(max(0.0, feats[i][1])) if i < len(feats) and len(feats[i]) > 1 else 0.0
        pr.predicted_return_pct = (
            predict._feedback_calibrated_return(
                pred_pct / 100.0,
                code=code,
                n_hit=n_hit,
                mention=mention,
                feedback_ctx=feedback_ctx,
            )
            * 100.0
        )
        pr.reasons = [
            f"감독학습 랭커(HistGradientBoosting)가 당일 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) "
            f"추정 확률 {p * 100:.1f}% (뉴스·급등이력·시세 피처 {len(FEATURE_NAMES)}개). "
            f"표시 예측 상승률은 이 확률을 {_ML_RETURN_MAP_FLOOR_PCT:.0f}~{config.PRED_RETURN_MAX * 100:.0f}% 구간에 선형 정렬한 값입니다."
        ] + list(pr.reasons)
        out.append(pr)
    return out
