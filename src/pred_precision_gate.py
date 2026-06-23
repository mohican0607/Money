"""
고확신(pred_high) 적중률 극대화 게이트.

``pred_factors`` 다요인 기둥 합의로 ``confidence_tier=high`` 를 재필터링합니다.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from . import config, pred_factors, prediction_accuracy_cache

if TYPE_CHECKING:
    from .predict import PredictionRow


def _pillar_scores(row: PredictionRow) -> dict[str, float]:
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return pred_factors.compute_pillar_scores(row, ks11_ret_lag1=ks11)


def conviction_score(row: PredictionRow) -> float:
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return pred_factors.conviction_score(row, ks11_ret_lag1=ks11)


def _bucket_ok(row: PredictionRow, feedback_ctx: dict[str, object] | None) -> bool:
    if not feedback_ctx:
        return True
    stats = feedback_ctx.get("signal_bucket_stats")
    if not isinstance(stats, dict):
        return True
    key = prediction_accuracy_cache._signal_bucket_key(
        pred_ret=float(getattr(row, "predicted_return_pct", 0.0) or 0.0),
        keyword_hits=int(getattr(row, "keyword_hits", 0) or 0),
        mention_score=float(getattr(row, "mention_score", 0.0) or 0.0),
    )
    b = stats.get(key)
    if not isinstance(b, dict):
        return True
    n = int(b.get("count", 0) or 0)
    if n < int(config.PRED_PRECISION_BUCKET_MIN_SAMPLES):
        return True
    mean_r = float(b.get("mean_ratio", 0.0) or 0.0)
    return mean_r + 1e-12 >= float(config.PRED_PRECISION_BUCKET_MIN_RATIO)


def _code_history_ok(code: str) -> bool:
    payload = prediction_accuracy_cache._load_payload()
    hp = payload.get("high_pred_by_code")
    tap = payload.get("t_code_actual_pct")
    if not isinstance(hp, dict) or not isinstance(tap, dict):
        return True
    hist = hp.get(str(code).zfill(6))
    if not isinstance(hist, list) or len(hist) < int(config.PRED_PRECISION_CODE_MIN_TRIES):
        return True
    hits = 0
    n = 0
    thr = float(config.BIG_MOVE_THRESHOLD) * 100.0
    for ent in hist[-12:]:
        if not isinstance(ent, dict):
            continue
        t = str(ent.get("trading_day") or ent.get("day") or "")
        if not t:
            continue
        key = f"{t}:{str(code).zfill(6)}"
        act = tap.get(key)
        if act is None:
            continue
        n += 1
        if float(act) + 1e-9 >= thr:
            hits += 1
    if n < int(config.PRED_PRECISION_CODE_MIN_TRIES):
        return True
    rate = hits / n
    return rate + 1e-12 >= float(config.PRED_PRECISION_CODE_MIN_HIT_RATE)


def passes_precision_gate(
    row: PredictionRow,
    *,
    top_ml: float,
    rank_position: int,
    feedback_ctx: dict[str, object] | None = None,
) -> bool:
    if rank_position > int(config.PRED_PRECISION_MAX_RANK):
        return False
    conv = conviction_score(row)
    if conv + 1e-12 < float(config.PRED_PRECISION_MIN_CONVICTION):
        return False
    pillars = _pillar_scores(row)
    non_news = pred_factors.count_non_news_strong_pillars(pillars, threshold=0.46)
    if non_news < int(config.PRED_PRECISION_MIN_PILLARS):
        return False
    ml = float(getattr(row, "ml_prob", 0.0) or 0.0)
    floor = max(
        float(config.PRED_ML_HIGH_CONFIDENCE_PROB),
        float(config.PRED_PRECISION_ML_FLOOR),
    )
    if ml + 1e-12 < floor:
        return False
    if top_ml > 1e-9 and ml + 1e-12 < top_ml * float(config.PRED_ML_HIGH_RELATIVE_PROB):
        return False
    if pillars["news"] + 1e-12 >= 0.52 and non_news < 2:
        return False
    non_news_signals = 0
    if pillars["momentum"] + 1e-12 >= 0.40:
        non_news_signals += 1
    if pillars["flow"] + 1e-12 >= 0.40:
        non_news_signals += 1
    if pillars["relative_strength"] + 1e-12 >= 0.38:
        non_news_signals += 1
    if pillars["sector"] + 1e-12 >= 0.42:
        non_news_signals += 1
    if pillars["ml"] + 1e-12 >= 0.45:
        non_news_signals += 1
    if non_news_signals < 2:
        return False
    if not _bucket_ok(row, feedback_ctx):
        return False
    if not _code_history_ok(str(row.code).zfill(6)):
        return False
    return True


def refine_confidence_tiers(
    pool: list[PredictionRow],
    *,
    feedback_ctx: dict[str, object] | None = None,
    regime_scale: float = 1.0,
) -> None:
    if not pool or not config.PRED_PRECISION_GATE_ENABLED:
        return
    rs = max(0.25, float(regime_scale))
    max_high = max(1, int(round(int(config.PRED_PRECISION_MAX_HIGH) * rs)))
    top_ml = max(
        (float(getattr(r, "ml_prob", 0.0) or 0.0) for r in pool),
        default=0.0,
    )
    ranked = sorted(
        pool,
        key=lambda r: (conviction_score(r), float(getattr(r, "ml_prob", 0.0) or 0.0)),
        reverse=True,
    )
    promoted: list[str] = []
    for pos, row in enumerate(ranked, start=1):
        if len(promoted) >= max_high:
            break
        if not passes_precision_gate(
            row, top_ml=top_ml, rank_position=pos, feedback_ctx=feedback_ctx
        ):
            continue
        promoted.append(str(row.code).zfill(6))

    promoted_set = set(promoted)
    for row in pool:
        code = str(row.code).zfill(6)
        if code in promoted_set:
            row.confidence_tier = "high"
        elif str(getattr(row, "confidence_tier", "")) == "high":
            row.confidence_tier = "mid"

