"""
고확신(pred_high) 적중률 극대화 게이트.

``pred_factors`` 다요인 기둥 합의로 ``confidence_tier=high`` 를 재필터링합니다.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from .. import config, prediction_accuracy_cache
from . import pred_factors

if TYPE_CHECKING:
    from .predict import PredictionRow


def _pillar_scores(row: PredictionRow) -> dict[str, float]:
    """``pred_factors.compute_pillar_scores`` 래퍼(행에 KS11 lag1 이 있으면 전달)."""
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return pred_factors.compute_pillar_scores(row, ks11_ret_lag1=ks11)


def conviction_score(row: PredictionRow) -> float:
    """다요인 기둥 합의 기반 확신 점수(0~1). ``pred_factors.conviction_score`` 위임."""
    ks11 = getattr(row, "ks11_ret_lag1", None)
    return pred_factors.conviction_score(row, ks11_ret_lag1=ks11)


def _bucket_ok(row: PredictionRow, feedback_ctx: dict[str, object] | None) -> bool:
    """과거 예측–실적 버킷 평균 정합도가 하한 이상이면 True(표본 부족 시 통과)."""
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
    """종목별 과거 고확신 예측의 급등 적중률이 ``PRED_PRECISION_CODE_MIN_HIT_RATE`` 이상이면 True."""
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
    """
    고확신 슬롯 승격 조건: 순위·확신·비뉴스 기둥·ML 절대/상대 하한·피드백 버킷·종목 이력.

    ``top_ml`` 은 당일 풀 최고 ML 확률(상대 하한 ``PRED_ML_HIGH_RELATIVE_PROB`` 용).
    """
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
    # 뉴스만 강하고 비뉴스 기둥이 부족하면 뉴스 단독 고확신 차단
    if pillars["news"] + 1e-12 >= 0.52 and non_news < 2:
        return False
    # 모멘텀·수급·상대강도·섹터·ML 중 2개 이상이 각 임계 이상이어야 함
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
    """
    하이브리드 tier 부여 후 ``confidence_tier=high`` 를 정밀 게이트로 재필터링합니다.

    통과 종목만 ``high`` 유지, 기존 ``high`` 미통과는 ``mid`` 로 강등. ``PRED_PRECISION_GATE_ENABLED=0`` 이면 no-op.
    """
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

