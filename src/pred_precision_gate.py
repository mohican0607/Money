"""
고확신(pred_high) 적중률 극대화 게이트.

하이브리드 랭킹 후 **다중 신호 합의(ML·뉴스·모멘텀·테마·업종)** 와
누적 피드백(버킷·종목 이력)으로 ``confidence_tier=high`` 를 재필터링합니다.
"""
from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from . import config, prediction_accuracy_cache

if TYPE_CHECKING:
    from .predict import PredictionRow


def _pillar_scores(row: PredictionRow) -> dict[str, float]:
    ml = float(getattr(row, "ml_prob", 0.0) or 0.0)
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    ind_m = float(getattr(row, "industry_momentum", 0.0) or 0.0)
    ind_ov = float(getattr(row, "industry_theme_overlap", 0.0) or 0.0)
    ind_lim = float(getattr(row, "industry_limit_up_heat", 0.0) or 0.0)
    return {
        "ml": min(1.0, ml / 0.20),
        "news": min(
            1.0,
            0.40 * nctx + 0.32 * min(1.0, nh / 3.0) + 0.28 * min(1.0, mention),
        ),
        "momentum": min(1.0, mom / 0.58),
        "theme": min(1.0, max(ind_ov, ind_lim * 0.85)),
        "industry": min(1.0, (ind_m * 0.65 + ind_lim * 0.35) / 0.38),
    }


def conviction_score(row: PredictionRow) -> float:
    """0~1 확신 점수(상위 기둥 가중 평균)."""
    pillars = _pillar_scores(row)
    vals = sorted(pillars.values(), reverse=True)
    if len(vals) < 3:
        return 0.0
    w = (0.34, 0.28, 0.22, 0.10, 0.06)
    s = sum(v * w[i] for i, v in enumerate(vals[: len(w)]))
    strong = sum(1 for v in vals if v + 1e-12 >= 0.50)
    if strong < int(config.PRED_PRECISION_MIN_PILLARS):
        s *= 0.72
    return max(0.0, min(1.0, s))


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
    """과거 고확신 이력이 나쁜 종목은 제외."""
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
    """고확신 슬롯 통과 여부."""
    if rank_position > int(config.PRED_PRECISION_MAX_RANK):
        return False
    conv = conviction_score(row)
    if conv + 1e-12 < float(config.PRED_PRECISION_MIN_CONVICTION):
        return False
    pillars = _pillar_scores(row)
    strong = sum(1 for v in pillars.values() if v + 1e-12 >= 0.48)
    if strong < int(config.PRED_PRECISION_MIN_PILLARS):
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
    nh = int(getattr(row, "keyword_hits", 0) or 0)
    mention = float(getattr(row, "mention_score", 0.0) or 0.0)
    nctx = float(getattr(row, "news_context_score", 0.0) or 0.0)
    mom = float(getattr(row, "momentum_score", 0.0) or 0.0)
    signals = 0
    if nh >= int(config.PRED_ML_HIGH_MIN_KEYWORD_HITS):
        signals += 1
    if mention + 1e-12 >= float(config.PRED_MENTION_GATE_MIN):
        signals += 1
    if nctx + 1e-12 >= 0.30:
        signals += 1
    if mom + 1e-12 >= 0.22:
        signals += 1
    if pillars["theme"] + 1e-12 >= 0.45 or pillars["industry"] + 1e-12 >= 0.40:
        signals += 1
    if signals < 2:
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
    하이브리드 tier 부여 후 고확신을 **확신 점수·다중 합의** 로 재선별합니다.

    통과 종목만 ``high`` 유지. 아무도 통과하지 못하면 하이브리드 배정을 유지하거나
    상위 ML 순위로 폴백해 예측 후보가 비지 않게 합니다.
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
    hybrid_high_codes = {
        str(r.code).zfill(6)
        for r in pool
        if str(getattr(r, "confidence_tier", "")) == "high"
    }
    promoted: list[str] = []
    for pos, row in enumerate(ranked, start=1):
        if len(promoted) >= max_high:
            break
        if not passes_precision_gate(
            row, top_ml=top_ml, rank_position=pos, feedback_ctx=feedback_ctx
        ):
            continue
        promoted.append(str(row.code).zfill(6))

    if not promoted:
        if hybrid_high_codes:
            return
        ml_floor = float(config.PRED_ML_MIN_OUTPUT_PROB)
        for row in ranked[:max_high]:
            ml = float(getattr(row, "ml_prob", 0.0) or 0.0)
            if ml + 1e-12 >= ml_floor:
                promoted.append(str(row.code).zfill(6))
        if not promoted and ranked:
            promoted.append(str(ranked[0].code).zfill(6))

    promoted_set = set(promoted)
    for row in pool:
        code = str(row.code).zfill(6)
        if code in promoted_set:
            row.confidence_tier = "high"
        elif str(getattr(row, "confidence_tier", "")) == "high":
            row.confidence_tier = "mid"
