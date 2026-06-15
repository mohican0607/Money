"""
랭킹 우선 예측 모드 — 문제 정의·확신 게이트·Hit@K 평가.

기존 ``20~30%`` 순위 매핑 대신 ML 확률(또는 휴리스틱 순위)을 1차 신호로 쓰고,
``pred_high`` / ``pred_mid`` 는 **순위 상위 + 엄격 확률 하한** 으로 각 최대 10건씩만 부여합니다.
"""
from __future__ import annotations

import math
from datetime import date
from typing import TYPE_CHECKING, Any

from . import config

if TYPE_CHECKING:
    from .predict import PredictionRow


def rank_score_for_row(row: PredictionRow) -> float:
    """정렬·게이트에 쓰는 단일 순위 점수(ML 확률 우선)."""
    p = getattr(row, "ml_prob", None)
    if p is not None and math.isfinite(float(p)):
        return float(p)
    return float(getattr(row, "score", 0.0) or 0.0)


def _heuristic_pseudo_prob(rank_position: int, pool_size: int) -> float:
    """ML 없을 때 순위만으로 0~1 사이 의사 확률(평가·표시용)."""
    if pool_size <= 0:
        return 0.0
    t = 1.0 - (float(rank_position) - 1.0) / float(max(1, pool_size))
    return max(0.005, min(0.12, 0.012 + 0.10 * t))


def effective_probability(row: PredictionRow, *, rank_position: int, pool_size: int) -> float:
    """종목의 급등 추정 확률(ML ``ml_prob`` 또는 휴리스틱 의사 확률)."""
    p = getattr(row, "ml_prob", None)
    if p is not None and math.isfinite(float(p)):
        return float(p)
    return _heuristic_pseudo_prob(rank_position, pool_size)


def _tier_thresholds(*, regime_scale: float) -> tuple[float, float, float]:
    rs = max(0.25, float(regime_scale))
    return (
        float(config.PRED_ML_HIGH_CONFIDENCE_PROB) / rs,
        float(config.PRED_ML_MID_CONFIDENCE_PROB) / rs,
        float(config.PRED_ML_MIN_OUTPUT_PROB) / rs,
    )


def assign_confidence_tiers_by_rank(
    pool: list[PredictionRow],
    *,
    regime_scale: float = 1.0,
) -> None:
    """
    ML 순위대로 고확신·중확신 슬롯을 각 최대 ``PRED_OUTPUT_MAX`` / ``PRED_MID_OUTPUT_MAX`` 까지 채웁니다.

    임계 확률·최소 신호를 통과한 종목만 슬롯에 들어가며, 부족하면 10건 미만으로 둡니다.
    """
    pool_size = len(pool)
    high_thr, mid_thr, min_out = _tier_thresholds(regime_scale=regime_scale)
    max_high = max(1, int(round(int(config.PRED_OUTPUT_MAX) * regime_scale)))
    max_mid = max(1, int(round(int(config.PRED_MID_OUTPUT_MAX) * regime_scale)))
    min_score = float(config.PRED_HEURISTIC_MIN_SCORE)

    for row in pool:
        row.confidence_tier = "none"

    high_n = 0
    mid_n = 0
    for pos, row in enumerate(pool, start=1):
        prob = effective_probability(row, rank_position=pos, pool_size=pool_size)
        score = float(getattr(row, "score", 0.0) or 0.0)
        raw_ml = getattr(row, "ml_prob", None)
        has_ml = raw_ml is not None and math.isfinite(float(raw_ml))

        if prob + 1e-12 < min_out and score + 1e-12 < min_score:
            continue

        if has_ml:
            if high_n < max_high and prob + 1e-12 >= high_thr:
                row.confidence_tier = "high"
                high_n += 1
                continue
            if mid_n < max_mid and prob + 1e-12 >= mid_thr:
                row.confidence_tier = "mid"
                mid_n += 1
            continue

        # ML 없음: 휴리스틱만 — 상위 소수·고점수만 중확신 이하로 제한
        hi_rank = int(config.PRED_HEURISTIC_HIGH_RANK_MAX)
        if hi_rank > 0 and high_n < max_high and pos <= hi_rank and score + 1e-12 >= min_score:
            row.confidence_tier = "high"
            high_n += 1


def confidence_tier(
    row: PredictionRow,
    *,
    rank_position: int,
    pool_size: int,
    regime_scale: float = 1.0,
) -> str:
    """레거시 호출용 — ``assign_confidence_tiers_by_rank`` 가 최종 tier 를 덮어씁니다."""
    prob = effective_probability(row, rank_position=rank_position, pool_size=pool_size)
    high_thr, mid_thr, min_out = _tier_thresholds(regime_scale=regime_scale)
    if prob + 1e-12 < min_out:
        return "none"
    if prob + 1e-12 >= high_thr:
        return "high"
    if prob + 1e-12 >= mid_thr:
        return "mid"
    return "none"


def is_high_confidence_prediction(row: PredictionRow) -> bool:
    """``pred_high`` 대체: 확신 ``high`` 또는 (레거시) 표시 % ≥ 급등 임계."""
    if config.PRED_RANKING_MODE:
        tier = str(getattr(row, "confidence_tier", "") or "")
        return tier == "high"
    pct = float(getattr(row, "predicted_return_pct", 0.0) or 0.0)
    return pct >= float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def is_mid_confidence_prediction(row: PredictionRow) -> bool:
    if config.PRED_RANKING_MODE:
        return str(getattr(row, "confidence_tier", "") or "") == "mid"
    pct = float(getattr(row, "predicted_return_pct", 0.0) or 0.0)
    return 10.0 - 1e-9 <= pct < float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def row_pred_high_from_dict(row: dict) -> bool:
    """``rows_compare`` dict 에서 ``pred_high`` 판정."""
    if config.PRED_RANKING_MODE:
        tier = str(row.get("confidence_tier") or "")
        if tier == "high":
            return True
        if tier:
            return False
    pr = row.get("pred_ret")
    if pr is None:
        return False
    return float(pr) >= float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def row_pred_mid_from_dict(row: dict) -> bool:
    if config.PRED_RANKING_MODE:
        return str(row.get("confidence_tier") or "") == "mid"
    pr = row.get("pred_ret")
    if pr is None:
        return False
    return 10.0 - 1e-9 <= float(pr) < float(config.BIG_MOVE_THRESHOLD) * 100.0 - 1e-9


def probability_to_display_pct(prob: float) -> float:
    """(레거시) 급등 확률(0~1)을 [20%, PRED_RETURN_MAX] 표시 %%로 변환. 랭킹 모드 기본 경로에서는 쓰지 않음."""
    p = max(0.0, min(1.0, float(prob)))
    lo = float(config.BIG_MOVE_THRESHOLD) * 100.0
    hi = float(config.PRED_RETURN_MAX) * 100.0
    scaled = lo + (hi - lo) * min(1.0, p / max(0.02, float(config.PRED_ML_HIGH_CONFIDENCE_PROB) * 2.5))
    return float(max(0.0, min(hi, scaled)))


def regime_output_scale(ks11_ret_lag1: float | None) -> float:
    """KOSPI 전일 수익률 기반 출력 축소 배율(1.0=정상, 0.5=리스크오프)."""
    if not config.PRED_REGIME_GATE_ENABLED or ks11_ret_lag1 is None:
        return 1.0
    if not math.isfinite(float(ks11_ret_lag1)):
        return 1.0
    soft = float(config.PRED_REGIME_KS11_SOFT_MIN)
    if float(ks11_ret_lag1) >= soft:
        return 1.0
    return float(config.PRED_REGIME_OUTPUT_SCALE)


def finalize_ranked_predictions(
    rows: list[PredictionRow],
    *,
    target_day: date,
    ks11_ret_lag1: float | None = None,
) -> list[PredictionRow]:
    """
    순위·확신 구간·표시 % 를 일괄 확정합니다.

    랭킹 모드: 순위 상위부터 고확신 최대 10·중확신 최대 10(엄격 ML 확률 하한 통과분만).
    """
    if not rows:
        return rows

    from . import predict

    pool = sorted(rows, key=rank_score_for_row, reverse=True)
    pool_size = len(pool)
    r_scale = regime_output_scale(ks11_ret_lag1)

    if config.PRED_USE_DISPLAY_RANK_MAPPING:
        predict.apply_display_return_pct_ranking(
            pool,
            rank_value=rank_score_for_row,
            reason_prefix="표시 예측 상승률은 순위 지표(ML 확률·휴리스틱 점수) 기준으로",
        )

    for pos, row in enumerate(pool, start=1):
        row.rank_position = pos
        row.rank_score = rank_score_for_row(row)
        prob = effective_probability(row, rank_position=pos, pool_size=pool_size)
        if getattr(row, "ml_prob", None) is None:
            row.ml_prob = prob if config.PRED_RANKING_MODE else None

    if config.PRED_RANKING_MODE:
        assign_confidence_tiers_by_rank(pool, regime_scale=r_scale)
    else:
        for pos, row in enumerate(pool, start=1):
            row.confidence_tier = confidence_tier(
                row, rank_position=pos, pool_size=pool_size, regime_scale=r_scale
            )

    if config.PRED_RANKING_MODE and not config.PRED_USE_DISPLAY_RANK_MAPPING:
        for pos, row in enumerate(pool, start=1):
            prob = effective_probability(row, rank_position=pos, pool_size=pool_size)
            tier = str(row.confidence_tier or "none")
            rank_note = (
                f"랭킹 모드: 익일 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) 추정 확률 "
                f"{prob * 100:.2f}% · 순위 {pos}/{pool_size} · 확신 {tier}"
                f" (고확신≤{config.PRED_OUTPUT_MAX}·중확신≤{config.PRED_MID_OUTPUT_MAX}, "
                f"ML≥{config.PRED_ML_HIGH_CONFIDENCE_PROB * 100:.1f}%/"
                f"{config.PRED_ML_MID_CONFIDENCE_PROB * 100:.1f}%)"
                f"{'' if r_scale >= 0.99 else f' · 시장 레짐 보수({r_scale:.0%})'}"
            )
            row.reasons = [rank_note] + [
                x for x in row.reasons if not x.startswith("랭킹 모드:")
            ]

    return pool


def compute_hit_at_k_metrics(
    ranked_codes: list[str],
    actual_big_codes: set[str],
    *,
    universe_size: int,
    k_list: tuple[int, ...] | None = None,
) -> dict[str, Any]:
    """순위 리스트 대비 Hit@K·Precision@K·Recall·Lift."""
    ks = tuple(k_list or config.PRED_EVAL_HIT_AT_K)
    n_actual = len(actual_big_codes)
    base_rate = (n_actual / universe_size) if universe_size > 0 else 0.0
    out: dict[str, Any] = {
        "universe_size": int(universe_size),
        "actual_big_count": int(n_actual),
        "base_rate": round(base_rate, 6),
    }
    codes = [str(c).zfill(6) for c in ranked_codes]
    actual = {str(c).zfill(6) for c in actual_big_codes}
    best_k_hit = 0
    for k in ks:
        if k <= 0:
            continue
        top = codes[:k]
        hit = sum(1 for c in top if c in actual)
        prec = hit / k if k else 0.0
        out[f"hit_at_{k}"] = int(hit)
        out[f"precision_at_{k}"] = round(prec, 6)
        if base_rate > 1e-12:
            out[f"lift_at_{k}"] = round(prec / base_rate, 4)
        else:
            out[f"lift_at_{k}"] = None
        best_k_hit = max(best_k_hit, hit)
    if n_actual > 0:
        out["recall_at_max_k"] = round(best_k_hit / n_actual, 6)
    else:
        out["recall_at_max_k"] = None
    return out
