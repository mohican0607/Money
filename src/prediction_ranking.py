"""
랭킹 우선 예측 모드 — 문제 정의·확신 게이트·Hit@K 평가.

기존 ``20~35%`` 순위 매핑 대신 ML 확률(또는 휴리스틱 순위)을 1차 신호로 쓰고,
``pred_high`` 는 **표시 %** 가 아니라 **확신 구간(confidence tier)** 으로 정의합니다.
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


def confidence_tier(
    row: PredictionRow,
    *,
    rank_position: int,
    pool_size: int,
    regime_scale: float = 1.0,
) -> str:
    """
    ``high`` / ``mid`` / ``low`` / ``none`` 확신 구간.

    ML 경로: ``PRED_ML_*_CONFIDENCE_PROB`` 임계값.
    휴리스틱: 상위 순위 + 최소 점수.
    ``regime_scale`` < 1 이면 임계값을 올려 보수적으로 동작.
    """
    prob = effective_probability(row, rank_position=rank_position, pool_size=pool_size)
    high_thr = float(config.PRED_ML_HIGH_CONFIDENCE_PROB) / max(0.25, regime_scale)
    mid_thr = float(config.PRED_ML_MID_CONFIDENCE_PROB) / max(0.25, regime_scale)
    min_out = float(config.PRED_ML_MIN_OUTPUT_PROB) / max(0.25, regime_scale)

    if prob + 1e-12 < min_out:
        if rank_position <= int(config.PRED_HEURISTIC_HIGH_RANK_MAX) and float(
            getattr(row, "score", 0.0) or 0.0
        ) >= float(config.PRED_HEURISTIC_MIN_SCORE):
            return "mid"
        return "none"

    if prob + 1e-12 >= high_thr:
        return "high"
    if prob + 1e-12 >= mid_thr:
        return "mid"
    if rank_position <= int(config.PRED_HEURISTIC_MID_RANK_MAX) and float(
        getattr(row, "score", 0.0) or 0.0
    ) >= float(config.PRED_HEURISTIC_MIN_SCORE):
        return "low"
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
    """급등 확률(0~1)을 리포트 ``pred_ret`` 열에 쓸 퍼센트 포인트로 변환."""
    p = max(0.0, min(1.0, float(prob)))
    lo = float(config.BIG_MOVE_THRESHOLD) * 100.0
    hi = float(config.PRED_RETURN_MAX) * 100.0
    # base rate ~0.7% 가정 시 prob 0.05 → 약 20%, prob 0.10 → 약 28% (단조, 상한 클램프)
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

    - ``rank_position`` / ``rank_score`` / ``confidence_tier`` 부여
    - 랭킹 모드: ``pred_ret`` = 확률 기반 표시 % (20~35 일괄 매핑 기본 OFF)
    - 레거시: ``PRED_USE_DISPLAY_RANK_MAPPING=1`` 이면 기존 구간 매핑 유지
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
        row.confidence_tier = confidence_tier(
            row, rank_position=pos, pool_size=pool_size, regime_scale=r_scale
        )
        if config.PRED_RANKING_MODE and not config.PRED_USE_DISPLAY_RANK_MAPPING:
            row.predicted_return_pct = probability_to_display_pct(prob)
            row.reasons = [
                f"랭킹 모드: 익일 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) 추정 확률 "
                f"{prob * 100:.2f}% · 순위 {pos}/{pool_size} · 확신 {row.confidence_tier}"
                f"{'' if r_scale >= 0.99 else f' · 시장 레짐 보수({r_scale:.0%})'}"
            ] + [x for x in row.reasons if "표시 예측 상승률" not in x and "20~35" not in x]

    max_high = max(3, int(round(int(config.PRED_OUTPUT_MAX) * r_scale)))
    high_count = 0
    for row in pool:
        if row.confidence_tier == "high":
            high_count += 1
            if high_count > max_high:
                row.confidence_tier = "mid"

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
