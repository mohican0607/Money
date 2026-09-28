"""
예측 오판 반성 루프 — recency 가중·적응형 보수화·랭킹 패널티·증분 miss 부스트.

``accuracy_cache.build_feedback_context`` 위에 최근 오판 이력을 반영해
추론 시점의 후보 선별·확신 게이트·표시 % 보정을 강화합니다.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from .. import config, trading_calendar
from . import accuracy_cache as prediction_accuracy_cache

INCREMENTAL_MISS_PATH = config.TRAIN_CACHE_DIR / "incremental_miss_boost.json"
MISS_REFLECTION_PATH = config.TRAIN_CACHE_DIR / "miss_reflection_lessons.json"


def _parse_t_code_key(key: str) -> tuple[date | None, str]:
    if not isinstance(key, str) or ":" not in key:
        return None, ""
    t_s, code = key.rsplit(":", 1)
    try:
        return date.fromisoformat(t_s), str(code).zfill(6)
    except ValueError:
        return None, code


def _recency_weight(day: date, *, as_of: date, half_life_days: float) -> float:
    hl = max(1.0, float(half_life_days))
    delta = (as_of - day).days
    if delta < 0:
        return 0.0
    return 0.5 ** (float(delta) / hl)


def _hit_at_10_rate(rec: dict) -> float | None:
    """``hit_at_10`` — int(적중 수) 또는 {hits,k} dict 모두 처리."""
    h10 = rec.get("hit_at_10")
    if isinstance(h10, dict):
        n = int(h10.get("k", 10) or 10)
        h = int(h10.get("hits", 0) or 0)
        return float(h) / float(max(1, n))
    if isinstance(h10, (int, float)):
        return float(int(h10)) / 10.0
    return None


def _recent_hit_rate(payload: dict, *, as_of: date, lookback_days: int = 12) -> float | None:
    by_day = payload.get("hit_at_k_by_day")
    if not isinstance(by_day, dict):
        return None
    cutoff = as_of - timedelta(days=max(3, int(lookback_days)))
    hits = 0
    total = 0
    for t_iso, rec in sorted(by_day.items()):
        if not isinstance(rec, dict):
            continue
        try:
            t_d = date.fromisoformat(str(t_iso)[:10])
        except ValueError:
            continue
        if t_d < cutoff or t_d > as_of:
            continue
        h10 = rec.get("hit_at_10")
        if isinstance(h10, dict):
            h = int(h10.get("hits", 0) or 0)
            n = int(h10.get("k", 10) or 10)
            hits += h
            total += n
        elif isinstance(h10, (int, float)):
            hits += int(h10)
            total += 10
    if total <= 0:
        return None
    return float(hits) / float(total)


def _recent_precision_from_ratios(
    payload: dict, *, as_of: date, lookback_days: int = 10
) -> float | None:
    raw = payload.get("t_code_ratio")
    if not isinstance(raw, dict):
        return None
    cutoff = as_of - timedelta(days=max(3, int(lookback_days)))
    hits = 0
    total = 0
    for key, ratio in raw.items():
        t_d, _code = _parse_t_code_key(key)
        if t_d is None or t_d < cutoff or t_d > as_of:
            continue
        if not isinstance(ratio, (int, float)) or not math.isfinite(float(ratio)):
            continue
        total += 1
        if float(ratio) >= 0.99:
            hits += 1
    if total <= 0:
        return None
    return float(hits) / float(total)


def compute_adaptive_tightness(
    payload: dict,
    *,
    as_of: date | None = None,
) -> float:
    """
    최근 적중률 기반 보수화 계수 (0.50~1.0).

    낮을수록 확신 게이트·랭킹 패널티·표시 % 보정이 강해집니다.
    """
    if not config.PRED_FEEDBACK_ADAPTIVE_ENABLED:
        return 1.0
    as_of = as_of or date.today()
    hit_rate = _recent_hit_rate(payload, as_of=as_of)
    prec = _recent_precision_from_ratios(payload, as_of=as_of)
    scores: list[float] = []
    if hit_rate is not None:
        if hit_rate < 0.03:
            scores.append(0.50)
        elif hit_rate < 0.06:
            scores.append(0.58)
        elif hit_rate < 0.10:
            scores.append(0.68)
        elif hit_rate < 0.15:
            scores.append(0.78)
        elif hit_rate < 0.20:
            scores.append(0.88)
        else:
            scores.append(1.0)
    if prec is not None:
        if prec < 0.05:
            scores.append(0.52)
        elif prec < 0.10:
            scores.append(0.62)
        elif prec < 0.15:
            scores.append(0.72)
        elif prec < 0.22:
            scores.append(0.82)
        else:
            scores.append(1.0)
    if not scores:
        return 1.0
    t = min(scores)
    floor = float(config.PRED_FEEDBACK_TIGHTNESS_FLOOR)
    return float(max(floor, min(1.0, t)))


def _recent_weighted_code_stats(
    payload: dict,
    *,
    as_of: date,
    half_life_days: float,
    window_days: int,
) -> tuple[dict[str, float], dict[str, int]]:
    raw = payload.get("t_code_ratio")
    if not isinstance(raw, dict):
        return {}, {}
    cutoff = as_of - timedelta(days=max(5, int(window_days)))
    acc_w: dict[str, float] = {}
    acc_v: dict[str, float] = {}
    acc_n: dict[str, int] = {}
    for key, ratio in raw.items():
        t_d, code = _parse_t_code_key(key)
        if t_d is None or not code or t_d < cutoff or t_d > as_of:
            continue
        if not isinstance(ratio, (int, float)) or not math.isfinite(float(ratio)):
            continue
        w = _recency_weight(t_d, as_of=as_of, half_life_days=half_life_days)
        if w <= 0:
            continue
        rv = min(max(abs(float(ratio)), 0.0), 1.0)
        acc_w[code] = acc_w.get(code, 0.0) + w
        acc_v[code] = acc_v.get(code, 0.0) + rv * w
        acc_n[code] = acc_n.get(code, 0) + 1
    by_mean = {
        c: (acc_v[c] / acc_w[c]) for c in acc_w if acc_w[c] > 1e-12
    }
    return by_mean, acc_n


def build_enriched_feedback_context(*, as_of: date | None = None) -> dict[str, object]:
    """``build_feedback_context`` + recency·적응형·최근 miss streak."""
    base = prediction_accuracy_cache.build_feedback_context()
    payload = prediction_accuracy_cache._load_payload()
    as_of = as_of or date.today()
    hl = float(config.PRED_FEEDBACK_RECENCY_HALF_LIFE_DAYS)
    win = int(config.PRED_FEEDBACK_RECENCY_WINDOW_DAYS)
    recent_mean, recent_count = _recent_weighted_code_stats(
        payload, as_of=as_of, half_life_days=hl, window_days=win
    )
    tightness = compute_adaptive_tightness(payload, as_of=as_of)
    recent_hit = _recent_hit_rate(payload, as_of=as_of)
    recent_prec = _recent_precision_from_ratios(payload, as_of=as_of)

    streak = 0
    by_day = payload.get("hit_at_k_by_day")
    if isinstance(by_day, dict):
        for t_iso in sorted(by_day.keys(), reverse=True):
            rec = by_day.get(t_iso)
            if not isinstance(rec, dict):
                continue
            try:
                t_d = date.fromisoformat(str(t_iso)[:10])
            except ValueError:
                continue
            if t_d > as_of:
                continue
            rate = _hit_at_10_rate(rec)
            if rate is None:
                break
            if rate >= float(config.PRED_FEEDBACK_STREAK_HIT_MIN):
                break
            streak += 1
            if streak >= int(config.PRED_FEEDBACK_MISS_STREAK_MAX_DAYS):
                break

    base["by_code_recent_mean_ratio"] = recent_mean
    base["by_code_recent_count"] = recent_count
    base["adaptive_tightness"] = tightness
    base["recent_hit_rate_at_10"] = recent_hit
    base["recent_pred_high_precision"] = recent_prec
    base["recent_miss_streak_days"] = streak
    base["feedback_as_of"] = as_of.isoformat()
    fn_ind, fn_tag_top, fp_tag_top = _load_reflection_weights(as_of=as_of)
    base["fn_industry_weights"] = fn_ind
    base["fn_miss_tag_top"] = fn_tag_top
    base["fp_miss_tag_top"] = fp_tag_top
    return base


def should_block_code_fast(
    code: str,
    mention: float,
    feedback_ctx: dict[str, object] | None,
) -> bool:
    """최근 창 오판·누적 chronic miss — 종목명 직접 언급 없으면 후보 제외."""
    if mention >= float(config.PRED_MENTION_GATE_MIN):
        return False
    if not feedback_ctx:
        return False
    code_key = str(code).zfill(6)
    if config.PRED_CHRONIC_MISS_BLOCK_ENABLED:
        by_mean = feedback_ctx.get("by_code_mean_ratio")
        by_count = feedback_ctx.get("by_code_count")
        if isinstance(by_mean, dict) and isinstance(by_count, dict):
            n = int(by_count.get(code_key, 0) or 0)
            mean = by_mean.get(code_key)
            if (
                n >= int(config.PRED_CHRONIC_MISS_MIN_SAMPLES)
                and isinstance(mean, (int, float))
                and float(mean) < float(config.PRED_CHRONIC_MISS_RATIO_MAX)
            ):
                return True
    recent_mean_map = feedback_ctx.get("by_code_recent_mean_ratio")
    recent_count_map = feedback_ctx.get("by_code_recent_count")
    if isinstance(recent_mean_map, dict) and isinstance(recent_count_map, dict):
        rn = int(recent_count_map.get(code_key, 0) or 0)
        rmean = recent_mean_map.get(code_key)
        if (
            rn >= int(config.PRED_CHRONIC_MISS_RECENT_MIN_SAMPLES)
            and isinstance(rmean, (int, float))
            and float(rmean) < float(config.PRED_CHRONIC_MISS_RECENT_RATIO_MAX)
        ):
            return True
    return False


def feedback_rank_penalty(row: Any, feedback_ctx: dict[str, object] | None) -> float:
    """0~PRED_FEEDBACK_RANK_PENALTY_MAX — ``rank_score`` 에 곱해 뺄 비율."""
    if not config.PRED_FEEDBACK_ADAPTIVE_ENABLED or not feedback_ctx:
        return 0.0
    code = str(getattr(row, "code", "") or "").zfill(6)
    max_p = float(config.PRED_FEEDBACK_RANK_PENALTY_MAX)
    tightness = float(feedback_ctx.get("adaptive_tightness", 1.0) or 1.0)
    boost = (1.0 - tightness) * 0.5 + 0.5

    penalty = 0.0
    recent_mean_map = feedback_ctx.get("by_code_recent_mean_ratio")
    recent_count_map = feedback_ctx.get("by_code_recent_count")
    if isinstance(recent_mean_map, dict) and isinstance(recent_count_map, dict):
        rn = int(recent_count_map.get(code, 0) or 0)
        rmean = recent_mean_map.get(code)
        if rn >= 2 and isinstance(rmean, (int, float)):
            penalty = max(penalty, (1.0 - float(rmean)) * max_p * boost)

    by_mean = feedback_ctx.get("by_code_mean_ratio")
    by_count = feedback_ctx.get("by_code_count")
    if isinstance(by_mean, dict) and isinstance(by_count, dict):
        n = int(by_count.get(code, 0) or 0)
        mean = by_mean.get(code)
        if n >= int(config.PRED_ERROR_FEEDBACK_MIN_SAMPLES) and isinstance(mean, (int, float)):
            penalty = max(penalty, (1.0 - float(mean)) * max_p * 0.65 * boost)

    stats = feedback_ctx.get("signal_bucket_stats")
    if isinstance(stats, dict):
        key = prediction_accuracy_cache._signal_bucket_key(
            pred_ret=float(getattr(row, "predicted_return_pct", 0.0) or 0.0),
            keyword_hits=int(getattr(row, "keyword_hits", 0) or 0),
            mention_score=float(getattr(row, "mention_score", 0.0) or 0.0),
        )
        b = stats.get(key)
        if isinstance(b, dict):
            bn = int(b.get("count", 0) or 0)
            br = b.get("mean_ratio")
            if bn >= int(config.PRED_ERROR_FEEDBACK_MIN_SAMPLES) and isinstance(br, (int, float)):
                if float(br) < float(config.PRED_PRECISION_BUCKET_MIN_RATIO):
                    penalty = max(
                        penalty,
                        (float(config.PRED_PRECISION_BUCKET_MIN_RATIO) - float(br))
                        * max_p
                        * 1.2
                        * boost,
                    )
    return float(min(max_p, max(0.0, penalty)))


def stamp_miss_reflection_boosts(
    rows: list[Any],
    feedback_ctx: dict[str, object] | None,
) -> None:
    """행에 ``miss_reflection_boost`` 만 찍음(랭크 재곱셈 없음)."""
    if not rows:
        return
    for row in rows:
        boost = miss_reflection_industry_boost(row, feedback_ctx)
        try:
            setattr(row, "miss_reflection_boost", boost)
        except Exception:
            pass


def apply_feedback_rank_penalties(
    rows: list[Any],
    feedback_ctx: dict[str, object] | None,
) -> None:
    """풀 내 ``rank_score``·``score`` 에 오판 이력 패널티·미포착 업종 가산 반영(in-place)."""
    if not rows or not feedback_ctx:
        return
    for row in rows:
        p = feedback_rank_penalty(row, feedback_ctx)
        boost = miss_reflection_industry_boost(row, feedback_ctx)
        try:
            setattr(row, "miss_reflection_boost", boost)
        except Exception:
            pass
        if p <= 1e-12 and boost <= 1e-12:
            continue
        mult = (1.0 - p) * (1.0 + boost)
        if hasattr(row, "rank_score") and row.rank_score is not None:
            row.rank_score = float(row.rank_score) * mult
        if hasattr(row, "score") and row.score is not None:
            row.score = float(row.score) * mult


def miss_reflection_industry_boost(
    row: Any,
    feedback_ctx: dict[str, object] | None,
) -> float:
    """최근 미포착 급등과 같은 업표면 0~MAX 가산 비율."""
    if not config.PRED_MISS_REFLECTION_ENABLED or not feedback_ctx:
        return 0.0
    weights = feedback_ctx.get("fn_industry_weights")
    if not isinstance(weights, dict) or not weights:
        return 0.0
    code = str(getattr(row, "code", "") or "").zfill(6)
    if not code:
        return 0.0
    try:
        from .. import stocks as stocks_mod

        ic = str(stocks_mod.industry_code_for_stock(code) or "").strip()
    except Exception:
        return 0.0
    if not ic:
        return 0.0
    raw = weights.get(ic)
    if not isinstance(raw, (int, float)) or not math.isfinite(float(raw)):
        return 0.0
    cap = float(config.PRED_MISS_REFLECTION_INDUSTRY_BOOST_MAX)
    return float(min(cap, max(0.0, float(raw))))


def _default_reflection_payload() -> dict[str, Any]:
    return {"version": 1, "days": {}, "updated_at": ""}


def _load_reflection_weights(
    *, as_of: date
) -> tuple[dict[str, float], list[str], list[str]]:
    """최근 창의 FN 업종 가중·태그 요약."""
    empty: tuple[dict[str, float], list[str], list[str]] = ({}, [], [])
    if not config.PRED_MISS_REFLECTION_ENABLED:
        return empty
    if not MISS_REFLECTION_PATH.is_file():
        return empty
    try:
        data = json.loads(MISS_REFLECTION_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty
    days = data.get("days")
    if not isinstance(days, dict):
        return empty
    hl = float(config.PRED_MISS_REFLECTION_HALF_LIFE_DAYS)
    win = int(config.PRED_MISS_REFLECTION_WINDOW_DAYS)
    cutoff = as_of - timedelta(days=max(3, win))
    ind_acc: dict[str, float] = {}
    fn_tags: dict[str, float] = {}
    fp_tags: dict[str, float] = {}
    for t_iso, rec in days.items():
        if not isinstance(rec, dict):
            continue
        try:
            t_d = date.fromisoformat(str(t_iso)[:10])
        except ValueError:
            continue
        # as_of 당일 결과는 아직 미래 정보 — 전일까지만.
        if t_d < cutoff or t_d >= as_of:
            continue
        w = _recency_weight(t_d, as_of=as_of, half_life_days=hl)
        if w <= 0:
            continue
        for ic, v in (rec.get("fn_industries") or {}).items():
            if not isinstance(ic, str) or not ic:
                continue
            if not isinstance(v, (int, float)) or not math.isfinite(float(v)):
                continue
            ind_acc[ic] = ind_acc.get(ic, 0.0) + float(v) * w
        for tag, c in (rec.get("fn_tags") or {}).items():
            if isinstance(tag, str) and isinstance(c, (int, float)):
                fn_tags[tag] = fn_tags.get(tag, 0.0) + float(c) * w
        for tag, c in (rec.get("fp_tags") or {}).items():
            if isinstance(tag, str) and isinstance(c, (int, float)):
                fp_tags[tag] = fp_tags.get(tag, 0.0) + float(c) * w
    if not ind_acc:
        return (
            {},
            [k for k, _ in sorted(fn_tags.items(), key=lambda x: -x[1])[:5]],
            [k for k, _ in sorted(fp_tags.items(), key=lambda x: -x[1])[:5]],
        )
    peak = max(ind_acc.values())
    cap = float(config.PRED_MISS_REFLECTION_INDUSTRY_BOOST_MAX)
    if peak <= 1e-12:
        scaled: dict[str, float] = {}
    else:
        scaled = {
            ic: float(min(cap, (v / peak) * cap))
            for ic, v in ind_acc.items()
            if v > 1e-12
        }
    return (
        scaled,
        [k for k, _ in sorted(fn_tags.items(), key=lambda x: -x[1])[:5]],
        [k for k, _ in sorted(fp_tags.items(), key=lambda x: -x[1])[:5]],
    )


def append_miss_reflection_lessons(dr: Any) -> None:
    """장 마감 확정일의 FN/FP 진단을 업종·태그 교훈으로 저장."""
    if not config.PRED_MISS_REFLECTION_ENABLED:
        return
    if getattr(dr, "forward_observation", False):
        return
    from ..learning import support as miss_diag
    from .. import stocks as stocks_mod

    missed, pred_misses = miss_diag.build_miss_rows_for_day(dr)
    if not missed and not pred_misses:
        return
    t_iso = dr.trading_day.isoformat()
    fn_ind: dict[str, float] = {}
    fn_ind_labels: dict[str, str] = {}
    fn_tags: dict[str, int] = {}
    fp_tags: dict[str, int] = {}
    fn_names: list[str] = []
    for m in missed:
        code = str(m.get("code") or "").zfill(6)
        if not code:
            continue
        try:
            ic = str(stocks_mod.industry_code_for_stock(code) or "").strip()
        except Exception:
            ic = ""
        ap = float(m.get("actual_pct") or 0.0)
        # 급등 폭이 클수록 업종 교훈 가중 ↑ (20%→1.0, 30%→1.5)
        w = 1.0 + max(0.0, min(1.5, (ap - 20.0) / 20.0))
        if ic:
            fn_ind[ic] = fn_ind.get(ic, 0.0) + w
            if ic not in fn_ind_labels:
                try:
                    fn_ind_labels[ic] = (
                        stocks_mod.industry_name_for_code(code) or ic
                    )
                except Exception:
                    fn_ind_labels[ic] = ic
        for t in m.get("tags") or []:
            if isinstance(t, str) and t:
                fn_tags[t] = fn_tags.get(t, 0) + 1
        name = str(m.get("name") or code)
        if name and len(fn_names) < 6:
            fn_names.append(name)
    for m in pred_misses:
        for t in m.get("tags") or []:
            if isinstance(t, str) and t:
                fp_tags[t] = fp_tags.get(t, 0) + 1

    path = MISS_REFLECTION_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = _default_reflection_payload()
    else:
        data = _default_reflection_payload()
    days = data.get("days")
    if not isinstance(days, dict):
        days = {}
    days[t_iso] = {
        "fn_industries": {k: round(v, 4) for k, v in fn_ind.items()},
        "fn_tags": fn_tags,
        "fp_tags": fp_tags,
        "fn_count": len(missed),
        "fp_count": len(pred_misses),
        "fn_sample": fn_names,
    }
    # 오래된 일 정리
    keep_after = dr.trading_day - timedelta(days=max(30, int(config.PRED_MISS_REFLECTION_WINDOW_DAYS) * 3))
    pruned = {}
    for k, v in days.items():
        try:
            kd = date.fromisoformat(str(k)[:10])
        except ValueError:
            continue
        if kd >= keep_after:
            pruned[k] = v
    data["days"] = pruned
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8")

    top_ind = sorted(fn_ind.items(), key=lambda x: -x[1])[:4]
    ind_labels = [fn_ind_labels.get(ic, ic) for ic, _w in top_ind]
    print(
        f"오판 반성 기록: T={t_iso} 미포착={len(missed)} 오판={len(pred_misses)}"
        + (f" → 업종가산 {', '.join(ind_labels)}" if ind_labels else "")
        + (
            f" FN태그={','.join(list(fn_tags.keys())[:3])}"
            if fn_tags
            else ""
        ),
        flush=True,
    )


def adaptive_feedback_shrink_bounds(
    feedback_ctx: dict[str, object] | None,
) -> tuple[float, float]:
    """(min_mult, max_mult) — ``_feedback_calibrated_return`` 용."""
    lo, hi = 0.78, 1.02
    if not feedback_ctx or not config.PRED_FEEDBACK_ADAPTIVE_ENABLED:
        return lo, hi
    tightness = float(feedback_ctx.get("adaptive_tightness", 1.0) or 1.0)
    streak = int(feedback_ctx.get("recent_miss_streak_days", 0) or 0)
    if tightness < 0.65 or streak >= 3:
        return 0.58, 0.95
    if tightness < 0.80 or streak >= 2:
        return 0.65, 0.98
    if tightness < 0.90:
        return 0.72, 1.0
    return lo, hi


def _default_incremental_miss() -> dict[str, list]:
    return {"version": 1, "pos_boost": [], "neg_boost": [], "updated_at": ""}


def append_incremental_miss_from_day_report(dr: Any) -> None:
    """장 마감 확정일 miss 진단을 경량 JSON에 누적(ML 부스트용)."""
    append_miss_reflection_lessons(dr)
    if not config.ML_INCREMENTAL_MISS_BOOST_ENABLED:
        return
    if getattr(dr, "forward_observation", False):
        return
    from ..learning import support as miss_diag

    missed, pred_misses = miss_diag.build_miss_rows_for_day(dr)
    if not missed and not pred_misses:
        return
    t_iso = dr.trading_day.isoformat()
    path = INCREMENTAL_MISS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = _default_incremental_miss()
    else:
        data = _default_incremental_miss()
    pos: set[tuple[str, str]] = {
        tuple(x) for x in (data.get("pos_boost") or []) if isinstance(x, list) and len(x) == 2
    }
    neg: set[tuple[str, str]] = {
        tuple(x) for x in (data.get("neg_boost") or []) if isinstance(x, list) and len(x) == 2
    }
    for m in missed:
        tags = set(m.get("tags") or [])
        code = str(m.get("code") or "").zfill(6)
        if code and tags & {"not_in_top_predictions", "pred_below_threshold", "weak_keyword_hits"}:
            pos.add((t_iso, code))
    for m in pred_misses:
        tags = set(m.get("tags") or [])
        code = str(m.get("code") or "").zfill(6)
        if code and tags & {
            "actual_negative",
            "large_overprediction",
            "ml_high_keyword_weak",
            "high_tier_weak_followthrough",
        }:
            neg.add((t_iso, code))
    cap = int(config.ML_INCREMENTAL_MISS_MAX_ENTRIES)
    pos_list = sorted(pos)[-cap:]
    neg_list = sorted(neg)[-cap:]
    data["pos_boost"] = [list(x) for x in pos_list]
    data["neg_boost"] = [list(x) for x in neg_list]
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    path.write_text(json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8")


def load_incremental_miss_boost() -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """경량 miss JSON → (pos, neg) ``(day_iso, code6)`` 집합."""
    pos: set[tuple[str, str]] = set()
    neg: set[tuple[str, str]] = set()
    if not INCREMENTAL_MISS_PATH.is_file():
        return pos, neg
    try:
        data = json.loads(INCREMENTAL_MISS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return pos, neg
    for key, target in (("pos_boost", pos), ("neg_boost", neg)):
        raw = data.get(key)
        if not isinstance(raw, list):
            continue
        for item in raw:
            if isinstance(item, list) and len(item) == 2:
                target.add((str(item[0])[:10], str(item[1]).zfill(6)))
    return pos, neg
