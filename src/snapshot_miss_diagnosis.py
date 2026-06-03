"""
``--rebuild-train-snapshot`` 구간 병합 시 ``rebuild_learning`` 일자별 레코드에 붙는
미예측 급등·고예측 실패 진단.

- **missed_big_movers_today**: 실제 ≥ 임계 급등인데 ``pred_high`` 가 아닌 종목(후보 밖·임계 미만 등).
- **pred_high_misses_today**: ``pred_high`` 였으나 실제 < 임계(빗나감) — 음수·저수익·지연뉴스·약신호 태그.

``ml_move_rank`` 는 스냅샷에 쌓인 진단으로 급등(pos)·오판(neg) 행을 소폭 복제해 재학습 시 가중치를 준다.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from . import config
from .report import DayReport


def _pct_from_actual_ratio(ar: object) -> float | None:
    """비율 형태 실제 수익률(예: 0.215)을 퍼센트(21.5)로 변환."""
    if ar is None:
        return None
    try:
        return float(ar) * 100.0
    except (TypeError, ValueError):
        return None


def _tags_for_missed(
    *,
    code: str,
    pred_row: object | None,
    row: dict[str, Any],
    thr_pct: float,
) -> tuple[list[str], list[str], float | None, int | None]:
    """
    Returns:
        tags, hints_ko, pred_pct_if_in_list, keyword_hits_if_in_list
    """
    tags: list[str] = []
    hints: list[str] = []
    pred_pct: float | None = None
    kw_hits: int | None = None
    if pred_row is None:
        tags.append("not_in_top_predictions")
        hints.append("당일 상위 예측 후보(top_n)에 포함되지 않음 — 키워드·종목명·ML 점수가 상위에 못 미침")
        return tags, hints, None, None
    try:
        pred_pct = float(getattr(pred_row, "predicted_return_pct", float("nan")))
        kw_hits = int(getattr(pred_row, "keyword_hits", 0) or 0)
    except (TypeError, ValueError):
        pred_pct = None
        kw_hits = None
    if pred_pct is not None and pred_pct + 1e-9 < thr_pct:
        tags.append("pred_below_threshold")
        hints.append(
            f"후보에는 들었으나 표시 예측 상승률이 {thr_pct:.0f}% 미만 — 신호가 약하거나 보정·랭킹에서 밀림"
        )
    if kw_hits is not None and kw_hits <= 1:
        tags.append("weak_keyword_hits")
        hints.append("과거 급등 뉴스 키워드와 당일 뉴스 교집합이 1 이하 — 프로필 매칭 부족")
    try:
        mention = float(getattr(pred_row, "mention_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        mention = 0.0
    if mention < 0.10:
        tags.append("low_name_mention")
        hints.append("당일 뉴스에서 종목명 언급이 거의 없음")
    kws = row.get("keywords") or []
    if isinstance(kws, list) and not kws:
        tags.append("no_row_keywords")
        hints.append("표 행에 일치 키워드가 비어 있음(후보 밖이거나 매칭 실패)")
    if not tags:
        tags.append("other")
        hints.append("기타 — 표에서 pred_high가 아니나 후보 리스트에는 있음(표시 임계·필터 불일치 등)")
    return tags, hints, pred_pct, kw_hits


def _tags_for_pred_miss(
    *,
    row: dict[str, Any],
    thr_pct: float,
    actual_pct: float,
) -> tuple[list[str], list[str]]:
    """고예측·실제 미달 행에 대한 원인 태그·한글 힌트 목록."""
    tags: list[str] = []
    hints: list[str] = []
    if actual_pct < 0:
        tags.append("actual_negative")
        hints.append("실제 수익률이 음수 — 예측과 방향이 크게 어긋남")
    elif actual_pct + 1e-9 < thr_pct:
        tags.append("actual_below_threshold")
        hints.append(f"실제 상승률이 {thr_pct:.0f}% 미만 — 급등 조건 미달")
    if bool(row.get("late_news_hit")):
        tags.append("late_news_keyword_overlap")
        hints.append("N-1 컷오프 이후 뉴스에 예측 키워드가 겹침(참고·인과 단정 아님)")
    if int(row.get("keyword_hits", 0) or 0) <= 1 or float(row.get("mention_score", 0.0) or 0.0) < 0.10:
        tags.append("weak_signal_at_prediction")
        hints.append("예측 시점 키워드/종목명 신호가 약했음")
    pr = row.get("pred_ret")
    if isinstance(pr, (int, float)) and actual_pct + 1e-9 < thr_pct:
        gap = float(pr) - actual_pct
        if gap > 8.0:
            tags.append("large_overprediction")
            hints.append("예측이 실제보다 8%p 이상 높게 나감 — 과대 예측 구간")
    if not tags:
        tags.append("pred_miss_other")
        hints.append("고예측이었으나 실제 급등 미달(기타)")
    return tags, hints


def build_miss_rows_for_day(dr: DayReport) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """단일 ``DayReport`` 에 대한 진단 행 두 목록."""
    thr = float(config.BIG_MOVE_THRESHOLD)
    thr_pct = thr * 100.0
    pred_by_code = {str(getattr(p, "code", "")).zfill(6): p for p in (dr.predictions or [])}

    missed: list[dict[str, Any]] = []
    pred_misses: list[dict[str, Any]] = []

    for r in dr.rows_compare or []:
        code = str(r.get("code", "")).zfill(6)
        if not code:
            continue
        ar = r.get("actual_ret")
        actual_pct = _pct_from_actual_ratio(ar)
        if actual_pct is None:
            continue
        pred_high = bool(r.get("pred_high"))
        actual_big = bool(r.get("actual_big")) or (ar is not None and float(ar) >= thr - 1e-12)

        if actual_big and not pred_high:
            pr_obj = pred_by_code.get(code)
            tags, hints, in_list_pct, kh = _tags_for_missed(
                code=code, pred_row=pr_obj, row=r, thr_pct=thr_pct
            )
            missed.append(
                {
                    "code": code,
                    "name": str(r.get("name") or ""),
                    "actual_pct": round(actual_pct, 3),
                    "tags": tags,
                    "hints_ko": hints,
                    "pred_in_list_pct": round(in_list_pct, 3) if in_list_pct is not None else None,
                    "keyword_hits_if_in_list": kh,
                }
            )
        if pred_high and actual_pct + 1e-9 < thr_pct:
            tags, hints = _tags_for_pred_miss(row=r, thr_pct=thr_pct, actual_pct=actual_pct)
            pr = r.get("pred_ret")
            pred_misses.append(
                {
                    "code": code,
                    "name": str(r.get("name") or ""),
                    "pred_pct": round(float(pr), 3) if isinstance(pr, (int, float)) else None,
                    "actual_pct": round(actual_pct, 3),
                    "tags": tags,
                    "hints_ko": hints,
                    "keywords_sample": [str(x) for x in (r.get("keywords") or [])[:10] if x],
                    "late_news_hit": bool(r.get("late_news_hit"))
                    if r.get("late_news_hit") is not None
                    else None,
                }
            )

    return missed[:50], pred_misses[:50]


def recompute_miss_summary_from_daily(daily: list[dict[str, Any]]) -> dict[str, Any]:
    """병합된 ``daily`` 로부터 누적 요약(스냅샷·로그용)."""
    n_missed = 0
    n_pred_miss = 0
    tag_c: Counter[str] = Counter()
    for row in daily:
        for m in row.get("missed_big_movers_today") or []:
            if not isinstance(m, dict):
                continue
            n_missed += 1
            for t in m.get("tags") or []:
                if isinstance(t, str) and t:
                    tag_c[f"missed:{t}"] += 1
        for m in row.get("pred_high_misses_today") or []:
            if not isinstance(m, dict):
                continue
            n_pred_miss += 1
            for t in m.get("tags") or []:
                if isinstance(t, str) and t:
                    tag_c[f"pred_miss:{t}"] += 1
    return {
        "miss_diagnosis_missed_big_mover_events": n_missed,
        "miss_diagnosis_pred_high_miss_events": n_pred_miss,
        "miss_diagnosis_tag_counts": [
            {"tag": k, "count": int(v)} for k, v in tag_c.most_common(48)
        ],
    }


def load_ml_boost_sets_from_snapshot(path: Any | None = None) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """
    ``(trading_day_iso, code6)`` 집합 두 개:
    - pos_boost: 미예측 급등 중 ``not_in_top_predictions`` 또는 ``pred_below_threshold`` 태그
    - neg_boost: 고예측 실패 중 ``actual_negative`` 또는 ``large_overprediction`` 태그
    """
    import json
    from pathlib import Path

    from . import config as _cfg

    p = Path(path) if path is not None else _cfg.TRAIN_SNAPSHOT_PATH
    pos: set[tuple[str, str]] = set()
    neg: set[tuple[str, str]] = set()
    if not p.is_file():
        return pos, neg
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return pos, neg
    rl = data.get("rebuild_learning")
    if not isinstance(rl, dict):
        return pos, neg
    for drow in rl.get("daily") or []:
        if not isinstance(drow, dict):
            continue
        t_iso = str(drow.get("trading_day") or "")
        if len(t_iso) < 10:
            continue
        for m in drow.get("missed_big_movers_today") or []:
            if not isinstance(m, dict):
                continue
            code = str(m.get("code") or "").zfill(6)
            tags = set(m.get("tags") or [])
            if not code:
                continue
            if tags & {"not_in_top_predictions", "pred_below_threshold"}:
                pos.add((t_iso[:10], code))
        for m in drow.get("pred_high_misses_today") or []:
            if not isinstance(m, dict):
                continue
            code = str(m.get("code") or "").zfill(6)
            tags = set(m.get("tags") or [])
            if not code:
                continue
            if tags & {"actual_negative", "large_overprediction"}:
                neg.add((t_iso[:10], code))
    return pos, neg
