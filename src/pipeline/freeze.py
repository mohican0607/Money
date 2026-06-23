"""예측 고정(freeze) 캐시 load/save."""
from __future__ import annotations

import json
import math
from datetime import date

from src import config, predict

def _load_prediction_freeze_payload() -> dict[str, list[dict]]:
    """``prediction_freeze_by_t.json`` 에서 관측일 T별 고정 예측 후보를 읽습니다. 스키마 불일치·손상 시 빈 dict."""
    path = config.PREDICTION_FREEZE_PATH
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    if raw.get("_schema_version") != config.PREDICTION_FREEZE_SCHEMA_VERSION:
        return {}
    out: dict[str, list[dict]] = {}
    for k, v in raw.items():
        if k.startswith("_"):
            continue
        if isinstance(k, str) and isinstance(v, list):
            out[k] = [x for x in v if isinstance(x, dict)]
    return out


def _save_prediction_freeze_payload(payload: dict[str, list[dict]]) -> None:
    """관측일 T별 고정 예측 후보를 ``prediction_freeze_by_t.json`` 에 저장합니다."""
    path = config.PREDICTION_FREEZE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    to_write = {
        "_schema_version": config.PREDICTION_FREEZE_SCHEMA_VERSION,
        **payload,
    }
    path.write_text(json.dumps(to_write, ensure_ascii=False, indent=2), encoding="utf-8")


def _frozen_predicted_return_pct(item: dict) -> float | None:
    """고정 캐시 항목에서 ``predicted_return_pct``(또는 레거시 ``pred_ret``)를 유한 실수로 꺼냅니다."""
    raw = item.get("predicted_return_pct")
    if raw is None:
        raw = item.get("pred_ret")
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _freeze_entry_usable(items: list[dict]) -> bool:
    """관측일 T별 고정 캐시에 재사용할 예측 수익률이 하나라도 있으면 True."""
    if not items:
        return False
    for x in items:
        p = _frozen_predicted_return_pct(x)
        if p is not None and math.isfinite(p):
            return True
    return False


def _ignore_freeze_for_trading_day(
    T: date,
    *,
    train_snapshot_mode: str,
    cal_scope: tuple[date, date] | None,
    respect_prediction_freeze: bool = False,
) -> bool:
    """
    예측 고정 캐시를 쓰지 않고 다시 예측할 관측일 T 인지 판단.

    - ``python main.py From To`` (기본): **freeze 가 있으면 재사용**, 없는 T 만 신규 예측·저장.
      (예: N일 14:30 ``main.py T`` 로 확정한 예측은 이후 구간 실행에서 T 블록에 유지)
    - ``--rebuild-train-snapshot`` + From~To: 구간 안 모든 T 를 freeze 무시·재계산.
    - ``--use-freeze``: ``--rebuild`` 구간에서도 freeze 재사용(명시적).
    - ``--weekly``·단일일 등 ``cal_scope`` 없음: ``rebuild`` 일 때만 freeze 무시.
      ``append_learning`` 단일일(장 마감 후 actual·테마만 갱신)은 freeze 유지.
    """
    if respect_prediction_freeze:
        return False
    if train_snapshot_mode == "rebuild" and cal_scope is None:
        return True
    if cal_scope is None:
        return False
    if train_snapshot_mode == "rebuild":
        s0, s1 = cal_scope
        return s0 <= T <= s1
    return False


def _prediction_rows_from_frozen_items(items: list[dict]) -> list[predict.PredictionRow]:
    """고정 캐시 dict 목록을 ``PredictionRow`` 리스트로 변환합니다. 파싱 실패 항목은 건너뜁니다."""
    rows: list[predict.PredictionRow] = []
    for x in items:
        try:
            pct = _frozen_predicted_return_pct(x)
            if pct is None:
                pct = 0.0
            rows.append(
                predict.PredictionRow(
                    code=str(x.get("code", "")).zfill(6),
                    name=str(x.get("name", "")),
                    score=float(x.get("score", 0.0) or 0.0),
                    predicted_return_pct=pct,
                    matched_keywords=[str(k) for k in (x.get("matched_keywords") or [])],
                    reasons=[str(r) for r in (x.get("reasons") or [])],
                    ml_prob=(
                        float(x["ml_prob"])
                        if x.get("ml_prob") is not None
                        else None
                    ),
                    keyword_hits=int(x.get("keyword_hits", 0) or 0),
                    mention_score=float(x.get("mention_score", 0.0) or 0.0),
                    rank_score=(
                        float(x["rank_score"])
                        if x.get("rank_score") is not None
                        else None
                    ),
                    rank_position=(
                        int(x["rank_position"])
                        if x.get("rank_position") is not None
                        else None
                    ),
                    confidence_tier=str(x.get("confidence_tier") or "none"),
                    investor_flow_score=float(x.get("investor_flow_score", 0.0) or 0.0),
                    foreign_net_vol_ratio=float(x.get("foreign_net_vol_ratio", 0.0) or 0.0),
                )
            )
        except (TypeError, ValueError):
            continue
    return rows


def _prediction_rows_to_frozen_items(rows: list[predict.PredictionRow]) -> list[dict]:
    """``PredictionRow`` 리스트를 고정 캐시 JSON에 넣을 dict 목록으로 직렬화합니다."""
    return [
        {
            "code": str(r.code).zfill(6),
            "name": r.name,
            "score": float(r.score),
            "predicted_return_pct": float(r.predicted_return_pct),
            "matched_keywords": list(r.matched_keywords),
            "reasons": list(r.reasons),
            "ml_prob": (None if r.ml_prob is None else float(r.ml_prob)),
            "keyword_hits": int(getattr(r, "keyword_hits", 0) or 0),
            "mention_score": float(getattr(r, "mention_score", 0.0) or 0.0),
            "rank_score": (
                None if getattr(r, "rank_score", None) is None else float(r.rank_score)
            ),
            "rank_position": (
                None
                if getattr(r, "rank_position", None) is None
                else int(r.rank_position)
            ),
            "confidence_tier": str(getattr(r, "confidence_tier", "none") or "none"),
            "investor_flow_score": float(getattr(r, "investor_flow_score", 0.0) or 0.0),
            "foreign_net_vol_ratio": float(getattr(r, "foreign_net_vol_ratio", 0.0) or 0.0),
        }
        for r in rows
    ]
