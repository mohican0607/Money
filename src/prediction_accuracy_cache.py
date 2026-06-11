"""
관측 거래일 T마다 (실제%÷예측%) 비율·고예측 이력을 종목별로 누적 저장·조회.

- ``t_code_ratio``: ``T:코드`` → 비율 (누적 정확도 평균용).
- ``t_code_actual_pct``: ``T:코드`` → 실제 등락률(% 포인트, 부호 유지). 툴팁 이력과 동기화.
- ``high_pred_by_code``: 종목별로 예측 수익률이 급등 기준(예: 20%) 이상이었던 관측일 목록(리포트 툴팁).
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime
from pathlib import Path

from . import config, trading_calendar
from . import stocks

TRACK_PATH = config.CACHE_DIR / "train" / "prediction_accuracy_track.json"


def track_path_display() -> str:
    """리포트·로그용 상대 경로 문자열."""
    try:
        return str(TRACK_PATH.relative_to(config.ROOT))
    except ValueError:
        return str(TRACK_PATH)


def _ratio(pred_ret: float | None, actual_ret: float | None) -> float | None:
    """``main._actual_over_pred_ratio`` 와 동일: 일별 달성률 ``min(|실제%|/|예측%|,1)``."""
    if pred_ret is None or actual_ret is None:
        return None
    p = abs(float(pred_ret))
    a = float(actual_ret)
    if not math.isfinite(p) or not math.isfinite(a):
        return None
    if abs(p) < 1e-9:
        return None
    raw = abs(a * 100.0) / p
    return min(raw, 1.0)


def _default_payload() -> dict:
    """빈 추적 캐시 페이로드(version 5) 기본 구조를 만듭니다."""
    return {
        "version": 5,
        "t_code_ratio": {},
        "t_code_actual_pct": {},
        "high_pred_by_code": {},
        "mid_pred_by_code": {},
        "all_pred_by_code": {},
        "feedback_bucket_stats": {},
        "keyword_feedback_weights": {},
        "hit_at_k_by_day": {},
    }


_payload_cache: dict | None = None


def _invalidate_payload_cache() -> None:
    """프로세스 내 ``_payload_cache`` 를 비워 다음 ``_load_payload`` 가 디스크를 다시 읽게 합니다."""
    global _payload_cache
    _payload_cache = None


def _load_payload() -> dict:
    """``prediction_accuracy_track.json`` 을 읽어 정규화된 페이로드를 돌려줍니다. 프로세스 내 캐시가 있으면 디스크를 건너뜁니다(``_save_payload`` 시 무효화)."""
    global _payload_cache
    if _payload_cache is not None:
        return _payload_cache
    if not TRACK_PATH.is_file():
        _payload_cache = _default_payload()
        return _payload_cache
    try:
        raw = json.loads(TRACK_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        _payload_cache = _default_payload()
        return _payload_cache
    if not isinstance(raw, dict):
        _payload_cache = _default_payload()
        return _payload_cache
    out = _default_payload()
    tr = raw.get("t_code_ratio")
    if isinstance(tr, dict):
        for k, v in tr.items():
            if (
                isinstance(k, str)
                and isinstance(v, (int, float))
                and math.isfinite(float(v))
            ):
                # 구버전(부호 포함) 캐시도 읽을 때 절대값·달성률 상한 1로 정규화.
                out["t_code_ratio"][k] = min(abs(float(v)), 1.0)
    tap = raw.get("t_code_actual_pct")
    if isinstance(tap, dict):
        for k, v in tap.items():
            if isinstance(k, str) and isinstance(v, (int, float)) and math.isfinite(float(v)):
                out["t_code_actual_pct"][k] = float(v)
    hp = raw.get("high_pred_by_code")
    if isinstance(hp, dict):
        out["high_pred_by_code"] = hp
    mp = raw.get("mid_pred_by_code")
    if isinstance(mp, dict):
        out["mid_pred_by_code"] = mp
    ap = raw.get("all_pred_by_code")
    if isinstance(ap, dict):
        out["all_pred_by_code"] = ap
    fb = raw.get("feedback_bucket_stats")
    if isinstance(fb, dict):
        out["feedback_bucket_stats"] = fb
    kw = raw.get("keyword_feedback_weights")
    if isinstance(kw, dict):
        out["keyword_feedback_weights"] = kw
    hak = raw.get("hit_at_k_by_day")
    if isinstance(hak, dict):
        out["hit_at_k_by_day"] = hak
    _payload_cache = out
    return out


def _save_payload(payload: dict) -> None:
    """페이로드를 디스크에 저장하고 메모리 캐시를 무효화합니다."""
    _invalidate_payload_cache()
    TRACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 5,
        "t_code_ratio": dict(payload.get("t_code_ratio") or {}),
        "t_code_actual_pct": dict(payload.get("t_code_actual_pct") or {}),
        "high_pred_by_code": dict(payload.get("high_pred_by_code") or {}),
        "mid_pred_by_code": dict(payload.get("mid_pred_by_code") or {}),
        "all_pred_by_code": dict(payload.get("all_pred_by_code") or {}),
        "feedback_bucket_stats": dict(payload.get("feedback_bucket_stats") or {}),
        "keyword_feedback_weights": dict(payload.get("keyword_feedback_weights") or {}),
        "hit_at_k_by_day": dict(payload.get("hit_at_k_by_day") or {}),
    }
    TRACK_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def keyword_feedback_weights() -> dict[str, float]:
    """최근 오판 기반으로 학습된 키워드 가중치(가벼운 온라인 피드백)."""
    p = _load_payload()
    raw = p.get("keyword_feedback_weights")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or len(k.strip()) < 2:
            continue
        if not isinstance(v, (int, float)) or not math.isfinite(float(v)):
            continue
        out[k.strip()] = float(v)
    return out


def merge_keyword_feedback_from_day_reports(
    day_reports: list,
    *,
    threshold: float,
) -> None:
    """
    빗나간 예측을 바탕으로 키워드 가중치를 누적 업데이트합니다.

    - False positive(예측≥threshold인데 실제<threshold): 키워드 가중치 하향
    - False negative(예측<threshold인데 실제≥threshold, 단 예측 후보에 키워드가 존재): 키워드 가중치 상향

    이 가중치는 다음 실행의 휴리스틱 스코어에 직접 반영됩니다.
    """
    if not config.KEYWORD_FEEDBACK_ENABLED:
        return
    p = _load_payload()
    w_raw = p.get("keyword_feedback_weights")
    weights: dict[str, float] = dict(w_raw) if isinstance(w_raw, dict) else {}

    thr_pct = float(threshold) * 100.0
    decay = max(0.0, min(0.25, float(config.KEYWORD_FEEDBACK_DECAY)))
    step = max(0.0, min(0.50, float(config.KEYWORD_FEEDBACK_STEP)))
    if decay > 0 and weights:
        for k in list(weights.keys()):
            v = float(weights.get(k, 0.0) or 0.0) * (1.0 - decay)
            if abs(v) < 1e-5:
                weights.pop(k, None)
            else:
                weights[k] = v

    def _keywords_from_row(r: dict) -> list[str]:
        """rows_compare 행에서 2자 이상 키워드 문자열 목록을 추출."""
        kws = r.get("keywords") or []
        out: list[str] = []
        if isinstance(kws, list):
            for x in kws:
                if isinstance(x, str):
                    s = x.strip()
                    if len(s) >= 2:
                        out.append(s)
        return out[:30]

    changed = False
    for dr in day_reports:
        if getattr(dr, "forward_observation", False):
            continue
        for r in getattr(dr, "rows_compare", []) or []:
            pr = r.get("pred_ret")
            ar = r.get("actual_ret")
            if ar is None or not math.isfinite(float(ar)):
                continue
            act_pct = float(ar) * 100.0
            pred_pct = None
            if pr is not None and math.isfinite(float(pr)):
                pred_pct = float(pr)
            kws = _keywords_from_row(r)
            if not kws:
                continue

            is_fp = pred_pct is not None and pred_pct + 1e-9 >= thr_pct and act_pct < thr_pct - 1e-9
            is_fn = pred_pct is not None and pred_pct < thr_pct - 1e-9 and act_pct + 1e-9 >= thr_pct
            if not (is_fp or is_fn):
                continue

            # 오차가 클수록 업데이트 폭을 약간 키움(캡)
            gap = abs((pred_pct or thr_pct) - act_pct)
            mag = min(1.7, 0.6 + gap / 18.0)
            delta = step * mag * (-1.0 if is_fp else 1.0)

            for k in kws:
                cur = float(weights.get(k, 0.0) or 0.0) + float(delta)
                cur = max(-2.0, min(2.0, cur))
                if abs(cur) < 1e-5:
                    if k in weights:
                        weights.pop(k, None)
                        changed = True
                else:
                    if weights.get(k) != cur:
                        weights[k] = cur
                        changed = True

    if changed:
        p["keyword_feedback_weights"] = weights
        _save_payload(p)


def _key(t: object, code: str) -> str:
    """관측일 T와 종목코드를 ``T:code`` 형식 키로 만듭니다."""
    td = getattr(t, "isoformat", lambda: str(t))()
    return f"{td}:{str(code).zfill(6)}"


def _sync_history_actuals_in_payload(p: dict) -> bool:
    """
    이력 맵 항목 중 ``actual_pct`` 가 비어 있고 ``t_code_actual_pct`` 에 값이 있으면 채웁니다.

    예측 전용 실행만으로 이력이 남은 뒤, 다른 날만 재실행해 ``merge_high_pred_history`` 가 해당 T를
    건드리지 못한 경우에도 ``merge_from_day_reports`` 한 번으로 툴팁 실적%를 맞출 수 있습니다.
    """
    act_map = p.get("t_code_actual_pct")
    if not isinstance(act_map, dict) or not act_map:
        return False
    changed = False
    for map_key in ("high_pred_by_code", "mid_pred_by_code", "all_pred_by_code"):
        hp = p.get(map_key)
        if not isinstance(hp, dict):
            continue
        for code_key, lst in hp.items():
            if not isinstance(lst, list):
                continue
            c6 = str(code_key).zfill(6)
            new_lst: list = []
            list_changed = False
            for x in lst:
                if not isinstance(x, dict):
                    new_lst.append(x)
                    continue
                t_iso = str(x.get("t") or "")
                if x.get("actual_pct") is not None or not t_iso:
                    new_lst.append(x)
                    continue
                v = act_map.get(f"{t_iso}:{c6}")
                if isinstance(v, (int, float)) and math.isfinite(float(v)):
                    new_lst.append({**x, "actual_pct": float(v)})
                    list_changed = True
                    changed = True
                else:
                    new_lst.append(x)
            if list_changed:
                hp[code_key] = new_lst
    return changed


def merge_from_day_reports(day_reports: list) -> None:
    """
    ``t_code_ratio``(예측·실적 둘 다 있을 때만)와 ``t_code_actual_pct``(실적만 있어도)를 기록합니다.

    예전에는 ``_ratio`` 가 None이면 행 전체를 건너뛰어, 예측이 없는 급등 행 등에서
    ``t_code_actual_pct`` 가 비어 누적 이력 툴팁이 ``실적 미확정`` 으로 남을 수 있었습니다.
    """
    p = _load_payload()
    data: dict[str, float] = dict(p.get("t_code_ratio") or {})
    act_pct: dict[str, float] = dict(p.get("t_code_actual_pct") or {})
    changed = False
    for dr in day_reports:
        if getattr(dr, "forward_observation", False):
            continue
        t = dr.trading_day
        for r in dr.rows_compare:
            code = str(r.get("code", "")).zfill(6)
            k = _key(t, code)
            ar = r.get("actual_ret")
            if ar is not None and math.isfinite(float(ar)):
                ap = float(ar) * 100.0
                if act_pct.get(k) != ap:
                    act_pct[k] = ap
                    changed = True
            cur = _ratio(r.get("pred_ret"), r.get("actual_ret"))
            if cur is None:
                continue
            if data.get(k) != cur:
                data[k] = cur
                changed = True
    if changed:
        p["t_code_ratio"] = data
        p["t_code_actual_pct"] = act_pct
        _sync_history_actuals_in_payload(p)
        _save_payload(p)
    elif _sync_history_actuals_in_payload(p):
        # 이번 실행에 새 비율은 없어도, 이미 쌓인 실적% 맵으로 이력만 보정
        _save_payload(p)


def merge_pred_history_from_day_reports(
    day_reports: list,
    *,
    min_pred_pct: float,
    max_pred_pct: float | None,
    history_key: str,
) -> None:
    """예측 구간(``min<=pred<max``)에 드는 행을 종목별 이력 맵 ``history_key`` 에 반영."""
    p = _load_payload()
    hist_raw = p.get(history_key)
    hist: dict[str, list] = dict(hist_raw) if isinstance(hist_raw, dict) else {}
    act_map: dict[str, float] = dict(p.get("t_code_actual_pct") or {})
    changed = False
    thr_lo = float(min_pred_pct)
    thr_hi = float(max_pred_pct) if max_pred_pct is not None else None
    for dr in day_reports:
        if getattr(dr, "forward_observation", False):
            continue
        t = dr.trading_day
        t_iso = t.isoformat()
        for r in dr.rows_compare:
            if config.PRED_RANKING_MODE:
                tier = str(r.get("confidence_tier") or "")
                if history_key == "high_pred_by_code" and tier != "high":
                    continue
                if history_key == "mid_pred_by_code" and tier != "mid":
                    continue
                if history_key == "all_pred_by_code" and tier not in ("high", "mid", "low"):
                    continue
            pr = r.get("pred_ret")
            if pr is None:
                continue
            prf = float(pr)
            if not config.PRED_RANKING_MODE:
                if prf + 1e-9 < thr_lo:
                    continue
                if thr_hi is not None and not (prf < thr_hi - 1e-9):
                    continue
            code = str(r.get("code", "")).zfill(6)
            k = _key(t, code)
            prev_row = next(
                (
                    x
                    for x in hist.get(code, [])
                    if isinstance(x, dict) and x.get("t") == t_iso
                ),
                None,
            )
            ar = r.get("actual_ret")
            if ar is not None and math.isfinite(float(ar)):
                actual_pct = float(ar) * 100.0
            elif prev_row is not None and prev_row.get("actual_pct") is not None:
                actual_pct = float(prev_row["actual_pct"])
            else:
                cached = act_map.get(k)
                if isinstance(cached, (int, float)) and math.isfinite(float(cached)):
                    actual_pct = float(cached)
                else:
                    intr = r.get("actual_ret_intraday_pct")
                    if (
                        intr is not None
                        and math.isfinite(float(intr))
                        and trading_calendar.is_krx_daily_bar_effective_closed(t)
                    ):
                        actual_pct = float(intr)
                    else:
                        actual_pct = None
            entry = {
                "t": t_iso,
                "pred_pct": prf,
                "actual_pct": actual_pct,
            }
            lst = [x for x in hist.get(code, []) if isinstance(x, dict) and x.get("t") != t_iso]
            lst.append(entry)
            lst.sort(key=lambda x: str(x.get("t", "")), reverse=True)
            hist[code] = lst
            changed = True
    if changed:
        p[history_key] = hist
        _sync_history_actuals_in_payload(p)
        _save_payload(p)
    elif _sync_history_actuals_in_payload(p):
        _save_payload(p)


def merge_high_pred_history_from_day_reports(
    day_reports: list, *, threshold_pct: float
) -> None:
    """예측 수익률이 ``threshold_pct`` 이상인 행을 종목별 이력에 반영."""
    merge_pred_history_from_day_reports(
        day_reports,
        min_pred_pct=float(threshold_pct),
        max_pred_pct=None,
        history_key="high_pred_by_code",
    )


def merge_mid_pred_history_from_day_reports(
    day_reports: list, *, low_pct: float, high_pct: float
) -> None:
    """예측 수익률이 ``[low_pct, high_pct)`` 인 행을 종목별 이력에 반영."""
    merge_pred_history_from_day_reports(
        day_reports,
        min_pred_pct=float(low_pct),
        max_pred_pct=float(high_pct),
        history_key="mid_pred_by_code",
    )


def merge_all_pred_history_from_day_reports(
    day_reports: list, *, min_pct: float
) -> None:
    """예측 수익률이 ``min_pct`` 이상인 행을 종목별 이력에 반영."""
    merge_pred_history_from_day_reports(
        day_reports,
        min_pred_pct=float(min_pct),
        max_pred_pct=None,
        history_key="all_pred_by_code",
    )


def _parse_hist_t_iso(t_raw: object) -> date | None:
    """이력 항목 ``t`` ISO 문자열을 ``date`` 로 파싱(실패 시 None)."""
    if not t_raw:
        return None
    parts = str(t_raw).strip().split("-")
    if len(parts) != 3:
        return None
    try:
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except ValueError:
        return None


def _backfill_closed_high_pred_actuals_from_market(day_reports: list) -> None:
    """
    장 마감이 지난 관측일 T에 대해 이력 ``actual_pct`` 가 비어 있으면 pykrx로 조회해
    ``t_code_actual_pct`` 에 넣고, ``high_pred_by_code`` 를 동기화·저장합니다.

    이번 실행에 해당 T가 ``day_reports`` 에 없어도 캐시 이력만으로 누락을 보정합니다.
    """
    needs: dict[date, set[str]] = {}
    for dr in day_reports:
        for r in dr.rows_compare:
            code = str(r.get("code", "")).zfill(6)
            if not code:
                continue
            for h in r.get("pred_high_history") or []:
                if not isinstance(h, dict) or h.get("actual_pct") is not None:
                    continue
                t_d = _parse_hist_t_iso(h.get("t"))
                if t_d is None or not trading_calendar.is_krx_daily_bar_effective_closed(t_d):
                    continue
                needs.setdefault(t_d, set()).add(code)
    if not needs:
        return
    p = _load_payload()
    act_map: dict[str, float] = dict(p.get("t_code_actual_pct") or {})
    changed = False
    for t_d, codes in needs.items():
        m = stocks.try_krx_change_pct_by_code(t_d)
        if not m:
            m = stocks.try_krx_change_pct_for_codes_direct(t_d, sorted(codes)) or {}
        for c in codes:
            pct = m.get(c)
            if pct is None or not math.isfinite(float(pct)):
                continue
            k = f"{t_d.isoformat()}:{c}"
            fv = float(pct)
            if act_map.get(k) != fv:
                act_map[k] = fv
                changed = True
    if not changed:
        return
    p["t_code_actual_pct"] = act_map
    if _sync_history_actuals_in_payload(p):
        changed = True
    if changed:
        _save_payload(p)


def _pred_history_for_code_from_payload(
    payload: dict, code: str, *, history_key: str
) -> list[dict]:
    """``_load_payload()`` 결과를 재사용해 종목별 예측 이력을 만듭니다."""
    c = str(code).zfill(6)
    act_map: dict[str, float] = dict(payload.get("t_code_actual_pct") or {})
    hist_obj = payload.get(history_key)
    if not isinstance(hist_obj, dict):
        return []
    lst = hist_obj.get(c, [])
    if not isinstance(lst, list):
        return []
    out: list[dict] = []
    for x in lst:
        if not isinstance(x, dict):
            continue
        h = dict(x)
        t_iso = str(h.get("t") or "")
        if h.get("actual_pct") is None and t_iso:
            k = f"{t_iso}:{c}"
            v = act_map.get(k)
            if isinstance(v, (int, float)) and math.isfinite(float(v)):
                h["actual_pct"] = float(v)
        out.append(h)
    return out


def _pred_history_for_code(code: str, *, history_key: str) -> list[dict]:
    """관측일 내림차순 ``[{t, pred_pct, actual_pct}, ...]``.

    ``high_pred_by_code`` 에 ``actual_pct`` 가 비어 있어도 ``t_code_actual_pct``(관측일·코드별 실제%)가
    있으면 툴팁에 실제%를 채웁니다(예측 전용 실행 후 전체 재생성 시 이력만 남는 경우).
    """
    return _pred_history_for_code_from_payload(_load_payload(), code, history_key=history_key)


def high_pred_history_for_code(code: str) -> list[dict]:
    """급등 기준(예: 20%) 이상 예측 이력을 관측일 내림차순으로 돌려줍니다."""
    return _pred_history_for_code(code, history_key="high_pred_by_code")


def mid_pred_history_for_code(code: str) -> list[dict]:
    """10~20% 구간 예측 이력을 관측일 내림차순으로 돌려줍니다."""
    return _pred_history_for_code(code, history_key="mid_pred_by_code")


def all_pred_history_for_code(code: str) -> list[dict]:
    """10% 이상 예측 이력(고·중 구간 포함)을 관측일 내림차순으로 돌려줍니다."""
    return _pred_history_for_code(code, history_key="all_pred_by_code")


def _patch_today_open_session_intraday_in_histories(day_reports: list) -> None:
    """
    KST 기준 당일·장 마감 전 관측 블록에서, 표 행에 채워진 장중 등락률을
    ``pred_high_history`` 의 **당일 T** 항목 ``actual_pct`` 에 반영합니다.

    캐시 이력만 읽을 때는 당일 ``actual_pct`` 가 비어 툴팁이 ``실적 미확정`` 으로
    남는 문제를 막습니다(표의 ``— (xx%)`` 와 숫자 일치).
    """
    now_kst = datetime.now(trading_calendar.KST)
    today = now_kst.date()
    for dr in day_reports:
        d = dr.trading_day
        if d != today or not trading_calendar.is_trading_day(d):
            continue
        if trading_calendar.is_krx_daily_bar_effective_closed(d, now_kst=now_kst):
            continue
        t_iso = d.isoformat()
        for r in dr.rows_compare:
            intr = r.get("actual_ret_intraday_pct")
            if intr is None or not math.isfinite(float(intr)):
                continue
            iv = float(intr)
            hist = r.get("pred_high_history")
            if not isinstance(hist, list):
                continue
            new_hist: list = []
            for item in hist:
                if not isinstance(item, dict):
                    new_hist.append(item)
                    continue
                h = dict(item)
                if str(h.get("t") or "") == t_iso and h.get("actual_pct") is None:
                    h["actual_pct"] = iv
                new_hist.append(h)
            r["pred_high_history"] = new_hist


def _mean_ratio_by_code_from_payload(payload: dict) -> dict[str, float]:
    """``t_code_ratio`` 키 ``T:code`` 를 종목별 평균 달성률로 집계."""
    sums: dict[str, float] = {}
    counts: dict[str, int] = {}
    for k, v in (payload.get("t_code_ratio") or {}).items():
        if not isinstance(k, str) or ":" not in k or not isinstance(v, (int, float)):
            continue
        _t, code = k.rsplit(":", 1)
        c = str(code).zfill(6)
        fv = min(float(v), 1.0)
        sums[c] = sums.get(c, 0.0) + fv
        counts[c] = counts.get(c, 0) + 1
    return {c: sums[c] / counts[c] for c in sums if counts.get(c)}


def enrich_rows_pred_high_history(day_reports: list) -> None:
    """각 ``rows_compare`` 행에 구간별 예측 이력 리스트를 붙입니다."""
    payload = _load_payload()
    for dr in day_reports:
        for r in dr.rows_compare:
            code = str(r.get("code", "")).zfill(6)
            r["pred_high_history"] = _pred_history_for_code_from_payload(
                payload, code, history_key="high_pred_by_code"
            )
            r["pred_mid_history"] = _pred_history_for_code_from_payload(
                payload, code, history_key="mid_pred_by_code"
            )
            r["pred_all_history"] = _pred_history_for_code_from_payload(
                payload, code, history_key="all_pred_by_code"
            )
    _patch_today_open_session_intraday_in_histories(day_reports)
    _backfill_closed_high_pred_actuals_from_market(day_reports)
    payload = _load_payload()
    for dr in day_reports:
        for r in dr.rows_compare:
            code = str(r.get("code", "")).zfill(6)
            r["pred_high_history"] = _pred_history_for_code_from_payload(
                payload, code, history_key="high_pred_by_code"
            )
            r["pred_mid_history"] = _pred_history_for_code_from_payload(
                payload, code, history_key="mid_pred_by_code"
            )
            r["pred_all_history"] = _pred_history_for_code_from_payload(
                payload, code, history_key="all_pred_by_code"
            )


def mean_ratio_for_code(code: str) -> float | None:
    """캐시에 있는 해당 종목의 모든 관측일 비율 산술 평균."""
    c = str(code).zfill(6)
    return _mean_ratio_by_code_from_payload(_load_payload()).get(c)


def export_gap_rollup_for_calendar_range(s0: date, s1: date) -> dict[str, object]:
    """
    ``--rebuild-train-snapshot`` 구간 병합용: 구간 내 관측일 T의 달성률·버킷 통계 스냅샷.

    ``train_snapshot`` JSON 의 ``prediction_gap_rollup`` 에 넣습니다.
    """
    p = _load_payload()
    ratios_raw = p.get("t_code_ratio") or {}
    ratios_in: dict[str, float] = {}
    vals: list[float] = []
    if isinstance(ratios_raw, dict):
        for k, v in ratios_raw.items():
            if not isinstance(k, str) or ":" not in k:
                continue
            t_s, _code = k.rsplit(":", 1)
            try:
                t_d = date.fromisoformat(t_s)
            except ValueError:
                continue
            if s0 <= t_d <= s1 and isinstance(v, (int, float)) and math.isfinite(float(v)):
                ratios_in[k] = min(abs(float(v)), 1.0)
                vals.append(ratios_in[k])
    fb = p.get("feedback_bucket_stats") if isinstance(p.get("feedback_bucket_stats"), dict) else {}
    fb_summary: list[dict[str, object]] = []
    for bkey, rec in fb.items():
        if not isinstance(rec, dict):
            continue
        c = int(rec.get("count", 0) or 0)
        if c <= 0:
            continue
        sr = rec.get("sum_ratio")
        if not isinstance(sr, (int, float)):
            continue
        fb_summary.append(
            {
                "bucket": str(bkey),
                "count": c,
                "mean_ratio": round(float(sr) / float(c), 6),
                "mean_abs_gap_pct": round(
                    float(rec.get("sum_abs_gap", 0.0) or 0.0) / float(c), 4,
                ),
            }
        )
    fb_summary.sort(key=lambda x: -int(x.get("count", 0)))
    mean_ratio = sum(vals) / len(vals) if vals else None
    return {
        "calendar_from": s0.isoformat(),
        "calendar_to": s1.isoformat(),
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "t_code_ratio_keys_in_range": len(ratios_in),
        "mean_achievement_ratio_in_range": round(mean_ratio, 6) if mean_ratio is not None else None,
        "t_code_ratio_slice": ratios_in,
        "feedback_bucket_top": fb_summary[:40],
        "feedback_bucket_stats_full": fb,
    }


def build_feedback_context() -> dict[str, object]:
    """
    예측 보정용 오차 요약 컨텍스트를 만듭니다.

    ``t_code_ratio``(= min(|실제%|/|예측%|, 1))를 종목별/전체로 집계해
    추론 시점의 예측 수익률 보정에 사용합니다.
    """
    payload = _load_payload()
    raw = payload.get("t_code_ratio")
    if not isinstance(raw, dict):
        return {
            "global_mean_ratio": None,
            "global_count": 0,
            "by_code_mean_ratio": {},
            "by_code_count": {},
        }
    by_code_vals: dict[str, list[float]] = {}
    all_vals: list[float] = []
    for k, v in raw.items():
        if not isinstance(k, str) or ":" not in k:
            continue
        if not isinstance(v, (int, float)) or not math.isfinite(float(v)):
            continue
        code = k.rsplit(":", 1)[-1].zfill(6)
        rv = min(max(abs(float(v)), 0.0), 1.0)
        by_code_vals.setdefault(code, []).append(rv)
        all_vals.append(rv)
    by_code_mean = {
        c: (sum(xs) / len(xs)) for c, xs in by_code_vals.items() if xs
    }
    by_code_count = {c: len(xs) for c, xs in by_code_vals.items()}
    global_mean = (sum(all_vals) / len(all_vals)) if all_vals else None
    return {
        "global_mean_ratio": global_mean,
        "global_count": len(all_vals),
        "by_code_mean_ratio": by_code_mean,
        "by_code_count": by_code_count,
        "signal_bucket_stats": _signal_bucket_mean_stats(payload),
    }


def _signal_bucket_key(*, pred_ret: float, keyword_hits: int, mention_score: float) -> str:
    """예측%·키워드 hit·mention 구간을 묶은 피드백 버킷 키."""
    hit_band = "h0" if keyword_hits <= 0 else ("h1_2" if keyword_hits <= 2 else ("h3_5" if keyword_hits <= 5 else "h6p"))
    m = max(0.0, min(1.0, float(mention_score)))
    mention_band = "m0" if m < 0.08 else ("m1" if m < 0.25 else ("m2" if m < 0.45 else "m3"))
    p = float(pred_ret)
    pred_band = "p10_15" if p < 15.0 else ("p15_20" if p < 20.0 else ("p20_25" if p < 25.0 else "p25p"))
    return f"{hit_band}|{mention_band}|{pred_band}"


def _signal_bucket_mean_stats(payload: dict) -> dict[str, dict[str, float]]:
    """``feedback_bucket_stats`` 를 버킷별 평균 달성률·괴리로 요약."""
    raw = payload.get("feedback_bucket_stats")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, float]] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not isinstance(v, dict):
            continue
        n = int(v.get("count", 0) or 0)
        sum_ratio = v.get("sum_ratio")
        if n <= 0 or not isinstance(sum_ratio, (int, float)) or not math.isfinite(float(sum_ratio)):
            continue
        out[k] = {
            "count": float(n),
            "mean_ratio": float(sum_ratio) / float(n),
        }
    return out


def merge_feedback_buckets_from_day_reports(day_reports: list) -> None:
    """
    신호 버킷별 예측-실제 달성률 통계를 누적합니다.

    버킷 축: 키워드 일치 수 / 종목명 언급 강도 / 예측 수익률 구간.
    """
    p = _load_payload()
    raw = p.get("feedback_bucket_stats")
    buckets: dict[str, dict[str, float]] = dict(raw) if isinstance(raw, dict) else {}
    changed = False
    for dr in day_reports:
        if getattr(dr, "forward_observation", False):
            continue
        for r in dr.rows_compare:
            pred_ret = r.get("pred_ret")
            actual_ret = r.get("actual_ret")
            if pred_ret is None or actual_ret is None:
                continue
            rr = _ratio(pred_ret, actual_ret)
            if rr is None:
                continue
            k_hits = r.get("keyword_hits")
            if k_hits is None:
                k_hits = len(r.get("keywords") or [])
            mention = r.get("mention_score")
            if mention is None:
                mention = 0.0
            key = _signal_bucket_key(
                pred_ret=float(pred_ret),
                keyword_hits=int(k_hits),
                mention_score=float(mention),
            )
            rec = dict(buckets.get(key) or {})
            c = int(rec.get("count", 0) or 0) + 1
            rec["count"] = float(c)
            rec["sum_ratio"] = float(rec.get("sum_ratio", 0.0) or 0.0) + float(rr)
            rec["sum_abs_gap"] = float(rec.get("sum_abs_gap", 0.0) or 0.0) + abs(float(pred_ret) - float(actual_ret) * 100.0)
            buckets[key] = rec
            changed = True
    if changed:
        p["feedback_bucket_stats"] = buckets
        _save_payload(p)


def apply_cached_cumulative_fallback(day_reports: list) -> int:
    """
    ``actual_ret`` 이 없어 이번 실행만으로 누적을 못 쓴 행에, 캐시 평균을 ``cumulative_accuracy_avg`` 로 넣습니다.

    Returns:
        값을 채운 행 수.
    """
    n = 0
    means = _mean_ratio_by_code_from_payload(_load_payload())
    for dr in day_reports:
        for r in dr.rows_compare:
            if r.get("actual_ret") is not None:
                continue
            if r.get("cumulative_accuracy_avg") is not None:
                continue
            code = str(r.get("code", "")).zfill(6)
            m = means.get(code)
            if m is None:
                continue
            r["cumulative_accuracy_avg"] = m
            n += 1
    return n


def merge_hit_at_k_from_day_reports(day_reports: list) -> None:
    """``DayReport.hit_at_k_metrics`` 를 ``hit_at_k_by_day`` 맵에 병합합니다."""
    p = _load_payload()
    by_day: dict = dict(p.get("hit_at_k_by_day") or {})
    changed = False
    for dr in day_reports:
        if getattr(dr, "forward_observation", False):
            continue
        m = getattr(dr, "hit_at_k_metrics", None)
        if not isinstance(m, dict) or not m:
            continue
        t_iso = dr.trading_day.isoformat()
        row = {"trading_day": t_iso, **m}
        if by_day.get(t_iso) != row:
            by_day[t_iso] = row
            changed = True
    if changed:
        p["hit_at_k_by_day"] = by_day
        _save_payload(p)
