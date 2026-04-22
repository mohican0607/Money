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
    return {
        "version": 2,
        "t_code_ratio": {},
        "t_code_actual_pct": {},
        "high_pred_by_code": {},
    }


def _load_payload() -> dict:
    if not TRACK_PATH.is_file():
        return _default_payload()
    try:
        raw = json.loads(TRACK_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _default_payload()
    if not isinstance(raw, dict):
        return _default_payload()
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
    return out


def _save_payload(payload: dict) -> None:
    TRACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 2,
        "t_code_ratio": dict(payload.get("t_code_ratio") or {}),
        "t_code_actual_pct": dict(payload.get("t_code_actual_pct") or {}),
        "high_pred_by_code": dict(payload.get("high_pred_by_code") or {}),
    }
    TRACK_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _key(t: object, code: str) -> str:
    td = getattr(t, "isoformat", lambda: str(t))()
    return f"{td}:{str(code).zfill(6)}"


def _sync_high_pred_actuals_in_payload(p: dict) -> bool:
    """
    ``high_pred_by_code`` 항목 중 ``actual_pct`` 가 비어 있고 ``t_code_actual_pct`` 에 값이 있으면 채웁니다.

    예측 전용 실행만으로 이력이 남은 뒤, 다른 날만 재실행해 ``merge_high_pred_history`` 가 해당 T를
    건드리지 못한 경우에도 ``merge_from_day_reports`` 한 번으로 툴팁 실적%를 맞출 수 있습니다.
    """
    act_map = p.get("t_code_actual_pct")
    if not isinstance(act_map, dict) or not act_map:
        return False
    hp = p.get("high_pred_by_code")
    if not isinstance(hp, dict):
        return False
    changed = False
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
        _sync_high_pred_actuals_in_payload(p)
        _save_payload(p)
    elif _sync_high_pred_actuals_in_payload(p):
        # 이번 실행에 새 비율은 없어도, 이미 쌓인 실적% 맵으로 이력만 보정
        _save_payload(p)


def merge_high_pred_history_from_day_reports(
    day_reports: list, *, threshold_pct: float
) -> None:
    """예측 수익률이 ``threshold_pct`` 이상인 행을 종목별 이력에 반영(같은 T·코드는 덮어씀)."""
    p = _load_payload()
    hist_raw = p.get("high_pred_by_code")
    hist: dict[str, list] = dict(hist_raw) if isinstance(hist_raw, dict) else {}
    act_map: dict[str, float] = dict(p.get("t_code_actual_pct") or {})
    changed = False
    thr = float(threshold_pct)
    for dr in day_reports:
        t = dr.trading_day
        t_iso = t.isoformat()
        for r in dr.rows_compare:
            pr = r.get("pred_ret")
            if pr is None or float(pr) + 1e-9 < thr:
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
                "pred_pct": float(pr),
                "actual_pct": actual_pct,
            }
            lst = [x for x in hist.get(code, []) if isinstance(x, dict) and x.get("t") != t_iso]
            lst.append(entry)
            lst.sort(key=lambda x: str(x.get("t", "")), reverse=True)
            hist[code] = lst
            changed = True
    if changed:
        p["high_pred_by_code"] = hist
        _sync_high_pred_actuals_in_payload(p)
        _save_payload(p)
    elif _sync_high_pred_actuals_in_payload(p):
        _save_payload(p)


def _parse_hist_t_iso(t_raw: object) -> date | None:
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
    if _sync_high_pred_actuals_in_payload(p):
        changed = True
    if changed:
        _save_payload(p)


def high_pred_history_for_code(code: str) -> list[dict]:
    """관측일 내림차순 ``[{t, pred_pct, actual_pct}, ...]``.

    ``high_pred_by_code`` 에 ``actual_pct`` 가 비어 있어도 ``t_code_actual_pct``(관측일·코드별 실제%)가
    있으면 툴팁에 실제%를 채웁니다(예측 전용 실행 후 전체 재생성 시 이력만 남는 경우).
    """
    c = str(code).zfill(6)
    payload = _load_payload()
    hp = payload.get("high_pred_by_code")
    act_map: dict[str, float] = dict(payload.get("t_code_actual_pct") or {})
    if not isinstance(hp, dict):
        return []
    lst = hp.get(c, [])
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


def enrich_rows_pred_high_history(day_reports: list) -> None:
    """각 ``rows_compare`` 행에 ``pred_high_history`` 리스트를 붙입니다."""
    for dr in day_reports:
        for r in dr.rows_compare:
            code = str(r.get("code", "")).zfill(6)
            r["pred_high_history"] = high_pred_history_for_code(code)
    _patch_today_open_session_intraday_in_histories(day_reports)
    _backfill_closed_high_pred_actuals_from_market(day_reports)
    for dr in day_reports:
        for r in dr.rows_compare:
            code = str(r.get("code", "")).zfill(6)
            r["pred_high_history"] = high_pred_history_for_code(code)


def mean_ratio_for_code(code: str) -> float | None:
    """캐시에 있는 해당 종목의 모든 관측일 비율 산술 평균."""
    c = str(code).zfill(6)
    suffix = f":{c}"
    vals = [
        min(float(v), 1.0)
        for k, v in (_load_payload().get("t_code_ratio") or {}).items()
        if isinstance(k, str) and k.endswith(suffix) and isinstance(v, (int, float))
    ]
    if not vals:
        return None
    return sum(vals) / len(vals)


def apply_cached_cumulative_fallback(day_reports: list) -> int:
    """
    ``actual_ret`` 이 없어 이번 실행만으로 누적을 못 쓴 행에, 캐시 평균을 ``cumulative_accuracy_avg`` 로 넣습니다.

    Returns:
        값을 채운 행 수.
    """
    n = 0
    for dr in day_reports:
        for r in dr.rows_compare:
            if r.get("actual_ret") is not None:
                continue
            if r.get("cumulative_accuracy_avg") is not None:
                continue
            code = str(r.get("code", "")).zfill(6)
            m = mean_ratio_for_code(code)
            if m is None:
                continue
            r["cumulative_accuracy_avg"] = m
            n += 1
    return n
