"""
훈련 스냅샷 + 테마 이월 + 미스 진단 (snapshot + rebuild_support).
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .. import config, trading_calendar
from ..features import BreakoutEvent, filter_specific_keywords, keyword_set
from .. import news
from ..prediction import predict
from ..report.render import DayReport


# --- snapshot (from snapshot.py) ---

FORMAT_VERSION = 1


def fingerprint() -> str:
    """스냅샷 무효화용 설정 지문(코드·시세 데이터 변경은 포함하지 않음)."""
    payload = {
        "train_start": config.TRAIN_START_DEFAULT.isoformat(),
        "test_start": config.TEST_START.isoformat(),
        "big_move": config.BIG_MOVE_THRESHOLD,
        "decision_cutoff": config.USE_DECISION_NEWS_INTRADAY_CUTOFF,
        "news_cutoff_kst": (
            f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}"
            if config.USE_DECISION_NEWS_INTRADAY_CUTOFF
            else None
        ),
        "mock_news": config.MOCK_NEWS,
        "naver_mode": config.NEWS_NAVER_QUERY_MODE,
        "google_rss_fallback": config.USE_GOOGLE_NEWS_RSS_FALLBACK,
        "has_naver_creds": bool(config.NAVER_CLIENT_ID and config.NAVER_CLIENT_SECRET),
        # early 뉴스 컷오프를 T 직전 거래일 세션으로 둠(주말·연휴 직전 장 뉴스 포함).
        "news_early_anchor": "last_krx_session_before_t_close",
        "walk_forward_train_events": True,
        "krx_ad_hoc_closures": sorted(d.isoformat() for d in config.KRX_AD_HOC_SESSION_CLOSURES),
        # 급등 이벤트 키워드: 종목 직접 언급 뉴스만(시장 전체 blob 공유 폐기)
        "breakout_keywords": "stock_specific_v1",
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def _event_to_json(e: BreakoutEvent) -> dict[str, Any]:
    """``BreakoutEvent`` 를 스냅샷 JSON 행 dict로 직렬화."""
    return {
        "trading_day": e.trading_day.isoformat(),
        "code": e.code,
        "name": e.name,
        "return_pct": e.return_pct,
        "news_keywords": sorted(e.news_keywords),
        "news_snippets": list(e.news_snippets),
    }


def _event_from_json(d: dict[str, Any]) -> BreakoutEvent:
    """스냅샷 JSON 행 dict에서 ``BreakoutEvent`` 를 복원."""
    return BreakoutEvent(
        trading_day=date.fromisoformat(str(d["trading_day"])),
        code=str(d["code"]),
        name=str(d["name"]),
        return_pct=float(d["return_pct"]),
        news_keywords=frozenset(str(x) for x in d.get("news_keywords") or []),
        news_snippets=[str(x) for x in d.get("news_snippets") or []],
    )


def filter_train_events_before(
    events: list[BreakoutEvent],
    before_exclusive: date,
    *,
    train_start: date | None = None,
) -> list[BreakoutEvent]:
    """관측일 ``before_exclusive``(T) **이전** 거래일 급등 이벤트만 반환(워크포워드 학습 라벨)."""
    lo = train_start if train_start is not None else config.TRAIN_START_DEFAULT
    return [e for e in events if lo <= e.trading_day < before_exclusive]


def events_for_storage(
    events: list[BreakoutEvent],
    *,
    train_start: date,
    storage_end: date,
) -> list[BreakoutEvent]:
    """스냅샷 JSON에 저장할 이벤트(``train_start`` ~ ``storage_end`` 거래일)."""
    return [e for e in events if train_start <= e.trading_day <= storage_end]


@dataclass
class TrainSnapshot:
    fingerprint: str
    events: list[BreakoutEvent]
    calendar_days_covered: set[date]


def load_snapshot(path: Path | None = None) -> TrainSnapshot | None:
    """훈련 스냅샷 JSON을 읽어 ``TrainSnapshot`` 으로 반환. 없거나 형식 불일치 시 ``None``."""
    p = path or config.TRAIN_SNAPSHOT_PATH
    if not p.is_file():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if int(data.get("format_version", 0)) != FORMAT_VERSION:
        return None
    fp = str(data.get("fingerprint") or "")
    days_raw = data.get("calendar_days_covered") or []
    cov: set[date] = set()
    for x in days_raw:
        try:
            cov.add(date.fromisoformat(str(x)))
        except ValueError:
            continue
    evs: list[BreakoutEvent] = []
    for row in data.get("train_events") or []:
        try:
            evs.append(_event_from_json(row))
        except (KeyError, TypeError, ValueError):
            continue
    if not fp:
        return None
    return TrainSnapshot(fingerprint=fp, events=evs, calendar_days_covered=cov)


_SNAPSHOT_PRESERVE_KEYS = (
    "rebuild_learning",
    "market_theme_flow",
    "prediction_gap_rollup",
)


def save_snapshot(
    events: list[BreakoutEvent],
    calendar_days_covered: set[date],
    *,
    path: Path | None = None,
    fp: str | None = None,
) -> None:
    """급등 이벤트·캘린더 커버리지를 JSON에 저장. 기존 확장 필드(``rebuild_learning`` 등)는 보존."""
    p = path or config.TRAIN_SNAPSHOT_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    fp_use = fp if fp is not None else fingerprint()
    preserved: dict[str, Any] = {}
    if p.is_file():
        try:
            with open(p, encoding="utf-8") as f:
                old = json.load(f)
            if int(old.get("format_version", 0)) == FORMAT_VERSION:
                for k in _SNAPSHOT_PRESERVE_KEYS:
                    if k in old and old[k] is not None:
                        preserved[k] = old[k]
        except (OSError, json.JSONDecodeError, TypeError):
            pass
    out: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "fingerprint": fp_use,
        "calendar_days_covered": sorted(d.isoformat() for d in calendar_days_covered),
        "train_events": [_event_to_json(e) for e in events],
    }
    out.update(preserved)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=0)


def merge_snapshot_fields(
    fields: dict[str, Any],
    *,
    path: Path | None = None,
) -> bool:
    """
    기존 스냅샷 JSON 최상위에 ``fields`` 를 병합해 다시 저장합니다.

    ``--rebuild-train-snapshot`` + From~To 구간 실행 말미에 예측–실제 괴리 누적 분석 등을 붙일 때 사용합니다.
    파일이 없거나 ``format_version`` 이 맞지 않으면 ``False`` 를 돌려줍니다.
    """
    p = path or config.TRAIN_SNAPSHOT_PATH
    if not p.is_file():
        return False
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    if int(data.get("format_version", 0)) != FORMAT_VERSION:
        return False
    data.update(fields)
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=0)
    except OSError:
        return False
    return True

# --- rebuild_support (from rebuild_support.py) ---

_SNAPSHOT_PATH = config.TRAIN_CACHE_DIR / "daily_theme_snapshots.json"
_CACHE_MTIME: float | None = None
_CACHE_PAYLOAD: dict[str, Any] | None = None


def snapshot_path() -> Path:
    """일별 테마 스냅샷 JSON 파일 경로(``daily_theme_snapshots.json``)."""
    return _SNAPSHOT_PATH


def _default_payload() -> dict[str, Any]:
    """빈 ``by_day`` 를 가진 기본 스냅샷 딕셔너리."""
    return {"version": 1, "by_day": {}}


def _load_payload() -> dict[str, Any]:
    """디스크 JSON을 읽어 모듈 캐시에 두고 반환. 없거나 손상 시 기본 페이로드."""
    global _CACHE_MTIME, _CACHE_PAYLOAD
    p = _SNAPSHOT_PATH
    if not p.is_file():
        _CACHE_MTIME, _CACHE_PAYLOAD = None, _default_payload()
        return _CACHE_PAYLOAD
    try:
        m = p.stat().st_mtime
    except OSError:
        _CACHE_MTIME, _CACHE_PAYLOAD = None, _default_payload()
        return _CACHE_PAYLOAD
    if _CACHE_MTIME == m and _CACHE_PAYLOAD is not None:
        return _CACHE_PAYLOAD
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        _CACHE_MTIME, _CACHE_PAYLOAD = m, _default_payload()
        return _CACHE_PAYLOAD
    if not isinstance(raw, dict):
        raw = {}
    out = _default_payload()
    bd = raw.get("by_day")
    if isinstance(bd, dict):
        out["by_day"] = bd
    _CACHE_MTIME, _CACHE_PAYLOAD = m, out
    return out


def _invalidate_cache() -> None:
    """메모리 캐시(``_CACHE_MTIME``·``_CACHE_PAYLOAD``)를 비웁니다."""
    global _CACHE_MTIME, _CACHE_PAYLOAD
    _CACHE_MTIME, _CACHE_PAYLOAD = None, None


def _save_payload(payload: dict[str, Any]) -> None:
    """스냅샷 JSON을 디스크에 쓰고 메모리 캐시를 무효화합니다."""
    _SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": 1, "by_day": dict(payload.get("by_day") or {})}
    _SNAPSHOT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _invalidate_cache()


def _normalize_weights(c: Counter, *, top_n: int = 96) -> dict[str, float]:
    """키워드 카운터를 최댓값 기준 [0, 1] 가중치 dict로 정규화(상위 ``top_n``)."""
    if not c:
        return {}
    mx = max(c.values())
    if mx <= 0:
        return {}
    out: dict[str, float] = {}
    for k, v in c.most_common(top_n):
        if not isinstance(k, str) or len(k.strip()) < 2:
            continue
        fv = float(v) / float(mx)
        if math.isfinite(fv) and fv > 0:
            out[k.strip()] = fv
    return out


def _normalize_signed_weights(
    pos: Counter[str],
    neg: Counter[str],
    *,
    top_n: int = 120,
) -> dict[str, float]:
    """
    양(급등)·음(급락) 카운터를 각각 정규화해 합친 signed weights.

    반환값은 대략 [-1, 1] 범위의 값이며, 급락(음수) 키워드는 다음날 점수/테마에 감점으로 작동할 수 있습니다.
    """
    w_pos = _normalize_weights(pos, top_n=top_n)
    w_neg = _normalize_weights(neg, top_n=top_n)
    out: dict[str, float] = {}
    for k, v in w_pos.items():
        out[k] = out.get(k, 0.0) + float(v)
    for k, v in w_neg.items():
        out[k] = out.get(k, 0.0) - float(v)
    # 너무 큰 값을 방지(향후 스케일 파라미터가 이미 존재)
    for k in list(out.keys()):
        vv = float(out[k])
        if not math.isfinite(vv) or abs(vv) < 1e-6:
            out.pop(k, None)
            continue
        out[k] = max(-1.0, min(1.0, vv))
    return out


def _early_blob_for_trading_day(news_by_calendar: dict[date, list[dict[str, str]]], d: date) -> str:
    """거래일 ``d`` 의 early 뉴스 텍스트 blob(컷오프 설정에 따라 집계 방식 분기)."""
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by_calendar, d)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(d)
    return predict.aggregate_news_for_window(news_by_calendar, ws, we)


def synthesize_weights_for_day(
    session_day: date,
    *,
    train_events: list[BreakoutEvent],
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: pd.DataFrame,
    threshold: float,
) -> dict[str, float]:
    """
    ``session_day`` 에 20%↑였던 종목의 훈련 키워드와, 그날 early 뉴스 키워드 교집합으로 테마 가중치를 만듭니다.

    스냅샷이 없을 때 예측·ML 학습 보조용.
    """
    blob = _early_blob_for_trading_day(news_by_calendar, session_day)
    if not blob.strip():
        return {}
    kw_news = keyword_set(blob, k=120)
    ts = pd.Timestamp(session_day)
    if returns_df is None or returns_df.empty or "Date" not in returns_df.columns:
        return {}
    m = (returns_df["Date"] == ts) & (
        pd.to_numeric(returns_df["return_pct"], errors="coerce") >= float(threshold) - 1e-9
    )
    day_rows = returns_df.loc[m]
    if day_rows.empty:
        return {}
    c: Counter[str] = Counter()
    thr = float(threshold)
    for _, row in day_rows.iterrows():
        code = str(row["Code"]).zfill(6)
        rp = float(row["return_pct"])
        if not math.isfinite(rp):
            continue
        w = 1.0 + max(0.0, rp - thr) * 3.0
        hist: set[str] = set()
        for e in train_events:
            if e.code == code and e.trading_day < session_day:
                hist |= set(e.news_keywords)
        for k in filter_specific_keywords(hist & set(kw_news)):
            c[k] += w
        ind_name = ""
        try:
            from .. import stocks as stocks_mod

            ind_name = (stocks_mod.industry_name_for_code(code) or "").strip()
        except Exception:
            ind_name = ""
        if len(ind_name) >= 2:
            c[ind_name] += w * 0.55
            for tok in ind_name.replace("·", " ").split():
                if len(tok) >= 2:
                    c[tok] += w * 0.35
    return _normalize_weights(c)


def build_rich_weights_from_pipeline(
    *,
    actual_big_movers: list[dict[str, Any]],
    actual_big_decliners: list[dict[str, Any]] | None = None,
    rows_compare: list[dict[str, Any]],
    highlight_terms: list[str],
    threshold: float,
) -> dict[str, float]:
    """당일 파이프라인 표·급등/급락 목록·하이라이트 용어로 테마(+-) 가중치."""
    pos: Counter[str] = Counter()
    neg: Counter[str] = Counter()
    mover_up = {str(m.get("code", "")).zfill(6) for m in actual_big_movers if m.get("code")}
    mover_dn = {
        str(m.get("code", "")).zfill(6)
        for m in (actual_big_decliners or [])
        if m.get("code")
    }
    thr = float(threshold)
    for r in rows_compare:
        code = str(r.get("code", "")).zfill(6)
        if code not in mover_up and code not in mover_dn:
            continue
        ar = r.get("actual_ret")
        if ar is None or not math.isfinite(float(ar)):
            continue
        ar_f = float(ar)
        if code in mover_up and ar_f < thr - 1e-9:
            continue
        if code in mover_dn and ar_f > -thr + 1e-9:
            continue
        mag = abs(ar_f)
        w = 1.0 + max(0.0, mag - thr) * 3.0
        for kw in r.get("keywords") or []:
            if isinstance(kw, str):
                kk = kw.strip()
                if len(kk) >= 2:
                    (pos if code in mover_up else neg)[kk] += w
    for t in highlight_terms or []:
        if isinstance(t, str):
            tt = t.strip()
            if len(tt) >= 2:
                pos[tt] += 0.35
    return _normalize_signed_weights(pos, neg)


def load_weights_for_calendar_day(day: date) -> dict[str, float]:
    """저장된 스냅샷에서 ``day``(거래일) 테마 가중치만 읽습니다."""
    p = _load_payload()
    row = (p.get("by_day") or {}).get(day.isoformat())
    if not isinstance(row, dict):
        return {}
    w = row.get("weights")
    if not isinstance(w, dict):
        return {}
    out: dict[str, float] = {}
    for k, v in w.items():
        if isinstance(k, str) and isinstance(v, (int, float)) and math.isfinite(float(v)):
            out[k.strip()] = float(v)
    return out


def persist_rich_snapshot(
    trading_day: date,
    *,
    actual_big_movers: list[dict[str, Any]],
    actual_big_decliners: list[dict[str, Any]] | None = None,
    rows_compare: list[dict[str, Any]],
    highlight_terms: list[str],
    threshold: float,
) -> None:
    """관측일 ``trading_day`` 처리 후, 그날 급등·표 기반 테마를 JSON에 병합합니다."""
    if not config.THEME_CARRYOVER_ENABLED:
        return
    weights = build_rich_weights_from_pipeline(
        actual_big_movers=actual_big_movers,
        actual_big_decliners=actual_big_decliners,
        rows_compare=rows_compare,
        highlight_terms=highlight_terms,
        threshold=threshold,
    )
    p = _load_payload()
    by_day = dict(p.get("by_day") or {})
    t_iso = trading_day.isoformat()
    by_day[t_iso] = {
        "weights": dict(weights),
        "n_movers": len(actual_big_movers),
        "n_decliners": len(actual_big_decliners or []),
        "source": "pipeline",
    }
    p["by_day"] = by_day
    _save_payload(p)


def weights_for_observation_day(
    target_day: date,
    *,
    train_events: list[BreakoutEvent],
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: pd.DataFrame,
    threshold: float,
) -> dict[str, float]:
    """
    관측일 ``target_day`` 예측에 쓸 테마 가중치 = **직전 영업일** 스냅샷(또는 합성).
    """
    if not config.THEME_CARRYOVER_ENABLED:
        return {}
    try:
        prev_td = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        return {}
    snap = load_weights_for_calendar_day(prev_td)
    if snap:
        return snap
    return synthesize_weights_for_day(
        prev_td,
        train_events=train_events,
        news_by_calendar=news_by_calendar,
        returns_df=returns_df,
        threshold=threshold,
    )


def theme_kw_overlap_score(kw_news: frozenset[str], theme_weights: dict[str, float] | None) -> float:
    """뉴스 키워드와 테마 가중치의 가중합(상한 캡). ML·로그용."""
    if not theme_weights:
        return 0.0
    s = 0.0
    for k in kw_news:
        v = theme_weights.get(k)
        if v is not None and math.isfinite(float(v)):
            s += float(v)
    return float(min(s, 14.0))

# --- miss diagnosis ---

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

    from .. import config as _cfg

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