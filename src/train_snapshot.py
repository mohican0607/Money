"""
훈련 구간 ``BreakoutEvent`` 스냅샷(JSON) — ``main.py`` 기본(증분 병합), ``--no-train-snapshot``, ``--rebuild-train-snapshot``.

- 지문(fingerprint): 훈련 구간·급등 임계·뉴스 컷오프·수집 소스 등이 바뀌면 스냅샷을 쓰지 않고 전체 재계산합니다.
- ``calendar_days_covered``: 이 스냅샷이 반영했다고 기록한 캘린더 일자(뉴스 JSON 일자와 대응).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from . import config
from .features import BreakoutEvent


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
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


def _event_to_json(e: BreakoutEvent) -> dict[str, Any]:
    return {
        "trading_day": e.trading_day.isoformat(),
        "code": e.code,
        "name": e.name,
        "return_pct": e.return_pct,
        "news_keywords": sorted(e.news_keywords),
        "news_snippets": list(e.news_snippets),
    }


def _event_from_json(d: dict[str, Any]) -> BreakoutEvent:
    return BreakoutEvent(
        trading_day=date.fromisoformat(str(d["trading_day"])),
        code=str(d["code"]),
        name=str(d["name"]),
        return_pct=float(d["return_pct"]),
        news_keywords=frozenset(str(x) for x in d.get("news_keywords") or []),
        news_snippets=[str(x) for x in d.get("news_snippets") or []],
    )


@dataclass
class TrainSnapshot:
    fingerprint: str
    events: list[BreakoutEvent]
    calendar_days_covered: set[date]


def load_snapshot(path: Path | None = None) -> TrainSnapshot | None:
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
