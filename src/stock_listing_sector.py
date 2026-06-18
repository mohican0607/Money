"""
네이버 증권 업종(``industryCode``) 조회·캐시.

- 종목: ``m.stock.naver.com/api/stock/{code}/integration`` → ``industryCode``
- 업종명: ``finance.naver.com/sise/sise_group_detail.naver?type=upjong&no=`` 제목
- 기사 없는 25%↑ 종목에 **업종 pill**(예: 자동차부품, 반도체) 표시·ML 피처에 사용
"""
from __future__ import annotations

import json
import re
import time
from datetime import date
from functools import lru_cache
from pathlib import Path

import pandas as pd
import requests

from . import config

_CACHE_DIR = config.LISTING_META_CACHE_DIR
_CODE_TO_INDUSTRY = _CACHE_DIR / "stock_industry_code.json"
_INDUSTRY_NAMES = _CACHE_DIR / "industry_name_by_code.json"
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Money/1.0)"}
_FETCH_SLEEP = 0.12

# 네이버 업종명 → 리포트 짧은 라벨
_INDUSTRY_LABEL: tuple[tuple[str, str], ...] = (
    ("자동차부품", "자동차부품"),
    ("운송장비", "운송장비"),
    ("반도체", "반도체"),
    ("반도체와 반도체장비", "반도체"),
    ("전기전자", "전자"),
    ("전기·전자", "전자"),
    ("화학", "화학"),
    ("제약", "제약"),
    ("바이오", "바이오"),
    ("의료정밀", "의료기기"),
    ("기계", "기계"),
    ("조선", "조선"),
    ("철강", "철강"),
    ("건설", "건설"),
    ("은행", "금융"),
    ("증권", "금융"),
    ("보험", "금융"),
    ("소프트웨어", "소프트웨어"),
    ("게임", "게임"),
    ("통신", "통신"),
    ("유통", "유통"),
    ("음식료", "음식료"),
    ("섬유", "섬유"),
    ("운송", "운송"),
    ("에너지", "에너지"),
    ("2차전지", "2차전지"),
    ("배터리", "2차전지"),
)


def _ensure_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _read_json(path: Path, default: dict) -> dict:
    if not path.is_file():
        return dict(default)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else dict(default)
    except (OSError, json.JSONDecodeError, TypeError):
        return dict(default)


def _write_json(path: Path, data: dict) -> None:
    _ensure_dir()
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _fetch_industry_code(code: str) -> str:
    c = str(code).zfill(6)
    url = f"https://m.stock.naver.com/api/stock/{c}/integration"
    r = requests.get(url, headers=_HEADERS, timeout=14)
    r.raise_for_status()
    payload = r.json()
    return str(payload.get("industryCode") or "").strip()


def _fetch_industry_name(industry_code: str) -> str:
    ic = str(industry_code).strip()
    if not ic:
        return ""
    url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={ic}"
    r = requests.get(url, headers=_HEADERS, timeout=14)
    r.encoding = "euc-kr"
    m = re.search(r"<title>\s*([^<:]+)", r.text)
    return m.group(1).strip() if m else ""


def industry_code_for_stock(code: str, *, refresh: bool = False) -> str:
    """종목코드 → 네이버 업종코드(캐시)."""
    c = str(code).zfill(6)
    mapping = _read_json(_CODE_TO_INDUSTRY, {})
    if not refresh and c in mapping:
        return str(mapping[c] or "")
    try:
        ic = _fetch_industry_code(c)
    except (requests.RequestException, ValueError, TypeError):
        return str(mapping.get(c, "") or "")
    mapping[c] = ic
    _write_json(_CODE_TO_INDUSTRY, mapping)
    time.sleep(_FETCH_SLEEP)
    return ic


def industry_name_for_code(code: str, *, refresh: bool = False) -> str:
    """종목코드 → 업종명(예: 자동차부품)."""
    ic = industry_code_for_stock(code, refresh=refresh)
    if not ic:
        return ""
    names = _read_json(_INDUSTRY_NAMES, {})
    if not refresh and ic in names and str(names[ic]).strip():
        return str(names[ic])
    try:
        name = _fetch_industry_name(ic)
    except (requests.RequestException, ValueError, TypeError):
        name = str(names.get(ic, "") or "")
    if name:
        names[ic] = name
        _write_json(_INDUSTRY_NAMES, names)
        time.sleep(_FETCH_SLEEP)
    return str(name or "")


def compact_industry_label(raw: str) -> str:
    """업종명을 리포트 pill용 짧은 라벨로."""
    s = str(raw or "").strip()
    if not s:
        return ""
    for key, label in _INDUSTRY_LABEL:
        if key in s:
            return label
    return s.replace(" 관련주", "").strip()


def compact_label_for_code(code: str, *, name: str = "") -> str:
    """종목 업종 짧은 라벨. 실패 시 빈 문자열."""
    lab = compact_industry_label(industry_name_for_code(code))
    return lab


def prefetch_industry_codes(codes: list[str]) -> None:
    """여러 종목 업종코드·업종명을 미리 캐시(리포트·학습 전 호출)."""
    seen_ic: set[str] = set()
    for raw in codes:
        c = str(raw).zfill(6)
        ic = industry_code_for_stock(c)
        if ic and ic not in seen_ic:
            industry_name_for_code(c)
            seen_ic.add(ic)


@lru_cache(maxsize=1)
def _code_to_industry_map() -> dict[str, str]:
    return _read_json(_CODE_TO_INDUSTRY, {})


def peers_for_industry(industry_code: str) -> list[str]:
    """동일 업종코드 종목코드 목록(캐시 기준)."""
    ic = str(industry_code).strip()
    if not ic:
        return []
    m = _code_to_industry_map()
    return [c for c, v in m.items() if str(v) == ic]


def industry_momentum_for_code(
    code: str,
    returns_ml: pd.DataFrame,
    target_day: date,
) -> float:
    """
    동일 업종 전일 평균 수익률(``ret_lag1``) → 0~1 정규화.

    ML·하이브리드 랭킹 보조 피처.
    """
    ic = industry_code_for_stock(code)
    if not ic:
        return 0.0
    peers = peers_for_industry(ic)
    if not peers:
        return 0.0
    peer_set = {str(p).zfill(6) for p in peers}
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    if sl.empty:
        return 0.0
    rets: list[float] = []
    for _, row in sl.iterrows():
        c = str(row["Code"]).zfill(6)
        if c not in peer_set:
            continue
        try:
            v = float(row.get("ret_lag1") or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        if v == v:
            rets.append(v)
    if not rets:
        return 0.0
    avg = sum(rets) / len(rets)
    return max(0.0, min(1.0, avg / 0.12))


def industry_theme_overlap(code: str, kw_blob: str | frozenset[str]) -> float:
    """당일 뉴스 키워드와 업종명 겹침(0~1)."""
    ind = industry_name_for_code(code).lower()
    if not ind or len(ind) < 2:
        return 0.0
    if isinstance(kw_blob, frozenset):
        kws = [str(k).lower() for k in kw_blob if str(k).strip()]
    else:
        kws = [str(k).lower() for k in str(kw_blob).split() if k.strip()]
    best = 0.0
    for kw in kws:
        if len(kw) < 2:
            continue
        if kw in ind or ind in kw:
            best = max(best, 1.0)
        elif any(tok in ind for tok in (kw[:4], kw[:3]) if len(tok) >= 2):
            best = max(best, 0.55)
    return best
