"""
네이버 증권 종목 공시(공시사항) 수집·캐시·간단 분류.

- 대상 페이지: https://finance.naver.com/item/news_notice.naver?code=XXXXXX
- 캐시: data/cache/disclosure/naver/YYYY/day_YYYYMMDD.json
- 용도: 단일일(N->T) 리포트에서 예측·실제 차이에 공시 맥락 추가
"""
from __future__ import annotations

import json
import re
import time
from datetime import date
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from . import config

_CACHE_DISC = config.CACHE_DIR / "disclosure" / "naver"

_KIND_RULES: list[tuple[str, str]] = [
    ("거래정지", "거래정지"),
    ("매매거래정지", "거래정지"),
    ("대량보유", "대량보유"),
    ("주식등의 대량보유", "대량보유"),
    ("공매도", "공매도"),
    ("유상증자", "유상증자"),
    ("무상증자", "무상증자"),
    ("전환사채", "전환사채"),
    ("신주인수권부사채", "BW/CB"),
    ("교환사채", "BW/CB"),
    ("조회공시", "조회공시"),
    ("단일판매", "단일판매"),
    ("실적", "실적"),
    ("영업(잠정)실적", "실적"),
]


def naver_disclosure_url(code: str) -> str:
    c = str(code).zfill(6)
    return f"https://finance.naver.com/item/news_notice.naver?code={c}"


def _day_file(target: date) -> Path:
    return _CACHE_DISC / target.strftime("%Y") / f"day_{target.strftime('%Y%m%d')}.json"


def _classify_kind(title: str) -> str:
    t = (title or "").strip()
    for needle, tag in _KIND_RULES:
        if needle in t:
            return tag
    return "기타"


def _parse_ymd_dot(s: str) -> date | None:
    m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", (s or "").strip())
    if not m:
        return None
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _fetch_one_code_on_day(
    session: requests.Session, code: str, target: date, *, max_pages: int = 2
) -> list[dict]:
    code = str(code).zfill(6)
    out: list[dict] = []
    for page in range(1, max_pages + 1):
        url = naver_disclosure_url(code) + f"&page={page}"
        try:
            r = session.get(
                url,
                timeout=20,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            r.raise_for_status()
        except requests.RequestException:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table.type5 tr")
        if not rows:
            break

        stop_old = False
        page_hit = 0
        for tr in rows:
            a = tr.select_one("a")
            dtd = tr.select_one("td.date")
            if a is None or dtd is None:
                continue
            pub_d = _parse_ymd_dot(dtd.get_text(" ", strip=True))
            if pub_d is None:
                continue
            if pub_d < target:
                stop_old = True
                continue
            if pub_d != target:
                continue

            href = a.get("href") or ""
            link = f"https://finance.naver.com{href}" if href.startswith("/") else href
            title = a.get_text(" ", strip=True)
            out.append(
                {
                    "code": code,
                    "day": target.isoformat(),
                    "title": title,
                    "kind": _classify_kind(title),
                    "link": link,
                    "source": "naver_notice",
                }
            )
            page_hit += 1

        if stop_old:
            break
        if page_hit == 0 and page >= 2:
            break
        time.sleep(0.03)
    return out


def fetch_disclosures_for_codes_on_day(target: date, codes: list[str]) -> dict[str, list[dict]]:
    """
    target 하루의 종목 공시를 code별로 반환.
    캐시가 있으면 재사용하고, 요청된 code가 누락된 경우에만 보강 조회.
    """
    uniq_codes = sorted({str(c).zfill(6) for c in codes if c})
    if not uniq_codes:
        return {}

    day_file = _day_file(target)
    day_file.parent.mkdir(parents=True, exist_ok=True)

    cached_rows: list[dict] = []
    if day_file.is_file():
        try:
            cached_rows = json.loads(day_file.read_text(encoding="utf-8"))
        except Exception:
            cached_rows = []

    cached_codes = {str(r.get("code", "")).zfill(6) for r in cached_rows if r.get("code")}
    miss_codes = [c for c in uniq_codes if c not in cached_codes]

    new_rows: list[dict] = []
    if miss_codes:
        sess = requests.Session()
        try:
            for c in miss_codes:
                new_rows.extend(_fetch_one_code_on_day(sess, c, target))
        finally:
            sess.close()

    merged = cached_rows + new_rows
    day_file.write_text(json.dumps(merged, ensure_ascii=False), encoding="utf-8")

    out: dict[str, list[dict]] = {c: [] for c in uniq_codes}
    for row in merged:
        c = str(row.get("code", "")).zfill(6)
        if c in out:
            out[c].append(row)
    return out
