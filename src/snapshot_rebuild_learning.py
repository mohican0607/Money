"""
``--rebuild-train-snapshot`` + From~To 실행 시 ``breakout_train_snapshot.json`` 에 병합되는 확장 학습 묶음.

- ``market_theme_flow``: 관측일 T별 early 뉴스 기반 시장 키워드·테마 시드 교차·급등/준급등 다발 구간 요약.
- ``prediction_gap_rollup``: ``prediction_accuracy_track.json`` 에서 구간별 괴리·버킷 통계 스냅샷.
- ``rebuild_learning``: 기존 일자와 병합(동일 ``trading_day`` 는 최신 실행으로 덮어씀) 후 요약 재계산.
  일자별 ``missed_big_movers_today``·``pred_high_misses_today``(원인 태그·한글 힌트)는 ``snapshot_miss_diagnosis`` 가 채우며,
  ML 재학습 시 스냅샷의 진단을 읽어 어려운 급등·오판 샘플을 소폭 복제 가중합니다.
"""
from __future__ import annotations

import html
import json
import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from . import config, features, news, predict, trading_calendar, train_snapshot, snapshot_miss_diagnosis


def _news_blob_for_trading_day(
    news_by_calendar: dict[date, list[dict[str, str]]], t_day: date
) -> str:
    """거래일 ``t_day`` 의 early 뉴스 텍스트 blob."""
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by_calendar, t_day)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(t_day)
    return predict.aggregate_news_for_window(news_by_calendar, ws, we)


# (표시 라벨, 뉴스·종목명 매칭용 별칭들) — 첫 별칭이 문장 원인 추출 시 대표 키워드.
_THEME_SECTORS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("반도체 관련주", ("반도체", "HBM", "메모리", "칩", "파운드리", "SOC", "반도체장비", "웨이퍼")),
    ("2차전지·배터리 관련주", ("2차전지", "배터리", "양극재", "음극재", "전해질", "리튬", "전고체")),
    ("바이오·제약 관련주", ("바이오", "제약", "신약", "임상", "바이오시밀러", "의료기기", "헬스케어")),
    ("AI·데이터 관련주", ("AI", "인공지능", "LLM", "챗봇", "딥러닝", "데이터센터", "GPU")),
    ("로봇·자동화 관련주", ("로봇", "로보", "로보틱", "로보틱스", "휴머노이드", "자동화", "스마트팩토리", "협동로봇", "robot", "robotics")),
    ("조선·해운 관련주", ("조선", "LNG선", "해운", "선박", "컨테이너선")),
    ("방산·국방 관련주", ("방산", "국방", "무기", "K방산", "우주항공")),
    ("원전·SMR 관련주", ("원전", "SMR", "핵융합", "원자력", "모듈원전")),
    ("전력·전력기기 관련주", ("전력", "전력기기", "변압기", "송전", "전선", "전기차충전")),
    ("전기차·모빌리티 관련주", ("전기차", "EV", "완성차", "자율주행", "모빌리티", "충전소", "부품")),
    ("디스플레이 관련주", ("디스플레이", "OLED", "LCD", "패널", "마이크로LED")),
    ("게임·엔터 관련주", ("게임", "엔터", "콘텐츠", "IP", "웹툰", "OTT")),
    ("화장품·뷰티 관련주", ("화장품", "K뷰티", "뷰티", "코스메틱")),
    ("유통·소비재 관련주", ("유통", "소비", "백화점", "면세", "리테일", "음식료")),
    ("건설·부동산 관련주", ("건설", "부동산", "리츠", "재건축", "인프라", "토목")),
    ("금융·은행 관련주", ("금융", "은행", "보험", "증권", "카드", "핀테크", "결제")),
    ("화학·소재 관련주", ("화학", "석유화학", "정유", "소재", "철강", "비철", "골판지")),
    ("수소·연료전지 관련주", ("수소", "연료전지", "그린수소", "암모니아")),
    ("신재생·에너지 관련주", ("태양광", "풍력", "신재생", "ESS", "에너지저장")),
    ("통신·플랫폼 관련주", ("통신", "5G", "6G", "플랫폼", "클라우드", "SaaS", "IT", "네트워크", "네트웍")),
    ("섬유·패션 관련주", ("섬유", "직물", "원단", "패션", "의류", "방직", "섬유소재")),
    ("자동차·부품 관련주", ("자동차부품", "자동차", "부품", "전장", "완성차", "모빌리티", "충전")),
    ("미국·글로벌 증시 연관주", ("미국증시", "나스닥", "다우", "뉴욕증시", "S&P", "연준", "FOMC")),
    ("환율·금리 민감주", ("환율", "원달러", "금리", "한국은행", "기준금리", "채권")),
    ("수출·무역 관련주", ("수출", "무역", "관세", "수입", "환헤지")),
    ("실적·어닝 관련주", ("실적", "분기실적", "영업이익", "컨센서스", "어닝", "가이던스")),
    ("배당·가치 관련주", ("배당", "배당금", "자사주", "주주환원")),
    ("IPO·상장 관련주", ("IPO", "상장", "공모", "스팩", "딜리스팅")),
    ("M&A·지배구조 관련주", ("합병", "인수", "M&A", "지분", "경영권", "유상증자", "무상증자")),
    ("여행·항공·레저 관련주", ("항공", "여행", "관광", "호텔", "면세점", "레저")),
    ("의료·병원 관련주", ("병원", "의료", "원격의료", "디지털헬스")),
    ("교육·에듀테크 관련주", ("교육", "에듀테크", "학원", "온라인교육")),
    ("농업·식품 관련주", ("농업", "식품", "사료", "축산", "수산")),
    ("해양·수산 관련주", ("해양", "수산", "양식", "어업")),
    ("중국·신흥국 관련주", ("중국", "중국경제", "신흥국", "베트남", "인도")),
    ("지정학·안보 이슈 관련주", ("지정학", "중동", "우크라이나", "대만", "북한")),
    ("규제·정책 수혜·피해주", ("규제", "정책", "세제", "보조금", "규제완화")),
)

_GENERIC_THEME_KW = frozenset(
    {
        "코스피",
        "코스닥",
        "증시",
        "주가",
        "급등",
        "상한가",
        "거래량",
        "테마주",
        "한국거래소",
        "kospi",
        "kosdaq",
        "상장",
        "공시",
        "금융",
        "증권",
        "주식시장",
        "국내증시",
    }
)

# 종목명 stem → 섹터 (당일 early 뉴스에 해당 테마가 살아 있을 때만 2차 배정)
_NAME_STEM_SECTORS: tuple[tuple[str, str], ...] = (
    ("로보", "로봇·자동화 관련주"),
    ("robot", "로봇·자동화 관련주"),
    ("반도체", "반도체 관련주"),
    ("하이닉", "반도체 관련주"),
    ("hbm", "반도체 관련주"),
    ("바이오", "바이오·제약 관련주"),
    ("제약", "바이오·제약 관련주"),
    ("2차전지", "2차전지·배터리 관련주"),
    ("배터리", "2차전지·배터리 관련주"),
    ("전지", "2차전지·배터리 관련주"),
    ("에코프로", "2차전지·배터리 관련주"),
    ("조선", "조선·해운 관련주"),
    ("방산", "방산·국방 관련주"),
    ("섬유", "섬유·패션 관련주"),
    ("직물", "섬유·패션 관련주"),
    ("네트웍", "통신·플랫폼 관련주"),
    ("네트워크", "통신·플랫폼 관련주"),
    ("부품", "자동차·부품 관련주"),
)

_EVENT_KW = (
    "실적",
    "예상실적",
    "컨센서스",
    "하회",
    "상회",
    "부진",
    "호실적",
    "수주",
    "규제",
    "급락",
    "급등",
    "전망",
    "우려",
    "악재",
    "호재",
)

# KRX 상한가 근사(코스피·코스닥 29~30%대). ``returns_df.return_pct`` 는 소수(0.298=29.8%).
# ``_stock_dict`` 등 표시용 필드 ``return_pct`` 는 퍼센트 포인트(29.8).
_LIMIT_UP_PCT = 29.5
_BIG_MOVE_PCT = 20.0
_MID_MOVE_PCT = 10.0


def _limit_up_threshold_pct() -> float:
    return _LIMIT_UP_PCT


def _stock_dict(
    stock_row: pd.Series,
    listing_names: dict[str, str],
    kw_set: frozenset[str],
) -> dict[str, Any]:
    code = str(stock_row["Code"]).zfill(6)
    name = str(stock_row.get("Name") or listing_names.get(code, ""))
    rp = round(float(stock_row["return_pct"]) * 100.0, 3)
    name_kw = features.keyword_set(name, k=12)
    overlap = features.filter_keywords(sorted(name_kw & kw_set, key=len, reverse=True))[:5]
    return {
        "code": code,
        "name": name,
        "return_pct": rp,
        "name_keyword_overlap_with_news": overlap,
        "is_limit_up": rp + 1e-9 >= _LIMIT_UP_PCT,
    }


def _sectors_for_seed(seed: str) -> list[tuple[str, tuple[str, ...]]]:
    """시드 토큰과 별칭이 겹치는 테마 섹터(라벨, 별칭) 목록."""
    t = str(seed).strip().lower()
    if len(t) < 2:
        return []
    out: list[tuple[str, tuple[str, ...]]] = []
    for sector_label, aliases in _THEME_SECTORS:
        for alias in aliases:
            a = str(alias).strip().lower()
            if not a:
                continue
            if t == a or t in a or a in t:
                out.append((sector_label, aliases))
                break
    return out


def _movers_for_seed(
    seed: str,
    leaders_ge20: list[dict[str, Any]],
    *,
    sector_aliases: list[tuple[str, ...]] | None = None,
) -> list[dict[str, Any]]:
    """시드·섹터 별칭과 맞닿은 20%↑ 종목(중복 제거)."""
    seen: set[str] = set()
    matched: list[dict[str, Any]] = []
    aliases_pool: list[tuple[str, ...]] = list(sector_aliases or [])
    aliases_pool.append((str(seed),))
    for aliases in aliases_pool:
        for stock in leaders_ge20:
            if not isinstance(stock, dict):
                continue
            code = str(stock.get("code") or "").zfill(6)
            if not code or code in seen:
                continue
            if _stock_matches_sector(stock, aliases):
                seen.add(code)
                matched.append(stock)
    seed_t = (str(seed),)
    for stock in leaders_ge20:
        if not isinstance(stock, dict):
            continue
        code = str(stock.get("code") or "").zfill(6)
        if not code or code in seen:
            continue
        name = str(stock.get("name") or "")
        ov_blob = " ".join(str(x) for x in (stock.get("name_keyword_overlap_with_news") or []))
        if _alias_in_blob(name, seed_t) or _alias_in_blob(ov_blob, seed_t):
            seen.add(code)
            matched.append(stock)
    matched.sort(key=lambda x: float(x.get("return_pct") or 0.0), reverse=True)
    return matched


def _compact_sector_label(label: str) -> str:
    """표·신호 열용 짧은 테마 라벨."""
    s = str(label or "").strip()
    return s.replace(" 관련주", "").strip() or s


def _sector_aliases_for_label(label: str) -> tuple[str, ...]:
    for sector_label, aliases in _THEME_SECTORS:
        if sector_label == label:
            return aliases
    return ()


def _stock_theme_keywords(stock: dict[str, Any]) -> list[str]:
    """종목명·뉴스·예측 키워드를 테마 매칭용으로 합칩니다."""
    parts: list[str] = []
    for key in ("theme_keywords", "name_keyword_overlap_with_news"):
        raw = stock.get(key)
        if isinstance(raw, list):
            parts.extend(str(x).strip() for x in raw if str(x).strip())
    return features.filter_keywords(parts)


def _score_keywords_against_sector(
    keywords: list[str],
    aliases: tuple[str, ...],
) -> int:
    """외부 키워드(뉴스·예측 교집합)와 섹터 별칭 적합도."""
    if not keywords:
        return 0
    blob = " ".join(str(k).lower() for k in keywords)
    best = 0
    for alias in aliases:
        a = str(alias).strip().lower()
        if len(a) < 2:
            continue
        if a in blob:
            best = max(best, 55 + len(a))
        for kw in keywords:
            kl = str(kw).lower()
            if a == kl:
                best = max(best, 60 + len(a))
            elif len(a) >= 2 and (a in kl or kl in a):
                best = max(best, 48 + min(len(a), len(kl)))
    return best


def _stock_name_tokens(stock: dict[str, Any]) -> set[str]:
    """종목명에서 테마 매칭용 토큰(전체 문자열 + keyword_set)."""
    name = str(stock.get("name") or "").strip()
    if not name:
        return set()
    toks = {t.lower() for t in features.filter_keywords(list(features.keyword_set(name, k=24)))}
    toks.add(name.lower())
    return toks


def _score_stock_sector_match(stock: dict[str, Any], aliases: tuple[str, ...]) -> int:
    """종목–섹터 적합도(클수록 강함). 0이면 미매칭."""
    name = str(stock.get("name") or "").lower()
    name_tokens = _stock_name_tokens(stock)
    theme_kw = _stock_theme_keywords(stock)
    best = _score_keywords_against_sector(theme_kw, aliases)
    overlap_blob = " ".join(str(x).lower() for x in theme_kw)
    for alias in aliases:
        a = str(alias).strip().lower()
        if len(a) < 2:
            continue
        if a in name:
            best = max(best, 100 + len(a))
        for tok in name_tokens:
            if a == tok:
                best = max(best, 92 + len(a))
            elif len(a) >= 2 and (a in tok or tok in a):
                best = max(best, 78 + min(len(a), len(tok)))
        if a in overlap_blob:
            best = max(best, 40 + len(a))
    return best


def _assign_sector_by_name_stem(
    stock: dict[str, Any],
    *,
    seeds: list[str],
    top_kw: list[str],
) -> str:
    """종목명 stem + 당일 active 뉴스 테마로 2차 섹터 추론."""
    name_l = str(stock.get("name") or "").lower()
    if not name_l:
        return ""
    for stem, sector_label in _NAME_STEM_SECTORS:
        s = stem.lower()
        if len(s) < 2 or s not in name_l:
            continue
        aliases = _sector_aliases_for_label(sector_label)
        if aliases and _sector_active_in_news(aliases, seeds, top_kw):
            return sector_label
    return ""


def _stock_return_pct_display(stock: dict[str, Any]) -> float:
    """종목 dict의 등락률(퍼센트 포인트). 없으면 0."""
    raw = stock.get("return_pct")
    if raw is None:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _dominant_sectors_from_assignments(
    assignments: dict[str, str],
    *,
    seeds: list[str],
    top_kw: list[str],
    min_count: int = 1,
) -> list[tuple[str, int]]:
    """이미 배정된 섹터 중 뉴스·시드가 살아 있거나 다수 급등이 모인 섹터."""
    from collections import Counter

    counts = Counter(assignments.values())
    out: list[tuple[str, int]] = []
    for label, cnt in counts.most_common():
        if cnt < min_count:
            continue
        aliases = _sector_aliases_for_label(label)
        if not aliases:
            continue
        if _sector_active_in_news(aliases, seeds, top_kw) or cnt >= 2:
            out.append((label, cnt))
    return out


def _assign_spillover_sectors(
    stocks: list[dict[str, Any]],
    assignments: dict[str, str],
    *,
    seeds: list[str],
    top_kw: list[str],
    min_return_pct: float = 10.0,
) -> None:
    """
    이름·키워드로 못 묶인 10%↑ 급등 종목을 당일 주도 테마(이미 배정된 섹터)에 동반 배정.

    KRX 테마장에서 종목명과 무관하게 같이 오르는 경우를 커버합니다.
    """
    if not assignments:
        return
    dominant = _dominant_sectors_from_assignments(
        assignments, seeds=seeds, top_kw=top_kw, min_count=1
    )
    if not dominant:
        return
    primary_label, primary_cnt = dominant[0]
    if primary_cnt < 2 and len(dominant) == 1:
        aliases = _sector_aliases_for_label(primary_label)
        if not aliases or not _sector_active_in_news(aliases, seeds, top_kw):
            return
    for stock in stocks:
        if not isinstance(stock, dict):
            continue
        code = str(stock.get("code") or "").zfill(6)
        if not code or code in assignments:
            continue
        if _stock_return_pct_display(stock) + 1e-9 < min_return_pct:
            continue
        best_label = ""
        best_score = 0
        for label, _cnt in dominant[:3]:
            aliases = _sector_aliases_for_label(label)
            if not aliases:
                continue
            sc = _score_stock_sector_match(stock, aliases)
            if sc > best_score:
                best_score = sc
                best_label = label
        if best_label and best_score > 0:
            assignments[code] = best_label
            continue
        assignments[code] = primary_label


def _assign_stock_sectors(
    stocks: list[dict[str, Any]],
    *,
    seeds: list[str],
    top_kw: list[str],
) -> dict[str, str]:
    """급등 종목마다 최적 테마 섹터 하나만 배정(중복·과다 테마 방지)."""
    assignments: dict[str, str] = {}
    for stock in stocks:
        if not isinstance(stock, dict):
            continue
        code = str(stock.get("code") or "").zfill(6)
        if not code:
            continue
        best_label = ""
        best_score = 0
        for sector_label, aliases in _THEME_SECTORS:
            sc = _score_stock_sector_match(stock, aliases)
            if sc <= 0:
                continue
            boost = 10 if _sector_active_in_news(aliases, seeds, top_kw) else 0
            effective = sc + boost
            if effective > best_score:
                best_score = effective
                best_label = sector_label
        if best_label:
            assignments[code] = best_label
    for stock in stocks:
        if not isinstance(stock, dict):
            continue
        code = str(stock.get("code") or "").zfill(6)
        if not code or code in assignments:
            continue
        stem_label = _assign_sector_by_name_stem(stock, seeds=seeds, top_kw=top_kw)
        if stem_label:
            assignments[code] = stem_label
    _assign_spillover_sectors(stocks, assignments, seeds=seeds, top_kw=top_kw)
    return assignments


def _stock_dict_for_theme_lookup(
    *,
    code: str,
    name: str,
    news_overlap: list[str] | None = None,
    theme_keywords: list[str] | None = None,
    return_pct: float | None = None,
) -> dict[str, Any]:
    """비교 표 행 등에서 on-demand 테마 배정용 최소 종목 dict."""
    kw = features.filter_keywords(list(theme_keywords or []) + list(news_overlap or []))
    out: dict[str, Any] = {
        "code": str(code).zfill(6),
        "name": str(name or "").strip(),
        "name_keyword_overlap_with_news": kw[:12],
        "theme_keywords": kw[:20],
    }
    if return_pct is not None and math.isfinite(float(return_pct)):
        out["return_pct"] = float(return_pct)
    return out


def theme_sectors_for_stock_dict(
    stock: dict[str, Any],
    *,
    seeds: list[str],
    top_kw: list[str],
    day_assignments: dict[str, str] | None = None,
) -> list[str]:
    """단일 종목 dict에 대한 당일 테마 섹터(최대 1개)."""
    code = str(stock.get("code") or "").zfill(6)
    if not code:
        return []
    if isinstance(day_assignments, dict):
        preset = day_assignments.get(code)
        if preset:
            return [_compact_sector_label(str(preset))]
    pool = [stock]
    assignments: dict[str, str] = {}
    for s in pool:
        c = str(s.get("code") or "").zfill(6)
        if not c:
            continue
        partial = _assign_stock_sectors([s], seeds=seeds, top_kw=top_kw)
        assignments.update(partial)
    if isinstance(day_assignments, dict) and day_assignments:
        spill_base = dict(day_assignments)
        spill_base.update(assignments)
        _assign_spillover_sectors(pool, spill_base, seeds=seeds, top_kw=top_kw)
        label = spill_base.get(code)
    else:
        label = assignments.get(code)
    return [_compact_sector_label(str(label))] if label else []


def _build_seed_theme_details(
    seed_hits: list[str],
    leaders_ge20: list[dict[str, Any]],
    top_kw: list[str],
    *,
    sector_summaries: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """테마 시드별 early 뉴스·연결 급등 종목(실제 20%↑ 종목이 있는 시드만)."""
    out: list[dict[str, Any]] = []
    seen_seeds: set[str] = set()
    generic = {str(x).lower() for x in _GENERIC_THEME_KW}

    for cl in sector_summaries or []:
        if not isinstance(cl, dict):
            continue
        tops = [s for s in (cl.get("top_stocks") or []) if isinstance(s, dict)]
        if not tops:
            continue
        sector = str(cl.get("sector") or "")
        news_aliases = list(cl.get("news_aliases") or [])
        seed = ""
        for alias in news_aliases:
            a = str(alias).strip()
            if a and a.lower() not in generic:
                seed = a
                break
        if not seed:
            seed = _compact_sector_label(sector)
        sk = seed.lower()
        if not seed or sk in seen_seeds:
            continue
        seen_seeds.add(sk)
        lim_n = int(cl.get("limit_up_count") or 0)
        cnt = int(cl.get("count") or len(tops))
        catalyst = str(cl.get("catalyst") or _catalyst_phrase(top_kw, seed))
        summary = f"20%↑ {cnt}종"
        if lim_n:
            summary += f"(상한 {lim_n})"
        out.append(
            {
                "seed": seed,
                "sectors": [sector] if sector else [],
                "catalyst": catalyst,
                "count_ge20": cnt,
                "limit_up_count": lim_n,
                "summary": summary,
                "top_stocks": tops[:8],
            }
        )

    for seed in seed_hits:
        sl = str(seed).strip()
        if len(sl) < 2 or sl.lower() in generic or sl.lower() in seen_seeds:
            continue
        sectors = _sectors_for_seed(sl)
        sector_labels = [label for label, _ in sectors]
        aliases_list = [aliases for _, aliases in sectors]
        movers = _movers_for_seed(sl, leaders_ge20, sector_aliases=aliases_list)
        if not movers:
            continue
        lim_n = sum(
            1
            for s in movers
            if s.get("is_limit_up") or float(s.get("return_pct") or 0) + 1e-9 >= _LIMIT_UP_PCT
        )
        kw_pool = [
            k
            for k in top_kw
            if sl.lower() in str(k).lower() or str(k).lower() in sl.lower()
        ]
        catalyst = _catalyst_phrase(kw_pool or top_kw, sl)
        summary = f"20%↑ {len(movers)}종"
        if lim_n:
            summary += f"(상한 {lim_n})"
        seen_seeds.add(sl.lower())
        out.append(
            {
                "seed": sl,
                "sectors": sector_labels[:2],
                "catalyst": catalyst,
                "count_ge20": len(movers),
                "limit_up_count": lim_n,
                "summary": summary,
                "top_stocks": movers[:8],
            }
        )

    out.sort(
        key=lambda x: (
            int(x.get("limit_up_count") or 0),
            int(x.get("count_ge20") or 0),
            len(str(x.get("seed") or "")),
        ),
        reverse=True,
    )
    cap = max(len(leaders_ge20), 1)
    return out[: min(len(out), cap)]


def _collect_theme_seed_hits(blob: str, top_kw: list[str], seeds: set[str]) -> list[str]:
    """early blob·상위 키워드에서 설정 테마 시드(반도체·AI 등) 적중 목록."""
    hits: list[str] = []
    seen: set[str] = set()
    blob_l = blob.lower() if blob else ""
    for s in sorted(seeds, key=len, reverse=True):
        if len(s) < 2 or s in seen:
            continue
        if s in blob_l:
            hits.append(s)
            seen.add(s)
    for w in top_kw:
        wl = str(w).lower()
        if wl in seeds and wl not in seen:
            hits.append(wl)
            seen.add(wl)
    return hits[:24]


def _enrich_seed_hits(
    seed_hits: list[str],
    top_kw: list[str],
    seeds_lexicon: set[str],
) -> list[str]:
    """설정 시드 적중이 적을 때 early 상위 키워드·섹터 별칭으로 테마 시드를 보강."""
    out = list(seed_hits)
    seen = {str(s).lower() for s in out}
    generic = {str(x).lower() for x in _GENERIC_THEME_KW}
    for k in top_kw[:50]:
        kl = str(k).strip().lower()
        if len(kl) < 2 or kl in seen or kl in generic:
            continue
        if kl in seeds_lexicon:
            out.append(kl)
            seen.add(kl)
            continue
        for _, aliases in _THEME_SECTORS:
            if any(kl == str(a).strip().lower() for a in aliases):
                out.append(kl)
                seen.add(kl)
                break
    return out[:24]


def _format_stock_brief(stock: dict[str, Any], *, with_news: bool = True) -> str:
    nm = str(stock.get("name") or "").strip()
    rp = float(stock.get("return_pct") or 0.0)
    lim = "·상한" if stock.get("is_limit_up") or rp + 1e-9 >= _LIMIT_UP_PCT else ""
    base = f"{nm} {rp:+.1f}%{lim}"
    if not with_news:
        return base
    ov = stock.get("name_keyword_overlap_with_news") or []
    if ov:
        return f"{base} ({', '.join(str(x) for x in ov[:3])})"
    return base


def _build_sector_summaries(
    row: dict[str, Any],
    *,
    leaders_ge20: list[dict[str, Any]],
    top_kw: list[str],
    seeds: list[str],
    stock_sector_map: dict[str, str] | None = None,
    leaders_ge10: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """테마 섹터별 급등 종목(종목당 1섹터·실제 급등 종목 기준)."""
    pool_for_news = list(seeds) + list(top_kw)
    assign_pool = list(leaders_ge20)
    if leaders_ge10:
        seen_codes = {str(s.get("code") or "").zfill(6) for s in assign_pool}
        for s in leaders_ge10:
            c = str(s.get("code") or "").zfill(6)
            if c and c not in seen_codes:
                assign_pool.append(s)
                seen_codes.add(c)
    assignments = stock_sector_map or _assign_stock_sectors(
        assign_pool, seeds=seeds, top_kw=top_kw
    )
    by_sector: dict[str, list[dict[str, Any]]] = {}
    for stock in leaders_ge20:
        if not isinstance(stock, dict):
            continue
        code = str(stock.get("code") or "").zfill(6)
        label = assignments.get(code)
        if not label:
            continue
        by_sector.setdefault(label, []).append(stock)

    out: list[dict[str, Any]] = []
    for sector_label, matched in by_sector.items():
        aliases = _sector_aliases_for_label(sector_label)
        if not matched:
            continue
        matched.sort(key=lambda x: float(x.get("return_pct") or 0.0), reverse=True)
        lim_n = sum(
            1
            for s in matched
            if s.get("is_limit_up") or float(s.get("return_pct") or 0) + 1e-9 >= _LIMIT_UP_PCT
        )
        catalyst = _catalyst_phrase(top_kw, aliases[0] if aliases else sector_label)
        news_aliases = [
            a for a in aliases if _alias_in_blob(" ".join(pool_for_news), (a,))
        ]
        out.append(
            {
                "sector": sector_label,
                "count": len(matched),
                "limit_up_count": lim_n,
                "catalyst": catalyst,
                "news_aliases": news_aliases[:4],
                "top_stocks": matched[:10],
            }
        )
    out.sort(
        key=lambda c: (
            int(c.get("limit_up_count") or 0),
            int(c.get("count") or 0),
            float(c["top_stocks"][0].get("return_pct") or 0) if c.get("top_stocks") else 0.0,
        ),
        reverse=True,
    )
    cap = max(len(leaders_ge20), 1)
    return out[: min(len(out), cap)]


def _build_theme_narratives(
    row: dict[str, Any],
    *,
    sector_summaries: list[dict[str, Any]] | None = None,
) -> list[str]:
    """테마·급등 종목을 구체 종목명·건수와 함께 서술."""
    seeds = list(row.get("theme_seed_hits") or [])
    top_kw = list(row.get("top_keywords_sample") or [])
    n_lim = int(row.get("n_limit_up") or 0)
    n20 = int(row.get("n_movers_ge_20pct") or 0)
    n10 = int(row.get("n_movers_10_to_20pct") or 0)
    narratives: list[str] = []

    if n_lim or n20 or n10:
        bits = []
        if n_lim:
            bits.append(f"상한가(≥{_LIMIT_UP_PCT:.0f}%) {n_lim}종")
        if n20:
            bits.append(f"20%↑ {n20}종")
        if n10:
            bits.append(f"10~20% {n10}종")
        narratives.append("당일 종가 기준 " + ", ".join(bits) + "이 동시다발적으로 출현.")

    sectors = sector_summaries if sector_summaries is not None else list(
        row.get("theme_sector_summaries") or []
    )
    for cl in sectors:
        if not isinstance(cl, dict):
            continue
        sector = str(cl.get("sector") or "")
        cnt = int(cl.get("count") or 0)
        lim_n = int(cl.get("limit_up_count") or 0)
        catalyst = str(cl.get("catalyst") or "")
        tops = cl.get("top_stocks") or []
        if not sector or cnt <= 0 or not tops:
            continue
        head = f"{catalyst}·early 뉴스({', '.join(cl.get('news_aliases') or []) or '테마'})와 맞물려 "
        if lim_n:
            head += f"{sector} {cnt}종(상한 {lim_n}) — "
        else:
            head += f"{sector} {cnt}종 — "
        body = ", ".join(_format_stock_brief(s, with_news=False) for s in tops[:6])
        if cnt > 6:
            body += f" 외 {cnt - 6}종"
        narratives.append(head + body)

    if not sectors and (seeds or top_kw):
        leaders = list(row.get("theme_leaders_ge_20pct") or row.get("theme_leaders_10pct_plus") or [])
        catalyst = _catalyst_phrase(top_kw, "")
        if leaders:
            sample = ", ".join(
                _format_stock_brief(s, with_news=False) for s in leaders[:8] if isinstance(s, dict)
            )
            narratives.append(
                f"{catalyst} 등 early 이슈 속 20%↑ {len(leaders)}종 — {sample}"
            )

    decliners = list(row.get("theme_decliners_8pct_minus") or [])
    if decliners and len(decliners) >= 3:
        dn = ", ".join(
            _format_stock_brief(s, with_news=False)
            for s in decliners[:4]
            if isinstance(s, dict)
        )
        narratives.append(f"강세 테마와 대비해 8%↓ 약세도 {len(decliners)}종 이상 — {dn} 등")

    return narratives[:14]


def _alias_in_blob(blob: str, aliases: tuple[str, ...]) -> bool:
    low = blob.lower()
    for alias in aliases:
        a = str(alias).strip().lower()
        if len(a) >= 2 and a in low:
            return True
    return False


def _stock_matches_sector(stock: dict[str, Any], aliases: tuple[str, ...]) -> bool:
    name = str(stock.get("name") or "")
    if _alias_in_blob(name, aliases):
        return True
    overlap = stock.get("name_keyword_overlap_with_news") or []
    overlap_blob = " ".join(str(x) for x in overlap)
    return _alias_in_blob(overlap_blob, aliases)


def _catalyst_phrase(top_kw: list[str], sector_key: str) -> str:
    """early 뉴스 상위 키워드에서 테마 문장용 원인 구절을 뽑습니다."""
    candidates = [
        str(k).strip()
        for k in top_kw
        if str(k).strip() and str(k).lower() not in _GENERIC_THEME_KW and str(k) != sector_key
    ]
    if not candidates:
        return sector_key
    eng = [k for k in candidates if re.fullmatch(r"[A-Za-z][A-Za-z0-9&.\-]{1,24}", k)]
    events = [k for k in candidates if any(ev in k for ev in _EVENT_KW)]
    if eng and events:
        lead = eng[0].upper() if eng[0].isascii() else eng[0]
        return f"{lead}의 {events[0]}"
    if events:
        return events[0]
    return candidates[0]


def _sector_active_in_news(aliases: tuple[str, ...], seeds: list, top_kw: list) -> bool:
    pool_blob = " ".join(str(x) for x in list(seeds) + list(top_kw))
    return _alias_in_blob(pool_blob, aliases)


def _theme_seed_lexicon() -> set[str]:
    """설정 뉴스 시드 쿼리에서 테마 프록시용 소문자 키워드 집합."""
    seeds: set[str] = set()
    extra = getattr(config, "NEWS_NAVER_MARKET_QUERY_SEEDS_EXTRA", None) or []
    for s in (
        config.NEWS_QUERY_SEEDS
        + config.GOOGLE_NEWS_RSS_QUERY_SEEDS_EXTRA
        + list(extra)
    ):
        t = str(s).strip().lower()
        if len(t) >= 2:
            seeds.add(t)
    return seeds


def build_market_theme_flow(
    trading_days: list[date],
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: pd.DataFrame,
    listing_names: dict[str, str],
) -> list[dict[str, Any]]:
    """
    거래일 T별로 early 뉴스 blob과 당일 수익률 분포를 묶어 테마·시장 흐름 요약을 만듭니다.

    테마 프록시: 설정 시드 키워드와 뉴스 상위 키워드의 교집합, 10%/20% 이상 급등 종목 수 및 상위 종목 샘플.
    """
    seeds = _theme_seed_lexicon()
    thr = float(config.BIG_MOVE_THRESHOLD)
    thr10 = 0.10
    out: list[dict[str, Any]] = []
    for t_day in sorted(set(trading_days)):
        blob = _news_blob_for_trading_day(news_by_calendar, t_day)
        top_kw = features.top_keywords(blob, k=80) if blob.strip() else []
        seed_hits = _collect_theme_seed_hits(blob, top_kw, seeds)
        seed_hits = _enrich_seed_hits(seed_hits, top_kw, seeds)
        day_slice = returns_df[returns_df["Date"] == pd.Timestamp(t_day)]
        n_lim = int((day_slice["return_pct"] >= 0.295).sum()) if not day_slice.empty else 0
        n20 = int((day_slice["return_pct"] >= thr).sum()) if not day_slice.empty else 0
        n10 = int(
            ((day_slice["return_pct"] >= thr10) & (day_slice["return_pct"] < thr)).sum()
        ) if not day_slice.empty else 0
        kw_set = features.keyword_set(blob, k=150) if blob.strip() else frozenset()

        leaders_10: list[dict[str, Any]] = []
        leaders_20: list[dict[str, Any]] = []
        limit_up: list[dict[str, Any]] = []
        decliners: list[dict[str, Any]] = []

        leaders_10_all: list[dict[str, Any]] = []
        if not day_slice.empty:
            ge10_slice = day_slice[day_slice["return_pct"] >= thr10].sort_values(
                "return_pct", ascending=False
            )
            for _, stock_row in ge10_slice.iterrows():
                sd = _stock_dict(stock_row, listing_names, kw_set)
                leaders_10_all.append(sd)
                if float(stock_row["return_pct"]) >= thr:
                    leaders_20.append(sd)
                if sd.get("is_limit_up"):
                    limit_up.append(sd)
            leaders_10 = leaders_10_all[:40]
            for _, stock_row in (
                day_slice[day_slice["return_pct"] <= -0.08]
                .sort_values("return_pct", ascending=True)
                .head(15)
                .iterrows()
            ):
                decliners.append(_stock_dict(stock_row, listing_names, kw_set))

        assign_pool = list(leaders_20)
        seen_assign = {str(s.get("code") or "").zfill(6) for s in assign_pool}
        for s in leaders_10_all:
            c = str(s.get("code") or "").zfill(6)
            if c and c not in seen_assign:
                assign_pool.append(s)
                seen_assign.add(c)
        stock_sector_map = _assign_stock_sectors(
            assign_pool, seeds=seed_hits, top_kw=top_kw
        )
        sector_summaries = _build_sector_summaries(
            {},
            leaders_ge20=leaders_20,
            top_kw=top_kw,
            seeds=seed_hits,
            stock_sector_map=stock_sector_map,
            leaders_ge10=leaders_10_all,
        )
        seed_details = _build_seed_theme_details(
            seed_hits, leaders_20, top_kw, sector_summaries=sector_summaries
        )
        flow_row = {
            "trading_day": t_day.isoformat(),
            "news_chars": len(blob),
            "top_keywords_sample": top_kw[:30],
            "theme_seed_hits": seed_hits,
            "theme_seed_details": seed_details,
            "theme_stock_sectors": stock_sector_map,
            "n_limit_up": n_lim,
            "n_movers_ge_20pct": n20,
            "n_movers_10_to_20pct": n10,
            "theme_limit_up": limit_up[:20],
            "theme_leaders_ge_20pct": leaders_20[:30],
            "theme_leaders_10pct_plus": leaders_10[:40],
            "theme_decliners_8pct_minus": decliners,
            "theme_sector_summaries": sector_summaries,
        }
        flow_row["theme_narratives"] = _build_theme_narratives(
            flow_row, sector_summaries=sector_summaries
        )
        out.append(flow_row)
    return out


def theme_sectors_for_code(row: dict[str, Any] | None, code: str) -> list[str]:
    """``market_theme_flow`` 일자 행에서 종목의 당일 테마 섹터(최대 1개)."""
    if not row:
        return []
    c = str(code).zfill(6)
    mapping = row.get("theme_stock_sectors")
    if isinstance(mapping, dict):
        label = mapping.get(c)
        if label:
            return [_compact_sector_label(str(label))]
    stock: dict[str, Any] | None = None
    for key in ("theme_leaders_ge_20pct", "theme_leaders_10pct_plus"):
        for leader in row.get(key) or []:
            if not isinstance(leader, dict):
                continue
            if str(leader.get("code", "")).zfill(6) == c:
                stock = leader
                break
        if stock is not None:
            break
    if stock is not None:
        sectors = theme_sectors_for_stock_dict(
            stock,
            seeds=list(row.get("theme_seed_hits") or []),
            top_kw=list(row.get("top_keywords_sample") or []),
            day_assignments=(
                row.get("theme_stock_sectors")
                if isinstance(row.get("theme_stock_sectors"), dict)
                else None
            ),
        )
        if sectors:
            return sectors
    for cl in row.get("theme_sector_summaries") or []:
        if not isinstance(cl, dict):
            continue
        sector = str(cl.get("sector") or "")
        for stock in cl.get("top_stocks") or []:
            if not isinstance(stock, dict):
                continue
            if str(stock.get("code", "")).zfill(6) == c and sector:
                return [_compact_sector_label(sector)]
    return []


def format_theme_sectors_signal_html(sectors: list[str]) -> str:
    """비교 표 ``예측 신호`` 열에 붙일 당일 테마 pill HTML."""
    labels = [_compact_sector_label(s) for s in sectors if str(s).strip()]
    if not labels:
        return ""
    pills = "".join(
        f'<span class="pill" style="margin:2px 0 0;font-size:0.68rem;'
        f'background:#1a2e42;color:var(--ok)">{html.escape(l, quote=True)}</span> '
        for l in labels[:2]
    )
    return (
        '<br/><span style="font-size:0.78rem;line-height:1.6" '
        'title="당일 급등 테마(종목명·뉴스 휴리스틱, 참고용)">'
        f"테마 {pills}</span>"
    )


def theme_overlap_for_code(row: dict[str, Any] | None, code: str) -> list[str]:
    """``market_theme_flow`` 일자 행에서 종목코드별 early 뉴스·종목명 키워드 겹침."""
    if not row:
        return []
    c = str(code).zfill(6)
    for key in ("theme_leaders_ge_20pct", "theme_leaders_10pct_plus", "theme_limit_up"):
        for leader in row.get(key) or []:
            if not isinstance(leader, dict):
                continue
            if str(leader.get("code", "")).zfill(6) == c:
                ov = leader.get("name_keyword_overlap_with_news")
                if isinstance(ov, list):
                    return [str(x) for x in ov if str(x).strip()]
    return []


def format_market_theme_flow_html(
    row: dict[str, Any] | None,
    *,
    news_cutoff_label: str,
    threshold_pct: float = 20.0,
) -> str:
    """
    ``build_market_theme_flow`` 한 일자 결과를 리포트용 HTML로 변환합니다.

    뉴스 blob은 관측일 ``T`` 예측·학습과 동일한 **early**(``T`` 직전 거래일 ``news_cutoff_label`` 까지)입니다.
    """
    if not row:
        return ""
    t_iso = str(row.get("trading_day") or "")
    try:
        t_day = date.fromisoformat(t_iso)
    except ValueError:
        t_day = None
    n_buy = (
        trading_calendar.last_trading_day_before(t_day).isoformat()
        if t_day is not None
        else "직전 거래일"
    )
    n_lim = int(row.get("n_limit_up") or 0)
    n20 = int(row.get("n_movers_ge_20pct") or 0)
    n10 = int(row.get("n_movers_10_to_20pct") or 0)
    seeds = row.get("theme_seed_hits") or []
    top_kw = row.get("top_keywords_sample") or []
    leaders_20 = row.get("theme_leaders_ge_20pct") or row.get("theme_leaders_10pct_plus") or []
    limit_up = row.get("theme_limit_up") or []
    sectors = row.get("theme_sector_summaries") or []
    news_chars = int(row.get("news_chars") or 0)

    def _esc(s: str) -> str:
        return html.escape(s, quote=True)

    sector_summaries = sectors if sectors else _build_sector_summaries(
        row,
        leaders_ge20=[x for x in leaders_20 if isinstance(x, dict)],
        top_kw=list(top_kw),
        seeds=list(seeds),
        stock_sector_map=(
            row.get("theme_stock_sectors")
            if isinstance(row.get("theme_stock_sectors"), dict)
            else None
        ),
    )
    narratives = list(row.get("theme_narratives") or []) or _build_theme_narratives(
        row, sector_summaries=sector_summaries
    )

    stat_pills = []
    if n_lim:
        stat_pills.append(
            f'<span class="pill" style="background:#3d2a1a;color:var(--warn)">'
            f"상한≥{_LIMIT_UP_PCT:.0f}% {_esc(str(n_lim))}종</span>"
        )
    if n20:
        stat_pills.append(
            f'<span class="pill" style="background:#1e3d2f;color:var(--ok)">'
            f"20%↑ {_esc(str(n20))}종</span>"
        )
    if n10:
        stat_pills.append(
            f'<span class="pill">10~20% {_esc(str(n10))}종</span>'
        )

    parts = [
        "<p class=\"sub\" style=\"margin:0 0 8px;line-height:1.55\">",
        f"<strong>{_esc(t_iso)}</strong> 종가 · early 입력 "
        f"<strong>{_esc(n_buy)}</strong> {_esc(news_cutoff_label)}(KST) · "
        f"뉴스 약 {_esc(f'{news_chars:,}')}자",
        "</p>",
    ]
    if stat_pills:
        parts.append(
            '<p class="theme-stats-pills" style="margin:0 0 12px;line-height:2">'
            + " ".join(stat_pills)
            + "</p>"
        )

    if narratives:
        parts.append('<h4 class="combo-tip-h" style="margin:0 0 6px">시장·테마 요약</h4>')
        parts.append("<ul class=\"nl theme-sector-block\" style=\"margin:0 0 12px\">")
        for sent in narratives:
            parts.append(f"<li style=\"margin-bottom:8px;line-height:1.5\">{_esc(sent)}</li>")
        parts.append("</ul>")

    if sector_summaries:
        parts.append('<h4 class="combo-tip-h" style="margin:0 0 6px">테마별 급등(종목명·등락)</h4>')
        parts.append('<ul class="nl theme-sector-block" style="margin:0 0 12px">')
        for cl in sector_summaries:
            if not isinstance(cl, dict):
                continue
            sector = str(cl.get("sector") or "")
            cnt = int(cl.get("count") or 0)
            lim_n = int(cl.get("limit_up_count") or 0)
            tops = cl.get("top_stocks") or []
            if not sector or not tops:
                continue
            title = f"{_esc(sector)} · {cnt}종"
            if lim_n:
                title += f" (상한 {lim_n})"
            stock_bits = [
                _esc(_format_stock_brief(s, with_news=True))
                for s in tops[:8]
                if isinstance(s, dict)
            ]
            parts.append(
                f'<li style="margin-bottom:8px;line-height:1.55">'
                f"<strong>{title}</strong><br/>"
                f'<span style="font-size:0.82rem;color:#d0dce8">{", ".join(stock_bits)}</span>'
                f"</li>"
            )
        parts.append("</ul>")

    seed_details = row.get("theme_seed_details") or []
    if not seed_details and seeds and sector_summaries:
        seed_details = _build_seed_theme_details(
            list(seeds),
            [x for x in leaders_20 if isinstance(x, dict)],
            list(top_kw),
            sector_summaries=sector_summaries,
        )
    seed_details = [
        sd for sd in seed_details if isinstance(sd, dict) and (sd.get("top_stocks") or [])
    ]
    if seed_details:
        parts.append(
            '<h4 class="combo-tip-h" style="margin:0 0 6px">'
            "테마 시드 (early 뉴스 적중 → 당일 급등 연결)</h4>"
        )
        parts.append(
            '<ul class="nl theme-seed-block" style="margin:0 0 12px">'
        )
        for sd in seed_details:
            if not isinstance(sd, dict):
                continue
            seed = str(sd.get("seed") or "")
            tops = sd.get("top_stocks") or []
            if not seed or not tops:
                continue
            sectors = sd.get("sectors") or []
            summary = str(sd.get("summary") or "")
            catalyst = str(sd.get("catalyst") or "")
            title = f'<span class="pill" style="margin-right:6px">{_esc(seed)}</span>'
            if sectors:
                title += _esc(" → " + " · ".join(str(x) for x in sectors[:2]))
            if summary:
                title += f' <span style="color:var(--ok);font-size:0.84rem">({_esc(summary)})</span>'
            body_parts: list[str] = []
            if catalyst and catalyst.lower() != seed.lower():
                body_parts.append(
                    f'<span style="color:var(--muted);font-size:0.8rem">'
                    f"early 이슈: {_esc(catalyst)}</span>"
                )
            if tops:
                stock_bits = [
                    _esc(_format_stock_brief(s, with_news=True))
                    for s in tops[:6]
                    if isinstance(s, dict)
                ]
                cnt = int(sd.get("count_ge20") or len(tops))
                tail = f" 외 {cnt - 6}종" if cnt > 6 else ""
                body_parts.append(
                    f'<span style="font-size:0.82rem;color:#d0dce8">'
                    f"{', '.join(stock_bits)}{_esc(tail)}</span>"
                )
            parts.append(
                f'<li style="margin-bottom:10px;line-height:1.55">'
                f"{title}"
                + (f"<br/>{' · '.join(body_parts)}" if body_parts else "")
                + "</li>"
            )
        parts.append("</ul>")
        generic_seeds = [
            s
            for s in seeds
            if str(s).lower() in {str(x).lower() for x in _GENERIC_THEME_KW}
        ]
        if generic_seeds:
            g_pills = "".join(
                f'<span class="pill" style="margin:2px 4px 2px 0;font-size:0.72rem">'
                f"{_esc(str(s))}</span>"
                for s in generic_seeds[:8]
            )
            parts.append(
                '<p style="margin:0 0 10px;font-size:0.78rem;color:var(--muted)">'
                f"<strong>시장·거시 시드</strong> {g_pills}</p>"
            )
    elif seeds:
        seed_pills = "".join(
            f'<span class="pill" style="margin:2px 4px 2px 0">{_esc(str(s))}</span>'
            for s in seeds[:18]
        )
        parts.append('<h4 class="combo-tip-h" style="margin:0 0 6px">테마 시드 (early 뉴스 적중)</h4>')
        parts.append(
            f'<p class="theme-seed-pills" style="margin:0 0 10px;line-height:1.9">{seed_pills}</p>'
        )

    extra_kw = [k for k in top_kw if str(k).lower() not in {str(s).lower() for s in seeds}]
    if extra_kw:
        kw_pills = "".join(
            f'<span class="pill" style="margin:2px 4px 2px 0;font-size:0.72rem">{_esc(str(k))}</span>'
            for k in extra_kw[:14]
        )
        parts.append(
            '<p style="margin:0 0 10px;font-size:0.78rem;color:var(--muted)">'
            f"<strong>기타 early 키워드</strong> {kw_pills}</p>"
        )

    if limit_up:
        lim_lines = [
            _esc(_format_stock_brief(s, with_news=True))
            for s in limit_up[:12]
            if isinstance(s, dict)
        ]
        parts.append(
            '<p class="theme-mover-table" style="margin:0 0 10px;font-size:0.82rem;line-height:1.55">'
            f"<strong>상한가·{_LIMIT_UP_PCT:.0f}%↑</strong> — {', '.join(lim_lines)}</p>"
        )
    elif leaders_20:
        top20_lines = [
            _esc(_format_stock_brief(s, with_news=True))
            for s in leaders_20[:12]
            if isinstance(s, dict)
        ]
        parts.append(
            '<p class="theme-mover-table" style="margin:0 0 10px;font-size:0.82rem;line-height:1.55">'
            f"<strong>20%↑ 상위</strong> — {', '.join(top20_lines)}</p>"
        )

    if not narratives and not sector_summaries and not leaders_20:
        parts.append(
            "<p class=\"sub\" style=\"margin:0 0 10px\">"
            "당일 뚜렷한 테마·급등 패턴을 만들 데이터가 부족합니다.</p>"
        )

    parts.append(
        "<p class=\"combo-tip-empty\" style=\"margin:10px 0 0;font-size:0.78em\">"
        "자동 생성·참고용입니다. 종목명·뉴스 키워드·당일 등락을 휴리스틱으로 묶었으며 인과 단정이 아닙니다.</p>"
    )
    return "".join(parts)


def market_theme_html_is_incomplete(html: str) -> bool:
    """리포트 「당일 테마 요약」이 장 마감 후에도 채울 여지가 있는지(비었거나 placeholder)."""
    s = (html or "").strip()
    if not s:
        return True
    if "등락 패턴이 부족" in s:
        return True
    if "theme-seed-pills" in s and "theme-seed-block" not in s:
        return True
    if 'class="nl"' in s and "<li" in s:
        return False
    if (
        "theme-sector-block" in s
        or "theme-mover-table" in s
        or "theme-seed-block" in s
    ):
        return False
    if "theme-stats-pills" in s and "20%↑" in s:
        return False
    return True


def market_theme_html_for_trading_day(
    t_day: date,
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: pd.DataFrame,
    listing_names: dict[str, str],
    *,
    news_cutoff_label: str,
    threshold_pct: float | None = None,
) -> str:
    """거래일 하나에 대한 ``당일 테마 요약`` HTML(내부 블록만)."""
    thr = (
        float(threshold_pct)
        if threshold_pct is not None
        else float(config.BIG_MOVE_THRESHOLD) * 100.0
    )
    rows = build_market_theme_flow(
        [t_day], news_by_calendar, returns_df, listing_names
    )
    row = rows[0] if rows else None
    return format_market_theme_flow_html(
        row,
        news_cutoff_label=news_cutoff_label,
        threshold_pct=thr,
    )


def enrich_day_reports_market_theme(
    day_reports: list[Any],
    news_by_calendar: dict[date, list[dict[str, str]]],
    returns_df: pd.DataFrame,
    listing_names: dict[str, str],
    *,
    news_cutoff_label: str,
) -> list[dict[str, Any]]:
    """
    ``DayReport`` 목록에 ``market_theme_html`` 을 채우고, 비교 행 ``rise_reason_html`` 에
    early 뉴스·종목명 겹침 한 줄을 덧붙입니다. 스냅샷 병합용 ``market_theme_flow`` 행 목록을 반환합니다.
    """
    from . import move_reference

    days = sorted({dr.trading_day for dr in day_reports})
    if not days:
        return []
    theme_rows = build_market_theme_flow(
        days, news_by_calendar, returns_df, listing_names
    )
    by_day = {str(r.get("trading_day")): r for r in theme_rows if r.get("trading_day")}
    thr_pct = float(config.BIG_MOVE_THRESHOLD) * 100.0
    for dr in day_reports:
        row = by_day.get(dr.trading_day.isoformat())
        dr.market_theme_html = format_market_theme_flow_html(
            row,
            news_cutoff_label=news_cutoff_label,
            threshold_pct=thr_pct,
        )
        if not row:
            continue
        for r in dr.rows_compare or []:
            code = str(r.get("code", "")).zfill(6)
            seeds = list(row.get("theme_seed_hits") or [])
            top_kw = list(row.get("top_keywords_sample") or [])
            day_map = row.get("theme_stock_sectors")
            day_map = day_map if isinstance(day_map, dict) else {}
            ov_pre = theme_overlap_for_code(row, code)
            pred_kw = [str(k) for k in (r.get("keywords") or []) if str(k).strip()]
            act = r.get("actual_ret")
            rp: float | None = None
            if act is not None:
                try:
                    rp = float(act) * 100.0
                except (TypeError, ValueError):
                    rp = None
            sectors = theme_sectors_for_code(row, code)
            if not sectors:
                adhoc = _stock_dict_for_theme_lookup(
                    code=code,
                    name=str(r.get("name") or ""),
                    news_overlap=ov_pre,
                    theme_keywords=pred_kw + ov_pre,
                    return_pct=rp,
                )
                sectors = theme_sectors_for_stock_dict(
                    adhoc,
                    seeds=seeds,
                    top_kw=top_kw,
                    day_assignments=day_map,
                )
            r["market_theme_sectors"] = sectors
            act = r.get("actual_ret")
            if sectors and act is not None:
                try:
                    act_f = float(act)
                except (TypeError, ValueError):
                    act_f = None
                if act_f is not None and act_f + 1e-9 >= 0.10:
                    base = str(r.get("prediction_signal_html") or "—")
                    theme_bit = format_theme_sectors_signal_html(sectors)
                    if theme_bit and theme_bit not in base:
                        r["prediction_signal_html"] = base + theme_bit
            ov = theme_overlap_for_code(row, code)
            extra = move_reference.theme_early_overlap_html(ov, news_cutoff_label=news_cutoff_label)
            if extra:
                base = r.get("rise_reason_html") or ""
                r["rise_reason_html"] = base + extra
    return theme_rows


def _merge_by_trading_day(
    old_rows: list[dict[str, Any]] | None,
    new_rows: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """``trading_day`` 키로 일자별 행을 병합. 동일 일자는 ``new_rows`` 가 덮어씀."""
    by: dict[str, dict[str, Any]] = {}
    for row in old_rows or []:
        if isinstance(row, dict) and row.get("trading_day"):
            by[str(row["trading_day"])] = dict(row)
    for row in new_rows or []:
        if isinstance(row, dict) and row.get("trading_day"):
            by[str(row["trading_day"])] = dict(row)
    return sorted(by.values(), key=lambda x: str(x.get("trading_day", "")))


def _recompute_rebuild_learning_daily_and_summary(
    daily: list[dict[str, Any]], thr: float
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """병합된 ``daily`` 에 누적 통계 필드를 채우고 구간 ``summary`` 를 재계산."""
    cum_abs_gap = 0.0
    cum_gap_n = 0
    cum_ph_hits = 0
    cum_ph_den = 0
    total_over_pred = 0
    total_under_pred = 0
    total_false_high = 0
    total_false_high_neg = 0
    total_false_high_low = 0
    total_false_high_late_hit = 0
    total_false_high_weak_signal = 0
    false_high_kw_counter: dict[str, int] = {}
    fixed: list[dict[str, Any]] = []
    for d in daily:
        row = dict(d)
        n_both = int(row.get("rows_pred_actual_both") or 0)
        mae = row.get("mean_abs_gap_pred_minus_actual_pct")
        if mae is not None and n_both > 0 and isinstance(mae, (int, float)):
            cum_abs_gap += float(mae) * float(n_both)
            cum_gap_n += n_both
        ph_den = int(row.get("pred_high_n_with_actual_today") or 0)
        ph_prec = row.get("pred_high_precision_today")
        if ph_den > 0 and ph_prec is not None:
            cum_ph_hits += int(round(float(ph_prec) * ph_den))
            cum_ph_den += ph_den
        total_over_pred += int(row.get("over_pred_count_today") or 0)
        total_under_pred += int(row.get("under_pred_count_today") or 0)
        total_false_high += int(row.get("false_positive_high_today") or 0)
        total_false_high_neg += int(row.get("false_positive_high_negative_today") or 0)
        total_false_high_low += int(row.get("false_positive_high_low_return_today") or 0)
        total_false_high_late_hit += int(
            row.get("false_positive_high_late_news_hit_today") or 0
        )
        total_false_high_weak_signal += int(
            row.get("false_positive_high_weak_signal_today") or 0
        )
        kw_rows = row.get("false_positive_high_top_keywords_today")
        if isinstance(kw_rows, list):
            for item in kw_rows:
                if not isinstance(item, dict):
                    continue
                k = str(item.get("keyword") or "").strip()
                c = int(item.get("count") or 0)
                if not k or c <= 0:
                    continue
                false_high_kw_counter[k] = false_high_kw_counter.get(k, 0) + c
        row["cum_through_mean_abs_gap_pct"] = (
            round(cum_abs_gap / cum_gap_n, 4) if cum_gap_n else None
        )
        row["pred_high_precision_cumulative"] = (
            round(cum_ph_hits / cum_ph_den, 4) if cum_ph_den else None
        )
        row["pred_high_n_with_actual_cumulative"] = cum_ph_den
        row["over_pred_count_cumulative"] = total_over_pred
        row["under_pred_count_cumulative"] = total_under_pred
        row["false_positive_high_cumulative"] = total_false_high
        fixed.append(row)
    summary = {
        "days": len(fixed),
        "final_cum_mean_abs_gap_pct": round(cum_abs_gap / cum_gap_n, 4) if cum_gap_n else None,
        "final_pred_high_precision": round(cum_ph_hits / cum_ph_den, 4) if cum_ph_den else None,
        "pred_high_total_with_actual": cum_ph_den,
        "big_move_threshold": thr,
        "over_pred_count_total": total_over_pred,
        "under_pred_count_total": total_under_pred,
        "false_positive_high_total": total_false_high,
        "false_positive_high_negative_total": total_false_high_neg,
        "false_positive_high_low_return_total": total_false_high_low,
        "false_positive_high_late_news_hit_total": total_false_high_late_hit,
        "false_positive_high_weak_signal_total": total_false_high_weak_signal,
        "false_positive_high_top_keywords_total": [
            {"keyword": k, "count": int(v)}
            for k, v in sorted(
                false_high_kw_counter.items(),
                key=lambda x: (-int(x[1]), x[0]),
            )[:20]
        ],
    }
    return fixed, summary


def merge_rebuild_learning_dict(old: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    """기존·신규 ``rebuild_learning`` 을 일자 병합 후 요약·``merge_runs`` 를 갱신."""
    old = old if isinstance(old, dict) else {}
    thr = float(
        new.get("big_move_threshold")
        or old.get("big_move_threshold")
        or config.BIG_MOVE_THRESHOLD
    )
    merged_daily = _merge_by_trading_day(
        old.get("daily") if isinstance(old.get("daily"), list) else [],
        new.get("daily") if isinstance(new.get("daily"), list) else [],
    )
    fixed_daily, summary = _recompute_rebuild_learning_daily_and_summary(merged_daily, thr)
    summary = {**summary, **snapshot_miss_diagnosis.recompute_miss_summary_from_daily(fixed_daily)}
    runs = list(old.get("merge_runs") or [])
    runs.append(
        {
            "at": datetime.now().isoformat(timespec="seconds"),
            "calendar_from": new.get("calendar_from"),
            "calendar_to": new.get("calendar_to"),
            "days_merged": len(new.get("daily") or []),
        }
    )
    iso_dates = [
        str(new.get("calendar_from") or ""),
        str(new.get("calendar_to") or ""),
        str(old.get("calendar_from") or ""),
        str(old.get("calendar_to") or ""),
    ]
    iso_dates = [x for x in iso_dates if len(x) >= 10]
    cal_from = min(iso_dates) if iso_dates else str(new.get("calendar_from") or "")
    cal_to = max(iso_dates) if iso_dates else str(new.get("calendar_to") or "")
    return {
        "calendar_from": cal_from,
        "calendar_to": cal_to,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "big_move_threshold": thr,
        "daily": fixed_daily,
        "summary": summary,
        "merge_runs": runs[-24:],
    }


def _merge_gap_rollup(
    old: dict[str, Any] | None, new: dict[str, Any]
) -> dict[str, Any]:
    """``prediction_gap_rollup`` 에 새 구간 스냅샷을 추가하고 ``latest`` 를 갱신."""
    hist = list((old or {}).get("range_exports") or [])
    hist.append(new)
    hist = hist[-36:]
    return {
        "latest": new,
        "range_exports": hist,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def merge_extended_rebuild_into_snapshot(
    *,
    rebuild_learning: dict[str, Any],
    market_theme_flow: list[dict[str, Any]],
    prediction_gap_rollup: dict[str, Any],
    path: Path | None = None,
) -> bool:
    """기존 스냅샷 JSON에 확장 필드를 병합 저장합니다."""
    p = path or config.TRAIN_SNAPSHOT_PATH
    if not p.is_file():
        return False
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, TypeError):
        return False
    if int(data.get("format_version", 0)) != train_snapshot.FORMAT_VERSION:
        return False
    data["rebuild_learning"] = merge_rebuild_learning_dict(
        data.get("rebuild_learning"), rebuild_learning
    )
    data["market_theme_flow"] = _merge_by_trading_day(
        data.get("market_theme_flow") if isinstance(data.get("market_theme_flow"), list) else [],
        market_theme_flow,
    )
    data["prediction_gap_rollup"] = _merge_gap_rollup(
        data.get("prediction_gap_rollup") if isinstance(data.get("prediction_gap_rollup"), dict) else None,
        prediction_gap_rollup,
    )
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=0)
    except OSError:
        return False
    return True
