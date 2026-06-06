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
    ("로봇·자동화 관련주", ("로봇", "휴머노이드", "자동화", "스마트팩토리", "협동로봇")),
    ("조선·해운 관련주", ("조선", "LNG선", "해운", "선박", "컨테이너선")),
    ("방산·국방 관련주", ("방산", "국방", "무기", "K방산", "우주항공")),
    ("원전·SMR 관련주", ("원전", "SMR", "핵융합", "원자력", "모듈원전")),
    ("전력·전력기기 관련주", ("전력", "전력기기", "변압기", "송전", "전선", "전기차충전")),
    ("전기차·모빌리티 관련주", ("전기차", "EV", "완성차", "자율주행", "모빌리티", "충전소")),
    ("디스플레이 관련주", ("디스플레이", "OLED", "LCD", "패널", "마이크로LED")),
    ("게임·엔터 관련주", ("게임", "엔터", "콘텐츠", "IP", "웹툰", "OTT")),
    ("화장품·뷰티 관련주", ("화장품", "K뷰티", "뷰티", "코스메틱")),
    ("유통·소비재 관련주", ("유통", "소비", "백화점", "면세", "리테일", "음식료")),
    ("건설·부동산 관련주", ("건설", "부동산", "리츠", "재건축", "인프라", "토목")),
    ("금융·은행 관련주", ("금융", "은행", "보험", "증권", "카드", "핀테크", "결제")),
    ("화학·소재 관련주", ("화학", "석유화학", "정유", "소재", "철강", "비철", "골판지")),
    ("수소·연료전지 관련주", ("수소", "연료전지", "그린수소", "암모니아")),
    ("신재생·에너지 관련주", ("태양광", "풍력", "신재생", "ESS", "에너지저장")),
    ("통신·플랫폼 관련주", ("통신", "5G", "6G", "플랫폼", "클라우드", "SaaS", "IT")),
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


def _build_theme_narratives(row: dict[str, Any]) -> list[str]:
    """'○○○ 이유로 로봇 관련주가 상승함' 형태의 서술 문장 목록."""
    seeds = list(row.get("theme_seed_hits") or [])
    top_kw = list(row.get("top_keywords_sample") or [])
    leaders = list(row.get("theme_leaders_10pct_plus") or [])
    decliners = list(row.get("theme_decliners_8pct_minus") or [])
    narratives: list[str] = []
    for sector_label, aliases in _THEME_SECTORS:
        if not _sector_active_in_news(aliases, seeds, top_kw):
            continue
        up_n = sum(
            1 for s in leaders if isinstance(s, dict) and _stock_matches_sector(s, aliases)
        )
        down_n = sum(
            1 for s in decliners if isinstance(s, dict) and _stock_matches_sector(s, aliases)
        )
        if up_n == 0 and down_n == 0:
            continue
        catalyst = _catalyst_phrase(top_kw, aliases[0])
        if up_n >= down_n and up_n > 0:
            narratives.append(f"{catalyst} 등으로 {sector_label} 부분이 상승함")
        elif down_n > 0:
            narratives.append(f"{catalyst}로 {sector_label}가 하락함")
    if not narratives and (seeds or top_kw):
        catalyst = _catalyst_phrase(top_kw, "")
        if leaders and not decliners:
            narratives.append(f"{catalyst} 등으로 시장 내 강세 종목이 다수 출현함")
        elif decliners and not leaders:
            narratives.append(f"{catalyst} 등으로 시장 내 약세 종목이 다수 출현함")
        elif leaders or decliners:
            narratives.append(
                f"{catalyst} 등으로 당일 강세·약세 종목이 섞여 나타남"
            )
    return narratives[:12]


def _theme_seed_lexicon() -> set[str]:
    """설정 뉴스 시드 쿼리에서 테마 프록시용 소문자 키워드 집합."""
    seeds: set[str] = set()
    for s in config.NEWS_QUERY_SEEDS + config.GOOGLE_NEWS_RSS_QUERY_SEEDS_EXTRA:
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
        top_kw = features.top_keywords(blob, k=55) if blob.strip() else []
        seed_hits = [w for w in top_kw if str(w).lower() in seeds]
        day_slice = returns_df[returns_df["Date"] == pd.Timestamp(t_day)]
        n20 = int((day_slice["return_pct"] >= thr).sum()) if not day_slice.empty else 0
        n10 = int(
            ((day_slice["return_pct"] >= thr10) & (day_slice["return_pct"] < thr)).sum()
        ) if not day_slice.empty else 0
        movers = (
            day_slice[day_slice["return_pct"] >= thr10]
            .sort_values("return_pct", ascending=False)
            .head(18)
        )
        losers = (
            day_slice[day_slice["return_pct"] <= -0.08]
            .sort_values("return_pct", ascending=True)
            .head(12)
        )
        leaders: list[dict[str, Any]] = []
        decliners: list[dict[str, Any]] = []
        kw_set = features.keyword_set(blob, k=120) if blob.strip() else frozenset()

        def _append_stock(bucket: list[dict[str, Any]], stock_row: pd.Series) -> None:
            code = str(stock_row["Code"]).zfill(6)
            name = str(stock_row.get("Name") or listing_names.get(code, ""))
            rp = float(stock_row["return_pct"])
            name_kw = features.keyword_set(name, k=12)
            overlap = sorted(name_kw & kw_set)[:6]
            bucket.append(
                {
                    "code": code,
                    "name": name,
                    "return_pct": round(rp * 100.0, 3),
                    "name_keyword_overlap_with_news": overlap,
                }
            )

        for _, row in movers.iterrows():
            _append_stock(leaders, row)
        for _, row in losers.iterrows():
            _append_stock(decliners, row)
        flow_row = {
            "trading_day": t_day.isoformat(),
            "news_chars": len(blob),
            "top_keywords_sample": top_kw[:24],
            "theme_seed_hits": seed_hits[:20],
            "n_movers_ge_20pct": n20,
            "n_movers_10_to_20pct": n10,
            "theme_leaders_10pct_plus": leaders,
            "theme_decliners_8pct_minus": decliners,
        }
        flow_row["theme_narratives"] = _build_theme_narratives(flow_row)
        out.append(flow_row)
    return out


def theme_overlap_for_code(row: dict[str, Any] | None, code: str) -> list[str]:
    """``market_theme_flow`` 일자 행에서 종목코드별 early 뉴스·종목명 키워드 겹침."""
    if not row:
        return []
    c = str(code).zfill(6)
    for leader in row.get("theme_leaders_10pct_plus") or []:
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
    n20 = int(row.get("n_movers_ge_20pct") or 0)
    n10 = int(row.get("n_movers_10_to_20pct") or 0)
    seeds = row.get("theme_seed_hits") or []
    top_kw = row.get("top_keywords_sample") or []
    leaders = row.get("theme_leaders_10pct_plus") or []
    news_chars = int(row.get("news_chars") or 0)

    def _esc(s: str) -> str:
        return html.escape(s, quote=True)

    narratives = list(row.get("theme_narratives") or []) or _build_theme_narratives(row)
    parts = [
        "<p class=\"sub\" style=\"margin:0 0 10px;line-height:1.55\">",
        f"<strong>{_esc(t_iso)}</strong> 종가 기준 "
        f"<strong>10%↑ {n10 + n20}</strong>종 "
        f"(그중 ≥{threshold_pct:.0f}% <strong>{n20}</strong>종). "
        f"테마 문장은 <strong>{_esc(n_buy)}</strong> 거래일 "
        f"<strong>{_esc(news_cutoff_label)}</strong>(KST)까지 early 뉴스·당일 등락을 단순 매칭한 참고입니다.",
        "</p>",
    ]
    if narratives:
        parts.append("<ul class=\"nl\" style=\"margin:0 0 10px\">")
        for sent in narratives:
            parts.append(f"<li style=\"margin-bottom:6px\">{_esc(sent)}</li>")
        parts.append("</ul>")
    else:
        parts.append(
            "<p class=\"sub\" style=\"margin:0 0 10px\">"
            "당일 뚜렷한 테마 문장을 만들 키워드·등락 패턴이 부족합니다.</p>"
        )
    if seeds or top_kw:
        ref_bits: list[str] = []
        if seeds:
            ref_bits.append(
                "테마 시드: "
                + ", ".join(_esc(str(s)) for s in seeds[:8])
            )
        if top_kw:
            ref_bits.append(
                "early 키워드: "
                + ", ".join(_esc(str(k)) for k in top_kw[:10])
            )
        parts.append(
            "<p style=\"margin:0 0 8px;font-size:0.8rem;color:var(--muted)\">"
            + " · ".join(ref_bits)
            + f" (뉴스 약 {news_chars:,}자)</p>"
        )
    if leaders:
        sample = []
        for L in leaders[:5]:
            if not isinstance(L, dict):
                continue
            nm = str(L.get("name") or "")
            rp = L.get("return_pct")
            rp_s = f"{float(rp):+.1f}%" if rp is not None else "—"
            sample.append(f"{nm} {rp_s}")
        if sample:
            parts.append(
                "<p style=\"margin:0;font-size:0.8rem;color:var(--muted)\">"
                f"<strong>강세 샘플</strong> — {_esc(', '.join(sample))}</p>"
            )
    parts.append(
        "<p class=\"combo-tip-empty\" style=\"margin:10px 0 0;font-size:0.78em\">"
        "자동 생성 문장이며 인과 단정이 아닙니다. 종목명·뉴스 키워드·당일 등락 분포를 휴리스틱으로 묶었습니다.</p>"
    )
    return "".join(parts)


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
