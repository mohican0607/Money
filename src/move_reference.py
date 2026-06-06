"""
관측일 T 종목의 상·하락 **참고 요약**(HTML).

시세 추세·거래량, KOSPI 흐름, 뉴스(수급·테마·전망/목표가·리스크) 분류, 공시를 묶어
리포트 「통합 보기 → 상승/하락 참고」에 넣습니다. 인과 단정이 아닌 자동 수집·키워드 매칭입니다.
"""
from __future__ import annotations

import html
import math
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from . import config, news, trading_calendar


@dataclass
class _NewsClip:
    day: date | None
    title: str
    description: str
    link: str
    matched: str
    category: str
    timing_bucket: str = ""
    timing_label: str = ""


_INVESTOR_KW = (
    "외국인",
    "기관",
    "순매수",
    "순매도",
    "수급",
    "공매도",
    "프로그램 매수",
    "프로그램 매도",
    "개인",
    "기관투자자",
    "연기금",
)
_EXPERT_KW = (
    "목표가",
    "목표주가",
    "투자의견",
    "컨센서스",
    "리포트",
    "증권가",
    "증권사",
    "전망",
    "상향",
    "하향",
    "매수",
    "매도",
    "목표",
    "OP",
    "TP",
)
_THEME_KW = (
    "테마",
    "섹터",
    "반도체",
    "2차전지",
    "바이오",
    "AI",
    "로봇",
    "조선",
    "방산",
    "원전",
    "전력",
    "실적",
    "수주",
    "공급",
    "수출",
)
_RISK_KW = (
    "우려",
    "리스크",
    "경고",
    "악재",
    "조정",
    "하락",
    "급락",
    "적자",
    "부진",
    "소송",
    "규제",
    "상폐",
    "거래정지",
)


def _esc(s: str) -> str:
    return html.escape(s, quote=False)


def _clip_li(c: _NewsClip) -> str:
    day_s = c.day.isoformat() if isinstance(c.day, date) else ""
    me = _esc(c.matched or c.category)
    te = _esc(c.title)
    desc = (c.description or "").strip()
    desc_html = f" <span style=\"color:var(--muted)\">{_esc(desc[:160])}</span>" if desc else ""
    timing_html = ""
    if c.timing_label:
        tone = "#9fd3ff" if c.timing_bucket == "early" else ("#e6c07b" if c.timing_bucket == "late" else "#3ecf8e")
        timing_html = (
            f'<code style="font-size:0.7rem;color:{tone};margin-right:6px">'
            f"{_esc(c.timing_label)}</code>"
        )
    if c.link:
        le = html.escape(c.link, quote=True)
        body = f'<a href="{le}" target="_blank" rel="noopener">{te}</a>{desc_html}'
    else:
        body = f"{te}{desc_html}"
    return (
        f'<li><span class="pill">{day_s}</span> {timing_html}'
        f'<code style="font-size:0.72rem;color:#9fd3ff">{me}</code> {body}</li>'
    )


def _classify_blob(blob: str) -> str:
    """기사 제목·요약을 수급/전문가·테마·일반/리스크 톤으로 분류."""
    for k in _RISK_KW:
        if k in blob:
            return "리스크·우려"
    for k in _INVESTOR_KW:
        if k in blob:
            return "수급·투자자"
    for k in _EXPERT_KW:
        if k in blob:
            return "전망·의견"
    for k in _THEME_KW:
        if k in blob:
            return "테마·업종"
    return "종목·이슈"


def _news_clips_from_hits(hits: list[dict] | None, *, limit: int = 12) -> list[_NewsClip]:
    out: list[_NewsClip] = []
    for h in hits or []:
        if len(out) >= limit:
            break
        title = str(h.get("title") or "").strip()
        if not title:
            continue
        day_o = h.get("day")
        day_v = day_o if isinstance(day_o, date) else None
        blob = f"{title} {h.get('description') or ''}"
        out.append(
            _NewsClip(
                day=day_v,
                title=title,
                description=str(h.get("description") or "")[:220],
                link=str(h.get("link") or "").strip(),
                matched=str(h.get("matched") or ""),
                category=_classify_blob(blob),
                timing_bucket=str(h.get("timing_bucket") or ""),
                timing_label=str(h.get("timing_label") or ""),
            )
        )
    return out


def _merge_rise_news_hits(
    pred_news_hits: list[dict] | None,
    actual_news_hits: list[dict] | None,
    *,
    limit: int = 14,
) -> list[dict]:
    """early(14:30까지) + late/당일 뉴스 hit을 제목 기준 중복 제거 후 합칩니다."""
    merged: list[dict] = []
    seen: set[str] = set()
    for h in list(pred_news_hits or []) + list(actual_news_hits or []):
        title = str(h.get("title") or "").strip()
        if not title:
            continue
        key = str(h.get("link") or title)
        if key in seen:
            continue
        seen.add(key)
        merged.append(h)
        if len(merged) >= limit:
            break
    return merged


def _news_timing_summary_html(hits: list[dict]) -> str:
    """상승 참고 뉴스가 14:30까지/이후/당일 중 어디에 주로 있는지 요약."""
    if not hits:
        return ""
    counts = {"early": 0, "late": 0, "t_day": 0}
    for h in hits:
        b = str(h.get("timing_bucket") or "")
        if b in counts:
            counts[b] += 1
    total = sum(counts.values())
    if total <= 0:
        return ""
    n_early, n_late, n_t = counts["early"], counts["late"], counts["t_day"]
    parts = []
    if n_early:
        parts.append(f"14:30까지 <strong>{n_early}</strong>건")
    if n_late:
        parts.append(f"14:30이후 <strong>{n_late}</strong>건")
    if n_t:
        parts.append(f"관측일 당일 <strong>{n_t}</strong>건")
    detail = " · ".join(parts)
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    top_bucket, top_n = ranked[0]
    if top_n <= 0:
        attribution = ""
    elif top_n == total:
        label = {"early": "14:30까지 뉴스", "late": "14:30이후 뉴스", "t_day": "관측일 당일 뉴스"}.get(
            top_bucket, ""
        )
        attribution = f" → 상승 참고는 <strong>{label}</strong> 쪽에 모여 있습니다."
    else:
        second_bucket, second_n = ranked[1] if len(ranked) > 1 else ("", 0)
        if second_n > 0 and top_n == second_n:
            attribution = " → 14:30 전·후·당일 뉴스가 고르게 겹칩니다."
        else:
            label = {"early": "14:30까지 뉴스", "late": "14:30이후 뉴스", "t_day": "관측일 당일 뉴스"}.get(
                top_bucket, ""
            )
            attribution = f" → 상승 참고는 주로 <strong>{label}</strong> 때문으로 보입니다."
    return (
        "<p style=\"margin:0 0 10px;padding:8px 10px;background:#152232;"
        "border-left:3px solid var(--accent);border-radius:6px;font-size:0.84rem\">"
        f"<strong>뉴스 시점</strong> — {detail}{attribution}</p>"
    )


def _scan_context_news(
    actual_ctx_rows: list[tuple[date, dict]] | None,
    *,
    stock_name: str,
    keywords: list[str],
    limit: int = 16,
) -> list[_NewsClip]:
    """관측일 late+당일 뉴스에서 종목·키워드·수급/전망 키워드 기사를 추가 수집."""
    if not actual_ctx_rows:
        return []
    name = (stock_name or "").strip()
    kws = list(keywords or [])
    for extra in _INVESTOR_KW[:6] + _EXPERT_KW[:6] + _THEME_KW[:4]:
        if extra not in kws:
            kws.append(extra)
    hits = news.match_stock_news_rows(actual_ctx_rows, name, kws, limit=limit)
    broad: list[_NewsClip] = []
    seen: set[str] = set()
    for cal_d, row in actual_ctx_rows:
        title = (row.get("title") or "").strip()
        desc = (row.get("description") or "").strip()
        blob = f"{title} {desc}"
        if not blob.strip():
            continue
        key = row.get("link") or (title + desc[:80])
        if key in seen:
            continue
        cat = _classify_blob(blob)
        if cat in ("수급·투자자", "전망·의견", "테마·업종", "리스크·우려"):
            if name and name in blob:
                matched = name
            else:
                matched = cat
            seen.add(key)
            broad.append(
                _NewsClip(
                    day=cal_d,
                    title=title,
                    description=desc[:220],
                    link=(row.get("link") or "").strip(),
                    matched=matched,
                    category=cat,
                )
            )
        if len(broad) >= limit:
            break
    by_title = {c.title: c for c in _news_clips_from_hits(hits, limit=limit)}
    for c in broad:
        if c.title not in by_title:
            by_title[c.title] = c
    return list(by_title.values())[:limit]


def _technical_bullets(
    code: str,
    t_day: date,
    returns_df: pd.DataFrame | None,
    returns_ml: pd.DataFrame | None,
    *,
    returns_sub: pd.DataFrame | None = None,
    returns_ml_sub: pd.DataFrame | None = None,
) -> list[str]:
    """당일·직전 거래일 시세·거래량 특징(일봉 캐시 기준)."""
    code = str(code).zfill(6)
    bullets: list[str] = []
    sub = returns_sub
    if sub is None:
        if returns_df is None or returns_df.empty:
            return ["일봉(OHLCV) 데이터가 없어 추세·거래량 요약을 생략했습니다."]
        sub = returns_df[returns_df["Code"].astype(str).str.zfill(6) == code].sort_values("Date")
    if sub.empty:
        return ["캐시에 해당 종목 일봉이 없습니다."]

    ts = pd.Timestamp(t_day)
    row = sub[sub["Date"] == ts]
    if row.empty:
        return [f"{t_day.isoformat()} 거래일 일봉 행이 없습니다(상장·거래정지·휴장 등)."]
    r0 = row.iloc[-1]
    ret_t = float(r0.get("return_pct", 0) or 0) * 100.0
    vol = float(r0.get("Volume", 0) or 0)

    prior = sub[sub["Date"] < ts].tail(5)
    if len(prior) >= 1:
        ret_prev = float(prior.iloc[-1].get("return_pct", 0) or 0) * 100.0
        bullets.append(f"직전 거래일 수익률 약 <strong>{ret_prev:+.2f}%</strong>.")
    if len(prior) >= 3:
        cum3 = (float(prior.tail(3)["return_pct"].sum()) * 100.0) if "return_pct" in prior.columns else 0.0
        bullets.append(f"직전 3거래일 누적 약 <strong>{cum3:+.2f}%</strong> (단기 추세 참고).")

    if len(prior) >= 5 and "Volume" in prior.columns:
        vmean = float(prior["Volume"].mean())
        if vmean > 0 and vol > 0:
            ratio = vol / vmean
            if ratio >= 2.5:
                bullets.append(
                    f"당일 거래량이 직전 5일 평균 대비 약 <strong>{ratio:.1f}배</strong> (거래 급증 참고)."
                )
            elif ratio <= 0.45:
                bullets.append(
                    f"당일 거래량이 직전 5일 평균 대비 약 <strong>{ratio:.1f}배</strong> (거래 위축)."
                )

    ml_slice = returns_ml_sub
    if ml_slice is None and returns_ml is not None and not returns_ml.empty:
        ml_slice = returns_ml[
            (returns_ml["Code"].astype(str).str.zfill(6) == code)
            & (returns_ml["Date"] == ts)
        ]
    if ml_slice is not None and not ml_slice.empty:
        ml = ml_slice[ml_slice["Date"] == ts] if "Date" in ml_slice.columns else ml_slice
        if not ml.empty:
            m = ml.iloc[-1]
            ma_r = float(m.get("close_ma20_ratio", 0) or 0)
            if ma_r > 0.08:
                bullets.append(
                    "20일 이평 대비 직전 종가가 <strong>위쪽</strong>에 있음(중기 추세 상단 근처)."
                )
            elif ma_r < -0.08:
                bullets.append(
                    "20일 이평 대비 직전 종가가 <strong>아래쪽</strong>에 있음(중기 추세 하단 근처)."
                )
            std5 = float(m.get("ret_roll_std5", 0) or 0)
            if std5 >= 0.06:
                bullets.append(
                    f"최근 5일 수익률 변동성(표준편차)이 <strong>큼</strong> ({std5*100:.1f}%p 수준)."
                )

    direction = "상승" if ret_t > 0.5 else ("하락" if ret_t < -0.5 else "보합")
    bullets.insert(
        0,
        f"{t_day.isoformat()} 종가 기준 일간 <strong>{ret_t:+.2f}%</strong> ({direction}).",
    )
    return bullets[:8]


def _market_bullet(kospi_return: float | None, t_day: date) -> str | None:
    if kospi_return is None or not math.isfinite(float(kospi_return)):
        return None
    pct = float(kospi_return) * 100.0
    tone = "상승" if pct > 0.15 else ("하락" if pct < -0.15 else "보합")
    return f"같은 날 KOSPI 지수 전일 대비 약 <strong>{pct:+.2f}%</strong> ({tone}) — 시장 흐름 참고."


def _section(title: str, bullets: list[str]) -> str:
    if not bullets:
        return ""
    items = "".join(f"<li>{b}</li>" for b in bullets)
    return f"<p><strong>{_esc(title)}</strong></p><ul class=\"nl\">{items}</ul>"


def _section_news(title: str, clips: list[_NewsClip], *, empty: str) -> str:
    if not clips:
        return f"<p><strong>{_esc(title)}</strong> — {empty}</p>"
    by_cat: dict[str, list[_NewsClip]] = {}
    for c in clips:
        by_cat.setdefault(c.category, []).append(c)
    parts = [f"<p><strong>{_esc(title)}</strong> (제목·요약 키워드 분류, 인과 아님)</p>"]
    for cat in ("수급·투자자", "전망·의견", "테마·업종", "종목·이슈", "리스크·우려"):
        group = by_cat.get(cat)
        if not group:
            continue
        parts.append(f"<p style=\"margin:8px 0 4px;font-size:0.8rem;color:var(--muted)\">▸ {_esc(cat)}</p>")
        parts.append("<ul class=\"nl\">" + "".join(_clip_li(c) for c in group[:5]) + "</ul>")
    return "".join(parts)


def theme_early_overlap_html(
    overlap: list[str],
    *,
    news_cutoff_label: str,
) -> str:
    """예측 입력(early) 뉴스·종목명 키워드 겹침 — ``rise_reason_html`` 말미용."""
    if not overlap:
        return ""
    kw = ", ".join(_esc(k) for k in overlap[:10])
    return (
        "<p style=\"margin-top:10px;padding-top:8px;border-top:1px solid #2a3f5c\">"
        f"<strong>테마·early 뉴스 상관</strong> — 직전 거래일 "
        f"<strong>{_esc(news_cutoff_label)}</strong>(KST)까지 뉴스 키워드와 "
        f"종목명 토큰 겹침: <span class=\"pill\" style=\"margin-left:4px\">{kw}</span>"
        " (당일 시장 테마 표와 동일 규칙)</p>"
    )


def build_move_reference_html(
    *,
    code: str,
    name: str,
    t_trading_day: date | None,
    actual_ret: float | None,
    actual_intraday_pct: float | None = None,
    pred_news_hits: list[dict] | None = None,
    actual_news_hits: list[dict] | None = None,
    actual_ctx_rows: list[tuple[date, dict]] | None = None,
    disclosure_hits: list[dict] | None = None,
    keywords: list[str] | None = None,
    kospi_return: float | None = None,
    news_evidence_collected: bool = False,
    returns_df: Any = None,
    returns_ml: Any = None,
    returns_sub: pd.DataFrame | None = None,
    returns_ml_sub: pd.DataFrame | None = None,
) -> str:
    """
    리포트 「상·하락 참고」 HTML 블록 전체를 생성합니다.

    ``news_evidence_collected`` 가 False이면 뉴스·공시 상세는 수집 생략 안내만 넣습니다.
    """
    tlabel = t_trading_day.isoformat() if t_trading_day is not None else "관측일"
    chunks: list[str] = []

    if actual_ret is None:
        if actual_intraday_pct is not None and math.isfinite(float(actual_intraday_pct)):
            ip = float(actual_intraday_pct)
            chunks.append(
                "<p><strong>참고</strong> — "
                f"{tlabel} <strong>장중·실시간 등락률 약 {ip:+.2f}%</strong> (종가 확정 전). "
                "아래 시세·뉴스 해석은 장 마감 후 갱신될 수 있습니다.</p>"
            )
        else:
            chunks.append(
                "<p class=\"combo-tip-empty\"><strong>참고</strong> — "
                f"{tlabel} 실제 등락률이 없거나 미확정입니다.</p>"
            )
        return "".join(chunks)

    act_pct = float(actual_ret) * 100.0
    move_word = "상승" if act_pct > 0.5 else ("하락" if act_pct < -0.5 else "보합")
    chunks.append(
        f"<p><strong>{tlabel} 종가 {act_pct:+.2f}%</strong> ({move_word}) 기준 자동 참고입니다. "
        "단일 원인이 아닐 수 있으며, 아래는 공개 뉴스·시세·공시 매칭 결과입니다.</p>"
    )

    if t_trading_day is not None and trading_calendar.is_trading_day(t_trading_day):
        tech = _technical_bullets(
            code,
            t_trading_day,
            returns_df if isinstance(returns_df, pd.DataFrame) else None,
            returns_ml if isinstance(returns_ml, pd.DataFrame) else None,
            returns_sub=returns_sub,
            returns_ml_sub=returns_ml_sub,
        )
        chunks.append(_section("종목 특징·추세·거래량 (일봉)", tech))

    mkt = _market_bullet(kospi_return, t_trading_day or date.today())
    if mkt:
        chunks.append(_section("시장·국제 흐름", [mkt]))

    if not news_evidence_collected:
        chunks.append(
            "<p><em>이번 배치 실행</em>에서는 관측일 뉴스·공시 매칭을 생략했습니다. "
            "단일일·구간 실행 시 <code>include_target_calendar_news</code> 가 켜지면 "
            "수급·전망·테마 뉴스 분류가 채워집니다.</p>"
        )
    else:
        merged_hits = _merge_rise_news_hits(pred_news_hits, actual_news_hits, limit=14)
        timing_html = _news_timing_summary_html(merged_hits)
        if timing_html:
            chunks.append(timing_html)
        clips = _news_clips_from_hits(merged_hits, limit=14)
        extra = _scan_context_news(
            actual_ctx_rows,
            stock_name=name,
            keywords=list(keywords or []),
            limit=8,
        )
        seen_t = {c.title for c in clips}
        for c in extra:
            if c.title not in seen_t:
                clips.append(c)
                seen_t.add(c.title)
        chunks.append(
            _section_news(
                "뉴스 참고 (수급·테마·전문가 의견·리스크)",
                clips[:14],
                empty="관측일·전후 구간에서 종목명·키워드·수급/전망 키워드와 겹친 기사를 찾지 못했습니다.",
            )
        )

        dh = list(disclosure_hits or [])
        if dh:
            disc_parts = ["<p><strong>공시·IR</strong> (네이버 증권 종목 공시)</p><ul class=\"nl\">"]
            for d in dh[:6]:
                kind = _esc(str(d.get("kind") or ""))
                title = _esc(str(d.get("title") or ""))
                link = (d.get("link") or "").strip()
                if link:
                    le = html.escape(link, quote=True)
                    disc_parts.append(
                        f'<li><code style="font-size:0.72rem;color:#9fd3ff">{kind}</code> '
                        f'<a href="{le}" target="_blank" rel="noopener">{title}</a></li>'
                    )
                else:
                    disc_parts.append(
                        f'<li><code style="font-size:0.72rem;color:#9fd3ff">{kind}</code> {title}</li>'
                    )
            disc_parts.append("</ul>")
            chunks.append("".join(disc_parts))
        else:
            chunks.append("<p><strong>공시·IR</strong> — 당일 표시할 공시 항목이 없거나 매칭되지 않았습니다.</p>")

    if keywords:
        kw_show = ", ".join(_esc(k) for k in list(keywords)[:10])
        chunks.append(
            f"<p style=\"font-size:0.82rem;color:var(--muted)\">예측 시 참고 키워드: {kw_show}</p>"
        )

    chunks.append(
        "<p class=\"combo-tip-empty\" style=\"margin-top:8px;font-size:0.78em\">"
        "자동 요약이며 투자 조언이 아닙니다. 수급·전문가 의견은 뉴스 제목·요약 키워드 분류이고, "
        "거래소 공식 투자자별 순매수 통계와 다를 수 있습니다.</p>"
    )
    return "".join(chunks)
