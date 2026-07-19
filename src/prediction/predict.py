"""
익일(관측일 T) 수익률 후보 스코어링(휴리스틱 + 선택적 ML).

**입력(의사결정 시점)**: N−1~N 거래일 **15:30(KST)까지** 수집·분류한 뉴스(시장·국제·테마·종목),
전일 급등·테마 가중치, 시세·KOSPI 흐름, (보조) 과거 급등 종목의 뉴스 프로필·피드백 가중치.
**출력**: T일 20%↑ 후보 **랭킹**(ML 확률·확신 구간). 레거시 표시 매핑은 ``PRED_USE_DISPLAY_RANK_MAPPING=1``.
"""
from __future__ import annotations

import html
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from collections.abc import Callable
from typing import Any

from .. import config
from ..prediction import accuracy_cache as prediction_accuracy_cache
from ..features import (
    BreakoutEvent,
    filter_keywords,
    filter_specific_keywords,
    keyword_set,
    name_mention_score,
)


def keyword_intersection_hit_line_html(n_hit: int, keywords: list[str]) -> str:
    """표 ``이유/차이`` 열용 ``{n}개 교집합`` — 숫자에만 키워드 목록 툴팁."""
    if n_hit <= 0:
        return "—"
    return f"{keyword_intersection_count_html(n_hit, keywords)}개 교집합"


def keyword_intersection_count_html(n_hit: int, keywords: list[str]) -> str:
    """교집합 개수 숫자에 hover 툴팁으로 전체 키워드 목록을 붙입니다."""
    if n_hit <= 0:
        return str(n_hit)
    kw_list = filter_keywords(keywords or [])
    pills = "".join(
        f'<span class="pill" style="margin:2px 4px 2px 0">{html.escape(k)}</span>'
        for k in kw_list[:120]
    )
    if len(kw_list) > 120:
        pills += f'<span class="pill">+{len(kw_list) - 120}</span>'
    return (
        f'<span class="gap-tip kw-count-tip">'
        f'<span class="gap-tip-trigger" tabindex="0" role="button" '
        f'aria-label="교집합 키워드 {n_hit}개 전체 보기">{n_hit}</span>'
        f'<div class="gap-tip-popup kw-list-popup" role="tooltip">'
        f'<p style="margin:0 0 6px;font-weight:600">교집합 키워드 ({n_hit}개)</p>'
        f'<p style="margin:0;font-size:0.76rem;color:var(--muted)">'
        f"당일 early 뉴스 토큰 ∩ 과거 급등일 뉴스 프로필</p>"
        f'<div class="kw-pills" style="margin-top:8px;line-height:1.8">{pills}</div>'
        f"</div></span>"
    )


def format_prediction_reason_detail_html(pr: PredictionRow) -> str:
    """``pr.reasons`` 를 HTML로 변환(통합 보기용). 교집합 툴팁은 표 ``이유/차이`` 열에만 둡니다."""
    lines: list[str] = []
    for line in pr.reasons:
        lines.append(html.escape(line))
    return "<br/>".join(lines)


def format_forward_pred_rationale_html(
    pr: PredictionRow,
    row: dict[str, Any] | None = None,
) -> str:
    """
    장 시작 전·예측 전용 관측일: 예측 상승률이 높게 나온 근거를 표·패널용 HTML로 정리합니다.
    """
    from . import prediction_ranking as prk

    row = row or {}
    ks11 = getattr(pr, "ks11_ret_lag1", None)
    pillars = prk.compute_pillar_scores(pr, ks11_ret_lag1=ks11)
    pred_pct = float(getattr(pr, "predicted_return_pct", 0.0) or 0.0)
    rank_pos = getattr(pr, "rank_position", None)
    tier = str(getattr(pr, "confidence_tier", "none") or "none")
    tier_ko = {"high": "고확신", "mid": "중확신", "none": "후보"}.get(tier, tier)

    bullets: list[str] = []
    ml = getattr(pr, "ml_prob", None)
    if ml is not None and math.isfinite(float(ml)):
        bullets.append(
            f"ML 급등(≥{config.BIG_MOVE_THRESHOLD:.0%}) 추정 확률 "
            f"<strong>{float(ml) * 100:.1f}%</strong>"
            + (f" · 당일 예측 순위 <strong>#{int(rank_pos)}</strong>" if rank_pos else "")
            + f" · 확신 <strong>{html.escape(tier_ko)}</strong>"
        )
    elif rank_pos:
        bullets.append(
            f"다요인 랭킹 순위 <strong>#{int(rank_pos)}</strong> · 확신 <strong>{html.escape(tier_ko)}</strong>"
        )

    factor_line = prk.format_factor_summary(pr, ks11_ret_lag1=ks11)
    bullets.append(f"요인 강도: {html.escape(factor_line)}")

    nh = int(getattr(pr, "keyword_hits", 0) or 0)
    mention = float(getattr(pr, "mention_score", 0.0) or 0.0)
    if nh > 0:
        bullets.append(
            f"당일 예측 입력 뉴스와 과거 급등 프로필 <strong>키워드 교집합 {nh}개</strong>"
            + (f" · 종목명 언급 {mention:.2f}" if mention >= 0.12 else "")
        )
    elif mention >= 0.15:
        bullets.append(f"뉴스 제목·본문 <strong>종목명 직접 언급</strong> (점수 {mention:.2f})")

    mom = float(getattr(pr, "momentum_score", 0.0) or 0.0)
    ind_m = float(getattr(pr, "industry_momentum", 0.0) or 0.0)
    ind_h = float(getattr(pr, "industry_limit_up_heat", 0.0) or 0.0)
    if mom + 1e-12 >= 0.28:
        bullets.append(f"전일·최근 <strong>가격·거래량 모멘텀</strong> ({mom * 100:.0f}%)")
    if ind_m + 1e-12 >= 0.22 or ind_h + 1e-12 >= 0.22:
        bullets.append(
            f"동일 업종·테마 <strong>섹터 열기</strong> "
            f"(업종 {ind_m * 100:.0f}% · 상한가 근접 {ind_h * 100:.0f}%)"
        )
    flow = float(getattr(pr, "investor_flow_score", 0.0) or 0.0)
    if flow + 1e-12 >= 0.35:
        bullets.append(f"외국인·기관 <strong>수급</strong> 신호 ({flow * 100:.0f}%)")
    rs = float(getattr(pr, "relative_strength_score", 0.0) or 0.0)
    if rs + 1e-12 >= 0.38:
        bullets.append(f"시장 대비 <strong>상대강도</strong> ({rs * 100:.0f}%)")

    strong_non_news = prk.count_non_news_strong_pillars(pillars, threshold=0.42)
    if strong_non_news >= 2:
        bullets.append(
            f"뉴스 외 다요인 <strong>{strong_non_news}개</strong>가 동시에 강함 "
            f"(뉴스 단독 후보가 아님)"
        )
    elif pillars["news"] + 1e-12 >= 0.50 and strong_non_news < 1:
        bullets.append(
            '<span class="muted">뉴스 키워드는 강하나 시세·섹터 확인 신호는 약함 — 보수적 해석 권장</span>'
        )

    if ks11 is not None and math.isfinite(float(ks11)):
        bullets.append(f"전일 KOSPI {float(ks11):+.2f}% 반영(시장 레짐)")

    bullets.append(
        f"표시 예측 상승률 <strong>{pred_pct:.2f}%</strong> — "
        f"순위·ML·모멘텀·섹터를 합친 익일 {config.BIG_MOVE_THRESHOLD:.0%}↑ 후보 점수입니다."
    )

    parts: list[str] = ['<ul class="nl forward-pred-rationale" style="margin:6px 0 0 0">']
    for b in bullets:
        parts.append(f"<li>{b}</li>")
    parts.append("</ul>")

    kws = filter_keywords(list(pr.matched_keywords or []))
    if kws:
        pills = "".join(
            f'<span class="pill" style="margin:2px 4px 2px 0;font-size:0.72rem">{html.escape(k)}</span>'
            for k in kws[:24]
        )
        if len(kws) > 24:
            pills += f'<span class="pill" style="font-size:0.72rem">+{len(kws) - 24}</span>'
        parts.append(f'<div class="kw-pills" style="margin-top:8px;line-height:1.7">{pills}</div>')

    return "".join(parts)


def format_forward_pred_reason_tooltip_html(
    pr: PredictionRow,
    row: dict[str, Any] | None = None,
    *,
    early_rows: list[tuple[date, dict[str, str]]] | None = None,
    actual_ctx_rows: list[tuple[date, dict[str, str]]] | None = None,
    flow_row: dict[str, Any] | None = None,
) -> str:
    """장 미개장 관측일 ``예측 근거`` 열 tooltip — ``content.format_forward_pred_reason_narrative_html`` 위임."""
    from ..report.content import format_forward_pred_reason_narrative_html

    return format_forward_pred_reason_narrative_html(
        pr,
        row or {},
        early_rows=early_rows,
        actual_ctx_rows=actual_ctx_rows,
        flow_row=flow_row,
    )


@dataclass
class PredictionRow:
    """한 종목에 대한 스코어링 결과(리포트 테이블·갭 분석에 전달)."""

    code: str
    name: str
    score: float
    predicted_return_pct: float
    matched_keywords: list[str]
    reasons: list[str]
    ml_prob: float | None = None
    # confidence 전용 binary head raw score. 순위용 ml_rank_score와 분리.
    ml_precision_score: float | None = None
    # ML raw 혼합 점수(분류+회귀) — 하이브리드·Hit@K 정렬용. 보정 ml_prob 과 분리.
    ml_rank_score: float | None = None
    momentum_score: float = 0.0
    news_context_score: float = 0.0
    keyword_hits: int = 0
    mention_score: float = 0.0
    industry_momentum: float = 0.0
    industry_theme_overlap: float = 0.0
    industry_limit_up_heat: float = 0.0
    prior_industry_hot: float = 0.0
    sector_breadth_hot: float = 0.0
    vol_surge_ratio: float = 0.0
    ret_lag1: float = 0.0
    open_gap: float = 0.0
    investor_flow_score: float = 0.0
    foreign_net_vol_ratio: float = 0.0
    relative_strength_score: float = 0.0
    ks11_ret_lag1: float | None = None
    rank_score: float | None = None
    rank_position: int | None = None
    confidence_tier: str = "none"


def _build_code_keyword_profile(train_events: list[BreakoutEvent]) -> dict[str, frozenset[str]]:
    """
    훈련 이벤트에서 종목코드별로 등장한 모든 ``news_keywords`` 를 합집합하여 프로필 생성.
    """
    acc: dict[str, set[str]] = defaultdict(set)
    for e in train_events:
        acc[e.code].update(filter_specific_keywords(e.news_keywords))
    return {c: frozenset(v) for c, v in acc.items()}


def _chronic_miss_blocks_prediction(
    code: str,
    mention: float,
    feedback_ctx: dict[str, object] | None,
) -> bool:
    """누적 적중률이 매우 낮은 종목은 종목명 직접 언급 없이 후보에서 제외."""
    if mention >= float(config.PRED_MENTION_GATE_MIN):
        return False
    if not config.PRED_CHRONIC_MISS_BLOCK_ENABLED or not feedback_ctx:
        return False
    by_mean = feedback_ctx.get("by_code_mean_ratio")
    by_count = feedback_ctx.get("by_code_count")
    if not isinstance(by_mean, dict) or not isinstance(by_count, dict):
        return False
    code_key = str(code).zfill(6)
    n = int(by_count.get(code_key, 0) or 0)
    if n < int(config.PRED_CHRONIC_MISS_MIN_SAMPLES):
        return False
    mean = by_mean.get(code_key)
    if not isinstance(mean, (int, float)):
        return False
    return float(mean) < float(config.PRED_CHRONIC_MISS_RATIO_MAX)


def _historical_mean_return(train_events: list[BreakoutEvent], code: str) -> float:
    """
    해당 ``code`` 의 과거 급등 이벤트 상승률의 스무딩 평균(소수).

    - 종목 이력이 없으면: 전체 훈련 급등 사건 평균을 사용
    - 종목 이력이 적으면: 종목 평균과 전체 평균을 가중 혼합(베이지안 스무딩)
    - 최종 값은 ``PRED_RETURN_MIN``~``PRED_RETURN_MAX`` 구간으로 클램프
    """
    lo = float(config.PRED_RETURN_MIN)
    hi = float(config.PRED_RETURN_MAX)
    all_xs = [e.return_pct for e in train_events]
    global_mean = sum(all_xs) / len(all_xs) if all_xs else lo
    prior = float(min(hi, max(lo, global_mean)))

    xs = [e.return_pct for e in train_events if e.code == code]
    if not xs:
        return prior

    code_mean = sum(xs) / len(xs)
    # 이력이 적은 종목은 전체 평균으로 당겨 과대/과소 추정을 완화.
    prior_strength = 5.0
    n = float(len(xs))
    blended = (n * code_mean + prior_strength * prior) / (n + prior_strength)
    return float(min(hi, max(lo, blended)))


def _count_code_events(train_events: list[BreakoutEvent], code: str) -> int:
    """해당 종목의 훈련 급등 이벤트 건수."""
    return sum(1 for e in train_events if e.code == code)


def _calibrate_predicted_return(
    base_ret: float,
    *,
    n_hit: int,
    mention: float,
    code_event_count: int,
) -> float:
    """
    기본 예측치(base_ret)를 신호 강도 기반으로 완만하게 보정.

    과거 버전은 곱셈 감쇠가 겹쳐 예측이 과도하게 낮아져 (실제÷예측) 분포가 악화되었습니다.
    여기서는 키워드·종목명 신호로 ``base`` 근처만 조정해, 과대·과소 예측을 동시에 완화합니다.
    """
    if not config.PRED_RETURN_CALIBRATION_ENABLED:
        lo = min(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
        hi = max(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
        return float(min(hi, max(lo, base_ret)))

    hit = max(0, int(n_hit))
    m = max(0.0, min(1.0, float(mention)))

    # 신호 0~1: 키워드 일치 비중 + 종목명 언급(0~1 스코어) 혼합
    w_hit = min(1.0, hit / 7.0)
    w_m = min(1.0, m / 0.5) if m > 0 else 0.0
    signal = 0.62 * w_hit + 0.38 * w_m
    signal = max(0.0, min(1.0, signal))

    # base 주변만 조정: 약한 신호는 소폭 하향, 강한 신호는 소폭 상향 (대략 0.94~1.06배)
    mult = 0.94 + 0.12 * signal
    if code_event_count == 0:
        mult *= 0.98
    elif code_event_count <= 2:
        mult *= 0.99

    calibrated = float(base_ret) * mult
    lo = min(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
    hi = max(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
    return float(min(hi, max(lo, calibrated)))


def _feedback_calibrated_return(
    pred_ret: float,
    *,
    code: str,
    n_hit: int,
    mention: float,
    feedback_ctx: dict[str, object] | None,
    clamp_lo: float | None = None,
    clamp_hi: float | None = None,
) -> float:
    """
    누적 오차 이력 기반 보정.

    ``t_code_ratio=min(|실제%|/|예측%|,1)`` 평균을 이용해 과대 예측을 점진적으로 줄인다.
    표본이 적거나 신호(키워드/언급)가 약하면 1.0(무보정)에 더 가깝게 축소한다.

    ``clamp_lo``·``clamp_hi`` 는 예측값이 **소수 단위**(0.2 = 20%)일 때 최종 클램프 구간.
    ML 표시 매핑(약 11~30%)처럼 휴리스틱 기본 구간과 다르면 둘 다 넘겨 동일 보정을 적용한다.
    """
    if clamp_lo is not None and clamp_hi is not None:
        lo = float(min(clamp_lo, clamp_hi))
        hi = float(max(clamp_lo, clamp_hi))
    else:
        lo = min(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
        hi = max(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
    if not config.PRED_ERROR_FEEDBACK_ENABLED or not feedback_ctx:
        return float(min(hi, max(lo, pred_ret)))

    by_code_mean = feedback_ctx.get("by_code_mean_ratio")
    by_code_count = feedback_ctx.get("by_code_count")
    global_mean = feedback_ctx.get("global_mean_ratio")
    if not isinstance(by_code_mean, dict) or not isinstance(by_code_count, dict):
        return float(min(hi, max(lo, pred_ret)))
    if not isinstance(global_mean, (int, float)):
        global_mean = None
    code_key = str(code).zfill(6)
    c_mean = by_code_mean.get(code_key)
    c_n = int(by_code_count.get(code_key, 0) or 0)
    if c_n < int(config.PRED_ERROR_FEEDBACK_MIN_SAMPLES):
        if global_mean is None:
            return float(min(hi, max(lo, pred_ret)))
        c_mean = float(global_mean)
        c_n = int(feedback_ctx.get("global_count", 0) or 0)
    if not isinstance(c_mean, (int, float)):
        return float(min(hi, max(lo, pred_ret)))

    prior = float(global_mean) if isinstance(global_mean, (int, float)) else 0.70
    shrink = max(1.0, float(config.PRED_ERROR_FEEDBACK_SHRINK_STRENGTH))
    mean_ratio = (float(c_mean) * c_n + prior * shrink) / (float(c_n) + shrink)
    target_mult = max(0.78, min(1.02, mean_ratio))

    bucket_stats = feedback_ctx.get("signal_bucket_stats")
    if isinstance(bucket_stats, dict):
        hit_band = "h0" if n_hit <= 0 else ("h1_2" if n_hit <= 2 else ("h3_5" if n_hit <= 5 else "h6p"))
        m = max(0.0, min(1.0, float(mention)))
        mention_band = "m0" if m < 0.08 else ("m1" if m < 0.25 else ("m2" if m < 0.45 else "m3"))
        pred_pct = float(pred_ret) * 100.0
        pred_band = "p10_15" if pred_pct < 15.0 else ("p15_20" if pred_pct < 20.0 else ("p20_25" if pred_pct < 25.0 else "p25p"))
        bkey = f"{hit_band}|{mention_band}|{pred_band}"
        b = bucket_stats.get(bkey)
        if isinstance(b, dict):
            bn = int(b.get("count", 0) or 0)
            b_ratio = b.get("mean_ratio")
            if bn >= int(config.PRED_ERROR_FEEDBACK_MIN_SAMPLES) and isinstance(b_ratio, (int, float)):
                b_mult = max(0.78, min(1.02, float(b_ratio)))
                w = min(0.45, bn / 80.0)
                target_mult = target_mult * (1.0 - w) + b_mult * w

    hit = max(0, int(n_hit))
    m = max(0.0, min(1.0, float(mention)))
    signal = 0.62 * min(1.0, hit / 7.0) + 0.38 * (min(1.0, m / 0.5) if m > 0 else 0.0)
    sample_conf = min(1.0, float(c_n) / 20.0)
    conf = sample_conf * (0.65 + 0.35 * signal)
    mult = 1.0 + (target_mult - 1.0) * conf
    return float(min(hi, max(lo, float(pred_ret) * mult)))


def _display_return_pct_bounds() -> tuple[float, float]:
    """리포트 표시용 예측 상승률(%%) 하한·상한 — 급등 임계(기본 20%%)와 ``PRED_RETURN_MAX``."""
    lo = float(config.PRED_RETURN_MIN) * 100.0
    hi = float(config.PRED_RETURN_MAX) * 100.0
    return lo, hi


def display_return_pct_from_rank_value(
    value: float, *, value_min: float, value_max: float
) -> float:
    """상위 후보 묶음 안에서 순위 지표(점수·ML 확률)를 [PRED_RETURN_MIN, PRED_RETURN_MAX]%%로 단조 매핑."""
    lo, hi = _display_return_pct_bounds()
    if value_max <= value_min + 1e-12:
        t = 1.0
    else:
        t = (float(value) - float(value_min)) / (float(value_max) - float(value_min))
    t = max(0.0, min(1.0, t))
    return lo + (hi - lo) * t


def apply_display_return_pct_ranking(
    rows: list[PredictionRow],
    *,
    rank_value: Callable[[PredictionRow], float],
    reason_prefix: str,
) -> None:
    """``rows`` 안에서 ``rank_value(row)`` 순으로 표시 예측 상승률(%%)을 재할당."""
    if not rows:
        return
    vals = [float(rank_value(r)) for r in rows]
    vmin, vmax = min(vals), max(vals)
    lo, hi = _display_return_pct_bounds()
    note = (
        f"{reason_prefix} "
        f"상위 후보 {len(rows)}개를 {lo:.0f}~{hi:.0f}% 구간에 순위별로 정렬한 표시값입니다."
    )
    for r, v in zip(rows, vals):
        r.predicted_return_pct = display_return_pct_from_rank_value(
            v, value_min=vmin, value_max=vmax
        )
        r.reasons = [note] + list(r.reasons)


def build_scoring_context(
    news_text_blob: str,
    train_events: list[BreakoutEvent],
) -> tuple[frozenset[str], dict[str, frozenset[str]]]:
    """
    당일 뉴스 blob의 키워드 집합과 종목별 히스토리 프로필을 한 번만 계산합니다.

    ``predict_for_trading_day`` 가 종목 루프마다 동일 연산을 반복하지 않도록 합니다.
    """
    return keyword_set(news_text_blob, k=100), _build_code_keyword_profile(train_events)


def prediction_row_for_code(
    code: str,
    listing_names: dict[str, str],
    train_events: list[BreakoutEvent],
    news_text_blob: str,
    ctx: tuple[frozenset[str], dict[str, frozenset[str]]],
    min_keyword_hits: int,
    *,
    feedback_ctx: dict[str, object] | None = None,
    theme_weights: dict[str, float] | None = None,
    allow_momentum_only: bool = False,
    allow_news_context_only: bool = False,
    allow_investor_flow_only: bool = False,
) -> PredictionRow | None:
    """
    단일 종목에 대해 키워드 교집합 수 + 종목명 언급 점수로 스코어하고 ``PredictionRow`` 를 만듭니다.

    조건: 비범용 키워드 교집합이 ``min_keyword_hits`` 미만이고 종목명 언급도
    ``PRED_MENTION_GATE_MIN`` 미만이면 후보 제외(None). 반복 오탐 종목은 누적 오차로 추가 제외.

    Args:
        ctx: ``build_scoring_context`` 의 반환값을 그대로 넘깁니다.
    """
    kw_news, profile = ctx
    name = listing_names.get(code, "")
    hist_kw = profile.get(code, frozenset())
    inter = filter_specific_keywords(hist_kw & kw_news)
    n_hit = len(inter)
    mention = name_mention_score(news_text_blob, name)
    if _chronic_miss_blocks_prediction(code, mention, feedback_ctx):
        return None
    mention_gate = float(config.PRED_MENTION_GATE_MIN)
    if n_hit < min_keyword_hits and mention < mention_gate:
        if not (allow_momentum_only or allow_news_context_only or allow_investor_flow_only):
            return None
    if n_hit < 1 and mention < mention_gate:
        if not (allow_momentum_only or allow_news_context_only or allow_investor_flow_only):
            return None
    score = n_hit * 1.0 + mention * 5.0
    tw = theme_weights or {}
    theme_hit = 0.0
    if tw:
        for k in kw_news:
            v = tw.get(k)
            if v is not None and math.isfinite(float(v)):
                theme_hit += float(v)
        theme_hit = max(-12.0, min(theme_hit, 12.0))
    if config.THEME_CARRYOVER_ENABLED:
        score += float(config.THEME_CARRYOVER_SCORE_SCALE) * theme_hit
    try:
        from .. import stocks as stocks_mod

        ind_ov = stocks_mod.industry_theme_overlap(code, kw_news)
        if ind_ov + 1e-12 >= 0.55:
            score += 1.8 * ind_ov
    except Exception:
        ind_ov = 0.0

    # 최근 오판 기반 키워드 가중치 피드백(온라인 학습). 교집합 키워드에 대해 가중합을 점수에 더합니다.
    if config.KEYWORD_FEEDBACK_ENABLED and inter:
        try:
            kw_w = prediction_accuracy_cache.keyword_feedback_weights()
        except Exception:
            kw_w = {}
        if kw_w:
            s = 0.0
            for k in inter:
                v = kw_w.get(k)
                if v is not None and math.isfinite(float(v)):
                    s += float(v)
            if math.isfinite(s) and s != 0.0:
                score += float(config.KEYWORD_FEEDBACK_SCORE_SCALE) * s
    matched = sorted(inter, key=len, reverse=True)
    reasons: list[str] = []
    if theme_hit >= 0.06 and config.THEME_CARRYOVER_ENABLED:
        reasons.append(
            f"전일 급등·뉴스 테마 키워드와 당일 뉴스 가중 일치 약 {theme_hit:.2f}(익일 테마 캐리오버)"
        )
    if n_hit:
        reasons.append(f"당일 뉴스·과거 급등 프로필 키워드 교집합 {n_hit}개")
    if mention >= mention_gate:
        reasons.append("뉴스 본문·제목에 종목명 다수 등장")
    reasons.append(
        "표시 예측 상승률(%)은 키워드·종목명·테마·과거 급등 통계 등을 반영한 내부 추정과 "
        f"전체 훈련 급등 평균을 함께 반영한 스무딩 값입니다"
        f"({config.PRED_RETURN_MIN * 100:.0f}~{config.PRED_RETURN_MAX * 100:.0f}% 구간). "
        "훈련 사례가 없으면 전체 훈련 급등 평균을 사용합니다."
    )
    base_ret = _historical_mean_return(train_events, code)
    pred_ret = _calibrate_predicted_return(
        base_ret,
        n_hit=n_hit,
        mention=mention,
        code_event_count=_count_code_events(train_events, code),
    )
    pred_ret = _feedback_calibrated_return(
        pred_ret,
        code=code,
        n_hit=n_hit,
        mention=mention,
        feedback_ctx=feedback_ctx,
    )
    if config.PRED_RETURN_CALIBRATION_ENABLED:
        reasons.append(
            "예측 수익률은 신호 강도(키워드 일치·종목명 언급)에 따라 기본값 주변으로만 완만히 보정했습니다."
        )
    if config.PRED_ERROR_FEEDBACK_ENABLED:
        reasons.append(
            "추가로 누적 오차 이력(예측 대비 실제 달성률)을 반영해 과대 예측을 완만히 줄이는 자동 보정을 적용했습니다."
        )
    return PredictionRow(
        code=code,
        name=name,
        score=score,
        predicted_return_pct=pred_ret * 100,
        matched_keywords=matched,
        reasons=reasons,
        keyword_hits=n_hit,
        mention_score=mention,
    )


def predict_for_trading_day(
    target_day: date,
    listing_codes: list[str],
    listing_names: dict[str, str],
    train_events: list[BreakoutEvent],
    news_text_blob: str,
    top_n: int = 40,
    min_keyword_hits: int = 2,
    *,
    ml_bundle: dict[str, Any] | None = None,
    returns_ml: Any = None,
    theme_weights: dict[str, float] | None = None,
    feedback_ctx: dict[str, object] | None = None,
    forward_observation: bool = False,
) -> list[PredictionRow]:
    """
    상장 전 종목(또는 리스트)에 대해 스코어를 매기고 상위 ``top_n`` 만 반환합니다.

    ``target_day`` 는 시그니처상 관측일이나, 실제 예측 입력은 호출자가 만든 ``news_text_blob``
    (이미 T에 맞는 early 윈도우로 집계된 문자열)입니다.

    Args:
        min_keyword_hits: 훈련 이벤트가 없으면 main에서 0으로 내려 전체 완화할 수 있음.
        ml_bundle: ``ml_move_rank.fit_or_load_classifier`` 결과. ``pipeline`` 이 있으면
            급등 확률 순으로 후보를 고릅니다.
        returns_ml: ``stocks.enrich_daily_returns_for_ml(daily_returns_table(...))`` 결과.
            ML 랭커 시세 피처용(없으면 ML 경로를 건너뜁니다).
    """
    if (
        ml_bundle is not None
        and ml_bundle.get("pipeline") is not None
        and returns_ml is not None
    ):
        from . import ml_move_rank

        return ml_move_rank.rank_predictions_ml(
            target_day=target_day,
            listing_codes=listing_codes,
            listing_names=listing_names,
            train_events=train_events,
            news_text_blob=news_text_blob,
            returns_ml=returns_ml,
            pipeline=ml_bundle["pipeline"],
            top_n=top_n,
            min_keyword_hits=min_keyword_hits,
            theme_weights=theme_weights,
            feedback_ctx=feedback_ctx,
            ml_bundle=ml_bundle,
            forward_observation=forward_observation,
        )
    if ml_bundle is not None and ml_bundle.get("pipeline") is not None and returns_ml is None:
        print(
            "ML 랭커: returns_ml 이 없어 시세 피처를 쓸 수 없습니다 → 휴리스틱 순위만 사용합니다.",
            flush=True,
        )

    ctx = build_scoring_context(news_text_blob, train_events)
    if feedback_ctx is None:
        feedback_ctx = prediction_accuracy_cache.build_feedback_context()
    ranked: list[PredictionRow] = []

    for code in listing_codes:
        row = prediction_row_for_code(
            code,
            listing_names,
            train_events,
            news_text_blob,
            ctx,
            min_keyword_hits,
            feedback_ctx=feedback_ctx,
            theme_weights=theme_weights,
        )
        if row is not None:
            ranked.append(row)

    ranked.sort(key=lambda x: x.score, reverse=True)
    top = ranked[:top_n]
    from . import prediction_ranking

    ks11_ret = None
    try:
        from .market_features import ks11_market_feats

        ks11_ret, _ = ks11_market_feats(target_day)
    except Exception:
        ks11_ret = None
    return prediction_ranking.finalize_ranked_predictions(
        top,
        target_day=target_day,
        ks11_ret_lag1=ks11_ret,
        forward_observation=forward_observation,
        returns_ml=returns_ml,
    )


def aggregate_news_for_window(
    news_by_calendar: dict[date, list[dict[str, str]]],
    start: date,
    end: date,
) -> str:
    """
    캘린더 구간 [start, end]의 모든 뉴스 제목·설명을 순서대로 이어 붙인 문자열.

    컷오프가 꺼졌을 때 ``news_window_for_target_trading_day`` 와 함께 쓰입니다.
    """
    parts: list[str] = []
    d = start
    while d <= end:
        for row in news_by_calendar.get(d, []):
            parts.append(row.get("title", ""))
            parts.append(row.get("description", ""))
        d += timedelta(days=1)
    return "\n".join(parts)


def explain_return_gap_html(
    *,
    pred_ret_pct: float | None,
    actual_ret: float | None,
    actual_intraday_pct: float | None = None,
    prediction_row: PredictionRow | None,
    news_blob_early: str,
    kospi_change_hint: str | None,
    late_keywords_matched: bool | None,
    disclosure_hits: list[dict] | None = None,
) -> str:
    """
    예측 상승률(%) vs 실제 상승률(소수) 차이에 대한 참고용 설명(HTML 조각).
    인과·투자 조언이 아닌 후속 점검 포인트 정리.
    """
    parts: list[str] = []

    if actual_ret is None:
        if actual_intraday_pct is not None and str(actual_intraday_pct).strip() != "":
            try:
                ip = float(actual_intraday_pct)
            except (TypeError, ValueError):
                ip = float("nan")
            if math.isfinite(ip):
                if pred_ret_pct is None:
                    return (
                        "<p><strong>장중 등락률</strong> — 종가 미확정 기준 "
                        f"<strong>{ip:.2f}%</strong>입니다. 예측 후보 밖이면 수치 비교는 생략합니다.</p>"
                    )
                pred = float(pred_ret_pct)
                diff = pred - ip
                return (
                    "<p><strong>장중 참고</strong> — "
                    f"실시간(전일대비) 등락률 <strong>{ip:.2f}%</strong> · 예측 <strong>{pred:.2f}%</strong> · "
                    f"(예측−장중) <strong>{diff:+.2f}</strong> 퍼센트포인트. "
                    "장 마감 후에는 일봉 종가 기준으로 다시 집계됩니다.</p>"
                )
        if pred_ret_pct is not None:
            parts.append(
                "<p><strong>실제 상승률 미확정</strong> — T일 장 마감 전이거나 가격 데이터가 없어 "
                f"예측({pred_ret_pct:.2f}%)과의 차이 분석을 생략합니다.</p>"
            )
        return "".join(parts)

    act_pct = float(actual_ret) * 100.0

    if pred_ret_pct is None:
        parts.append(
            f"<p><strong>예측 후보 밖</strong> — 모델이 키워드·종목명 규칙으로 이 종목에 예측 상승률을 붙이지 못했습니다. "
            f"실제 상승률은 <strong>{act_pct:.2f}%</strong>입니다.</p>"
        )
        parts.append(
            "<ul>"
            "<li>15:30까지 뉴스·테마·시세 신호가 약하면 후보에서 제외될 수 있습니다.</li>"
            "<li>실제 급등은 공시·수급·테마·국제정세 등 뉴스만으로 설명되지 않는 경우가 있습니다.</li>"
            "<li>오른쪽「실제 맥락 뉴스」에서 당일 보도를 확인해 보세요.</li>"
            "</ul>"
        )
        return "".join(parts)

    pred = float(pred_ret_pct)
    diff = pred - act_pct
    parts.append(
        f"<p><strong>수치</strong> — 예측 <strong>{pred:.2f}%</strong> · 실제 <strong>{act_pct:.2f}%</strong> "
        f"· (예측−실제) <strong>{diff:+.2f}</strong> 퍼센트포인트</p>"
    )

    if abs(diff) < 1.5:
        parts.append("<p>편차가 작아 모델이 방향·크기를 대략 맞춘 편입니다.</p>")
        return "".join(parts)

    bullets: list[str] = []

    if diff > 4.0:
        bullets.append(
            "예측은 과거 해당 종목 급등일의 평균 상승률(하한·상한 클램프)을 쓰기 때문에, "
            "실제보다 높게 나오기 쉽습니다."
        )
        bullets.append(
            "실제가 낮았다면 당일 시장·업종 조정, 차익 실현, 거래량 부족 등으로 테마가 이어지지 않았을 수 있습니다."
        )
        if late_keywords_matched is True:
            cut = f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}"
            bullets.append(
                f"<strong>N-1일 {cut} 이후</strong> 뉴스에 예측에 쓰인 키워드가 포함된 흔적이 있습니다. "
                "장 마감 후·익일 오전 이슈가 예측 시점에는 반영되지 않았을 수 있습니다."
            )
        elif late_keywords_matched is False:
            bullets.append(
                "지연 구간 뉴스에서 예측 키워드가 크게 잡히지는 않았습니다. 다른 촉매 또는 기술적 조정을 의심해 볼 수 있습니다."
            )
    elif diff < -4.0:
        bullets.append(
            "실제가 예측보다 훨씬 높았습니다. 뉴스 키워드만으로는 설명되지 않는 공시·수급·테마 급확산이 있었을 수 있습니다."
        )
        bullets.append("모델 예측치는 과거 패턴의 보수적 평균이라, 단일 일 급등을 과소 추정할 수 있습니다.")
    else:
        bullets.append("편차는 중간 수준입니다. 예측은 참고용 확률 신호이며, 당일 뉴스·공시와 함께 보는 것이 좋습니다.")

    if prediction_row and prediction_row.matched_keywords:
        nk = len(prediction_row.matched_keywords)
        bullets.append(f"예측 시 <strong>{nk}개</strong> 키워드가 뉴스와 맞았습니다. 키워드 수가 많아도 당일 가격이 따라오지 않을 수 있습니다.")

    if prediction_row and prediction_row.name and prediction_row.name in news_blob_early:
        bullets.append("예측 입력 뉴스에 종목명이 직접 등장했습니다. 호재·악재·단순 거론 여부는 기사 제목·본문을 따로 확인해야 합니다.")

    if kospi_change_hint:
        bullets.append(kospi_change_hint)

    dh = list(disclosure_hits or [])
    if dh:
        kinds: list[str] = []
        for x in dh:
            k = str(x.get("kind", "")).strip()
            if k and k not in kinds:
                kinds.append(k)
        if kinds:
            bullets.append(
                "네이버 증권 종목 공시에서 당일 관련 공시가 "
                f"<strong>{len(dh)}건</strong> 확인되었습니다(유형: {', '.join(kinds[:5])}). "
                "뉴스 키워드 외에 공시 내용을 함께 확인해 해석 정확도를 높이세요."
            )
        else:
            bullets.append(
                f"네이버 증권 종목 공시에서 당일 관련 공시가 <strong>{len(dh)}건</strong> 확인되었습니다. "
                "뉴스 키워드 외에 공시 내용을 함께 확인해 해석 정확도를 높이세요."
            )
    else:
        bullets.append(
            "공시·거래정지·대량보유·공매도 등은 뉴스 기반 자동 분석에서 누락될 수 있습니다. 네이버 증권 종목 공시를 함께 확인하세요."
        )

    parts.append("<ul>" + "".join(f"<li>{b}</li>" for b in bullets[:8]) + "</ul>")
    return "".join(parts)


def format_pred_miss_tooltip_html(
    row: dict[str, Any],
    *,
    miss_hints: list[str] | None = None,
    t_trading_day: date | None = None,
) -> str:
    """
    장 마감 후 고·중확신 미적중 행의 「틀린 이유」 tooltip HTML.

    갭 분석·진단 힌트·당일 뉴스·공시를 한 패널로 묶습니다.
    """
    pred_pct = row.get("pred_ret")
    act = row.get("actual_ret")
    parts: list[str] = []
    tlabel = t_trading_day.isoformat() if t_trading_day is not None else "관측일"

    if pred_pct is not None and act is not None:
        act_pct = float(act) * 100.0
        pred_f = float(pred_pct)
        parts.append(
            f'<p><strong>미적중 요약</strong> — 예측 <strong>{pred_f:.2f}%</strong> · '
            f'실제 <strong>{act_pct:.2f}%</strong> · '
            f'차이 <strong>{pred_f - act_pct:+.2f}</strong>pp ({tlabel})</p>'
        )

    hints = [str(h).strip() for h in (miss_hints or []) if str(h).strip()]
    if hints:
        parts.append(
            '<p><strong>진단</strong></p><ul class="nl">'
            + "".join(f"<li>{html.escape(h, quote=False)}</li>" for h in hints[:7])
            + "</ul>"
        )

    reason_bits: list[str] = []
    for src in (
        row.get("pred_reason_summary"),
        row.get("pred_reason_detail_html"),
        row.get("reasons_html"),
        " ".join(str(x) for x in (row.get("reasons") or []) if x),
    ):
        plain = re.sub(r"<[^>]+>", " ", str(src or ""))
        plain = re.sub(r"\s+", " ", plain).strip()
        if plain and plain != "—":
            reason_bits.append(plain)
            break
    if reason_bits:
        snippet = reason_bits[0]
        if len(snippet) > 280:
            snippet = snippet[:279].rstrip() + "…"
        parts.append(
            f'<p style="margin-top:8px"><strong>예측 근거(요약)</strong> — '
            f"{html.escape(snippet, quote=False)}</p>"
        )

    kws = [str(k).strip() for k in (row.get("keywords") or []) if str(k).strip()]
    if kws:
        kw_show = ", ".join(html.escape(k, quote=False) for k in kws[:12])
        parts.append(
            f'<p><strong>매칭 키워드 {len(kws)}개</strong> — '
            f'<code style="font-size:0.8rem;color:var(--warn)">{kw_show}</code></p>'
        )
    elif int(row.get("keyword_hits", 0) or 0) <= 0:
        parts.append("<p><strong>매칭 키워드</strong> — 없음 (키워드 0)</p>")

    gap = str(row.get("gap_analysis_html") or "").strip()
    if gap:
        parts.append(
            f'<div style="margin-top:8px"><strong>예측·실제 차이</strong>{gap}</div>'
        )

    act_hits = list(row.get("actual_news_hits") or [])
    if act_hits:
        parts.append(f'<p style="margin-top:8px"><strong>{tlabel} 전후 뉴스</strong></p><ul class="nl">')
        for h in act_hits[:5]:
            title = html.escape(str(h.get("title") or ""), quote=False)
            matched = html.escape(str(h.get("matched") or ""), quote=False)
            day_o = h.get("day")
            day_s = (
                day_o.isoformat()
                if isinstance(day_o, date)
                else html.escape(str(day_o or ""), quote=False)
            )
            link = (h.get("link") or "").strip()
            if link:
                le = html.escape(link, quote=True)
                parts.append(
                    f'<li><span class="pill">{day_s}</span> '
                    f'<code style="font-size:0.75rem;color:var(--muted)">{matched}</code> '
                    f'<a href="{le}" target="_blank" rel="noopener">{title}</a></li>'
                )
            else:
                parts.append(
                    f'<li><span class="pill">{day_s}</span> '
                    f'<code style="font-size:0.75rem;color:var(--muted)">{matched}</code> {title}</li>'
                )
        parts.append("</ul>")

    dh = list(row.get("disclosure_hits") or [])
    if dh:
        kinds: list[str] = []
        for x in dh:
            k = str(x.get("kind") or "").strip()
            if k and k not in kinds:
                kinds.append(html.escape(k, quote=False))
        parts.append(
            f'<p style="margin-top:8px"><strong>당일 공시 {len(dh)}건</strong>'
            + (f" — {', '.join(kinds[:5])}" if kinds else "")
            + "</p>"
        )

    if not parts:
        return (
            "<p>예측 후보였으나 실제 급등 조건에 미달했습니다. "
            "갭 분석·키워드·공시 데이터가 비어 진단을 채우지 못했습니다.</p>"
        )
    return "".join(parts)


def explain_rise_reason_html(
    *,
    actual_ret: float | None,
    actual_intraday_pct: float | None = None,
    t_trading_day: date | None,
    actual_news_hits: list[dict] | None,
    disclosure_hits: list[dict] | None,
    news_evidence_collected: bool,
) -> str:
    """
    관측일 ``T`` 기준 실제 상승에 대한 참고용 요약(HTML).

    - ``actual_ret`` 이 없으면(미래·장전·데이터 없음) 상승 이유 블록은 비움에 가깝게 안내.
    - 뉴스·공시는 문자열 매칭 결과이며 인과를 단정하지 않음.
    """
    tlabel = t_trading_day.isoformat() if t_trading_day is not None else "관측일"

    if actual_ret is None:
        if actual_intraday_pct is not None:
            try:
                ip = float(actual_intraday_pct)
            except (TypeError, ValueError):
                ip = float("nan")
            if math.isfinite(ip):
                return (
                    "<p class=\"combo-tip-empty\"><strong>상승 이유</strong> — "
                    f"{tlabel} 기준 <strong>장중·실시간 등락률 약 {ip:.2f}%</strong>입니다 "
                    "(종가 확정 전). 장 마감 후 일봉 기준으로 뉴스·공시 매칭 해석이 달라질 수 있습니다.</p>"
                )
        return (
            "<p class=\"combo-tip-empty\"><strong>상승 이유</strong> — "
            f"{tlabel} 기준 <strong>실제 등락률이 아직 없거나 미확정</strong>입니다. "
            "장 마감·시세 반영 후에는 아래에 뉴스·공시 매칭을 채울 수 있습니다.</p>"
        )

    act_pct = float(actual_ret) * 100.0
    if float(actual_ret) <= 0:
        return (
            f"<p><strong>상승 이유</strong> — 전일 대비 종가 기준 <strong>{act_pct:.2f}%</strong>로 "
            "당일은 <strong>상승</strong>으로 보기 어렵습니다(보합·하락).</p>"
        )

    chunks: list[str] = []

    if news_evidence_collected:
        nh = list(actual_news_hits or [])
        if nh:
            chunks.append(
                "<p><strong>관측일·전후 참고 뉴스</strong> "
                "(종목명·키워드가 제목·요약에 포함된 기사, 인과 아님)</p><ul class=\"nl\">"
            )
            for h in nh[:8]:
                day_o = h.get("day")
                day_s = day_o.isoformat() if isinstance(day_o, date) else html.escape(str(day_o or ""), quote=False)
                title = str(h.get("title") or "")
                matched = str(h.get("matched") or "")
                link = (h.get("link") or "").strip()
                te = html.escape(title, quote=False)
                me = html.escape(matched, quote=False)
                if link:
                    le = html.escape(link, quote=True)
                    chunks.append(
                        f'<li><span class="pill">{day_s}</span> '
                        f'<code style="font-size:0.75rem;color:var(--warn)">{me}</code> '
                        f'<a href="{le}" target="_blank" rel="noopener">{te}</a></li>'
                    )
                else:
                    chunks.append(
                        f'<li><span class="pill">{day_s}</span> '
                        f'<code style="font-size:0.75rem;color:var(--warn)">{me}</code> {te}</li>'
                    )
            chunks.append("</ul>")
        else:
            chunks.append(
                "<p>관측일 구간에서 종목명·키워드와 겹친 뉴스 제목을 찾지 못했습니다. "
                "이슈가 제목에 드러나지 않았거나 수집 범위 밖일 수 있습니다.</p>"
            )

        dh = list(disclosure_hits or [])
        if dh:
            chunks.append("<p><strong>당일 공시</strong>(네이버 증권 종목 공시)</p><ul class=\"nl\">")
            for d in dh[:8]:
                kind = html.escape(str(d.get("kind") or ""), quote=False)
                title = html.escape(str(d.get("title") or ""), quote=False)
                link = (d.get("link") or "").strip()
                if link:
                    le = html.escape(link, quote=True)
                    chunks.append(
                        f'<li><code style="font-size:0.75rem;color:#9fd3ff">{kind}</code> '
                        f'<a href="{le}" target="_blank" rel="noopener">{title}</a></li>'
                    )
                else:
                    chunks.append(
                        f'<li><code style="font-size:0.75rem;color:#9fd3ff">{kind}</code> {title}</li>'
                    )
            chunks.append("</ul>")
        else:
            chunks.append("<p>당일 네이버 종목 공시에서 표시할 항목이 없거나 매칭되지 않았습니다.</p>")
    else:
        chunks.append(
            "<p><em>이 실행</em>에서는 관측일 뉴스·공시 상세 매칭을 생략했습니다. "
            "<code>python main.py YYYYMMDD</code> 단일일 리포트에서는 동일 항목이 채워집니다.</p>"
        )

    chunks.append(
        "<p class=\"combo-tip-empty\" style=\"margin-top:10px;font-size:0.82em\">"
        "위는 자동 매칭·공시 목록에 따른 참고 요약이며, 주가 상승의 원인을 단정하지 않습니다.</p>"
    )
    return "".join(chunks)


def explain_miss(
    pred: PredictionRow,
    actual_ret: float | None,
    news_blob: str,
    kospi_change_hint: str | None = None,
) -> str:
    """레거시 plain text — ``explain_miss_html`` 스트립 버전."""
    import re as _re

    html_out = explain_miss_html(
        pred,
        actual_ret,
        news_blob_early=news_blob,
        kospi_change_hint=kospi_change_hint,
    )
    plain = _re.sub(r"<[^>]+>", " ", html_out)
    return _re.sub(r"\s+", " ", plain).strip()


def explain_miss_html(
    pred: PredictionRow,
    actual_ret: float | None,
    *,
    news_blob_early: str = "",
    kospi_change_hint: str | None = None,
    late_keywords_matched: bool | None = None,
    disclosure_hits: list[dict] | None = None,
    pred_news_hits: list[dict] | None = None,
    actual_news_hits: list[dict] | None = None,
    t_trading_day: date | None = None,
    gap_analysis_html: str | None = None,
) -> str:
    """
    예측 대비 실제 하락(또는 큰 미스)에 대한 종목별 HTML 분석.

    ``false_negatives`` 블록·집중 점검용. 뉴스·공시·키워드·갭 분석을 종목마다 다르게 채웁니다.
    """
    if actual_ret is None:
        return (
            "<p>해당 일자에 거래 데이터가 없거나 휴장·신규상장 등으로 "
            "상승률을 계산하지 못했습니다.</p>"
        )
    if actual_ret >= 0:
        return "<p>음수 구간이 아니므로 오판 집중 분석 대상에서 제외됩니다.</p>"

    act_pct = float(actual_ret) * 100.0
    pred_pct = float(pred.predicted_return_pct)
    diff = pred_pct - act_pct
    tlabel = t_trading_day.isoformat() if t_trading_day is not None else "관측일"

    parts: list[str] = [
        f"<p><strong>오판 요약</strong> — 예측 <strong>{pred_pct:.2f}%</strong> · "
        f"실제 <strong class=\"bad\">{act_pct:.2f}%</strong> · "
        f"편차 <strong>{diff:+.2f}</strong>pp "
        f"({tlabel})</p>"
    ]

    if pred.reasons:
        reason_bits = [
            html.escape(str(x).strip(), quote=False)
            for x in pred.reasons[:4]
            if str(x).strip()
        ]
        if reason_bits:
            parts.append(
                "<p><strong>예측 근거(모델)</strong></p><ul class=\"nl\">"
                + "".join(f"<li>{b}</li>" for b in reason_bits)
                + "</ul>"
            )

    if pred.matched_keywords:
        kw_show = ", ".join(
            html.escape(str(k), quote=False) for k in pred.matched_keywords[:12]
        )
        parts.append(
            f"<p><strong>매칭 키워드 {len(pred.matched_keywords)}개</strong> — "
            f"<code style=\"font-size:0.8rem;color:var(--warn)\">{kw_show}</code></p>"
        )

    pred_hits = list(pred_news_hits or [])
    if pred_hits:
        parts.append(
            "<p><strong>예측 입력(early) 뉴스</strong> — 아래 기사가 신호를 키웠을 수 있습니다.</p>"
            "<ul class=\"nl\">"
        )
        for h in pred_hits[:6]:
            title = html.escape(str(h.get("title") or ""), quote=False)
            matched = html.escape(str(h.get("matched") or ""), quote=False)
            day_o = h.get("day")
            day_s = (
                day_o.isoformat()
                if isinstance(day_o, date)
                else html.escape(str(day_o or ""), quote=False)
            )
            link = (h.get("link") or "").strip()
            if link:
                le = html.escape(link, quote=True)
                parts.append(
                    f'<li><span class="pill">{day_s}</span> '
                    f'<code style="font-size:0.75rem;color:var(--warn)">{matched}</code> '
                    f'<a href="{le}" target="_blank" rel="noopener">{title}</a></li>'
                )
            else:
                parts.append(
                    f'<li><span class="pill">{day_s}</span> '
                    f'<code style="font-size:0.75rem;color:var(--warn)">{matched}</code> {title}</li>'
                )
        parts.append("</ul>")

    act_hits = list(actual_news_hits or [])
    if act_hits:
        parts.append(
            f"<p><strong>{tlabel} 전후 실제 뉴스</strong> — 당일 흐름·악재 단서.</p>"
            "<ul class=\"nl\">"
        )
        for h in act_hits[:8]:
            title = html.escape(str(h.get("title") or ""), quote=False)
            matched = html.escape(str(h.get("matched") or ""), quote=False)
            day_o = h.get("day")
            day_s = (
                day_o.isoformat()
                if isinstance(day_o, date)
                else html.escape(str(day_o or ""), quote=False)
            )
            link = (h.get("link") or "").strip()
            if link:
                le = html.escape(link, quote=True)
                parts.append(
                    f'<li><span class="pill">{day_s}</span> '
                    f'<code style="font-size:0.75rem;color:var(--muted)">{matched}</code> '
                    f'<a href="{le}" target="_blank" rel="noopener">{title}</a></li>'
                )
            else:
                parts.append(
                    f'<li><span class="pill">{day_s}</span> '
                    f'<code style="font-size:0.75rem;color:var(--muted)">{matched}</code> {title}</li>'
                )
        parts.append("</ul>")
    elif pred.name and pred.name not in news_blob_early:
        parts.append(
            f"<p><strong>{tlabel} 뉴스</strong> — 예측 입력·당일 구간 모두에서 "
            f"「{html.escape(pred.name, quote=False)}」 직접 언급 기사를 찾지 못했습니다. "
            "테마만 겹친 뒤 개별 종목 수급이 붙지 않았을 수 있습니다.</p>"
        )

    miss_bullets: list[str] = []
    if diff > 15:
        miss_bullets.append(
            "예측치는 과거 급등일 평균·클램프 기반이라 실제 하락일과 크게 어긋났습니다. "
            "당일 시장·업종 약세·차익 실현·거래량 부족을 우선 의심합니다."
        )
    if late_keywords_matched is True:
        cut = f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}"
        miss_bullets.append(
            f"<strong>N-1일 {cut} 이후</strong> 뉴스에 예측 키워드가 있었습니다. "
            "장 마감 후 악재·실망 매물이 나왔거나, 호재가 이미 선반영됐을 수 있습니다."
        )
    elif late_keywords_matched is False:
        miss_bullets.append(
            "지연 구간 뉴스에서 예측 키워드가 뚜렷하지 않습니다. "
            "테마 확산 실패·수급 이탈·기술적 조정 가능성이 큽니다."
        )
    if pred.name in news_blob_early and not pred_hits:
        miss_bullets.append(
            "종목명은 early 뉴스에 있었으나 제목 매칭 히트가 적습니다. "
            "단순 거론·부정 헤드라인·동명이인 여부를 기사별로 확인하세요."
        )
    if kospi_change_hint:
        miss_bullets.append(kospi_change_hint)

    dh = list(disclosure_hits or [])
    if dh:
        kinds: list[str] = []
        for x in dh:
            k = str(x.get("kind", "")).strip()
            if k and k not in kinds:
                kinds.append(html.escape(k, quote=False))
        miss_bullets.append(
            f"당일 공시 <strong>{len(dh)}건</strong>"
            + (f" (유형: {', '.join(kinds[:5])})" if kinds else "")
            + " — 유상증자·감자·소송·거래정지 등 악재 공시 여부를 확인하세요."
        )

    if not miss_bullets:
        miss_bullets.append(
            "뉴스 키워드는 과거 급등 패턴과 겹쳤으나 당일 가격은 반대로 움직였습니다. "
            "동일 테마 내 다른 종목 대비 수급·시총·공매도 비중을 비교해 보세요."
        )

    parts.append(
        "<p><strong>왜 틀렸나 (가설)</strong></p><ul class=\"nl\">"
        + "".join(f"<li>{b}</li>" for b in miss_bullets[:7])
        + "</ul>"
    )

    if gap_analysis_html and str(gap_analysis_html).strip():
        parts.append(
            "<details style=\"margin-top:8px\"><summary style=\"cursor:pointer;color:var(--muted)\">"
            "예측 vs 실제 갭 상세</summary>"
            f"<div style=\"margin-top:6px\">{gap_analysis_html}</div></details>"
        )

    return "".join(parts)
