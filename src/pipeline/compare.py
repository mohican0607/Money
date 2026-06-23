"""비교 표 rows_compare 구축·갭 분석."""
from __future__ import annotations

import math
from typing import Any

from src import config, predict, prediction_ranking

def _append_compare_row_from_prediction(
    rows_compare: list[dict],
    pr: predict.PredictionRow,
    *,
    market_by_code: dict[str, str],
    pred_pct_min: float,
    pred_pct_mid_min: float,
    actual_ret: float | None,
    reasons_html: str,
    blob: str,
    kospi_hint: str | None,
    late_blob: str,
    pr_for_gap: predict.PredictionRow | None,
) -> None:
    """``PredictionRow`` 한 건을 비교 표 ``rows_compare`` 행 dict로 append합니다."""
    ph = prediction_ranking.is_high_confidence_prediction(pr)
    pm = prediction_ranking.is_mid_confidence_prediction(pr)
    rows_compare.append(
        {
            "code": pr.code,
            "market_segment": market_by_code.get(str(pr.code).zfill(6), "other"),
            "name": pr.name,
            "reasons_html": reasons_html,
            "keywords": pr.matched_keywords,
            "keyword_hits": pr.keyword_hits,
            "mention_score": pr.mention_score,
            "pred_ret": pr.predicted_return_pct,
            "ml_prob": pr.ml_prob,
            "rank_position": getattr(pr, "rank_position", None),
            "confidence_tier": getattr(pr, "confidence_tier", "none"),
            "actual_ret": actual_ret,
            "actual_big": actual_ret is not None and actual_ret >= config.BIG_MOVE_THRESHOLD,
            "pred_high": ph,
            "pred_mid": pm,
            "rise_band": _rise_band_for_row(pr.predicted_return_pct, actual_ret),
            "gap_analysis_html": _gap_analysis_html_for_row(
                pr.predicted_return_pct,
                actual_ret,
                pr_for_gap or pr,
                pr.matched_keywords,
                blob,
                kospi_hint,
                late_blob,
            ),
            "late_news_hit": None,
            **_pred_reason_fields(pr_for_gap or pr, reasons_html),
        }
    )


def _sync_forward_day_rows_from_predictions(
    rows_compare: list[dict],
    preds: list[predict.PredictionRow],
    *,
    market_by_code: dict[str, str],
    pred_pct_min: float,
    pred_pct_mid_min: float,
    blob: str,
    kospi_hint: str | None,
    late_blob: str,
) -> None:
    """예측 전용일: 상위 후보 전부 표에 넣고, 빠진 ``pred_ret`` 를 보강."""
    by_code = {str(r.get("code", "")).zfill(6): r for r in rows_compare if r.get("code")}
    for pr in preds:
        if not math.isfinite(float(pr.predicted_return_pct)):
            continue
        code = str(pr.code).zfill(6)
        reasons_html = "<br/>".join(pr.reasons)
        existing = by_code.get(code)
        if existing is not None:
            existing["pred_ret"] = pr.predicted_return_pct
            existing["ml_prob"] = pr.ml_prob
            existing["rank_position"] = getattr(pr, "rank_position", None)
            existing["confidence_tier"] = getattr(pr, "confidence_tier", "none")
            existing["pred_high"] = prediction_ranking.is_high_confidence_prediction(pr)
            existing["pred_mid"] = prediction_ranking.is_mid_confidence_prediction(pr)
            existing["rise_band"] = _rise_band_for_row(
                pr.predicted_return_pct, existing.get("actual_ret")
            )
            if not existing.get("reasons_html"):
                existing["reasons_html"] = reasons_html
            if not existing.get("keywords"):
                existing["keywords"] = list(pr.matched_keywords)
            continue
        _append_compare_row_from_prediction(
            rows_compare,
            pr,
            market_by_code=market_by_code,
            pred_pct_min=pred_pct_min,
            pred_pct_mid_min=pred_pct_mid_min,
            actual_ret=None,
            reasons_html=reasons_html,
            blob=blob,
            kospi_hint=kospi_hint,
            late_blob=late_blob,
            pr_for_gap=pr,
        )
        by_code[code] = rows_compare[-1]
def _pred_reason_hit_line(pr: predict.PredictionRow | None) -> str:
    """표 ``이유/차이`` 열: ``{n}개 교집합`` — 숫자에 키워드 목록 툴팁(HTML)."""
    if pr is None:
        return "—"
    n_hit = int(getattr(pr, "keyword_hits", 0) or 0)
    if n_hit > 0:
        return predict.keyword_intersection_hit_line_html(n_hit, pr.matched_keywords)
    for line in pr.reasons:
        if "교집합" in line:
            m = re.search(r"교집합\s+(\d+)개", line)
            if m:
                n = int(m.group(1))
                if n > 0:
                    return predict.keyword_intersection_hit_line_html(n, pr.matched_keywords)
    return "—"


def _prediction_signal_html(r: dict) -> str:
    """표 ``예측 신호`` 열: 교집합·종목명 언급·ML·순위·확신 요약."""
    parts: list[str] = []
    kh = r.get("keyword_hits")
    if kh is not None:
        kh_i = int(kh)
        if kh_i > 0:
            parts.append(
                f'<span title="당일 early 뉴스 ∩ 과거 급등 프로필">교집합 {kh_i}</span>'
            )
    ms = r.get("mention_score")
    if ms is not None:
        fv = float(ms)
        if fv >= 0.15:
            parts.append(f'<span title="뉴스 제목·본문 종목명 언급">언급 {fv:.2f}</span>')
    mp = r.get("ml_prob")
    if mp is not None and math.isfinite(float(mp)):
        parts.append(f'<span class="warn" title="ML 급등 확률">P {float(mp) * 100:.0f}%</span>')
    rp = r.get("rank_position")
    if rp is not None:
        parts.append(f'<span title="당일 예측 순위">#{int(rp)}</span>')
    ct = str(r.get("confidence_tier") or "")
    if ct == "high":
        parts.append(
            '<span class="pill" style="font-size:0.68rem;background:#3d2a1a;color:var(--warn)">고확신</span>'
        )
    elif ct == "mid":
        parts.append('<span class="pill" style="font-size:0.68rem">중확신</span>')
    if r.get("late_news_hit") is True:
        parts.append(
            '<span class="pill" style="font-size:0.68rem;background:#1e3d2f;color:var(--ok)" '
            'title="컷오프 이후 뉴스에도 키워드 등장">14:30+</span>'
        )
    if not parts:
        if r.get("pred_ret") is None:
            return "—"
        return '<span class="muted" style="font-size:0.82rem">신호 약함</span>'
    return '<span style="font-size:0.8rem;line-height:1.55">' + " · ".join(parts) + "</span>"


def _enrich_rows_prediction_signal(rows_compare: list[dict]) -> None:
    """비교 행마다 ``prediction_signal_html`` (ML·키워드·확신 요약) 채움."""
    for r in rows_compare:
        r["prediction_signal_html"] = _prediction_signal_html(r)
def _rise_band_for_row(
    pred_ret_pct: float | None,
    actual_ret_ratio: float | None,
) -> str:
    """표시 구간: ``high``(>=20), ``mid``(10~20), ``low``(<10 또는 값 없음)."""
    max_pct = float("-inf")
    if pred_ret_pct is not None and math.isfinite(float(pred_ret_pct)):
        max_pct = max(max_pct, float(pred_ret_pct))
    if actual_ret_ratio is not None and math.isfinite(float(actual_ret_ratio)):
        max_pct = max(max_pct, float(actual_ret_ratio) * 100.0)
    if max_pct >= config.BIG_MOVE_THRESHOLD * 100.0 - 1e-9:
        return "high"
    if 10.0 - 1e-9 <= max_pct < config.BIG_MOVE_THRESHOLD * 100.0 - 1e-9:
        return "mid"
    return "low"
def _pred_reason_fields(
    pr: predict.PredictionRow | None,
    reasons_html: str,
    *,
    summary_max: int = 72,
) -> dict[str, str | bool]:
    """
    표 ``이유/차이`` 열·툴팁용.

    ``pred_reason_hit_line`` 은 표용 짧은 HTML(예: ``36개 교집합``, 숫자에 키워드 툴팁). 통합 보기에는 툴팁 없음.
    ``reasons_html`` 은 레거시(카드 등)용으로 유지.
    """
    hit_line = _pred_reason_hit_line(pr)
    if pr is not None:
        detail_html = predict.format_prediction_reason_detail_html(pr)
        plain = " ".join(pr.reasons)
    else:
        detail_html = reasons_html
        plain = re.sub(r"<[^>]+>", " ", reasons_html or "")
        plain = " ".join(plain.split())
    plain = plain.strip()
    if not plain:
        return {
            "pred_reason_summary": "—",
            "pred_reason_hit_line": hit_line,
            "pred_reason_detail_html": "—",
            "pred_reason_use_tooltip": False,
        }
    use_tooltip = len(plain) > summary_max or (pr is not None and len(pr.reasons) > 2)
    if len(plain) <= summary_max:
        summary = plain
    else:
        summary = plain[: summary_max - 1].rstrip() + "…"
    return {
        "pred_reason_summary": summary,
        "pred_reason_hit_line": hit_line,
        "pred_reason_detail_html": detail_html,
        "pred_reason_use_tooltip": use_tooltip,
    }


def _gap_analysis_html_for_row(
    pred_ret: float | None,
    act: float | None,
    pr: predict.PredictionRow | None,
    keywords: list[str],
    blob: str,
    kospi_hint: str | None,
    late_blob: str,
    disclosure_hits: list[dict] | None = None,
    *,
    actual_intraday_pct: float | None = None,
) -> str:
    """
    표 한 줄에 붙는 「예측 vs 실제」 갭 설명 HTML 조각을 생성합니다.

    컷오프가 켜져 있으면 late 뉴스에 키워드가 있었는지 ``late_blob_covers_keywords`` 로 넘깁니다.
    """
    late_hit: bool | None = None
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF and late_blob and keywords:
        late_hit = news.late_blob_covers_keywords(late_blob, list(keywords))
    return predict.explain_return_gap_html(
        pred_ret_pct=pred_ret,
        actual_ret=act,
        actual_intraday_pct=actual_intraday_pct,
        prediction_row=pr,
        news_blob_early=blob,
        kospi_change_hint=kospi_hint,
        late_keywords_matched=late_hit,
        disclosure_hits=disclosure_hits,
    )
