"""비교 표·DayReport 행 보강 (compare + enrich)."""
from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any

from src import (
    config,
    disclosure,
    features,
    news,
    predict,
    prediction_accuracy_cache,
    prediction_ranking,
    report,
    snapshot_rebuild_learning,
    stocks,
    trading_calendar,
)


# --- compare (from compare.py) ---

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

# --- enrich (from enrich.py) ---


def _movers_data_note_for_report(krx_unavailable: bool) -> str | None:
    """
    pykrx·KRX 전종목 등락률을 쓰지 못할 때 리포트 상단에 넣을 HTML 메타 각주 문구.

    ``SAMPLE_TICKERS`` 사용 여부에 따라 안내 문장이 달라집니다.
    """
    if not krx_unavailable:
        return None
    if config.SAMPLE_TICKERS_N:
        return (
            "시장 전체 20%↑: pykrx·KRX 일별 등락률 조회에 실패했습니다. "
            f"아래 표의 「실제」 급등은 다운로드한 OHLCV 안에서만 집계됩니다. SAMPLE_TICKERS={config.SAMPLE_TICKERS_N}(으)로 "
            "표본이 좁혀져 네이버 상한가 등 전 시장 현황과 다를 수 있습니다. SAMPLE_TICKERS를 비우고 재실행하는 것을 권장합니다."
        )
    return (
        "pykrx·KRX 전종목 등락률 조회에 실패했습니다. 아래 「실제」 20%↑는 OHLCV에 담긴 상장 종목(전종목 일봉 다운로드 시 전 시장에 가깝게) "
        "기준입니다. 네이버 시세와 맞추려면 pykrx가 동작하도록 네트워크·영업일을 확인하세요."
    )


def _enrich_rows_news_evidence(
    rows: list[dict],
    early_rows: list[tuple[date, dict]],
    actual_ctx_rows: list[tuple[date, dict]],
    *,
    target_trading_day: date,
) -> None:
    """
    비교 표 row에 ``pred_news_hits`` / ``actual_news_hits`` 를 채웁니다.

    ``include_target_calendar_news`` 일 때 호출됩니다(단일일·월간·구간 공통).
    """
    for r in rows:
        nm = r["name"]
        kw = r.get("keywords") or []
        r["pred_news_hits"] = news.match_stock_news_rows(
            early_rows, nm, kw, limit=8, target_trading_day=target_trading_day
        )
        r["actual_news_hits"] = news.match_stock_news_rows(
            actual_ctx_rows, nm, kw, limit=8, target_trading_day=target_trading_day
        )
def _actual_over_pred_ratio(pred_ret: float | None, actual_ret: float | None) -> float | None:
    """
    일별 **정합도** ``min(|실제%|, |예측%|) / max(|실제%|, |예측%|)`` (0~1).

    ``actual_ret`` 은 소수(0.2=20%), ``pred_ret`` 은 퍼센트 값.
    두 값의 크기가 완전히 같을 때만 1(100%), 차이가 커질수록 0에 가까워집니다.
    """
    if pred_ret is None or actual_ret is None:
        return None
    p = abs(float(pred_ret))
    a = float(actual_ret)
    if not math.isfinite(p) or not math.isfinite(a):
        return None
    if a < 0:
        return 0.0
    if abs(p) < 1e-9:
        return None
    a_pct = abs(a * 100.0)
    den = max(a_pct, p)
    if den < 1e-9:
        return None
    return min(a_pct, p) / den


def _prediction_row_strict_or_loose(
    code: str,
    names: dict[str, str],
    train_events: list,
    blob: str,
    scoring_ctx: tuple,
    min_hits: int,
) -> predict.PredictionRow | None:
    """
    엄격(``min_hits``) 행이 없으면 완화(``min_hits=0``)로 한 번 더 조회합니다.
    뉴스 재보강 후 갭 HTML·툴팁이 예측 행 없이 끊기지 않게 합니다.
    """
    pr = predict.prediction_row_for_code(
        code, names, train_events, blob, scoring_ctx, min_hits
    )
    if pr is not None:
        return pr
    return predict.prediction_row_for_code(
        code, names, train_events, blob, scoring_ctx, 0
    )


def _enrich_cumulative_actual_over_pred_from_history(day_reports: list) -> None:
    """
    ``pred_high_history`` 가 있으면 ``cumulative_accuracy_avg`` 를 이력만으로 다시 씁니다.

    각 이력 행에 대해 ``min(|실제%|,|예측%|)/max(|실제%|,|예측%|)``(0~1)을 더합니다.
    예측이 ``0`` 에 가깝거나 실적·예측 값이 없으면 그 항목은 ``0``을 더합니다.
    평균은 **이력 행 개수**로 나눕니다.
    """
    for dr in day_reports:
        for r in dr.rows_compare:
            if not bool(r.get("pred_high")):
                continue
            hist = r.get("pred_high_history") or []
            items = sorted(
                (h for h in hist if isinstance(h, dict)),
                key=lambda h: str(h.get("t", "")),
            )
            n = len(items)
            if n == 0:
                continue
            acc = 0.0
            for h in items:
                pr = h.get("pred_pct")
                ap = h.get("actual_pct")
                if ap is None or pr is None:
                    acc += 0.0
                    continue
                prf = abs(float(pr))
                apv = float(ap)
                if apv < 0:
                    acc += 0.0
                    continue
                apf = abs(apv)
                den = max(prf, apf)
                if den < 1e-9:
                    acc += 0.0
                else:
                    acc += min(prf, apf) / den
            r["cumulative_accuracy_avg"] = acc / n
            r["cumulative_accuracy_from_hist"] = True


def _enrich_cumulative_actual_over_pred_from_history_for_field(
    day_reports: list[report.DayReport],
    *,
    history_field: str,
    out_field: str,
) -> None:
    """지정 이력 필드 기준 누적 달성률 평균(0~1)을 ``out_field`` 에 씁니다."""
    for dr in day_reports:
        for r in dr.rows_compare:
            hist = r.get(history_field) or []
            items = sorted(
                (h for h in hist if isinstance(h, dict)),
                key=lambda h: str(h.get("t", "")),
            )
            n = len(items)
            if n == 0:
                r[out_field] = None
                continue
            acc = 0.0
            for h in items:
                pr = h.get("pred_pct")
                ap = h.get("actual_pct")
                if ap is None or pr is None:
                    continue
                prf = abs(float(pr))
                apv = float(ap)
                if apv < 0:
                    continue
                apf = abs(apv)
                den = max(prf, apf)
                if den >= 1e-9:
                    acc += min(prf, apf) / den
            r[out_field] = acc / n


def _enrich_rows_actual_ret_prev_day(
    rows: list[dict], returns, t_day: date
) -> None:
    """각 행에 관측일 T 직전 KRX 영업일의 일봉 수익률(소수, ``actual_ret`` 와 동일)을 붙입니다."""
    try:
        prev_td = trading_calendar.last_trading_day_before(t_day)
    except ValueError:
        for r in rows:
            r["actual_ret_prev_day"] = None
        return
    for r in rows:
        code = str(r.get("code", "")).zfill(6)
        if not code:
            r["actual_ret_prev_day"] = None
            continue
        r["actual_ret_prev_day"] = stocks.actual_return_on_date(returns, code, prev_td)


def _enrich_rows_stock_ret_tooltip(
    rows: list[dict],
    returns,
    t_day: date,
    *,
    n_day: date | None = None,
    now_kst: datetime | None = None,
) -> None:
    """종목별 tooltip: 기준일 N(현재)·N-1·N-2 일봉 등락률(%)."""
    try:
        n_basis = n_day or trading_calendar.last_trading_day_before(t_day)
    except ValueError:
        for r in rows:
            r["stock_ret_tooltip"] = []
        return

    day_chain: list[tuple[str, date]] = []
    d = n_basis
    for label in ("N", "N-1", "N-2"):
        day_chain.append((label, d))
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break

    today = (now_kst or datetime.now(trading_calendar.KST)).date()
    intraday_map: dict[str, float] = {}
    if (
        n_basis == today
        and trading_calendar.is_trading_day(n_basis)
        and not trading_calendar.is_krx_daily_bar_effective_closed(n_basis, now_kst=now_kst)
    ):
        codes = sorted({str(r.get("code", "")).zfill(6) for r in rows if r.get("code")})
        if codes:
            snap = stocks.best_effort_intraday_pct_by_code(n_basis, codes, returns_df=returns)
            if snap:
                intraday_map = snap

    for r in rows:
        code = str(r.get("code", "")).zfill(6)
        tips: list[dict[str, object]] = []
        for label, td in day_chain:
            ret = stocks.actual_return_on_date(returns, code, td) if code else None
            intraday = False
            pct: float | None = float(ret) * 100.0 if ret is not None else None
            if label == "N" and pct is None and code and code in intraday_map:
                pct = float(intraday_map[code])
                intraday = True
            tips.append(
                {
                    "label": label,
                    "date": td.isoformat(),
                    "pct": pct,
                    "intraday": intraday,
                }
            )
        r["stock_ret_tooltip"] = tips


def _build_rebuild_learning_payload(
    day_reports: list[report.DayReport],
    s0: date,
    s1: date,
) -> dict[str, object]:
    """
    From~To(포함) 캘린더 구간에 해당하는 ``DayReport`` 만 골라,
    일자별 뉴스 샘플 규모·예측%−실제% 괴리·누적 MAE·pred_high 적중 누적을 만듭니다.

    ``snapshot_rebuild_learning.merge_extended_rebuild_into_snapshot`` 에 넘길
    ``rebuild_learning`` 하위 dict(``daily``·``summary`` 등)를 만듭니다.
    """
    thr = float(config.BIG_MOVE_THRESHOLD)
    in_range = [
        dr
        for dr in day_reports
        if s0 <= dr.trading_day <= s1 and not getattr(dr, "forward_observation", False)
    ]
    in_range.sort(key=lambda dr: dr.trading_day)

    cum_abs_gap = 0.0
    cum_gap_n = 0
    cum_ph_hits = 0
    cum_ph_den = 0

    # 구간 전체 원인 누적(예측 과대/과소, 고확신 오판, 지연뉴스 관여, 약신호 관여 등)
    total_over_pred = 0
    total_under_pred = 0
    total_false_high = 0
    total_false_high_neg = 0
    total_false_high_low = 0
    total_false_high_late_hit = 0
    total_false_high_weak_signal = 0
    false_high_kw_counter: Counter[str] = Counter()

    daily: list[dict[str, object]] = []
    for dr in in_range:
        gaps: list[float] = []
        for r in dr.rows_compare:
            pr = r.get("pred_ret")
            ar = r.get("actual_ret")
            if pr is None or ar is None:
                continue
            g = float(pr) - float(ar) * 100.0
            if math.isfinite(g):
                gaps.append(g)

        n_both = len(gaps)
        day_mae = sum(abs(x) for x in gaps) / n_both if n_both else None
        day_mean_gap = sum(gaps) / n_both if n_both else None

        cum_abs_gap += sum(abs(x) for x in gaps)
        cum_gap_n += n_both
        cum_mae = cum_abs_gap / cum_gap_n if cum_gap_n else None

        ph_hits = 0
        ph_den = 0
        over_pred = 0
        under_pred = 0
        false_high = 0
        false_high_neg = 0
        false_high_low = 0
        false_high_late_hit = 0
        false_high_weak_signal = 0
        day_false_high_kw_counter: Counter[str] = Counter()
        for r in dr.rows_compare:
            pr = r.get("pred_ret")
            ar = r.get("actual_ret")
            if pr is not None and ar is not None:
                g = float(pr) - float(ar) * 100.0
                if math.isfinite(g):
                    if g > 1e-9:
                        over_pred += 1
                    elif g < -1e-9:
                        under_pred += 1
            if not r.get("pred_high"):
                continue
            if ar is None:
                continue
            ph_den += 1
            if float(ar) >= thr:
                ph_hits += 1
            else:
                false_high += 1
                ar_f = float(ar)
                if ar_f < 0:
                    false_high_neg += 1
                elif ar_f < thr:
                    false_high_low += 1
                if bool(r.get("late_news_hit")):
                    false_high_late_hit += 1
                if int(r.get("keyword_hits", 0) or 0) <= 1 or float(
                    r.get("mention_score", 0.0) or 0.0
                ) < 0.10:
                    false_high_weak_signal += 1
                for kw in (r.get("keywords") or [])[:12]:
                    if isinstance(kw, str) and kw.strip():
                        day_false_high_kw_counter[kw.strip()] += 1
        cum_ph_hits += ph_hits
        cum_ph_den += ph_den
        day_prec = ph_hits / ph_den if ph_den else None
        cum_prec = cum_ph_hits / cum_ph_den if cum_ph_den else None

        titles = dr.news_titles_sample or []
        terms = dr.news_highlight_terms or []

        total_over_pred += over_pred
        total_under_pred += under_pred
        total_false_high += false_high
        total_false_high_neg += false_high_neg
        total_false_high_low += false_high_low
        total_false_high_late_hit += false_high_late_hit
        total_false_high_weak_signal += false_high_weak_signal
        false_high_kw_counter.update(day_false_high_kw_counter)

        missed_rows, pred_miss_rows = snapshot_miss_diagnosis.build_miss_rows_for_day(dr)

        hit_row = getattr(dr, "hit_at_k_metrics", None) or {}
        daily.append(
            {
                "trading_day": dr.trading_day.isoformat(),
                "news_titles_count": len(titles),
                "news_highlight_terms_count": len(terms),
                "news_highlight_terms_sample": terms[:12],
                "compare_rows": len(dr.rows_compare),
                "hit_at_k": hit_row if isinstance(hit_row, dict) else {},
                "rows_pred_actual_both": n_both,
                "mean_abs_gap_pred_minus_actual_pct": round(day_mae, 4)
                if day_mae is not None
                else None,
                "mean_gap_pred_minus_actual_pct": round(day_mean_gap, 4)
                if day_mean_gap is not None
                else None,
                "cum_through_mean_abs_gap_pct": round(cum_mae, 4)
                if cum_mae is not None
                else None,
                "pred_high_precision_today": round(day_prec, 4)
                if day_prec is not None
                else None,
                "pred_high_precision_cumulative": round(cum_prec, 4)
                if cum_prec is not None
                else None,
                "pred_high_n_with_actual_today": ph_den,
                "pred_high_n_with_actual_cumulative": cum_ph_den,
                "over_pred_count_today": over_pred,
                "under_pred_count_today": under_pred,
                "false_positive_high_today": false_high,
                "false_positive_high_negative_today": false_high_neg,
                "false_positive_high_low_return_today": false_high_low,
                "false_positive_high_late_news_hit_today": false_high_late_hit,
                "false_positive_high_weak_signal_today": false_high_weak_signal,
                "false_positive_high_top_keywords_today": [
                    {"keyword": k, "count": int(v)}
                    for k, v in day_false_high_kw_counter.most_common(10)
                ],
                "missed_big_movers_today": missed_rows,
                "pred_high_misses_today": pred_miss_rows,
            }
        )

    miss_summary = snapshot_miss_diagnosis.recompute_miss_summary_from_daily(daily)

    return {
        "rebuild_learning": {
            "calendar_from": s0.isoformat(),
            "calendar_to": s1.isoformat(),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "big_move_threshold": thr,
            "daily": daily,
            "summary": {
                "days": len(daily),
                "final_cum_mean_abs_gap_pct": round(cum_abs_gap / cum_gap_n, 4)
                if cum_gap_n
                else None,
                "final_pred_high_precision": round(cum_ph_hits / cum_ph_den, 4)
                if cum_ph_den
                else None,
                "pred_high_total_with_actual": cum_ph_den,
                "over_pred_count_total": total_over_pred,
                "under_pred_count_total": total_under_pred,
                "false_positive_high_total": total_false_high,
                "false_positive_high_negative_total": total_false_high_neg,
                "false_positive_high_low_return_total": total_false_high_low,
                "false_positive_high_late_news_hit_total": total_false_high_late_hit,
                "false_positive_high_weak_signal_total": total_false_high_weak_signal,
                "false_positive_high_top_keywords_total": [
                    {"keyword": k, "count": int(v)}
                    for k, v in false_high_kw_counter.most_common(20)
                ],
                **miss_summary,
            },
        }
    }
def _enrich_cumulative_hit_rate(
    day_reports: list[report.DayReport], *, threshold_pct: float
) -> None:
    """
    ``pred_high_history`` 기준으로, 예측이 ``threshold_pct`` 이상이었던 날의 실제 분포를 집계합니다.
    괄호 표기용으로 ``cumulative_hit_x``(실제≥threshold 건수), ``cumulative_hit_z``(0<실제<threshold 건수),
    ``cumulative_hit_neg``(실제<0 건수), ``cumulative_hit_y``(실적 확정·예측≥임계 전체)와
    ``cumulative_nonneg_rate_pct``(실제>=0 비율, 0~100)를 넣습니다.
    """
    thr = float(threshold_pct)
    for dr in day_reports:
        for r in dr.rows_compare:
            if not bool(r.get("pred_high")):
                r["cumulative_hit_rate_pct"] = None
                r["cumulative_hit_x"] = None
                r["cumulative_hit_z"] = None
                r["cumulative_hit_neg"] = None
                r["cumulative_hit_y"] = None
                r["cumulative_nonneg_rate_pct"] = None
                continue
            hist = r.get("pred_high_history") or []
            hits = 0
            mid_band = 0
            neg_band = 0
            nonneg_band = 0
            n_known = 0
            for h in hist:
                if not isinstance(h, dict):
                    continue
                pr = h.get("pred_pct")
                if pr is None or float(pr) + 1e-9 < thr:
                    continue
                ap = h.get("actual_pct")
                if ap is None:
                    continue
                n_known += 1
                apf = float(ap)
                if apf >= -1e-9:
                    nonneg_band += 1
                if apf >= thr - 1e-9:
                    hits += 1
                elif apf > 1e-9 and apf < thr - 1e-9:
                    mid_band += 1
                elif apf < -1e-9:
                    neg_band += 1
            r["cumulative_hit_rate_pct"] = (100.0 * hits / n_known) if n_known else None
            r["cumulative_hit_x"] = hits if n_known else None
            r["cumulative_hit_z"] = mid_band if n_known else None
            r["cumulative_hit_neg"] = neg_band if n_known else None
            r["cumulative_hit_y"] = n_known if n_known else None
            r["cumulative_nonneg_rate_pct"] = (
                100.0 * nonneg_band / n_known
            ) if n_known else None


def _enrich_cumulative_accuracy_avg(day_reports: list[report.DayReport]) -> None:
    """
    각 ``rows_compare`` 행에 ``cumulative_accuracy_avg`` 를 넣습니다.

    같은 종목코드에 대해, **이번 파이프라인 실행의 관측 거래일 T를 시간순**으로 보며
    일별 달성률 ``min(|실제%|/|예측%|,1)`` 를 쌓고, 해당 행 시점까지의 **산술 평균**(0~1)을 기록합니다.
    당일 비율을 계산할 수 없으면(예측·실제 중 하나 없음, 예측 0) 직전까지 평균만 씁니다.
    """
    history: dict[str, list[float]] = defaultdict(list)
    for dr in sorted(day_reports, key=lambda d: d.trading_day):
        for r in dr.rows_compare:
            # 전략 목표(익일 20%↑ 후보)와 맞추기 위해, 누적 정확도는 예측 20% 이상 후보만 집계.
            if not bool(r.get("pred_high")):
                r["cumulative_accuracy_avg"] = None
                continue
            code = str(r["code"])
            cur = _actual_over_pred_ratio(r.get("pred_ret"), r.get("actual_ret"))
            past = history[code]
            if cur is not None:
                r["cumulative_accuracy_avg"] = (sum(past) + cur) / (len(past) + 1)
                past.append(cur)
            elif past:
                r["cumulative_accuracy_avg"] = sum(past) / len(past)
            else:
                r["cumulative_accuracy_avg"] = None
def _enrich_rows_disclosure_hits(rows: list[dict], target_day: date) -> None:
    """rows_compare 각 행에 네이버 종목 공시 hit를 붙입니다."""
    codes = [str(r.get("code", "")).zfill(6) for r in rows if r.get("code")]
    by_code = disclosure.fetch_disclosures_for_codes_on_day(target_day, codes)
    for r in rows:
        code = str(r.get("code", "")).zfill(6)
        hits = by_code.get(code, [])
        r["disclosure_hits"] = hits[:8]


def _apply_preclose_actual_snapshot_rows(
    rows_compare: list[dict],
    *,
    snapshot_pct_by_code: dict[str, float] | None,
) -> None:
    """
    장 마감 전 표시용으로 ``rows_compare`` 각 행에 ``actual_ret_intraday_pct`` 를 채웁니다.

    - ``snapshot_pct_by_code``(pykrx 퍼센트 포인트)가 있으면 우선 사용
    - 없으면 이미 계산된 ``actual_ret``(소수)를 퍼센트 포인트로 변환해 보조 표시
    """
    if not rows_compare:
        return
    for r in rows_compare:
        code = str(r.get("code", "")).zfill(6)
        v: float | None = None
        if snapshot_pct_by_code is not None:
            raw = snapshot_pct_by_code.get(code)
            if raw is not None and math.isfinite(float(raw)):
                v = float(raw)
        if v is None:
            ar = r.get("actual_ret")
            if ar is not None and math.isfinite(float(ar)):
                v = float(ar) * 100.0
        if v is None:
            continue
        r["actual_ret_intraday_pct"] = v
        r["actual_cell_pre_close_snapshot"] = True


def _fetch_live_intraday_snapshot_pct_by_code(
    t_day: date,
    *,
    returns_df=None,
    attempts: int = 3,
    sleep_sec: float = 0.8,
) -> dict[str, float] | None:
    """
    main 실행 시점에 pykrx 장중 등락률 스냅샷을 강제 재조회합니다.

    장중에는 응답이 일시적으로 비는 경우가 있어 짧게 재시도합니다.
    ``returns_df`` 가 있으면 pykrx 전종목 실패 시 OHLCV 기반 등락률 맵으로 폴백합니다.
    """
    last: dict[str, float] | None = None
    for i in range(max(1, attempts)):
        snap = stocks.try_krx_change_pct_by_code(t_day, returns_df=returns_df)
        if snap:
            return snap
        last = snap
        if i + 1 < max(1, attempts):
            time.sleep(max(0.0, sleep_sec))
    return last


def _apply_postclose_actual_snapshot_rows(
    rows_compare: list[dict],
    *,
    snapshot_pct_by_code: dict[str, float] | None,
    threshold: float,
) -> None:
    """
    장 마감 후(pykrx 조회 가능 시) ``rows_compare`` 실제값을 스냅샷으로 즉시 갱신합니다.

    - ``actual_ret`` 는 pykrx 퍼센트 포인트를 소수 수익률로 변환해 반영
    - 종가 확정 전용 괄호 표시는 제거
    """
    if not rows_compare or not snapshot_pct_by_code:
        return
    for r in rows_compare:
        code = str(r.get("code", "")).zfill(6)
        raw = snapshot_pct_by_code.get(code)
        if raw is None or not math.isfinite(float(raw)):
            continue
        act = float(raw) / 100.0
        r["actual_ret"] = act
        r["actual_big"] = bool(act >= threshold)
        r.pop("actual_cell_pre_close_snapshot", None)


def _backfill_day_actuals_from_returns(
    dr: report.DayReport,
    *,
    returns,
    threshold: float,
) -> None:
    """
    ``returns``(일봉 수익률)에서 당일 실제값을 다시 읽어 ``DayReport``를 보정합니다.

    긴 실행이 장중에 시작되어도, 실행 말미에 장이 끝났다면 오늘(T)의 실제값 표기를 업데이트하기 위한 후처리.
    """
    t_day = dr.trading_day
    any_changed = False
    for r in dr.rows_compare:
        code = str(r.get("code", "")).zfill(6)
        act = stocks.actual_return_on_date(returns, code, t_day)
        if act is None:
            continue
        r["actual_ret"] = float(act)
        r["actual_big"] = bool(float(act) >= threshold)
        # 종가 확정값이 있으면 장중 스냅샷 표시는 제거
        r.pop("actual_cell_pre_close_snapshot", None)
        any_changed = True

    if any_changed:
        dr.rows_compare.sort(key=lambda r: (not r["actual_big"], not r["pred_high"], r["code"]))

    movers = stocks.big_movers_on_date(returns, t_day, threshold)
    dr.actual_big_movers = [
        {
            "code": str(r["Code"]).zfill(6),
            "name": str(r["Name"]),
            "ret_pct": float(r["return_pct"]) * 100.0,
        }
        for _, r in movers.iterrows()
    ]