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
        leaders: list[dict[str, Any]] = []
        kw_set = features.keyword_set(blob, k=120) if blob.strip() else frozenset()
        for _, row in movers.iterrows():
            code = str(row["Code"]).zfill(6)
            name = str(row.get("Name") or listing_names.get(code, ""))
            rp = float(row["return_pct"])
            name_kw = features.keyword_set(name, k=12)
            overlap = sorted(name_kw & kw_set)[:6]
            leaders.append(
                {
                    "code": code,
                    "name": name,
                    "return_pct": round(rp * 100.0, 3),
                    "name_keyword_overlap_with_news": overlap,
                }
            )
        out.append(
            {
                "trading_day": t_day.isoformat(),
                "news_chars": len(blob),
                "top_keywords_sample": top_kw[:24],
                "theme_seed_hits": seed_hits[:20],
                "n_movers_ge_20pct": n20,
                "n_movers_10_to_20pct": n10,
                "theme_leaders_10pct_plus": leaders,
            }
        )
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

    parts = [
        "<p class=\"sub\" style=\"margin:0 0 10px;line-height:1.55\">",
        f"<strong>{_esc(t_iso)}</strong> 종가 기준 "
        f"<strong>10%↑ {n10 + n20}</strong>종 "
        f"(그중 ≥{threshold_pct:.0f}% <strong>{n20}</strong>종). "
        f"아래 뉴스·키워드 상관은 예측·ML·휴리스틱과 같은 입력 시점 "
        f"(<strong>{_esc(n_buy)}</strong> 거래일 <strong>{_esc(news_cutoff_label)}</strong>(KST)까지 early, "
        f"약 <strong>{news_chars:,}</strong>자)입니다.</p>",
    ]
    if seeds:
        pills = "".join(
            f'<span class="pill" style="margin:2px 4px 2px 0">{_esc(str(s))}</span>'
            for s in seeds[:16]
        )
        parts.append(
            f"<p style=\"margin:0 0 8px\"><strong>테마 시드 × 뉴스 상위 키워드</strong> {pills}</p>"
        )
    if top_kw:
        kw_pills = "".join(
            f'<span class="pill" style="margin:2px 4px 2px 0;background:#1a2d42">{_esc(str(k))}</span>'
            for k in top_kw[:18]
        )
        parts.append(
            f"<p style=\"margin:0 0 10px\"><strong>early 뉴스 상위 키워드</strong> {kw_pills}</p>"
        )
    if leaders:
        parts.append(
            "<table class=\"rows-compare\" style=\"font-size:0.82rem;margin-top:6px\">"
            "<thead><tr>"
            "<th>종목</th><th>등락%</th><th>종목명·early 뉴스 키워드 겹침</th>"
            "</tr></thead><tbody>"
        )
        for L in leaders[:14]:
            if not isinstance(L, dict):
                continue
            nm = _esc(str(L.get("name") or ""))
            rp = L.get("return_pct")
            rp_s = f"{float(rp):+.2f}" if rp is not None else "—"
            ov = L.get("name_keyword_overlap_with_news") or []
            if ov:
                ov_s = ", ".join(_esc(str(x)) for x in ov[:8])
            else:
                ov_s = "<span style=\"color:var(--muted)\">겹침 없음</span>"
            parts.append(
                f"<tr><td>{nm}</td><td class=\"ok\" style=\"white-space:nowrap\">{rp_s}%</td>"
                f"<td>{ov_s}</td></tr>"
            )
        parts.append("</tbody></table>")
    else:
        parts.append(
            "<p class=\"sub\" style=\"margin:0\">당일 10% 이상 상승 종목이 없거나 집계되지 않았습니다.</p>"
        )
    parts.append(
        "<p class=\"combo-tip-empty\" style=\"margin:10px 0 0;font-size:0.78em\">"
        "종목명 토큰과 early 뉴스 키워드 집합의 단순 교집합이며, 인과·상관계수 검정이 아닙니다.</p>"
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
