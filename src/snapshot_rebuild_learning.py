"""
``--rebuild-train-snapshot`` + From~To 실행 시 ``breakout_train_snapshot.json`` 에 병합되는 확장 학습 묶음.

- ``market_theme_flow``: 관측일 T별 early 뉴스 기반 시장 키워드·테마 시드 교차·급등/준급등 다발 구간 요약.
- ``prediction_gap_rollup``: ``prediction_accuracy_track.json`` 에서 구간별 괴리·버킷 통계 스냅샷.
- ``rebuild_learning``: 기존 일자와 병합(동일 ``trading_day`` 는 최신 실행으로 덮어씀) 후 요약 재계산.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from . import config, features, news, predict, trading_calendar, train_snapshot


def _news_blob_for_trading_day(
    news_by_calendar: dict[date, list[dict[str, str]]], t_day: date
) -> str:
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by_calendar, t_day)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(t_day)
    return predict.aggregate_news_for_window(news_by_calendar, ws, we)


def _theme_seed_lexicon() -> set[str]:
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


def _merge_by_trading_day(
    old_rows: list[dict[str, Any]] | None,
    new_rows: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
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
    cum_abs_gap = 0.0
    cum_gap_n = 0
    cum_ph_hits = 0
    cum_ph_den = 0
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
        row["cum_through_mean_abs_gap_pct"] = (
            round(cum_abs_gap / cum_gap_n, 4) if cum_gap_n else None
        )
        row["pred_high_precision_cumulative"] = (
            round(cum_ph_hits / cum_ph_den, 4) if cum_ph_den else None
        )
        row["pred_high_n_with_actual_cumulative"] = cum_ph_den
        fixed.append(row)
    summary = {
        "days": len(fixed),
        "final_cum_mean_abs_gap_pct": round(cum_abs_gap / cum_gap_n, 4) if cum_gap_n else None,
        "final_pred_high_precision": round(cum_ph_hits / cum_ph_den, 4) if cum_ph_den else None,
        "pred_high_total_with_actual": cum_ph_den,
        "big_move_threshold": thr,
    }
    return fixed, summary


def merge_rebuild_learning_dict(old: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
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
