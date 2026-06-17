"""6월 리포트·파이프라인 예측 적중률 요약.

``output/report_2026.06.html`` 의 고확신·중확신 행을 파싱해 20% 급등 적중률·Hit@K 를 출력합니다.
v12/v13 변경 후 리포트 재생성 뒤 정확도 회귀 여부를 빠르게 확인할 때 사용합니다.

실행: ``python scripts/diag_pred_accuracy_jun.py`` (프로젝트 루트, PYTHONPATH 불필요)
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src import config, news, predict, prediction_ranking, stocks, trading_calendar, train_snapshot

THR = float(config.BIG_MOVE_THRESHOLD)


def parse_report_html(path: Path) -> dict[str, dict[str, list[float | None]]]:
    html = path.read_text(encoding="utf-8")
    by_day: dict[str, dict[str, list[float | None]]] = {}
    for m in re.finditer(
        r'id="day-(\d{4}-\d{2}-\d{2})"[\s\S]*?(?=<section\s|</body>)',
        html,
    ):
        day = m.group(1)
        body = m.group(0)
        bands: dict[str, list[float | None]] = {"high": [], "mid": []}
        for row_m in re.finditer(
            r'data-rise-band="(high|mid)"[\s\S]*?고확신[\s\S]*?data-sort-col="actual" data-sort-value="([^"]*)"',
            body,
        ):
            band = row_m.group(1)
            raw = row_m.group(2).strip()
            try:
                bands[band].append(float(raw))
            except ValueError:
                bands[band].append(None)
        by_day[day] = bands
    return by_day


def hit_stats(by_day: dict[str, dict[str, list[float | None]]]) -> None:
    print("=== report HTML (pred_high / pred_mid vs actual 20%+) ===")
    th = tm = tn = mn = 0
    for day in sorted(by_day):
        h = by_day[day]["high"]
        mid = by_day[day]["mid"]
        hh = sum(1 for x in h if x is not None and x >= THR)
        hm = sum(1 for x in mid if x is not None and x >= THR)
        th += hh
        tm += hm
        tn += len(h)
        mn += len(mid)
        print(
            f"  {day}: high {hh}/{len(h)} mid {hm}/{len(mid)} "
            f"(high actual% sample: {[round(x * 100, 1) if x is not None else None for x in h[:4]]})"
        )
    print(
        f"TOTAL high: {th}/{tn} = {100 * th / max(1, tn):.1f}% | "
        f"mid: {tm}/{mn} = {100 * tm / max(1, mn):.1f}%"
    )


def live_replay_june() -> None:
    print("\n=== live replay (ML rank pool) ===")
    print(
        f"PRED_OUTPUT_MAX={config.PRED_OUTPUT_MAX} "
        f"HIGH_PROB={config.PRED_ML_HIGH_CONFIDENCE_PROB} "
        f"MID_PROB={config.PRED_ML_MID_CONFIDENCE_PROB} "
        f"MIN_HITS={config.PRED_MIN_KEYWORD_HITS}"
    )
    test_days = [
        d
        for d in (
            date(2026, 6, 1),
            date(2026, 6, 2),
            date(2026, 6, 4),
            date(2026, 6, 5),
            date(2026, 6, 8),
            date(2026, 6, 9),
            date(2026, 6, 10),
            date(2026, 6, 11),
            date(2026, 6, 12),
            date(2026, 6, 15),
        )
        if trading_calendar.is_trading_day(d)
    ]
    ohlcv = pd.read_parquet(config.CACHE_DIR / "ohlcv_long_full.parquet")
    returns = stocks.daily_returns_table(ohlcv)
    returns_ml = stocks.enrich_daily_returns_for_ml(returns)
    listing = stocks.load_listing()
    codes = listing["Code"].astype(str).str.zfill(6).tolist()
    names = {str(r["Code"]).zfill(6): str(r["Name"]) for _, r in listing.iterrows()}
    from src import ml_move_rank, train_snapshot as ts_mod
    snap = ts_mod.load_snapshot(config.TRAIN_SNAPSHOT_PATH)
    events = snap.events if snap else []

    news_by: dict = {}
    d = date(2026, 5, 1)
    while d <= date(2026, 6, 15):
        p = news.day_news_json_path(d)
        if p.is_file():
            news_by[d] = json.loads(p.read_text(encoding="utf-8"))
        d += timedelta(days=1)

    fp = ts_mod.fingerprint()
    th = tm = tn = mn = h10 = 0
    for T in test_days:
        blob, _ = news.aggregate_early_late_for_target(news_by, T)
        bundle = ml_move_rank.fit_or_load_classifier(
            train_events=events,
            news_by_calendar=news_by,
            returns_ml=returns_ml,
            listing_names=names,
            fp=fp,
            label_before_exclusive=T,
        )
        preds = predict.predict_for_trading_day(
            T,
            codes,
            names,
            events,
            blob,
            top_n=config.PRED_RANK_POOL_N,
            min_keyword_hits=config.PRED_MIN_KEYWORD_HITS,
            ml_bundle=bundle,
            returns_ml=returns_ml,
        )
        actual = stocks.big_movers_on_date(returns, T, THR)
        actual_set = set(actual["Code"].astype(str).str.zfill(6))
        ranked = [p.code for p in preds]
        hit = prediction_ranking.compute_hit_at_k_metrics(
            ranked, actual_set, universe_size=len(codes)
        )
        high = [p for p in preds if prediction_ranking.is_high_confidence_prediction(p)]
        mid = [p for p in preds if prediction_ranking.is_mid_confidence_prediction(p)]
        hh = sum(1 for p in high if p.code in actual_set)
        hm = sum(1 for p in mid if p.code in actual_set)
        th += hh
        tm += hm
        tn += len(high)
        mn += len(mid)
        h10 += int(hit.get("hit_at_10") or 0)
        print(
            f"  {T}: actual_20={len(actual_set)} hit@10={hit.get('hit_at_10')} "
            f"high {hh}/{len(high)} mid {hm}/{len(mid)} pool={len(preds)}"
        )
    print(
        f"TOTAL high: {th}/{tn} = {100 * th / max(1, tn):.1f}% | "
        f"mid: {tm}/{mn} = {100 * tm / max(1, mn):.1f}% | hit@10 sum={h10}"
    )


def main() -> None:
    report = config.OUTPUT_DIR / "report_2026.06.html"
    if report.is_file():
        hit_stats(parse_report_html(report))
    else:
        print(f"no report at {report}")
    live_replay_june()


if __name__ == "__main__":
    main()
