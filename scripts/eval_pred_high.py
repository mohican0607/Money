#!/usr/bin/env python
"""관측일 구간 pred_high·Hit@10 오프라인 평가."""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import config, trading_calendar
from src.diagnostics.prediction import load_prediction_eval_context
from src.prediction import feedback_loop, prediction_ranking


def _parse_day(s: str) -> date:
    s = s.strip().replace("-", "")
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def trading_days_between(start: date, end: date) -> list[date]:
    out: list[date] = []
    d = start
    while d <= end:
        if trading_calendar.is_trading_day(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def eval_range(start: date, end: date, *, label: str = "") -> int:
    days = trading_days_between(start, end)
    if not days:
        print("거래일 없음")
        return 1

    ph_total = ph_hits = 0
    pm_total = pm_hits = 0
    hit10_sum = 0
    n = 0
    tag = f"[{label}] " if label else ""

    for T in days:
        ctx = load_prediction_eval_context(
            T, news_start=T - timedelta(days=45)
        )
        big = ctx.actual_big_movers()
        if big.empty:
            continue
        actual = {
            str(r["Code"]).zfill(6): float(r["return_pct"]) * 100.0
            for _, r in big.iterrows()
        }
        fb = feedback_loop.build_enriched_feedback_context(as_of=T)
        preds = ctx.rank_predictions(feedback_ctx=fb)
        if not preds:
            continue
        n += 1
        m = ctx.hit_at_k_metrics(preds)
        hit10 = int(m.get("hit_at_10", 0))
        hit10_sum += hit10

        day_ph = day_ph_hit = day_pm = day_pm_hit = 0
        for p in preds:
            code = str(p.code).zfill(6)
            act = actual.get(code)
            if act is None:
                continue
            if prediction_ranking.is_high_confidence_prediction(p):
                day_ph += 1
                ph_total += 1
                if act + 1e-9 >= config.BIG_MOVE_THRESHOLD * 100.0:
                    day_ph_hit += 1
                    ph_hits += 1
            elif prediction_ranking.is_mid_confidence_prediction(p):
                day_pm += 1
                pm_total += 1
                if act + 1e-9 >= config.BIG_MOVE_THRESHOLD * 100.0:
                    day_pm_hit += 1
                    pm_hits += 1

        print(
            f"{tag}{T.isoformat()} ph={day_ph_hit}/{day_ph} pm={day_pm_hit}/{day_pm} "
            f"hit10={hit10} tight={fb.get('adaptive_tightness')}",
            flush=True,
        )

    print(f"{tag}T={start}..{end} closed days={n}")
    print(
        f"{tag}  pred_high: {ph_hits}/{ph_total} hit20+ "
        f"({100*ph_hits/max(1,ph_total):.1f}%)"
    )
    print(
        f"{tag}  pred_mid:  {pm_hits}/{pm_total} hit20+ "
        f"({100*pm_hits/max(1,pm_total):.1f}%)"
    )
    print(f"{tag}  Hit@10 avg/day: {hit10_sum/max(1,n):.2f}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("start", type=_parse_day)
    p.add_argument("end", type=_parse_day)
    p.add_argument("--label", default="", help="출력 접두(예: balanced-B)")
    args = p.parse_args(argv)
    return eval_range(args.start, args.end, label=args.label)


if __name__ == "__main__":
    raise SystemExit(main())
