#!/usr/bin/env python
"""예측·Hit@K·후보 풀 오프라인 진단 (통합 CLI)."""
from __future__ import annotations

import argparse
import calendar
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pandas as pd

from src import config, news, stocks, trading_calendar
from src.diagnostics.prediction import load_prediction_eval_context, load_news_by_calendar
from src.prediction import ml_move_rank, prediction_ranking


def _parse_day(s: str) -> date:
    s = s.strip().replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise argparse.ArgumentTypeError(f"YYYYMMDD 필요: {s!r}")
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def cmd_recall(args: argparse.Namespace) -> int:
    ctx = load_prediction_eval_context(args.day)
    cands = set(ctx.candidate_codes())
    big = ctx.actual_big_movers()
    in_pool = sum(
        1 for _, r in big.iterrows() if str(r["Code"]).zfill(6) in cands
    )
    print(
        f"T={ctx.target_day} candidates={len(cands)} "
        f"actual20+={len(big)} in_pool={in_pool}/{len(big)}"
    )
    preds = ctx.rank_predictions(top_n=args.top_n)
    metrics = ctx.hit_at_k_metrics(preds)
    print(
        "Hit@K",
        {k: metrics[k] for k in metrics if str(k).startswith("hit_at")},
    )
    if args.show_top:
        sl = ctx.returns_ml[ctx.returns_ml["Date"] == pd.Timestamp(ctx.target_day)]
        actual = {
            str(r["Code"]).zfill(6)
            for _, r in big.iterrows()
        }
        print(f"Top{args.show_top}:")
        for i, p in enumerate(preds[: args.show_top], 1):
            c6 = str(p.code).zfill(6)
            sub = sl[sl["Code"].astype(str).str.zfill(6) == c6]
            act = (
                float(sub.iloc[0]["return_pct"]) * 100
                if not sub.empty and c6 in actual
                else None
            )
            ind = (stocks.industry_name_for_code(c6) or "?")[:8]
            nm = (ctx.listing_names.get(c6, "") or "")[:10]
            hit = "HIT" if c6 in actual else "miss"
            if act is not None:
                print(
                    f"  #{i} {c6} {nm:10s} {ind:8s} "
                    f"tier={p.confidence_tier} act={act:.1f}% {hit}"
                )
            else:
                print(
                    f"  #{i} {c6} {nm:10s} {ind:8s} tier={p.confidence_tier}"
                )
    return 0


def cmd_pool_miss(args: argparse.Namespace) -> int:
    ctx = load_prediction_eval_context(args.day)
    cands = set(ctx.candidate_codes())
    big = ctx.actual_big_movers().copy()
    big["code"] = big["Code"].astype(str).str.zfill(6)
    miss = big[~big["code"].isin(cands)].sort_values("return_pct", ascending=False)
    hit = big[big["code"].isin(cands)]
    print(
        f"candidates={len(cands)} hit={len(hit)}/{len(big)} miss={len(miss)}"
    )
    print(f"\n--- MISSED (top {args.limit}) ---")
    for _, r in miss.head(args.limit).iterrows():
        c = r["code"]
        nm = (ctx.listing_names.get(c, "") or "")[:8]
        ind = (stocks.industry_name_for_code(c) or "?")[:10]
        rl = float(r.get("ret_lag1") or 0)
        vs = float(r.get("vol_surge_ratio") or 0)
        mom = prediction_ranking.momentum_raw_from_row(r)
        print(
            f"  {c} {nm:8s} {float(r['return_pct'])*100:5.1f}% "
            f"ind={ind:10s} lag1={rl*100:5.1f}% vs={vs:.2f} mom={mom:.2f}"
        )
    return 0


def cmd_rank_hits(args: argparse.Namespace) -> int:
    ctx = load_prediction_eval_context(args.day)
    preds = ctx.rank_predictions(top_n=args.top_n)
    rank_map = {str(p.code).zfill(6): i for i, p in enumerate(preds, 1)}
    big = ctx.actual_big_movers().copy()
    big["code"] = big["Code"].astype(str).str.zfill(6)
    cands = set(ctx.candidate_codes())
    print("IN POOL — rank of actual 20%+ movers:")
    for _, r in big.sort_values("return_pct", ascending=False).iterrows():
        c = r["code"]
        if c not in rank_map:
            continue
        nm = (ctx.listing_names.get(c, "") or "")[:8]
        tier = next(
            p.confidence_tier for p in preds if str(p.code).zfill(6) == c
        )
        print(
            f"  rank #{rank_map[c]:2d} {c} {nm:8s} "
            f"act={float(r['return_pct'])*100:5.1f}% tier={tier}"
        )
    print("\nIN POOL but NOT in top40:")
    for _, r in big.sort_values("return_pct", ascending=False).iterrows():
        c = r["code"]
        if c in cands and c not in rank_map:
            nm = (ctx.listing_names.get(c, "") or "")[:8]
            print(
                f"  MISS top{args.top_n} {c} {nm} "
                f"act={float(r['return_pct'])*100:.1f}%"
            )
    return 0


def cmd_backtest_jun(args: argparse.Namespace) -> int:
    from src.learning import support as theme_support

    year = args.year
    month = args.month
    last_dom = calendar.monthrange(year, month)[1]
    days = [
        d
        for d in (date(year, month, day) for day in range(1, last_dom + 1))
        if trading_calendar.is_trading_day(d)
    ]
    if not days:
        print("거래일 없음")
        return 1

    ctx0 = load_prediction_eval_context(
        days[0],
        news_start=date(year, month - 1 if month > 1 else 12, 1),
    )
    returns_ml = ctx0.returns_ml
    codes = ctx0.listing_codes
    names = ctx0.listing_names
    events = ctx0.train_events
    fp = ctx0.fp
    news_by = load_news_by_calendar(
        date(year, month - 1 if month > 1 else 12, 1),
        date(year, month, 28) if month < 12 else date(year, 12, 31),
    )

    ks = [5, 10, 20, 40]
    sums = {k: 0 for k in ks}
    n_days = 0
    per_day: list[tuple[str, dict[int, int]]] = []

    for T in days:
        prev = trading_calendar.last_trading_day_before(T)
        blob, _ = news.aggregate_early_late_for_target(news_by, prev)
        bundle = ml_move_rank.fit_or_load_classifier(
            train_events=events,
            returns_ml=returns_ml,
            news_by_calendar=news_by,
            listing_names=names,
            fp=fp,
            label_before_exclusive=T,
            force_retrain=False,
        )
        tw = theme_support.weights_for_observation_day(
            T,
            train_events=events,
            news_by_calendar=news_by,
            returns_df=returns_ml,
            threshold=config.BIG_MOVE_THRESHOLD,
        )
        top_n = max(config.PRED_RANK_POOL_N, max(config.PRED_EVAL_HIT_AT_K))
        preds = ml_move_rank.rank_predictions_ml(
            target_day=T,
            listing_codes=codes,
            listing_names=names,
            train_events=events,
            news_text_blob=blob,
            returns_ml=returns_ml,
            pipeline=(bundle or {}).get("pipeline"),
            theme_weights=tw,
            ml_bundle=bundle,
            top_n=top_n,
        )
        sl = returns_ml[returns_ml["Date"] == pd.Timestamp(T)]
        actual = {
            str(r["Code"]).zfill(6)
            for _, r in sl.iterrows()
            if float(r.get("return_pct") or 0) >= config.BIG_MOVE_THRESHOLD
        }
        if not actual:
            continue
        ranked = [str(p.code).zfill(6) for p in preds]
        m = prediction_ranking.compute_hit_at_k_metrics(
            ranked, actual, universe_size=len(codes)
        )
        n_days += 1
        line = {k: int(m.get(f"hit_at_{k}", 0)) for k in ks}
        per_day.append((T.isoformat(), line))
        for k in ks:
            sums[k] += line[k]

    print(f"ML v{ml_move_rank.ML_MODEL_VERSION} | days={n_days}")
    for k in ks:
        print(
            f"  Hit@{k} avg {sums[k] / max(1, n_days):.2f}/day  total {sums[k]}"
        )
    print("worst @10:")
    for iso, line in sorted(per_day, key=lambda x: x[1][10])[:5]:
        print(f"  {iso} hit@10={line[10]}")
    print("best @10:")
    for iso, line in sorted(per_day, key=lambda x: -x[1][10])[:5]:
        print(f"  {iso} hit@10={line[10]}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="예측·Hit@K 오프라인 진단")
    sub = p.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("recall", help="후보 풀·Hit@K·Top-N")
    pr.add_argument("day", type=_parse_day)
    pr.add_argument("--top-n", type=int, default=40)
    pr.add_argument("--show-top", type=int, default=10)
    pr.set_defaults(func=cmd_recall)

    pm = sub.add_parser("pool-miss", help="풀 밖 급등주 분석")
    pm.add_argument("day", type=_parse_day)
    pm.add_argument("--limit", type=int, default=20)
    pm.set_defaults(func=cmd_pool_miss)

    rh = sub.add_parser("rank-hits", help="풀 내 급등주 순위 분포")
    rh.add_argument("day", type=_parse_day)
    rh.add_argument("--top-n", type=int, default=40)
    rh.set_defaults(func=cmd_rank_hits)

    bt = sub.add_parser("backtest-month", help="월간 Hit@K 백테스트")
    bt.add_argument("--year", type=int, default=2026)
    bt.add_argument("--month", type=int, default=6)
    bt.set_defaults(func=cmd_backtest_jun)

    args = p.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
