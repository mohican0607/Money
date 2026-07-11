"""Round4 Hit@K + high precision eval on June sample days."""
from __future__ import annotations

from datetime import date

from src import config
from src.diagnostics.prediction import load_prediction_eval_context
from src.prediction import prediction_ranking

DAYS = [
    date(2026, 6, 10),
    date(2026, 6, 11),
    date(2026, 6, 16),
    date(2026, 6, 18),
]


def main() -> None:
    print(
        "cap",
        config.PRED_ML_SCORE_POOL_CAP,
        "enrich",
        config.PRED_ML_ENRICH_TOP_N,
        "freeze",
        config.PREDICTION_FREEZE_SCHEMA_VERSION,
    )
    sums = {5: 0, 10: 0, 20: 0, 40: 0}
    th = tm = tn = mn = 0
    for T in DAYS:
        ctx = load_prediction_eval_context(T, news_start=date(2026, 5, 1))
        preds = ctx.rank_predictions(top_n=max(config.PRED_RANK_POOL_N, 80))
        m = ctx.hit_at_k_metrics(preds)
        actual = {str(r["Code"]).zfill(6) for _, r in ctx.actual_big_movers().iterrows()}
        high = [p for p in preds if prediction_ranking.is_high_confidence_prediction(p)]
        mid = [p for p in preds if prediction_ranking.is_mid_confidence_prediction(p)]
        hh = sum(1 for p in high if p.code in actual)
        hm = sum(1 for p in mid if p.code in actual)
        th += hh
        tm += hm
        tn += len(high)
        mn += len(mid)
        for k in sums:
            sums[k] += int(m.get(f"hit_at_{k}", 0))
        print(
            f"{T}: act={len(actual)} "
            f"hit@5={m.get('hit_at_5')} @10={m.get('hit_at_10')} "
            f"@20={m.get('hit_at_20')} @40={m.get('hit_at_40')} "
            f"high={hh}/{len(high)} mid={hm}/{len(mid)}"
        )
        for p in high:
            hit = "HIT" if p.code in actual else "miss"
            print(
                f"  HIGH {p.code} ml={getattr(p, 'ml_proba', None)} "
                f"rs={getattr(p, 'ml_rank_score', None)} "
                f"rl={getattr(p, 'ret_lag1', None)} {hit}"
            )
    n = len(DAYS)
    print("AVG", {k: round(sums[k] / n, 2) for k in sums})
    print(
        f"HIGH {th}/{tn}={100 * th / max(1, tn):.1f}% "
        f"MID {tm}/{mn}={100 * tm / max(1, mn):.1f}%"
    )


if __name__ == "__main__":
    main()
