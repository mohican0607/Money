"""최근 Hit@K·적중률 요약 + 개선 힌트 (오프라인, 캐시만 사용)."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config


def main() -> int:
    path = config.TRAIN_CACHE_DIR / "prediction_accuracy_track.json"
    if not path.is_file():
        print(f"캐시 없음: {path}")
        return 1
    d = json.loads(path.read_text(encoding="utf-8"))
    by_day = d.get("hit_at_k_by_day") or {}
    if not by_day:
        print("hit_at_k_by_day 비어 있음")
        return 1
    days = sorted(by_day)
    recent = days[-20:]
    print(f"Hit@K summary - recent {len(recent)} days ({recent[0]} ~ {recent[-1]})")
    for k in (5, 10, 20, 40):
        hits = [by_day[day].get(f"hit_at_{k}", 0) for day in recent]
        precs = [by_day[day].get(f"precision_at_{k}", 0.0) for day in recent]
        lifts = [by_day[day].get(f"lift_at_{k}", 0.0) for day in recent]
        br = [by_day[day].get("base_rate", 0.0) for day in recent]
        big = [by_day[day].get("actual_big_count", 0) for day in recent]
        print(
            f"  @{k:2d}: hit평균 {mean(hits):.2f}  "
            f"precision {mean(precs)*100:.2f}%  lift {mean(lifts):.2f}  "
            f"zero-hit일 {sum(1 for h in hits if not h)}/{len(recent)}"
        )
        print(
            f"       base_rate {mean(br)*100:.3f}%  "
            f"실제20%+ {mean(big):.1f}종/일"
        )
    last5 = recent[-5:]
    z5 = sum(1 for day in last5 if not by_day[day].get("hit_at_10"))
    if z5 >= 4:
        print(
            "\n[WARN] recent 5d Hit@10=0 on "
            f"{z5} days - run pool-miss / rank-hits:"
        )
        for day in last5:
            iso = day.replace("-", "")
            print(f"    python scripts/diag_prediction.py pool-miss {iso}")
            print(f"    python scripts/diag_prediction.py rank-hits {iso}")
    else:
        print("\nrecent Hit@10 ok - tune precision gate / pred_high")
    print(f"\n캐시: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
