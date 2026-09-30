# -*- coding: utf-8 -*-
"""signal_study 에 공시 신호를 더했을 때 개선되는지 확인."""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding="utf-8")
import scripts.signal_study as ss  # noqa: E402

KINDS = {
    "d_contract": r"공급계약|수주|판매계약",
    "d_bonus": r"무상증자",
    "d_rights": r"유상증자",
    "d_buyback": r"자기주식|자사주",
    "d_control": r"최대주주|경영권|주식양수도",
    "d_bio": r"임상|품목허가|허가신청|FDA",
    "d_mna": r"합병|인수|분할",
    "d_cb": r"전환사채|신주인수권",
    "d_patent": r"특허",
}

feats = pd.read_parquet(ss.OUT)
days = sorted(feats["Date"].unique())
next_day = {}
j = 0
cal = pd.date_range(days[0] - pd.Timedelta(days=10), days[-1])
for d in cal:
    while j < len(days) and days[j] <= d:
        j += 1
    if j < len(days):
        next_day[d.normalize()] = days[j]

agg: dict[tuple[str, pd.Timestamp], dict[str, int]] = defaultdict(lambda: defaultdict(int))
for f in sorted((ROOT / "data" / "cache" / "disclosure" / "naver").rglob("day_*.json")):
    try:
        rows = json.loads(f.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        continue
    for r in rows or []:
        code = str(r.get("code") or "").zfill(6)
        try:
            d = pd.Timestamp(str(r.get("day"))[:10]).normalize()
        except ValueError:
            continue
        T = next_day.get(d)
        if T is None:
            continue
        a = agg[(code, T)]
        a["disc_n"] += 1
        title = str(r.get("title") or "")
        for k, pat in KINDS.items():
            if re.search(pat, title):
                a[k] += 1
cols = ["disc_n", *KINDS]
dd = pd.DataFrame(
    [{"Code": c, "Date": T, **{k: v.get(k, 0) for k in cols}} for (c, T), v in agg.items()]
)
print("disclosure rows", len(dd), "date range", dd["Date"].min(), dd["Date"].max())
feats = feats.merge(dd, on=["Code", "Date"], how="left")
feats[cols] = feats[cols].fillna(0)
cover = feats[feats["Date"] >= dd["Date"].min()]
print("공시 있는 (종목,일) 비율", f"{(cover['disc_n'] > 0).mean():.2%}")
for k in cols:
    m = cover[cover[k] > 0]
    if len(m):
        print(f"  {k:11s} n={len(m):6d}  익일 20%↑ 비율 {m['big'].mean():.2%}  시가→종가 +10% 비율 {m['oc10'].mean():.2%}")
print(f"  (전체 기준) 익일 20%↑ {cover['big'].mean():.2%}  +10% {cover['oc10'].mean():.2%}")

ss.FAMILIES = {
    "가격+테마": ss.FAMILIES["가격+테마"],
    "공시": cols,
    "가격+테마+공시": ss.FAMILIES["가격+테마"] + cols,
}
ss.run(feats, "big")
ss.run(feats, "oc10", skip_gap_over=0.10)
