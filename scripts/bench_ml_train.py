"""
ML 랭커(v13) 학습 속도 벤치마크 — 관측일 T 1일분 ``fit_or_load_classifier`` 소요 시간 측정.

용도:
  - ``ML_TRAIN_LOOKBACK_DAYS``·피처 최적화 적용 후 재학습이 몇 분인지 빠르게 확인
  - ``move_ranker_v13_*`` joblib 캐시를 미리 생성해 ``main.py`` 구간 실행 시 대기 시간 점검

실행 (프로젝트 루트):
  set PYTHONPATH=<프로젝트 루트>
  python scripts/bench_ml_train.py

출력 예: ``elapsed_sec=380.4 version=13`` — 표본 수·저장 파일명은 ``ml_move_rank`` 로그 참고.
"""
from __future__ import annotations

import json
import time
from datetime import date, timedelta

import pandas as pd

from src import config, ml_move_rank, news, stocks, train_snapshot

# OHLCV·스냅샷·뉴스 캐시 로드 (main.py 와 동일 전제)
ohlcv = pd.read_parquet(config.CACHE_DIR / "ohlcv_long_full.parquet")
returns_ml = stocks.enrich_daily_returns_for_ml(stocks.daily_returns_table(ohlcv))
listing = stocks.load_listing()
names = {str(r["Code"]).zfill(6): str(r["Name"]) for _, r in listing.iterrows()}
events = train_snapshot.load_snapshot(config.TRAIN_SNAPSHOT_PATH).events

# 벤치에 쓸 뉴스 구간 — 필요 시 날짜 범위만 조정
news_by: dict[date, list[dict[str, str]]] = {}
d = date(2026, 5, 1)
while d <= date(2026, 6, 15):
    p = news.day_news_json_path(d)
    if p.is_file():
        news_by[d] = json.loads(p.read_text(encoding="utf-8"))
    d += timedelta(days=1)

# 워크포워드 컷오프 T (이 날짜 직전 거래일까지 라벨·이벤트로 학습)
T = date(2026, 6, 9)
fp = train_snapshot.fingerprint()
t0 = time.perf_counter()
bundle = ml_move_rank.fit_or_load_classifier(
    train_events=events,
    news_by_calendar=news_by,
    returns_ml=returns_ml,
    listing_names=names,
    fp=fp,
    label_before_exclusive=T,
    force_retrain=True,  # 캐시 무시·항상 재학습(벤치용)
)
elapsed = time.perf_counter() - t0
print(f"elapsed_sec={elapsed:.1f} version={bundle.get('version') if bundle else None}")
