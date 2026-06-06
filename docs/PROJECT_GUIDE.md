# Money 프로젝트 가이드

KOSPI·KOSDAQ 상장 종목에 대해, **N거래일 장 마감 전(14:30 KST까지)** 수집·분류한 **뉴스(시장·테마·종목·국제정세 등)**, **전일 급등·테마 가중치**, **시세·KOSPI 흐름**, (보조) 과거 급등 종목 프로필·오판 피드백을 이용해 **다음 거래일(관측일 T) 수익률** 후보를 HTML 리포트로 출력하는 도구입니다.

예측은 **휴리스틱**과 **ML 랭커(HistGradientBoosting)** 를 함께 쓰며, `PRED_USE_ML_RANKER=0` 이면 휴리스틱만 사용합니다. 기본(**랭킹 모드**)에서는 상위 40개를 **ML 급등 확률 순**으로 정렬하고, **고확신/중확신**만 `pred_high`·`pred_mid`로 표시합니다(`PRED_RANKING_MODE=1`, `PRED_USE_DISPLAY_RANK_MAPPING=0`). 레거시 20~35% 일괄 매핑은 `PRED_USE_DISPLAY_RANK_MAPPING=1` 로 되돌릴 수 있습니다.

---

## 1. 디렉터리 구성

| 경로 | 역할 |
|------|------|
| `main.py` | CLI 진입점. 모드별 파이프라인(`_run_pipeline`) 호출, HTML 저장, (선택) 자동 열기 |
| `src/config.py` | 경로, 환경 변수, 훈련·테스트 날짜, 급등 임계값, ML·테마·피드백 플래그 |
| `src/stocks.py` | KRX 상장·OHLCV Parquet 캐시, 일간 수익률, pykrx 전종목 등락률, 장중 스냅샷 |
| `src/news.py` | 일자별 뉴스(네이버 API / Google RSS / mock), early·late 분류, 캐시 JSON |
| `src/trading_calendar.py` | XKRX 거래일 + `KRX_AD_HOC_SESSION_CLOSURES`(선거·제헌절 등 임시 휴장) |
| `src/features.py` | 토큰·키워드, `BreakoutEvent`(과거 급등–뉴스) 구축 |
| `src/predict.py` | 종목 스코어·예측 수익률·갭 설명 HTML·키워드 피드백 반영 |
| `src/prediction_ranking.py` | 랭킹 확정·확신 게이트·Hit@K·레짐 보수 조정 |
| `src/ml_move_rank.py` | 감독학습 랭커 학습·추론, KS11 시장 피처, miss 진단 가중 |
| `src/market_index.py` | KOSPI 등 지수 일봉·당일 수익률 |
| `src/report.py` | Jinja2 HTML(월간 탭·단일일·dated 누적 리포트) |
| `src/train_snapshot.py` | `breakout_train_snapshot.json` 로드·저장(`train_events`) |
| `src/snapshot_rebuild_learning.py` | `rebuild_learning`·`market_theme_flow`·`prediction_gap_rollup` 병합 |
| `src/snapshot_miss_diagnosis.py` | 미예측 급등·고예측 실패 원인 태그·한글 힌트 |
| `src/prediction_accuracy_cache.py` | 관측일별 예측–실적 비율·이력·키워드 피드백 JSON |
| `src/theme_carryover.py` | 전일 급등·뉴스 테마 가중치 JSON(`daily_theme_snapshots.json`) |
| `src/disclosure.py` | 네이버 종목 공시 hit(리포트 툴팁) |
| `scripts/` | OHLCV·뉴스 백필·일일 스케줄·더미 리포트 등 보조 |
| `data/cache/` | OHLCV, 뉴스, 학습·예측 캐시 |
| `output/` | `report_*.html` |

각 Python 모듈의 **함수 docstring**은 소스 파일에 한국어로 정리되어 있습니다.

---

## 2. 데이터 흐름

### 2.1 한 줄 요약

```
상장 + OHLCV(캐시) → 일간 수익률
        ↓
캘린더 일자별 뉴스(JSON) + OHLCV → BreakoutEvent 풀(과거 급등 라벨·보조 프로필)
        ↓
관측일 T: N−1~N **14:30까지** early 뉴스·테마·시세·시장 → ML/휴리스틱 → 상위 40후보
        (학습 라벨·이벤트는 **T 직전**만 사용 — 워크포워드)
        ↓
실제 10%+·20%+: pykrx(우선) 또는 OHLCV → rows_compare
        ↓
prediction_accuracy_track · theme · rebuild_learning(구간 실행 시) 병합
        ↓
DayReport → HTML
```

### 2.2 기준일 N과 관측일 T

- `python main.py YYYYMMDD` 에서 **YYYYMMDD = 관측일 T**, **N = T 직전 거래일**.
- 시나리오: **N일 장 마감 전** 주문 → **T일 급등** 목표.
- 예측 뉴스: **N−1 거래일 14:30(KST)까지**(early). `USE_DECISION_NEWS_INTRADAY_CUTOFF=1` 기본.

### 2.3 학습 라벨 vs 관측(리포트)

| 구분 | 규칙 | 용도 |
|------|------|------|
| **워크포워드 학습** | 각 관측일 `T`마다 `TRAIN_START_DEFAULT` ≤ 급등일 &lt; `T` 인 `BreakoutEvent`·ML 라벨만 사용 | 누수 방지 |
| **스냅샷 저장** | `TRAIN_START_DEFAULT` ~ 파이프라인 `end_date` 급등 이벤트 전부 | 증분 병합·재사용 |
| **기본 관측 시작** | `TEST_START` (`2026-01-01`) | `python main.py --weekly` 등 **인자 없을 때** 리포트 시작일 |
| **구간 실행** | CLI `From To` 거래일 | 위 `TEST_START`와 무관하게 해당 일만 예측·리포트 |

`TEST_START`는 **학습 상한이 아닙니다.** `20260501 20260604`를 실행하면 2026년 5~6월 급등도 **T 이전**이면 학습·예측에 반영됩니다(스냅샷 지문 변경 후 `--rebuild-train-snapshot` 권장).

---

## 3. 실행 방법

프로젝트 루트에서:

```bash
python main.py
python main.py YYYYMMDD
python main.py YYYYMMDD YYYYMMDD
python main.py --weekly
```

### 3.1 CLI 모드

| 명령 | 동작 |
|------|------|
| `python main.py` | N=오늘(거래일), T=다음 거래일. dated rollup HTML |
| `python main.py YYYYMMDD` | 지정 T, N 자동. 과거 T면 실제·예측 비교 |
| `python main.py From To` | 구간 거래일 배치 → `report_YYYY.MM.html` + 목차 |
| `python main.py --weekly` | `REPORT_TEST_DAY_START`~`END` 월간 배치 |

### 3.2 학습 스냅샷·예측 고정 플래그

| 플래그 | 동작 |
|--------|------|
| *(기본)* | `breakout_train_snapshot.json` 재사용, 미반영 캘린더만 병합 |
| `--no-train-snapshot` | 스냅샷 없이 매번 `train_events` 전체 재계산 |
| `--rebuild-train-snapshot` | `train_events` **전체 재계산** + ML **재학습** + **From~To 안** 예측·freeze·`rebuild_learning` 갱신 |
| `--append-rebuild-learning` | `train_events`·ML **재사용**, **From~To 안**만 예측·freeze·`rebuild_learning` (빠른 갭 채우기) |
| `--no-report-expand` | 구간 실행 시 기존 월간 HTML 날짜 **자동 추가 안 함** (인자 일수만) |

**예측 고정 캐시 (`prediction_freeze_by_t.json`) — `python main.py From To` 구간 실행 시**

| 관측일 | freeze 동작 |
|--------|-------------|
| **CLI From ~ To 안** | **항상 무시** → 매번 재예측 후 freeze 저장 (플래그 유무 동일) |
| 구간 밖 (리포트 병합일 등) | 캐시가 있으면 **재사용** |
| 단일일 / `--weekly` | 캐시가 있으면 **재사용** (기본 동작) |

**갭 채우기 예 (10거래일, ML 반영, 리포트 병합 없음):**

```bash
python main.py --append-rebuild-learning --no-report-expand 20260516 20260601
```

**전체 초기화·ML 재학습:**

```bash
python main.py --rebuild-train-snapshot 20260401 20260601
```

### 3.3 보조 스크립트

| 스크립트 | 역할 |
|----------|------|
| `scripts/build_ohlcv_full_cache.py` | 전종목 OHLCV Parquet만 생성 |
| `scripts/backfill_news.py` | `day_*.json` 뉴스 백필 |
| `scripts/migrate_news_cache_to_monthly.py` | 구 캐시 → `news/<provider>/YYYY/` 이전 |
| `scripts/fetch_news_naver_day.py` | 특정일 네이버 뉴스 수집 |
| `scripts/check_trading_day_for_daily.py` | 일일 스케줄용 거래일 확인(종료 코드) |
| `scripts/render_report_dummy.py` | 리포트 UI 더미 렌더 |

---

## 4. 학습·피드백 캐시 (`data/cache/train/`)

| 파일 | 내용 | 갱신 시점 |
|------|------|-----------|
| `breakout_train_snapshot.json` | `train_events`, `rebuild_learning`, `market_theme_flow`, `prediction_gap_rollup` | `--rebuild` / `--append` 말미 병합. `market_theme_flow` 는 리포트 「당일 상승·테마와 early 뉴스 상관」에도 표시(매 구간 실행) |
| `prediction_accuracy_track.json` | `t_code_ratio`, 종목별 예측 이력, `keyword_feedback_weights` | 매 구간 실행 말미 |
| `prediction_freeze_by_t.json` | 관측일 T별 상위 후보·예측% 고정 | **From~To 구간** 실행마다 해당 T 재계산·저장 |
| `daily_theme_snapshots.json` | 전일 급등·테마 가중치 | 매 거래일 파이프라인 |
| `move_ranker_v*.joblib` | ML 랭커 모델 | `--rebuild-train-snapshot` 시 재학습 |

### 4.1 `rebuild_learning` (사후 처리·학습 진단)

`--rebuild-train-snapshot` 또는 `--append-rebuild-learning` + From~To 실행 **말미**에 병합됩니다.

일별(`daily[]`) 항목 예:

- `missed_big_movers_today` — 20%↑였으나 예측 후보에 없던 종목 + 원인 태그
- `pred_high_misses_today` — 고예측(≥20%)였으나 실적 부진 + 진단
- 괴리 통계(`mean_abs_gap_*`), `pred_high_precision_*`, false positive 키워드 누적

ML 재학습 시 `snapshot_miss_diagnosis` 가 이 진단을 읽어 **어려운 급등·오판 샘플 가중**을 적용합니다.

### 4.2 예측·학습 입력 vs `market_theme_flow` (리포트)

| 구분 | 시점·내용 | 학습/예측 반영 |
|------|-----------|----------------|
| **early 뉴스** | 관측일 `T` 직전 거래일 **14:30(KST)** 까지 (`USE_DECISION_NEWS_INTRADAY_CUTOFF`) | `train_events.news_keywords`, 휴리스틱·ML 랭커 피처 |
| **테마 캐리오버** | `T` 직전일 급등·뉴스 → `daily_theme_snapshots.json` | `theme_carryover` 점수·ML `theme_kw_overlap` |
| **`market_theme_flow`** | 당일 10%+ 상승 종목 vs early 뉴스 키워드·테마 시드 교집합 | 스냅샷 JSON·**리포트 표** (ML 피처로 직접 넣지 않음) |
| **`rebuild_learning`** | 예측 괴리·미적중 진단 | ML 샘플 가중(진단 키 복제) |

리포트 각 거래일 상단 **「당일 상승·테마와 예측 입력 뉴스(early) 상관」** 과 종목 **통합 보기 → 상·하락 참고** 의 「테마·early 뉴스 상관」은 위 early blob과 동일 규칙입니다.

---

## 5. 뉴스 수집

캐시: `data/cache/news/{naver|google|mock}/YYYY/day_YYYYMMDD.json`

| `NEWS_NAVER_QUERY_MODE` | 설명 |
|-------------------------|------|
| `market` | 시장 키워드 쿼리(기본) |
| `ticker` | 종목명+일자 쿼리 후 pubDate 필터 |
| `both` | 시장+종목 병합(권장, 네이버 키 필요) |

네이버 API에는 날짜 From/To가 없어 **캘린더 일마다** API를 호출합니다.

---

## 6. 환경 변수 (.env) 요약

| 변수 | 의미 |
|------|------|
| `NAVER_CLIENT_ID` / `SECRET` | 네이버 뉴스 API |
| `MOCK_NEWS` | API 없이 모의 뉴스 |
| `USE_GOOGLE_NEWS_RSS_FALLBACK` | 네이버 없을 때 RSS(기본 1) |
| `NEWS_NAVER_QUERY_MODE` | `market` / `ticker` / `both` |
| `NEWS_FETCH_MAX_WORKERS` | 일자 병렬 수(429 시 1~2) |
| `OHLCV_MAX_WORKERS` | OHLCV 다운로드 워커(기본 12) |
| `USE_KRX_OHLCV` | KRX 일봉 우선 |
| `SAMPLE_TICKERS` | 디버그용 상위 N종만 |
| `USE_DECISION_NEWS_INTRADAY_CUTOFF` | N−1 14:30 KST early/late(기본 1) |
| `PRED_USE_ML_RANKER` | ML 랭커 사용(기본 1) |
| `PREDICTION_FREEZE_ENABLED` | T별 예측 고정 캐시(기본 1) |
| `THEME_CARRYOVER_ENABLED` | 전일 테마 가중치(기본 1) |
| `KEYWORD_FEEDBACK_*` | 오판 키워드 온라인 가중치 |
| `PRED_RETURN_MIN` / `MAX` | 표시 예측% 하한·상한(기본 0.20~0.35) |
| `NO_AUTO_OPEN_OUTPUT` | 실행 후 HTML 자동 열기 끔 |
| `REPORT_SKIP_DISCLOSURE_FETCH` | 1이면 공시 HTTP 조회 생략(캐시만) |
| `--use-freeze` | 구간 실행 시 예측 고정 캐시 재사용(재예측 생략) |
| `--no-report-expand` | 구간만 처리·기존 월간 HTML 날짜 병합 안 함 |

코드 상수: `BIG_MOVE_THRESHOLD=0.20`, `TRAIN_START_DEFAULT`, `TEST_START`, `KRX_AD_HOC_SESSION_CLOSURES`(임시 증시 휴장) → `src/config.py`.

---

## 7. 캐시·산출물

- **OHLCV**: `data/cache/ohlcv_long_full.parquet`
- **상장**: `data/cache/krx_listing.parquet`
- **뉴스**: `data/cache/news/.../day_YYYYMMDD.json`
- **리포트**: `output/report_YYYY.MM.html`, `report_index_monthly.html`, `report_dated_by_n.html`

---

## 8. 모듈별 주요 함수

### `main.py`

| 함수 | 역할 |
|------|------|
| `_parse_cli` | 모드·날짜·스냅샷 플래그 파싱 |
| `_run_pipeline` | OHLCV·뉴스·학습·일별 예측·캐시 병합 |
| `_build_rebuild_learning_payload` | 구간 `DayReport` → `rebuild_learning` dict |
| `_render_monthly_batch` | 월별 HTML·목차 |
| `_load/_save_prediction_freeze_payload` | 예측 고정 JSON |

### `src/predict.py`

| 함수 | 역할 |
|------|------|
| `predict_for_trading_day` | 전 종목 스코어 → 상위 N, ML 또는 휴리스틱 |
| `prediction_row_for_code` | 단일 종목 점수·키워드·피드백 |
| `apply_display_return_pct_ranking` | 순위 → 20~35% 표시 매핑 |
| `explain_return_gap_html` | 예측 vs 실제 갭 HTML |

### `src/ml_move_rank.py`

| 함수 | 역할 |
|------|------|
| `fit_or_load_classifier` | 학습 또는 joblib 로드 |
| `rank_predictions_ml` | 급등 확률 순 상위 후보 |
| `_ks11_market_feats` | KOSPI 전일 수익·변동성 피처 |
| `_feat_vector` | 종목별 ML 입력 벡터 |

### `src/prediction_accuracy_cache.py`

| 함수 | 역할 |
|------|------|
| `merge_from_day_reports` | `t_code_ratio`·실적% 병합 |
| `merge_keyword_feedback_from_day_reports` | FP/FN 키워드 가중치 |
| `enrich_rows_pred_high_history` | 리포트 행에 누적 이력 |
| `export_gap_rollup_for_calendar_range` | `prediction_gap_rollup` 스냅샷 |

### `src/features.py`

| 함수 | 역할 |
|------|------|
| `build_breakout_events` | 훈련 구간 급등–뉴스 이벤트 |
| `keyword_set` / `top_keywords` | 뉴스 텍스트 토큰 |

### `src/news.py`

| 함수 | 역할 |
|------|------|
| `fetch_news_for_calendar_day` | 하루치 뉴스 API/RSS |
| `aggregate_early_late_for_target` | early/late blob 분리 |
| `news_fetch_calendar_span` | 구간 캘린더 일 목록 |

### `src/stocks.py`

| 함수 | 역할 |
|------|------|
| `build_ohlcv_long` | Parquet 캐시·증분 다운로드 |
| `daily_returns_table` | Code×Date 수익률 |
| `try_krx_change_pct_by_code` | pykrx 당일 등락률 맵 |
| `big_movers_from_krx_pct_map` | 급등·급락 종목 목록 |

### `src/snapshot_rebuild_learning.py`

| 함수 | 역할 |
|------|------|
| `build_market_theme_flow` | 일별 시장·테마 요약 |
| `merge_rebuild_learning_dict` | `daily` 병합·누적 재계산 |
| `merge_extended_rebuild_into_snapshot` | JSON 파일 저장 |

---

## 9. 실행 시간·성능

| 단계 | 대략 |
|------|------|
| OHLCV·뉴스 (캐시 히트) | 수 분 |
| `train_events` 전체 (`--rebuild`) | 수십 분 |
| **거래일 1일 예측** (`--rebuild`/`--append`) | **~35~40분** (전종목 재점수) |
| 사후 처리 (JSON 병합·HTML) | 수 분~30분 |

**한 달(~22거래일) `--append`**: 예측만 **~12~15시간**. `--no-report-expand`로 불필요한 일수 제외 권장.

---

## 10. 코드 읽는 순서(권장)

1. `main.py` — `_parse_cli` → `main` → `_run_pipeline`
2. `src/config.py`
3. `src/stocks.py` → `src/news.py`
4. `src/features.py` → `src/predict.py` → `src/ml_move_rank.py`
5. `src/prediction_accuracy_cache.py` → `src/snapshot_rebuild_learning.py`
6. `src/report.py`

---

## 11. 의존성

`requirements.txt`: `pandas`, `FinanceDataReader`, `exchange_calendars`, `requests`, `python-dotenv`, `tqdm`, `jinja2`, `scikit-learn`, `joblib` 등. **pykrx** 권장(전종목 등락률).

---

## 12. 랭킹 모드(구조적 정확도 개선)

| 항목 | 내용 |
|------|------|
| **문제 정의** | 절대 20% 회귀 대신 **상위 K 랭킹 + ML 확률(P)** · 확신 구간(`high`/`mid`) |
| **평가** | 일별 **Hit@5/10/20/40**·lift·recall → `prediction_accuracy_track.json` `hit_at_k_by_day`·리포트 패널 |
| **피처** | ML v6: `ret_roll_mean5`, `vol_surge_ratio`, `ret_vs_ks11_lag1`, `ks11_regime_risk_off` |
| **레짐 게이트** | KOSPI 전일 수익률 &lt; `PRED_REGIME_KS11_SOFT_MIN` 이면 고확신 출력 수 축소 |
| **ML 재학습** | 피처 변경 후 `python main.py --rebuild-train-snapshot From To` 권장 |

주요 환경 변수: `PRED_ML_HIGH_CONFIDENCE_PROB`, `PRED_OUTPUT_MAX`, `PRED_RANK_POOL_N`, `PRED_EVAL_HIT_AT_K`.

---

## 13. 한계·주의

- 투자 권유가 아닌 **패턴 탐색·리포트**용입니다.
- 뉴스·공시 검색 결과에 의존하며, 수급·재무 등 비뉴스 요인은 제한적으로만 반영됩니다.
- 표시 예측%는 **확률→표시 변환** 또는(레거시) 순위 매핑 결과이며, 투자 수익률 보장이 아닙니다.
- `rebuild_learning` 은 `--rebuild` / `--append` + **From~To** 실행이 **정상 완료**되어야 해당 구간에 쌓입니다.
