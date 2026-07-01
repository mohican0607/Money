# Money 프로젝트 가이드

KOSPI·KOSDAQ 상장 종목에 대해, **N거래일 장 마감 전(14:30 KST까지)** 수집·분류한 **뉴스(시장·테마·종목·국제정세 등)**, **전일 급등·테마 가중치**, **시세·KOSPI 흐름**, (보조) 과거 급등 종목 프로필·오판 피드백을 이용해 **다음 거래일(관측일 T) 수익률** 후보를 HTML 리포트로 출력하는 도구입니다.

예측은 **휴리스틱**과 **ML 랭커(HistGradientBoosting)** 를 함께 쓰며, `PRED_USE_ML_RANKER=0` 이면 휴리스틱만 사용합니다. 기본(**랭킹 모드**)에서는 후보 풀을 **키워드·시세 모멘텀·전체뉴스 TF-IDF 맥락**(`news_context_ml`)의 합집합으로 넓힌 뒤 **하이브리드 점수**(`pred_hybrid`: ML + 모멘텀 + 뉴스맥락)로 정렬하고, **고확신/중확신**만 `pred_high`·`pred_mid`로 표시합니다(`PRED_RANKING_MODE=1`, `PRED_USE_DISPLAY_RANK_MAPPING=0`). **예측 상승률(%)** 은 종목별 과거 급등 평균·키워드 보정값이며, ML 확률은 순위·확신만 반영합니다(확률을 %로 바꿔 붙이지 않음). 레거시 순위→20~30% 일괄 매핑은 `PRED_USE_DISPLAY_RANK_MAPPING=1` 로 되돌릴 수 있습니다.

**현재 ML·freeze 버전:** `ML_MODEL_VERSION=20`, `PREDICTION_FREEZE_SCHEMA_VERSION=25`

**14:30 실행 → 30분 내 완료 목표:** ML 풀 cap 260·enrich 130·업종 피처 캐시·병렬 피처(12 workers). 캐시 히트 시 **ML 추론 ~1분**. 캐시 미스 경량 학습 **~5분**.

---

## 1. 디렉터리 구성

| 경로 | 역할 |
|------|------|
| `main.py` | **CLI 진입점만** (~330줄). 모드 분기 후 `src.pipeline` 호출 |
| `src/__init__.py` | 패키지 루트. `from src import config, stocks, predict` 등 **lazy 호환** |
| `src/config.py` | 경로·환경 변수·상수 (프로젝트 ``ROOT`` = repo 루트) |
| `src/trading_calendar.py` | XKRX 거래일·뉴스 윈도우 |
| `src/features.py` | 토큰·키워드·``BreakoutEvent`` |
| `src/snapshot_rebuild_learning.py` | 진단 스크립트용 shim → ``learning.market_theme`` |
| `src/stocks.py` | OHLCV·지수·업종·수급(``investor_flow`` API 포함) |
| `src/news.py` | 뉴스 수집·캐시·공시 |
| `src/investor_flow.py` | 수급 shim → ``stocks`` |
| `src/data/` | **호환 shim** (구 ``src.data.*``) |
| `src/prediction/` | `predict` · `prediction_ranking`(하이브리드·정밀게이트) · `ml_move_rank`(학습·추론) · `candidate_pool`(후보 풀) · `news_context`(TF-IDF) · `market_features`(시세·KS11) · `accuracy_cache` |
| `src/diagnostics/` | `prediction`(Hit@K·후보 풀 오프라인 진단 공통 로더) |
| `src/pipeline/` | `run` · `batch` · `cli` · `rows`(compare+enrich) · `support`(뉴스·freeze·스케줄) · `types` |
| `src/learning/` | `market_theme` · `support`(스냅샷·테마이월·미스진단) |
| `src/report/` | `render`(타입·렌더) · `templates` · `content`(급등근거·테마리포트) |
| `scripts/` | OHLCV·뉴스 백필·일일 스케줄·더미 리포트 등 보조 |
| `data/cache/` | OHLCV, 뉴스, 학습·예측 캐시 |
| `output/` | `report_*.html` |

**루트 `src/*.py`는 8개** — 설정·달력·피처·시세·뉴스·수급 + ``__init__`` + 진단 shim.

각 Python 모듈의 **공개·내부 함수 docstring**은 소스에 한국어로 정리되어 있습니다.

**문서 위치:** 모듈 상단 docstring → 함수 docstring → 본 가이드 §8 함수 표.

---

## 2. 데이터 흐름

### 2.1 한 줄 요약

```
상장 + OHLCV(캐시) → 일간 수익률
        ↓
캘린더 일자별 뉴스(JSON) + OHLCV → BreakoutEvent 풀(과거 급등 라벨·보조 프로필)
        ↓
관측일 T: N−1~N **14:30까지** early 뉴스·테마·시세·시장
        → 후보 = 키워드/종목명 ∪ **모멘텀** ∪ **수급** ∪ **뉴스맥락(TF-IDF)** ∪ **전일급등·업종 peer** (`candidate_pool.day_candidate_codes`)
        → ML 확률 → **하이브리드 순위** → finalize(섹터 다양화·캐리오버 재랭킹) → 상위 N·고/중 확신
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
| `--append-rebuild-learning` | ML joblib **재사용**, **마지막 `train_events` 이후~`end_date` 급등 이벤트 증분 병합**, From~To 예측·freeze·`rebuild_learning` |
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

```bash
python scripts/diag_prediction.py recall 20260629
python scripts/diag_prediction.py pool-miss 20260629
python scripts/diag_prediction.py backtest-month --year 2026 --month 6
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
| `scripts/bench_ml_train.py` | 관측일 T 1일분 ML 재학습 소요 시간 벤치 |
| `scripts/diag_prediction.py` | **통합** 예측 진단 CLI — `recall` · `pool-miss` · `rank-hits` · `backtest-month` |
| `scripts/diag_pred_accuracy_jun.py` | 6월 HTML 고확신·중확신 20% 적중률·Hit@K 요약 |
| `scripts/backtest_hit_jun.py` | (shim) → `diag_prediction.py backtest-month` |
| `scripts/diag_recall_0629.py` 등 | (shim) → `diag_prediction.py recall YYYYMMDD` |

---

## 4. 학습·피드백 캐시 (`data/cache/train/`)

| 파일 | 내용 | 갱신 시점 |
|------|------|-----------|
| `breakout_train_snapshot.json` | `train_events`, `rebuild_learning`, `market_theme_flow`, `prediction_gap_rollup` | `--rebuild` / `--append` 말미 병합. `market_theme_flow` 는 리포트 「당일 상승·테마와 early 뉴스 상관」에도 표시(매 구간 실행) |
| `prediction_accuracy_track.json` | `t_code_ratio`, 종목별 예측 이력, `keyword_feedback_weights` | 매 구간 실행 말미 |
| `prediction_freeze_by_t.json` | 관측일 T별 상위 후보·예측% 고정 | **From~To 구간** 실행마다 해당 T 재계산·저장 |
| `daily_theme_snapshots.json` | 전일 급등·테마 가중치 | 매 거래일 파이프라인 |
| `move_ranker_v*.joblib` | ML 랭커 모델 + `news_ctx` bundle | 관측일 T마다 `label_before_exclusive=T` 캐시. **버전·지문 불일치 시 재학습** |

**ML v19 학습 단축(기본값):** 최근 `ML_TRAIN_LOOKBACK_DAYS`(90)거래일만 표본 생성, 일별 음성 상한 `ML_TRAIN_MAX_NEG_PER_DAY`(120), miss 부스트 키·복제 수 제한. 일자당 재학습 **~5~8분**(캐시 미스), 캐시 히트 시 추론 **~6분**(후보 풀 ~900종).

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
| **뉴스맥락(TF-IDF)** | T 직전 구간 전체 early 뉴스 + 급등일 프로필 | `news_context_ml` 5피처·맥락 후보·`news_ctx` joblib |
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
| `PRED_ML_HIGH_CONFIDENCE_PROB` | 고확신 ML 확률 하한(기본 **0.10**, 코드 최소 0.08) |
| `PRED_ML_MID_CONFIDENCE_PROB` | 중확신 하한(기본 0.07) |
| `PRED_OUTPUT_MAX` / `PRED_MID_OUTPUT_MAX` | 고/중 확신 최대 출력 수(기본 각 **10**) |
| `PRED_RANK_POOL_N` | ML 출력·비교표 상위 후보 수(기본 **65**) |
| `PRED_ML_FINALIZE_POOL_N` | finalize 재랭킹 전 풀 크기(기본 **150**) |
| `PRED_MENTION_GATE_MIN` | 종목명 언급 게이트(기본 0.35) |
| `PRED_CHRONIC_MISS_*` | 반복 오탐 종목 후보 제외 |
| `ML_TRAIN_LOOKBACK_DAYS` | ML 학습 최근 N거래일(0=전구간, 기본 **90**) |
| `ML_TRAIN_MAX_NEG_PER_DAY` | 일별 음성 샘플 상한(기본 **120**) |
| `ML_MISS_BOOST_MAX_KEYS` / `ML_MISS_BOOST_DUP` | miss 진단 부스트 키 수·복제(기본 320·**1**) |
| `THEME_CARRYOVER_ENABLED` | 전일 테마 가중치(기본 1) |
| `KEYWORD_FEEDBACK_*` | 오판 키워드 온라인 가중치 |
| `PRED_RETURN_MIN` / `MAX` | 표시 예측% 하한·상한(기본 0.20~0.30) |
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

### 7.1 OHLCV란?

**OHLCV**는 주식 **일봉**(하루 단위 시세) 데이터를 부를 때 쓰는 약어입니다.

| 글자 | 영문 | 의미 |
|------|------|------|
| **O** | Open | **시가** — 그날 첫 거래 가격 |
| **H** | High | **고가** — 그날 최고가 |
| **L** | Low | **저가** — 그날 최저가 |
| **C** | Close | **종가** — 그날 마지막 거래 가격 |
| **V** | Volume | **거래량** |

이 프로젝트에서 OHLCV는 **KOSPI·KOSDAQ 상장 종목 전체**의 위 항목을 **거래일×종목** 단위로 모은 것입니다.

| 항목 | 내용 |
|------|------|
| **수집** | `FinanceDataReader`(선택: `USE_KRX_OHLCV=1` 시 KRX 일봉 우선) — `src/stocks.py` |
| **캐시** | `data/cache/ohlcv_long_full.parquet` (또는 표본 시 `ohlcv_long_*Ntickers.parquet`) |
| **파이프라인** | `main.py` 로그의 「가격 데이터 수집(캐시 있으면 재사용)」 단계 |
| **용도** | 일간 **수익률**·20% **급등** 판정, ML **시세 피처**, **모멘텀 후보**, 리포트 **실제 상승률** 비교 |

캐시에 요청 구간이 이미 있으면 재다운로드 없이 Parquet만 읽습니다. `OHLCV_MAX_WORKERS`로 전종목 다운로드 병렬 수를 조절합니다.

---

## 8. 모듈별 주요 함수

### `main.py` / `src/pipeline/`

| 함수 | 역할 |
|------|------|
| `main()` | CLI 모드 분기(단일일·구간·weekly) |
| `pipeline.run_pipeline` | OHLCV·뉴스·학습·일별 예측·비교 (구 `_run_pipeline`) |
| `pipeline.render_monthly_batch` | 월별 HTML·목차 |
| `pipeline.freeze` | `prediction_freeze_by_t.json` load/save |

### `src/prediction/` (모듈 분리)

| 모듈 | 역할 |
|------|------|
| `predict.py` | 휴리스틱·`PredictionRow`·리포트 근거 HTML |
| `prediction_ranking.py` | 다요인 하이브리드·확신 tier·finalize·Hit@K |
| `ml_move_rank.py` | ML 학습·`rank_predictions_ml`·확률 보정 |
| `candidate_pool.py` | `day_candidate_codes` — 후보 풀 합집합 |
| `news_context.py` | TF-IDF 뉴스 맥락·`affinity_candidate_codes` |
| `market_features.py` | KS11·OHLCV·업종·`prior_industry_hot` |
| `accuracy_cache.py` | 예측 이력·키워드 피드백 |

| 함수 | 역할 |
|------|------|
| `predict_for_trading_day` | 전 종목 스코어 → 상위 N |
| `finalize_ranked_predictions` | 순위·확신·캐리오버 재랭킹 |
| `fit_or_load_classifier` | ML 학습 또는 joblib 로드(**v19**) |
| `day_candidate_codes` | 예측·학습 공통 후보 풀 |
| `rank_predictions_ml` | 급등 확률·하이브리드 순 상위 후보 |
| `refine_confidence_tiers` | 고확신 정밀 게이트 |

루트 shim: `from src import predict` · `ml_move_rank` · `news_context_ml` → `news_context`

### `src/diagnostics/prediction.py`

| 함수 | 역할 |
|------|------|
| `load_prediction_eval_context` | OHLCV·뉴스·스냅샷 로드 |
| `PredictionEvalContext.rank_predictions` | 단일일 ML 예측·Hit@K 평가 |

### `src/prediction_accuracy_cache.py` (→ `prediction.accuracy_cache`)

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

### `src/learning/` (shim: `snapshot_rebuild_learning.py`)

| 함수 | 역할 |
|------|------|
| `build_market_theme_flow` | 일별 시장·테마 요약 |
| `merge_rebuild_learning_dict` | `daily` 병합·누적 재계산 |
| `merge_extended_rebuild_into_snapshot` | JSON 파일 저장 |
| `format_market_theme_flow_html` | 일자 시장·테마 HTML 블록 |
| `_theme_labels_for_compare_row` | 비교 행·뉴스·시드로 테마 라벨 추정 |

### `src/report/`

| 함수 | 역할 |
|------|------|
| `render_compact_tabbed_report` | 월간/구간 HTML |
| `render_dated_n_report` | 단일일 dated rollup |
| `DayReport` | 거래일 파이프라인 결과 dataclass |

### `src/prediction/prediction_ranking.py` (구 `pred_hybrid` · `pred_precision_gate` 병합)

| 함수 | 역할 |
|------|------|
| `compute_pillar_scores` | ML·모멘텀·수급·섹터·뉴스·시장 0~1 기둥 |
| `multi_factor_rank_score` / `hybrid_rank_score` | 가중 합산 하이브리드 랭킹 점수 |
| `format_factor_summary` | 리포트용 다요인 한 줄 요약 |

### `src/prediction/ml_move_rank.py`

| 함수 | 역할 |
|------|------|
| `fit_or_load_classifier` / `rank_predictions_ml` | ML 랭커 학습·추론 |

### `src/prediction/news_context.py` (구 `news_context_ml`)

| 함수 | 역할 |
|------|------|
| `make_news_ctx_bundle` | TF-IDF 뉴스 맥락 bundle |
| `affinity_candidate_codes` | 맥락 유사도 후보 확장 |

### `src/prediction/prediction_ranking.py` — 정밀 게이트

| 함수 | 역할 |
|------|------|
| `finalize_ranked_predictions` | 순위·확신·표시 % 일괄 확정 |
| `passes_precision_gate` | 고확신 정밀 게이트(순위·기둥·ML·피드백) |
| `refine_confidence_tiers` | `high` 슬롯 재필터·미통과 `mid` 강등 |
| `compute_hit_at_k_metrics` | Hit@K·Precision·Lift 평가 |

루트 `src/pred_factors.py` · `pred_precision_gate.py` · `prediction_ranking.py` 는 위 모듈로 연결되는 **shim** 입니다.

### `src/investor_flow.py`

| 함수 | 역할 |
|------|------|
| `warm_cache_for_codes` | 누락 종목 네이버 수급 캐시 프리페치 |
| `flow_features_for_day` | lag1·3일 누적 수급 피처 |
| `investor_flow_candidate_codes` | 수급 강한 종목 후보 확장 |

### `src/report/content.py` (구 `rationale` + `theme_movers`)

| 함수 | 역할 |
|------|------|
| `format_day_mover_rationale_html` | 상한·20%↑ 종목별 급등 이유 bullet HTML |
| `enrich_day_report_mover_rationale` | ``DayReport`` 에 ``mover_rationale_html`` 주입 |
| `build_move_reference_html` | 상·하락 참고 패널(뉴스 카테고리·시세·공시) |
| `theme_early_overlap_html` | early 뉴스·테마 키워드 겹침 HTML |

### `src/report/render.py` (구 `types` 병합 — ``DayReport`` 등)

| 함수 | 역할 |
|------|------|
| `load_index_frame` / `index_daily_return_pct` | KS11 등 지수 일봉·당일 수익률 |
| `industry_code_for_stock` | 네이버 업종코드(캐시) |
| `industry_momentum_for_code` | 동종 업종 전일 평균 수익 → 0~1 |

### `src/learning/support.py` (구 `snapshot` · `rebuild_support` · `theme_carryover` · `snapshot_miss_diagnosis`)

| 함수 | 역할 |
|------|------|
| `weights_for_observation_day` | 직전 영업일 테마 가중치 |
| `build_miss_rows_for_day` | 미예측 급등·고예측 실패 진단 행 |

---

## 9. 실행 시간·성능

| 단계 | 대략 |
|------|------|
| OHLCV·뉴스 (캐시 히트) | 수 분 |
| `train_events` 전체 (`--rebuild`) | 수십 분 |
| **ML 재학습 1일** (v19, 캐시 미스) | **~5~8분** (표본 ~4k, lookback 90일) |
| **거래일 1일 예측** (v19 캐시 히트) | **~6~7분** (후보 풀 ~900종 ML 스코어링) |
| **월간 Hit@K 백테스트** (20거래일) | **~2~3시간** (일자별 재학습) |
| 사후 처리 (JSON 병합·HTML) | 수 분~30분 |

**한 달(~20거래일) 구간, v19·캐시 미스:** ML 재학습 포함 **수 시간** 예상. 캐시 히트 시 훨씬 짧음.

**속도 조절:** `ML_TRAIN_LOOKBACK_DAYS=60`, `ML_TRAIN_MAX_NEG_PER_DAY=80` 등. 벤치: `python scripts/bench_ml_train.py`. Hit@K: `python scripts/diag_prediction.py backtest-month`.

**주의:** ML·freeze 스키마가 바뀌면 구간 전체를 재실행해 일관성을 맞추세요.

---

## 10. 코드 읽는 순서(권장)

1. `main.py` — `parse_cli` → `main` → `src.pipeline.run_pipeline`
2. `src/pipeline/run.py` — 핵심 파이프라인(구 `main._run_pipeline`)
3. `src/config.py` → `src/stocks.py` · `src/news.py` → `src/prediction/` (`candidate_pool` · `ml_move_rank` · `prediction_ranking`)
4. `src/prediction/accuracy_cache.py` → `src/learning/support.py` → `src/learning/market_theme.py`
5. `src/report/content.py` → `src/report/render.py` → `src/report/templates.py`

---

## 11. 의존성

`requirements.txt`: `pandas`, `FinanceDataReader`, `exchange_calendars`, `requests`, `python-dotenv`, `tqdm`, `jinja2`, `scikit-learn`, `joblib` 등. **pykrx** 권장(전종목 등락률).

---

## 12. 랭킹·하이브리드·후보 풀 (v19)

| 항목 | 내용 |
|------|------|
| **후보 풀** | `candidate_pool.day_candidate_codes` = 키워드 ∪ 모멘텀(220) ∪ 수급 ∪ TF-IDF ∪ 전일급등 peer ∪ 업종 버스트 ∪ 상대강도 |
| **finalize** | 상위 `PRED_ML_FINALIZE_POOL_N`(150)에서 섹터 다양화·캐리오버 재랭킹 후 출력 |
| **순위** | `multi_factor_rank_score` — ML·모멘텀·수급·RS·섹터·뉴스·시장 가중 |
| **ML 피처** | v19: `prior_industry_hot` + 뉴스맥락 5종 + 시세·수급·KS11 |
| **평가** | `python scripts/diag_prediction.py recall YYYYMMDD` · `backtest-month` |
| **재학습** | `ML_MODEL_VERSION`·freeze schema 변경 후 구간 `main.py From To` |

주요 환경 변수: `PRED_RANK_POOL_N`, `PRED_ML_FINALIZE_POOL_N`, `PRED_EVAL_HIT_AT_K`, `ML_TRAIN_LOOKBACK_DAYS`.

### 12.1 버전·캐시 갱신 체크리스트

1. `src/prediction/ml_move_rank.py` 의 `ML_MODEL_VERSION` 과 `config.PREDICTION_FREEZE_SCHEMA_VERSION` 확인(현재 **19** / **23**).
2. `move_ranker_v19_*` 가 없는 관측일 T 는 첫 실행 시 재학습.
3. freeze `_schema_version` 불일치 시 **전체 freeze 무시** → 구간 재예측.
4. 6월 Hit@K 백테스트: `python scripts/diag_prediction.py backtest-month --year 2026 --month 6`

---

## 13. 한계·주의

- 투자 권유가 아닌 **패턴 탐색·리포트**용입니다.
- 뉴스·공시 검색 결과에 의존하며, 수급·재무 등 비뉴스 요인은 제한적으로만 반영됩니다.
- 표시 예측%는 **확률→표시 변환** 또는(레거시) 순위 매핑 결과이며, 투자 수익률 보장이 아닙니다.
- `rebuild_learning` 은 `--rebuild` / `--append` + **From~To** 실행이 **정상 완료**되어야 해당 구간에 쌓입니다.
