# Money 프로젝트 가이드

KOSPI·KOSDAQ 상장 종목에 대해 **일별 뉴스**와 **주가 급등(예: 일 20% 이상)** 의 패턴을 휴리스틱으로 연결하고, **익일(다음 거래일) 급등 후보**를 HTML 리포트로 출력하는 도구입니다. 머신러닝 모델이 아니라, 과거 급등일의 뉴스 키워드 프로필과 당일 뉴스 키워드의 **교집합·종목명 언급**으로 점수를 매깁니다.

---

## 1. 디렉터리 구성

| 경로 | 역할 |
|------|------|
| `main.py` | CLI 진입점. 모드별로 파이프라인 호출 후 HTML 저장·(선택) 자동 열기 |
| `src/config.py` | 경로(`ROOT`, `CACHE_DIR`, `OUTPUT_DIR`), 환경 변수, 훈련·테스트 날짜 상수 |
| `src/stocks.py` | KRX 상장 리스트·OHLCV 다운로드·Parquet 캐시, 일간 수익률, pykrx 전종목 등락률 |
| `src/news.py` | 일자별 뉴스 수집(네이버 API / Google RSS / 모의), early·late 분류, 캐시 JSON |
| `src/trading_calendar.py` | `exchange_calendars` XKRX 기준 거래일·뉴스 윈도 경계 |
| `src/features.py` | 토큰·키워드, 과거 급등–뉴스 `BreakoutEvent` 구축 |
| `src/predict.py` | 훈련 이벤트 기반 종목 스코어·예측 수익률(클램프된 평균)·갭 설명 HTML |
| `src/market_index.py` | KOSPI 등 지수 일봉·당일 수익률(코멘트용) |
| `src/report.py` | Jinja2 기반 HTML(월간 탭형·단일 N 리포트) |
| `scripts/` | 백필·스케줄 보조·OHLCV 캐시 전용 스크립트 |
| `data/cache/` | `krx_listing.parquet`, `ohlcv_long_*.parquet`, `news/naver|google|mock/…/YYYY/day_*.json` |
| `output/` | 생성된 `report_*.html` |

---

## 2. 데이터 흐름(한 줄 요약)

### 뉴스 캐시는 출처·연도·일자(`{naver|google|mock|none}/YYYY/day_*.json`)

네이버 API 사용 시 **`news/naver/`**, Google RSS 시 **`news/google/`**, 모의 뉴스는 **`news/mock/`** 아래에 연도 폴더를 두고 하루당 JSON 1개입니다. 구 평면·구 `news/YYYYMM/`·구 `<provider>/YYYYMM/` 은 읽을 때 연도 폴더로 이전하는 등 호환 처리됩니다.

- **`NEWS_NAVER_QUERY_MODE=market`(기본)**  
  API 호출은 「YYYY년 M월 D일 증시」 같은 **시장 단위** 쿼리입니다. 네이버 뉴스 API에는 **날짜 From/To 파라미터가 없고**, `pubDate`가 해당 일인 기사만 남깁니다.

- **`NEWS_NAVER_QUERY_MODE=ticker`** (네이버 키 필요)  
  **종목마다** `YYYY년 M월 D일 {종목명}` 로 검색한 뒤, 마찬가지로 **`pubDate`로 그날만** 필터해 한 날짜 파일에 합칩니다.  
  “날짜 From~To”는 파이프라인이 **필요한 각 캘린더일**에 대해 이 함수를 반복 호출하면서 구간을 채우는 방식입니다(하루 단위가 곧 조회 단위).

- **`NEWS_NAVER_QUERY_MODE=both`** (네이버 키 필요)  
  `market` + `ticker`를 **같은 날짜에 모두 조회**한 뒤 기사 단위로 중복을 병합합니다.  
  종목 단위 신호와 시장 전체 흐름을 동시에 학습·추론에 반영하고 싶을 때 권장합니다.

- 네이버 키가 없고 RSS만 쓰는 경우: `ticker`/`both`를 켜도 **종목별 API가 불가**해 자동으로 **시장 일자 쿼리**로 대체됩니다.

### 흐름도

```
상장 리스트 + OHLCV(캐시) → 일간 수익률
        ↓
캘린더 일자별 뉴스(JSON 캐시) ──→ 훈련 구간에서 급등일마다 BreakoutEvent(키워드 집합)
        ↓
관측일 T: early 뉴스 blob → 키워드 vs 과거 프로필 → 상위 후보 + 예측 수익률
        ↓
실제 급등: pykrx 전종목 등락률(우선) 또는 OHLCV 기반 폴백
        ↓
DayReport → HTML
```

### 2.1 기준일 N과 관측일 T

- 사용자가 **기준일 N**을 고르면(예: `python main.py 20260410`), **관측 거래일 T**는 `next_trading_day_after(N)` 입니다.
- 시나리오: **N일 장 마감 전**에 주문을 넣고 **T일 급등**을 노리는 경우.
- 예측에 쓰는 뉴스는 **N−1 거래일 15:00(KST)까지**(early)로 자르는 옵션이 기본입니다(`USE_DECISION_NEWS_INTRADAY_CUTOFF`).

### 2.2 훈련 vs 테스트

- `config.TRAIN_START_DEFAULT` ~ `TEST_START` 직전: 훈련 구간에서 `BreakoutEvent` 누적.
- `TEST_START` 이후 날짜가 리포트의 “테스트/관측” 구간으로 쓰입니다.
- 월간 모드(`--weekly`)와 구간 모드는 `REPORT_TEST_DAY_START` / `END` 또는 CLI 인자로 테스트 거래일 목록을 한정합니다.

---

## 3. 실행 방법

프로젝트 루트에서 가상환경 활성화 후:

```bash
python main.py
```

| 명령 | 동작 |
|------|------|
| `python main.py` | 오늘이 거래일이면 N=오늘, T=다음 거래일. 라이브 모드(실제 급등 표 생략 등) |
| `python main.py YYYYMMDD` | N=해당일, T=다음 거래일. **과거 N**이면 pykrx·OHLCV로 실제 20%↑와 예측 비교 |
| `python main.py YYYYMMDD YYYYMMDD` | 두 날짜 사이 **거래일만** 모아 월별 HTML + `report_index_monthly.html` |
| `python main.py --weekly` | `config`에 설정된 테스트 구간을 월 단위 배치 |

자세한 안내는 `main.py` 상단 모듈 독스트링을 참고하세요.

### 3.1 보조 스크립트

- `python scripts/build_ohlcv_full_cache.py` — 뉴스 없이 **전종목 OHLCV Parquet**만 생성.
- `python scripts/backfill_news.py [--start ...] [--end ...]` — 월별 `day_*.json` 백필.
- `python scripts/migrate_news_cache_to_monthly.py` — 구 평면·구 `news/YYYYMM/`·구 `<provider>/YYYYMM/` 을 `news/<provider>/YYYY/` 로 일괄 이동.
- `python scripts/check_trading_day_for_daily.py` — 일일 작업 스케줄에서 오늘/T 거래일 여부 확인(종료 코드).

---

## 4. 환경 변수(.env) 요약

| 변수 | 의미 |
|------|------|
| `NAVER_CLIENT_ID` / `NAVER_CLIENT_SECRET` | 있으면 네이버 뉴스 검색 API 사용. **앱에서「검색」API 사용** 필수. `.env`는 **프로젝트 루트**에 둠(CWD 무관 로드). BOM·따옴표는 자동 제거 |
| `USE_GOOGLE_NEWS_RSS_FALLBACK` | 네이버 키 없을 때 Google News RSS(기본 1) |
| `GOOGLE_NEWS_RSS_DUAL_LOCALE` | 1이면 동일 쿼리를 `KR:en` 에디션에서 한 번 더 호출(건수↑·요청 약 2배, 기본 0) |
| `GOOGLE_NEWS_RSS_QUERY_SLEEP_SEC` | Google RSS **쿼리 묶음** 사이 대기 초(기본 0.09). 너무 낮추면 차단 위험 |
| `MOCK_NEWS` | 1이면 API 없이 모의 뉴스 |
| `SAMPLE_TICKERS` | 숫자 N이면 상장 리스트 상위 N종만 OHLCV(빠른 디버그). 과거 전시장 비교 시 비우는 것을 권장 |
| `USE_KRX_OHLCV` | 1이면 KRX 일봉 우선, 캐시 파일명 `ohlcv_long_krx_*.parquet` |
| `USE_DECISION_NEWS_INTRADAY_CUTOFF` | 1이면 N−1 15:00 KST early/late 분리(기본 1) |
| `MAX_TEST_DAYS` | 월간 모드에서 테스트 거래일 수 상한(0이면 제한 없음) |
| `NO_AUTO_OPEN_OUTPUT` | 1이면 실행 후 `output` 폴더/HTML 자동 열기 안 함 |
| `OHLCV_MAX_WORKERS` | 전종목 일봉 다운로드 동시 스레드 수(기본 12). 느리면 네트워크·CPU에 맞게 조정 |
| `NEWS_FETCH_MAX_WORKERS` | **서로 다른 캘린더 일** 뉴스를 동시에 받을 워커 수(기본 4). 네이버 429·차단 시 **1~2**로 낮출 것 |
| `NEWS_NAVER_QUERY_MODE` | `market`(기본) / `ticker`(종목별) / `both`(시장+종목 병합, 권장·네이버 키 필수) |
| `NEWS_TICKER_NAVER_MAX_WORKERS` | `ticker` 모드에서 **하루 안** 종목 쿼리 병렬 수(기본 8) |
| `NEWS_TICKER_NAVER_MAX_PAGES` | 종목당 네이버 페이지 수(100건/페이지, 기본 2, 상한 10) |

상수 날짜·임계값은 `src/config.py`에서 코드로 정의된 항목도 있습니다(예: `BIG_MOVE_THRESHOLD = 0.2`).

---

## 5. 캐시·산출물

- **OHLCV**: `data/cache/ohlcv_long_full.parquet`(또는 KRX 모드·표본 모드에 따라 파일명 변형). 최초 전종목 다운로드는 시간이 오래 걸릴 수 있습니다.
- **상장 리스트**: `data/cache/krx_listing.parquet`
- **뉴스**: `data/cache/news/naver/…` 또는 `…/google/…` 등 (`YYYY/day_YYYYMMDD.json`)
- **리포트**: `output/report_YYYYMMDD.html`, `report_YYYY.MM.html`, `report_index_monthly.html`

---

## 6. 의존성

`requirements.txt` 기준: `pandas`, `FinanceDataReader`, `exchange_calendars`, `requests`, `python-dotenv`, `tqdm`, `jinja2` 등. **실제 20%↑ 전종목**을 pykrx로 보강하려면 `pykrx` 설치가 필요합니다(없으면 OHLCV 범위로 폴백).

---

## 7. 코드 읽는 순서(권장)

1. `main.py` — `_parse_cli` → `main` → `_run_pipeline`
2. `src/config.py` — 상수·경로
3. `src/stocks.py` — `build_ohlcv_long`, `daily_returns_table`, `try_krx_change_pct_by_code`
4. `src/news.py` — `news_fetch_calendar_span`, `fetch_news_for_calendar_day`, `aggregate_early_late_for_target`
5. `src/features.py` — `build_breakout_events`
6. `src/predict.py` — `predict_for_trading_day`, `explain_return_gap_html`
7. `src/report.py` — `render_compact_tabbed_report`, `render_dated_n_report`

각 함수의 **상세 docstring**은 소스 파일에 추가되어 있습니다.

---

## 8. 한계·주의

- 투자 권유가 아닌 **패턴 요약·탐색용** 리포트입니다.
- 뉴스는 검색/RSS 결과에 의존하며, 공시·수급 등 비뉴스 요인은 반영하지 않습니다.
- 예측 수익률 숫자는 과거 급등일 수익률의 **단순 평균·클램프**이며, 확률校정된 모델 출력이 아닙니다.
