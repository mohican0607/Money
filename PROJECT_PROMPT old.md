# =============================================================================
# Money 재현 명세 (텍스트 에디터용)
# 작성일: 2026-08-14
# =============================================================================
#
# 사용법
#   1) 아래 PATHS 블록이 이 문서의 사전이다. # 로 시작하는 줄은
#      Python 주석이 아니라 "제목/구분선"이다. PATHS 값은 살아 있다.
#   2) 본문의 ROOT\main.py 는 PATHS의 ROOT + \main.py 로 읽는다.
#      예: ROOT\main.py  →  E:\Git\Money\main.py
#   3) 채팅에 붙여넣을 때 PATHS 포함 전체를 넣는다. 에이전트는
#      PATHS를 먼저 치환한 뒤 구현한다. 폴더를 옮기면 PATHS만 고친다.
#
# -----------------------------------------------------------------------------
# PATHS  (여기만 절대경로. 본문은 왼쪽 이름만 사용)
# -----------------------------------------------------------------------------

PATHS
  SPEC     = E:\Git\Money.md
  ROOT     = E:\Git\Money
  VENV     = ROOT\.venv
  PY       = VENV\Scripts\python.exe
  PYW      = VENV\Scripts\pythonw.exe
  SRC      = ROOT\src
  PIPE     = SRC\pipeline
  PRED     = SRC\prediction
  LEARN    = SRC\learning
  RPT      = SRC\report
  SCRIPTS  = ROOT\scripts
  TESTS    = ROOT\tests
  DOCS     = ROOT\docs
  DATA     = ROOT\data
  CACHE    = DATA\cache
  NEWS     = CACHE\news
  OUT      = ROOT\output
  TEMP     = F:\temp\Money\tmp          (없으면 ROOT\data\cache\_tmp)
  MCACHE   = F:\temp\Money\cache        (env MONEY_CACHE_DIR, 없으면 CACHE)
  TRAIN    = MCACHE\train
  LISTING  = MCACHE\listing
  LOGS     = SCRIPTS\logs
  ENV      = ROOT\.env                  (비밀. git/SPEC에 넣지 말 것)
  ENVEX    = ROOT\.env.example

펼치면
  ROOT\main.py              = E:\Git\Money\main.py
  PYW                       = E:\Git\Money\.venv\Scripts\pythonw.exe
  TRAIN\prediction_freeze_by_t.json
                            = F:\temp\Money\cache\train\prediction_freeze_by_t.json

이 파일에 없는 것
  VENV, CACHE(parquet·joblib·뉴스 JSON), OUT HTML, ENV 비밀, __pycache__
  RPT\templates.py 등 수천 줄 소스는 git 워킹트리가 정본.
  바이트 단위 복원 = git clone / ROOT 폴더 복사.

# =============================================================================


# =============================================================================
# 프롬프트 시작
# =============================================================================

너는 Windows에서 동작하는 Python 3.10+ 프로젝트 Money 를 ROOT 에 구현한다.
주석·CLI·리포트·로그는 한국어.


# -----------------------------------------------------------------------------
# 1. 목적
# -----------------------------------------------------------------------------

KOSPI·KOSDAQ 전 종목.
기준일 N 14:30(KST)까지 아는 뉴스·시세·테마·수급으로
관측일 T = N+1 거래일의 20%↑ 급등 후보를 고른다.
산출: HTML 리포트 + 이메일.
매수 가정: N일 14:00~14:50 주문, T일 급등.


# -----------------------------------------------------------------------------
# 2. 불변 규칙 (어기면 실패)
# -----------------------------------------------------------------------------

  1. 워크포워드
     T 예측·학습 라벨은 T 직전 급등만. T 당일·이후 누수 금지.

  2. 예측 뉴스 컷오프
     USE_DECISION_NEWS_INTRADAY_CUTOFF=1.
     early = N일 15:30 KST까지.
     CLI 전방 실행 허용 = 14:30 (CLI_FORWARD_RUN_KST_*).

  3. 완성 일봉
     N일 15:30 전에 N일 종가·학습 증분을 확정으로 쓰지 말 것.
     --append-rebuild-learning 으로 N일 train_events 넣는 것은 15:30 이후.

  4. 예측 freeze
     파일: TRAIN\prediction_freeze_by_t.json
     스키마 버전: SRC\config.py 상수 (현재 49).
     - 이미 저장된 freeze는 지우지 않는다. none-tier 과거 slate도 재사용.
     - 신규 계산에서 고·중 확신 0이면 none-tier 21.81% 더미로 cap 채우지 말 것.
       빈 slate만: { "_empty_slate": true }
     - 장 마감 후 --append-rebuild-learning + 단일 T:
       freeze 후보 유지, actual·테마·rebuild_learning 만 갱신.

  5. 장중/미래 T 표
     freeze 재사용이면 freeze slate 표시.
     신규면 고·중 확신만.
     실제 급등만인 행은 forward 표에서 제외.

  6. 장 마감 확정일 표
     예측 후보 또는 당일 실제 20%↑
     (_compare_row_belongs_in_closed_day_table).
     예측을 지우고 실제만 남기지 말 것.

  7. PowerShell 창 금지
     스케줄 TR = PYW SCRIPTS\run_daily_email.py --slot …
     powershell.exe -File 로 창을 띄우지 말 것.
     main.py 자식: Windows CREATE_NO_WINDOW.

  8. 이메일
     14:30 / 15:30 / 16:00 모두 성공·실패·타임아웃 메일.
     16:00만 skip 금지. --skip-email 은 수동만.

  9. 16:00 슬롯
     ROOT\main.py --append-rebuild-learning YYYYMMDD
     YYYYMMDD = 관측일 T=오늘 (N이 아님).
     14:30·15:30의 N→N+1 forward 와 다름.
     첨부 dated 리포트 = OUT\report_dated_by_MMDD.html (MMDD=오늘 T).

 10. lock
     LOGS\.run_1430.lock
     LOGS\.run_1530.lock
     16:00은 둘 다 풀릴 때까지 최대 90분 대기.


# -----------------------------------------------------------------------------
# 3. 의존성  ROOT\requirements.txt
# -----------------------------------------------------------------------------

    pandas>=2.0
    requests>=2.28
    python-dotenv>=1.0
    jinja2>=3.1
    tqdm>=4.65
    finance-datareader>=0.9.50
    pykrx>=1.0.40
    exchange-calendars>=4.5
    pyarrow>=14.0
    scikit-learn>=1.3.0
    numpy>=1.24


# -----------------------------------------------------------------------------
# 4. ROOT\.gitignore
# -----------------------------------------------------------------------------

    .venv/
    .env
    *.pyc
    _tmp_*.txt
    .pytest_cache/
    __pycache__/
    data/cache/
    output/
    z_temp/


# -----------------------------------------------------------------------------
# 5. 디렉터리 (ROOT 기준)
# -----------------------------------------------------------------------------

    ROOT\
      main.py
      requirements.txt
      .env.example                 ENVEX
      .gitignore
      docs\PROJECT_GUIDE.md        DOCS
      src\                         SRC
        __init__.py                lazy: config, stocks, predict, news
        config.py
        trading_calendar.py        XKRX + KRX_AD_HOC_SESSION_CLOSURES
        features.py                BreakoutEvent, 키워드
        stocks.py                  OHLCV parquet, 수익률, pykrx, 정지
        news.py                    네이버/구글RSS/mock, early/late, 공시
        investor_flow.py           shim → stocks 수급
        snapshot_rebuild_learning.py
        data\                      구 경로 호환 shim
        notify\email_report.py
        pipeline\                  PIPE
          __init__.py
          cli.py
          early_validate.py
          run.py
          batch.py
          rows.py
          support.py
          types.py
        prediction\                PRED
          predict.py
          prediction_ranking.py
          ml_move_rank.py          HistGradientBoosting v24
          candidate_pool.py
          news_context.py          TF-IDF, 기본 OFF
          market_features.py
          accuracy_cache.py
          feedback_loop.py
        learning\                  LEARN
          market_theme.py
          support.py
        report\                    RPT
          render.py
          templates.py
          content.py
          live_quotes.py           127.0.0.1:8765
        diagnostics\prediction.py
      scripts\                     SCRIPTS
        run_daily_email.py         --slot 1430|1530|1600
        run_daily_1430_email.ps1
        run_daily_1530_email.ps1
        run_daily_1600_append.ps1
        register_daily_email_tasks.ps1
        run_daily_1500.ps1         레거시: 15:40 대기
        check_trading_day_for_daily.py
        build_ohlcv_full_cache.py
        backfill_news.py
        import_freeze_from_report.py
        restore_freeze_from_report.py
        diag_prediction.py
        (기타 diag_*.py, compare_*.py — 진단)
      tests\                       TESTS
        test_prediction_integrity.py
        test_range_n_plus_one.py
        test_run_daily_email.py
        test_email_report.py
        test_precision_forecast_alignment.py
        test_high_evidence_gate.py
        test_feedback_loop.py
        test_chart_tooltip.py
        test_live_quotes.py
        test_report_theme_inject.py
      data\cache\                  CACHE  git 제외
      output\                      OUT    git 제외


# -----------------------------------------------------------------------------
# 6. ROOT\main.py
# -----------------------------------------------------------------------------

  __main__:
    _parse_cli
    + validate_cli_or_none(..., use_n_day=)
    → main()
    → 60초 이상이면 소요시간 출력

  모드:
    daily              인자 없음. N=오늘, T=N+1
    dated              YYYYMMDD=T. --n-day 이면 N
    range              From To
    weekly
    serve_live_quotes

  dated/daily:
    observation_day_forward_mode
    장 마감 전 T 캘린더 뉴스 omit
    run_pipeline
    report.render_dated_n_report
      → OUT\report_dated_by_MMDD.html
    render_monthly_batch 병합

  NO_AUTO_OPEN_OUTPUT=1 이면 HTML 자동 열기 금지.
  스케줄 러너는 이 값을 켠다.


# -----------------------------------------------------------------------------
# 7. SRC\config.py
# -----------------------------------------------------------------------------

  load_dotenv(ROOT / ".env")     → ENV
  BOM·따옴표 제거: _env_str

  MONEY_TEMP_DIR  → TEMP (드라이브 F: 있으면 기본 TEMP)
  MONEY_CACHE_DIR → MCACHE → TRAIN, LISTING

  BIG_MOVE_THRESHOLD = 0.20
  TRAIN_START_DEFAULT  env TRAIN_START 또는 2025-04-11
  TEST_START = 2026-01-01          학습 상한 아님
  NEWS_CUTOFF_KST_HOUR=15 MINUTE=30
  PREDICTION_FREEZE_SCHEMA_VERSION
  ML_MODEL_VERSION 24
  joblib: TRAIN\move_ranker_v24_*.joblib
  ML_REUSE_PRIOR_FOR_FORWARD=1     장중은 직전 joblib
  PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD
    장중 스케줄에서는 0
    (SCRIPTS\run_daily_email.py _ensure_env_defaults)
  RUN_DAILY_MAIN_TIMEOUT_SEC 기본 5400
  KRX_AD_HOC_SESSION_CLOSURES: 2026-06-03, 2026-07-17

  정밀도·출력은 ENV 로 덮어씀.
    PRED_OUTPUT_MAX
    PRED_FORWARD_SHOW_MAX
    PRED_PRECISION_GATE_ENABLED
    PRED_ML_HIGH_CONFIDENCE_PROB
    …

  이메일:
    EMAIL_ENABLED
    EMAIL_SMTP_*
    EMAIL_RECIPIENTS
    EMAIL_SUBJECT_PREFIX

  경로 상수:
    OUTPUT_DIR              = OUT
    TRAIN_SNAPSHOT_PATH     = TRAIN\breakout_train_snapshot.json
    PREDICTION_FREEZE_PATH  = TRAIN\prediction_freeze_by_t.json
    OHLCV                   = CACHE\ohlcv_long_full.parquet
    뉴스                    = NEWS\{naver|google|mock}\YYYY\day_YYYYMMDD.json


# -----------------------------------------------------------------------------
# 8. CLI   PIPE\cli.py + PIPE\early_validate.py
# -----------------------------------------------------------------------------

  argv에서 날짜가 아닌 플래그 제거:
    --use-train-snapshot
    --rebuild-train-snapshot
    --append-rebuild-learning
    --no-train-snapshot
    --no-report-expand
    --use-freeze
    --n-day

  PY ROOT\main.py                         daily
  PY ROOT\main.py YYYYMMDD                dated T
  PY ROOT\main.py --n-day YYYYMMDD        dated N, T=N 다음 거래일
  PY ROOT\main.py From To                 range. KRX 거래일만.
                                          To ≤ 현재 N+1
                                          N+1 포함 시 N일 14:30 이후만
  --weekly
  --serve-live-quotes

  구현: validate_dated_n_day / validate_dated_t_day


# -----------------------------------------------------------------------------
# 9. 파이프라인  PIPE\run.py
# -----------------------------------------------------------------------------

  1. 상장 + OHLCV 캐시 보강
     should_skip_ohlcv_right_gap:
     N=오늘이고 15:30 전이면 우측 갭 스킵

  2. 캘린더 뉴스
     omit_target_calendar_days:
     N 장 마감 전이면 T 당일 캘린더 뉴스 수집 안 함

  3. BreakoutEvent 스냅샷
     load / rebuild / append_learning 증분

  4. 각 T
     freeze 재사용 → predict_for_trading_day 또는 freeze rows

  5. freeze 저장
     _display_prediction_rows_for_freeze (고·중만)
     비면 empty slate

  6. rows_compare
     day_forward → 실제 수익률 None
     확정일 → pykrx/OHLCV actual, 미예측 20%↑ 병합

  7. 정확도 캐시, 테마, rebuild_learning 병합
     append_learning/rebuild 이고 forward가 아닐 때

  _observation_day_forward_mode:
    T>오늘  또는  (T=오늘 거래일 and 15:30 전)  → True


# -----------------------------------------------------------------------------
# 10. freeze  PIPE\support.py
# -----------------------------------------------------------------------------

  _EMPTY_SLATE_KEY = "_empty_slate"

  _freeze_entry_usable
    빈 목록 False
    empty slate True
    pred% 있는 기존 항목(none-tier 포함) True
    신규 더미 금지는 저장 함수에서

  _display_prediction_rows_for_freeze
    high/mid만, cap=PRED_FORWARD_SHOW_MAX
    없으면 []

  _prediction_rows_to_frozen_items([])
    → [{_empty_slate: true}]

  _prediction_rows_from_frozen_items
    empty slate → []


# -----------------------------------------------------------------------------
# 11. 일일 스케줄  SCRIPTS\run_daily_email.py
# -----------------------------------------------------------------------------

  slot  시각    main.py                                      메일  lock
  ----  -----   -------------------------------------------  ----  ----------
  1430  14:30   ROOT\main.py  (daily N→N+1)                   예    획득 1430
  1530  15:30   ROOT\main.py --append-rebuild-learning        예    획득 1530
                (같은 daily, freeze 유지)
  1600  16:00   ROOT\main.py --append-rebuild-learning        예    1430+1530
                {오늘 YYYYMMDD}  T=오늘 확정                       대기

  거래일 아니면 exit 0 (_check_trading_day_exit 2/3/4)
  stdout/stderr 임시파일. Windows CREATE_NO_WINDOW
  로그: LOGS\run_daily_YYYYMMDD.log

  작업 스케줄러: SCRIPTS\register_daily_email_tasks.ps1
    TR = PYW SCRIPTS\run_daily_email.py --slot …
    MoneyKRX_Daily1430_Email
    MoneyKRX_Daily1530_Email
    MoneyKRX_Daily1600_Append
  구 MoneyKRX_Daily1500_Append 는 1600 등록 후에만 삭제

  수동 콘솔: PY  또는  ps1 -ShowWindow

  메일: SRC\notify\email_report.py
    SMTP TLS, 일별+월간 HTML 첨부
    제목: [완료] / 실패 / 실패·타임아웃
    16:00 제목의 T = 오늘


# -----------------------------------------------------------------------------
# 12. 리포트  OUT
# -----------------------------------------------------------------------------

  월간    OUT\report_YYYY.MM.html
          탭=ISO 주, 일자 섹션 id="day-YYYY-MM-DD"
  dated   OUT\report_dated_by_MMDD.html     MMDD = T
  테마    OUT\report_theme_25pct_YYYY.MM.html
  목차    OUT\report_index_monthly.html
  장중    OUT\live_quotes.js + 로컬 프록시

  표: 구분, 종목(차트 툴팁), 예측%, N일봉 체인, 예측 근거,
      보정, 누적 정확도, 이유/차이, 예측 신호
  forward: 「예측 전용」 pill, actual —
  확정: 예측%와 실제% 함께. 예측 전용 금지


# -----------------------------------------------------------------------------
# 13. 예측·ML
# -----------------------------------------------------------------------------

  후보 풀 PRED\candidate_pool.py day_candidate_codes
    키워드/종목명 ∪ 모멘텀 ∪ 수급 ∪ (옵션 TF-IDF) ∪ 전일급등·업종 peer

  휴리스틱 + HistGradientBoosting
  PRED_USE_ML_RANKER=0 이면 휴리스틱만

  v24: 순위용 binary+soft-return, 확신용 binary 분리, 보정 ml_prob
  finalize_ranked_predictions → high/mid/none
  정밀 게이트: passes_precision_gate / refine_confidence_tiers
  전일 과열: prior_day_exhaustion_blocks_confidence
    (상한 직후 high 금지)

  표시 예측% PRED_RETURN_MIN~MAX (기본 0.20~0.30)
  게이트 탈락 시 같은 숫자로 10종 채우지 말 것


# -----------------------------------------------------------------------------
# 14. 학습 캐시  TRAIN
# -----------------------------------------------------------------------------

  breakout_train_snapshot.json
    train_events, rebuild_learning, market_theme_flow, prediction_gap_rollup
  prediction_freeze_by_t.json
    T → 후보 리스트 또는 empty slate
  prediction_accuracy_track.json
    종목별 이력, t_code_ratio
  daily_theme_snapshots.json
    테마 캐리오버
  move_ranker_v24_*.joblib
    T별 또는 재사용 모델


# -----------------------------------------------------------------------------
# 15. ENVEX 스키마 (비밀은 플레이스홀더)
# -----------------------------------------------------------------------------

  NAVER_CLIENT_ID / NAVER_CLIENT_SECRET
  USE_GOOGLE_NEWS_RSS_FALLBACK=1
  MOCK_NEWS=0
  EMAIL_*
  RUN_DAILY_MAIN_TIMEOUT_SEC=5400
  MONEY_TEMP_DIR     → TEMP
  MONEY_CACHE_DIR    → MCACHE

  정밀도 튜닝 키는 ENV 에 다수.
  SRC\config.py 의 getenv 전부를 지원할 것.


# -----------------------------------------------------------------------------
# 16. 테스트  (pytest, 통과 필수)  TESTS
# -----------------------------------------------------------------------------

  freeze: high+mid만 직렬화, few-tier 패딩 없음, empty slate
          과거 none-tier freeze는 usable
  CLI 14:30 cutoff, --n-day 허용
  run_daily_email._run_main_py 빠른 exit, 제목 완료/타임아웃
  이메일 제목, 차트 N일, live quotes, precision 정렬,
  high evidence gate, feedback, theme inject


# -----------------------------------------------------------------------------
# 17. 구현 순서
# -----------------------------------------------------------------------------

  1. config + trading_calendar + features
  2. stocks OHLCV 캐시 + news 캐시
  3. pipeline CLI/validate + ROOT\main.py 뼈대
  4. predict + ranking + ml_move_rank (joblib)
  5. freeze + rows + report HTML
  6. run_pipeline 한 날 MOCK_NEWS=1
  7. email + run_daily_email + PYW 스케줄
  8. tests


# -----------------------------------------------------------------------------
# 18. 하지 말 것
# -----------------------------------------------------------------------------

  - 확신 0건을 PRED_FORWARD_SHOW_MAX 만큼 none-tier로 채우기
  - 확정일 리런에서 freeze를 empty로 덮어 예측 삭제
  - 16:00에 N→N+1 forward만 돌리고 당일 T를 「예측 전용」으로 방치
  - 스케줄을 powershell 창으로 띄우기
  - ENV 실비밀을 git 또는 SPEC 에 커밋


# =============================================================================
# 프롬프트 끝
# =============================================================================

정본은 ROOT 의 Python / PS1 / 테스트다.
SPEC 은 의미 단위 복원용이다.
1:1 바이트 복원 = git clone 또는 ROOT 폴더 복사.

런타임 데이터는 실행으로 재생성:
  CACHE parquet, NEWS day JSON, TRAIN joblib/freeze, OUT HTML.

ML v24. freeze 스키마는 SRC\config.py 상수를 따른다.
