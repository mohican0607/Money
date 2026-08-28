"""
경로·상수·환경 변수.

이 모듈은 함수 없이 프로젝트 전역 설정만 노출합니다.
- ``ROOT`` / ``DATA_DIR`` / ``CACHE_DIR`` / ``OUTPUT_DIR``: 디렉터리 루트(``load_dotenv()`` 후 계산).
- 네이버·뉴스·시세 관련 플래그는 ``os.getenv`` 기반이며, 실행 전 ``.env`` 또는 환경 변수로 덮어씁니다.
- 날짜 상수: ``TRAIN_START_DEFAULT``(급등 이벤트 수집 시작), ``TEST_START``(``--weekly`` 등 **기본 관측 시작일**; 학습 상한 아님), ``REPORT_TEST_DAY_*``.

상세 사용법과 흐름은 ``docs/PROJECT_GUIDE.md`` 를 참고하세요.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv


def _env_str(key: str, default: str = "") -> str:
    """공백·UTF-8 BOM·양끝 따옴표 제거. .env 편집기 BOM/따옴표로 인한 401 방지."""
    v = os.getenv(key, default)
    if v is None:
        return ""
    v = v.strip()
    if v.startswith("\ufeff"):
        v = v.lstrip("\ufeff").strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        v = v[1:-1].strip()
    return v


def _bool_env(key: str, default: bool = True) -> bool:
    """환경 변수 Y/N·1/0·true/false → bool."""
    raw = _env_str(key, "1" if default else "0").strip().lower()
    if raw in ("0", "false", "no", "off", "n"):
        return False
    if raw in ("1", "true", "yes", "on", "y"):
        return True
    return default


def run_daily_auto_enabled(slot: str) -> bool:
    """거래일 스케줄 슬롯(1430/1530/1600) 자동 실행 여부 — ``RUN_DAILY_AUTO_<slot>``."""
    key = f"RUN_DAILY_AUTO_{slot}"
    raw = _env_str(key)
    if not raw:
        return True
    return _bool_env(key, True)


def _positive_int_env(key: str, default: int) -> int:
    """환경 변수 정수(최소 1). 비어 있거나 잘못되면 default."""
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        n = int(raw)
        return n if n >= 1 else default
    except ValueError:
        return default


def _non_negative_int_env(key: str, default: int) -> int:
    """환경 변수 정수(0 이상). 비어 있거나 잘못되면 default."""
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        n = int(raw)
        return n if n >= 0 else default
    except ValueError:
        return default


def _date_env(key: str, default: date) -> date:
    """``YYYYMMDD`` 또는 ``YYYY-MM-DD`` 환경 변수 → ``date``. 비어 있거나 잘못되면 default."""
    raw = _env_str(key).replace("-", "").replace("/", "")
    if len(raw) != 8 or not raw.isdigit():
        return default
    try:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    except ValueError:
        return default


def _float_env(key: str, default: float) -> float:
    """환경 변수 실수. 비어 있거나 잘못되면 default."""
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


ROOT = Path(__file__).resolve().parents[1]
# CWD와 무관하게 프로젝트 루트의 .env 를 읽음 (IDE/다른 폴더에서 실행 시 키 누락 방지)
load_dotenv(ROOT / ".env")


def _configure_process_temp_dir() -> Path:
    """
  OS·Python tempfile·joblib 등이 쓰는 임시 디렉터리.

  ``MONEY_TEMP_DIR``(권장: ``F:\\temp\\Money\\tmp``) 로 C: 사용자 Temp 사용을 피합니다.
  """
    import tempfile

    raw = _env_str("MONEY_TEMP_DIR")
    if raw:
        temp_dir = Path(raw)
    elif Path("F:/").exists():
        temp_dir = Path("F:/temp/Money/tmp")
    else:
        temp_dir = ROOT / "data" / "cache" / "_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    resolved = str(temp_dir.resolve())
    os.environ["TEMP"] = resolved
    os.environ["TMP"] = resolved
    os.environ["TMPDIR"] = resolved
    os.environ["JOBLIB_TEMP_FOLDER"] = resolved
    tempfile.tempdir = resolved
    return temp_dir


def _resolve_train_cache_dir() -> Path:
    """ML joblib·학습 스냅샷·예측 freeze 등 무거운 학습 캐시(``MONEY_CACHE_DIR/train``)."""
    explicit = _env_str("MONEY_TRAIN_CACHE_DIR")
    if explicit:
        return Path(explicit)
    runtime = _env_str("MONEY_CACHE_DIR")
    if runtime:
        return Path(runtime) / "train"
    return ROOT / "data" / "cache" / "train"


def _resolve_listing_cache_dir() -> Path:
    """네이버 업종 코드·업종명 API 캐시(``MONEY_CACHE_DIR/listing``)."""
    explicit = _env_str("MONEY_LISTING_CACHE_DIR")
    if explicit:
        return Path(explicit)
    runtime = _env_str("MONEY_CACHE_DIR")
    if runtime:
        return Path(runtime) / "listing"
    return ROOT / "data" / "cache" / "listing"


TEMP_DIR = _configure_process_temp_dir()
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
TRAIN_CACHE_DIR = _resolve_train_cache_dir()
LISTING_META_CACHE_DIR = _resolve_listing_cache_dir()
TRAIN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
LISTING_META_CACHE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR = ROOT / "output"
# ``python main.py YYYYMMDD`` / 일일 N 실행 시 기준일별 블록을 한 파일에 누적·같은 N 재실행 시 해당 블록만 갱신.
REPORT_DATED_ROLLUP_HTML = OUTPUT_DIR / "report_dated_by_n.html"
# 과거 급등–뉴스 ``BreakoutEvent`` 학습 스냅샷(JSON). ``main.py`` 기본이 로드·증분 병합, ``--no-train-snapshot`` 로 끔.
TRAIN_SNAPSHOT_PATH = TRAIN_CACHE_DIR / "breakout_train_snapshot.json"
# 관측일(T)별 예측 후보 고정 캐시: 같은 T 재실행 시 예측 종목/예측수익률을 동일하게 유지.
# 갱신은 해당 T에 대한 최초 예측 저장, 또는 main.py --rebuild-train-snapshot 실행 시에만.
PREDICTION_FREEZE_PATH = TRAIN_CACHE_DIR / "prediction_freeze_by_t.json"

NAVER_CLIENT_ID = _env_str("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = _env_str("NAVER_CLIENT_SECRET")
MOCK_NEWS = os.getenv("MOCK_NEWS", "0").strip() in ("1", "true", "True", "yes")

SAMPLE_TICKERS = os.getenv("SAMPLE_TICKERS", "").strip()
SAMPLE_TICKERS_N = int(SAMPLE_TICKERS) if SAMPLE_TICKERS.isdigit() else None

# 1이면 시세를 KRX 일봉 우선(거래소 등락률 Change). 실패 시 기본 소스. 캐시 파일명이 달라짐.
USE_KRX_OHLCV = os.getenv("USE_KRX_OHLCV", "0").strip() in ("1", "true", "True", "yes")

# 0이면 테스트 구간 전체
MAX_TEST_DAYS = int(os.getenv("MAX_TEST_DAYS", "0") or "0")

# 분석 파라미터
BIG_MOVE_THRESHOLD = 0.20  # 일일 수익률 20%
INCLUDE_ACTUAL_BIG_MOVERS_IN_ROWS_COMPARE = os.getenv(
    "INCLUDE_ACTUAL_BIG_MOVERS_IN_ROWS_COMPARE", "0"
).strip().lower() in ("1", "true", "yes", "on")

# 예측 수익률 캘리브레이션(키워드/종목명 신호 강도 기반)
PRED_RETURN_CALIBRATION_ENABLED = os.getenv(
    "PRED_RETURN_CALIBRATION_ENABLED", "1"
).strip().lower() in ("1", "true", "yes", "on")
# 예측-실제 누적 오차(t_code_ratio) 기반 자동 보정 루프.
PRED_ERROR_FEEDBACK_ENABLED = os.getenv("PRED_ERROR_FEEDBACK_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PRED_ERROR_FEEDBACK_MIN_SAMPLES = _positive_int_env("PRED_ERROR_FEEDBACK_MIN_SAMPLES", 6)
PRED_ERROR_FEEDBACK_SHRINK_STRENGTH = _float_env("PRED_ERROR_FEEDBACK_SHRINK_STRENGTH", 12.0)
PREDICTION_FREEZE_ENABLED = os.getenv("PREDICTION_FREEZE_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# 감독학습 랭커(scikit-learn HistGradientBoosting)로 후보 종목 순위를 확률 기반 정렬(0이면 휴리스틱만).
PRED_USE_ML_RANKER = os.getenv("PRED_USE_ML_RANKER", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# 외국인·기관·개인 순매수(네이버) 피처를 ML·하이브리드 예측에 반영.
PRED_INVESTOR_FLOW_ENABLED = os.getenv("PRED_INVESTOR_FLOW_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
INVESTOR_FLOW_PREFETCH_MAX_CODES = _positive_int_env("INVESTOR_FLOW_PREFETCH_MAX_CODES", 520)
INVESTOR_FLOW_FETCH_WORKERS = _positive_int_env("INVESTOR_FLOW_FETCH_WORKERS", 4)
PRED_INVESTOR_FLOW_HYBRID_WEIGHT = _float_env("PRED_INVESTOR_FLOW_HYBRID_WEIGHT", 0.14)
# 전일 급등·뉴스 테마 가중치를 익일 예측에 반영(B안: JSON 스냅샷 + 휴리스틱·ML 피처 1개).
THEME_CARRYOVER_ENABLED = os.getenv("THEME_CARRYOVER_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
THEME_CARRYOVER_SCORE_SCALE = _float_env("THEME_CARRYOVER_SCORE_SCALE", 2.0)
# 캘리브레이션 후 최종 예측 수익률(소수) 클램프 범위 (BIG_MOVE 20%와 맞춤)
PRED_RETURN_MIN = _float_env("PRED_RETURN_MIN", 0.20)
PRED_RETURN_MAX = _float_env("PRED_RETURN_MAX", 0.30)
# 예측 고정 캐시(JSON) 표시 매핑 스키마. 로직 변경 시 숫자를 올리면 재계산됩니다.
PREDICTION_FREEZE_SCHEMA_VERSION = 50

# --- 다요인(멀티팩터) 랭킹 가중치 (합≈1, 뉴스 최소) ---
PRED_FACTOR_W_ML = _float_env("PRED_FACTOR_W_ML", 0.48)
PRED_FACTOR_W_MOMENTUM = _float_env("PRED_FACTOR_W_MOMENTUM", 0.14)
PRED_FACTOR_W_FLOW = _float_env("PRED_FACTOR_W_FLOW", 0.11)
PRED_FACTOR_W_RELATIVE_STRENGTH = _float_env("PRED_FACTOR_W_RELATIVE_STRENGTH", 0.08)
PRED_FACTOR_W_SECTOR = _float_env("PRED_FACTOR_W_SECTOR", 0.15)
PRED_FACTOR_W_NEWS = _float_env("PRED_FACTOR_W_NEWS", 0.02)
PRED_FACTOR_W_MARKET = _float_env("PRED_FACTOR_W_MARKET", 0.02)
PRED_FACTOR_MIN_STRONG_PILLARS = _positive_int_env("PRED_FACTOR_MIN_STRONG_PILLARS", 2)
# 상위 순위 업종 쏠림 완화(동일 업종 최대 N종 / 상위 K)
PRED_SECTOR_DIVERSITY_TOP_K = _positive_int_env("PRED_SECTOR_DIVERSITY_TOP_K", 20)
PRED_SECTOR_DIVERSITY_MAX_PER_INDUSTRY = _positive_int_env(
    "PRED_SECTOR_DIVERSITY_MAX_PER_INDUSTRY", 3
)
# 최근 거래일 업종 급등 peer 후보·피처 반영 일수
PRED_INDUSTRY_HEAT_LOOKBACK_DAYS = _positive_int_env("PRED_INDUSTRY_HEAT_LOOKBACK_DAYS", 5)
PRED_INDUSTRY_LEADER_SLOTS = _positive_int_env("PRED_INDUSTRY_LEADER_SLOTS", 5)
PRED_INDUSTRY_MUST_KEEP_MAX = _positive_int_env("PRED_INDUSTRY_MUST_KEEP_MAX", 280)
PRED_INDUSTRY_MUST_KEEP_SWAP_MAX = _positive_int_env("PRED_INDUSTRY_MUST_KEEP_SWAP_MAX", 180)
PRED_PREPOOL_HOT_PROMOTE_MAX = _positive_int_env("PRED_PREPOOL_HOT_PROMOTE_MAX", 14)
# 핫 섹터 내 전일 과열 리더 대신 중간 모멘텀(로테이션) 종목 우선
PRED_THEME_ROTATION_ENABLED = os.getenv("PRED_THEME_ROTATION_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PRED_THEME_ROTATION_LAG_MIN = _float_env("PRED_THEME_ROTATION_LAG_MIN", 0.04)
PRED_THEME_ROTATION_LAG_MAX = _float_env("PRED_THEME_ROTATION_LAG_MAX", 0.19)
PRED_THEME_ROTATION_RANK_BOOST = _float_env("PRED_THEME_ROTATION_RANK_BOOST", 0.22)
PRED_THEME_ROTATION_TIER_MIN = _float_env("PRED_THEME_ROTATION_TIER_MIN", 0.32)
PRED_THEME_ROTATION_ENRICH_MAX = _positive_int_env("PRED_THEME_ROTATION_ENRICH_MAX", 48)

# --- 고확신 정밀 게이트(다중 신호 합의) ---
PRED_PRECISION_GATE_ENABLED = os.getenv("PRED_PRECISION_GATE_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# 외부표본 precision 기준을 통과하기 전에는 high/mid 배지를 내보내지 않는다.
# 랭킹 watchlist는 유지된다.
PRED_CONFIDENCE_OUTPUT_ENABLED = os.getenv(
    "PRED_CONFIDENCE_OUTPUT_ENABLED", "1"
).strip().lower() in ("1", "true", "yes", "on")
PRED_PRECISION_MAX_HIGH = _positive_int_env("PRED_PRECISION_MAX_HIGH", 3)
PRED_PRECISION_MIN_CONVICTION = _float_env("PRED_PRECISION_MIN_CONVICTION", 0.42)
PRED_PRECISION_MIN_PILLARS = _positive_int_env("PRED_PRECISION_MIN_PILLARS", 1)
PRED_PRECISION_MAX_RANK = _positive_int_env("PRED_PRECISION_MAX_RANK", 15)
PRED_PRECISION_ML_FLOOR = _float_env("PRED_PRECISION_ML_FLOOR", 0.06)
# 고확신 전용: 전일 수익률 ≥ 이 값이면 high 제외(일반 과열 차단보다 엄격)
PRED_HIGH_EXHAUSTION_BLOCK_RET = _float_env("PRED_HIGH_EXHAUSTION_BLOCK_RET", 0.12)
# 비과열 후보 풀 최고 ML 대비 상대 하한·절대 하한
PRED_HIGH_RELATIVE_ML = _float_env("PRED_HIGH_RELATIVE_ML", 0.48)
PRED_HIGH_ABS_ML_FLOOR = _float_env("PRED_HIGH_ABS_ML_FLOOR", 0.28)
PRED_HIGH_SELECT_FLOOR = _float_env("PRED_HIGH_SELECT_FLOOR", 0.32)
PRED_PRECISION_BUCKET_MIN_SAMPLES = _positive_int_env("PRED_PRECISION_BUCKET_MIN_SAMPLES", 8)
PRED_PRECISION_BUCKET_MIN_RATIO = _float_env("PRED_PRECISION_BUCKET_MIN_RATIO", 0.28)
PRED_PRECISION_CODE_MIN_TRIES = _positive_int_env("PRED_PRECISION_CODE_MIN_TRIES", 5)
PRED_PRECISION_CODE_MIN_HIT_RATE = _float_env("PRED_PRECISION_CODE_MIN_HIT_RATE", 0.12)
# True: 고확신 이력 표본 부족 종목은 high 슬롯 불가(미검증 승격 차단)
PRED_PRECISION_CODE_FAIL_CLOSED = os.getenv(
    "PRED_PRECISION_CODE_FAIL_CLOSED", "1"
).strip().lower() in ("1", "true", "yes", "on")
# forward 슬레이트 패딩: miss streak·tightness 조건 미달 시 mid 승격 금지
PRED_FORWARD_SLATE_PAD_MIN_TIGHTNESS = _float_env(
    "PRED_FORWARD_SLATE_PAD_MIN_TIGHTNESS", 0.85
)
PRED_FORWARD_SLATE_PAD_MAX_MISS_STREAK = _non_negative_int_env(
    "PRED_FORWARD_SLATE_PAD_MAX_MISS_STREAK", 0
)
# N일 → N+1일 실전 확신은 보정확률·최종 순위·뉴스 근거를 모두 통과해야 한다.
# 통과자가 없으면 빈 슬롯을 허용한다(강제 high/mid 금지).
# 모집단 보정 후 상위 확률도 대개 수 %대다. 0.30 같은 구버전 하한은 전부 기각한다.
PRED_FORWARD_HIGH_CALIBRATED_MIN = _float_env(
    "PRED_FORWARD_HIGH_CALIBRATED_MIN", 0.04
)
# 고확신: 보정 ML 절대·상대 하한(``ml_rank_score`` 가 아닌 ``ml_prob`` 기준)
PRED_HIGH_CALIBRATED_ABS_MIN = _float_env("PRED_HIGH_CALIBRATED_ABS_MIN", 0.06)
PRED_HIGH_CALIBRATED_RELATIVE = _float_env("PRED_HIGH_CALIBRATED_RELATIVE", 0.72)
# 뉴스·언급·맥락 합성 근거(0~1). 이 값 미만이면 high 불가.
PRED_HIGH_NEWS_EVIDENCE_MIN = _float_env("PRED_HIGH_NEWS_EVIDENCE_MIN", 0.55)
PRED_FORWARD_HIGH_MAX_RANK = _positive_int_env("PRED_FORWARD_HIGH_MAX_RANK", 5)
PRED_FORWARD_HIGH_RELATIVE_PRECISION = _float_env(
    "PRED_FORWARD_HIGH_RELATIVE_PRECISION", 0.85
)
PRED_FORWARD_HIGH_MIN_KEYWORD_HITS = _positive_int_env(
    "PRED_FORWARD_HIGH_MIN_KEYWORD_HITS", 1
)
PRED_FORWARD_MID_ENABLED = os.getenv("PRED_FORWARD_MID_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PRED_FORWARD_MID_CALIBRATED_MIN = _float_env(
    "PRED_FORWARD_MID_CALIBRATED_MIN", 0.02
)
PRED_FORWARD_MID_MAX_RANK = _positive_int_env("PRED_FORWARD_MID_MAX_RANK", 12)

# --- 랭킹 우선 예측(구조적 정확도 개선) ---
# 1: ML 확률·순위 기반, pred_high=확신구간 / 0: 레거시(표시%≥20%)
PRED_RANKING_MODE = os.getenv("PRED_RANKING_MODE", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# 1: 상위 N개를 20~30% 구간에 일괄 매핑(기본 OFF — 랭킹 모드와 분리)
PRED_USE_DISPLAY_RANK_MAPPING = os.getenv("PRED_USE_DISPLAY_RANK_MAPPING", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PRED_RANK_POOL_N = _positive_int_env("PRED_RANK_POOL_N", 80)
# ML 하이브리드 점수 1차 컷 전 finalize(캐리오버·섹터 재랭킹)에 넣을 후보 수
PRED_ML_FINALIZE_POOL_N = _positive_int_env("PRED_ML_FINALIZE_POOL_N", 220)
# ML predict_proba 대상 상한(후보 리콜↑ — 정확도 우선)
PRED_ML_SCORE_POOL_CAP = _positive_int_env("PRED_ML_SCORE_POOL_CAP", 650)
# 확률 산출 후 PredictionRow 풀구성·finalize에 쓸 상한
PRED_ML_ENRICH_TOP_N = _positive_int_env("PRED_ML_ENRICH_TOP_N", 280)
PRED_ML_FEAT_WORKERS = _positive_int_env("PRED_ML_FEAT_WORKERS", 12)
# 휴리스틱·ML 후보: 종목 관련(비범용) 키워드 교집합 최소 개수
PRED_MIN_KEYWORD_HITS = _positive_int_env("PRED_MIN_KEYWORD_HITS", 2)
# ML ``predict_proba`` 대상 풀만 완화(고확신 게이트는 ``PRED_MIN_KEYWORD_HITS`` 유지)
PRED_ML_POOL_MIN_KEYWORD_HITS = _positive_int_env("PRED_ML_POOL_MIN_KEYWORD_HITS", 1)
# 1: 리포트·freeze 예측 후보는 early 뉴스(키워드·종목명·TF-IDF 맥락)가 있어야 한다.
# 모멘텀·수급·업종 must-keep 만으로는 표를 채우지 않는다.
PRED_REQUIRE_NEWS_EVIDENCE = os.getenv(
    "PRED_REQUIRE_NEWS_EVIDENCE", "1"
).strip().lower() in ("1", "true", "yes", "on")
# 종목명 직접 언급 없을 때 후보 통과에 필요한 mention 점수(0~1, 5회 이상=1.0)
PRED_MENTION_GATE_MIN = _float_env("PRED_MENTION_GATE_MIN", 0.35)
# 누적 오차로 반복 오탐 종목을 후보에서 제외(종목명 직접 언급 없을 때만)
PRED_CHRONIC_MISS_BLOCK_ENABLED = os.getenv(
    "PRED_CHRONIC_MISS_BLOCK_ENABLED", "1"
).strip().lower() in ("1", "true", "yes", "on")
PRED_CHRONIC_MISS_MIN_SAMPLES = _positive_int_env("PRED_CHRONIC_MISS_MIN_SAMPLES", 10)
PRED_CHRONIC_MISS_RATIO_MAX = _float_env("PRED_CHRONIC_MISS_RATIO_MAX", 0.22)
# 전일 급등·상한가 직후 익일 추격 진입 억제 (7/2 테마 소진 오판 방지)
PRED_PRIOR_DAY_EXHAUSTION_ENABLED = os.getenv(
    "PRED_PRIOR_DAY_EXHAUSTION_ENABLED", "1"
).strip().lower() in ("1", "true", "yes", "on")
PRED_PRIOR_DAY_EXHAUSTION_WARN_RET = _float_env("PRED_PRIOR_DAY_EXHAUSTION_WARN_RET", 0.08)
PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET = _float_env(
    "PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET", 0.18
)
# 전일 업종 급등 테마 캐리오버 리랭크 (연속 테마일만 켜기)
PRED_CARRYOVER_INDUSTRY_RERANK_ENABLED = os.getenv(
    "PRED_CARRYOVER_INDUSTRY_RERANK_ENABLED", "0"
).strip().lower() in ("1", "true", "yes", "on")
PRED_CARRYOVER_MIN_HEAT = _float_env("PRED_CARRYOVER_MIN_HEAT", 0.34)
PRED_CARRYOVER_FOCUS_MODE = os.getenv("PRED_CARRYOVER_FOCUS_MODE", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PRED_CARRYOVER_FOCUS_MIN_RET = _float_env("PRED_CARRYOVER_FOCUS_MIN_RET", 0.15)
PRED_CARRYOVER_FOCUS_MIN_COUNT = _positive_int_env("PRED_CARRYOVER_FOCUS_MIN_COUNT", 2)
PRED_CARRYOVER_FOCUS_TOP_K = _positive_int_env("PRED_CARRYOVER_FOCUS_TOP_K", 4)
PRED_PRIOR_DAY_DROP_WARN_RET = _float_env("PRED_PRIOR_DAY_DROP_WARN_RET", -0.03)
PRED_PRIOR_DAY_DROP_BLOCK_RET = _float_env("PRED_PRIOR_DAY_DROP_BLOCK_RET", -0.07)
PRED_SECTOR_BURST_PRIOR_MAX = _float_env("PRED_SECTOR_BURST_PRIOR_MAX", 0.24)
PRED_SECTOR_BURST_LIM_MIN = _float_env("PRED_SECTOR_BURST_LIM_MIN", 0.26)
PRED_SECTOR_BURST_LIM_W = _float_env("PRED_SECTOR_BURST_LIM_W", 0.10)
PRED_SECTOR_BURST_BREADTH_W = _float_env("PRED_SECTOR_BURST_BREADTH_W", 0.08)
PRED_SECTOR_BURST_THEME_W = _float_env("PRED_SECTOR_BURST_THEME_W", 0.06)
PRED_SECTOR_BURST_BREADTH_EXTRA = _float_env("PRED_SECTOR_BURST_BREADTH_EXTRA", 0.05)
PRED_SECTOR_BURST_MAX = _float_env("PRED_SECTOR_BURST_MAX", 0.28)
PRED_SECTOR_BURST_RANK_MIN = _float_env("PRED_SECTOR_BURST_RANK_MIN", 0.10)
PRED_BURST_INDUSTRY_LEADER_SLOTS = _positive_int_env("PRED_BURST_INDUSTRY_LEADER_SLOTS", 4)
PRED_BURST_INDUSTRY_MIN_SCORE = _float_env("PRED_BURST_INDUSTRY_MIN_SCORE", 0.07)
PRED_OBS_BURST_LIM_MIN = _float_env("PRED_OBS_BURST_LIM_MIN", 0.27)
PRED_OBS_BURST_PRIOR_MAX = _float_env("PRED_OBS_BURST_PRIOR_MAX", 0.25)
PRED_OBS_BURST_BREADTH_MIN = _float_env("PRED_OBS_BURST_BREADTH_MIN", 0.10)
PRED_OBS_BURST_MOM_MIN = _float_env("PRED_OBS_BURST_MOM_MIN", 0.16)
PRED_OBS_BURST_PEERS_PER_INDUSTRY = _positive_int_env("PRED_OBS_BURST_PEERS_PER_INDUSTRY", 52)
PRED_OBS_BURST_PEERS_HOT = _positive_int_env("PRED_OBS_BURST_PEERS_HOT", 78)
PRED_OBS_BURST_MAX_INDUSTRIES = _positive_int_env("PRED_OBS_BURST_MAX_INDUSTRIES", 12)
PRED_OBS_BURST_COLD_PRIOR_MAX = _float_env("PRED_OBS_BURST_COLD_PRIOR_MAX", 0.16)
PRED_OBS_BURST_COLD_LIM_MIN = _float_env("PRED_OBS_BURST_COLD_LIM_MIN", 0.30)
PRED_OBS_BURST_PEERS_FULL = _positive_int_env("PRED_OBS_BURST_PEERS_FULL", 80)
PRED_OBS_BURST_COLD_SCORE_BOOST = _float_env("PRED_OBS_BURST_COLD_SCORE_BOOST", 0.38)
PRED_OBS_BURST_COLD_INDUSTRY_MAX = _positive_int_env("PRED_OBS_BURST_COLD_INDUSTRY_MAX", 8)
PRED_PRIORITY_COLD_MAX_INDUSTRIES = _positive_int_env("PRED_PRIORITY_COLD_MAX_INDUSTRIES", 6)
PRED_PRIORITY_COLD_PEERS = _positive_int_env("PRED_PRIORITY_COLD_PEERS", 36)
PRED_PRIORITY_COLD_LIM_MIN = _float_env("PRED_PRIORITY_COLD_LIM_MIN", 0.34)
PRED_PRIORITY_COLD_SCORE_BOOST = _float_env("PRED_PRIORITY_COLD_SCORE_BOOST", 0.30)
PRED_PRIORITY_COLD_LIM_HIGH = _float_env("PRED_PRIORITY_COLD_LIM_HIGH", 0.48)
PRED_PRIORITY_COLD_BREADTH_MIN = _float_env("PRED_PRIORITY_COLD_BREADTH_MIN", 0.08)
# 정밀 게이트 탈락 시 섹터 열기로 고확신 승격(리콜 구제) — 기본 끔
PRED_SECTOR_RESCUE_HIGH_ENABLED = os.getenv(
    "PRED_SECTOR_RESCUE_HIGH_ENABLED", "0"
).strip().lower() in ("1", "true", "yes", "on")
PRED_OUTPUT_MAX = _positive_int_env("PRED_OUTPUT_MAX", 4)
PRED_MID_OUTPUT_MAX = _positive_int_env("PRED_MID_OUTPUT_MAX", 5)
# 예측 전용일 표 노출 상한: 고확신(~3)+중확신(~5). 게이트 0건이면 순위 watchlist 로 채움(20분 검토).
PRED_FORWARD_SHOW_MAX = _positive_int_env("PRED_FORWARD_SHOW_MAX", 8)
# 고/중 확신: ML 급등 확률 하한(순위 상위 슬롯에만 적용)
PRED_ML_HIGH_CONFIDENCE_PROB = max(0.08, _float_env("PRED_ML_HIGH_CONFIDENCE_PROB", 0.11))
PRED_ML_MID_CONFIDENCE_PROB = max(0.04, _float_env("PRED_ML_MID_CONFIDENCE_PROB", 0.07))
PRED_ML_MIN_OUTPUT_PROB = _float_env("PRED_ML_MIN_OUTPUT_PROB", 0.025)
# 고확신: 당일 키워드 교집합·종목명 언급 최소(ML 과대확률 오탐 억제)
PRED_ML_HIGH_MIN_KEYWORD_HITS = _positive_int_env("PRED_ML_HIGH_MIN_KEYWORD_HITS", 2)
# 고확신: 풀 1위 ML 확률 대비 상대 하한(0.55 = 1위의 55% 미만이면 고확신 제외)
PRED_ML_HIGH_RELATIVE_PROB = _float_env("PRED_ML_HIGH_RELATIVE_PROB", 0.65)
PRED_HEURISTIC_HIGH_RANK_MAX = _positive_int_env("PRED_HEURISTIC_HIGH_RANK_MAX", 0)
PRED_HEURISTIC_MID_RANK_MAX = _positive_int_env("PRED_HEURISTIC_MID_RANK_MAX", 0)
PRED_HEURISTIC_MIN_SCORE = _float_env("PRED_HEURISTIC_MIN_SCORE", 3.5)
PRED_REGIME_GATE_ENABLED = os.getenv("PRED_REGIME_GATE_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# KOSPI 전일 수익률(소수)이 이 값 미만이면 추천 수·확신 임계를 보수적으로 조정
PRED_REGIME_KS11_SOFT_MIN = _float_env("PRED_REGIME_KS11_SOFT_MIN", -0.025)
PRED_REGIME_OUTPUT_SCALE = _float_env("PRED_REGIME_OUTPUT_SCALE", 0.55)
# Hit@K 평가에 쓸 K 목록(쉼표 구분 환경 변수 ``PRED_EVAL_HIT_AT_K=5,10,20,40``)
_hit_k_raw = os.getenv("PRED_EVAL_HIT_AT_K", "5,10,20,40").strip()
PRED_EVAL_HIT_AT_K: tuple[int, ...] = tuple(
    sorted(
        {
            int(x.strip())
            for x in _hit_k_raw.split(",")
            if x.strip().isdigit() and int(x.strip()) >= 1
        }
    )
) or (5, 10, 20, 40)
TRAIN_START_DEFAULT = _date_env("TRAIN_START", date(2025, 4, 11))  # 급등–뉴스 이벤트·스냅샷 수집 시작
# ML 랭커 학습: 관측일 T 직전 최근 N거래일만 사용(0=전구간). 표본·시간 폭증 방지.
ML_TRAIN_LOOKBACK_DAYS = _non_negative_int_env("ML_TRAIN_LOOKBACK_DAYS", 150)
# 캐시 미스 시 일일 추론용 경량 학습(``--rebuild-train-snapshot`` 제외). 기본 끔 → 정확도 우선.
ML_TRAIN_FAST_ON_CACHE_MISS = os.getenv("ML_TRAIN_FAST_ON_CACHE_MISS", "0").strip().lower() in (
    "1",
    "true",
    "True",
    "yes",
)
ML_TRAIN_LOOKBACK_DAYS_FAST = _non_negative_int_env("ML_TRAIN_LOOKBACK_DAYS_FAST", 28)
# 일별 음성 샘플 상한·miss 부스트(학습 시간 단축)
ML_TRAIN_MAX_NEG_PER_DAY = _positive_int_env("ML_TRAIN_MAX_NEG_PER_DAY", 160)
ML_TRAIN_MAX_NEG_PER_DAY_FAST = _positive_int_env("ML_TRAIN_MAX_NEG_PER_DAY_FAST", 40)
ML_TRAIN_DAY_CANDIDATE_CAP = _positive_int_env("ML_TRAIN_DAY_CANDIDATE_CAP", 110)
ML_USE_NEWS_CONTEXT_FEATURES = os.getenv(
    "ML_USE_NEWS_CONTEXT_FEATURES", "0"
).strip().lower() in ("1", "true", "yes", "on")
ML_MISS_BOOST_MAX_KEYS = _positive_int_env("ML_MISS_BOOST_MAX_KEYS", 320)
ML_MISS_BOOST_DUP = _positive_int_env("ML_MISS_BOOST_DUP", 2)
ML_MISS_BOOST_MAX_KEYS_FAST = _positive_int_env("ML_MISS_BOOST_MAX_KEYS_FAST", 48)
ML_MISS_BOOST_DUP_FAST = _positive_int_env("ML_MISS_BOOST_DUP_FAST", 0)
# 파이프라인 기본: 전체 학습(정확도). 속도가 필요하면 환경변수로 1.
ML_PIPELINE_ALWAYS_FAST_TRAIN = os.getenv("ML_PIPELINE_ALWAYS_FAST_TRAIN", "0").strip().lower() in (
    "1",
    "true",
    "True",
    "yes",
)
# 직전 거래일 모델 재사용(학습 생략). 기본 끔 → 관측일 T 맞춤 재학습.
# 장중 라이브(예측 전용)는 ``ML_REUSE_PRIOR_FOR_FORWARD=1`` 로 강제 재사용.
ML_REUSE_PRIOR_TRADING_DAY_MODEL = os.getenv("ML_REUSE_PRIOR_TRADING_DAY_MODEL", "0").strip().lower() in (
    "1",
    "true",
    "True",
    "yes",
)
# 예측 전용(미래·당일 장중) 관측일: 직전 가용 joblib 재사용(14:30 창 필수). 기본 ON.
ML_REUSE_PRIOR_FOR_FORWARD = os.getenv("ML_REUSE_PRIOR_FOR_FORWARD", "1").strip().lower() in (
    "1",
    "true",
    "True",
    "yes",
)
# 월간 HTML에 남은 「장후 미갱신 예측전용」일자 자동 재처리.
# 장중·예측전용 실행에서는 끄세요(수십 분 소요). 장후 백필 때 1.
PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD = os.getenv(
    "PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD", "1"
).strip().lower() in ("1", "true", "True", "yes")
# 분류 확률 vs 수익률 회귀 점수 혼합(합=1 권장). Hit@K 정렬에 회귀 신호 반영.
ML_RANK_BLEND_CLF = _float_env("ML_RANK_BLEND_CLF", 0.55)
ML_RANK_BLEND_REG = _float_env("ML_RANK_BLEND_REG", 0.45)
# --weekly 등 날짜 인자 없을 때 관측(리포트) 시작일. 학습 라벨 상한은 관측일 T 직전(워크포워드).
TEST_START = date(2026, 1, 1)

# KRX 정규 휴장 외 임시 휴장(``exchange_calendars`` XKRX 미반영). ``trading_calendar``·OHLCV 보강에 사용.
KRX_AD_HOC_SESSION_CLOSURES: tuple[date, ...] = (
    date(2026, 6, 3),  # 제9회 전국동시지방선거
    date(2026, 7, 17),  # 제헌절(2026~ 법정 공휴일·증시 휴장)
)

# 예측 시나리오: N거래일 15:30 장 마감 후 완성 일봉·뉴스로 N+1일 급등을 예측한다.
# (N = T 직전 거래일, T = 수익률 관측일)
USE_DECISION_NEWS_INTRADAY_CUTOFF = os.getenv("USE_DECISION_NEWS_INTRADAY_CUTOFF", "1").strip() in (
    "1",
    "true",
    "True",
    "yes",
)
NEWS_CUTOFF_KST_HOUR = 15
NEWS_CUTOFF_KST_MINUTE = 30

# HTML 리포트에 포함할 테스트 거래일(이 범위가 설정되면 MAX_TEST_DAYS는 무시)
REPORT_TEST_DAY_START = date(2026, 1, 2)
REPORT_TEST_DAY_END = date(2026, 4, 10)

#NAVER_NEWS_URL = "https://openapi.naver.com/v1/search/news.json"
NAVER_NEWS_URL = "https://openapi.naver.com/v1/search/news.json"

# 네이버 개발자 API가 없을 때 Google News RSS(키 불필요)로 일자별 뉴스 수집. 0이면 뉴스 생략.
USE_GOOGLE_NEWS_RSS_FALLBACK = os.getenv("USE_GOOGLE_NEWS_RSS_FALLBACK", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# 뉴스 검색 쿼리(하루치를 넓게 수집)
NEWS_QUERY_SEEDS = [
    "코스피",
    "코스닥",
    "증시",
    "한국거래소",
    "KOSPI",
    "KOSDAQ",
    "상장사",
    "공시",
]

MIN_NEWS_CHARS = 400  # 하루 합산 본문·제목 최소 길이(미만이면 경고)

# Google News RSS 전용: 네이버 시드 외 추가 키워드(날짜 문자열과 결합해 쿼리 수 확장).
GOOGLE_NEWS_RSS_QUERY_SEEDS_EXTRA = [
    "금융",
    "증권",
    "주식시장",
    "국내증시",
    "코스피지수",
    "코스닥지수",
    "선물옵션",
    "외국인",
    "기관",
    "환율",
    "금리",
    "한국은행",
    "실적",
    "분기실적",
    "배당",
    "반도체",
    "바이오",
    "2차전지",
    "전기차",
    "AI",
    "미국증시",
    "나스닥",
    "다우존스",
    "뉴욕증시",
    "상장",
    "공시",
    "IR",
]

# Google RSS: 동일 쿼리를 영문 에디션(ceid=KR:en)에서 한 번 더 호출해 결과 다양화(요청 약 2배).
GOOGLE_NEWS_RSS_DUAL_LOCALE = os.getenv("GOOGLE_NEWS_RSS_DUAL_LOCALE", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Google RSS: 쿼리(로케일 묶음) 사이 대기(초). 너무 낮추면 차단 위험.
GOOGLE_NEWS_RSS_QUERY_SLEEP_SEC = max(
    0.03,
    _float_env("GOOGLE_NEWS_RSS_QUERY_SLEEP_SEC", 0.09),
)

# 전종목 OHLCV 병렬 다운로드 스레드 수(기본 12). 낮추면 API·회선 부담 감소.
OHLCV_MAX_WORKERS = _positive_int_env("OHLCV_MAX_WORKERS", 12)

# 1이면 리포트 비교 표에 네이버 종목 공시 HTTP 조회를 생략(캐시만 사용·미스 시 빈 목록).
REPORT_SKIP_DISCLOSURE_FETCH = os.getenv("REPORT_SKIP_DISCLOSURE_FETCH", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# 리포트 「당일 테마 요약」 길이(0이면 해당 블록 생략)
REPORT_THEME_SUMMARY_MAX_SECTORS = _positive_int_env("REPORT_THEME_SUMMARY_MAX_SECTORS", 4)
REPORT_THEME_SUMMARY_MAX_STOCKS_PER_SECTOR = _positive_int_env(
    "REPORT_THEME_SUMMARY_MAX_STOCKS_PER_SECTOR", 3
)
REPORT_THEME_SUMMARY_MAX_NARRATIVES = _positive_int_env("REPORT_THEME_SUMMARY_MAX_NARRATIVES", 0)
REPORT_THEME_SUMMARY_MAX_SEED_DETAILS = _positive_int_env("REPORT_THEME_SUMMARY_MAX_SEED_DETAILS", 0)
REPORT_THEME_SUMMARY_MAX_LEADERS_25 = _positive_int_env("REPORT_THEME_SUMMARY_MAX_LEADERS_25", 0)
REPORT_THEME_SUMMARY_SHOW_EXTRA_KW = os.getenv(
    "REPORT_THEME_SUMMARY_SHOW_EXTRA_KW", "0"
).strip().lower() in ("1", "true", "yes", "on")

# 장 마감 확정일 「왜 올랐나」 상한가·20%↑ 한 줄 요약
REPORT_MOVER_RATIONALE_ENABLED = os.getenv(
    "REPORT_MOVER_RATIONALE_ENABLED", "1"
).strip().lower() in ("1", "true", "yes", "on")
REPORT_MOVER_RATIONALE_MAX_LIMIT_UP = _positive_int_env(
    "REPORT_MOVER_RATIONALE_MAX_LIMIT_UP", 25
)
REPORT_MOVER_RATIONALE_MAX_BIG = _positive_int_env("REPORT_MOVER_RATIONALE_MAX_BIG", 15)
# 네이버 API 한도(429)가 나오면 1~2로 낮추세요.
NEWS_FETCH_MAX_WORKERS = _positive_int_env("NEWS_FETCH_MAX_WORKERS", 4)

# 네이버 뉴스 검색 방식. API에 dateFrom/dateTo 파라미터 없음 → pubDate로 해당 일만 남김.
# market: 일자+증시 키워드(기본).
# ticker: 종목마다 ``YYYY년 M월 D일 {종목명}`` 쿼리 후 합침.
# both: market + ticker 를 동시에 수집·병합해 학습/추론 입력으로 사용.
_NM = os.getenv("NEWS_NAVER_QUERY_MODE", "both").strip().lower()
NEWS_NAVER_QUERY_MODE = _NM if _NM in ("market", "ticker", "both") else "market"
# ticker 모드: 하루치에서 종목 쿼리 병렬 수·종목당 최대 API 페이지(100건/페이지).
# 네이버 뉴스 검색은 start 최대 1000 → 페이지 상한 10.
NEWS_TICKER_NAVER_MAX_WORKERS = _positive_int_env("NEWS_TICKER_NAVER_MAX_WORKERS", 12)
NEWS_TICKER_NAVER_MAX_PAGES = min(10, _positive_int_env("NEWS_TICKER_NAVER_MAX_PAGES", 10))

# market / both 의 시장 쿼리: 쿼리마다 페이징 깊이(1..10). 기본 10이면 API가 허용하는 최대 건수에 가깝게 수집.
NEWS_NAVER_MARKET_MAX_PAGES = min(10, _positive_int_env("NEWS_NAVER_MARKET_MAX_PAGES", 10))

# 네이버 시장(news) 쿼리 확장 시드. 25,000건/일 호출 한도를 적극 활용하려면 늘리세요.
NEWS_NAVER_MARKET_QUERY_SEEDS_EXTRA = [
    "주가",
    "급등",
    "상한가",
    "거래량",
    "테마주",
    "2차전지",
    "반도체",
    "바이오",
    "AI",
    "로봇",
    "원전",
    "환율",
    "금리",
    "한국은행",
    "연준",
    "실적",
    "공시",
    "유상증자",
    "무상증자",
    "합병",
    "IPO",
]

# 최근 오판 기반 키워드 가중치 피드백(자동 학습)
KEYWORD_FEEDBACK_ENABLED = os.getenv("KEYWORD_FEEDBACK_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
KEYWORD_FEEDBACK_SCORE_SCALE = _float_env("KEYWORD_FEEDBACK_SCORE_SCALE", 1.1)
KEYWORD_FEEDBACK_DECAY = _float_env("KEYWORD_FEEDBACK_DECAY", 0.02)
KEYWORD_FEEDBACK_STEP = _float_env("KEYWORD_FEEDBACK_STEP", 0.06)

# --- 적응형 오판 반성 루프 (feedback_loop) ---
PRED_FEEDBACK_ADAPTIVE_ENABLED = os.getenv(
    "PRED_FEEDBACK_ADAPTIVE_ENABLED", "1"
).strip().lower() in ("1", "true", "yes", "on")
PRED_FEEDBACK_RECENCY_HALF_LIFE_DAYS = _float_env(
    "PRED_FEEDBACK_RECENCY_HALF_LIFE_DAYS", 5.0
)
PRED_FEEDBACK_RECENCY_WINDOW_DAYS = _positive_int_env(
    "PRED_FEEDBACK_RECENCY_WINDOW_DAYS", 15
)
PRED_FEEDBACK_RANK_PENALTY_MAX = _float_env("PRED_FEEDBACK_RANK_PENALTY_MAX", 0.38)
PRED_FEEDBACK_TIGHTNESS_FLOOR = _float_env("PRED_FEEDBACK_TIGHTNESS_FLOOR", 0.50)
PRED_FEEDBACK_STREAK_HIT_MIN = _float_env("PRED_FEEDBACK_STREAK_HIT_MIN", 0.12)
PRED_FEEDBACK_MISS_STREAK_MAX_DAYS = _positive_int_env(
    "PRED_FEEDBACK_MISS_STREAK_MAX_DAYS", 8
)
# 최근 N일·M회 오판 종목 빠른 차단(종목명 직접 언급 없을 때)
PRED_CHRONIC_MISS_RECENT_MIN_SAMPLES = _positive_int_env(
    "PRED_CHRONIC_MISS_RECENT_MIN_SAMPLES", 2
)
PRED_CHRONIC_MISS_RECENT_RATIO_MAX = _float_env(
    "PRED_CHRONIC_MISS_RECENT_RATIO_MAX", 0.18
)
# ML 증분 miss 부스트(스냅샷 rebuild 없이 전일 오판 반영)
ML_INCREMENTAL_MISS_BOOST_ENABLED = os.getenv(
    "ML_INCREMENTAL_MISS_BOOST_ENABLED", "1"
).strip().lower() in ("1", "true", "yes", "on")
ML_INCREMENTAL_MISS_MAX_ENTRIES = _positive_int_env(
    "ML_INCREMENTAL_MISS_MAX_ENTRIES", 480
)

# --- 스케줄 리포트 이메일 (scripts/run_daily_email.py) ---
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
EMAIL_SMTP_HOST = _env_str("EMAIL_SMTP_HOST", "smtp.gmail.com")
EMAIL_SMTP_PORT = _positive_int_env("EMAIL_SMTP_PORT", 587)
EMAIL_SMTP_USER = _env_str("EMAIL_SMTP_USER")
EMAIL_SMTP_PASSWORD = _env_str("EMAIL_SMTP_PASSWORD")
EMAIL_FROM = _env_str("EMAIL_FROM")
EMAIL_RECIPIENTS_RAW = _env_str(
    "EMAIL_RECIPIENTS",
    "mohican0607@gmail.com",
)
EMAIL_USE_TLS = os.getenv("EMAIL_USE_TLS", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
EMAIL_SMTP_TIMEOUT_SEC = _positive_int_env("EMAIL_SMTP_TIMEOUT_SEC", 60)
EMAIL_SUBJECT_PREFIX = _env_str("EMAIL_SUBJECT_PREFIX", "[Money]")
# 거래일 자동 스케줄(14:30/15:30/16:00) 슬롯별 실행 — 0·N·false 이면 건너뜀(기본 1).
RUN_DAILY_AUTO_1430 = run_daily_auto_enabled("1430")
RUN_DAILY_AUTO_1530 = run_daily_auto_enabled("1530")
RUN_DAILY_AUTO_1600 = run_daily_auto_enabled("1600")
# scripts/run_daily_email.py — main.py 최대 대기(초). 초과 시 프로세스 종료 + 실패 메일.
RUN_DAILY_MAIN_TIMEOUT_SEC = _positive_int_env("RUN_DAILY_MAIN_TIMEOUT_SEC", 5400)
