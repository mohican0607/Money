"""
경로·상수·환경 변수.

이 모듈은 함수 없이 프로젝트 전역 설정만 노출합니다.
- ``ROOT`` / ``DATA_DIR`` / ``CACHE_DIR`` / ``OUTPUT_DIR``: 디렉터리 루트(``load_dotenv()`` 후 계산).
- 네이버·뉴스·시세 관련 플래그는 ``os.getenv`` 기반이며, 실행 전 ``.env`` 또는 환경 변수로 덮어씁니다.
- 날짜 상수(``TRAIN_START_DEFAULT``, ``TEST_START``, ``REPORT_TEST_DAY_*``)는 훈련·테스트·월간 리포트 범위를 고정합니다.

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
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "cache"
OUTPUT_DIR = ROOT / "output"
# ``python main.py YYYYMMDD`` / 일일 N 실행 시 기준일별 블록을 한 파일에 누적·같은 N 재실행 시 해당 블록만 갱신.
REPORT_DATED_ROLLUP_HTML = OUTPUT_DIR / "report_dated_by_n.html"
# 과거 급등–뉴스 ``BreakoutEvent`` 학습 스냅샷(JSON). ``main.py`` 기본이 로드·증분 병합, ``--no-train-snapshot`` 로 끔.
TRAIN_SNAPSHOT_PATH = CACHE_DIR / "train" / "breakout_train_snapshot.json"

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
# 감독학습 랭커(scikit-learn HistGradientBoosting)로 후보 종목 순위를 확률 기반 정렬(0이면 휴리스틱만).
PRED_USE_ML_RANKER = os.getenv("PRED_USE_ML_RANKER", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# 캘리브레이션 후 최종 예측 수익률(소수) 클램프 범위 (BIG_MOVE 20%와 맞춤)
PRED_RETURN_MIN = _float_env("PRED_RETURN_MIN", 0.20)
#PRED_RETURN_MAX = _float_env("PRED_RETURN_MAX", 0.35)
PRED_RETURN_MAX = _float_env("PRED_RETURN_MAX", 0.30)
TRAIN_START_DEFAULT = date(2025, 4, 11)  # 약 1년 전(실행일 기준 조정은 main에서)
TEST_START = date(2026, 1, 1)

# 매수 시나리오: N거래일 장마감 전(약 14:00~14:50)에 주문해 N+1일 급등을 노릴 때,
# 예측·훈련에 쓰는 뉴스는 'N-1 거래일' 15:00(KST)까지로 제한한다. (N = T 직전 거래일, T = 수익률 관측일)
USE_DECISION_NEWS_INTRADAY_CUTOFF = os.getenv("USE_DECISION_NEWS_INTRADAY_CUTOFF", "1").strip() in (
    "1",
    "true",
    "True",
    "yes",
)
NEWS_CUTOFF_KST_HOUR = 15
NEWS_CUTOFF_KST_MINUTE = 0

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

# 캘린더 **일자별** 뉴스 캐시를 채울 때 동시에 처리할 일 수(기본 4). 1이면 기존처럼 순차.
# 네이버 API 한도(429)가 나오면 1~2로 낮추세요.
NEWS_FETCH_MAX_WORKERS = _positive_int_env("NEWS_FETCH_MAX_WORKERS", 4)

# 네이버 뉴스 검색 방식. API에 dateFrom/dateTo 파라미터 없음 → pubDate로 해당 일만 남김.
# market: 일자+증시 키워드(기본).
# ticker: 종목마다 ``YYYY년 M월 D일 {종목명}`` 쿼리 후 합침.
# both: market + ticker 를 동시에 수집·병합해 학습/추론 입력으로 사용.
_NM = os.getenv("NEWS_NAVER_QUERY_MODE", "market").strip().lower()
NEWS_NAVER_QUERY_MODE = _NM if _NM in ("market", "ticker", "both") else "market"
# ticker 모드: 하루치에서 종목 쿼리 병렬 수·종목당 최대 API 페이지(100건/페이지).
# 네이버 뉴스 검색은 start 최대 1000 → 페이지 상한 10.
NEWS_TICKER_NAVER_MAX_WORKERS = _positive_int_env("NEWS_TICKER_NAVER_MAX_WORKERS", 8)
NEWS_TICKER_NAVER_MAX_PAGES = min(10, _positive_int_env("NEWS_TICKER_NAVER_MAX_PAGES", 5))

# market / both 의 시장 쿼리: 쿼리마다 페이징 깊이(1..10). 기본 10이면 API가 허용하는 최대 건수에 가깝게 수집.
NEWS_NAVER_MARKET_MAX_PAGES = min(10, _positive_int_env("NEWS_NAVER_MARKET_MAX_PAGES", 10))
