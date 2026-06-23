"""Pipeline datatypes."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from src import report


@dataclass
class PipelineOut:
    """
    ``_run_pipeline`` 한 번 호출의 집계 결과.

    월간 HTML 메타(훈련 구간, 뉴스 출처, 상관 키워드)와 late-뉴스 프로브 카운터,
    pykrx 실패 시 리포트 각주 문구를 담습니다.
    """

    day_reports: list[report.DayReport]
    news_source: str
    correlation_rows: list[tuple[str, int]]
    train_start: date
    test_start: date
    end_date: date
    late_below_n: int
    late_below_kw: int
    late_gte_n: int
    late_gte_kw: int
    movers_data_note: str | None = None
    returns_df: Any = None
    news_by_calendar: dict | None = None
    listing_names: dict[str, str] | None = None
