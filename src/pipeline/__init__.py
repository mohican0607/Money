"""
파이프라인 오케스트레이션 — ``main.py`` 에서 분리.

- ``run.run_pipeline``: OHLCV·뉴스·예측·비교 일괄 실행
- ``batch.render_monthly_batch``: 월간 HTML
- ``cli.parse_cli``: CLI 인자 파싱
"""
from .run import _run_pipeline as run_pipeline
from .batch import _render_monthly_batch as render_monthly_batch
from .cli import _parse_cli as parse_cli, _print_usage as print_usage, _open_report_outputs as open_report_outputs
from .scheduling import _omit_target_calendar_before_close as omit_target_calendar_before_close
from .scheduling import (
    _observation_day_forward_mode as observation_day_forward_mode,
    _should_skip_ohlcv_right_gap as should_skip_ohlcv_right_gap,
)
from .types import PipelineOut

__all__ = [
    "run_pipeline",
    "render_monthly_batch",
    "parse_cli",
    "print_usage",
    "open_report_outputs",
    "omit_target_calendar_before_close",
    "observation_day_forward_mode",
    "should_skip_ohlcv_right_gap",
    "PipelineOut",
]
