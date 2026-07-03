"""
파이프라인 오케스트레이션 — ``main.py`` 에서 분리.

무거운 하위 모듈은 속성 접근 시에만 import 합니다(``main.py`` 선검증 등 빠른 종료용).
"""
from __future__ import annotations

from typing import Any

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "run_pipeline": (".run", "_run_pipeline"),
    "render_monthly_batch": (".batch", "_render_monthly_batch"),
    "parse_cli": (".cli", "_parse_cli"),
    "print_usage": (".cli", "_print_usage"),
    "open_report_outputs": (".cli", "_open_report_outputs"),
    "omit_target_calendar_before_close": (".support", "_omit_target_calendar_before_close"),
    "observation_day_forward_mode": (".support", "_observation_day_forward_mode"),
    "should_skip_ohlcv_right_gap": (".support", "_should_skip_ohlcv_right_gap"),
    "validate_range_n_plus_one": (".support", "validate_range_n_plus_one"),
    "validate_dated_n_day": (".support", "validate_dated_n_day"),
    "validate_dated_t_day": (".support", "validate_dated_t_day"),
    "PipelineOut": (".types", "PipelineOut"),
}


def __getattr__(name: str) -> Any:
    if name not in _LAZY_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    mod_name, attr = _LAZY_EXPORTS[name]
    import importlib

    mod = importlib.import_module(mod_name, __name__)
    return getattr(mod, attr)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_EXPORTS))
