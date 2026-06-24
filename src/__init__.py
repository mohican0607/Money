"""
KRX 뉴스–수익률 분석 파이프라인.

하위 패키지: ``data``(shim) · ``prediction`` · ``pipeline`` · ``learning`` · ``report``
루트: ``config`` · ``features`` · ``trading_calendar`` · ``stocks`` · ``news`` · ``investor_flow``
기존 ``from src import config, stocks, predict`` 등은 lazy 호환을 유지합니다.
"""
from __future__ import annotations

import importlib
import sys
import types
from typing import Any

_LAZY: dict[str, str] = {
    "config": "src.config",
    "trading_calendar": "src.trading_calendar",
    "features": "src.features",
    "stocks": "src.stocks",
    "news": "src.news",
    "investor_flow": "src.investor_flow",
    "prediction_accuracy_cache": "src.prediction.accuracy_cache",
    "predict": "src.prediction.predict",
    "pred_hybrid": "src.prediction.prediction_ranking",
    "pred_factors": "src.prediction.prediction_ranking",
    "pred_precision_gate": "src.prediction.prediction_ranking",
    "prediction_ranking": "src.prediction.prediction_ranking",
    "ml_move_rank": "src.prediction.ml_move_rank",
    "news_context_ml": "src.prediction.ml_move_rank",
    "train_snapshot": "src.learning.support",
    "theme_carryover": "src.learning.support",
    "snapshot_miss_diagnosis": "src.learning.support",
    "snapshot_rebuild_learning": "src.learning.market_theme",
    "day_mover_rationale": "src.report.content",
    "move_reference": "src.report.content",
    "theme_strong_mover_report": "src.report.content",
    "report": "src.report",
}

_COMPAT_SUBMODULES: dict[str, str] = {
    "market_index": "src.stocks",
    "stock_listing_sector": "src.stocks",
    "disclosure": "src.news",
}


def _alias_module(target: str, alias_name: str) -> types.ModuleType:
    mod = importlib.import_module(target)
    wrapper = types.ModuleType(f"src.{alias_name}")
    wrapper.__dict__.update(mod.__dict__)
    wrapper.__doc__ = mod.__doc__
    sys.modules[f"src.{alias_name}"] = wrapper
    return wrapper


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        return importlib.import_module(_LAZY[name])
    if name in _COMPAT_SUBMODULES:
        return _alias_module(_COMPAT_SUBMODULES[name], name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY) | set(_COMPAT_SUBMODULES))
