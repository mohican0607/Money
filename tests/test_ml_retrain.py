"""ML 일일 강제 재학습 헬퍼."""
from __future__ import annotations

from datetime import date

from scripts.run_daily_email import _main_py_cmd
from src.pipeline.ml_retrain import forward_ml_retrain_t


def test_forward_ml_retrain_t_matches_n_plus_one() -> None:
    n = date(2026, 8, 28)
    t = forward_ml_retrain_t(n_day=n)
    assert t > n


def test_main_py_cmd_1630_force_ml_retrain() -> None:
    n = date(2026, 8, 28)
    t = date(2026, 8, 31)
    cmd = _main_py_cmd(slot="1630", n_day=n, t_day=t)
    assert "--force-ml-retrain" in cmd
    assert cmd[-1] == "20260831"
