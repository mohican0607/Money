"""scripts/run_daily_email.py — 제목·경로 헬퍼."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from scripts.run_daily_email import (
    _build_email_subject,
    _expected_dated_report_path,
    _expected_monthly_report_path,
    _run_main_py,
    _RUN_STATUS_OK,
    _RUN_STATUS_TIMEOUT,
)


def test_expected_monthly_report_path() -> None:
    t = date(2026, 8, 7)
    assert _expected_monthly_report_path(t).name == "report_2026.08.html"


def test_expected_dated_report_path() -> None:
    t = date(2026, 8, 7)
    assert _expected_dated_report_path(t).name == "report_dated_by_0807.html"


def test_build_email_subject_success() -> None:
    subj = _build_email_subject(
        prefix="[Money]",
        n_day=date(2026, 8, 6),
        t_day=date(2026, 8, 7),
        slot_label="14:30",
        run_status=_RUN_STATUS_OK,
        main_exit=0,
    )
    assert "[완료]" in subj
    assert "14:30" in subj


def test_build_email_subject_timeout() -> None:
    subj = _build_email_subject(
        prefix="[Money]",
        n_day=date(2026, 8, 6),
        t_day=date(2026, 8, 7),
        slot_label="14:30",
        run_status=_RUN_STATUS_TIMEOUT,
        main_exit=None,
    )
    assert "실패·타임아웃" in subj


def test_run_main_py_quick_exit(monkeypatch, tmp_path: Path) -> None:
    def fake_kill(_pid: int) -> None:
        return None

    monkeypatch.setattr("scripts.run_daily_email._kill_process_tree", fake_kill)
    monkeypatch.setattr("scripts.run_daily_email.ROOT", tmp_path)
    monkeypatch.setattr("scripts.run_daily_email._python_exe", lambda: Path(sys.executable))
    (tmp_path / "main.py").write_text("print('ok')\n", encoding="utf-8")

    code, log, status = _run_main_py(timeout_sec=30, slot="1430")
    assert status == _RUN_STATUS_OK
    assert code == 0
    assert "ok" in log
