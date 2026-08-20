"""scripts/run_daily_email.py — 제목·경로 헬퍼."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from scripts.run_daily_email import (
    _build_email_body,
    _build_email_subject,
    _expected_dated_report_path,
    _expected_monthly_report_path,
    _run_main_py,
    _send_run_email,
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


def test_build_email_body_1600_skips_report_notes() -> None:
    body = _build_email_body(
        slot="1600",
        n_day=date(2026, 8, 20),
        t_day=date(2026, 8, 20),
        main_exit=0,
        run_status=_RUN_STATUS_OK,
        log_text="ok",
        dated_path=Path("output/report_dated_by_0820.html"),
        monthly_path=Path("output/report_2026.08.html"),
    )
    assert "첨부: 없음" in body
    assert "일별 리포트" not in body
    assert "월간 리포트" not in body


def test_send_run_email_1600_no_attachments(monkeypatch, tmp_path: Path) -> None:
    dated = tmp_path / "report_dated_by_0820.html"
    monthly = tmp_path / "report_2026.08.html"
    dated.write_text("d", encoding="utf-8")
    monthly.write_text("m", encoding="utf-8")
    captured: dict = {}

    monkeypatch.setattr(
        "src.notify.email_report.email_configured",
        lambda: True,
    )
    monkeypatch.setattr(
        "src.notify.email_report.parse_recipients",
        lambda: ["a@b.com"],
    )
    monkeypatch.setattr(
        "src.config.EMAIL_SUBJECT_PREFIX",
        "[Money]",
    )

    def _fake_send(*, subject, body_text, attachment_paths):
        captured["attachments"] = list(attachment_paths)

    monkeypatch.setattr("src.notify.email_report.send_report_email", _fake_send)

    ok, msg = _send_run_email(
        slot="1600",
        n_day=date(2026, 8, 20),
        t_day=date(2026, 8, 20),
        main_exit=0,
        run_status=_RUN_STATUS_OK,
        log_text="ok",
        dated_path=dated,
        monthly_path=monthly,
    )
    assert ok
    assert captured["attachments"] == []
    assert "발송 완료" in msg


def test_run_main_py_slot_args(monkeypatch, tmp_path: Path) -> None:
    captured: list[list[str]] = []

    class _FakeProc:
        returncode = 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(**kwargs):
        captured.append(list(kwargs["args"]))
        return _FakeProc()

    monkeypatch.setattr("scripts.run_daily_email._kill_process_tree", lambda _pid: None)
    monkeypatch.setattr("scripts.run_daily_email.ROOT", tmp_path)
    monkeypatch.setattr("scripts.run_daily_email._python_exe", lambda: Path(sys.executable))
    monkeypatch.setattr("scripts.run_daily_email.subprocess.Popen", fake_popen)
    monkeypatch.setattr("scripts.run_daily_email._acquire_run_lock", lambda _p: None)
    monkeypatch.setattr("scripts.run_daily_email._release_run_lock", lambda _p: None)
    (tmp_path / "main.py").write_text("pass\n", encoding="utf-8")

    _run_main_py(timeout_sec=30, slot="1530")
    assert "--append-rebuild-learning" not in captured[-1]

    _run_main_py(timeout_sec=30, slot="1600", n_day=date(2026, 8, 20))
    assert captured[-1][-2:] == ["--append-rebuild-learning", "20260820"]
