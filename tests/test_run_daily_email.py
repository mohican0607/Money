"""scripts/run_daily_email.py — 제목·경로 헬퍼."""
from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

from scripts.run_daily_email import (
    _build_email_body,
    _build_email_subject,
    _copy_slot_report_snapshot,
    _email_attachment_paths,
    _expected_dated_report_path,
    _expected_monthly_report_path,
    _format_elapsed_line,
    _is_slot_report_snapshot,
    _run_main_py,
    _send_run_email,
    _slot_report_snapshot_path,
    _RUN_STATUS_OK,
    _RUN_STATUS_TIMEOUT,
)


def test_format_elapsed_line() -> None:
    assert _format_elapsed_line(0) == "소요시간: 0분 00초"
    assert _format_elapsed_line(33) == "소요시간: 0분 33초"
    assert _format_elapsed_line(24 * 60 + 33) == "소요시간: 24분 33초"
    assert _format_elapsed_line(90 * 60 + 5.4) == "소요시간: 90분 05초"
    assert (
        _format_elapsed_line(8 * 60 + 59, label="append-rebuild-learning 소요시간")
        == "append-rebuild-learning 소요시간: 8분 59초"
    )
    assert (
        _format_elapsed_line(15 * 60 + 40, label="--force-ml-retrain 소요시간")
        == "--force-ml-retrain 소요시간: 15분 40초"
    )


def test_run_daily_auto_enabled_env(monkeypatch) -> None:
    from src import config

    monkeypatch.setenv("RUN_DAILY_AUTO_1430", "Y")
    assert config.run_daily_auto_enabled("1430") is True
    monkeypatch.setenv("RUN_DAILY_AUTO_1430", "N")
    assert config.run_daily_auto_enabled("1430") is False
    monkeypatch.setenv("RUN_DAILY_AUTO_1530", "0")
    assert config.run_daily_auto_enabled("1530") is False
    monkeypatch.delenv("RUN_DAILY_AUTO_1600", raising=False)
    assert config.run_daily_auto_enabled("1600") is True


def test_ml_retrain_after_append_enabled(monkeypatch) -> None:
    from scripts.run_daily_email import _ml_retrain_after_append_enabled

    monkeypatch.setenv("RUN_DAILY_AUTO_1630", "1")
    assert _ml_retrain_after_append_enabled() is True
    monkeypatch.setenv("RUN_DAILY_AUTO_1630", "0")
    assert _ml_retrain_after_append_enabled() is False

    from scripts import run_daily_email as rde

    monkeypatch.setenv("RUN_DAILY_AUTO_1430", "N")
    monkeypatch.setattr(rde, "_append_run_log", lambda lines: None)
    code = rde.main(["--slot", "1430"])
    assert code == 0


def test_main_force_runs_disabled_slot(monkeypatch) -> None:
    from scripts import run_daily_email as rde

    monkeypatch.setenv("RUN_DAILY_AUTO_1430", "N")
    monkeypatch.setattr(rde, "_check_trading_day_exit", lambda: 2)
    monkeypatch.setattr(rde, "_append_run_log", lambda lines: None)
    code = rde.main(["--slot", "1430", "--force"])
    assert code == 0


def test_ensure_env_defaults_slot_supplement_flag(monkeypatch) -> None:
    monkeypatch.delenv("PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD", raising=False)
    from scripts.run_daily_email import _ensure_env_defaults

    _ensure_env_defaults(slot="1430")
    assert os.environ["PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD"] == "0"
    _ensure_env_defaults(slot="1530")
    assert os.environ["PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD"] == "1"
    _ensure_env_defaults(slot="1600")
    assert os.environ["PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD"] == "1"
    _ensure_env_defaults(slot="1630")
    assert os.environ["PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD"] == "1"
    t = date(2026, 8, 7)
    assert _expected_monthly_report_path(t).name == "report_2026.08.html"


def test_expected_dated_report_path() -> None:
    t = date(2026, 8, 7)
    assert _expected_dated_report_path(t).name == "report_dated_by_0807.html"


def test_slot_report_snapshot_path() -> None:
    run_day = date(2026, 8, 21)
    p1 = _slot_report_snapshot_path(run_day, "1430")
    p2 = _slot_report_snapshot_path(run_day, "1530")
    assert p1 is not None and p1.name == "report_2026.0821-1430.html"
    assert p2 is not None and p2.name == "report_2026.0821-1530.html"
    assert _slot_report_snapshot_path(run_day, "1600") is None
    assert _slot_report_snapshot_path(run_day, "1630") is None


def test_copy_slot_report_snapshot(monkeypatch, tmp_path: Path) -> None:
    monthly = tmp_path / "report_2026.08.html"
    monthly.write_text("monthly-html", encoding="utf-8")
    monkeypatch.setattr("src.config.OUTPUT_DIR", tmp_path)

    dest = _copy_slot_report_snapshot(
        monthly, run_day=date(2026, 8, 21), slot="1430"
    )
    assert dest is not None
    assert dest.name == "report_2026.0821-1430.html"
    assert dest.read_text(encoding="utf-8") == "monthly-html"

    dest2 = _copy_slot_report_snapshot(
        monthly, run_day=date(2026, 8, 21), slot="1530"
    )
    assert dest2 is not None
    assert dest2.name == "report_2026.0821-1530.html"
    assert dest2.read_text(encoding="utf-8") == "monthly-html"


def test_copy_slot_report_snapshot_skips_missing(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("src.config.OUTPUT_DIR", tmp_path)
    assert (
        _copy_slot_report_snapshot(None, run_day=date(2026, 8, 21), slot="1430")
        is None
    )
    assert (
        _copy_slot_report_snapshot(
            tmp_path / "missing.html",
            run_day=date(2026, 8, 21),
            slot="1430",
        )
        is None
    )


def test_is_slot_report_snapshot() -> None:
    assert _is_slot_report_snapshot(Path("report_2026.0821-1430.html"))
    assert _is_slot_report_snapshot(Path("report_2026.0821-1530.html"))
    assert not _is_slot_report_snapshot(Path("report_2026.08.html"))
    assert not _is_slot_report_snapshot(Path("report_dated_by_0821.html"))


def test_email_attachment_paths_excludes_slot_snapshots(tmp_path: Path) -> None:
    dated = tmp_path / "report_dated_by_0824.html"
    monthly = tmp_path / "report_2026.08.html"
    snap1 = tmp_path / "report_2026.0821-1430.html"
    snap2 = tmp_path / "report_2026.0821-1530.html"
    for path in (dated, monthly, snap1, snap2):
        path.write_text("x", encoding="utf-8")

    attached = _email_attachment_paths(
        slot="1430", dated_path=dated, monthly_path=monthly
    )
    assert attached == [dated, monthly]

    attached_if_snapshot_passed = _email_attachment_paths(
        slot="1530", dated_path=snap1, monthly_path=snap2
    )
    assert attached_if_snapshot_passed == []

    assert _email_attachment_paths(
        slot="1600", dated_path=dated, monthly_path=monthly
    ) == []


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


def test_acquire_run_lock_blocks_live_holder(tmp_path: Path, monkeypatch) -> None:
    from scripts import run_daily_email as rde

    lock = tmp_path / ".run_1530.lock"
    lock.write_text("pid=999999\n", encoding="utf-8")
    monkeypatch.setattr(rde, "_pid_is_running", lambda pid: pid == 999999)
    assert rde._acquire_run_lock(lock) is False
    monkeypatch.setattr(rde, "_pid_is_running", lambda pid: False)
    assert rde._acquire_run_lock(lock) is True
    assert "pid=" in lock.read_text(encoding="utf-8")


def test_build_email_subject_failure_on_nonzero_exit() -> None:
    subj = _build_email_subject(
        prefix="[Money]",
        n_day=date(2026, 8, 27),
        t_day=date(2026, 8, 28),
        slot_label="15:30",
        run_status=_RUN_STATUS_OK,
        main_exit=1,
    )
    assert "[실패]" in subj


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

    code, log, status = _run_main_py(
        timeout_sec=30,
        slot="1430",
        n_day=date(2026, 8, 21),
        t_day=date(2026, 8, 24),
    )
    assert status == _RUN_STATUS_OK
    assert code == 0
    assert "ok" in log


def test_build_email_body_1430_starts_with_command() -> None:
    body = _build_email_body(
        slot="1430",
        n_day=date(2026, 8, 21),
        t_day=date(2026, 8, 24),
        main_exit=0,
        run_status=_RUN_STATUS_OK,
        log_text="ok",
        dated_path=Path("output/report_dated_by_0824.html"),
        monthly_path=Path("output/report_2026.08.html"),
    )
    first = body.splitlines()[0]
    assert first.startswith("실행 명령:")
    assert "main.py" in first
    assert "20260821" in first
    assert "20260824" in first
    assert "--append-rebuild-learning" not in first


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
        main_n_day=date(2026, 8, 20),
    )
    assert "첨부: 없음" in body
    assert "일별 리포트" not in body
    assert "월간 리포트" not in body
    first = body.splitlines()[0]
    assert first.startswith("실행 명령:")
    assert "--append-rebuild-learning 20260820" in first


def test_send_run_email_1430_skips_slot_snapshots(monkeypatch, tmp_path: Path) -> None:
    dated = tmp_path / "report_dated_by_0824.html"
    monthly = tmp_path / "report_2026.08.html"
    snap = tmp_path / "report_2026.0821-1430.html"
    dated.write_text("d", encoding="utf-8")
    monthly.write_text("m", encoding="utf-8")
    snap.write_text("s", encoding="utf-8")
    captured: dict = {}

    monkeypatch.setattr("src.notify.email_report.email_configured", lambda: True)
    monkeypatch.setattr("src.notify.email_report.parse_recipients", lambda: ["a@b.com"])
    monkeypatch.setattr("src.config.EMAIL_SUBJECT_PREFIX", "[Money]")

    def _fake_send(*, subject, body_text, attachment_paths):
        captured["attachments"] = [p.name for p in attachment_paths]

    monkeypatch.setattr("src.notify.email_report.send_report_email", _fake_send)

    ok, msg = _send_run_email(
        slot="1430",
        n_day=date(2026, 8, 21),
        t_day=date(2026, 8, 24),
        main_exit=0,
        run_status=_RUN_STATUS_OK,
        log_text="ok",
        dated_path=dated,
        monthly_path=monthly,
    )
    assert ok
    assert captured["attachments"] == ["report_dated_by_0824.html", "report_2026.08.html"]
    assert "report_2026.0821-1430.html" not in captured["attachments"]
    assert "발송 완료" in msg


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
    monkeypatch.setattr("scripts.run_daily_email._acquire_run_lock", lambda _p: True)
    monkeypatch.setattr("scripts.run_daily_email._release_run_lock", lambda _p: None)
    (tmp_path / "main.py").write_text("pass\n", encoding="utf-8")

    _run_main_py(
        timeout_sec=30,
        slot="1530",
        n_day=date(2026, 8, 21),
        t_day=date(2026, 8, 24),
    )
    assert captured[-1][-2:] == ["20260821", "20260824"]
    assert "--append-rebuild-learning" not in captured[-1]

    _run_main_py(timeout_sec=30, slot="1600", n_day=date(2026, 8, 20))
    assert captured[-1][-2:] == ["--append-rebuild-learning", "20260820"]


def test_1600_logs_append_and_ml_elapsed_separately(monkeypatch) -> None:
    from scripts import run_daily_email as rde

    log_blocks: list[list[str]] = []
    slots_run: list[str] = []

    monkeypatch.setenv("RUN_DAILY_AUTO_1600", "1")
    monkeypatch.setenv("RUN_DAILY_AUTO_1630", "1")
    monkeypatch.setattr(rde, "_append_run_log", lambda lines: log_blocks.append(list(lines)))
    monkeypatch.setattr(rde, "_check_trading_day_exit", lambda: 0)
    monkeypatch.setattr(rde, "_wait_for_prior_slots_1600", lambda: True)
    monkeypatch.setattr(rde, "_ml_retrain_after_append_enabled", lambda: True)
    monkeypatch.setattr(
        "src.pipeline.early_validate.current_n_and_n_plus_one",
        lambda: (date(2026, 9, 2), date(2026, 9, 3)),
    )
    monkeypatch.setattr(
        "src.trading_calendar.last_trading_day_before",
        lambda _d: date(2026, 9, 1),
    )
    monkeypatch.setattr(
        "src.pipeline.ml_retrain.forward_ml_retrain_t",
        lambda n_day: date(2026, 9, 3),
    )

    def fake_run_main(timeout_sec, *, slot, n_day=None, t_day=None):
        slots_run.append(slot)
        return 0, f"ok-{slot}", rde._RUN_STATUS_OK

    monkeypatch.setattr(rde, "_run_main_py", fake_run_main)

    code = rde.main(["--slot", "1600", "--skip-email"])
    assert code == 0
    assert slots_run == ["1600", "1630"]
    joined = "\n".join("\n".join(block) for block in log_blocks)
    assert "append-rebuild-learning 소요시간:" in joined
    assert "--force-ml-retrain 소요시간:" in joined
    assert "슬롯 전체 소요시간:" in joined
