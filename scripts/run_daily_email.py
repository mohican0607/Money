"""
거래일 14:30 / 15:30 / 16:00 스케줄: ``python main.py`` 실행 후 리포트를 이메일로 발송.

- 14:30 / 15:30: 리포트 HTML 갱신 후 첨부 발송
- 16:00: ``--append-rebuild-learning`` 로 학습 진단만 갱신(이메일 본문만, 리포트 첨부 없음)

성공·실패·타임아웃 모두 이메일로 알립니다(``--skip-email`` 제외).

사용:
  python scripts/run_daily_email.py --slot 1430
  python scripts/run_daily_email.py --slot 1530
  python scripts/run_daily_email.py --slot 1600
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

KST = ZoneInfo("Asia/Seoul")

_LOG_DIR = ROOT / "scripts" / "logs"
_RUN_STATUS_OK = "ok"
_RUN_STATUS_TIMEOUT = "timeout"
_RUN_STATUS_ERROR = "error"

SLOT_LABELS = {
    "1430": "14:30",
    "1530": "15:30",
    "1600": "16:00",
}

_LOCK_1430 = _LOG_DIR / ".run_1430.lock"
_LOCK_1530 = _LOG_DIR / ".run_1530.lock"
_WAIT_PRIOR_MAX_SEC = 90 * 60  # 선행 슬롯(main.py timeout과 동일) 대기 상한


def _append_run_log(lines: list[str]) -> Path:
    """스케줄 실행 기록 — 작업 스케줄러 콘솔 없을 때 확인용."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(KST).strftime("%Y%m%d")
    path = _LOG_DIR / f"run_daily_{day}.log"
    stamp = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"\n=== {stamp} ===\n")
        fh.write("\n".join(lines))
        if lines and not lines[-1].endswith("\n"):
            fh.write("\n")
    return path


def _smtp_auth_hint(exc: BaseException) -> str:
    msg = str(exc).lower()
    if "authentication" in msg or "535" in msg:
        if "basic authentication is disabled" in msg:
            return (
                "Hotmail/Outlook SMTP 기본 인증이 Microsoft 에서 막혀 있습니다. "
                "Gmail(smtp.gmail.com) + 앱 비밀번호로 바꾸세요."
            )
        return (
            "SMTP 인증 실패 — Gmail 이면 myaccount.google.com/apppasswords 앱 비밀번호를 "
            "EMAIL_SMTP_PASSWORD 에 넣었는지 확인하세요."
        )
    return ""


def _ensure_env_defaults() -> None:
    os.environ.setdefault("MOCK_NEWS", "0")
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("NO_AUTO_OPEN_OUTPUT", "1")
    os.environ.setdefault("ML_REUSE_PRIOR_FOR_FORWARD", "1")
    os.environ.setdefault("PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD", "0")
    if not os.environ.get("MONEY_TEMP_DIR") and Path("F:/").exists():
        os.environ["MONEY_TEMP_DIR"] = r"F:\temp\Money\tmp"
    if not os.environ.get("MONEY_CACHE_DIR") and Path("F:/").exists():
        os.environ["MONEY_CACHE_DIR"] = r"F:\temp\Money\cache"
    temp = os.environ.get("MONEY_TEMP_DIR")
    if temp:
        os.environ["TEMP"] = temp
        os.environ["TMP"] = temp
        os.environ["JOBLIB_TEMP_FOLDER"] = temp
        Path(temp).mkdir(parents=True, exist_ok=True)
    cache = os.environ.get("MONEY_CACHE_DIR")
    if cache:
        Path(cache).mkdir(parents=True, exist_ok=True)


def _python_exe() -> Path:
    venv = ROOT / ".venv" / "Scripts" / "python.exe"
    if venv.is_file():
        return venv
    return Path(sys.executable)


def _check_trading_day_exit() -> int:
    """오늘이 거래일이고 T=다음 거래일도 거래일이면 0."""
    from src import trading_calendar

    today = datetime.now(KST).date()
    if not trading_calendar.is_trading_day(today):
        print(f"[run_daily_email] {today} — 거래일 아님, 건너뜀.")
        return 2
    try:
        t_day = trading_calendar.next_trading_day_after(today)
    except ValueError:
        print("[run_daily_email] 다음 거래일(T)을 찾을 수 없어 건너뜀.")
        return 4
    if not trading_calendar.is_trading_day(t_day):
        print(f"[run_daily_email] T={t_day} — 거래일 아님, 건너뜀.")
        return 3
    return 0


def _expected_dated_report_path(t_day: date) -> Path:
    from src import config

    return config.OUTPUT_DIR / f"report_dated_by_{t_day.strftime('%m%d')}.html"


def _expected_monthly_report_path(t_day: date) -> Path:
    from src import config

    return config.OUTPUT_DIR / f"report_{t_day.year}.{t_day.month:02d}.html"


def _acquire_run_lock(path: Path) -> None:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(f"pid={os.getpid()}\n", encoding="utf-8")


def _release_run_lock(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _wait_for_run_lock(path: Path, *, label: str, max_wait_sec: int = _WAIT_PRIOR_MAX_SEC) -> bool:
    """선행 슬롯 lock 이 풀릴 때까지 대기. 타임아웃 시 False."""
    if not path.is_file():
        return True
    deadline = time.monotonic() + max_wait_sec
    while path.is_file():
        if time.monotonic() >= deadline:
            print(
                f"[run_daily_email] {label} lock 대기 {max_wait_sec}초 초과",
                flush=True,
            )
            return False
        time.sleep(30)
    return True


def _wait_for_prior_slots_1600() -> bool:
    """16:00 — 14:30·15:30 선행 실행 종료 대기."""
    if not _wait_for_run_lock(_LOCK_1430, label="14:30"):
        return False
    return _wait_for_run_lock(_LOCK_1530, label="15:30")


def _kill_process_tree(pid: int) -> None:
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            check=False,
        )
        return
    try:
        os.kill(pid, 9)
    except OSError:
        pass


def _run_main_py(
    timeout_sec: int,
    *,
    slot: str,
    n_day: date | None = None,
) -> tuple[int | None, str, str]:
    """
    main.py 실행. stdout/stderr 는 임시 파일로 받아 파이프 교착을 피합니다.

    Returns:
        (exit_code, combined_log, status) — status: ok | timeout | error
    """
    py = _python_exe()
    main_py = ROOT / "main.py"
    cmd = [str(py), str(main_py)]
    if slot == "1600":
        # 장마감된 당일(T=오늘) 학습 진단 병합만 — 리포트 HTML 은 쓰지 않음.
        assert n_day is not None
        cmd.extend(["--append-rebuild-learning", n_day.strftime("%Y%m%d")])
    # 15:30: 플래그 없이 main.py → N→T=N+1 리포트 갱신(확정 반영).
    # --append-rebuild-learning 은 리포트를 쓰지 않으므로 여기선 붙이지 않음.
    slot_lock: Path | None = None
    if slot == "1430":
        slot_lock = _LOCK_1430
    elif slot == "1530":
        slot_lock = _LOCK_1530
    if slot_lock is not None:
        _acquire_run_lock(slot_lock)
    try:
        with tempfile.TemporaryDirectory(prefix="money_main_") as td:
            stdout_path = Path(td) / "stdout.txt"
            stderr_path = Path(td) / "stderr.txt"
            with stdout_path.open("w", encoding="utf-8", errors="replace") as out_fh, stderr_path.open(
                "w", encoding="utf-8", errors="replace"
            ) as err_fh:
                popen_kw: dict = {
                    "args": cmd,
                    "cwd": str(ROOT),
                    "stdout": out_fh,
                    "stderr": err_fh,
                    "env": os.environ.copy(),
                }
                if sys.platform == "win32":
                    popen_kw["creationflags"] = subprocess.CREATE_NO_WINDOW
                proc = subprocess.Popen(**popen_kw)
                try:
                    proc.wait(timeout=timeout_sec)
                except subprocess.TimeoutExpired:
                    _kill_process_tree(proc.pid)
                    try:
                        proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        pass
                    combined = _read_log_files(stdout_path, stderr_path)
                    combined += (
                        f"\n[run_daily_email] main.py 타임아웃 ({timeout_sec}초 초과) — "
                        "프로세스를 강제 종료했습니다.\n"
                    )
                    print(combined, end="" if combined.endswith("\n") else "\n", flush=True)
                    return None, combined, _RUN_STATUS_TIMEOUT

            combined = _read_log_files(stdout_path, stderr_path)
            print(combined, end="" if combined.endswith("\n") else "\n", flush=True)
            return proc.returncode, combined, _RUN_STATUS_OK
    finally:
        if slot_lock is not None:
            _release_run_lock(slot_lock)


def _read_log_files(stdout_path: Path, stderr_path: Path) -> str:
    parts: list[str] = []
    for path in (stdout_path, stderr_path):
        if path.is_file() and path.stat().st_size:
            parts.append(path.read_text(encoding="utf-8", errors="replace"))
    combined = ""
    for chunk in parts:
        combined += chunk
        if chunk and not chunk.endswith("\n"):
            combined += "\n"
    return combined


def _resolve_report_paths(t_day: date) -> tuple[Path | None, Path | None]:
    from src import config

    dated = _expected_dated_report_path(t_day)
    monthly = _expected_monthly_report_path(t_day)
    if not dated.is_file():
        candidates = sorted(
            config.OUTPUT_DIR.glob("report_dated_by_*.html"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            dated = candidates[0]
    if not dated.is_file():
        dated = None
    if not monthly.is_file():
        monthly = None
    return dated, monthly


def _format_report_note(path: Path | None, *, label: str) -> str:
    if path and path.is_file():
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=KST)
        return f"{label}: {path.name} (mtime {mtime:%H:%M:%S})"
    return f"{label}: 없음"


def _build_email_subject(
    *,
    prefix: str,
    n_day: date,
    t_day: date,
    slot_label: str,
    run_status: str,
    main_exit: int | None,
) -> str:
    base = (
        f"N={n_day:%Y-%m-%d} → T={t_day:%Y-%m-%d} ({slot_label})"
        if not prefix
        else f"{prefix} N={n_day:%Y-%m-%d} → T={t_day:%Y-%m-%d} ({slot_label})"
    )
    if run_status == _RUN_STATUS_TIMEOUT:
        tag = "실패·타임아웃"
    elif run_status == _RUN_STATUS_ERROR or (main_exit is not None and main_exit != 0):
        tag = "실패"
    else:
        tag = "완료"
    return f"{base} [{tag}]"


def _build_email_body(
    *,
    slot: str,
    n_day: date,
    t_day: date,
    main_exit: int | None,
    run_status: str,
    log_text: str,
    dated_path: Path | None,
    monthly_path: Path | None,
    extra_notes: list[str] | None = None,
) -> str:
    label = SLOT_LABELS.get(slot, slot)
    if run_status == _RUN_STATUS_TIMEOUT:
        status_line = f"실행 결과: 타임아웃 (main.py 미완료, exit={main_exit})"
    elif run_status == _RUN_STATUS_ERROR:
        status_line = f"실행 결과: 오류 (main.py exit={main_exit})"
    elif main_exit is None:
        status_line = "실행 결과: main.py 종료 코드를 확인하지 못함"
    elif main_exit == 0:
        status_line = f"실행 결과: 성공 (main.py exit=0)"
    else:
        status_line = f"실행 결과: 실패 (main.py exit={main_exit})"

    lines = [
        f"Money KRX 예측 리포트 ({label} 스케줄)",
        f"기준일 N: {n_day.isoformat()}",
        f"관측일 T (N+1): {t_day.isoformat()}",
        status_line,
        "",
    ]
    if slot == "1600":
        lines.append("첨부: 없음 (16:00은 학습 진단만 — 리포트 HTML 미첨부)")
    else:
        lines.extend(
            [
                _format_report_note(dated_path, label="일별 리포트"),
                _format_report_note(monthly_path, label="월간 리포트"),
            ]
        )
    if extra_notes:
        lines.extend(extra_notes)
    lines.extend(["", "--- main.py 출력 ---", ""])
    tail = log_text.strip()
    if len(tail) > 12000:
        tail = tail[-12000:]
        lines.append("(… 출력 일부 생략 …)\n")
    lines.append(tail or "(출력 없음)")
    return "\n".join(lines)


def _send_run_email(
    *,
    slot: str,
    n_day: date,
    t_day: date,
    main_exit: int | None,
    run_status: str,
    log_text: str,
    dated_path: Path | None,
    monthly_path: Path | None,
    extra_notes: list[str] | None = None,
) -> tuple[bool, str]:
    from src import config
    from src.notify.email_report import email_configured, parse_recipients, send_report_email

    if not email_configured():
        return False, (
            "[run_daily_email] 이메일 설정 미비 — "
            "EMAIL_SMTP_USER, EMAIL_SMTP_PASSWORD, EMAIL_RECIPIENTS 를 .env 에 추가하세요."
        )

    prefix = config.EMAIL_SUBJECT_PREFIX.strip()
    subject = _build_email_subject(
        prefix=prefix,
        n_day=n_day,
        t_day=t_day,
        slot_label=SLOT_LABELS[slot],
        run_status=run_status,
        main_exit=main_exit,
    )
    body = _build_email_body(
        slot=slot,
        n_day=n_day,
        t_day=t_day,
        main_exit=main_exit,
        run_status=run_status,
        log_text=log_text,
        dated_path=dated_path,
        monthly_path=monthly_path,
        extra_notes=extra_notes,
    )
    attachments: list[Path] = []
    if slot != "1600":
        for path in (dated_path, monthly_path):
            if path and path.is_file():
                attachments.append(path)

    try:
        send_report_email(subject=subject, body_text=body, attachment_paths=attachments)
    except Exception as exc:
        hint = _smtp_auth_hint(exc)
        msg = f"[run_daily_email] 이메일 발송 실패: {exc}"
        if hint:
            msg += f" — {hint}"
        return False, msg

    ok = f"[run_daily_email] 이메일 발송 완료 → {', '.join(parse_recipients())}"
    return True, ok


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="거래일 N→N+1 main.py 실행 후 이메일 발송")
    parser.add_argument(
        "--slot",
        required=True,
        choices=sorted(SLOT_LABELS),
        help="실행 슬롯 (1430=장중, 1530=장마감 직후, 1600=당일 T 확정)",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        help="main.py 만 실행하고 이메일은 보내지 않음",
    )
    args = parser.parse_args(argv)

    _ensure_env_defaults()

    from src import config
    from src import trading_calendar
    from src.pipeline.early_validate import current_n_and_n_plus_one

    log_lines: list[str] = [f"slot={args.slot} skip_email={args.skip_email}", "status=started"]
    _append_run_log(log_lines)

    check_code = _check_trading_day_exit()
    if check_code != 0:
        log_lines.append(f"거래일 검사 종료 code={check_code}")
        _append_run_log(log_lines)
        return 0 if check_code in (2, 3, 4) else check_code

    n_day, t_day = current_n_and_n_plus_one()
    # 16:00: 당일(T=오늘) 확정 — 이메일·첨부 리포트도 그 T 기준
    if args.slot == "1600":
        email_t = n_day
        email_n = trading_calendar.last_trading_day_before(email_t)
        report_t = email_t
        header = (
            f"[run_daily_email] {SLOT_LABELS[args.slot]} — "
            f"T={email_t.isoformat()} 학습 진단 병합 "
            f"(N={email_n.isoformat()}, 리포트 HTML 생략)"
        )
    else:
        email_n, email_t = n_day, t_day
        report_t = t_day
        header = (
            f"[run_daily_email] {SLOT_LABELS[args.slot]} — "
            f"N={n_day.isoformat()} → T={t_day.isoformat()}"
        )
    print(header, flush=True)
    log_lines.append(header)

    main_exit: int | None = None
    log_text = ""
    run_status = _RUN_STATUS_OK
    dated_path: Path | None = None
    monthly_path: Path | None = None

    try:
        if args.slot == "1600":
            if not _wait_for_prior_slots_1600():
                msg = (
                    f"[run_daily_email] 선행 슬롯 lock 대기 {_WAIT_PRIOR_MAX_SEC}초 초과 — "
                    "16:00 보강 실행을 건너뜁니다."
                )
                print(msg, flush=True)
                log_lines.append(msg)
                _append_run_log(log_lines)
                return 0

        timeout_sec = config.RUN_DAILY_MAIN_TIMEOUT_SEC
        log_lines.append(f"main.py timeout={timeout_sec}s")
        if args.slot == "1600":
            log_lines.append(
                f"main.py args=--append-rebuild-learning {n_day.strftime('%Y%m%d')} "
                "(학습 진단만, 리포트 HTML 생략)"
            )
        main_exit, log_text, run_status = _run_main_py(
            timeout_sec, slot=args.slot, n_day=n_day if args.slot == "1600" else None
        )
        log_lines.append(f"main.py exit={main_exit} status={run_status}")

        if args.slot == "1600":
            dated_path, monthly_path = None, None
            note = "리포트 첨부: 없음 (16:00 학습 진단만)"
            print(note, flush=True)
            log_lines.append(note)
        else:
            dated_path, monthly_path = _resolve_report_paths(report_t)
            dated_note = _format_report_note(dated_path, label="일별 리포트")
            monthly_note = _format_report_note(monthly_path, label="월간 리포트")
            print(dated_note, flush=True)
            print(monthly_note, flush=True)
            log_lines.extend([dated_note, monthly_note])

        if args.skip_email:
            _append_run_log(log_lines)
            if run_status == _RUN_STATUS_TIMEOUT:
                return 124
            return main_exit if main_exit is not None else 1

        sent, msg = _send_run_email(
            slot=args.slot,
            n_day=email_n,
            t_day=email_t,
            main_exit=main_exit,
            run_status=run_status,
            log_text=log_text,
            dated_path=dated_path,
            monthly_path=monthly_path,
        )
        print(msg, file=sys.stderr if not sent else sys.stdout, flush=True)
        log_lines.append(msg)
        _append_run_log(log_lines)

        if run_status == _RUN_STATUS_TIMEOUT:
            return 124
        if not sent:
            return 1 if main_exit in (None, 0) else (main_exit or 1)
        return main_exit if main_exit is not None else 1

    except Exception as exc:
        run_status = _RUN_STATUS_ERROR
        tb = traceback.format_exc()
        log_text = (log_text + "\n" + tb).strip()
        log_lines.append(f"예외: {exc}")
        print(tb, file=sys.stderr, flush=True)

        dated_path, monthly_path = (
            (None, None)
            if args.slot == "1600"
            else _resolve_report_paths(report_t)
        )
        if not args.skip_email:
            sent, msg = _send_run_email(
                slot=args.slot,
                n_day=email_n,
                t_day=email_t,
                main_exit=main_exit,
                run_status=run_status,
                log_text=log_text,
                dated_path=dated_path,
                monthly_path=monthly_path,
                extra_notes=[f"스크립트 예외: {exc}"],
            )
            print(msg, file=sys.stderr if not sent else sys.stdout, flush=True)
            log_lines.append(msg)

        _append_run_log(log_lines)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
