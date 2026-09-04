"""
거래일 14:30 / 15:30 / 16:00 / 16:30 스케줄: ``python main.py`` 실행 후 리포트를 이메일로 발송.

- 14:30 / 15:30: 리포트 HTML 갱신 후 첨부 발송.
  정상 종료 시 월간 리포트(``report_YYYY.MM.html``)를 당일 스냅샷으로 복사:
  14:30 → ``report_YYYY.MMDD-1430.html``, 15:30 → ``report_YYYY.MMDD-1530.html``
  (스냅샷은 로컬 보관용 — 이메일에는 첨부하지 않음. 월간 HTML 이 이미 첨부됨)
- 16:00: ``--append-rebuild-learning`` 로 학습 진단 갱신 후, 성공 시 즉시
  ``--force-ml-retrain`` (``RUN_DAILY_AUTO_1630=1`` 일 때) — 이메일 본문만, 리포트 첨부 없음
- 16:30: 수동 재실행용(``--slot 1630``). 스케줄러에는 등록하지 않음.

성공·실패·타임아웃 모두 이메일로 알립니다(``--skip-email`` 제외).

사용:
  python scripts/run_daily_email.py --slot 1430
  python scripts/run_daily_email.py --slot 1530
  python scripts/run_daily_email.py --slot 1600
  python scripts/run_daily_email.py --slot 1630
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
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
_RUN_STATUS_SKIPPED = "skipped"

SLOT_LABELS = {
    "1430": "14:30",
    "1530": "15:30",
    "1600": "16:00",
    "1630": "16:30",
}

_SLOT_SNAPSHOT_SLOTS = frozenset({"1430", "1530"})

_LOCK_1430 = _LOG_DIR / ".run_1430.lock"
_LOCK_1530 = _LOG_DIR / ".run_1530.lock"
_LOCK_1600 = _LOG_DIR / ".run_1600.lock"
_LOCK_1630 = _LOG_DIR / ".run_1630.lock"
_WAIT_PRIOR_MAX_SEC = 90 * 60  # 선행 슬롯(main.py timeout과 동일) 대기 상한
_LOG_BLOCK_SEPARATOR = "\n\n\n\n"  # run_daily 로그 블록 사이 빈 줄 3줄
_LOG_ML_RETRAIN_DIVIDER = "--- ML 재학습 (--force-ml-retrain) ---"


def _append_run_log(lines: list[str]) -> Path:
    """스케줄 실행 기록 — 작업 스케줄러 콘솔 없을 때 확인용."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = datetime.now(KST).strftime("%Y%m%d")
    path = _LOG_DIR / f"run_daily_{day}.log"
    stamp = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    with path.open("a", encoding="utf-8") as fh:
        fh.write(_LOG_BLOCK_SEPARATOR)
        fh.write(f"=== {stamp} ===\n")
        fh.write("\n".join(lines))
        if lines and not lines[-1].endswith("\n"):
            fh.write("\n")
    return path


def _format_elapsed_line(
    elapsed_sec: float,
    *,
    label: str = "소요시간",
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> str:
    """소요시간 한 줄 — 기본 ``소요시간: 24분 33초``. 시각이 있으면 ``(14:30:08 ~ 14:56:54)``."""
    total_s = max(0, int(round(elapsed_sec)))
    minutes, seconds = divmod(total_s, 60)
    line = f"{label}: {minutes}분 {seconds:02d}초"
    if started_at is not None:
        end = ended_at or datetime.now(KST)
        line += f" ({started_at.strftime('%H:%M:%S')} ~ {end.strftime('%H:%M:%S')})"
    return line


def _append_elapsed(
    log_lines: list[str],
    started: float,
    *,
    label: str = "소요시간",
    started_at: datetime | None = None,
) -> None:
    """이메일 완료(또는 종료) 다음 행에 소요시간을 붙인다."""
    line = _format_elapsed_line(
        time.perf_counter() - started,
        label=label,
        started_at=started_at,
        ended_at=datetime.now(KST) if started_at is not None else None,
    )
    log_lines.append(line)
    print(line, flush=True)


def _finish_run_log(log_lines: list[str], *, status: str) -> None:
    """완료 블록 — 시작 하트비트의 slot/status=started 는 반복하지 않는다."""
    _append_run_log([f"status={status}", *log_lines])


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


def _ensure_env_defaults(*, slot: str | None = None) -> None:
    os.environ.setdefault("MOCK_NEWS", "0")
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("NO_AUTO_OPEN_OUTPUT", "1")
    os.environ.setdefault("ML_REUSE_PRIOR_FOR_FORWARD", "1")
    # 14:30: 장중이라 과거 「예측 전용」 백필을 끄고 N+1 예측만.
    # 15:30·16:00·16:30: 장후이므로 마감된 미갱신 일자(예: 당일 T)를 자동 확정 갱신.
    if slot == "1430":
        os.environ["PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD"] = "0"
    elif slot in ("1530", "1600", "1630"):
        os.environ["PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD"] = "1"
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


def _main_py_cmd(
    *,
    slot: str,
    n_day: date | None = None,
    t_day: date | None = None,
) -> list[str]:
    """슬롯별 ``main.py`` 실행 argv.

    - 14:30 / 15:30: ``main.py N N+1`` (관측일 구간 From=N, To=T)
    - 16:00: ``main.py --append-rebuild-learning N``
    - 16:30: ``main.py --force-ml-retrain T`` (T=N 다음 거래일)
    """
    cmd = [str(_python_exe()), str(ROOT / "main.py")]
    if slot == "1600":
        assert n_day is not None
        cmd.extend(["--append-rebuild-learning", n_day.strftime("%Y%m%d")])
    elif slot == "1630":
        assert t_day is not None
        cmd.extend(["--force-ml-retrain", t_day.strftime("%Y%m%d")])
    elif slot in ("1430", "1530"):
        assert n_day is not None and t_day is not None
        cmd.extend([n_day.strftime("%Y%m%d"), t_day.strftime("%Y%m%d")])
    return cmd


def _format_cmd_line(cmd: list[str]) -> str:
    """경로에 공백이 있으면 따옴표로 감싼 한 줄 명령."""
    parts: list[str] = []
    for p in cmd:
        if any(ch.isspace() for ch in p):
            parts.append(f'"{p}"')
        else:
            parts.append(p)
    return " ".join(parts)


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


def _slot_report_snapshot_path(run_day: date, slot: str) -> Path | None:
    """14:30 → report_YYYY.MMDD-1430.html, 15:30 → report_YYYY.MMDD-1530.html. 그 외 슬롯은 None."""
    if slot not in _SLOT_SNAPSHOT_SLOTS:
        return None
    from src import config

    return (
        config.OUTPUT_DIR
        / f"report_{run_day.year}.{run_day.month:02d}{run_day.day:02d}-{slot}.html"
    )


def _copy_slot_report_snapshot(
    monthly_path: Path | None,
    *,
    run_day: date,
    slot: str,
) -> Path | None:
    """월간 리포트를 슬롯 스냅샷으로 복사. 파일이 없거나 해당 슬롯이 아니면 None."""
    dest = _slot_report_snapshot_path(run_day, slot)
    if dest is None:
        return None
    label = SLOT_LABELS.get(slot, slot)
    if monthly_path is None or not monthly_path.is_file():
        print(
            f"[run_daily_email] {label} 스냅샷 생략: 월간 리포트 없음",
            flush=True,
        )
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(monthly_path, dest)
    print(
        f"[run_daily_email] 리포트 스냅샷: {monthly_path.name} → {dest.name}",
        flush=True,
    )
    return dest


def _pid_is_running(pid: int) -> bool:
    """프로세스가 살아 있는지(Windows 포함)."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        try:
            import ctypes

            SYNCHRONIZE = 0x00100000
            handle = ctypes.windll.kernel32.OpenProcess(SYNCHRONIZE, False, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except Exception:
            return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _acquire_run_lock(path: Path) -> bool:
    """
    슬롯 lock 획득. 다른 살아 있는 프로세스가 잡고 있으면 False.
    죽은 pid 의 stale lock 은 덮어씁니다.
    """
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            text = ""
        m = re.search(r"pid\s*=\s*(\d+)", text)
        if m:
            other = int(m.group(1))
            if other != os.getpid() and _pid_is_running(other):
                return False
    path.write_text(f"pid={os.getpid()}\n", encoding="utf-8")
    return True


def _release_run_lock(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _wait_for_run_lock(path: Path, *, label: str, max_wait_sec: int = _WAIT_PRIOR_MAX_SEC) -> bool:
    """선행 슬롯 lock 이 풀릴 때까지 대기. 타임아웃 시 False."""
    if not path.is_file():
        return True
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"pid\s*=\s*(\d+)", text)
        if m and not _pid_is_running(int(m.group(1))):
            path.unlink(missing_ok=True)
            return True
    except OSError:
        pass
    deadline = time.monotonic() + max_wait_sec
    while path.is_file():
        if time.monotonic() >= deadline:
            print(
                f"[run_daily_email] {label} lock 대기 {max_wait_sec}초 초과",
                flush=True,
            )
            return False
        time.sleep(30)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            m = re.search(r"pid\s*=\s*(\d+)", text)
            if m and not _pid_is_running(int(m.group(1))):
                path.unlink(missing_ok=True)
                return True
        except OSError:
            return True
    return True


def _wait_for_prior_slots_1600() -> bool:
    """16:00 — 14:30·15:30 선행 실행 종료 대기."""
    if not _wait_for_run_lock(_LOCK_1430, label="14:30"):
        return False
    return _wait_for_run_lock(_LOCK_1530, label="15:30")


def _wait_for_prior_slots_1630() -> bool:
    """16:30(수동) — 16:00 append·연쇄 ML 재학습 종료 대기."""
    return _wait_for_run_lock(_LOCK_1600, label="16:00")


def _ml_retrain_after_append_enabled() -> bool:
    from src import config

    return config.run_daily_auto_enabled("1630")


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
    t_day: date | None = None,
) -> tuple[int | None, str, str]:
    """
    main.py 실행. stdout/stderr 는 임시 파일로 받아 파이프 교착을 피합니다.

    Returns:
        (exit_code, combined_log, status) — status: ok | timeout | error
    """
    cmd = _main_py_cmd(slot=slot, n_day=n_day, t_day=t_day)
    slot_lock: Path | None = None
    if slot == "1430":
        slot_lock = _LOCK_1430
    elif slot == "1530":
        slot_lock = _LOCK_1530
    elif slot == "1600":
        slot_lock = _LOCK_1600
    elif slot == "1630":
        slot_lock = _LOCK_1630
    if slot_lock is not None:
        if not _acquire_run_lock(slot_lock):
            msg = (
                f"[run_daily_email] {SLOT_LABELS.get(slot, slot)} 이미 실행 중"
                f"(lock={slot_lock.name}) — 중복 실행을 건너뜁니다.\n"
            )
            print(msg, flush=True)
            return 1, msg, _RUN_STATUS_ERROR
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
                    _persist_main_log(slot=slot, n_day=n_day, text=combined)
                    return None, combined, _RUN_STATUS_TIMEOUT

            combined = _read_log_files(stdout_path, stderr_path)
            print(combined, end="" if combined.endswith("\n") else "\n", flush=True)
            _persist_main_log(slot=slot, n_day=n_day, text=combined)
            code = proc.returncode
            status = _RUN_STATUS_OK if code == 0 else _RUN_STATUS_ERROR
            return code, combined, status
    finally:
        if slot_lock is not None:
            _release_run_lock(slot_lock)


def _persist_main_log(*, slot: str, n_day: date | None, text: str) -> Path | None:
    """main.py stdout/stderr 를 슬롯별 파일로 남겨 실패 원인을 추적."""
    if not text.strip():
        return None
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    day = n_day or datetime.now(KST).date()
    path = _LOG_DIR / f"main_{slot}_{day.strftime('%Y%m%d')}.log"
    try:
        path.write_text(text, encoding="utf-8", errors="replace")
        print(f"[run_daily_email] main 로그 저장: {path.name}", flush=True)
        return path
    except OSError as exc:
        print(f"[run_daily_email] main 로그 저장 실패: {exc}", flush=True)
        return None


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


def _is_slot_report_snapshot(path: Path) -> bool:
    """``report_YYYY.MMDD-1430.html`` / ``report_YYYY.MMDD-1530.html`` 슬롯 스냅샷 여부."""
    name = path.name
    return name.startswith("report_") and name.endswith(("-1430.html", "-1530.html"))


def _email_attachment_paths(
    *,
    slot: str,
    dated_path: Path | None,
    monthly_path: Path | None,
) -> list[Path]:
    """이메일 첨부: 일별·월간만. 슬롯 스냅샷(-1430/-1530)과 16:00·16:30 은 제외."""
    if slot in ("1600", "1630"):
        return []
    attachments: list[Path] = []
    for path in (dated_path, monthly_path):
        if path is None or not path.is_file():
            continue
        if _is_slot_report_snapshot(path):
            continue
        attachments.append(path)
    return attachments


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
    command_line: str | None = None,
    main_n_day: date | None = None,
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

    if command_line:
        cmd = command_line
    elif slot == "1600":
        append_day = main_n_day if main_n_day is not None else n_day
        cmd = _format_cmd_line(_main_py_cmd(slot=slot, n_day=append_day))
    elif slot == "1630":
        cmd = _format_cmd_line(_main_py_cmd(slot=slot, n_day=n_day, t_day=t_day))
    else:
        cmd = _format_cmd_line(_main_py_cmd(slot=slot, n_day=n_day, t_day=t_day))
    lines = [
        f"실행 명령: {cmd}",
        "",
        f"Money KRX 예측 리포트 ({label} 스케줄)",
        f"기준일 N: {n_day.isoformat()}",
        f"관측일 T (N+1): {t_day.isoformat()}",
        status_line,
        "",
    ]
    if slot == "1600":
        lines.append("첨부: 없음 (16:00은 학습 진단만 — 리포트 HTML 미첨부)")
    elif slot == "1630":
        lines.append("첨부: 없음 (16:30은 ML joblib 재학습만 — 리포트 HTML 미첨부)")
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
    command_line: str | None = None,
    main_n_day: date | None = None,
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
        command_line=command_line,
        main_n_day=main_n_day,
    )
    attachments = _email_attachment_paths(
        slot=slot, dated_path=dated_path, monthly_path=monthly_path
    )

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
    if sys.platform == "win32":
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, OSError, ValueError):
                pass

    parser = argparse.ArgumentParser(description="거래일 N→N+1 main.py 실행 후 이메일 발송")
    parser.add_argument(
        "--slot",
        required=True,
        choices=sorted(SLOT_LABELS),
        help="실행 슬롯 (1430=장중, 1530=장마감 직후, 1600=당일 T 확정, 1630=ML 재학습)",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        help="main.py 만 실행하고 이메일은 보내지 않음",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=".env RUN_DAILY_AUTO_*=0 이더도 강제 실행(수동 테스트용)",
    )
    args = parser.parse_args(argv)
    started = time.perf_counter()
    started_at = datetime.now(KST)
    slot_total_label = "슬롯 전체 소요시간" if args.slot == "1600" else "소요시간"

    _ensure_env_defaults(slot=args.slot)

    from src import config
    from src import trading_calendar
    from src.pipeline.early_validate import current_n_and_n_plus_one

    if not args.force and not config.run_daily_auto_enabled(args.slot):
        label = SLOT_LABELS.get(args.slot, args.slot)
        msg = (
            f"[run_daily_email] {label} 자동 실행 비활성 "
            f"(RUN_DAILY_AUTO_{args.slot}=0) — 건너뜀. "
            f"수동 실행은 --force"
        )
        print(msg, flush=True)
        _append_run_log(
            [
                f"slot={args.slot} skip_email={args.skip_email}",
                "status=skipped_auto_disabled",
                msg,
                _format_elapsed_line(
                    time.perf_counter() - started,
                    started_at=started_at,
                    ended_at=datetime.now(KST),
                ),
            ]
        )
        return 0

    # config 가 이미 import 된 프로세스에서도 슬롯별 보강 플래그를 반영.
    config.PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD = (
        os.getenv("PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD", "1").strip().lower()
        in ("1", "true", "True", "yes")
    )

    slot_header = f"slot={args.slot} skip_email={args.skip_email}"
    log_lines: list[str] = []
    # 중단·행 대비 하트비트. 완료 블록에는 slot/status=started 를 다시 쓰지 않는다.
    _append_run_log([slot_header, "status=started"])

    check_code = _check_trading_day_exit()
    if check_code != 0:
        log_lines.append(f"거래일 검사 종료 code={check_code}")
        _append_elapsed(
            log_lines, started, label=slot_total_label, started_at=started_at
        )
        _finish_run_log(
            log_lines,
            status=_RUN_STATUS_SKIPPED if check_code in (2, 3, 4) else _RUN_STATUS_ERROR,
        )
        return 0 if check_code in (2, 3, 4) else check_code

    n_day, t_day = current_n_and_n_plus_one()
    # 16:00: 당일(T=오늘) 확정 — 이메일·첨부 리포트도 그 T 기준
    if args.slot == "1600":
        email_t = n_day
        email_n = trading_calendar.last_trading_day_before(email_t)
        report_t = email_t
        ml_after = _ml_retrain_after_append_enabled()
        chain_note = (
            " → append 직후 ML 재학습"
            if ml_after
            else " (ML 재학습 생략: RUN_DAILY_AUTO_1630=0)"
        )
        header = (
            f"[run_daily_email] {SLOT_LABELS[args.slot]} — "
            f"T={email_t.isoformat()} 학습 진단 병합"
            f"{chain_note} "
            f"(N={email_n.isoformat()}, 리포트 HTML 생략)"
        )
    elif args.slot == "1630":
        from src.pipeline.ml_retrain import forward_ml_retrain_t

        ml_t = forward_ml_retrain_t(n_day=n_day)
        email_n, email_t = n_day, ml_t
        report_t = ml_t
        header = (
            f"[run_daily_email] {SLOT_LABELS[args.slot]} — "
            f"ML 강제 재학습 T={ml_t.isoformat()} "
            f"(N={n_day.isoformat()}, 16:00 append 이후, 리포트 HTML 생략)"
        )
        t_day = ml_t
    else:
        email_n, email_t = n_day, t_day
        report_t = t_day
        header = (
            f"[run_daily_email] {SLOT_LABELS[args.slot]} — "
            f"N={n_day.isoformat()} → T={t_day.isoformat()}"
        )
    print(header, flush=True)
    log_lines.append(header)

    if args.slot == "1600":
        main_cmd_line = _format_cmd_line(_main_py_cmd(slot=args.slot, n_day=n_day))
    elif args.slot == "1630":
        main_cmd_line = _format_cmd_line(
            _main_py_cmd(slot=args.slot, n_day=n_day, t_day=t_day)
        )
    else:
        main_cmd_line = _format_cmd_line(
            _main_py_cmd(slot=args.slot, n_day=n_day, t_day=t_day)
        )
    log_lines.append(f"cmd={main_cmd_line}")

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
                _append_elapsed(
                    log_lines, started, label=slot_total_label, started_at=started_at
                )
                _finish_run_log(log_lines, status=_RUN_STATUS_SKIPPED)
                return 0
        elif args.slot == "1630":
            if not _wait_for_prior_slots_1630():
                msg = (
                    f"[run_daily_email] 16:00 lock 대기 {_WAIT_PRIOR_MAX_SEC}초 초과 — "
                    "16:30 ML 재학습을 건너뜁니다."
                )
                print(msg, flush=True)
                log_lines.append(msg)
                _append_elapsed(
                    log_lines, started, label=slot_total_label, started_at=started_at
                )
                _finish_run_log(log_lines, status=_RUN_STATUS_SKIPPED)
                return 0

        timeout_sec = config.RUN_DAILY_MAIN_TIMEOUT_SEC
        log_lines.append(f"main.py timeout={timeout_sec}s")
        if args.slot == "1600":
            log_lines.append(
                f"main.py args=--append-rebuild-learning {n_day.strftime('%Y%m%d')} "
                "(학습 진단만, 리포트 HTML 생략)"
            )
        elif args.slot == "1630":
            log_lines.append(
                f"main.py args=--force-ml-retrain {t_day.strftime('%Y%m%d')} "
                "(ML joblib 재학습만, 리포트 HTML 생략)"
            )
        else:
            log_lines.append(
                f"main.py args={n_day.strftime('%Y%m%d')} {t_day.strftime('%Y%m%d')}"
            )
        step_started = time.perf_counter()
        main_exit, log_text, run_status = _run_main_py(
            timeout_sec,
            slot=args.slot,
            n_day=n_day,
            t_day=t_day if args.slot not in ("1600",) else None,
        )
        log_lines.append(f"main.py exit={main_exit} status={run_status}")
        if args.slot == "1600":
            _append_elapsed(
                log_lines,
                step_started,
                label="append-rebuild-learning 소요시간",
            )
        elif args.slot == "1630":
            _append_elapsed(
                log_lines,
                step_started,
                label="--force-ml-retrain 소요시간",
            )

        if args.slot == "1600":
            dated_path, monthly_path = None, None
            if (
                run_status == _RUN_STATUS_OK
                and main_exit == 0
                and _ml_retrain_after_append_enabled()
            ):
                from src.pipeline.ml_retrain import forward_ml_retrain_t

                ml_t = forward_ml_retrain_t(n_day=n_day)
                chain_msg = (
                    f"[run_daily_email] 16:00 append 완료 → ML 재학습 즉시 실행 "
                    f"T={ml_t.isoformat()}"
                )
                print(chain_msg, flush=True)
                log_lines.append(_LOG_ML_RETRAIN_DIVIDER)
                log_lines.append(chain_msg)
                log_lines.append(
                    f"chain_cmd={_format_cmd_line(_main_py_cmd(slot='1630', n_day=n_day, t_day=ml_t))}"
                )
                ml_started = time.perf_counter()
                ml_exit, ml_log, ml_status = _run_main_py(
                    timeout_sec,
                    slot="1630",
                    n_day=n_day,
                    t_day=ml_t,
                )
                log_lines.append(f"ML 재학습 exit={ml_exit} status={ml_status}")
                _append_elapsed(
                    log_lines,
                    ml_started,
                    label="--force-ml-retrain 소요시간",
                )
                log_text = (
                    log_text.rstrip()
                    + "\n\n--- ML 재학습 (--force-ml-retrain) ---\n"
                    + ml_log
                ).strip()
                if ml_status == _RUN_STATUS_TIMEOUT:
                    run_status = _RUN_STATUS_TIMEOUT
                    main_exit = ml_exit
                elif ml_status != _RUN_STATUS_OK or ml_exit != 0:
                    run_status = _RUN_STATUS_ERROR
                    main_exit = ml_exit if ml_exit is not None else 1
            note = (
                "리포트 첨부: 없음 (16:00 학습 진단 + ML 재학습)"
                if _ml_retrain_after_append_enabled()
                else "리포트 첨부: 없음 (16:00 학습 진단만)"
            )
            print(note, flush=True)
            log_lines.append(note)
        elif args.slot == "1630":
            dated_path, monthly_path = None, None
            note = "리포트 첨부: 없음 (16:30 ML 재학습만)"
            print(note, flush=True)
            log_lines.append(note)
        else:
            dated_path, monthly_path = _resolve_report_paths(report_t)
            dated_note = _format_report_note(dated_path, label="일별 리포트")
            monthly_note = _format_report_note(monthly_path, label="월간 리포트")
            print(dated_note, flush=True)
            print(monthly_note, flush=True)
            log_lines.extend([dated_note, monthly_note])
            if run_status == _RUN_STATUS_OK and main_exit == 0:
                snap = _copy_slot_report_snapshot(
                    monthly_path, run_day=n_day, slot=args.slot
                )
                if snap is not None:
                    log_lines.append(f"스냅샷: {snap.name}")
            else:
                log_lines.append(
                    f"스냅샷 생략: main.py exit={main_exit} status={run_status} "
                    "(월간 리포트가 갱신되지 않았을 수 있음)"
                )

        if args.skip_email:
            _append_elapsed(
                log_lines, started, label=slot_total_label, started_at=started_at
            )
            _finish_run_log(log_lines, status=run_status)
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
            command_line=main_cmd_line,
            main_n_day=n_day if args.slot == "1600" else None,
        )
        print(msg, file=sys.stderr if not sent else sys.stdout, flush=True)
        log_lines.append(msg)
        _append_elapsed(
            log_lines, started, label=slot_total_label, started_at=started_at
        )
        _finish_run_log(log_lines, status=run_status)

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
                command_line=main_cmd_line,
                main_n_day=n_day if args.slot == "1600" else None,
            )
            print(msg, file=sys.stderr if not sent else sys.stdout, flush=True)
            log_lines.append(msg)

        _append_elapsed(
            log_lines, started, label=slot_total_label, started_at=started_at
        )
        _finish_run_log(log_lines, status=run_status)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
