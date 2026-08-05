"""
거래일 14:30 / 15:30 스케줄: ``python main.py`` (N→N+1) 실행 후 리포트를 이메일로 발송.

사용:
  python scripts/run_daily_email.py --slot 1430
  python scripts/run_daily_email.py --slot 1530
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

KST = ZoneInfo("Asia/Seoul")

SLOT_LABELS = {
    "1430": "14:30",
    "1530": "15:30",
}

_LOG_DIR = ROOT / "scripts" / "logs"


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


def _expected_report_path(n_day: date, t_day: date) -> Path:
    from src import config

    return config.OUTPUT_DIR / f"report_dated_by_{t_day.strftime('%m%d')}.html"


def _run_main_py() -> tuple[int, str]:
    py = _python_exe()
    main_py = ROOT / "main.py"
    proc = subprocess.run(
        [str(py), str(main_py)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=os.environ.copy(),
    )
    combined = ""
    if proc.stdout:
        combined += proc.stdout
        if not combined.endswith("\n"):
            combined += "\n"
    if proc.stderr:
        combined += proc.stderr
    print(combined, end="" if combined.endswith("\n") else "\n", flush=True)
    return proc.returncode, combined


def _build_email_body(
    *,
    slot: str,
    n_day: date,
    t_day: date,
    main_exit: int,
    log_text: str,
    report_path: Path | None,
) -> str:
    label = SLOT_LABELS.get(slot, slot)
    lines = [
        f"Money KRX 예측 리포트 ({label} 스케줄)",
        f"기준일 N: {n_day.isoformat()}",
        f"관측일 T (N+1): {t_day.isoformat()}",
        f"main.py 종료 코드: {main_exit}",
        "",
    ]
    if report_path and report_path.is_file():
        lines.append(f"리포트: {report_path.name} (첨부)")
    else:
        lines.append("리포트 HTML: 생성되지 않았거나 찾을 수 없음")
    lines.extend(["", "--- main.py 출력 ---", ""])
    tail = log_text.strip()
    if len(tail) > 12000:
        tail = tail[-12000:]
        lines.append("(… 출력 일부 생략 …)\n")
    lines.append(tail or "(출력 없음)")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="거래일 N→N+1 main.py 실행 후 이메일 발송")
    parser.add_argument(
        "--slot",
        required=True,
        choices=sorted(SLOT_LABELS),
        help="실행 슬롯 (1430=장중, 1530=장마감 직후)",
    )
    parser.add_argument(
        "--skip-email",
        action="store_true",
        help="main.py 만 실행하고 이메일은 보내지 않음",
    )
    args = parser.parse_args(argv)

    _ensure_env_defaults()

    from src import config
    from src.notify.email_report import email_configured, parse_recipients, send_report_email
    from src.pipeline.early_validate import current_n_and_n_plus_one

    log_lines: list[str] = [f"slot={args.slot} skip_email={args.skip_email}"]

    check_code = _check_trading_day_exit()
    if check_code != 0:
        log_lines.append(f"거래일 검사 종료 code={check_code}")
        _append_run_log(log_lines)
        return 0 if check_code in (2, 3, 4) else check_code

    n_day, t_day = current_n_and_n_plus_one()
    slot_label = SLOT_LABELS[args.slot]
    header = f"[run_daily_email] {slot_label} — N={n_day.isoformat()} → T={t_day.isoformat()}"
    print(header, flush=True)
    log_lines.append(header)

    main_exit, log_text = _run_main_py()
    log_lines.append(f"main.py exit={main_exit}")
    report_path = _expected_report_path(n_day, t_day)
    if not report_path.is_file():
        candidates = sorted(
            config.OUTPUT_DIR.glob("report_dated_by_*.html"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if candidates:
            report_path = candidates[0]

    if report_path.is_file():
        mtime = datetime.fromtimestamp(report_path.stat().st_mtime, tz=KST)
        report_note = f"리포트: {report_path.name} (mtime {mtime:%H:%M:%S})"
    else:
        report_note = "리포트: 없음"
    print(report_note, flush=True)
    log_lines.append(report_note)

    if args.skip_email:
        _append_run_log(log_lines)
        return main_exit

    if not email_configured():
        msg = (
            "[run_daily_email] 이메일 설정 미비 — "
            "EMAIL_SMTP_USER, EMAIL_SMTP_PASSWORD, EMAIL_RECIPIENTS 를 .env 에 추가하세요."
        )
        print(msg, file=sys.stderr)
        log_lines.append(msg)
        _append_run_log(log_lines)
        return main_exit if main_exit == 0 else main_exit

    prefix = config.EMAIL_SUBJECT_PREFIX.strip()
    subject = (
        f"{prefix} N={n_day:%Y-%m-%d} → T={t_day:%Y-%m-%d} ({slot_label})"
        if prefix
        else f"Money 리포트 N={n_day:%Y-%m-%d} → T={t_day:%Y-%m-%d} ({slot_label})"
    )
    body = _build_email_body(
        slot=args.slot,
        n_day=n_day,
        t_day=t_day,
        main_exit=main_exit,
        log_text=log_text,
        report_path=report_path if report_path.is_file() else None,
    )
    attachments = [report_path] if report_path.is_file() else []

    try:
        send_report_email(subject=subject, body_text=body, attachment_paths=attachments)
        ok = f"[run_daily_email] 이메일 발송 완료 → {', '.join(parse_recipients())}"
        print(ok)
        log_lines.append(ok)
    except Exception as exc:
        err = f"[run_daily_email] 이메일 발송 실패: {exc}"
        print(err, file=sys.stderr)
        log_lines.append(err)
        hint = _smtp_auth_hint(exc)
        if hint:
            print(f"[run_daily_email] {hint}", file=sys.stderr)
            log_lines.append(hint)
        _append_run_log(log_lines)
        return 1 if main_exit == 0 else main_exit

    _append_run_log(log_lines)
    return main_exit


if __name__ == "__main__":
    raise SystemExit(main())
