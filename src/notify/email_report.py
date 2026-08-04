"""스케줄 실행 리포트 HTML·로그를 SMTP 로 발송."""
from __future__ import annotations

import mimetypes
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path

from src import config


def parse_recipients(raw: str | None = None) -> list[str]:
    """쉼표·세미콜론·공백 구분 수신자 목록."""
    text = (raw if raw is not None else config.EMAIL_RECIPIENTS_RAW).strip()
    if not text:
        return []
    for sep in (";", "\n"):
        text = text.replace(sep, ",")
    return [p.strip() for p in text.split(",") if p.strip()]


def email_configured() -> bool:
    """SMTP 발송에 필요한 최소 설정이 있는지."""
    if not config.EMAIL_ENABLED:
        return False
    if not parse_recipients():
        return False
    if not config.EMAIL_SMTP_HOST.strip():
        return False
    if not config.EMAIL_SMTP_USER.strip() or not config.EMAIL_SMTP_PASSWORD:
        return False
    sender = (config.EMAIL_FROM or config.EMAIL_SMTP_USER).strip()
    return bool(sender)


def send_report_email(
    *,
    subject: str,
    body_text: str,
    attachment_paths: list[Path] | None = None,
    recipients: list[str] | None = None,
) -> None:
    """
    리포트 HTML 등을 첨부해 이메일을 보냅니다.

    Raises:
        RuntimeError: 설정 미비 또는 SMTP 오류.
    """
    if not config.EMAIL_ENABLED:
        raise RuntimeError("EMAIL_ENABLED=0 — 이메일 발송이 꺼져 있습니다.")
    to_addrs = recipients if recipients is not None else parse_recipients()
    if not to_addrs:
        raise RuntimeError("EMAIL_RECIPIENTS 가 비어 있습니다.")
    host = config.EMAIL_SMTP_HOST.strip()
    if not host:
        raise RuntimeError("EMAIL_SMTP_HOST 가 비어 있습니다.")
    user = config.EMAIL_SMTP_USER.strip()
    password = config.EMAIL_SMTP_PASSWORD
    if not user or not password:
        raise RuntimeError("EMAIL_SMTP_USER / EMAIL_SMTP_PASSWORD 를 .env 에 설정하세요.")
    sender = (config.EMAIL_FROM or user).strip()

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(body_text)

    for path in attachment_paths or []:
        p = Path(path)
        if not p.is_file():
            continue
        ctype, _ = mimetypes.guess_type(str(p))
        if ctype is None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)
        msg.add_attachment(
            p.read_bytes(),
            maintype=maintype,
            subtype=subtype,
            filename=p.name,
        )

    port = config.EMAIL_SMTP_PORT
    use_tls = config.EMAIL_USE_TLS
    timeout = config.EMAIL_SMTP_TIMEOUT_SEC

    if use_tls and port == 465:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, timeout=timeout, context=context) as smtp:
            smtp.login(user, password)
            smtp.send_message(msg)
        return

    with smtplib.SMTP(host, port, timeout=timeout) as smtp:
        if use_tls:
            smtp.starttls(context=ssl.create_default_context())
        smtp.login(user, password)
        smtp.send_message(msg)
