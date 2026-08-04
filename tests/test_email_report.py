"""이메일 설정 파싱."""
from __future__ import annotations

from src.notify.email_report import parse_recipients


def test_parse_recipients_comma_separated() -> None:
    raw = "a@example.com, b@example.com ; c@example.com"
    assert parse_recipients(raw) == [
        "a@example.com",
        "b@example.com",
        "c@example.com",
    ]


def test_parse_recipients_empty() -> None:
    assert parse_recipients("") == []
    assert parse_recipients("  ,  ") == []
