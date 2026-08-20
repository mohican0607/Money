"""장중 등락률 로컬 데몬·JS sidecar."""
from __future__ import annotations

import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from src.report.live_quotes import (
    LiveQuotesHandler,
    js_path,
    write_live_quotes_js,
    write_live_quotes_manifest,
)


def test_live_quotes_api(tmp_path: Path, monkeypatch) -> None:
    out = tmp_path / "output"
    out.mkdir()
    write_live_quotes_manifest(out, ["005930"])
    monkeypatch.setattr(
        "src.report.live_quotes._fetch_quotes",
        lambda codes: {"005930": 1.23},
    )

    def _handler(*args, **kwargs):
        h = LiveQuotesHandler(*args, **kwargs)
        h.output_dir = out
        return h

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _handler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/quotes?codes=005930",
            timeout=3,
        ) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        assert data["quotes"]["005930"] == 1.23
        write_live_quotes_js(out, {"005930": 4.5})
        assert "005930" in js_path(out).read_text(encoding="utf-8")
    finally:
        httpd.shutdown()


def test_collect_stock_codes_from_html() -> None:
    from src.report.live_quotes import collect_stock_codes_from_html

    html = (
        '<span data-stock-code="011080"></span>'
        '<span class="pred-live-intraday-pct" data-stock-code="043260">…</span>'
        '<span data-stock-code="011080"></span>'
    )
    assert collect_stock_codes_from_html(html) == ["011080", "043260"]


def test_ensure_writes_js_even_when_fetch_empty(tmp_path: Path, monkeypatch) -> None:
    from src.report.live_quotes import ensure_live_quotes_for_report, js_path

    out = tmp_path / "output"
    out.mkdir()
    monkeypatch.setattr("src.report.live_quotes._fetch_quotes", lambda codes: {})
    monkeypatch.setattr("src.report.live_quotes._ensure_daemon_process", lambda *a, **k: None)
    ensure_live_quotes_for_report(out, ["005930"])
    body = js_path(out).read_text(encoding="utf-8")
    assert "window.__MONEY_LIVE_QUOTES__" in body


def test_inject_live_quotes_script_despite_js_filename_in_body() -> None:
    from src.report.render import _ensure_report_interaction_script

    html = (
        "<!DOCTYPE html><html><head></head><body>\n"
        '<script>s.src = "live_quotes.js?t=" + Date.now();</script>\n'
        "</body></html>"
    )
    out = _ensure_report_interaction_script(html)
    assert 'id="money-live-quotes-js"' in out
    assert out.index('id="money-live-quotes-js"') < out.lower().index("</head>")


def test_interaction_snippet_hydrates_today_n_line() -> None:
    from src.report.templates import REPORT_TABLE_INTERACTION_SNIPPET

    assert "hydrateTodayLiveIntraday" in REPORT_TABLE_INTERACTION_SNIPPET
    assert "kstTodayIso" in REPORT_TABLE_INTERACTION_SNIPPET
