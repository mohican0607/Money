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
