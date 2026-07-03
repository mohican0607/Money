"""리포트 tooltip 장중 등락률 — 로컬 API + ``live_quotes.js`` 자동 갱신."""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from src import stocks

DEFAULT_PORT = 8765
MANIFEST_NAME = "live_quotes_manifest.json"
JS_NAME = "live_quotes.js"
_POLL_SEC = 10.0


def _fetch_quotes(codes: list[str]) -> dict[str, float]:
    return stocks.try_naver_realtime_fluctuations_pct_by_codes(codes)


def manifest_path(output_dir: Path | str) -> Path:
    return Path(output_dir) / MANIFEST_NAME


def js_path(output_dir: Path | str) -> Path:
    return Path(output_dir) / JS_NAME


def write_live_quotes_manifest(output_dir: Path | str, codes: list[str]) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    uniq = sorted({str(c).zfill(6) for c in codes if str(c).strip()})
    manifest_path(out).write_text(
        json.dumps({"codes": uniq}, ensure_ascii=False),
        encoding="utf-8",
    )


def write_live_quotes_js(output_dir: Path | str, quotes: dict[str, float]) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    payload = {str(k).zfill(6): float(v) for k, v in quotes.items()}
    body = (
        "window.__MONEY_LIVE_QUOTES__="
        + json.dumps(payload, ensure_ascii=False)
        + ";\n"
    )
    js_path(out).write_text(body, encoding="utf-8")


def collect_stock_codes_from_day_reports(day_reports: list[Any]) -> list[str]:
    codes: set[str] = set()
    for dr in day_reports or []:
        for r in getattr(dr, "rows_compare", None) or []:
            c = str(r.get("code", "")).zfill(6)
            if c.isdigit() and len(c) == 6:
                codes.add(c)
    return sorted(codes)


def ensure_live_quotes_for_report(
    output_dir: Path | str,
    codes: list[str] | None = None,
    *,
    port: int = DEFAULT_PORT,
) -> None:
    """리포트 output 폴더에 manifest·JS를 쓰고 백그라운드 갱신 데몬을 띄웁니다."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    if codes:
        write_live_quotes_manifest(out, codes)
        snap = _fetch_quotes(codes)
        if snap:
            write_live_quotes_js(out, snap)
    _ensure_daemon_process(out, port=port)


class LiveQuotesHandler(BaseHTTPRequestHandler):
    server_version = "MoneyLiveQuotes/1.0"

    output_dir: Path = Path(".")

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.getenv("LIVE_QUOTES_QUIET", "").strip().lower() in ("1", "true", "yes"):
            return
        super().log_message(fmt, *args)

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._json_response(200, {"ok": True})
            return
        if parsed.path == f"/{JS_NAME}":
            p = js_path(self.output_dir)
            if not p.is_file():
                self.send_error(404)
                return
            body = p.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/javascript; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path != "/api/quotes":
            self.send_error(404)
            return
        raw = parse_qs(parsed.query).get("codes", [""])[0]
        req_codes = [c.strip() for c in str(raw).split(",") if c.strip()]
        if not req_codes:
            self._json_response(400, {"error": "codes required"})
            return
        quotes = _fetch_quotes(req_codes)
        self._json_response(200, {"quotes": quotes})

    def _json_response(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _read_manifest_codes(output_dir: Path) -> list[str]:
    p = manifest_path(output_dir)
    if not p.is_file():
        return []
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        raw = obj.get("codes") if isinstance(obj, dict) else []
        return sorted({str(c).zfill(6) for c in raw if str(c).strip()})
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return []


def _poll_loop(output_dir: Path) -> None:
    while True:
        codes = _read_manifest_codes(output_dir)
        if codes:
            try:
                snap = _fetch_quotes(codes)
                if snap:
                    write_live_quotes_js(output_dir, snap)
            except Exception:
                pass
        time.sleep(_POLL_SEC)


def run_live_quotes_daemon(output_dir: Path | str, *, port: int = DEFAULT_PORT, host: str = "127.0.0.1") -> None:
    out = Path(output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)

    def _handler(*args: Any, **kwargs: Any) -> LiveQuotesHandler:
        h = LiveQuotesHandler(*args, **kwargs)
        h.output_dir = out
        return h

    threading.Thread(target=_poll_loop, args=(out,), daemon=True).start()
    httpd = ThreadingHTTPServer((host, port), _handler)
    if os.getenv("LIVE_QUOTES_QUIET", "").strip().lower() not in ("1", "true", "yes"):
        print(
            f"장중 등락률 데몬: http://{host}:{port}/api/quotes · {out / JS_NAME}",
            flush=True,
        )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n장중 등락률 데몬 종료", flush=True)


def run_live_quotes_server(*, port: int = DEFAULT_PORT, host: str = "127.0.0.1") -> None:
    """레거시: output 폴더 없이 API만."""
    out = Path(os.getenv("MONEY_OUTPUT_DIR", "output"))
    run_live_quotes_daemon(out, port=port, host=host)


def health_ok(port: int = DEFAULT_PORT, *, host: str = "127.0.0.1", timeout: float = 0.35) -> bool:
    url = f"http://{host}:{port}/health"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status == 200
    except (OSError, urllib.error.URLError, ValueError):
        return False


def _ensure_daemon_process(output_dir: Path, *, port: int = DEFAULT_PORT) -> None:
    if health_ok(port):
        return
    import subprocess

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cmd = [
        sys.executable,
        "-m",
        "src.report.live_quotes",
        "--daemon",
        str(output_dir.resolve()),
        "--port",
        str(port),
    ]
    kwargs: dict[str, Any] = {"cwd": root}
    if sys.platform == "win32":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    subprocess.Popen(cmd, **kwargs)
    for _ in range(20):
        if health_ok(port):
            return
        time.sleep(0.15)


def ensure_live_quotes_server_running(*, port: int = DEFAULT_PORT, host: str = "127.0.0.1") -> None:
    """``main.py`` 시작 시 output 기본 경로용 데몬 기동."""
    from src import config

    ensure_live_quotes_for_report(config.OUTPUT_DIR, port=port)


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="리포트 tooltip 장중 등락률 데몬")
    p.add_argument("--daemon", metavar="OUTPUT_DIR", default="")
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--host", default="127.0.0.1")
    args = p.parse_args()
    if args.daemon:
        run_live_quotes_daemon(args.daemon, port=args.port, host=args.host)
    else:
        run_live_quotes_server(port=args.port, host=args.host)


if __name__ == "__main__":
    main()
