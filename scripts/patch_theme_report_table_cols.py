#!/usr/bin/env python
"""report_theme_25pct HTML 테이블 열 정렬(colgroup·클래스) 일괄 보강."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

COLGROUP = """      <colgroup>
        <col class="col-stock"/><col class="col-theme"/><col class="col-market"/>
        <col class="col-chg"/><col class="col-reason"/>
      </colgroup>"""

OLD_HEAD = (
    '<thead><tr><th>종목</th><th>테마</th><th>시장</th>'
    '<th>등락</th><th>근거 요약</th></tr></thead>'
)
NEW_HEAD = """<thead><tr>
        <th class="col-stock">종목</th><th class="col-theme">테마</th><th class="col-market">시장</th>
        <th class="col-chg">등락</th><th class="col-reason">근거 요약</th>
      </tr></thead>"""

ROW_PAT = re.compile(
    r"<tr>\s*<td><strong>(.*?)</strong> <code>(\d{6})</code></td>\s*"
    r'<td class="theme">(.*?)</td>\s*'
    r'<td class="market">(.*?)</td>\s*'
    r'<td class="ok">(.*?)</td>\s*'
    r'<td class="reason muted">(.*?)</td>\s*</tr>',
    re.S,
)


def patch(path: Path) -> tuple[int, int]:
    html = path.read_text(encoding="utf-8")
    n_tables = html.count(OLD_HEAD)
    html = html.replace(
        f'    <table class="stocks">\n      {OLD_HEAD}',
        f'    <table class="stocks">\n{COLGROUP}\n      {NEW_HEAD}',
    )

    def _repl(m: re.Match[str]) -> str:
        return (
            "<tr>\n"
            f'          <td class="col-stock"><strong>{m.group(1)}</strong> '
            f"<code>{m.group(2)}</code></td>\n"
            f'          <td class="col-theme theme">{m.group(3)}</td>\n'
            f'          <td class="col-market market">{m.group(4)}</td>\n'
            f'          <td class="col-chg ok">{m.group(5)}</td>\n'
            f'          <td class="col-reason reason muted">{m.group(6)}</td>\n'
            "        </tr>"
        )

    html, n_rows = ROW_PAT.subn(_repl, html)
    path.write_text(html, encoding="utf-8")
    return n_tables, n_rows


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "output" / "report_theme_25pct_2026.07.html"
    t, r = patch(target)
    print(f"patched {target.name}: tables={t} rows={r}")
