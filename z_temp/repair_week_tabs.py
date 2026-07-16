"""Fast repair of week tab-panel leaks in monthly report HTML."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.report.render import _match_balanced_div  # noqa: E402

_DAY_RE = re.compile(
    r'<section\s[^>]*\bid="day-(\d{4}-\d{2}-\d{2})"[^>]*>.*?</section>',
    re.DOTALL | re.IGNORECASE,
)


def repair_day_fragment(frag: str) -> str:
    m = _match_balanced_div(frag, "market-theme-ref")
    if m is None:
        return frag
    after = frag[m.end() :]
    cut = re.search(
        r'(?is)(<div\s+class="hit-at-k-panel\b|<table\b class="rows-compare"|'
        r'<table class="rows-compare"|<h3\b[^>]*>예측)',
        after,
    )
    if cut and cut.start() > 0:
        gap = after[: cut.start()]
        if (
            "combo-tip-empty" in gap
            or "theme-sector-block" in gap
            or re.search(
                r'<p class="sub"[^>]*>\s*<strong>\d{4}-\d{2}-\d{2}</strong>', gap
            )
            or gap.count("</div>") > 0
        ):
            frag = frag[: m.end()] + "\n        " + after[cut.start() :]

    opens = len(re.findall(r"<div\b", frag, flags=re.I))
    closes = frag.count("</div>")
    if closes > opens:
        excess = closes - opens
        body, sep, tail = frag.rpartition("</section>")
        if sep:
            for _ in range(excess):
                body = re.sub(r"</div>\s*$", "", body.rstrip())
            frag = body + sep + tail
    return frag


def repair_file(path: Path) -> None:
    html = path.read_text(encoding="utf-8")
    matches = list(_DAY_RE.finditer(html))
    if not matches:
        raise SystemExit("no day sections")
    # rebuild from end so offsets stay valid
    out = html
    n_fixed = 0
    for m in reversed(matches):
        frag = m.group(0)
        fixed = repair_day_fragment(frag)
        if fixed != frag:
            out = out[: m.start()] + fixed + out[m.end() :]
            n_fixed += 1
            print("fixed", m.group(1))
    print(f"repaired {n_fixed}/{len(matches)} day sections")

    # verify panels
    wm = re.search(
        r'<section class="tabs-wrap week-tabs-wrap">(.*?)</section>\s*<script>',
        out,
        re.S,
    )
    if wm:
        body = wm.group(1)
        pos = 0
        pi = 0
        while True:
            pm = _match_balanced_div(body, "tab-panel", start=pos)
            if pm is None:
                break
            days = re.findall(r'id="day-(\d{4}-\d{2}-\d{2})"', pm.group(0))
            print(f"panel {pi}: {days}")
            pos = pm.end()
            pi += 1

    bak = path.with_suffix(path.suffix + ".bak_weektabs")
    if not bak.exists():
        bak.write_text(html, encoding="utf-8")
        print("backup", bak.name)
    path.write_text(out, encoding="utf-8")
    print("wrote", path)


if __name__ == "__main__":
    p = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "output" / "report_2026.07.html"
    repair_file(p)
