"""리포트 HTML에서 25%↑ 종목 중 예측 신호 테마 누락 집계."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import snapshot_rebuild_learning as srl  # noqa: E402

THEME_MARK = 'title="25%↑ 이상 급등·예측 종목의 당일 테마'


def _max_pct(actual_sort: str | None, pred_sort: str | None) -> float | None:
    vals: list[float] = []
    if actual_sort is not None and actual_sort.strip():
        try:
            v = float(actual_sort)
            vals.append(srl._compare_row_actual_ret_pct_points(v))
        except ValueError:
            pass
    if pred_sort is not None and pred_sort.strip():
        try:
            vals.append(float(pred_sort))
        except ValueError:
            pass
    return max(vals) if vals else None


def _day_chunk(html: str, day: str, all_days: list[str]) -> str:
    marker = f"<strong>{day}</strong> 종가"
    idx = html.find(marker)
    if idx < 0:
        return ""
    chunk = html[idx:]
    ends = []
    for other in all_days:
        if other == day:
            continue
        j = chunk.find(f"<strong>{other}</strong> 종가", 500)
        if j > 0:
            ends.append(j)
    if ends:
        chunk = chunk[: min(ends)]
    return chunk


def analyze_report(html_path: Path, days: list[str]) -> None:
    html = html_path.read_text(encoding="utf-8", errors="replace")
    total_missing = 0
    total_eligible = 0
    for day in days:
        chunk = _day_chunk(html, day, days)
        if not chunk:
            print(f"{day}: section NOT FOUND")
            continue

        missing: list[tuple[str, float, str, str]] = []
        themed = 0
        eligible = 0
        for tr in re.split(r"(?i)<tr\b", chunk):
            m_act = re.search(r'data-sort-col="actual"\s+data-sort-value="([^"]*)"', tr)
            if not m_act:
                continue
            m_code = re.search(
                r'data-sort-col="code"\s+data-sort-value="(\d{6})"', tr
            ) or re.search(r'data-code="(\d{6})"', tr)
            if not m_code:
                m_stock = re.search(
                    r'data-sort-col="stock"\s+data-sort-value="[^"]*\s(\d{6})"', tr
                )
                if m_stock:
                    m_code = m_stock
            m_pred = re.search(r'data-sort-col="pred"\s+data-sort-value="([^"]*)"', tr)
            m_name = re.search(r'data-sort-col="name"[^>]*>([^<]+)<', tr)
            code = m_code.group(1) if m_code else "??????"
            name = (m_name.group(1).strip() if m_name else "?")[:24]
            max_p = _max_pct(
                m_act.group(1) if m_act else None,
                m_pred.group(1) if m_pred else None,
            )
            if max_p is None or max_p + 1e-9 < srl._STRONG_MOVER_PCT:
                continue
            eligible += 1
            has_theme = THEME_MARK in tr
            if has_theme:
                themed += 1
            else:
                sig_snip = tr[tr.rfind("<td"):][:120].replace("\n", " ")
                missing.append((code, max_p, name, sig_snip))

        total_missing += len(missing)
        total_eligible += eligible
        print(f"=== {day} ===")
        print(f"25%+ compare rows: {eligible}, themed: {themed}, MISSING: {len(missing)}")
        for code, pct, name, sig in missing[:12]:
            print(f"  {code} {name.encode('ascii','replace').decode()} max={pct:.1f}%")
        if len(missing) > 12:
            print(f"  ... +{len(missing) - 12} more")
    print(f"\nTOTAL 25%+ rows: {total_eligible}, MISSING themes: {total_missing}")


def main() -> None:
    report = ROOT / "output" / "report_2026.06.html"
    if not report.is_file():
        print(f"Missing {report}")
        return
    analyze_report(
        report,
        ["2026-06-08", "2026-06-09", "2026-06-10", "2026-06-11"],
    )


if __name__ == "__main__":
    main()
