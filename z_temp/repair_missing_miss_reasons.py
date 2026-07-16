# -*- coding: utf-8 -*-
"""Replace generic 「틀린 이유」 with diagnostics scraped from the same row."""
from __future__ import annotations

import html
import re
import sys
from pathlib import Path

from bs4 import BeautifulSoup, NavigableString, Tag

ROOT = Path(__file__).resolve().parents[1]
REPORT = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "output" / "report_2026.07.html"

GARBAGE_SNIPPETS = (
    "실제 주가가 하락해 예측 방향과도 반대로 움직였습니다",
    "상승 동력이 약해 10% 구간에도 도달하지 못했습니다",
    "상승은 있었지만 급등 임계 20%까지 후속 매수세가 이어지지 않았습니다",
    "통합 보기의 예측 신호·뉴스·공시와 함께 확인하세요",
)


def _plain(el: Tag | None, limit: int = 0) -> str:
    if el is None:
        return ""
    text = re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()
    if limit and len(text) > limit:
        return text[: limit - 1].rstrip() + "…"
    return text


def _numeric(tr: Tag, col: str) -> float | None:
    td = tr.select_one(f"[data-sort-col={col}]")
    if td is None:
        return None
    raw = (td.get("data-sort-value") or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _integrate_bodies(tr: Tag) -> dict[str, Tag]:
    out: dict[str, Tag] = {}
    tip = tr.select_one(".integrate-tip")
    if tip is None:
        return out
    for col in tip.select(".combo-tip-col"):
        h = col.select_one(".combo-tip-h")
        b = col.select_one(".combo-tip-body")
        if h is None or b is None:
            continue
        out[h.get_text(strip=True)] = b
    return out


def _extract_factor_bits(reason_text: str) -> list[str]:
    hints: list[str] = []
    m = re.search(r"순위\s*(\d+)\s*/\s*(\d+)", reason_text)
    if m:
        hints.append(f"랭킹 {m.group(1)}/{m.group(2)}위였지만 실제 급등 임계에 미달")
    m = re.search(r"키워드\s*(\d+)", reason_text)
    if m and int(m.group(1)) <= 1:
        hints.append(f"예측 시점 키워드 매칭 {m.group(1)}개 — 뉴스 근거가 약했음")
    if "테마 캐리오버" in reason_text or "캐리오버" in reason_text:
        hints.append("전일 테마 캐리오버 가중치에 기댔는데 당일 가격이 따라오지 않음")
    m = re.search(r"ML\s*(\d+(?:\.\d+)?)%", reason_text)
    mom = re.search(r"모멘텀\s*(\d+(?:\.\d+)?)%", reason_text)
    if m and float(m.group(1)) >= 80 and mom and float(mom.group(1)) < 50:
        hints.append(
            f"ML 점수 {m.group(1)}%는 높았지만 모멘텀 {mom.group(1)}%로 가격 추세가 약했음"
        )
    elif m and float(m.group(1)) >= 90:
        hints.append(
            f"ML {m.group(1)}% 고점수였으나 실제 급등으로 이어지지 않음(허위 양성 후보)"
        )
    return hints


def _build_hints(
    *,
    pred_pct: float,
    actual_ratio: float,
    reason_text: str,
    gap_text: str,
    signal_text: str,
) -> list[str]:
    actual_pct = actual_ratio * 100.0
    gap = pred_pct - actual_pct
    hints: list[str] = []

    if actual_pct < 0:
        hints.append(
            f"실제 {actual_pct:.2f}% 하락 — 예측 {pred_pct:.2f}%와 방향이 반대"
        )
    elif actual_pct + 1e-9 < 20.0:
        hints.append(
            f"실제 {actual_pct:.2f}%로 급등 임계 20% 미달 (예측 {pred_pct:.2f}%, 차이 {gap:+.2f}pp)"
        )

    if gap > 8.0:
        hints.append(f"과대예측 {gap:.1f}pp — 표시 상승률이 과거 급등 평균·클램프에 끌려 높아짐")

    hints.extend(_extract_factor_bits(reason_text))

    if "키워드 0" in reason_text or "키워드 0" in signal_text:
        if "예측 시점 키워드" not in " ".join(hints):
            hints.append("키워드 0 — 종목명/테마 규칙·ML 점수 위주 선정")

    # Pull concrete facts already written in gap analysis (sentence-ish chunks).
    for pat in (
        r"당일 KOSPI[^.%]{0,60}?-?\d+(?:\.\d+)?%",
        r"N-1일.{0,140}?(?:있습니다|없습니다|수 있습니다)",
        r"지연 구간.{0,120}?(?:있습니다|없습니다|수 있습니다)",
        r"네이버 증권 종목 공시에서 당일 관련 공시.{0,100}?(?:습니다|다)",
        r"예측 시\s*\d+개\s*키워드.{0,80}?(?:습니다|다)",
    ):
        m = re.search(pat, gap_text)
        if m:
            piece = m.group(0).strip().rstrip("。.").strip()
            if piece and piece not in hints:
                hints.append(piece)

    # Dedup while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for h in hints:
        h = h.strip()
        if not h or h in seen:
            continue
        seen.add(h)
        out.append(h)
    return out[:7]


def _is_garbage_miss(body: Tag | None) -> bool:
    if body is None:
        return True
    text = body.get_text(" ", strip=True)
    return any(s in text for s in GARBAGE_SNIPPETS)


def _render_miss_html(
    *,
    day: str,
    pred_pct: float,
    actual_ratio: float,
    hints: list[str],
    gap_body: Tag | None,
) -> str:
    actual_pct = actual_ratio * 100.0
    parts: list[str] = [
        "<p><strong>미적중 요약</strong> — "
        f"예측 <strong>{pred_pct:.2f}%</strong> · "
        f"실제 <strong>{actual_pct:.2f}%</strong> · "
        f"차이 <strong>{pred_pct - actual_pct:+.2f}</strong>pp ({html.escape(day)})</p>"
    ]
    if hints:
        parts.append('<p><strong>진단</strong></p><ul class="nl">')
        for h in hints:
            parts.append(f"<li>{html.escape(h, quote=False)}</li>")
        parts.append("</ul>")
    if gap_body is not None:
        # Keep existing gap HTML (already escaped/safe from report).
        gap_html = "".join(str(c) for c in gap_body.contents).strip()
        if gap_html:
            parts.append(
                '<div style="margin-top:8px"><strong>예측·실제 차이</strong>'
                f"{gap_html}</div>"
            )
    return "".join(parts)


def _ensure_miss_tip(reason_td: Tag, inner_html: str) -> None:
    existing = reason_td.select_one(".pred-miss-tip")
    wrapper = (
        '<span class="gap-tip pred-miss-tip" style="margin-left:8px">'
        '<span class="gap-tip-trigger" tabindex="0" role="button" '
        'aria-label="틀린 이유">틀린 이유</span>'
        '<div class="gap-tip-popup pred-miss-popup" role="tooltip">'
        f'<div class="combo-tip-body">{inner_html}</div></div></span>'
    )
    new_tip = BeautifulSoup(wrapper, "html.parser").span
    assert new_tip is not None
    if existing is not None:
        existing.replace_with(new_tip)
    else:
        reason_td.append(NavigableString("\n"))
        reason_td.append(new_tip)
        reason_td.append(NavigableString("\n"))


def _process_row(tr: Tag, day: str) -> bool:
    text = tr.get_text(" ", strip=True)
    if "고확신" not in text and "중확신" not in text:
        return False
    pred = _numeric(tr, "pred")
    actual = _numeric(tr, "actual")
    if pred is None or actual is None or actual >= 0.20:
        return False

    reason_td = tr.select_one("td.pred-reason-forward")
    if reason_td is None:
        return False

    miss_body = tr.select_one(".pred-miss-tip .combo-tip-body")
    has_tip = miss_body is not None
    miss_text = miss_body.get_text(" ", strip=True) if miss_body else ""
    # 쓰레기 문구이거나, 이 스크립트가 채운 「진단」블록은 재생성 대상.
    if has_tip and not _is_garbage_miss(miss_body) and "진단" not in miss_text:
        return False

    integ = _integrate_bodies(tr)
    gap_body = integ.get("예측·실제 차이")
    reason_tip = reason_td.select_one(".gap-tip:not(.pred-miss-tip) .combo-tip-body")
    reason_text = _plain(reason_tip) or _plain(integ.get("예측 이유")) or _plain(reason_td)
    gap_text = _plain(gap_body)
    signal_td = tr.select_one("[data-sort-col=signal]")
    signal_text = _plain(signal_td)

    hints = _build_hints(
        pred_pct=pred,
        actual_ratio=actual,
        reason_text=reason_text,
        gap_text=gap_text,
        signal_text=signal_text,
    )
    inner = _render_miss_html(
        day=day,
        pred_pct=pred,
        actual_ratio=actual,
        hints=hints,
        gap_body=gap_body,
    )
    _ensure_miss_tip(reason_td, inner)
    return True


def main() -> None:
    source = REPORT.read_text(encoding="utf-8")
    soup = BeautifulSoup(source, "html.parser")
    changed = 0
    for sec in soup.select("section[id^=day-]"):
        day = (sec.get("id") or "").replace("day-", "")
        if not day:
            continue
        for tr in sec.select("table.rows-compare > tbody > tr"):
            if _process_row(tr, day):
                changed += 1

    backup = REPORT.with_suffix(REPORT.suffix + ".bak_miss_reason_v2")
    if not backup.exists():
        backup.write_text(source, encoding="utf-8")
    REPORT.write_text(str(soup), encoding="utf-8")
    print(f"교체 완료: {changed}행 → {REPORT}")


if __name__ == "__main__":
    main()
