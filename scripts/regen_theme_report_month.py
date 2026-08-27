"""캐시 OHLCV·뉴스만으로 ``report_theme_25pct_YYYY.MM.html`` 을 재생성(예측 생략)."""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config, stocks, trading_calendar
from src.learning import market_theme as srl
from src.pipeline.support import (
    _collect_calendar_days_for_trading_range,
    _fetch_news_for_calendar_days,
)
from src.report import content as theme_report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("year", type=int)
    ap.add_argument("month", type=int)
    args = ap.parse_args()
    y, m = args.year, args.month

    first = date(y, m, 1)
    if m == 12:
        month_end = date(y + 1, 1, 1) - __import__("datetime").timedelta(days=1)
    else:
        month_end = date(y, m + 1, 1) - __import__("datetime").timedelta(days=1)
    # 해당 월 거래일
    days = trading_calendar.trading_sessions_in_range(first, month_end)
    if not days:
        print("거래일 없음", flush=True)
        return 1

    print(f"OHLCV 로드… ({days[0]} ~ {days[-1]}, {len(days)}일)", flush=True)
    listing = stocks.load_listing()
    names = {
        str(r["Code"]).zfill(6): str(r["Name"])
        for _, r in listing.iterrows()
    }
    ohlcv = stocks.build_ohlcv_long(
        start=min(days) - __import__("datetime").timedelta(days=10),
        end=max(days),
        skip_gap_download=True,
    )
    returns_df = stocks.daily_returns_table(ohlcv)

    cal_days = _collect_calendar_days_for_trading_range(
        days,
        include_target_calendar_days=True,
        target_calendar_trading_days=frozenset(days),
    )
    print(f"뉴스 캐시 로드… ({len(cal_days)} 캘린더일)", flush=True)
    news_by_calendar = _fetch_news_for_calendar_days(cal_days)

    print("테마 flow 계산…", flush=True)
    theme_rows = srl.build_market_theme_flow(
        days, news_by_calendar, returns_df, names
    )
    day_reports = [
        SimpleNamespace(
            trading_day=d,
            forward_observation=False,
            rows_compare=[],
        )
        for d in days
    ]
    cutoff = (
        f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}"
    )
    out = config.OUTPUT_DIR / theme_report.theme_report_filename_for_month(y, m)
    path = theme_report.render_strong_mover_theme_report(
        out,
        day_reports=day_reports,
        theme_flow_rows=theme_rows,
        news_by_calendar=news_by_calendar,
        listing_names=names,
        news_cutoff_label=cutoff,
        title=f"25%↑ 급등 테마·상승 배경 · {out.name.replace('.html', '')}",
        subtitle=(
            f"{y}년 {m}월 · 25%↑ 테마 거래일 {days[0].isoformat()} ~ "
            f"{days[-1].isoformat()} ({len(days)}일)"
        ),
        meta_note="캐시 재생성(상승요인 근거 갱신)",
        preserved_day_html=None,
    )
    if not path:
        print("섹션 없음 — 저장 실패", flush=True)
        return 1
    text = path.read_text(encoding="utf-8")
    n_unknown = text.count("원인 미확인")
    n_unconf = text.count("미확인")
    print(f"완료: {path}", flush=True)
    print(f"원인 미확인={n_unknown}, 미확인={n_unconf}", flush=True)
    return 0 if n_unknown == 0 and n_unconf == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
