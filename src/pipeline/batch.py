"""월간 HTML 배치 렌더."""
from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

from src import (
    config,
    report,
    snapshot_rebuild_learning,
    theme_strong_mover_report,
    trading_calendar,
)

from .rows import (
    _enrich_cumulative_accuracy_avg,
    _enrich_cumulative_hit_rate,
    _enrich_cumulative_actual_over_pred_from_history,
)
from .support import _collect_calendar_days_for_trading_range, _fetch_news_for_calendar_days
from .types import PipelineOut

_MONTHLY_REPORT_RE = re.compile(r"^report_\d{4}\.\d{2}\.html$", re.IGNORECASE)


def _monthly_report_paths_for_po(po: PipelineOut) -> list[Path]:
    """이번 ``PipelineOut`` 과 인접 달(ISO 주 경계) 월간 리포트만 스캔합니다."""
    months: set[tuple[int, int]] = set()
    for dr in po.day_reports:
        y, m = dr.trading_day.year, dr.trading_day.month
        months.add((y, m))
        if m == 1:
            months.add((y - 1, 12))
        else:
            months.add((y, m - 1))
        if m == 12:
            months.add((y + 1, 1))
        else:
            months.add((y, m + 1))
    paths: list[Path] = []
    for y, m in sorted(months):
        p = config.OUTPUT_DIR / f"report_{y}.{m:02d}.html"
        if p.is_file():
            paths.append(p)
    return paths


def _supplement_stale_forward_preserved_reports(
    po: PipelineOut,
    *,
    merge_existing_monthly_days: bool,
) -> PipelineOut:
    """
    월간 HTML 병합으로 남은 **장 마감 후에도 예측 전용** 일자 블록을 자동 재처리합니다.
    """
    if not merge_existing_monthly_days:
        return po
    now_kst = datetime.now(trading_calendar.KST)
    in_po = {dr.trading_day for dr in po.day_reports}
    stale: list[date] = []
    for path in _monthly_report_paths_for_po(po):
        for d, html in report.extract_monthly_report_day_sections(path).items():
            if d in in_po:
                continue
            if report.preserved_day_section_is_stale_forward_observation(
                html, d, now_kst=now_kst
            ):
                stale.append(d)
    stale = sorted(set(stale))
    if not stale:
        return po
    print(
        "장 마감 후 미갱신 예측 전용 일자 자동 재처리: "
        + ", ".join(t.isoformat() for t in stale),
        flush=True,
    )
    from .run import _run_pipeline as run_pipeline

    end_date = max(stale + [dr.trading_day for dr in po.day_reports])
    extra = run_pipeline(
        stale,
        end_date,
        include_target_calendar_news=True,
        skip_ohlcv_gap_download=True,
    )
    by_day = {dr.trading_day: dr for dr in po.day_reports}
    for dr in extra.day_reports:
        by_day[dr.trading_day] = dr
    news_map = dict(po.news_by_calendar or {})
    news_map.update(extra.news_by_calendar or {})
    return PipelineOut(
        day_reports=sorted(by_day.values(), key=lambda x: x.trading_day),
        news_source=po.news_source,
        correlation_rows=po.correlation_rows,
        train_start=po.train_start,
        test_start=po.test_start,
        end_date=max(po.end_date, extra.end_date),
        late_below_n=po.late_below_n,
        late_below_kw=po.late_below_kw,
        late_gte_n=po.late_gte_n,
        late_gte_kw=po.late_gte_kw,
        movers_data_note=po.movers_data_note,
        returns_df=extra.returns_df or po.returns_df,
        news_by_calendar=news_map,
        listing_names=extra.listing_names or po.listing_names,
    )


def _render_monthly_batch(
    po: PipelineOut,
    *,
    test_range_label: str,
    merge_existing_monthly_days: bool = True,
) -> list[Path]:
    """
    ``PipelineOut.day_reports`` 를 달력 **월** 단위로 나눠 ``report_YYYY.MM.html`` 을 쓰고,
    ``report_index_monthly.html`` 목차를 갱신합니다.

    ``merge_existing_monthly_days`` 가 True이면 해당 월 HTML이 이미 있을 때
    이번 실행에 포함된 거래일만 새로 렌더하고, 나머지 일자 블록은 기존 HTML을 그대로 둡니다.
    ``report_theme_25pct_YYYY.MM.html`` 도 동일하게 병합합니다.

    Returns:
        생성·갱신된 HTML 경로 목록(폴더 자동 열기용).
    """
    meta_base = {
        "train_range": (
            f"{po.train_start} ~ 각 관측일 T 직전까지(워크포워드, "
            f"기본 관측 시작 {po.test_start})"
        ),
        "test_range": test_range_label,
        "threshold": f"{config.BIG_MOVE_THRESHOLD*100:.0f}%",
        "news_source": po.news_source,
        "correlation_rows": po.correlation_rows,
        "use_decision_cutoff": config.USE_DECISION_NEWS_INTRADAY_CUTOFF,
        "cutoff_kst": f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}",
        "run_subtitle": "",
        "ranking_mode": config.PRED_RANKING_MODE,
    }
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:

        def _pct(num: int, den: int) -> str:
            """late-뉴스 프로브용 백분율 문자열(분모 0이면 —)."""
            if den <= 0:
                return "—"
            return f"{100.0 * num / den:.1f}%"

        meta_base["late_news_probe"] = {
            "below_n": po.late_below_n,
            "below_kw": po.late_below_kw,
            "below_pct": _pct(po.late_below_kw, po.late_below_n),
            "gte_n": po.late_gte_n,
            "gte_kw": po.late_gte_kw,
            "gte_pct": _pct(po.late_gte_kw, po.late_gte_n),
        }

    if po.movers_data_note:
        meta_base["movers_data_note"] = po.movers_data_note

    po = _supplement_stale_forward_preserved_reports(
        po, merge_existing_monthly_days=merge_existing_monthly_days
    )

    month_batches: dict[tuple[int, int], list[report.DayReport]] = {}
    for dr in po.day_reports:
        t = dr.trading_day
        month_batches.setdefault((t.year, t.month), []).append(dr)

    sorted_months = sorted(month_batches.keys())
    written_paths: list[Path] = []

    for ym in sorted_months:
        batch = sorted(month_batches[ym], key=lambda x: x.trading_day)
        y, m = ym
        fname = f"report_{y}.{m:02d}.html"
        out_month = config.OUTPUT_DIR / fname
        preserved_day_html: dict[date, str] = {}
        if merge_existing_monthly_days and out_month.is_file():
            existing_sections = report.extract_monthly_report_day_sections(out_month)
            update_days = {dr.trading_day for dr in batch}
            preserved_day_html = {
                d: html
                for d, html in existing_sections.items()
                if d not in update_days
            }
            if existing_sections:
                n_keep = len(preserved_day_html)
                n_update = len(update_days & set(existing_sections.keys()))
                n_append = len(update_days - set(existing_sections.keys()))
                print(
                    f"월간 리포트 병합: {fname} — "
                    f"이번 실행 반영 {len(update_days)}일 "
                    f"(갱신 {n_update}, 추가 {n_append}), 기존 유지 {n_keep}일",
                    flush=True,
                )
        all_days = sorted(
            set(preserved_day_html.keys()) | {dr.trading_day for dr in batch}
        )
        first_d, last_d = all_days[0], all_days[-1]
        month_note = (
            f"{y}년 {m}월 · 포함 거래일 {first_d.isoformat()} ~ {last_d.isoformat()} "
            f"({len(all_days)}일, 주간 탭·탭 내 일자 순)"
        )
        cutoff_kst = (
            f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}"
        )
        now_kst = datetime.now(trading_calendar.KST)
        if preserved_day_html and po.returns_df is not None and po.listing_names:
            news_map = dict(po.news_by_calendar or {})
            need_news_days = [
                d
                for d in preserved_day_html
                if report.preserved_day_section_needs_market_theme_backfill(
                    preserved_day_html[d],
                    d,
                    now_kst=now_kst,
                )
            ]
            if need_news_days:
                cal_for_news = _collect_calendar_days_for_trading_range(
                    need_news_days,
                    include_target_calendar_days=True,
                    target_calendar_trading_days=frozenset(need_news_days),
                )
                missing_cal = [d for d in cal_for_news if d not in news_map]
                if missing_cal:
                    news_map.update(_fetch_news_for_calendar_days(missing_cal))
            preserved_day_html = report.backfill_preserved_day_sections_market_theme(
                preserved_day_html,
                news_by_calendar=news_map,
                returns_df=po.returns_df,
                listing_names=po.listing_names,
                news_cutoff_label=cutoff_kst,
                now_kst=now_kst,
            )
        if po.returns_df is not None and po.listing_names:
            theme_days = sorted(
                dr.trading_day for dr in batch if not dr.forward_observation
            )
            if theme_days:
                flow_rows = snapshot_rebuild_learning.build_market_theme_flow(
                    theme_days,
                    po.news_by_calendar or {},
                    po.returns_df,
                    po.listing_names,
                )
                by_flow = {
                    str(r.get("trading_day")): r
                    for r in flow_rows
                    if r.get("trading_day")
                }
                for dr in batch:
                    if dr.forward_observation or not dr.rows_compare:
                        continue
                    flow_row = by_flow.get(dr.trading_day.isoformat())
                    if not flow_row:
                        continue
                    early = snapshot_rebuild_learning._early_news_rows_for_trading_day(
                        po.news_by_calendar or {}, dr.trading_day
                    )
                    snapshot_rebuild_learning.enrich_compare_rows_theme_for_day(
                        dr,
                        flow_row,
                        listing_names=po.listing_names,
                        early_rows=early,
                        news_cutoff_label=cutoff_kst,
                    )
                    if not dr.market_theme_html:
                        dr.market_theme_html = (
                            snapshot_rebuild_learning.format_market_theme_flow_html(
                                flow_row,
                                news_cutoff_label=cutoff_kst,
                                threshold_pct=float(config.BIG_MOVE_THRESHOLD) * 100.0,
                            )
                            )
        report.render_compact_tabbed_report(
            title=f"실제 20%↑ 종목 · 예측 상승률 · {fname.replace('.html', '')}",
            days=batch,
            meta={**meta_base, "n_days": len(all_days)},
            out_path=out_month,
            week_note=month_note,
            week_tabs_stack_days=True,
            preserved_day_html=preserved_day_html or None,
        )
        written_paths.append(out_month)
        print(f"완료: {out_month}")

        theme_fname = theme_strong_mover_report.theme_report_filename_for_month(y, m)
        theme_path = config.OUTPUT_DIR / theme_fname
        theme_preserved: dict[date, str] = {}
        if merge_existing_monthly_days and theme_path.is_file():
            existing_theme = theme_strong_mover_report.extract_theme_report_day_sections(
                theme_path
            )
            update_theme_days = {
                dr.trading_day for dr in batch if not dr.forward_observation
            }
            theme_preserved = {
                d: html
                for d, html in existing_theme.items()
                if d not in update_theme_days
            }
            if existing_theme:
                n_keep = len(theme_preserved)
                n_update = len(update_theme_days & set(existing_theme.keys()))
                n_append = len(update_theme_days - set(existing_theme.keys()))
                print(
                    f"월간 테마 리포트 병합: {theme_fname} — "
                    f"이번 실행 반영 {len(update_theme_days)}일 "
                    f"(갱신 {n_update}, 추가 {n_append}), 기존 유지 {n_keep}일",
                    flush=True,
                )
        if (
            theme_preserved
            and po.returns_df is not None
            and po.listing_names
        ):
            news_map = dict(po.news_by_calendar or {})
            need_news_days = [
                d
                for d in theme_preserved
                if report.preserved_day_section_needs_market_theme_backfill(
                    theme_preserved[d],
                    d,
                    now_kst=datetime.now(trading_calendar.KST),
                )
            ]
            if need_news_days:
                cal_for_news = _collect_calendar_days_for_trading_range(
                    need_news_days,
                    include_target_calendar_days=True,
                    target_calendar_trading_days=frozenset(need_news_days),
                )
                missing_cal = [d for d in cal_for_news if d not in news_map]
                if missing_cal:
                    news_map.update(
                        _fetch_news_for_calendar_days(missing_cal)
                    )
            theme_preserved = theme_strong_mover_report.backfill_preserved_theme_day_sections_market_theme(
                theme_preserved,
                news_by_calendar=news_map,
                returns_df=po.returns_df,
                listing_names=po.listing_names,
                news_cutoff_label=cutoff_kst,
            )
        theme_days = sorted(
            dr.trading_day
            for dr in batch
            if not dr.forward_observation
        )
        theme_rows: list[dict] = []
        if theme_days and po.returns_df is not None and po.listing_names:
            theme_rows = snapshot_rebuild_learning.build_market_theme_flow(
                theme_days,
                po.news_by_calendar or {},
                po.returns_df,
                po.listing_names,
            )
            by_theme_day = {
                str(r.get("trading_day")): r for r in theme_rows if r.get("trading_day")
            }
            for dr in batch:
                if dr.forward_observation:
                    continue
                row = by_theme_day.get(dr.trading_day.isoformat())
                if row and dr.rows_compare:
                    early = snapshot_rebuild_learning._early_news_rows_for_trading_day(
                        po.news_by_calendar or {}, dr.trading_day
                    )
                    snapshot_rebuild_learning.enrich_compare_rows_theme_for_day(
                        dr,
                        row,
                        listing_names=po.listing_names,
                        early_rows=early,
                        news_cutoff_label=cutoff_kst,
                    )
        theme_new_sections = theme_strong_mover_report.build_strong_mover_report_days(
            batch,
            theme_rows,
            news_by_calendar=po.news_by_calendar or {},
            listing_names=po.listing_names or {},
            news_cutoff_label=cutoff_kst,
        )
        theme_all_days = sorted(
            {d["trading_day"] for d in theme_new_sections} | set(theme_preserved.keys())
        )
        theme_subtitle = test_range_label
        if theme_all_days:
            theme_subtitle = (
                f"{y}년 {m}월 · 25%↑ 테마 거래일 {theme_all_days[0].isoformat()} ~ "
                f"{theme_all_days[-1].isoformat()} ({len(theme_all_days)}일)"
            )
        theme_out = theme_strong_mover_report.render_strong_mover_theme_report(
            theme_path,
            day_reports=batch,
            theme_flow_rows=theme_rows,
            news_by_calendar=po.news_by_calendar or {},
            listing_names=po.listing_names or {},
            news_cutoff_label=cutoff_kst,
            title=f"25%↑ 급등 테마·상승 배경 · {theme_fname.replace('.html', '')}",
            subtitle=theme_subtitle,
            meta_note=meta_base.get("news_source", ""),
            preserved_day_html=theme_preserved or None,
        )
        if theme_out:
            # written_paths.append(theme_out)  # 자동 열기 제외(report_theme_25pct_*)
            print(f"완료(테마 25%↑): {theme_out}")

    index_html = config.OUTPUT_DIR / "report_index_monthly.html"
    month_links = report.collect_monthly_report_index_links(config.OUTPUT_DIR)
    report.render_movers_index(
        month_links,
        index_html,
        title="월간 리포트 목차 (달력 월 단위)",
    )
    written_paths.append(index_html)
    print(f"완료: {index_html}")
    return written_paths
