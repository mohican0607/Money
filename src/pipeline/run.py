"""핵심 데이터 파이프라인."""
from __future__ import annotations

import math
import os
import time
from collections import Counter
from datetime import date, datetime, timedelta

from tqdm import tqdm

from src import (
    config,
    disclosure,
    features,
    investor_flow,
    market_index,
    move_reference,
    news,
    predict,
    prediction_accuracy_cache,
    prediction_ranking,
    report,
    snapshot_miss_diagnosis,
    snapshot_rebuild_learning,
    stocks,
    theme_carryover,
    theme_strong_mover_report,
    train_snapshot,
    trading_calendar,
)

from .rows import (
    _append_compare_row_from_prediction,
    _compare_row_is_prediction_candidate,
    _compute_day_pred_accuracy_summary,
    _enrich_cumulative_accuracy_avg,
    _enrich_cumulative_actual_over_pred_from_history,
    _enrich_cumulative_actual_over_pred_from_history_for_field,
    _enrich_cumulative_hit_rate,
    _enrich_rows_prediction_signal,
    _enrich_forward_day_actual_display_rows,
    _enrich_forward_pred_rationale,
    _enrich_rows_pred_miss_tooltip,
    _gap_analysis_html_for_row,
    _pred_reason_fields,
    _prediction_row_strict_or_loose,
    _rise_band_for_row,
    _sync_forward_day_rows_from_predictions,
)
from .rows import (
    _apply_postclose_actual_snapshot_rows,
    _apply_preclose_actual_snapshot_rows,
    _backfill_day_actuals_from_returns,
    _build_rebuild_learning_payload,
    _enrich_rows_actual_ret_prev_day,
    _reconcile_closed_day_report,
    _enrich_rows_disclosure_hits,
    _enrich_rows_news_evidence,
    _enrich_rows_stock_ret_tooltip,
    _fetch_live_intraday_snapshot_pct_by_code,
    _movers_data_note_for_report,
)
from .support import (
    _display_prediction_rows_for_freeze,
    _freeze_entry_usable,
    _ignore_freeze_for_trading_day,
    _load_prediction_freeze_payload,
    _should_reuse_prediction_freeze,
    _prediction_rows_from_frozen_items,
    _prediction_rows_to_frozen_items,
    _save_prediction_freeze_payload,
    effective_train_snapshot_cal_scope,
)
from .support import _collect_calendar_days_for_trading_range, _fetch_news_for_calendar_days
from .support import (
    _observation_day_forward_mode,
    _refresh_incomplete_market_theme_on_day_reports,
    _resolve_pipeline_ohlcv_end,
    _should_skip_ohlcv_right_gap,
)
from .types import PipelineOut


def _run_pipeline(
    test_days: list[date],
    end_date: date,
    *,
    include_target_calendar_news: bool = False,
    forward_prediction_only: bool = False,
    train_snapshot_mode: str = "none",
    train_snapshot_cal_scope: tuple[date, date] | None = None,
    skip_ohlcv_gap_download: bool = False,
    omit_target_calendar_days: frozenset[date] | None = None,
    skip_news_fetch_after: date | None = None,
    respect_prediction_freeze: bool = False,
) -> PipelineOut:
    """
    공통 데이터 파이프라인: OHLCV → 뉴스 캐시 → 급등 이벤트 → 일자별 예측·비교.

    Args:
        test_days: 관측 거래일 T 목록(루프 돌며 ``DayReport`` 생성).
        end_date: 가격·뉴스 달력 상한(오늘과 설정에 맞게 잘림).
        include_target_calendar_news: True면 각 **테스트 관측일 T** 캘린더 뉴스를 받고,
            비교 표 row에 뉴스·공시 매칭·상승 이유(참고)를 채웁니다.
        forward_prediction_only: True면 모든 관측일을 예측 전용으로 처리(라이브 N=오늘).
            False여도 ``T`` 가 KST 오늘 이후이거나 일봉 상한 이후면 해당 일만 예측 전용(구간 종료일이 미래일 때).
        omit_target_calendar_days: 뉴스 fetch 시 해당 관측일 T의 **캘린더 일** 본문은 내려받지 않음( N 미마감 등).
        train_snapshot_mode: ``none`` | ``use`` | ``rebuild`` — 학습 스냅샷 재사용·갱신·전체 재생성.
        train_snapshot_cal_scope: ``(시작, 끝)`` 캘린더 일(포함) — ``use`` 모드에서 미반영 판단만 이 구간으로 한정.
            ``rebuild`` 모드이고 이 값이 있으면(구간 ``YYYYMMDD YYYYMMDD`` 실행) 파이프라인 말미에
            일자별 예측–실제 괴리 누적 분석을 ``breakout_train_snapshot.json`` 의 ``rebuild_learning`` 에 병합합니다.
            ``None`` 이면 이번 실행에 필요한 전체 캘린더 일자를 기준으로 병합합니다.
        skip_ohlcv_gap_download: True면 캐시 최신일 이후를 채우는 우측 보강 다운로드를 생략.
        skip_news_fetch_after: 지정 시 해당 일자를 초과한 미래 캘린더일 뉴스 조회를 생략한다.

    Returns:
        ``PipelineOut`` — ``_render_monthly_batch`` 또는 ``render_dated_n_report`` 에 넘김.
    """
    train_start = config.TRAIN_START_DEFAULT
    test_start = config.TEST_START

    config.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    now_kst_pipe = datetime.now(trading_calendar.KST)
    today_kst = now_kst_pipe.date()
    ohlcv_cap = trading_calendar.ohlcv_request_end_cap_today()
    ohlcv_end = _resolve_pipeline_ohlcv_end(
        end_date, test_days, now_kst=now_kst_pipe
    )
    if ohlcv_end < end_date:
        print(
            f"가격 데이터: 일봉 상한을 {ohlcv_end} 로 둡니다 "
            f"(요청 끝 {end_date}, 미래·미확정 봉 제외).",
            flush=True,
        )
    elif ohlcv_end > min(end_date, ohlcv_cap):
        print(
            f"가격 데이터: 마감 확정된 관측 거래일 반영으로 일봉 요청 끝을 {ohlcv_end} 로 합니다.",
            flush=True,
        )

    # 오늘 데이터가 포함되면 캐시가 장중 값으로 고정되지 않도록 마지막 1일은 재조회해 덮어쓴다.
    refresh_tail_days = 1 if ohlcv_end >= today_kst else 0

    print("가격 데이터 수집(캐시 있으면 재사용)…")
    ohlcv = stocks.build_ohlcv_long(
        train_start - timedelta(days=10),
        ohlcv_end,
        force_full_listing=not forward_prediction_only,
        skip_gap_download=skip_ohlcv_gap_download,
        refresh_tail_days=refresh_tail_days,
    )
    returns = stocks.daily_returns_table(ohlcv)
    returns_ml = stocks.enrich_daily_returns_for_ml(returns)
    returns_by_code, returns_ml_by_code = stocks.returns_by_code_index(returns, returns_ml)

    trading_days = trading_calendar.trading_sessions_in_range(train_start, end_date)
    news_trading_days = sorted(
        {t for t in trading_days if train_start <= t <= end_date} | set(test_days)
    )
    target_cal_days = frozenset(test_days) if include_target_calendar_news else None
    cal_days = _collect_calendar_days_for_trading_range(
        news_trading_days,
        include_target_calendar_days=False,
        target_calendar_trading_days=target_cal_days,
        omit_target_calendar_days=omit_target_calendar_days,
    )

    news_source = news.describe_news_fetch_source()

    if skip_news_fetch_after is not None:
        skipped_n = sum(1 for d in cal_days if d > skip_news_fetch_after)
        if skipped_n > 0:
            print(
                f"뉴스 수집 일수 {len(cal_days)} ({news_source})… "
                f"[일자 병렬 워커 {config.NEWS_FETCH_MAX_WORKERS}] "
                f"(미래 {skipped_n}일은 조회 생략)"
            )
        else:
            print(
                f"뉴스 수집 일수 {len(cal_days)} ({news_source})… "
                f"[일자 병렬 워커 {config.NEWS_FETCH_MAX_WORKERS}]"
            )
    else:
        print(
            f"뉴스 수집 일수 {len(cal_days)} ({news_source})… "
            f"[일자 병렬 워커 {config.NEWS_FETCH_MAX_WORKERS}]"
        )
    news_by_calendar = _fetch_news_for_calendar_days(cal_days, fetch_until=skip_news_fetch_after)

    cal_day_set = set(cal_days)
    fp_snap = train_snapshot.fingerprint()

    def _all_breakout_events_raw() -> list[features.BreakoutEvent]:
        """수익률·뉴스로 급등 이벤트 전체 생성(스냅샷 저장·워크포워드 풀)."""
        return features.build_breakout_events(
            returns,
            news_by_calendar,
            trading_calendar.news_window_for_target_trading_day,
            config.BIG_MOVE_THRESHOLD,
        )

    def _events_for_snapshot(ev: list[features.BreakoutEvent]) -> list[features.BreakoutEvent]:
        """스냅샷 저장 구간(``train_start``~``end_date``)에 해당하는 이벤트만 필터."""
        return train_snapshot.events_for_storage(
            ev, train_start=train_start, storage_end=end_date
        )

    train_td_set = {
        d
        for d in returns["Date"].dt.date.unique()
        if train_start <= d <= end_date
    }

    if train_snapshot_mode == "none":
        print("과거 급등-뉴스 이벤트 구축…")
        all_train_events = _events_for_snapshot(_all_breakout_events_raw())
    elif train_snapshot_mode == "rebuild":
        print("과거 급등-뉴스 이벤트 전체 재계산(학습 스냅샷 저장)…")
        all_train_events = _events_for_snapshot(_all_breakout_events_raw())
        train_snapshot.save_snapshot(all_train_events, set(cal_day_set), fp=fp_snap)
        print(f"학습 스냅샷 저장: {config.TRAIN_SNAPSHOT_PATH} (캘린더 {len(cal_day_set)}일)")
    else:
        # use | append_learning
        snap = train_snapshot.load_snapshot()
        fp_ok = snap is not None and snap.fingerprint == fp_snap
        if train_snapshot_cal_scope is not None:
            s0, s1 = train_snapshot_cal_scope
            scope_cal = {d for d in cal_day_set if s0 <= d <= s1}
        else:
            scope_cal = set(cal_day_set)
        missing_scope = scope_cal - (snap.calendar_days_covered if snap else set())

        gap_trading_days: set[date] = set()
        max_event_day: date | None = None
        if fp_ok and snap and snap.events:
            max_event_day = max(e.trading_day for e in snap.events)
            if train_snapshot_mode == "append_learning":
                gap_trading_days = {
                    d for d in train_td_set if max_event_day < d <= end_date
                }

        if not fp_ok:
            if snap is not None and snap.fingerprint != fp_snap:
                print("학습 스냅샷: 설정 지문 불일치 → 전체 재계산 후 저장.")
            else:
                print("학습 스냅샷: 파일 없음 또는 형식 오류 → 전체 재계산 후 저장.")
            print("과거 급등-뉴스 이벤트 구축…")
            all_train_events = _events_for_snapshot(_all_breakout_events_raw())
            train_snapshot.save_snapshot(all_train_events, set(cal_day_set), fp=fp_snap)
            print(f"학습 스냅샷 저장: {config.TRAIN_SNAPSHOT_PATH}")
        elif missing_scope or gap_trading_days:
            if gap_trading_days:
                print(
                    f"과거 급등-뉴스: --append-rebuild-learning — "
                    f"이벤트 마지막 {max_event_day} 이후 "
                    f"{len(gap_trading_days)}거래일(≤{end_date}) train_events 증분 병합",
                    flush=True,
                )
            if missing_scope:
                print(
                    f"과거 급등-뉴스: 스냅샷 병합 — 미반영 캘린더 일 {len(missing_scope)}일 "
                    f"(인자 구간 기준 {len(scope_cal)}일 중)"
                )
            recompute_td = set(gap_trading_days)
            if missing_scope:
                recompute_td |= {
                    d
                    for d in train_td_set
                    if news.calendar_days_for_breakout_training(d) & missing_scope
                }
            old = [e for e in snap.events if e.trading_day not in recompute_td]
            new = features.build_breakout_events_for_trading_days(
                returns,
                news_by_calendar,
                trading_calendar.news_window_for_target_trading_day,
                config.BIG_MOVE_THRESHOLD,
                recompute_td,
            )
            all_train_events = _events_for_snapshot(old + new)
            new_cov = snap.calendar_days_covered | cal_day_set
            train_snapshot.save_snapshot(all_train_events, new_cov, fp=fp_snap)
            print(
                f"학습 스냅샷 갱신 저장: 거래일 {len(recompute_td)}일 재계산 → "
                f"{len(all_train_events)}건 ({config.TRAIN_SNAPSHOT_PATH.name})",
                flush=True,
            )
        elif not missing_scope:
            all_train_events = _events_for_snapshot(snap.events)
            print(
                f"과거 급등-뉴스: 학습 스냅샷 재사용 "
                f"({config.TRAIN_SNAPSHOT_PATH.name}, 급등 이벤트 {len(all_train_events)}건, "
                f"예측 시 관측일 T 직전만 워크포워드)"
            )
            new_cov = snap.calendar_days_covered | cal_day_set
            if new_cov != snap.calendar_days_covered:
                train_snapshot.save_snapshot(all_train_events, new_cov, fp=fp_snap)

    kw_cooccur = Counter()
    kw_pool = (
        train_snapshot.filter_train_events_before(
            all_train_events, min(test_days), train_start=train_start
        )
        if test_days
        else all_train_events
    )
    for e in kw_pool:
        for w in e.news_keywords:
            kw_cooccur[w] += 1
    correlation_rows = kw_cooccur.most_common(40)

    listing = stocks.load_listing()
    codes = [
        c
        for c in listing["Code"].astype(str).str.zfill(6).tolist()
        if stocks.is_common_equity_code(c)
    ]
    names = {
        str(r["Code"]).zfill(6): str(r["Name"])
        for _, r in listing.iterrows()
        if stocks.is_common_equity_code(str(r["Code"]).zfill(6))
    }
    market_by_code = stocks.market_segment_by_code()

    if config.PRED_INVESTOR_FLOW_ENABLED:
        from src import investor_flow

        event_codes = [str(e.code).zfill(6) for e in all_train_events]
        prio = investor_flow.priority_codes_for_prefetch(
            returns, codes, train_event_codes=event_codes
        )
        n_flow = investor_flow.warm_cache_for_codes(prio)
        if n_flow:
            print(f"투자자 수급(외국인·기관·개인): {n_flow}종 신규 캐시", flush=True)
        returns_ml = investor_flow.merge_flow_features(returns_ml)
        returns_by_code, returns_ml_by_code = stocks.returns_by_code_index(
            returns, returns_ml
        )

    ks11 = market_index.load_index_frame(
        "KS11", train_start - timedelta(days=5), end_date
    )

    day_reports: list[report.DayReport] = []
    late_below_n = late_below_kw = late_gte_n = late_gte_kw = 0
    krx_movers_unavailable_any = False
    ml_bundle_prev: dict | None = None
    ml_bundle_prev_t: date | None = None
    freeze_payload = (
        _load_prediction_freeze_payload() if config.PREDICTION_FREEZE_ENABLED else {}
    )
    freeze_changed = False
    from ..prediction import feedback_loop as prediction_feedback_loop

    pipeline_feedback_ctx = prediction_feedback_loop.build_enriched_feedback_context(
        as_of=today_kst,
    )

    for T in tqdm(test_days, desc="테스트일별 예측"):
        t_loop0 = time.perf_counter()
        pipeline_feedback_ctx = prediction_feedback_loop.build_enriched_feedback_context(
            as_of=T,
        )
        tightness = float(pipeline_feedback_ctx.get("adaptive_tightness", 1.0) or 1.0)
        if tightness < 0.85 and config.PRED_FEEDBACK_ADAPTIVE_ENABLED:
            streak = int(pipeline_feedback_ctx.get("recent_miss_streak_days", 0) or 0)
            print(
                f"오판 반성: T={T.isoformat()} 적응 보수화 tightness={tightness:.2f} "
                f"miss_streak={streak}일",
                flush=True,
            )
        day_forward = _observation_day_forward_mode(
            T,
            today=today_kst,
            pipeline_forward_only=forward_prediction_only,
            now_kst=now_kst_pipe,
        )
        if day_forward and T > today_kst and not forward_prediction_only:
            print(
                f"관측일 T={T.isoformat()}: 예측 전용(KST 오늘={today_kst} 이후 거래일)",
                flush=True,
            )
        elif day_forward and T == today_kst and trading_calendar.is_before_krx_regular_close_kst(
            T, now_kst=now_kst_pipe
        ):
            print(
                f"관측일 T={T.isoformat()}: 장 마감 전 - 예측 전용"
                f"(실제 상승률·당일 테마·적중 통계 미확정)",
                flush=True,
            )
        if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
            blob, late_blob = news.aggregate_early_late_for_target(news_by_calendar, T)
            news_titles = news.sample_titles_early_for_target(news_by_calendar, T, limit=12)
            early_rows, _ = news.classified_rows_for_target(news_by_calendar, T)
            actual_ctx_rows = news.rows_for_actual_context(news_by_calendar, T)
        else:
            ws, we = trading_calendar.news_window_for_target_trading_day(T)
            blob = predict.aggregate_news_for_window(news_by_calendar, ws, we)
            late_blob = ""
            news_titles = []
            d = ws
            while d <= we:
                for row in news_by_calendar.get(d, []):
                    if row.get("title"):
                        news_titles.append(row["title"])
                d += timedelta(days=1)
            news_titles = news_titles[:12]
            early_rows, _ = news.classified_rows_for_target(news_by_calendar, T)
            actual_ctx_rows = news.rows_for_actual_context(news_by_calendar, T)

        train_events_t = train_snapshot.filter_train_events_before(
            all_train_events, T, train_start=train_start
        )
        ml_bundle = None
        if config.PRED_USE_ML_RANKER:
            try:
                from src import ml_move_rank

                if (
                    config.ML_REUSE_PRIOR_TRADING_DAY_MODEL
                    and ml_bundle_prev is not None
                    and ml_bundle_prev_t is not None
                    and train_snapshot_mode != "rebuild"
                ):
                    try:
                        prev_td = trading_calendar.last_trading_day_before(T)
                        if prev_td == ml_bundle_prev_t and ml_bundle_prev.get(
                            "pipeline"
                        ):
                            ml_bundle = ml_bundle_prev
                            print(
                                f"ML 랭커: 동일 실행 내 전일({ml_bundle_prev_t}) 모델 재사용 "
                                f"(T={T})",
                                flush=True,
                            )
                    except ValueError:
                        pass
                if ml_bundle is None:
                    ml_bundle = ml_move_rank.fit_or_load_classifier(
                        train_events=train_events_t,
                        returns_ml=returns_ml,
                        news_by_calendar=news_by_calendar,
                        listing_names=names,
                        fp=fp_snap,
                        label_before_exclusive=T,
                        force_retrain=train_snapshot_mode == "rebuild",
                        prefer_prior_reuse=bool(
                            day_forward and config.ML_REUSE_PRIOR_FOR_FORWARD
                        ),
                    )
                ml_bundle_prev = ml_bundle
                ml_bundle_prev_t = T
            except Exception as e:
                print(f"ML 랭커 초기화 실패(휴리스틱만 사용): {e}", flush=True)
                ml_bundle = None

        min_hits = config.PRED_MIN_KEYWORD_HITS if train_events_t else 0
        theme_w: dict[str, float] = {}
        if config.THEME_CARRYOVER_ENABLED:
            try:
                theme_w = theme_carryover.weights_for_observation_day(
                    T,
                    train_events=train_events_t,
                    news_by_calendar=news_by_calendar,
                    returns_df=returns_ml,
                    threshold=config.BIG_MOVE_THRESHOLD,
                )
            except (ValueError, TypeError, OSError):
                theme_w = {}
        t_key = T.isoformat()
        frozen_items = freeze_payload.get(t_key) if config.PREDICTION_FREEZE_ENABLED else None
        ignore_freeze_for_t = _ignore_freeze_for_trading_day(
            T,
            train_snapshot_mode=train_snapshot_mode,
            cal_scope=train_snapshot_cal_scope,
            respect_prediction_freeze=respect_prediction_freeze,
        )
        use_frozen = _should_reuse_prediction_freeze(
            ignore_freeze_for_trading_day=ignore_freeze_for_t,
            frozen_items=frozen_items,
        )
        if use_frozen:
            preds = _prediction_rows_from_frozen_items(frozen_items)
            print(f"예측 고정 캐시 재사용: T={t_key} ({len(preds)}건)", flush=True)
        else:
            if ignore_freeze_for_t:
                if train_snapshot_cal_scope is not None:
                    s0, s1 = train_snapshot_cal_scope
                    scope_note = f"구간({s0}~{s1}) "
                else:
                    scope_note = ""
                reason = "--rebuild-train-snapshot"
                if _freeze_entry_usable(frozen_items or []):
                    print(
                        f"관측일 T={t_key}: {reason} — "
                        f"{scope_note}예측 고정 캐시 무시·재계산합니다.",
                        flush=True,
                    )
                else:
                    print(
                        f"관측일 T={t_key}: {reason} — "
                        f"{scope_note}예측 고정 캐시 없음·신규 계산합니다.",
                        flush=True,
                    )
            elif not _freeze_entry_usable(frozen_items or []):
                print(
                    f"관측일 T={t_key}: 예측 고정 캐시 없음·신규 계산합니다.",
                    flush=True,
                )
            preds = predict.predict_for_trading_day(
                T,
                codes,
                names,
                train_events_t,
                blob,
                top_n=config.PRED_RANK_POOL_N,
                min_keyword_hits=min_hits,
                ml_bundle=ml_bundle,
                returns_ml=returns_ml,
                theme_weights=theme_w or None,
                feedback_ctx=pipeline_feedback_ctx,
                forward_observation=day_forward,
            )
            # ``predict_for_trading_day`` / ML 랭커가 이미 finalize 를 끝냄 — 여기서 다시 finalize 하면
            # 14:30 리포트와 freeze·장마감 후 재사용 예측이 어긋납니다.
            if config.PREDICTION_FREEZE_ENABLED and (
                ignore_freeze_for_t
                or (
                    day_forward
                    and not _freeze_entry_usable(freeze_payload.get(t_key) or [])
                )
            ):
                existing = freeze_payload.get(t_key) or []
                if not (
                    existing
                    and _freeze_entry_usable(existing)
                    and not ignore_freeze_for_t
                ):
                    freeze_payload[t_key] = _prediction_rows_to_frozen_items(
                        _display_prediction_rows_for_freeze(preds)
                    )
                    freeze_changed = True
        scoring_ctx = predict.build_scoring_context(blob, train_events_t)

        kospi_r = market_index.index_daily_return_pct(ks11, T)
        kospi_hint = None
        if kospi_r is not None:
            kospi_hint = f"당일 KOSPI 지수 전일대비 약 {kospi_r*100:.2f}%였습니다."

        if use_frozen and preds:
            prediction_ranking.enrich_prediction_rows_from_returns_ml(
                preds,
                returns_ml,
                target_day=T,
                ks11_ret_lag1=kospi_r,
            )
            # 14:30 freeze: tier·예측%·순위 고정 — finalize 재실행 없음.
            for pos, row in enumerate(preds, start=1):
                if getattr(row, "rank_position", None) is None:
                    row.rank_position = pos

        now_kst_td = datetime.now(trading_calendar.KST)
        is_today_t = T == now_kst_td.date()
        actual_big_movers: list[dict] = []
        actual_big_decliners: list[dict] = []
        krx_pct_by_code: dict[str, float] | None = None
        if not day_forward:
            krx_pct_by_code = stocks.try_krx_change_pct_by_code(T, returns_df=returns)
            # 오늘(T) 장중에는 일봉이 아직 없을 수 있어 best-effort 실시간 맵으로 한 번 더 보강.
            if (
                (not krx_pct_by_code)
                and is_today_t
                and trading_calendar.is_trading_day(T)
                and not trading_calendar.is_krx_daily_bar_effective_closed(
                    T, now_kst=now_kst_td
                )
            ):
                krx_pct_by_code = stocks.best_effort_intraday_pct_by_code(
                    T, codes, returns_df=returns
                )
            if krx_pct_by_code:
                actual_big_movers = stocks.big_movers_from_krx_pct_map(
                    krx_pct_by_code, config.BIG_MOVE_THRESHOLD, names
                )
                actual_big_decliners = stocks.big_movers_from_krx_pct_map(
                    krx_pct_by_code, config.BIG_MOVE_THRESHOLD, names, direction="down"
                )
            else:
                krx_movers_unavailable_any = True
                movers = stocks.big_movers_on_date(returns, T, config.BIG_MOVE_THRESHOLD)
                actual_big_movers = [
                    {
                        "code": str(r["Code"]).zfill(6),
                        "name": str(r["Name"]),
                        "ret_pct": float(r["return_pct"]) * 100.0,
                    }
                    for _, r in movers.iterrows()
                ]
                movers_dn = stocks.big_movers_on_date(returns, T, -config.BIG_MOVE_THRESHOLD)
                actual_big_decliners = [
                    {
                        "code": str(r["Code"]).zfill(6),
                        "name": str(r["Name"]),
                        "ret_pct": float(r["return_pct"]) * 100.0,
                    }
                    for _, r in movers_dn.iterrows()
                ]
                # 경고 문구는 콘솔에 반복 노이즈를 만들어 비활성화.
                # 실제 집계는 아래 OHLCV 폴백(actual_big_movers)로 계속 진행합니다.

        def _actual_ret_for_code(code: str) -> float | None:
            """관측일 T 종목 실제 수익률(OHLCV → pykrx 맵 순)."""
            a = stocks.actual_return_on_date(returns, code, T)
            if a is not None:
                return a
            if krx_pct_by_code is not None:
                p = krx_pct_by_code.get(code)
                if p is not None:
                    return p / 100.0
            return None

        hl_terms = features.top_keywords(blob, k=40)

        rows_compare: list[dict] = []
        false_negatives: list[dict] = []
        pred_pct_min = config.BIG_MOVE_THRESHOLD * 100.0
        pred_pct_mid_min = 10.0
        row_pred_min = 0.0
        actual_10up_by_code: dict[str, dict] = {}
        actual_10dn_by_code: dict[str, dict] = {}
        if not day_forward:
            if krx_pct_by_code:
                actual_10up_rows = stocks.big_movers_from_krx_pct_map(
                    krx_pct_by_code, pred_pct_mid_min / 100.0, names
                )
                actual_10dn_rows = stocks.big_movers_from_krx_pct_map(
                    krx_pct_by_code, -pred_pct_mid_min / 100.0, names, direction="down"
                )
            else:
                movers_10up = stocks.big_movers_on_date(returns, T, pred_pct_mid_min / 100.0)
                actual_10up_rows = [
                    {
                        "code": str(r["Code"]).zfill(6),
                        "name": str(r["Name"]),
                        "ret_pct": float(r["return_pct"]) * 100.0,
                    }
                    for _, r in movers_10up.iterrows()
                ]
                movers_10dn = stocks.big_movers_on_date(returns, T, -pred_pct_mid_min / 100.0)
                actual_10dn_rows = [
                    {
                        "code": str(r["Code"]).zfill(6),
                        "name": str(r["Name"]),
                        "ret_pct": float(r["return_pct"]) * 100.0,
                    }
                    for _, r in movers_10dn.iterrows()
                ]
            actual_10up_by_code = {
                str(x.get("code", "")).zfill(6): x for x in actual_10up_rows if x.get("code")
            }
            actual_10dn_by_code = {
                str(x.get("code", "")).zfill(6): x for x in actual_10dn_rows if x.get("code")
            }

        if (
            not day_forward
            and config.INCLUDE_ACTUAL_BIG_MOVERS_IN_ROWS_COMPARE
            and actual_10up_by_code
        ):
            preds_by_code: dict[str, predict.PredictionRow] = {pr.code: pr for pr in preds}
            # (옵션) 당일 실제 10%↑ 종목을 비교 표에 추가 — 기본 끔.
            for m in list(actual_10up_by_code.values()):
                code = m["code"]
                act = float(m["ret_pct"]) / 100.0
                pr = preds_by_code.get(code)
                if pr is not None:
                    reasons_html = "<br/>".join(pr.reasons)
                    keywords = pr.matched_keywords
                    pred_ret = pr.predicted_return_pct
                else:
                    reasons_html = "일일 예측 후보에 포함되지 않음(예측 수익률 재계산 안 함)"
                    keywords = []
                    pred_ret = None

                pred_high = (
                    prediction_ranking.is_high_confidence_prediction(pr)
                    if pr is not None
                    else False
                )
                pred_mid = (
                    prediction_ranking.is_mid_confidence_prediction(pr)
                    if pr is not None
                    else False
                )
                row_d = {
                    "code": code,
                    "market_segment": market_by_code.get(str(code).zfill(6), "other"),
                    "name": m["name"],
                    "reasons_html": reasons_html,
                    "keywords": keywords,
                    "keyword_hits": (pr.keyword_hits if pr is not None else len(keywords)),
                    "mention_score": (pr.mention_score if pr is not None else 0.0),
                    "pred_ret": pred_ret,
                    "ml_prob": (pr.ml_prob if pr is not None else None),
                    "rank_position": (
                        getattr(pr, "rank_position", None) if pr is not None else None
                    ),
                    "confidence_tier": (
                        getattr(pr, "confidence_tier", "none") if pr is not None else "none"
                    ),
                    "actual_ret": act,
                    "actual_big": act >= config.BIG_MOVE_THRESHOLD,
                    "pred_high": pred_high,
                    "pred_mid": pred_mid,
                    "rise_band": _rise_band_for_row(pred_ret, act),
                    "gap_analysis_html": _gap_analysis_html_for_row(
                        pred_ret, act, pr, keywords, blob, kospi_hint, late_blob
                    ),
                    **_pred_reason_fields(pr, reasons_html),
                }
                rows_compare.append(row_d)

                if (
                    config.USE_DECISION_NEWS_INTRADAY_CUTOFF
                    and late_blob
                    and keywords
                    and pr is not None
                ):
                    late_hit = news.late_blob_covers_keywords(late_blob, list(keywords))
                    late_gte_n += 1
                    if late_hit:
                        late_gte_kw += 1
                    row_d["late_news_hit"] = bool(late_hit)
                else:
                    row_d["late_news_hit"] = None

        seen_row_codes = {r["code"] for r in rows_compare}
        for pr in preds:
            # 예측 10% 이상 후보를 표에 반영(필터 라디오로 20%+/10~20 전환).
            if pr.predicted_return_pct < row_pred_min:
                continue
            if pr.code in seen_row_codes:
                continue
            seen_row_codes.add(pr.code)
            act = None if day_forward else _actual_ret_for_code(pr.code)
            reasons_html = "<br/>".join(pr.reasons)
            ph = prediction_ranking.is_high_confidence_prediction(pr)
            pm = prediction_ranking.is_mid_confidence_prediction(pr)
            rows_compare.append(
                {
                    "code": pr.code,
                    "market_segment": market_by_code.get(str(pr.code).zfill(6), "other"),
                    "name": pr.name,
                    "reasons_html": reasons_html,
                    "keywords": pr.matched_keywords,
                    "keyword_hits": pr.keyword_hits,
                    "mention_score": pr.mention_score,
                    "pred_ret": pr.predicted_return_pct,
                    "ml_prob": pr.ml_prob,
                    "rank_position": getattr(pr, "rank_position", None),
                    "confidence_tier": getattr(pr, "confidence_tier", "none"),
                    "actual_ret": act,
                    "actual_big": act is not None and act >= config.BIG_MOVE_THRESHOLD,
                    "pred_high": ph,
                    "pred_mid": pm,
                    "rise_band": _rise_band_for_row(pr.predicted_return_pct, act),
                    "gap_analysis_html": _gap_analysis_html_for_row(
                        pr.predicted_return_pct,
                        act,
                        pr,
                        pr.matched_keywords,
                        blob,
                        kospi_hint,
                        late_blob,
                    ),
                    "late_news_hit": (
                        news.late_blob_covers_keywords(late_blob, list(pr.matched_keywords))
                        if (
                            config.USE_DECISION_NEWS_INTRADAY_CUTOFF
                            and late_blob
                            and pr.matched_keywords
                        )
                        else None
                    ),
                    **_pred_reason_fields(pr, reasons_html),
                }
            )

        if preds:
            _sync_forward_day_rows_from_predictions(
                rows_compare,
                preds,
                market_by_code=market_by_code,
                pred_pct_min=pred_pct_min,
                pred_pct_mid_min=pred_pct_mid_min,
                blob=blob,
                kospi_hint=kospi_hint,
                late_blob=late_blob,
                actual_ret_for_code=None if day_forward else _actual_ret_for_code,
            )

        if day_forward:
            rows_compare = [
                r for r in rows_compare if _compare_row_is_prediction_candidate(r)
            ]
            cap = int(config.PRED_FORWARD_SHOW_MAX)
            if len(rows_compare) > cap:
                rows_compare.sort(
                    key=lambda r: (
                        0 if r.get("pred_high") else (1 if r.get("pred_mid") else 2),
                        int(r["rank_position"])
                        if r.get("rank_position") is not None
                        else 9999,
                        str(r.get("code", "")).zfill(6),
                    )
                )
                rows_compare = rows_compare[:cap]
            if len(rows_compare) < cap and preds and not use_frozen:
                seen = {str(r.get("code", "")).zfill(6) for r in rows_compare if r.get("code")}
                for pr in preds:
                    if len(rows_compare) >= cap:
                        break
                    code = str(pr.code).zfill(6)
                    if code in seen:
                        continue
                    if not math.isfinite(float(pr.predicted_return_pct)):
                        continue
                    reasons_html = "<br/>".join(pr.reasons)
                    _append_compare_row_from_prediction(
                        rows_compare,
                        pr,
                        market_by_code=market_by_code,
                        pred_pct_min=pred_pct_min,
                        pred_pct_mid_min=pred_pct_mid_min,
                        actual_ret=None,
                        reasons_html=reasons_html,
                        blob=blob,
                        kospi_hint=kospi_hint,
                        late_blob=late_blob,
                        pr_for_gap=pr,
                    )
                    seen.add(code)
            for r in rows_compare:
                r["actual_ret"] = None
                r["actual_big"] = False
                r.pop("actual_ret_intraday_pct", None)
                r.pop("actual_cell_pre_close_snapshot", None)
        if day_forward and not rows_compare and preds:
            top_show = min(int(config.PRED_FORWARD_SHOW_MAX), len(preds))
            for pr in preds[:top_show]:
                if not math.isfinite(float(pr.predicted_return_pct)):
                    continue
                reasons_html = "<br/>".join(pr.reasons)
                _append_compare_row_from_prediction(
                    rows_compare,
                    pr,
                    market_by_code=market_by_code,
                    pred_pct_min=pred_pct_min,
                    pred_pct_mid_min=pred_pct_mid_min,
                    actual_ret=None,
                    reasons_html=reasons_html,
                    blob=blob,
                    kospi_hint=kospi_hint,
                    late_blob=late_blob,
                    pr_for_gap=pr,
                )
        rows_compare.sort(key=lambda r: (not r["actual_big"], not r["pred_high"], r["code"]))

        today_td = now_kst_td.date()
        force_intraday_snapshot = os.getenv("FORCE_INTRADAY_SNAPSHOT", "").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        # 일봉이 아직 확정되지 않은 당일 거래일 T: 실제 상승률은 — (장중 등락%) 형태로 둡니다.
        pre_close_today = (
            T == today_td
            and trading_calendar.is_trading_day(T)
            and not trading_calendar.is_krx_daily_bar_effective_closed(T, now_kst=now_kst_td)
        )
        if force_intraday_snapshot and T == today_td and trading_calendar.is_trading_day(T):
            pre_close_today = True
        if pre_close_today and rows_compare and not day_forward:
            # 장중: pykrx(재시도)·종목별·네이버 실시간 순으로 등락률을 채운 뒤 — (xx%) 표기.
            row_codes = sorted(
                {str(r.get("code", "")).zfill(6) for r in rows_compare if r.get("code")}
            )
            merged = stocks.best_effort_intraday_pct_by_code(T, row_codes, returns_df=returns)
            _apply_preclose_actual_snapshot_rows(
                rows_compare, snapshot_pct_by_code=merged if merged else None
            )
            for r in rows_compare:
                r["actual_cell_pre_close_snapshot"] = True
        elif T == today_td and trading_calendar.is_trading_day(T) and rows_compare:
            # 장 마감 후에는 pykrx 값을 재조회해 실제 수익률 칸 자체를 최신화합니다.
            snap_post = stocks.try_krx_change_pct_by_code(T, returns_df=returns)
            _apply_postclose_actual_snapshot_rows(
                rows_compare,
                snapshot_pct_by_code=snap_post,
                threshold=config.BIG_MOVE_THRESHOLD,
            )

        if not config.REPORT_SKIP_DISCLOSURE_FETCH:
            _enrich_rows_disclosure_hits(rows_compare, T)
        else:
            for r in rows_compare:
                r.setdefault("disclosure_hits", [])
        if include_target_calendar_news:
            _enrich_rows_news_evidence(
                rows_compare, early_rows, actual_ctx_rows, target_trading_day=T
            )
        code_pr_map = {str(pr.code).zfill(6): pr for pr in preds}
        for r in rows_compare:
            code_r = str(r.get("code", "")).zfill(6)
            pr_row = code_pr_map.get(code_r)
            if pr_row is None:
                pr_row = _prediction_row_strict_or_loose(
                    code_r, names, train_events_t, blob, scoring_ctx, min_hits
                )
            r["gap_analysis_html"] = _gap_analysis_html_for_row(
                r.get("pred_ret"),
                r.get("actual_ret"),
                pr_row,
                list(r.get("keywords") or []),
                blob,
                kospi_hint,
                late_blob,
                disclosure_hits=list(r.get("disclosure_hits") or []),
                actual_intraday_pct=r.get("actual_ret_intraday_pct"),
            )
            r["rise_reason_html"] = move_reference.build_move_reference_html(
                code=code_r,
                name=str(r.get("name") or names.get(code_r, "")),
                t_trading_day=T,
                actual_ret=r.get("actual_ret"),
                actual_intraday_pct=r.get("actual_ret_intraday_pct"),
                pred_news_hits=r.get("pred_news_hits"),
                actual_news_hits=r.get("actual_news_hits"),
                actual_ctx_rows=actual_ctx_rows if include_target_calendar_news else None,
                disclosure_hits=r.get("disclosure_hits"),
                keywords=list(r.get("keywords") or []),
                kospi_return=kospi_r,
                news_evidence_collected=include_target_calendar_news,
                returns_sub=returns_by_code.get(code_r),
                returns_ml_sub=returns_ml_by_code.get(code_r),
            )
            r["rise_band"] = _rise_band_for_row(r.get("pred_ret"), r.get("actual_ret"))
            if pr_row is not None:
                r.update(_pred_reason_fields(pr_row, r.get("reasons_html") or ""))
        if not day_forward:
            _enrich_rows_pred_miss_tooltip(
                rows_compare,
                t_trading_day=T,
                kospi_hint=kospi_hint,
            )
        _enrich_rows_prediction_signal(rows_compare)
        forward_pred_rationale_html = ""
        if day_forward:
            forward_pred_rationale_html = _enrich_forward_pred_rationale(
                rows_compare,
                code_pr_map,
                t_trading_day=T,
                kospi_return=kospi_r,
                early_rows=early_rows,
                actual_ctx_rows=actual_ctx_rows if include_target_calendar_news else None,
                returns_by_code=returns_by_code,
                returns_ml_by_code=returns_ml_by_code,
            )
        rows_by_code = {str(r.get("code", "")).zfill(6): r for r in rows_compare}

        for pr in preds:
            act = None if day_forward else _actual_ret_for_code(pr.code)
            if (
                not day_forward
                and act is not None
                and act < config.BIG_MOVE_THRESHOLD
                and config.USE_DECISION_NEWS_INTRADAY_CUTOFF
                and late_blob
                and pr.matched_keywords
            ):
                late_hit = news.late_blob_covers_keywords(late_blob, list(pr.matched_keywords))
                late_below_n += 1
                if late_hit:
                    late_below_kw += 1

            if not day_forward and act is not None and act < 0:
                code6 = str(pr.code).zfill(6)
                row = rows_by_code.get(code6, {})
                false_negatives.append(
                    {
                        "code": pr.code,
                        "name": pr.name,
                        "pred_ret": pr.predicted_return_pct,
                        "actual_ret": act,
                        "keywords": list(pr.matched_keywords),
                        "analysis": predict.explain_miss(pr, act, blob, kospi_hint),
                        "analysis_html": predict.explain_miss_html(
                            pr,
                            act,
                            news_blob_early=blob,
                            kospi_change_hint=kospi_hint,
                            late_keywords_matched=row.get("late_news_hit"),
                            disclosure_hits=list(row.get("disclosure_hits") or []),
                            pred_news_hits=list(row.get("pred_news_hits") or []),
                            actual_news_hits=list(row.get("actual_news_hits") or []),
                            t_trading_day=T,
                            gap_analysis_html=str(row.get("gap_analysis_html") or ""),
                        ),
                    }
                )

        pred_accuracy_summary = (
            None
            if day_forward
            else _compute_day_pred_accuracy_summary(
                rows_compare, threshold=config.BIG_MOVE_THRESHOLD
            )
        )

        print(
            f"관측일 T={t_key}: 처리 완료 ({time.perf_counter() - t_loop0:.1f}s, "
            f"비교표 {len(rows_compare)}행)",
            flush=True,
        )

        _enrich_rows_actual_ret_prev_day(rows_compare, returns, T)
        _enrich_rows_stock_ret_tooltip(
            rows_compare,
            returns,
            T,
            forward_observation=day_forward,
            now_kst=now_kst_td,
        )
        if day_forward:
            _enrich_forward_day_actual_display_rows(
                rows_compare,
                returns,
                T,
                now_kst=now_kst_td,
            )

        if config.THEME_CARRYOVER_ENABLED and rows_compare and not day_forward:
            theme_carryover.persist_rich_snapshot(
                T,
                actual_big_movers=actual_big_movers,
                actual_big_decliners=actual_big_decliners,
                rows_compare=rows_compare,
                highlight_terms=hl_terms,
                threshold=config.BIG_MOVE_THRESHOLD,
            )

        hit_at_k_metrics: dict | None = None
        if not day_forward and preds:
            actual_big_codes = {
                str(m.get("code", "")).zfill(6) for m in actual_big_movers if m.get("code")
            }
            hit_at_k_metrics = prediction_ranking.compute_hit_at_k_metrics(
                [pr.code for pr in preds],
                actual_big_codes,
                universe_size=len(codes),
            )

        day_reports.append(
            report.DayReport(
                trading_day=T,
                predictions=preds,
                rows_compare=rows_compare,
                false_negatives=false_negatives,
                news_titles_sample=news_titles,
                news_highlight_terms=hl_terms,
                actual_big_movers=actual_big_movers,
                forward_observation=day_forward,
                show_stock_ret_column=day_forward,
                hit_at_k_metrics=hit_at_k_metrics,
                pred_accuracy_summary=pred_accuracy_summary,
                forward_pred_rationale_html=forward_pred_rationale_html,
            )
        )
        if not day_forward:
            _dr = day_reports[-1]
            prediction_accuracy_cache.merge_from_day_reports([_dr])
            prediction_accuracy_cache.merge_hit_at_k_from_day_reports([_dr])
            prediction_accuracy_cache.merge_feedback_buckets_from_day_reports([_dr])
            prediction_accuracy_cache.merge_keyword_feedback_from_day_reports(
                [_dr], threshold=config.BIG_MOVE_THRESHOLD
            )
            prediction_feedback_loop.append_incremental_miss_from_day_report(_dr)

    # 장중 시작 실행이 장마감 이후까지 이어진 경우: 확정된 관측 거래일 일봉을 다시 읽어 실제값 보정.
    now_end_kst = datetime.now(trading_calendar.KST)
    refresh_ohlcv_end = _resolve_pipeline_ohlcv_end(
        end_date, test_days, now_kst=now_end_kst
    )
    if refresh_ohlcv_end > ohlcv_end:
        print(
            f"장 마감 반영 재조회: 일봉을 {refresh_ohlcv_end} 까지 읽어 실제 수익률을 반영합니다.",
            flush=True,
        )
        ohlcv_post = stocks.build_ohlcv_long(
            train_start - timedelta(days=10),
            refresh_ohlcv_end,
            force_full_listing=not forward_prediction_only,
            skip_gap_download=skip_ohlcv_gap_download,
            refresh_tail_days=1,
        )
        returns_post = stocks.daily_returns_table(ohlcv_post)
        returns = returns_post
    else:
        returns_post = returns

    reconciled = 0
    for dr in day_reports:
        if dr.trading_day > refresh_ohlcv_end:
            continue
        if _reconcile_closed_day_report(
            dr,
            returns=returns_post,
            threshold=config.BIG_MOVE_THRESHOLD,
            listing_names=names,
            market_by_code=market_by_code,
            now_kst=now_end_kst,
            universe_size=len(codes),
        ):
            reconciled += 1
            prediction_feedback_loop.append_incremental_miss_from_day_report(dr)
            prediction_accuracy_cache.merge_from_day_reports([dr])
            prediction_accuracy_cache.merge_hit_at_k_from_day_reports([dr])
            prediction_accuracy_cache.merge_feedback_buckets_from_day_reports([dr])
            prediction_accuracy_cache.merge_keyword_feedback_from_day_reports(
                [dr], threshold=config.BIG_MOVE_THRESHOLD
            )
    if reconciled:
        print(
            f"장 마감 반영: 종가 기준 실적·Hit@K·테마 재구성 {reconciled}일",
            flush=True,
        )

    _enrich_cumulative_accuracy_avg(day_reports)
    prediction_accuracy_cache.merge_from_day_reports(day_reports)
    prediction_accuracy_cache.merge_hit_at_k_from_day_reports(day_reports)
    prediction_accuracy_cache.merge_feedback_buckets_from_day_reports(day_reports)
    prediction_accuracy_cache.merge_keyword_feedback_from_day_reports(
        day_reports, threshold=config.BIG_MOVE_THRESHOLD
    )
    prediction_accuracy_cache.merge_high_pred_history_from_day_reports(
        day_reports, threshold_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    prediction_accuracy_cache.merge_mid_pred_history_from_day_reports(
        day_reports, low_pct=10.0, high_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    prediction_accuracy_cache.merge_all_pred_history_from_day_reports(
        day_reports, min_pct=10.0
    )
    prediction_accuracy_cache.apply_cached_cumulative_fallback(day_reports)
    prediction_accuracy_cache.enrich_rows_pred_high_history(day_reports)
    _enrich_cumulative_hit_rate(
        day_reports, threshold_pct=config.BIG_MOVE_THRESHOLD * 100.0
    )
    _enrich_cumulative_actual_over_pred_from_history(day_reports)
    _enrich_cumulative_actual_over_pred_from_history_for_field(
        day_reports,
        history_field="pred_mid_history",
        out_field="cumulative_accuracy_10_20_avg",
    )
    _enrich_cumulative_actual_over_pred_from_history_for_field(
        day_reports,
        history_field="pred_all_history",
        out_field="cumulative_accuracy_all_avg",
    )

    theme_flow_rows: list[dict] = []
    if not forward_prediction_only and day_reports:
        _cutoff_kst = (
            f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}"
        )
        theme_flow_rows = snapshot_rebuild_learning.enrich_day_reports_market_theme(
            day_reports,
            news_by_calendar,
            returns,
            names,
            news_cutoff_label=_cutoff_kst,
        )
        _refresh_incomplete_market_theme_on_day_reports(
            day_reports,
            news_by_calendar=news_by_calendar,
            returns_df=returns,
            listing_names=names,
            news_cutoff_label=_cutoff_kst,
            now_kst=now_end_kst,
        )

    cal_scope = effective_train_snapshot_cal_scope(
        train_snapshot_mode=train_snapshot_mode,
        train_snapshot_cal_scope=train_snapshot_cal_scope,
        day_reports=day_reports,
        forward_prediction_only=forward_prediction_only,
    )
    if (
        train_snapshot_mode in ("rebuild", "append_learning")
        and cal_scope is not None
        and not forward_prediction_only
    ):
        s0, s1 = cal_scope
        rl = _build_rebuild_learning_payload(day_reports, s0, s1)["rebuild_learning"]
        if theme_flow_rows:
            theme = [
                r
                for r in theme_flow_rows
                if s0 <= date.fromisoformat(str(r["trading_day"])) <= s1
            ]
        else:
            theme_days = sorted(
                {dr.trading_day for dr in day_reports if s0 <= dr.trading_day <= s1}
            )
            theme = snapshot_rebuild_learning.build_market_theme_flow(
                theme_days,
                news_by_calendar,
                returns,
                names,
            )
        gap = prediction_accuracy_cache.export_gap_rollup_for_calendar_range(s0, s1)
        if snapshot_rebuild_learning.merge_extended_rebuild_into_snapshot(
            rebuild_learning=rl,
            market_theme_flow=theme,
            prediction_gap_rollup=gap,
        ):
            print(
                "학습 스냅샷: rebuild_learning(미예측 급등·고예측 실패 진단 포함) + "
                "market_theme_flow + prediction_gap_rollup 병합 저장.",
                flush=True,
            )
    if config.PREDICTION_FREEZE_ENABLED and freeze_changed:
        _save_prediction_freeze_payload(freeze_payload)

    return PipelineOut(
        day_reports=day_reports,
        news_source=news_source,
        correlation_rows=correlation_rows,
        train_start=train_start,
        test_start=test_start,
        end_date=end_date,
        late_below_n=late_below_n,
        late_below_kw=late_below_kw,
        late_gte_n=late_gte_n,
        late_gte_kw=late_gte_kw,
        movers_data_note=_movers_data_note_for_report(
            krx_movers_unavailable_any and not forward_prediction_only
        ),
        returns_df=returns,
        news_by_calendar=news_by_calendar,
        listing_names=names,
    )
