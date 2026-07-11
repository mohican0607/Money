"""ML 랭킹 후보 종목 풀(뉴스·모멘텀·맥락·수급·업종 열기)."""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

import pandas as pd

from .. import config, trading_calendar
from .. import investor_flow
from ..features import filter_specific_keywords, name_mention_score
from . import prediction_ranking as pred_hybrid
from .news_context import affinity_candidate_codes, tfidf_vector


def prior_day_hot_peer_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    min_prev_ret: float = 0.04,
    max_codes: int = 420,
) -> list[str]:
    """
    T 직전 거래일 급등·강한 전일 모멘텀 종목과 동업종 peer를 후보에 넣습니다.

    6/26처럼 전일·연속 급등 테마가 이어지는 날, 뉴스 키워드만으로는 풀에 안 잡히는
    실제 20%↑ 종목(금호계열 등)을 ML 랭킹 대상에 포함시키기 위함.
    """
    from .. import stocks as stocks_mod

    allowed = {str(c).zfill(6) for c in listing_codes}
    out: list[str] = []
    seen: set[str] = set()

    def _add(code: str) -> None:
        c6 = str(code).zfill(6)
        if c6 in allowed and c6 not in seen:
            seen.add(c6)
            out.append(c6)

    try:
        prev_td = trading_calendar.last_trading_day_before(target_day)
    except ValueError:
        prev_td = None

    if prev_td is not None:
        sl_prev = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(prev_td)]
        hot: list[tuple[float, str]] = []
        for _, r in sl_prev.iterrows():
            code = str(r["Code"]).zfill(6)
            if code not in allowed:
                continue
            try:
                ret = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                continue
            if ret + 1e-12 >= min_prev_ret:
                hot.append((ret, code))
        hot.sort(key=lambda x: (-x[0], x[1]))
        for _, code in hot[:130]:
            _add(code)
            ic = stocks_mod.industry_code_for_stock(code)
            if ic:
                for peer in stocks_mod.peers_for_industry(ic)[:24]:
                    _add(peer)

    lookback = max(1, int(config.PRED_INDUSTRY_HEAT_LOOKBACK_DAYS))
    session_days: list[date] = []
    d = prev_td
    for _ in range(lookback):
        if d is None:
            break
        session_days.append(d)
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break
    for sess in session_days:
        sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(sess)]
        for _, r in sl.iterrows():
            code = str(r["Code"]).zfill(6)
            if code not in allowed:
                continue
            try:
                ret = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                continue
            if ret + 1e-12 >= 0.08:
                _add(code)
                ic = stocks_mod.industry_code_for_stock(code)
                if ic:
                    for peer in stocks_mod.peers_for_industry(ic)[:14]:
                        _add(peer)

    sl_t = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    lag_hot: list[tuple[float, str]] = []
    for _, r in sl_t.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        try:
            rl = float(r.get("ret_lag1") or 0.0)
            vs = float(r.get("vol_surge_ratio") or 0.0)
        except (TypeError, ValueError):
            continue
        if rl + 1e-12 >= 0.028 or vs + 1e-12 >= 1.10:
            lag_hot.append((rl + 0.15 * min(vs, 3.0), code))
    lag_hot.sort(key=lambda x: (-x[0], x[1]))
    for _, code in lag_hot[:100]:
        _add(code)
        ic = stocks_mod.industry_code_for_stock(code)
        if ic:
            for peer in stocks_mod.peers_for_industry(ic)[:10]:
                _add(peer)

    return out[: min(max_codes, 480)]


def burst_industry_peer_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    min_ret: float = 0.08,
    min_count: int = 2,
    max_industries: int = 28,
    peers_per_industry: int = 40,
    hot_peers_per_industry: int = 90,
) -> list[str]:
    """최근 거래일 업종 내 다수 급등 시 동업종 전체를 후보에 넣습니다."""
    from .. import stocks as stocks_mod

    allowed = {str(c).zfill(6) for c in listing_codes}
    out: list[str] = []
    seen: set[str] = set()
    lookback = max(1, int(config.PRED_INDUSTRY_HEAT_LOOKBACK_DAYS))
    session_days: list[date] = []
    d: date | None = target_day
    for _ in range(lookback + 1):
        if d is None:
            break
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break
        session_days.append(d)

    industry_hits: dict[str, int] = defaultdict(int)
    industry_best: dict[str, float] = defaultdict(float)
    for sess in session_days:
        sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(sess)]
        for _, r in sl.iterrows():
            try:
                rp = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                continue
            if rp + 1e-12 < min_ret:
                continue
            ic = stocks_mod.industry_code_for_stock(str(r["Code"]))
            if not ic:
                continue
            k = str(ic)
            industry_hits[k] += 1
            industry_best[k] = max(industry_best[k], rp)

    hot_inds = sorted(
        industry_hits.keys(),
        key=lambda k: (industry_hits[k], industry_best[k]),
        reverse=True,
    )
    added_inds = 0
    for ic in hot_inds:
        if added_inds >= max_industries:
            break
        if industry_hits[ic] < min_count and industry_best[ic] + 1e-12 < 0.18:
            continue
        n_peers = peers_per_industry
        if industry_hits[ic] >= 12 or industry_best[ic] + 1e-12 >= 0.16:
            n_peers = max(peers_per_industry, hot_peers_per_industry)
        for peer in stocks_mod.peers_for_industry(ic)[:n_peers]:
            c6 = str(peer).zfill(6)
            if c6 in allowed and c6 not in seen:
                seen.add(c6)
                out.append(c6)
        added_inds += 1
    return out


def theme_rotation_peer_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    max_codes: int = 280,
) -> list[str]:
    """
    핫 업종 내 전일 중간 모멘텀(로테이션) 종목·동업종 peer 를 후보에 넣습니다.

    7/2처럼 전일 건설·전력 테마가 이어지되 상한가 직후 리더가 아닌
    전일 +5~18% 구간 종목(신원종합개발·진흥기업 등)이 급등하는 패턴을 풀에 포함.
    """
    if not config.PRED_THEME_ROTATION_ENABLED:
        return []
    from .. import stocks as stocks_mod

    allowed = {str(c).zfill(6) for c in listing_codes}
    lag_min = float(config.PRED_THEME_ROTATION_LAG_MIN)
    lag_max = float(config.PRED_THEME_ROTATION_LAG_MAX)
    block = float(config.PRED_PRIOR_DAY_EXHAUSTION_BLOCK_RET)
    lookback = max(1, int(config.PRED_INDUSTRY_HEAT_LOOKBACK_DAYS))
    session_days: list[date] = []
    d: date | None = target_day
    for _ in range(lookback + 1):
        if d is None:
            break
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break
        session_days.append(d)
    industry_hits: dict[str, int] = defaultdict(int)
    industry_best: dict[str, float] = defaultdict(float)
    for sess in session_days:
        sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(sess)]
        for _, r in sl.iterrows():
            try:
                rp = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                continue
            if rp + 1e-12 < 0.10:
                continue
            ic = stocks_mod.industry_code_for_stock(str(r["Code"]))
            if not ic:
                continue
            k = str(ic)
            industry_hits[k] += 1
            industry_best[k] = max(industry_best[k], rp)
    hot_ind_keys = {
        k
        for k in industry_hits
        if industry_hits[k] >= 2 or industry_best[k] + 1e-12 >= 0.18
    }

    out: list[str] = []
    seen: set[str] = set()

    def _add(code: str) -> None:
        c6 = str(code).zfill(6)
        if c6 in allowed and c6 not in seen:
            seen.add(c6)
            out.append(c6)

    sl_t = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    scored: list[tuple[float, str]] = []
    for _, r in sl_t.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        try:
            rl = float(r.get("ret_lag1") or 0.0)
            vs = float(r.get("vol_surge_ratio") or 0.0)
        except (TypeError, ValueError):
            continue
        if rl + 1e-12 < lag_min or rl + 1e-12 >= min(lag_max, block):
            continue
        ic = stocks_mod.industry_code_for_stock(code)
        if not ic or str(ic) not in hot_ind_keys:
            continue
        sweet = 1.0 - abs(rl - 0.10) / max(0.06, block - lag_min)
        scored.append((sweet + 0.10 * min(vs, 2.5), code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    for _, code in scored[: max_codes // 2]:
        _add(code)
        ic = stocks_mod.industry_code_for_stock(code)
        if ic:
            for peer in stocks_mod.peers_for_industry(ic)[:16]:
                _add(peer)
    return out[:max_codes]


def oversold_bounce_candidate_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    max_codes: int = 220,
) -> list[str]:
    """
    전일·최근 낙폭 후 거래량·업종 열기로 반등 가능 종목을 후보에 넣습니다.

    6/29처럼 금호·건설 테마 연속일에 전일 하락 종목이 급등하는 패턴을 풀에 포함.
    """
    from .. import stocks as stocks_mod

    allowed = {str(c).zfill(6) for c in listing_codes}
    out: list[str] = []
    seen: set[str] = set()

    def _add(code: str) -> None:
        c6 = str(code).zfill(6)
        if c6 in allowed and c6 not in seen:
            seen.add(c6)
            out.append(c6)

    hot_inds: set[str] = set()
    lookback = max(1, int(config.PRED_INDUSTRY_HEAT_LOOKBACK_DAYS))
    d: date | None = target_day
    session_days: list[date] = []
    for _ in range(lookback + 1):
        if d is None:
            break
        try:
            d = trading_calendar.last_trading_day_before(d)
        except ValueError:
            break
        session_days.append(d)
    for sess in session_days:
        sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(sess)]
        for _, r in sl.iterrows():
            try:
                rp = float(r.get("return_pct") or 0.0)
            except (TypeError, ValueError):
                continue
            if rp + 1e-12 < 0.10:
                continue
            ic = stocks_mod.industry_code_for_stock(str(r["Code"]))
            if ic:
                hot_inds.add(str(ic))

    sl_t = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    scored: list[tuple[float, str]] = []
    for _, r in sl_t.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        try:
            rl = float(r.get("ret_lag1") or 0.0)
            vs = float(r.get("vol_surge_ratio") or 0.0)
        except (TypeError, ValueError):
            continue
        if rl > -0.018:
            continue
        ic = stocks_mod.industry_code_for_stock(code)
        ind_hot = bool(ic and str(ic) in hot_inds)
        if vs + 1e-12 >= 0.06 or ind_hot or rl <= -0.05:
            scored.append((abs(rl) + 0.12 * min(vs, 2.5) + (0.14 if ind_hot else 0.0), code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    for _, code in scored[: max_codes]:
        _add(code)
        ic = stocks_mod.industry_code_for_stock(code)
        if ic:
            for peer in stocks_mod.peers_for_industry(ic)[:12]:
                _add(peer)
    return out[:max_codes]


def observation_day_burst_peer_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    max_codes: int = 480,
) -> list[str]:
    """
    당일 관측 시점 업종 상한·확산 열기 — 전일 급등 히스토리 없이도 동업종 전체를 풀에 넣습니다.

    7/1 건설처럼 당일 테마가 터지는 날, ML enrich 상위 160 밖 종목이 아예 예측 행으로
    생성되지 않는 문제를 막습니다.
    """
    from .. import stocks as stocks_mod
    from .market_features import IndustryFeatureCache, ohlcv_lookup

    allowed = {str(c).zfill(6) for c in listing_codes}
    ohlcv_idx = ohlcv_lookup(returns_ml)
    if ohlcv_idx is None:
        return []
    ind_cache = IndustryFeatureCache(target_day, ohlcv_idx, returns_ml, frozenset())
    ind_sample: dict[str, str] = {}
    for code in listing_codes:
        ic = stocks_mod.industry_code_for_stock(code)
        if not ic:
            continue
        k = str(ic)
        if k not in ind_sample:
            ind_sample[k] = str(code).zfill(6)
    if not ind_sample:
        return []
    ind_cache.prewarm(ind_sample.values())

    lim_min = float(config.PRED_OBS_BURST_LIM_MIN)
    prior_max = float(config.PRED_OBS_BURST_PRIOR_MAX)
    br_min = float(config.PRED_OBS_BURST_BREADTH_MIN)
    mom_min = float(config.PRED_OBS_BURST_MOM_MIN)

    scored_inds: list[tuple[float, str, float, float, float]] = []
    for ic, sample in ind_sample.items():
        mom, _, lim = ind_cache.industry_feats(sample)
        prior = ind_cache.prior_hot(sample)
        br = ind_cache.sector_breadth(sample)
        if lim + 1e-12 < lim_min:
            continue
        if prior + 1e-12 >= prior_max:
            continue
        if br + 1e-12 < br_min and mom + 1e-12 < mom_min:
            continue
        score = lim * (1.0 - min(0.85, prior)) * (0.50 + 0.50 * br)
        if (
            prior + 1e-12 < float(config.PRED_OBS_BURST_COLD_PRIOR_MAX)
            and lim + 1e-12 >= float(config.PRED_OBS_BURST_COLD_LIM_MIN)
        ):
            score += float(config.PRED_OBS_BURST_COLD_SCORE_BOOST)
        scored_inds.append((score, ic, lim, prior, br))
    scored_inds.sort(key=lambda x: (-x[0], -x[2], x[3]))

    max_inds = int(config.PRED_OBS_BURST_MAX_INDUSTRIES)
    cold_cap = int(config.PRED_OBS_BURST_COLD_INDUSTRY_MAX)
    cold_inds = [
        row
        for row in scored_inds
        if row[3] + 1e-12 < float(config.PRED_OBS_BURST_COLD_PRIOR_MAX)
        and row[2] + 1e-12 >= float(config.PRED_OBS_BURST_COLD_LIM_MIN)
    ]
    other_inds = [row for row in scored_inds if row not in cold_inds]
    picked = cold_inds[:cold_cap] + other_inds[: max(0, max_inds - cold_cap)]

    out: list[str] = []
    seen: set[str] = set()
    n_inds = len(picked)
    base_quota = max(22, max_codes // max(1, n_inds))

    for score, ic, lim, prior, br in picked:
        n_peers = base_quota
        if lim + 1e-12 >= 0.34:
            n_peers = max(n_peers, int(config.PRED_OBS_BURST_PEERS_HOT))
        if (
            prior + 1e-12 < float(config.PRED_OBS_BURST_COLD_PRIOR_MAX)
            and lim + 1e-12 >= float(config.PRED_OBS_BURST_COLD_LIM_MIN)
        ):
            n_peers = max(n_peers, int(config.PRED_OBS_BURST_PEERS_FULL))
        for peer in stocks_mod.peers_for_industry(ic)[:n_peers]:
            c6 = str(peer).zfill(6)
            if c6 in allowed and c6 not in seen:
                seen.add(c6)
                out.append(c6)
                if len(out) >= max_codes:
                    return out
    return out


def priority_cold_limit_peer_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    max_codes: int = 220,
) -> list[str]:
    """당일 상한 열기 + 낮은 전일 과열(건설 후발주) — must_keep 최우선 고정."""
    from .. import stocks as stocks_mod
    from .market_features import IndustryFeatureCache, ohlcv_lookup

    allowed = {str(c).zfill(6) for c in listing_codes}
    ohlcv_idx = ohlcv_lookup(returns_ml)
    if ohlcv_idx is None:
        return []
    ind_cache = IndustryFeatureCache(target_day, ohlcv_idx, returns_ml, frozenset())
    ind_sample: dict[str, str] = {}
    for code in listing_codes:
        ic = stocks_mod.industry_code_for_stock(code)
        if not ic:
            continue
        k = str(ic)
        if k not in ind_sample:
            ind_sample[k] = str(code).zfill(6)
    ind_cache.prewarm(ind_sample.values())

    lim_min = float(config.PRED_OBS_BURST_COLD_LIM_MIN)
    prior_max = float(config.PRED_OBS_BURST_COLD_PRIOR_MAX)
    n_peers = int(config.PRED_PRIORITY_COLD_PEERS)
    max_inds = int(config.PRED_PRIORITY_COLD_MAX_INDUSTRIES)

    scored: list[tuple[float, str, float, float]] = []
    for ic, sample in ind_sample.items():
        _mom, _, lim = ind_cache.industry_feats(sample)
        prior = ind_cache.prior_hot(sample)
        if lim + 1e-12 < lim_min or prior + 1e-12 >= prior_max:
            continue
        if lim + 1e-12 > float(config.PRED_PRIORITY_COLD_LIM_HIGH):
            continue
        br = ind_cache.sector_breadth(sample)
        if br + 1e-12 < float(config.PRED_PRIORITY_COLD_BREADTH_MIN):
            continue
        score = lim * (1.0 - min(0.85, prior))
        if (
            lim + 1e-12 >= float(config.PRED_PRIORITY_COLD_LIM_MIN)
            and prior + 1e-12 < prior_max
        ):
            score += float(config.PRED_PRIORITY_COLD_SCORE_BOOST)
        scored.append((score, ic, lim, prior))
    scored.sort(key=lambda x: (-x[0], -x[2]))

    out: list[str] = []
    seen: set[str] = set()
    for _, ic, lim, _prior in scored[:max_inds]:
        peers_n = n_peers
        if lim + 1e-12 >= float(config.PRED_PRIORITY_COLD_LIM_MIN):
            peers_n = max(peers_n, int(config.PRED_OBS_BURST_PEERS_FULL))
        for peer in stocks_mod.peers_for_industry(ic)[:peers_n]:
            c6 = str(peer).zfill(6)
            if c6 in allowed and c6 not in seen:
                seen.add(c6)
                out.append(c6)
                if len(out) >= max_codes:
                    return out
    return out


def ml_scoring_candidate_codes(
    listing_codes: list[str],
    listing_names: dict[str, str],
    news_text_blob: str,
    ctx: tuple[frozenset[str], dict[str, frozenset[str]]],
    min_keyword_hits: int,
) -> list[str]:
    """
    ML ``predict_proba`` 대상 종목을 종목 관련 신호가 있는 코드로만 축소합니다.

    후보가 적어도 전 종목 폴백은 하지 않습니다(저유동·범용 키워드 오탐 방지).
    """
    kw_news, profile = ctx
    spec_news = filter_specific_keywords(kw_news)
    mention_gate = float(config.PRED_MENTION_GATE_MIN)
    need_hits = max(1, int(min_keyword_hits))
    pool_hits = max(1, int(config.PRED_ML_POOL_MIN_KEYWORD_HITS))
    need_hits = min(need_hits, pool_hits)
    out: list[str] = []
    for code in listing_codes:
        name = listing_names.get(code, "") or ""
        if name_mention_score(news_text_blob, name) >= mention_gate:
            out.append(code)
            continue
        hist = profile.get(code)
        if hist and len(filter_specific_keywords(hist & spec_news)) >= need_hits:
            out.append(code)
            continue
    return out


def day_candidate_codes(
    listing_codes: list[str],
    listing_names: dict[str, str],
    news_text_blob: str,
    ctx: tuple[frozenset[str], dict[str, frozenset[str]]],
    returns_ml: pd.DataFrame,
    target_day: date,
    news_ctx_bundle: dict[str, Any] | None = None,
) -> list[str]:
    """뉴스 프로필 ∪ 시세 모멘텀 후보(예측·학습 동일 유니버스)."""
    from .. import stocks as stocks_mod

    listing_codes = [
        str(c).zfill(6)
        for c in listing_codes
        if stocks_mod.is_common_equity_code(str(c).zfill(6))
    ]
    news = ml_scoring_candidate_codes(
        listing_codes,
        listing_names,
        news_text_blob,
        ctx,
        int(config.PRED_ML_POOL_MIN_KEYWORD_HITS),
    )
    mom = pred_hybrid.momentum_candidate_codes(
        returns_ml, target_day, listing_codes
    )
    flow_codes = investor_flow.investor_flow_candidate_codes(
        returns_ml, target_day, listing_codes
    )
    news_ctx_codes: list[str] = []
    if news_ctx_bundle:
        today_v = tfidf_vector(
            news_text_blob,
            list(news_ctx_bundle.get("vocab") or []),
            news_ctx_bundle.get("idf"),
        )
        news_ctx_codes = affinity_candidate_codes(
            listing_codes,
            dict(news_ctx_bundle.get("profiles") or {}),
            today_v,
        )
    hot_peers = prior_day_hot_peer_codes(returns_ml, target_day, listing_codes)
    burst_peers = burst_industry_peer_codes(returns_ml, target_day, listing_codes)
    bounce_codes = oversold_bounce_candidate_codes(returns_ml, target_day, listing_codes)
    rotation_codes = theme_rotation_peer_codes(returns_ml, target_day, listing_codes)
    obs_burst = observation_day_burst_peer_codes(returns_ml, target_day, listing_codes)
    priority_burst = priority_cold_limit_peer_codes(returns_ml, target_day, listing_codes)
    gap_codes = open_gap_breakout_candidate_codes(
        returns_ml, target_day, listing_codes, max_codes=180
    )
    rs_codes = pred_hybrid.relative_strength_candidate_codes(
        returns_ml, target_day, listing_codes
    )
    return list(
        dict.fromkeys(
            news
            + mom
            + news_ctx_codes
            + flow_codes
            + priority_burst
            + gap_codes
            + obs_burst
            + hot_peers
            + burst_peers
            + rotation_codes
            + bounce_codes
            + rs_codes
        )
    )


def open_gap_breakout_candidate_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    max_codes: int = 160,
    min_gap: float = 0.04,
) -> list[str]:
    """
    당일 시가갭이 큰 종목(장 시작 확정) — 캡 컷에서 살아남아야 하는 돌파 후보.

    ``open_gap`` 은 시가/전일종가이므로 14:30 누수 없음. 갭↑ + 전일 과열↓ 우선.
    """
    allowed = {str(c).zfill(6) for c in listing_codes}
    sl = returns_ml.loc[returns_ml["Date"] == pd.Timestamp(target_day)]
    if sl.empty:
        return []
    scored: list[tuple[float, str]] = []
    for _, r in sl.iterrows():
        code = str(r["Code"]).zfill(6)
        if code not in allowed:
            continue
        try:
            gap = float(r.get("open_gap") or 0.0)
            rl = float(r.get("ret_lag1") or 0.0)
            vs = float(r.get("vol_surge_ratio") or 0.0)
        except (TypeError, ValueError):
            continue
        if gap + 1e-12 < min_gap:
            continue
        # 갭 크기가 1순위 — 전일 과열 갭도 캡에 남겨 Hit@K 리콜
        quiet = max(0.0, 0.12 - max(0.0, rl))
        score = gap * 14.0 + quiet * 1.2 + 0.10 * min(max(vs, 0.0), 2.5)
        if gap + 1e-12 >= 0.08:
            score += 2.0
        scored.append((score, code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [c for _, c in scored[: max(1, max_codes)]]


def industry_must_keep_codes(
    returns_ml: pd.DataFrame,
    target_day: date,
    listing_codes: list[str],
    *,
    max_codes: int | None = None,
) -> list[str]:
    """
    burst·hot·rotation·RS·bounce·갭돌파 peer — ML pool cap 에서 잘리지 않도록
    반드시 추론 풀에 남겨 둘 종목(앞쪽이 우선순위).
    """
    cap = max_codes if max_codes is not None else int(config.PRED_INDUSTRY_MUST_KEEP_MAX)
    priority = priority_cold_limit_peer_codes(returns_ml, target_day, listing_codes)
    gap = open_gap_breakout_candidate_codes(
        returns_ml, target_day, listing_codes, max_codes=160
    )
    obs = observation_day_burst_peer_codes(returns_ml, target_day, listing_codes)
    burst = burst_industry_peer_codes(returns_ml, target_day, listing_codes)
    hot = prior_day_hot_peer_codes(returns_ml, target_day, listing_codes)
    rot: list[str] = []
    if config.PRED_THEME_ROTATION_ENABLED:
        rot = theme_rotation_peer_codes(
            returns_ml,
            target_day,
            listing_codes,
            max_codes=max(
                int(config.PRED_THEME_ROTATION_ENRICH_MAX),
                80,
            ),
        )
    bounce = oversold_bounce_candidate_codes(returns_ml, target_day, listing_codes)
    rs_codes = pred_hybrid.relative_strength_candidate_codes(
        returns_ml, target_day, listing_codes, top_k=140
    )
    return list(
        dict.fromkeys(
            priority + gap + obs + rot + bounce + burst + rs_codes + hot
        )
    )[: max(1, cap)]


_day_candidate_codes = day_candidate_codes
