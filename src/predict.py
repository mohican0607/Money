"""
과거 급등-뉴스 패턴을 이용한 익일 급등 후보 스코어링(휴리스틱).

훈련 이벤트에서 종목별로 「급등일에 함께 나온 뉴스 키워드」 프로필을 만들고,
예측일 early 뉴스 blob의 키워드와 교집합 크기·종목명 언급 횟수로 점수를 매깁니다.
예측 상승률(%)은 후보 산출 시 과거 급등 평균·보정으로 내부 기준을 잡고, **최종 표시값**은
ML 경로에서는 급등 확률을, 휴리스틱만 쓸 때는 상위 후보 간 **점수 순위**를
``11~PRED_RETURN_MAX%`` 구간에 선형 정렬해 적중률 표기와 맞춥니다.
선택적으로 ``ml_move_rank`` 감독학습 랭커가 후보 **순위**를 확률 기준으로 바꿉니다(``PRED_USE_ML_RANKER``).
"""
from __future__ import annotations

import html
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from . import config
from .features import BreakoutEvent, keyword_set, name_mention_score


@dataclass
class PredictionRow:
    """한 종목에 대한 스코어링 결과(리포트 테이블·갭 분석에 전달)."""

    code: str
    name: str
    score: float
    predicted_return_pct: float
    matched_keywords: list[str]
    reasons: list[str]
    ml_prob: float | None = None


def _build_code_keyword_profile(train_events: list[BreakoutEvent]) -> dict[str, frozenset[str]]:
    """
    훈련 이벤트에서 종목코드별로 등장한 모든 ``news_keywords`` 를 합집합하여 프로필 생성.
    """
    acc: dict[str, set[str]] = defaultdict(set)
    for e in train_events:
        acc[e.code].update(e.news_keywords)
    return {c: frozenset(v) for c, v in acc.items()}


def _historical_mean_return(train_events: list[BreakoutEvent], code: str) -> float:
    """
    해당 ``code`` 의 과거 급등 이벤트 상승률의 스무딩 평균(소수).

    - 종목 이력이 없으면: 전체 훈련 급등 사건 평균을 사용
    - 종목 이력이 적으면: 종목 평균과 전체 평균을 가중 혼합(베이지안 스무딩)
    - 최종 값은 [0.20, 0.35] 구간으로 클램프
    """
    all_xs = [e.return_pct for e in train_events]
    global_mean = sum(all_xs) / len(all_xs) if all_xs else 0.20
    prior = float(min(0.35, max(0.20, global_mean)))

    xs = [e.return_pct for e in train_events if e.code == code]
    if not xs:
        return prior

    code_mean = sum(xs) / len(xs)
    # 이력이 적은 종목은 전체 평균으로 당겨 과대/과소 추정을 완화.
    prior_strength = 5.0
    n = float(len(xs))
    blended = (n * code_mean + prior_strength * prior) / (n + prior_strength)
    return float(min(0.35, max(0.20, blended)))


def _count_code_events(train_events: list[BreakoutEvent], code: str) -> int:
    """해당 종목의 훈련 급등 이벤트 건수."""
    return sum(1 for e in train_events if e.code == code)


def _calibrate_predicted_return(
    base_ret: float,
    *,
    n_hit: int,
    mention: float,
    code_event_count: int,
) -> float:
    """
    기본 예측치(base_ret)를 신호 강도 기반으로 완만하게 보정.

    과거 버전은 곱셈 감쇠가 겹쳐 예측이 과도하게 낮아져 (실제÷예측) 분포가 악화되었습니다.
    여기서는 키워드·종목명 신호로 ``base`` 근처만 조정해, 과대·과소 예측을 동시에 완화합니다.
    """
    if not config.PRED_RETURN_CALIBRATION_ENABLED:
        lo = min(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
        hi = max(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
        return float(min(hi, max(lo, base_ret)))

    hit = max(0, int(n_hit))
    m = max(0.0, min(1.0, float(mention)))

    # 신호 0~1: 키워드 일치 비중 + 종목명 언급(0~1 스코어) 혼합
    w_hit = min(1.0, hit / 7.0)
    w_m = min(1.0, m / 0.5) if m > 0 else 0.0
    signal = 0.62 * w_hit + 0.38 * w_m
    signal = max(0.0, min(1.0, signal))

    # base 주변만 조정: 약한 신호는 소폭 하향, 강한 신호는 소폭 상향 (대략 0.94~1.06배)
    mult = 0.94 + 0.12 * signal
    if code_event_count == 0:
        mult *= 0.98
    elif code_event_count <= 2:
        mult *= 0.99

    calibrated = float(base_ret) * mult
    lo = min(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
    hi = max(config.PRED_RETURN_MIN, config.PRED_RETURN_MAX)
    return float(min(hi, max(lo, calibrated)))


# ``ml_move_rank._ML_RETURN_MAP_FLOOR_PCT`` 와 동일 의미(순환 import 피해 여기에 둠).
_HEURISTIC_RETURN_MAP_FLOOR_PCT = 11.0


def _display_return_pct_from_heuristic_rank(
    score: float, *, score_min: float, score_max: float
) -> float:
    """상위 후보 묶음 안에서 휴리스틱 점수를 [하한, PRED_RETURN_MAX%%]로 단조 매핑."""
    lo = _HEURISTIC_RETURN_MAP_FLOOR_PCT
    hi = float(config.PRED_RETURN_MAX) * 100.0
    if score_max <= score_min + 1e-12:
        t = 1.0
    else:
        t = (float(score) - float(score_min)) / (float(score_max) - float(score_min))
    t = max(0.0, min(1.0, t))
    return lo + (hi - lo) * t


def build_scoring_context(
    news_text_blob: str,
    train_events: list[BreakoutEvent],
) -> tuple[frozenset[str], dict[str, frozenset[str]]]:
    """
    당일 뉴스 blob의 키워드 집합과 종목별 히스토리 프로필을 한 번만 계산합니다.

    ``predict_for_trading_day`` 가 종목 루프마다 동일 연산을 반복하지 않도록 합니다.
    """
    return keyword_set(news_text_blob, k=100), _build_code_keyword_profile(train_events)


def prediction_row_for_code(
    code: str,
    listing_names: dict[str, str],
    train_events: list[BreakoutEvent],
    news_text_blob: str,
    ctx: tuple[frozenset[str], dict[str, frozenset[str]]],
    min_keyword_hits: int,
) -> PredictionRow | None:
    """
    단일 종목에 대해 키워드 교집합 수 + 종목명 언급 점수로 스코어하고 ``PredictionRow`` 를 만듭니다.

    조건: 교집합 개수가 ``min_keyword_hits`` 미만이면서 종목명 언급 점수가 0.2 미만이면 후보 제외(None).

    Args:
        ctx: ``build_scoring_context`` 의 반환값을 그대로 넘깁니다.
    """
    kw_news, profile = ctx
    name = listing_names.get(code, "")
    hist_kw = profile.get(code, frozenset())
    inter = hist_kw & kw_news
    n_hit = len(inter)
    mention = name_mention_score(news_text_blob, name)
    score = n_hit * 1.0 + mention * 5.0
    if n_hit < min_keyword_hits and mention < 0.2:
        return None
    matched = sorted(inter, key=len, reverse=True)[:25]
    reasons: list[str] = []
    if n_hit:
        reasons.append(f"과거 20% 이상 급등일 뉴스 키워드와 {n_hit}개 일치")
    if mention >= 0.2:
        reasons.append("뉴스 본문·제목에 종목명 다수 등장")
    reasons.append(
        "표시 예측 상승률(%)은 이 종목 과거 급등일 실제 상승률 평균과 "
        "전체 훈련 급등 평균을 함께 반영한 스무딩 값입니다(20~35% 구간). "
        "훈련 사례가 없으면 전체 훈련 급등 평균을 사용합니다."
    )
    base_ret = _historical_mean_return(train_events, code)
    pred_ret = _calibrate_predicted_return(
        base_ret,
        n_hit=n_hit,
        mention=mention,
        code_event_count=_count_code_events(train_events, code),
    )
    if config.PRED_RETURN_CALIBRATION_ENABLED:
        reasons.append(
            "예측 수익률은 신호 강도(키워드 일치·종목명 언급)에 따라 기본값 주변으로만 완만히 보정했습니다."
        )
    return PredictionRow(
        code=code,
        name=name,
        score=score,
        predicted_return_pct=pred_ret * 100,
        matched_keywords=matched,
        reasons=reasons,
    )


def predict_for_trading_day(
    target_day: date,
    listing_codes: list[str],
    listing_names: dict[str, str],
    train_events: list[BreakoutEvent],
    news_text_blob: str,
    top_n: int = 40,
    min_keyword_hits: int = 2,
    *,
    ml_bundle: dict[str, Any] | None = None,
    returns_ml: Any = None,
) -> list[PredictionRow]:
    """
    상장 전 종목(또는 리스트)에 대해 스코어를 매기고 상위 ``top_n`` 만 반환합니다.

    ``target_day`` 는 시그니처상 관측일이나, 실제 예측 입력은 호출자가 만든 ``news_text_blob``
    (이미 T에 맞는 early 윈도우로 집계된 문자열)입니다.

    Args:
        min_keyword_hits: 훈련 이벤트가 없으면 main에서 0으로 내려 전체 완화할 수 있음.
        ml_bundle: ``ml_move_rank.fit_or_load_classifier`` 결과. ``pipeline`` 이 있으면
            급등 확률 순으로 후보를 고릅니다.
        returns_ml: ``stocks.enrich_daily_returns_for_ml(daily_returns_table(...))`` 결과.
            ML 랭커 시세 피처용(없으면 ML 경로를 건너뜁니다).
    """
    if (
        ml_bundle is not None
        and ml_bundle.get("pipeline") is not None
        and returns_ml is not None
    ):
        from . import ml_move_rank

        return ml_move_rank.rank_predictions_ml(
            target_day=target_day,
            listing_codes=listing_codes,
            listing_names=listing_names,
            train_events=train_events,
            news_text_blob=news_text_blob,
            returns_ml=returns_ml,
            pipeline=ml_bundle["pipeline"],
            top_n=top_n,
            min_keyword_hits=min_keyword_hits,
        )
    if ml_bundle is not None and ml_bundle.get("pipeline") is not None and returns_ml is None:
        print(
            "ML 랭커: returns_ml 이 없어 시세 피처를 쓸 수 없습니다 → 휴리스틱 순위만 사용합니다.",
            flush=True,
        )

    ctx = build_scoring_context(news_text_blob, train_events)
    ranked: list[PredictionRow] = []

    for code in listing_codes:
        row = prediction_row_for_code(
            code,
            listing_names,
            train_events,
            news_text_blob,
            ctx,
            min_keyword_hits,
        )
        if row is not None:
            ranked.append(row)

    ranked.sort(key=lambda x: x.score, reverse=True)
    top = ranked[:top_n]
    if top:
        sc = [x.score for x in top]
        smin, smax = min(sc), max(sc)
        note = (
            f"표시 예측 상승률은 상위 후보 내 휴리스틱 점수(키워드·종목명)를 "
            f"{_HEURISTIC_RETURN_MAP_FLOOR_PCT:.0f}~{config.PRED_RETURN_MAX * 100:.0f}% 구간에 선형 정렬한 값입니다."
        )
        for r in top:
            r.predicted_return_pct = _display_return_pct_from_heuristic_rank(
                r.score, score_min=smin, score_max=smax
            )
            r.reasons = [note] + list(r.reasons)
    return top


def aggregate_news_for_window(
    news_by_calendar: dict[date, list[dict[str, str]]],
    start: date,
    end: date,
) -> str:
    """
    캘린더 구간 [start, end]의 모든 뉴스 제목·설명을 순서대로 이어 붙인 문자열.

    컷오프가 꺼졌을 때 ``news_window_for_target_trading_day`` 와 함께 쓰입니다.
    """
    parts: list[str] = []
    d = start
    while d <= end:
        for row in news_by_calendar.get(d, []):
            parts.append(row.get("title", ""))
            parts.append(row.get("description", ""))
        d += timedelta(days=1)
    return "\n".join(parts)


def explain_return_gap_html(
    *,
    pred_ret_pct: float | None,
    actual_ret: float | None,
    actual_intraday_pct: float | None = None,
    prediction_row: PredictionRow | None,
    news_blob_early: str,
    kospi_change_hint: str | None,
    late_keywords_matched: bool | None,
    disclosure_hits: list[dict] | None = None,
) -> str:
    """
    예측 상승률(%) vs 실제 상승률(소수) 차이에 대한 참고용 설명(HTML 조각).
    인과·투자 조언이 아닌 후속 점검 포인트 정리.
    """
    parts: list[str] = []

    if actual_ret is None:
        if actual_intraday_pct is not None and str(actual_intraday_pct).strip() != "":
            try:
                ip = float(actual_intraday_pct)
            except (TypeError, ValueError):
                ip = float("nan")
            if math.isfinite(ip):
                if pred_ret_pct is None:
                    return (
                        "<p><strong>장중 등락률</strong> — 종가 미확정 기준 "
                        f"<strong>{ip:.2f}%</strong>입니다. 예측 후보 밖이면 수치 비교는 생략합니다.</p>"
                    )
                pred = float(pred_ret_pct)
                diff = pred - ip
                return (
                    "<p><strong>장중 참고</strong> — "
                    f"실시간(전일대비) 등락률 <strong>{ip:.2f}%</strong> · 예측 <strong>{pred:.2f}%</strong> · "
                    f"(예측−장중) <strong>{diff:+.2f}</strong> 퍼센트포인트. "
                    "장 마감 후에는 일봉 종가 기준으로 다시 집계됩니다.</p>"
                )
        if pred_ret_pct is not None:
            parts.append(
                "<p><strong>실제 상승률 미확정</strong> — T일 장 마감 전이거나 가격 데이터가 없어 "
                f"예측({pred_ret_pct:.2f}%)과의 차이 분석을 생략합니다.</p>"
            )
        return "".join(parts)

    act_pct = float(actual_ret) * 100.0

    if pred_ret_pct is None:
        parts.append(
            f"<p><strong>예측 후보 밖</strong> — 모델이 키워드·종목명 규칙으로 이 종목에 예측 상승률을 붙이지 못했습니다. "
            f"실제 상승률은 <strong>{act_pct:.2f}%</strong>입니다.</p>"
        )
        parts.append(
            "<ul>"
            "<li>과거 급등 사건 프로필·당일 뉴스 키워드가 겹치지 않으면 후보에서 제외됩니다.</li>"
            "<li>실제 급등은 공시·수급·테마 확산 등 뉴스 키워드에 잡히지 않는 요인일 수 있습니다.</li>"
            "<li>오른쪽「실제 맥락 뉴스」에서 당일 보도를 확인해 보세요.</li>"
            "</ul>"
        )
        return "".join(parts)

    pred = float(pred_ret_pct)
    diff = pred - act_pct
    parts.append(
        f"<p><strong>수치</strong> — 예측 <strong>{pred:.2f}%</strong> · 실제 <strong>{act_pct:.2f}%</strong> "
        f"· (예측−실제) <strong>{diff:+.2f}</strong> 퍼센트포인트</p>"
    )

    if abs(diff) < 1.5:
        parts.append("<p>편차가 작아 모델이 방향·크기를 대략 맞춘 편입니다.</p>")
        return "".join(parts)

    bullets: list[str] = []

    if diff > 4.0:
        bullets.append(
            "예측은 과거 해당 종목 급등일의 평균 상승률(하한·상한 클램프)을 쓰기 때문에, "
            "실제보다 높게 나오기 쉽습니다."
        )
        bullets.append(
            "실제가 낮았다면 당일 시장·업종 조정, 차익 실현, 거래량 부족 등으로 테마가 이어지지 않았을 수 있습니다."
        )
        if late_keywords_matched is True:
            cut = f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}"
            bullets.append(
                f"<strong>N-1일 {cut} 이후</strong> 뉴스에 예측에 쓰인 키워드가 포함된 흔적이 있습니다. "
                "장 마감 후·익일 오전 이슈가 예측 시점에는 반영되지 않았을 수 있습니다."
            )
        elif late_keywords_matched is False:
            bullets.append(
                "지연 구간 뉴스에서 예측 키워드가 크게 잡히지는 않았습니다. 다른 촉매 또는 기술적 조정을 의심해 볼 수 있습니다."
            )
    elif diff < -4.0:
        bullets.append(
            "실제가 예측보다 훨씬 높았습니다. 뉴스 키워드만으로는 설명되지 않는 공시·수급·테마 급확산이 있었을 수 있습니다."
        )
        bullets.append("모델 예측치는 과거 패턴의 보수적 평균이라, 단일 일 급등을 과소 추정할 수 있습니다.")
    else:
        bullets.append("편차는 중간 수준입니다. 예측은 참고용 확률 신호이며, 당일 뉴스·공시와 함께 보는 것이 좋습니다.")

    if prediction_row and prediction_row.matched_keywords:
        nk = len(prediction_row.matched_keywords)
        bullets.append(f"예측 시 <strong>{nk}개</strong> 키워드가 뉴스와 맞았습니다. 키워드 수가 많아도 당일 가격이 따라오지 않을 수 있습니다.")

    if prediction_row and prediction_row.name and prediction_row.name in news_blob_early:
        bullets.append("예측 입력 뉴스에 종목명이 직접 등장했습니다. 호재·악재·단순 거론 여부는 기사 제목·본문을 따로 확인해야 합니다.")

    if kospi_change_hint:
        bullets.append(kospi_change_hint)

    dh = list(disclosure_hits or [])
    if dh:
        kinds: list[str] = []
        for x in dh:
            k = str(x.get("kind", "")).strip()
            if k and k not in kinds:
                kinds.append(k)
        if kinds:
            bullets.append(
                "네이버 증권 종목 공시에서 당일 관련 공시가 "
                f"<strong>{len(dh)}건</strong> 확인되었습니다(유형: {', '.join(kinds[:5])}). "
                "뉴스 키워드 외에 공시 내용을 함께 확인해 해석 정확도를 높이세요."
            )
        else:
            bullets.append(
                f"네이버 증권 종목 공시에서 당일 관련 공시가 <strong>{len(dh)}건</strong> 확인되었습니다. "
                "뉴스 키워드 외에 공시 내용을 함께 확인해 해석 정확도를 높이세요."
            )
    else:
        bullets.append(
            "공시·거래정지·대량보유·공매도 등은 뉴스 기반 자동 분석에서 누락될 수 있습니다. 네이버 증권 종목 공시를 함께 확인하세요."
        )

    parts.append("<ul>" + "".join(f"<li>{b}</li>" for b in bullets[:8]) + "</ul>")
    return "".join(parts)


def explain_rise_reason_html(
    *,
    actual_ret: float | None,
    actual_intraday_pct: float | None = None,
    t_trading_day: date | None,
    actual_news_hits: list[dict] | None,
    disclosure_hits: list[dict] | None,
    news_evidence_collected: bool,
) -> str:
    """
    관측일 ``T`` 기준 실제 상승에 대한 참고용 요약(HTML).

    - ``actual_ret`` 이 없으면(미래·장전·데이터 없음) 상승 이유 블록은 비움에 가깝게 안내.
    - 뉴스·공시는 문자열 매칭 결과이며 인과를 단정하지 않음.
    """
    tlabel = t_trading_day.isoformat() if t_trading_day is not None else "관측일"

    if actual_ret is None:
        if actual_intraday_pct is not None:
            try:
                ip = float(actual_intraday_pct)
            except (TypeError, ValueError):
                ip = float("nan")
            if math.isfinite(ip):
                return (
                    "<p class=\"combo-tip-empty\"><strong>상승 이유</strong> — "
                    f"{tlabel} 기준 <strong>장중·실시간 등락률 약 {ip:.2f}%</strong>입니다 "
                    "(종가 확정 전). 장 마감 후 일봉 기준으로 뉴스·공시 매칭 해석이 달라질 수 있습니다.</p>"
                )
        return (
            "<p class=\"combo-tip-empty\"><strong>상승 이유</strong> — "
            f"{tlabel} 기준 <strong>실제 등락률이 아직 없거나 미확정</strong>입니다. "
            "장 마감·시세 반영 후에는 아래에 뉴스·공시 매칭을 채울 수 있습니다.</p>"
        )

    act_pct = float(actual_ret) * 100.0
    if float(actual_ret) <= 0:
        return (
            f"<p><strong>상승 이유</strong> — 전일 대비 종가 기준 <strong>{act_pct:.2f}%</strong>로 "
            "당일은 <strong>상승</strong>으로 보기 어렵습니다(보합·하락).</p>"
        )

    chunks: list[str] = []

    if news_evidence_collected:
        nh = list(actual_news_hits or [])
        if nh:
            chunks.append(
                "<p><strong>관측일·전후 참고 뉴스</strong> "
                "(종목명·키워드가 제목·요약에 포함된 기사, 인과 아님)</p><ul class=\"nl\">"
            )
            for h in nh[:8]:
                day_o = h.get("day")
                day_s = day_o.isoformat() if isinstance(day_o, date) else html.escape(str(day_o or ""), quote=False)
                title = str(h.get("title") or "")
                matched = str(h.get("matched") or "")
                link = (h.get("link") or "").strip()
                te = html.escape(title, quote=False)
                me = html.escape(matched, quote=False)
                if link:
                    le = html.escape(link, quote=True)
                    chunks.append(
                        f'<li><span class="pill">{day_s}</span> '
                        f'<code style="font-size:0.75rem;color:var(--warn)">{me}</code> '
                        f'<a href="{le}" target="_blank" rel="noopener">{te}</a></li>'
                    )
                else:
                    chunks.append(
                        f'<li><span class="pill">{day_s}</span> '
                        f'<code style="font-size:0.75rem;color:var(--warn)">{me}</code> {te}</li>'
                    )
            chunks.append("</ul>")
        else:
            chunks.append(
                "<p>관측일 구간에서 종목명·키워드와 겹친 뉴스 제목을 찾지 못했습니다. "
                "이슈가 제목에 드러나지 않았거나 수집 범위 밖일 수 있습니다.</p>"
            )

        dh = list(disclosure_hits or [])
        if dh:
            chunks.append("<p><strong>당일 공시</strong>(네이버 증권 종목 공시)</p><ul class=\"nl\">")
            for d in dh[:8]:
                kind = html.escape(str(d.get("kind") or ""), quote=False)
                title = html.escape(str(d.get("title") or ""), quote=False)
                link = (d.get("link") or "").strip()
                if link:
                    le = html.escape(link, quote=True)
                    chunks.append(
                        f'<li><code style="font-size:0.75rem;color:#9fd3ff">{kind}</code> '
                        f'<a href="{le}" target="_blank" rel="noopener">{title}</a></li>'
                    )
                else:
                    chunks.append(
                        f'<li><code style="font-size:0.75rem;color:#9fd3ff">{kind}</code> {title}</li>'
                    )
            chunks.append("</ul>")
        else:
            chunks.append("<p>당일 네이버 종목 공시에서 표시할 항목이 없거나 매칭되지 않았습니다.</p>")
    else:
        chunks.append(
            "<p><em>이 실행</em>에서는 관측일 뉴스·공시 상세 매칭을 생략했습니다. "
            "<code>python main.py YYYYMMDD</code> 단일일 리포트에서는 동일 항목이 채워집니다.</p>"
        )

    chunks.append(
        "<p class=\"combo-tip-empty\" style=\"margin-top:10px;font-size:0.82em\">"
        "위는 자동 매칭·공시 목록에 따른 참고 요약이며, 주가 상승의 원인을 단정하지 않습니다.</p>"
    )
    return "".join(chunks)


def explain_miss(
    pred: PredictionRow,
    actual_ret: float | None,
    news_blob: str,
    kospi_change_hint: str | None = None,
) -> str:
    """
    예측은 높았는데 실제 수익이 음수인 경우 리포트용 짧은 설명 문자열(plain text).

    ``false_negatives`` 블록에 들어갑니다.
    """
    bits = []
    if actual_ret is None:
        bits.append("해당 일자에 거래 데이터가 없거나 휴장·신규상장 등으로 상승률을 계산하지 못했습니다.")
        return " ".join(bits)
    if actual_ret >= 0:
        return "음수 구간이 아니므로 오판 집중 분석 대상에서 제외됩니다."
    bits.append(f"실제 상승률은 약 {actual_ret*100:.2f}%였습니다.")
    if pred.matched_keywords:
        bits.append(
            "뉴스 키워드는 과거 급등 사례와 겹쳤으나, 당일에는 테마 확산·거래량 부족· 시장 전체 조정 등으로 가격이 뒤집혔을 수 있습니다."
        )
    if pred.name in news_blob:
        bits.append("종목명이 뉴스에 있었더라도 부정적 이슈·단기 차익 실현만 있었을 가능성이 있습니다.")
    if kospi_change_hint:
        bits.append(kospi_change_hint)
    bits.append("추가로 공시·대주주 매매·거래정지 여부는 이 리포트 범위 밖이므로 네이버 증권 종목 공시에서 확인하는 것이 좋습니다.")
    return " ".join(bits)
