"""TF-IDF 뉴스 맥락 피처·후보 확장·학습 bundle (구 ``news_context_ml``)."""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import date
from typing import Any

import numpy as np

from .. import config, trading_calendar
from .. import news
from ..features import BreakoutEvent, filter_specific_keywords, keyword_set
from . import predict

VOCAB_SIZE = 56  # TF-IDF 상위 용어 수(학습·추론 공통)
_CONTEXT_FEAT_NAMES = (
    "news_cos_code",
    "news_cos_global",
    "news_dot_code",
    "news_lift",
    "news_today_norm",
)
CONTEXT_FEAT_NAMES = _CONTEXT_FEAT_NAMES


def early_blob_for_day(news_by: dict[date, list[dict[str, str]]], d: date) -> str:
    """거래일 ``d`` 의 early 뉴스 텍스트 blob(14:30 컷오프 또는 전체 윈도우)."""
    if config.USE_DECISION_NEWS_INTRADAY_CUTOFF:
        blob, _ = news.aggregate_early_late_for_target(news_by, d)
        return blob
    ws, we = trading_calendar.news_window_for_target_trading_day(d)
    return predict.aggregate_news_for_window(news_by, ws, we)


def build_vocab_idf(
    news_by: dict[date, list[dict[str, str]]],
    days: list[date],
) -> tuple[list[str], np.ndarray, np.ndarray]:
    """훈련 구간 전체 뉴스로 어휘·IDF·급등일 term lift."""
    tf_counter: Counter[str] = Counter()
    doc_freq: Counter[str] = Counter()
    n_docs = 0
    for d in days:
        blob = early_blob_for_day(news_by, d)
        if not blob.strip():
            continue
        toks = filter_specific_keywords(keyword_set(blob, k=280))
        if not toks:
            continue
        n_docs += 1
        tc = Counter(toks)
        tf_counter.update(tc)
        for t in set(toks):
            doc_freq[t] += 1
    vocab = [w for w, _ in tf_counter.most_common(VOCAB_SIZE)]
    if not vocab:
        vocab = ["__pad__"]
    idf = np.array(
        [math.log((1.0 + n_docs) / (1.0 + doc_freq.get(w, 0))) + 1.0 for w in vocab],
        dtype=np.float64,
    )
    lift = np.zeros(len(vocab), dtype=np.float64)
    return vocab, idf, lift


def enrich_vocab_with_breakouts(
    vocab: list[str],
    idf: np.ndarray,
    lift: np.ndarray,
    train_events: list[BreakoutEvent],
    news_by: dict[date, list[dict[str, str]]],
    before_exclusive: date,
) -> np.ndarray:
    """급등 이벤트 당일 뉴스로 term lift 재계산."""
    idx = {w: i for i, w in enumerate(vocab)}
    breakout_tf: Counter[str] = Counter()
    global_tf: Counter[str] = Counter()
    for e in train_events:
        if e.trading_day >= before_exclusive:
            continue
        blob = early_blob_for_day(news_by, e.trading_day)
        toks = filter_specific_keywords(keyword_set(blob, k=280))
        breakout_tf.update(toks)
        global_tf.update(toks)
    out = lift.copy()
    for w, i in idx.items():
        g = global_tf.get(w, 0) / max(1.0, sum(global_tf.values()))
        b = breakout_tf.get(w, 0) / max(1.0, sum(breakout_tf.values()))
        out[i] = math.log1p(max(0.0, b / max(g, 1e-8)))
    return out


def tfidf_vector(
    blob: str, vocab: list[str], idf: np.ndarray
) -> np.ndarray:
    """뉴스 blob → L2 정규화 TF-IDF 벡터(어휘 크기 ``len(vocab)``)."""
    idx = {w: i for i, w in enumerate(vocab)}
    tf = Counter(filter_specific_keywords(keyword_set(blob, k=280)))
    v = np.zeros(len(vocab), dtype=np.float64)
    for w, c in tf.items():
        j = idx.get(w)
        if j is not None:
            v[j] = float(c)
    v = v * idf
    n = float(np.linalg.norm(v))
    if n > 1e-12:
        v /= n
    return v


def _profiles(
    train_events: list[BreakoutEvent],
    news_by: dict[date, list[dict[str, str]]],
    vocab: list[str],
    idf: np.ndarray,
    before_exclusive: date,
) -> tuple[dict[str, np.ndarray], np.ndarray]:
    """급등일 뉴스 TF-IDF 평균 → 종목별 프로필·전체 급등일 global centroid."""
    by_code: dict[str, list[np.ndarray]] = defaultdict(list)
    all_vecs: list[np.ndarray] = []
    for e in train_events:
        if e.trading_day >= before_exclusive:
            continue
        blob = early_blob_for_day(news_by, e.trading_day)
        if not blob.strip():
            continue
        vec = tfidf_vector(blob, vocab, idf)
        by_code[e.code].append(vec)
        all_vecs.append(vec)
    prof = {c: np.mean(vs, axis=0) for c, vs in by_code.items() if vs}
    if all_vecs:
        global_c = np.mean(all_vecs, axis=0)
        gn = float(np.linalg.norm(global_c))
        if gn > 1e-12:
            global_c /= gn
    else:
        global_c = np.zeros(len(vocab), dtype=np.float64)
    return prof, global_c


def context_feature_vector(
    today_vec: np.ndarray,
    code_prof: np.ndarray,
    global_c: np.ndarray,
    lift: np.ndarray,
) -> list[float]:
    """
    ML 랭커용 뉴스 맥락 피처 5종.

    Returns:
        ``[news_cos_code, news_cos_global, news_dot_code, news_lift, news_today_norm]``
    """
    cos_code = float(np.dot(today_vec, code_prof))
    cos_glob = float(np.dot(today_vec, global_c))
    dot_code = float(np.sum(today_vec * code_prof))
    lift_sc = float(np.dot(today_vec, lift))
    norm = float(np.linalg.norm(today_vec))
    return [cos_code, cos_glob, dot_code, lift_sc, norm]


def context_score_from_feats(feats: list[float]) -> float:
    """0~1 뉴스 맥락 적합도(종목별 과거 급등일 뉴스 패턴 유사도 중심)."""
    if len(feats) < 5:
        return 0.0
    cos_c, cos_g, dot_c, lift, _norm = feats
    if cos_c + 1e-12 < 0.10:
        return max(0.0, min(0.22, 0.18 * max(0.0, cos_g)))
    s = (
        0.62 * max(0.0, cos_c)
        + 0.18 * min(1.0, max(0.0, lift) / 2.2)
        + 0.12 * min(1.0, max(0.0, dot_c))
        + 0.08 * max(0.0, cos_g)
    )
    return max(0.0, min(1.0, s))


def affinity_candidate_codes(
    listing_codes: list[str],
    profiles: dict[str, np.ndarray],
    today_vec: np.ndarray,
    *,
    top_k: int = 100,
    min_score: float = 0.26,
) -> list[str]:
    """당일 뉴스와 과거 급등 프로필 유사도가 높은 종목 — 키워드 교집합 없이도 ML 풀에 편입."""
    allowed = {str(c).zfill(6) for c in listing_codes}
    scored: list[tuple[float, str]] = []
    for code in allowed:
        prof = profiles.get(code)
        if prof is None:
            continue
        feats = context_feature_vector(today_vec, prof, np.zeros_like(today_vec), np.zeros_like(today_vec))
        sc = context_score_from_feats(feats)
        if sc + 1e-12 >= min_score:
            scored.append((sc, code))
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [c for _, c in scored[:top_k]]


def make_news_ctx_bundle(
    train_events: list[BreakoutEvent],
    news_by: dict[date, list[dict[str, str]]],
    train_start: date,
    before_exclusive: date,
) -> dict[str, Any]:
    """T 직전 구간 기준 뉴스 맥락 bundle — ML 학습·추론·joblib 캐시에 공통 저장."""
    days = sorted(
        d for d in news_by if train_start <= d < before_exclusive
    )
    vocab, idf, lift = build_vocab_idf(news_by, days)
    lift = enrich_vocab_with_breakouts(
        vocab, idf, lift, train_events, news_by, before_exclusive
    )
    profiles, global_c = _profiles(
        train_events, news_by, vocab, idf, before_exclusive
    )
    return {
        "vocab": vocab,
        "idf": idf,
        "lift": lift,
        "profiles": profiles,
        "global_c": global_c,
    }


def feats_for_code(
    bundle: dict[str, Any],
    blob: str,
    code: str,
) -> tuple[list[float], float]:
    """bundle·당일 뉴스 blob·종목코드 → (ML 피처 5종, 0~1 맥락 점수)."""
    vocab = bundle["vocab"]
    idf = bundle["idf"]
    lift = bundle["lift"]
    profiles: dict[str, np.ndarray] = bundle["profiles"]
    global_c: np.ndarray = bundle["global_c"]
    today = tfidf_vector(blob, vocab, idf)
    prof = profiles.get(str(code).zfill(6))
    if prof is None:
        prof = np.zeros(len(vocab), dtype=np.float64)
    feats = context_feature_vector(today, prof, global_c, lift)
    return feats, context_score_from_feats(feats)
