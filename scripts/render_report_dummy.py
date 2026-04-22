"""
UI 미리보기용: 더미 DayReport로 ``render_dated_n_report`` 와 동일한 HTML 생성.

  python scripts/render_report_dummy.py

출력: output/report_dummy_sample.html
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src import config, predict, report  # noqa: E402


def _hit(cal: date, title: str, matched: str, link: str = "https://example.com/dummy") -> dict:
    return {"day": cal, "title": title, "matched": matched, "link": link}


def main() -> None:
    n_day = date(2026, 4, 10)
    t_day = date(2026, 4, 13)

    rows = [
        {
            "code": "005930",
            "market_segment": "kospi",
            "name": "삼성전자",
            "keywords": ["반도체", "HBM", "AI", "메모리"],
            "pred_ret": 24.50,
            "actual_ret": 0.215,
            "actual_big": True,
            "pred_high": True,
            "pred_reason_hit_line": "36개 일치",
            "pred_reason_summary": "36개 일치 (더미)",
            "pred_reason_detail_html": "과거 20% 이상 급등일 뉴스 키워드와 36개 일치<br/>"
            "뉴스 본문·제목에 종목명 다수 등장<br/>"
            "표시 예측 수익률(%)은 … (더미)",
            "gap_analysis_html": "<p>예측 <strong>24.50%</strong> vs 실제 <strong>21.50%</strong> (더미 갭 설명).</p>",
            "cumulative_accuracy_avg": (0.215 * 100.0) / 24.50,
            "pred_news_hits": [
                _hit(n_day - timedelta(days=1), "[더미] 반도체 장비 수주 보도", "반도체"),
                _hit(n_day - timedelta(days=2), "[더미] 삼성전자 실적 전망", "삼성전자"),
            ],
            "actual_news_hits": [
                _hit(t_day, "[더미] 당일 특이 거래량 관련", "삼성전자"),
            ],
            "disclosure_hits": [
                {
                    "kind": "계약",
                    "title": "[더미] 단일판매·공급계약체결",
                    "link": "https://example.com/disclosure-dummy",
                },
            ],
        },
        {
            "code": "000660",
            "market_segment": "kospi",
            "name": "SK하이닉스",
            "keywords": ["D램", "낸드"],
            "pred_ret": 28.00,
            "actual_ret": 0.052,
            "actual_big": False,
            "pred_high": True,
            "pred_reason_hit_line": "18개 일치",
            "pred_reason_summary": "18개 일치 (더미)",
            "pred_reason_detail_html": "과거 20% 이상 급등일 뉴스 키워드와 18개 일치<br/>(더미 상세)",
            "gap_analysis_html": "<p>예측은 높았으나 실제 급등 구간에는 못 미친 더미 케이스.</p>",
            "cumulative_accuracy_avg": (0.052 * 100.0) / 28.00,
            "pred_news_hits": [
                _hit(n_day, "[더미] 메모리 가격 동향", "D램"),
            ],
            "actual_news_hits": [],
        },
        {
            "code": "060250",
            "market_segment": "kosdaq",
            "name": "NHN",
            "keywords": ["플랫폼"],
            "pred_ret": None,
            "actual_ret": 0.229,
            "actual_big": True,
            "pred_high": False,
            "pred_reason_hit_line": "—",
            "pred_reason_summary": "모델 후보 밖 (더미)",
            "pred_reason_detail_html": "<p>더미: 예측 후보에서 제외된 종목.</p>",
            "gap_analysis_html": "<p>실제만 20% 이상 (더미).</p>",
            "cumulative_accuracy_avg": None,
            "pred_news_hits": [],
            "actual_news_hits": [
                _hit(t_day, "[더미] 플랫폼 규제 이슈", "플랫폼"),
            ],
        },
    ]

    for row in rows:
        row["rise_reason_html"] = predict.explain_rise_reason_html(
            actual_ret=row.get("actual_ret"),
            t_trading_day=t_day,
            actual_news_hits=row.get("actual_news_hits"),
            disclosure_hits=row.get("disclosure_hits"),
            news_evidence_collected=True,
        )

    day = report.DayReport(
        trading_day=t_day,
        predictions=[],
        rows_compare=rows,
        false_negatives=[
            {
                "code": "035420",
                "name": "NAVER",
                "pred_ret": 22.0,
                "actual_ret": -0.031,
                "keywords": ["검색", "AI"],
                "analysis": "더미: 예측은 양수였으나 실제는 음수로 마감한 사례 설명 블록입니다.",
            }
        ],
        news_titles_sample=[
            "[더미] 코스피 외국인 매매 동향",
            "[더미] 금리·환율 시장 요약",
        ],
        news_highlight_terms=["반도체", "AI", "2차전지", "바이오"],
        actual_big_movers=[],
    )

    meta = {
        "train_range": "2020-01-01 ~ 2026-04-09 (dummy)",
        "test_range": "dummy · N→T 단일일 미리보기",
        "threshold": f"{config.BIG_MOVE_THRESHOLD * 100:.0f}%",
        "news_source": "DUMMY (scripts/render_report_dummy.py)",
        "use_decision_cutoff": True,
        "cutoff_kst": f"{config.NEWS_CUTOFF_KST_HOUR:02d}:{config.NEWS_CUTOFF_KST_MINUTE:02d}",
        "run_subtitle": "더미 샘플 — 실제 파이프라인 결과 아님",
        "n_days": 1,
        "total_preds": 42,
        "movers_data_note": "이 HTML은 레이아웃 확인용 더미입니다. 수치·기사·종목은 실제와 무관합니다.",
    }

    rollup_path = config.OUTPUT_DIR / "report_dummy_sample.html"
    report.render_dated_n_report(
        n_day=n_day,
        t_day=t_day,
        day=day,
        meta=meta,
        is_live_n=False,
        rollup_path=rollup_path,
        row_id_prefix="dummy-",
    )
    print(rollup_path)


if __name__ == "__main__":
    main()
