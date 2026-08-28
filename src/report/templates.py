"""Jinja2 HTML 템플릿 문자열(다크 테마)."""
from __future__ import annotations

import re

ROWS_COMPARE_GROUP_COL_CSS = """
    table.rows-compare { table-layout: fixed; }
    table.rows-compare col.col-group,
    table.rows-compare th.col-group,
    table.rows-compare td.col-group,
    table.rows-compare th:nth-child(1),
    table.rows-compare td:nth-child(1) {
      width: 5.75rem; min-width: 5.75rem; max-width: 5.75rem;
    }
    table.rows-compare td.col-group,
    table.rows-compare td:nth-child(1) { white-space: nowrap; vertical-align: top; }
"""

REPORT_TABLE_INTERACTION_MARKER = "money-report-table-interaction"
# 비교표 정렬·필터·차트 tooltip·장중 실시간 등락률 갱신 스크립트(리포트 </body> 직전 삽입).
REPORT_TABLE_INTERACTION_SNIPPET = r"""<!-- money-report-table-interaction -->
<style>
""" + ROWS_COMPARE_GROUP_COL_CSS + r"""
.stock-chart-popup.stock-chart-popup-floating {
  display: block !important;
  position: fixed !important;
  z-index: 4000 !important;
  min-width: min(720px, calc(100vw - 32px)) !important;
  max-width: min(720px, calc(100vw - 24px)) !important;
  max-height: min(90vh, 520px) !important;
  overflow: auto !important;
  box-sizing: border-box;
}
.pred-live-intraday-pct.live-pct-flash,
.stock-ret-line[data-live-intraday] .stock-ret-pct.live-pct-flash {
  text-decoration: underline;
  text-decoration-color: #e6c07b;
  text-decoration-thickness: 2px;
  text-underline-offset: 3px;
  text-decoration-skip-ink: none;
}
</style>
<script>
(function () {
  // --- 비교표 정렬·시장/상승률 필터 ---
  // 정렬: data-sort-value 를 숫자로 파싱(null 허용).
  function numKey(v) {
    if (v == null || v === "") return null;
    var n = parseFloat(String(v), 10);
    return isNaN(n) ? null : n;
  }
  // 정렬: 종목명 등 문자열 키(소문자).
  function textKey(v) {
    if (v == null) return "";
    return String(v).toLowerCase();
  }
  // 행·열에서 정렬용 data-sort-value 추출.
  function cellSortValue(tr, col) {
    var el = tr.querySelector('[data-sort-col="' + col + '"]');
    if (!el) return null;
    return el.getAttribute("data-sort-value");
  }
  // 비교표 헤더 클릭 정렬(숫자·종목명).
  function bindSortTable(table) {
    var tbody = table.querySelector("tbody");
    if (!tbody) return;
    var ths = table.querySelectorAll("th.sortable-col, th .sortable-col");
    if (!ths.length) return;
    ths.forEach(function (th) {
      th.addEventListener("click", function () {
        var col = th.getAttribute("data-sort");
        if (!col) return;
        var descending = th.getAttribute("data-sort-dir") !== "desc";
        table.querySelectorAll("th.sortable-col, th .sortable-col").forEach(function (h) {
          h.removeAttribute("data-sort-dir");
          h.classList.remove("sort-asc", "sort-desc");
        });
        th.setAttribute("data-sort-dir", descending ? "desc" : "asc");
        th.classList.add(descending ? "sort-desc" : "sort-asc");
        var rows = Array.prototype.slice.call(tbody.querySelectorAll("tr"));
        rows.sort(function (a, b) {
          var va = cellSortValue(a, col);
          var vb = cellSortValue(b, col);
          if (va == null && vb == null) return 0;
          if (va == null) return 1;
          if (vb == null) return -1;
          if (col === "stock") {
            var sa = textKey(va);
            var sb = textKey(vb);
            return descending ? sb.localeCompare(sa) : sa.localeCompare(sb);
          }
          var na = numKey(va);
          var nb = numKey(vb);
          if (na == null && nb == null) return 0;
          if (na == null) return 1;
          if (nb == null) return -1;
          return descending ? nb - na : na - nb;
        });
        rows.forEach(function (r) { tbody.appendChild(r); });
      });
    });
  }
  // 일자 블록별 시장(KOSPI/KOSDAQ)·상승률 구간 라디오 필터.
  function bindMarketRowFilters(root) {
    root.querySelectorAll(".day-market-block").forEach(function (block) {
      var table = block.querySelector("table.rows-compare");
      if (!table) return;
      var tbody = table.querySelector("tbody");
      if (!tbody) return;
      var marketRadios = block.querySelectorAll(".market-filter-radios input[type=radio]");
      var riseRadios = block.querySelectorAll(".rise-filter-radios input[type=radio]");
      if (!marketRadios.length && !riseRadios.length) return;
      // 라디오 선택에 따라 tbody 행 표시/숨김.
      function apply() {
        var marketSel = "all";
        for (var i = 0; i < marketRadios.length; i++) {
          if (marketRadios[i].checked) marketSel = marketRadios[i].value;
        }
        var riseSel = "all";
        for (var j = 0; j < riseRadios.length; j++) {
          if (riseRadios[j].checked) riseSel = riseRadios[j].value;
        }
        tbody.querySelectorAll("tr").forEach(function (tr) {
          var m = tr.getAttribute("data-market") || "other";
          var rb = tr.getAttribute("data-rise-band") || "low";
          var marketVisible = true;
          if (marketSel !== "all") {
            if (m === "kospi" || m === "kosdaq") {
              marketVisible = marketSel === m;
            } else {
              marketVisible = false;
            }
          }
          var riseVisible = true;
          if (riseSel === "all") {
            riseVisible = true;
          } else if (riseSel === "high") {
            riseVisible = rb === "high";
          } else if (riseSel === "mid") {
            riseVisible = rb === "mid";
          }
          if (marketVisible && riseVisible) {
            tr.style.removeProperty("display");
          } else {
            tr.style.display = "none";
          }
        });
      }
      marketRadios.forEach(function (r) { r.addEventListener("change", apply); });
      riseRadios.forEach(function (r) { r.addEventListener("change", apply); });
      apply();
    });
  }
  // 「통합 보기」 popup 고정 위치·호버 바인딩.
  function bindIntegrateTips(root) {
    var scope = root || document;

    // 통합 보기 popup 을 뷰포트 안에 맞게 fixed 배치.
    function positionIntegratePopup(tip) {
      var popup = tip.querySelector(".integrate-tip-popup");
      var anchor = tip.querySelector(".gap-tip-trigger");
      if (!popup || !anchor) return;
      var margin = 10;
      var gap = 6;
      popup.classList.add("integrate-tip-floating");
      popup.style.setProperty("display", "block", "important");
      var pw = popup.offsetWidth;
      var ph = popup.offsetHeight;
      var ar = anchor.getBoundingClientRect();
      var vw = window.innerWidth;
      var vh = window.innerHeight;
      var left = ar.right - pw;
      if (left < margin) left = margin;
      if (left + pw > vw - margin) left = Math.max(margin, vw - pw - margin);
      var top = ar.bottom + gap;
      if (top + ph > vh - margin) top = ar.top - ph - gap;
      if (top < margin) top = margin;
      popup.style.setProperty("left", left + "px", "important");
      popup.style.setProperty("top", top + "px", "important");
    }

    // 통합 보기 popup 스타일·위치 초기화.
    function resetIntegratePopup(tip) {
      var popup = tip.querySelector(".integrate-tip-popup");
      if (!popup) return;
      popup.classList.remove("integrate-tip-floating");
      popup.style.removeProperty("left");
      popup.style.removeProperty("top");
      popup.style.removeProperty("display");
    }

    // 열린 통합 보기 popup 위치 재계산(resize/scroll).
    function refreshOpenIntegrateTips() {
      scope.querySelectorAll(".integrate-tip").forEach(function (tip) {
        if (tip.matches(":hover") || tip.matches(":focus-within")) {
          positionIntegratePopup(tip);
        }
      });
    }

    scope.querySelectorAll(".integrate-tip").forEach(function (tip) {
      tip.addEventListener("mouseenter", function () { positionIntegratePopup(tip); });
      tip.addEventListener("focusin", function () { positionIntegratePopup(tip); });
      tip.addEventListener("mouseleave", function (e) {
        var to = e.relatedTarget;
        if (to && tip.contains(to)) return;
        resetIntegratePopup(tip);
      });
      tip.addEventListener("focusout", function (e) {
        var to = e.relatedTarget;
        if (to && tip.contains(to)) return;
        resetIntegratePopup(tip);
      });
    });

    window.addEventListener("resize", refreshOpenIntegrateTips);
    window.addEventListener("scroll", refreshOpenIntegrateTips, true);
  }
  // --- 장중 등락률 API / live_quotes.js ---
  // html data-live-quotes-base 또는 기본 127.0.0.1:8765.
  function liveQuotesBase() {
    var html = document.documentElement;
    var base = html && html.getAttribute("data-live-quotes-base");
    return (base && String(base).trim()) || "http://127.0.0.1:8765";
  }
  // KST 연·월·일·시·분 (리포트 생성 시점과 무관하게 열람 시각 기준).
  function kstClock() {
    var parts = {};
    new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Seoul",
      year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", hour12: false
    }).formatToParts(new Date()).forEach(function (p) {
      if (p.type !== "literal") parts[p.type] = p.value;
    });
    return parts;
  }
  function kstTodayIso() {
    var p = kstClock();
    return p.year + "-" + p.month + "-" + p.day;
  }
  // 정규장 종가(15:30 KST) 전 — 당일 N 등락률을 실시간으로 붙일 구간.
  function kstIsBeforeRegularClose() {
    var p = kstClock();
    var hm = parseInt(p.hour, 10) * 60 + parseInt(p.minute, 10);
    return hm < 15 * 60 + 30;
  }
  // output/live_quotes.js 재로드(API 실패 시 폴백).
  function reloadLiveQuotesJs(cb) {
    var s = document.getElementById("money-live-quotes-js");
    if (!s) {
      s = document.createElement("script");
      s.id = "money-live-quotes-js";
      document.head.appendChild(s);
    }
    s.onload = function () { if (cb) cb(); };
    s.onerror = function () { if (cb) cb(); };
    s.src = "live_quotes.js?t=" + Date.now();
  }
  // live_quotes.js 의 window.__MONEY_LIVE_QUOTES__ 에서 종목 등락률 읽기.
  function readLiveQuoteFromJs(code) {
    var q = window.__MONEY_LIVE_QUOTES__;
    if (!q || q[code] == null) return null;
    var v = parseFloat(q[code]);
    return isNaN(v) ? null : v;
  }
  // --- 종목 차트 tooltip(N 마커·장중 N %) ---
  function bindStockChartTips(root) {
    var NAVER_DAY_CHART_BARS = 72;
    var NAVER_CHART_PLOT_LEFT = 0.075;
    var NAVER_CHART_PLOT_WIDTH = 0.85;
    var NAVER_CHART_MARKER_TOP = "4%";
    var NAVER_CHART_MARKER_BOTTOM = "16%";
    var NAVER_CHART_LABEL_TOP = "1%";
    // ISO 날짜 문자열 → Date(로컬).
    function parseYmd(s) {
      var p = String(s || "").split("-");
      if (p.length < 3) return null;
      var y = parseInt(p[0], 10);
      var m = parseInt(p[1], 10) - 1;
      var d = parseInt(p[2], 10);
      if (isNaN(y) || isNaN(m) || isNaN(d)) return null;
      return new Date(y, m, d);
    }
    // Date → ISO YYYY-MM-DD.
    function formatYmd(d) {
      var y = d.getFullYear();
      var m = String(d.getMonth() + 1).padStart(2, "0");
      var day = String(d.getDate()).padStart(2, "0");
      return y + "-" + m + "-" + day;
    }
    // KST 시·분·요일 등 Intl 파트.
    function kstParts(now) {
      var parts = {};
      new Intl.DateTimeFormat("en-GB", {
        timeZone: "Asia/Seoul",
        year: "numeric", month: "2-digit", day: "2-digit",
        hour: "2-digit", minute: "2-digit", hour12: false,
        weekday: "short"
      }).formatToParts(now || new Date()).forEach(function (p) {
        if (p.type !== "literal") parts[p.type] = p.value;
      });
      return parts;
    }
    // 직전 평일(주말 건너뜀).
    function prevWeekday(d) {
      var x = new Date(d.getTime());
      do {
        x.setDate(x.getDate() - 1);
      } while (x.getDay() === 0 || x.getDay() === 6);
      return x;
    }
    // 네이버 일봉 차트 우측 끝 거래일(KST·정규장 기준).
    function liveChartAxisEndYmdKst() {
      var p = kstParts();
      var d = parseYmd(p.year + "-" + p.month + "-" + p.day);
      if (!d) return null;
      var wd = p.weekday;
      var isWeekday = wd !== "Sat" && wd !== "Sun";
      var hour = parseInt(p.hour, 10);
      var minute = parseInt(p.minute, 10);
      if (isWeekday) {
        // 정규장(09:00~15:30): 네이버 일봉 차트 우측 = 당일 봉
        if (hour > 9 || (hour === 9 && minute >= 0)) {
          if (hour < 15 || (hour === 15 && minute < 30)) {
            return formatYmd(d);
          }
        }
        if (hour < 9) {
          d = prevWeekday(d);
        }
      } else {
        d = prevWeekday(d);
      }
      return formatYmd(d);
    }
    // 차트 프레임용 축 끝일(현재는 liveChartAxisEndYmdKst 와 동일).
    function chartAxisEndYmdKst(frame) {
      return liveChartAxisEndYmdKst();
    }
    // 캡션에 차트 반영 종료일(~MM/DD까지) 접미사 갱신.
    function updateChartCaption(frame) {
      if (!frame || !frame.parentElement) return;
      var cap = frame.parentElement.querySelector(".stock-chart-caption");
      if (!cap) return;
      var base = cap.getAttribute("data-caption-base");
      if (!base) {
        base = (cap.textContent || "").replace(/\s*\(~[^)]*\)\s*$/, "");
        cap.setAttribute("data-caption-base", base);
      }
      var live = liveChartAxisEndYmdKst();
      var suffix = "";
      if (live) {
        suffix = " (~" + live.slice(5, 7) + "/" + live.slice(8, 10) + "까지)";
      }
      cap.textContent = base + suffix;
    }
    // startYmd 다음 거래일부터 endYmd 까지 평일 수(N 봉 offset 계산용).
    function tradingSessionsAfterExclusive(startYmd, endYmd) {
      var start = parseYmd(startYmd);
      var end = parseYmd(endYmd);
      if (!start || !end || end <= start) return 0;
      var d = new Date(start.getTime());
      d.setDate(d.getDate() + 1);
      var n = 0;
      while (d <= end) {
        if (d.getDay() !== 0 && d.getDay() !== 6) n++;
        d.setDate(d.getDate() + 1);
      }
      return n;
    }
    // 차트 우측 끝 대비 N 봉 offset(0=최우측 봉).
    function resolveNBarOffset(frame) {
      var nDay = frame.getAttribute("data-n-day");
      if (!nDay) return null;
      var chartEnd = chartAxisEndYmdKst(frame);
      if (!chartEnd) return null;
      var nDt = parseYmd(nDay);
      var endDt = parseYmd(chartEnd);
      if (!nDt || !endDt) return null;
      if (endDt < nDt) return null;
      var live = tradingSessionsAfterExclusive(nDay, chartEnd);
      if (live !== null && live >= 0 && live < NAVER_DAY_CHART_BARS) {
        return live;
      }
      var attr = frame.getAttribute("data-n-offset");
      if (attr !== null && attr !== "") {
        var v = parseInt(attr, 10);
        if (!isNaN(v) && v >= 0 && v < NAVER_DAY_CHART_BARS) return v;
      }
      return null;
    }
    // N 봉 세로 마커·라벨 위치.
    function positionStockChartNMarker(frame) {
      if (!frame) return;
      var nDay = frame.getAttribute("data-n-day");
      var marker = frame.querySelector(".stock-chart-n-marker");
      var label = frame.querySelector(".stock-chart-n-label");
      if (!nDay || !marker) {
        frame.classList.remove("has-n-marker");
        return;
      }
      var offset = resolveNBarOffset(frame);
      if (offset === null || offset < 0 || offset >= NAVER_DAY_CHART_BARS) {
        frame.classList.remove("has-n-marker");
        return;
      }
      var fracFromRight = offset / Math.max(1, NAVER_DAY_CHART_BARS - 1);
      var xPct = (NAVER_CHART_PLOT_LEFT + NAVER_CHART_PLOT_WIDTH * (1 - fracFromRight)) * 100;
      marker.style.left = xPct + "%";
      marker.style.top = NAVER_CHART_MARKER_TOP;
      marker.style.bottom = NAVER_CHART_MARKER_BOTTOM;
      if (label) {
        label.style.left = xPct + "%";
        label.style.top = NAVER_CHART_LABEL_TOP;
        label.textContent = "N " + (nDay.length >= 10 ? nDay.slice(5, 7) + "/" + nDay.slice(8, 10) : "N");
      }
      frame.classList.add("has-n-marker");
    }
    // 종목 차트 popup 을 뷰포트 안 fixed 배치.
    function positionStockChartPopup(tip) {
      var popup = tip.querySelector(".stock-chart-popup");
      var anchor = tip.querySelector(".stock");
      if (!popup || !anchor) return;
      var margin = 10;
      var gap = 6;
      popup.classList.add("stock-chart-popup-floating");
      popup.style.setProperty("display", "block", "important");
      var pw = popup.offsetWidth;
      var ph = popup.offsetHeight;
      var ar = anchor.getBoundingClientRect();
      var vw = window.innerWidth;
      var vh = window.innerHeight;
      var left = ar.left;
      if (left + pw > vw - margin) left = Math.max(margin, vw - pw - margin);
      if (left < margin) left = margin;
      var top = ar.bottom + gap;
      if (top + ph > vh - margin) top = ar.top - ph - gap;
      if (top < margin) top = margin;
      popup.style.setProperty("left", left + "px", "important");
      popup.style.setProperty("top", top + "px", "important");
    }
    // tooltip 내 N(장중) 등락률 DOM 반영.
    function applyLiveIntradayPct(tip, pct) {
      var line = tip.querySelector(".stock-ret-line[data-live-intraday]");
      if (!line) return;
      var pctEl = line.querySelector(".stock-ret-pct");
      if (pct === null || pct === undefined || isNaN(pct)) {
        if (pctEl) pctEl.textContent = "—";
        line.classList.remove("ok", "bad");
        return;
      }
      if (pctEl) pctEl.textContent = Number(pct).toFixed(2) + "%";
      line.classList.remove("ok", "bad");
      line.classList.add(pct >= 0 ? "ok" : "bad");
    }
    // tooltip 호버 시 N 장중 등락률 API/JS 폴백 조회.
    function refreshLiveIntradayLine(tip) {
      var line = tip.querySelector(".stock-ret-line[data-live-intraday]");
      if (!line) return;
      var code = tip.getAttribute("data-stock-code");
      if (!code) return;
      var pctEl = line.querySelector(".stock-ret-pct");
      if (pctEl) pctEl.textContent = "…";
      line.classList.remove("ok", "bad");
      // 조회 완료 후에도 tooltip 이 열려 있을 때만 DOM 반영.
      function done(v) {
        if (!tip.matches(":hover") && !tip.matches(":focus-within")) return;
        applyLiveIntradayPct(tip, v);
      }
      var proxyUrl = liveQuotesBase().replace(/\/$/, "")
        + "/api/quotes?codes=" + encodeURIComponent(code);
      fetch(proxyUrl, { credentials: "omit", cache: "no-store" })
        .then(function (res) { return res.ok ? res.json() : null; })
        .then(function (obj) {
          var quotes = obj && obj.quotes;
          var v = quotes && quotes[code] != null ? parseFloat(quotes[code]) : NaN;
          if (!isNaN(v)) { done(v); return; }
          reloadLiveQuotesJs(function () { done(readLiveQuoteFromJs(code)); });
        })
        .catch(function () {
          reloadLiveQuotesJs(function () { done(readLiveQuoteFromJs(code)); });
        });
    }
    // 차트 popup floating 스타일 해제.
    function resetStockChartPopup(tip) {
      var popup = tip.querySelector(".stock-chart-popup");
      if (!popup) return;
      popup.classList.remove("stock-chart-popup-floating");
      popup.style.removeProperty("left");
      popup.style.removeProperty("top");
      popup.style.removeProperty("display");
    }
    // 열린 차트 popup 위치 재계산.
    function refreshOpenStockChartTips() {
      var s = root || document;
      s.querySelectorAll(".stock-chart-tip").forEach(function (tip) {
        if (tip.matches(":hover") || tip.matches(":focus-within")) {
          positionStockChartPopup(tip);
        }
      });
    }
    var scope = root || document;
    scope.querySelectorAll(".stock-chart-tip").forEach(function (tip) {
      var frame = tip.querySelector(".stock-chart-frame");
      var img = tip.querySelector("img.stock-chart-img");
      if (!img) return;
      var base = img.getAttribute("data-chart-base");
      if (!base) {
        var legacySrc = img.getAttribute("src");
        if (!legacySrc) return;
        base = legacySrc.split("?")[0];
        img.setAttribute("data-chart-base", base);
        img.removeAttribute("src");
      }
      // 호버 시 차트 이미지·캡션·N 마커·장중 % 동시 갱신.
      function refreshChart() {
        positionStockChartPopup(tip);
        refreshLiveIntradayLine(tip);
        var bust = Date.now() + "-" + Math.random().toString(36).slice(2, 10);
        var url = base + "?sidcode=" + bust;
        img.addEventListener("load", function onChartLoad() {
          img.removeEventListener("load", onChartLoad);
          updateChartCaption(frame);
          positionStockChartNMarker(frame);
          positionStockChartPopup(tip);
        });
        img.removeAttribute("src");
        img.src = url;
        updateChartCaption(frame);
        positionStockChartNMarker(frame);
      }
      tip.addEventListener("mouseenter", refreshChart);
      tip.addEventListener("focusin", refreshChart);
      tip.addEventListener("mouseleave", function (e) {
        var to = e.relatedTarget;
        if (to && tip.contains(to)) return;
        resetStockChartPopup(tip);
      });
      tip.addEventListener("focusout", function (e) {
        var to = e.relatedTarget;
        if (to && tip.contains(to)) return;
        resetStockChartPopup(tip);
      });
      window.addEventListener("resize", function () {
        if (tip.matches(":hover") || tip.matches(":focus-within")) {
          positionStockChartNMarker(frame);
          positionStockChartPopup(tip);
        }
      });
    });
    window.addEventListener("resize", refreshOpenStockChartTips);
    window.addEventListener("scroll", refreshOpenStockChartTips, true);
  }
  // --- 표 내 실시간 상승률(예측 옆·N일봉 열) ---
  function fetchLiveQuotesMap(codes, cb) {
    if (!codes.length) { cb({}); return; }
    var url = liveQuotesBase().replace(/\/$/, "")
      + "/api/quotes?codes=" + encodeURIComponent(codes.join(","));
    fetch(url, { credentials: "omit", cache: "no-store" })
      .then(function (res) { return res.ok ? res.json() : null; })
      .then(function (obj) {
        if (obj && obj.quotes) { cb(obj.quotes); return; }
        reloadLiveQuotesJs(function () {
          var q = window.__MONEY_LIVE_QUOTES__ || {};
          var out = {};
          codes.forEach(function (c) { if (q[c] != null) out[c] = q[c]; });
          cb(out);
        });
      })
      .catch(function () {
        reloadLiveQuotesJs(function () {
          var q = window.__MONEY_LIVE_QUOTES__ || {};
          var out = {};
          codes.forEach(function (c) { if (q[c] != null) out[c] = q[c]; });
          cb(out);
        });
      });
  }
  // N일봉 열 표시용 등락률 문자열(숫자와 % 사이 공백).
  function formatStockRetPct(pct) {
    return Number(pct).toFixed(2) + " %";
  }
  var livePctFlashTimers = new WeakMap();
  // 셀 텍스트에서 등락률 숫자 파싱(…/— 는 null).
  function parseLivePctText(text) {
    var s = String(text || "").replace(/%/g, "").trim();
    if (!s || s === "…" || s === "—") return null;
    var v = parseFloat(s);
    return isNaN(v) ? null : v;
  }
  // 실시간 % 변경 시 노란 밑줄 하이라이트(3초).
  function flashLivePctEl(el) {
    if (!el) return;
    el.classList.add("live-pct-flash");
    var prev = livePctFlashTimers.get(el);
    if (prev) clearTimeout(prev);
    livePctFlashTimers.set(el, setTimeout(function () {
      el.classList.remove("live-pct-flash");
      livePctFlashTimers.delete(el);
    }, 5000));
  }
  // 아직 유효한 등락률이 없어 로딩(…) 표시가 필요한지.
  function livePctNeedsLoading(el) {
    if (!el) return true;
    var t = (el.textContent || "").trim();
    return !t || t === "…" || t === "—";
  }
  // 예측 상승률 옆 실시간 % span 갱신·변경 하이라이트.
  function applyPredLiveIntradayPct(el, pct) {
    if (!el) return;
    if (pct === null || pct === undefined || isNaN(pct)) {
      if (!livePctNeedsLoading(el)) return;
      el.textContent = "—";
      el.classList.remove("ok", "bad");
      return;
    }
    var next = Number(pct).toFixed(2);
    if (el.textContent === next) return;
    var prev = (el.textContent || "").trim();
    var hadPrior = parseLivePctText(prev) !== null;
    el.textContent = next;
    el.classList.remove("ok", "bad");
    el.classList.add(pct >= 0 ? "ok" : "bad");
    if (hadPrior && prev !== next) flashLivePctEl(el);
  }
  // 실시간 상승률 토글 버튼이 속한 일자 section 찾기.
  function dayRootForLiveToggle(btn) {
    var scopeId = btn.getAttribute("data-live-scope");
    if (scopeId) {
      var byId = document.getElementById(scopeId);
      if (byId) return byId;
    }
    var ref = btn.closest(".market-theme-ref");
    if (ref) {
      var sec = ref.closest("section[id^='day-']");
      if (sec) return sec;
    }
    return btn.closest(".day-market-block, .day-stack, section.day-forward-obs");
  }
  // N일봉(%) 열 N(당일) 실시간 % 갱신·변경 하이라이트.
  function applyStockRetIntradayPct(el, pct) {
    if (!el) return;
    var line = el.closest(".stock-ret-line");
    if (pct === null || pct === undefined || isNaN(pct)) {
      if (!livePctNeedsLoading(el)) return;
      el.textContent = "…";
      if (line) line.classList.remove("ok", "bad");
      return;
    }
    var next = formatStockRetPct(pct);
    var cls = pct >= 0 ? "ok" : "bad";
    if (el.textContent === next && line && line.classList.contains(cls)) return;
    var prev = (el.textContent || "").trim();
    var hadPrior = parseLivePctText(prev) !== null;
    el.textContent = next;
    if (line) {
      line.classList.remove("ok", "bad");
      line.classList.add(cls);
    }
    if (hadPrior && prev !== next) flashLivePctEl(el);
  }
  // 실시간 갱신 대상 요소에서 6자리 종목코드 추출.
  function stockCodeForLiveEl(el) {
    if (!el) return "";
    var c = el.getAttribute("data-stock-code");
    if (c) return c;
    var host = el.closest("[data-stock-code]");
    return host ? host.getAttribute("data-stock-code") || "" : "";
  }
  // 당일 N 행 라벨 — ``0828 (N  )`` 형식.
  function isNDayRetLine(line) {
    var lbl = line && line.querySelector(".stock-ret-lbl");
    if (!lbl) return false;
    return /^\d{4} \(N\s{0,2}\)$/.test((lbl.textContent || "").trim());
  }
  // 어제 생성된 예측 전용 HTML 에도, 오늘 장중이면 N 행에 실시간 훅을 붙입니다.
  function hydrateTodayLiveIntraday(root) {
    if (!kstIsBeforeRegularClose()) return null;
    var today = kstTodayIso();
    var scope = root || document;
    var sec = null;
    if (scope.id === "day-" + today) sec = scope;
    else if (scope.querySelector) sec = scope.querySelector("#day-" + today);
    if (!sec) return null;
    sec.querySelectorAll(".stock-ret-col-lines .stock-ret-line, .stock-ret-lines .stock-ret-line").forEach(function (line) {
      if (!isNDayRetLine(line)) return;
      var tr = line.closest("tr");
      var code = stockCodeForLiveEl(line);
      if (!code && tr) {
        var host = tr.querySelector("[data-stock-code]");
        code = host ? host.getAttribute("data-stock-code") || "" : "";
      }
      if (!code) return;
      line.setAttribute("data-live-intraday", "1");
      line.setAttribute("data-stock-code", code);
      var pctEl = line.querySelector(".stock-ret-pct");
      if (pctEl) {
        var t = (pctEl.textContent || "").trim();
        if (!t || t === "—" || t === "…") pctEl.textContent = "…";
      }
    });
    return sec;
  }
  // 일자 블록 내 예측 옆·N일봉 실시간 % 일괄 조회(showLoading 시 … 표시).
  function refreshPredLiveIntradayInScope(dayRoot, options) {
    options = options || {};
    var showLoading = !!options.showLoading;
    if (!dayRoot) return;
    var predSpans = dayRoot.querySelectorAll(".pred-live-intraday-pct");
    var retPcts = dayRoot.querySelectorAll(".stock-ret-line[data-live-intraday] .stock-ret-pct");
    var codes = [];
    predSpans.forEach(function (el) {
      var c = stockCodeForLiveEl(el);
      if (c) codes.push(c);
      if (showLoading || livePctNeedsLoading(el)) el.textContent = "…";
    });
    retPcts.forEach(function (el) {
      var c = stockCodeForLiveEl(el);
      if (c) codes.push(c);
      if (showLoading || livePctNeedsLoading(el)) el.textContent = "…";
    });
    codes = codes.filter(function (c, i, a) { return a.indexOf(c) === i; });
    fetchLiveQuotesMap(codes, function (quotes) {
      predSpans.forEach(function (el) {
        var c = stockCodeForLiveEl(el);
        var v = c && quotes[c] != null ? parseFloat(quotes[c]) : NaN;
        applyPredLiveIntradayPct(el, isNaN(v) ? null : v);
      });
      retPcts.forEach(function (el) {
        var c = stockCodeForLiveEl(el);
        var v = c && quotes[c] != null ? parseFloat(quotes[c]) : NaN;
        applyStockRetIntradayPct(el, isNaN(v) ? null : v);
      });
    });
  }
  var columnIntradayTimers = {};
  // 예측 전용 일자: N일봉 열 장중 % 자동 폴링(10초).
  function bindForwardColumnIntradayRefresh(root) {
    var sc = root || document;
    hydrateTodayLiveIntraday(sc);
    sc.querySelectorAll("section[id^='day-']").forEach(function (sec) {
      var todayLive = sec.id === "day-" + kstTodayIso() && kstIsBeforeRegularClose();
      if (!todayLive && !sec.querySelector(".stock-ret-col-lines [data-live-intraday]")) return;
      var tid = sec.id || "day";
      refreshPredLiveIntradayInScope(sec, { showLoading: true });
      if (columnIntradayTimers[tid]) return;
      columnIntradayTimers[tid] = setInterval(function () {
        refreshPredLiveIntradayInScope(sec);
      }, 10000);
    });
  }
  var predLiveTimers = {};
  // 「실시간 상승률」 토글·예측 열 옆 % 폴링.
  function bindLiveIntradayToggles(root) {
    var sc = root || document;
    sc.querySelectorAll(".live-intraday-toggle").forEach(function (btn) {
      if (btn.getAttribute("data-live-bound")) return;
      btn.setAttribute("data-live-bound", "1");
      btn.addEventListener("click", function () {
        var on = btn.getAttribute("aria-pressed") !== "true";
        btn.setAttribute("aria-pressed", on ? "true" : "false");
        btn.classList.toggle("active", on);
        btn.textContent = on ? "실시간 상승률 숨기기" : "실시간 상승률";
        var dayRoot = dayRootForLiveToggle(btn);
        if (!dayRoot) return;
        dayRoot.querySelectorAll(".pred-live-intraday").forEach(function (span) {
          span.hidden = !on;
          if (on) span.removeAttribute("aria-hidden");
          else span.setAttribute("aria-hidden", "true");
        });
        var tid = dayRoot.id || btn.getAttribute("data-live-scope") || "day";
        if (on) {
          refreshPredLiveIntradayInScope(dayRoot, { showLoading: true });
          if (predLiveTimers[tid]) clearInterval(predLiveTimers[tid]);
          predLiveTimers[tid] = setInterval(function () {
            if (btn.getAttribute("aria-pressed") !== "true") return;
            refreshPredLiveIntradayInScope(dayRoot);
          }, 10000);
        } else if (predLiveTimers[tid]) {
          clearInterval(predLiveTimers[tid]);
          delete predLiveTimers[tid];
        }
      });
    });
  }
  document.querySelectorAll("table.rows-compare").forEach(bindSortTable);
  bindMarketRowFilters(document);
  bindIntegrateTips(document);
  bindStockChartTips(document);
  bindLiveIntradayToggles(document);
  bindForwardColumnIntradayRefresh(document);
})();
</script>"""

_ACTUAL_RET_FMT_MACROS = r"""{% macro fmt_ret_ratio_pct(ratio) -%}
{% if ratio < 0 %}<span class="bad">{{ "%.2f"|format(ratio * 100) }}</span>{% else %}{{ "%.2f"|format(ratio * 100) }}{% endif %}
{%- endmacro %}
{% macro fmt_ret_pct_pts(pct) -%}
{% if pct < 0 %}<span class="bad">{{ "%.2f"|format(pct) }}</span>{% else %}{{ "%.2f"|format(pct) }}{% endif %}
{%- endmacro %}"""

_ACTUAL_RET_CELL_BODY = r"""{% if day_forward | default(false) %}{{ format_forward_actual_ret_cell(r) | safe }}{% elif r.actual_cell_pre_close_snapshot | default(false) %}{% if r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ fmt_ret_pct_pts(r.actual_ret_intraday_pct) }}%){% elif r.actual_ret is not none %}— ({{ fmt_ret_ratio_pct(r.actual_ret) }}%){% else %}—{% endif %}{% elif r.actual_ret is not none %}{{ fmt_ret_ratio_pct(r.actual_ret) }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}— ({{ fmt_ret_pct_pts(r.actual_ret_intraday_pct) }}%){% else %}—{% endif %}"""


def _actual_ret_cell_macro(name: str) -> str:
    return (
        _ACTUAL_RET_FMT_MACROS
        + f"{{% macro {name}(r, day_forward=false) -%}}{_ACTUAL_RET_CELL_BODY}{{%- endmacro %}}\n"
    )


_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko" data-live-quotes-base="http://127.0.0.1:8765">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root {
      --bg: #0f1419;
      --card: #1a2332;
      --text: #e7ecf3;
      --muted: #8b9cb3;
      --accent: #3d9cf5;
      --ok: #3ecf8e;
      --bad: #f07178;
      --warn: #e6c07b;
    }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.55; }
    h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
    .sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 1.5rem; }
    section { background: var(--card); border-radius: 12px; padding: 20px; margin-bottom: 20px;
              border: 1px solid #243044; }
    h2 { font-size: 1.15rem; margin: 0 0 12px 0; color: var(--accent); }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    th, td { padding: 10px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    tr:hover td { background: #1e2a3d; }
    a.stock { color: var(--accent); text-decoration: none; font-weight: 600; }
    a.stock:hover { text-decoration: underline; }
    mark { background: #e6c07b; color: #1a1a1a; padding: 0 2px; border-radius: 2px; }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 0.75rem;
            margin: 2px 4px 2px 0; background: #243044; color: var(--muted); }
    .ok { color: var(--ok); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .reasons { font-size: 0.82rem; color: var(--muted); }
    .fn-block { margin-top: 14px; padding: 12px; background: #131b28; border-radius: 8px;
                border-left: 3px solid var(--bad); }
    .gap-tip { position: relative; display: inline-block; margin-top: 6px; max-width: 100%; }
    .gap-tip.gap-tip-inline { margin-top: 0; margin-left: 10px; vertical-align: baseline; }
    .gap-tip.gap-tip-end .gap-tip-popup { left: auto; right: 0; }
    .gap-tip-trigger {
      cursor: help; border-bottom: 1px dotted var(--accent); color: var(--accent);
      font-size: 0.78rem; font-weight: 600; outline: none;
    }
    .gap-tip-trigger:hover, .gap-tip-trigger:focus { color: #7ec4ff; }
    .gap-tip-popup {
      display: none; position: absolute; z-index: 500; left: 0; top: calc(100% + 6px);
      min-width: 380px; max-width: min(920px, 96vw); max-height: 92vh;
      overflow: auto; padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e;
      border-radius: 8px; box-shadow: 0 8px 24px rgba(0,0,0,0.45);
      font-size: 0.82rem; line-height: 1.55; color: #d0dce8; text-align: left;
    }
    .gap-tip-popup p { margin: 0 0 6px 0; }
    .gap-tip-popup ul { margin: 4px 0 0 16px; padding: 0; }
    .gap-tip-popup li { margin-bottom: 4px; }
    .gap-tip:not(.integrate-tip):hover .gap-tip-popup,
    .gap-tip:not(.integrate-tip):focus-within .gap-tip-popup { display: block; }
    .gap-tip.integrate-tip:hover .gap-tip-trigger,
    .gap-tip.integrate-tip:focus-within .gap-tip-trigger { color: #7ec4ff; border-bottom-color: #7ec4ff; }
    .gap-tip.integrate-tip:hover .integrate-tip-popup,
    .gap-tip.integrate-tip:focus-within .integrate-tip-popup { display: block !important; }
    .kw-count-tip { display: inline; vertical-align: baseline; margin: 0 1px; }
    .kw-count-tip .gap-tip-trigger { font-size: inherit; font-weight: 700; color: var(--warn); border-bottom-color: var(--warn); }
    .gap-tip.pred-miss-tip .gap-tip-trigger { color: var(--bad); border-bottom-color: rgba(248,113,113,0.85); }
    .gap-tip.pred-miss-tip .gap-tip-trigger:hover,
    .gap-tip.pred-miss-tip .gap-tip-trigger:focus { color: #ffa8a8; border-bottom-color: #ffa8a8; }
    .kw-list-popup { min-width: 280px; max-width: min(520px, 92vw) !important; width: auto !important; max-height: 70vh; z-index: 4100; }
    .integrate-tip-floating .kw-list-popup { z-index: 4100; }
    .integrate-tip-popup.integrate-tip-floating {
      display: block !important;
      position: fixed !important;
      z-index: 4000 !important;
      transform: none !important;
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      max-height: 90vh !important;
      overflow: auto !important;
      box-sizing: border-box;
      padding: 14px 16px !important;
      background: #1a2838;
      border: 1px solid #3d6a9e;
      border-radius: 8px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.5);
    }
    .pred-reason-plain { font-size: 0.82rem; color: var(--muted); line-height: 1.45; max-width: 28em; display: inline-block; vertical-align: middle; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
    .combo-tip { margin-left: 8px; vertical-align: middle; white-space: nowrap; }
    .combo-tip-popup {
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      left: 0 !important;
      right: auto !important;
      transform: none !important;
      top: calc(100% + 8px) !important;
      box-sizing: border-box;
      max-height: 90vh;
      overflow: auto;
      padding: 14px 16px !important;
    }
    .combo-tip-inner {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0 20px;
      align-items: start;
      width: 100%;
    }
    .combo-tip-col { min-width: 0; border-left: 1px solid #2d4a6a; padding-left: 16px; }
    .combo-tip-col:first-child { border-left: none; padding-left: 0; padding-right: 4px; }
    .combo-tip-h {
      margin: 0 0 8px 0;
      font-size: 0.78rem;
      font-weight: 700;
      color: #8ec5f6;
      text-transform: none;
    }
    .combo-tip-body {
      font-size: 0.82rem; line-height: 1.6; color: #d0dce8;
      overflow-wrap: anywhere; word-wrap: break-word; word-break: break-word;
      white-space: normal;
    }
    .combo-tip-body p { margin: 0 0 6px 0; }
    .combo-tip-empty { margin: 0; color: var(--muted); font-style: italic; font-size: 0.82rem; }
    .disclosure-tip-popup { min-width: 300px; max-width: min(560px, 92vw) !important; width: auto !important; }
    .disclosure-tip-list { margin: 6px 0 0; padding-left: 0; list-style: none; }
    .disclosure-tip-list li { margin-bottom: 8px; line-height: 1.45; }
    .disc-kind { font-size: 0.75rem; color: #9fd3ff; margin-right: 4px; }
    .combo-tip-rise {
      grid-column: 1 / -1;
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid #2d4a6a;
      width: 100%;
    }
    ul.nl { margin: 4px 0 0 0; padding-left: 18px; }
    ul.nl li { margin-bottom: 6px; }
    @media (max-width: 760px) {
      .combo-tip-inner { grid-template-columns: 1fr; }
      .combo-tip-col { border-left: none; padding-left: 0; border-top: 1px solid #2d4a6a; padding-top: 12px; margin-top: 10px; }
      .combo-tip-col:first-child { border-top: none; padding-top: 0; margin-top: 0; }
    }
    .kw-pills { display: inline-flex; flex-wrap: wrap; gap: 4px 4px; align-items: center; vertical-align: middle; }
    .kw-pills .pill { font-size: 0.68rem; padding: 1px 6px; }
    ul.news { margin: 0; padding-left: 18px; color: var(--muted); font-size: 0.85rem; }
    .day-heading-row { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-start; gap: 8px 14px; margin-bottom: 6px; }
    .day-heading-row h2 { margin: 0; }
    .market-filter-radios, .rise-filter-radios { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; font-size: 0.82rem; color: var(--muted); }
    .market-filter-title, .rise-filter-title { font-weight: 600; color: var(--muted); margin-right: 2px; }
    .market-filter-label, .rise-filter-label { cursor: pointer; display: inline-flex; align-items: center; gap: 5px; margin: 0; font-weight: 500; }
    .market-filter-label input, .rise-filter-label input { accent-color: var(--accent); vertical-align: middle; }
    .tab-bar { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
    .tab-btn { font: inherit; cursor: pointer; padding: 8px 14px; border-radius: 8px; border: 1px solid #2a3548;
              background: #131b28; color: var(--muted); }
    .tab-btn.active { background: var(--accent); color: #0f1419; border-color: var(--accent); font-weight: 600; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    .tabs-wrap section { margin-bottom: 16px; }
    table.rows-compare { width: 100%; border-collapse: collapse; font-size: 0.88rem; table-layout: fixed; }
    table.rows-compare col.col-group,
    table.rows-compare th.col-group,
    table.rows-compare td.col-group,
    table.rows-compare th:nth-child(1),
    table.rows-compare td:nth-child(1) {
      width: 5.75rem; min-width: 5.75rem; max-width: 5.75rem;
    }
    table.rows-compare td.col-group,
    table.rows-compare td:nth-child(1) { white-space: nowrap; vertical-align: top; }
    table.rows-compare th, table.rows-compare td { padding: 10px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top; }
    table.rows-compare th.sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th.sortable-col:hover { text-decoration: underline; }
    table.rows-compare th.sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th.sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th .sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th .sortable-col:hover { text-decoration: underline; }
    table.rows-compare th .sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th .sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    td.forward-actual-ret { white-space: nowrap; }
    .forward-ret-chain { white-space: nowrap; }
    .live-intraday-toggle { cursor: pointer; border: 1px solid #3d6a9e; background: #1a2838; color: var(--accent); font-size: 0.78rem; }
    .live-intraday-toggle:hover { border-color: var(--accent); }
    .live-intraday-toggle.active { background: var(--accent); color: #0f1419; border-color: var(--accent); }
    .pred-live-intraday { margin-left: 2px; font-size: 0.92em; color: var(--muted); white-space: nowrap; }
    .pred-live-intraday-pct.ok { color: var(--ok); font-weight: 600; }
    .pred-live-intraday-pct.bad { color: var(--bad); font-weight: 600; }
    .cumulative-accuracy-td { position: relative; }
    .cumulative-accuracy-td .cumulative-sort-keys { position: absolute; left: -9999px; top: 0; width: 1px; height: 1px; overflow: hidden; }
    .gap-tip.cumulative-hist-tip { margin-top: 0; vertical-align: middle; }
    .gap-tip.cumulative-hist-tip .gap-tip-trigger {
      font-size: inherit;
      font-variant-numeric: tabular-nums;
    }
    .gap-tip.cumulative-hist-tip .gap-tip-popup.cumulative-hist-popup {
      min-width: 280px;
      max-width: min(440px, 94vw);
      z-index: 600;
    }
    .stock-chart-tip { position: relative; display: inline-block; vertical-align: baseline; max-width: 100%; }
    .stock-chart-tip .stock { position: relative; z-index: 1; }
    .stock-chart-popup {
      display: none; position: absolute; left: 0; top: calc(100% + 6px); z-index: 850;
      padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e; border-radius: 10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.55);
      min-width: min(720px, calc(100vw - 32px)); max-width: min(720px, calc(100vw - 24px));
      box-sizing: border-box;
    }
    .stock-chart-tip:hover .stock-chart-popup,
    .stock-chart-tip:focus-within .stock-chart-popup { display: block; }
    .stock-chart-frame { position: relative; display: block; width: 100%; max-width: 700px; }
    .stock-chart-frame .stock-chart-img {
      display: block; width: 100%; max-width: 700px; height: auto;
      background: #0f1419; border-radius: 6px;
    }
    .stock-chart-n-marker {
      display: none; position: absolute; width: 2px; margin-left: -1px;
      background: #e6c07b; box-shadow: 0 0 8px rgba(230, 192, 123, 0.9);
      pointer-events: none; z-index: 2;
    }
    .stock-chart-n-label {
      display: none; position: absolute; transform: translate(-50%, 0);
      font-size: 0.66rem; font-weight: 700; line-height: 1.2;
      color: #1a1a1a; background: #e6c07b; padding: 1px 5px; border-radius: 3px;
      pointer-events: none; z-index: 3; white-space: nowrap;
    }
    .stock-chart-frame.has-n-marker .stock-chart-n-marker,
    .stock-chart-frame.has-n-marker .stock-chart-n-label { display: block; }
    .stock-chart-caption { display: block; margin-top: 8px; font-size: 0.74rem; color: var(--muted); text-align: center; line-height: 1.4; }
    .stock-ret-lines {
      display: flex; flex-direction: row; flex-wrap: wrap; gap: 0 1.25em; justify-content: center; align-items: center;
      margin-bottom: 10px; padding: 8px 10px; background: #151c24; border-radius: 6px;
      font-size: 0.78rem; font-variant-numeric: tabular-nums; line-height: 1.45;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .stock-ret-line { white-space: nowrap; }
    .stock-ret-lbl { color: var(--text); font-weight: 600; }
    .stock-ret-line.ok .stock-ret-pct { color: var(--ok); }
    .stock-ret-line.bad .stock-ret-pct { color: var(--bad); }
    .stock-ret-dt { color: var(--muted); }
    .stock-ret-col-lines {
      display: flex; flex-direction: column; gap: 3px;
      font-size: 0.74rem; font-variant-numeric: tabular-nums; line-height: 1.35;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      min-width: 8.5rem;
    }
    .stock-ret-col-lines .stock-ret-line { display: flex; justify-content: space-between; gap: 10px; }
    .pred-reason-inline { font-size: 0.82rem; line-height: 1.45; white-space: normal; max-width: 28rem; }
  </style>
</head>
<body>
{% macro stock_name_link(code, name, r=none) -%}
<span class="stock-chart-tip" tabindex="0"{% if code %} data-stock-code="{{ (code|string).zfill(6) }}"{% endif %}>
  <a class="stock" target="_blank" rel="noopener" href="{{ naver_chart_url(code) }}"{# title="클릭: 네이버 차트 · 호버: 일봉·최근 등락률" #}>{{ name }}</a>
  <span class="stock-chart-popup" role="tooltip">
    {% if r and not (r.forward_observation | default(false)) %}{{ format_stock_ret_tooltip_lines(r) | safe }}{% endif %}
    {% set chart_n_day = stock_chart_n_day_iso(r) if r else "" %}
    {% set chart_n_off = stock_chart_n_bar_offset(r) if r else none %}
    <div class="stock-chart-frame"{% if chart_n_day %} data-n-day="{{ chart_n_day }}"{% endif %}{% if chart_n_off is not none %} data-n-offset="{{ chart_n_off }}"{% endif %}>
      <img class="stock-chart-img" data-chart-base="{{ naver_chart_day_img_url(code) }}" alt="{{ name }} 일봉 캔들 차트" width="700" height="289" decoding="async" referrerpolicy="no-referrer-when-downgrade"/>
      <span class="stock-chart-n-marker" aria-hidden="true"></span>
      <span class="stock-chart-n-label" aria-hidden="true">N</span>
    </div>
    <span class="stock-chart-caption">일봉 캔들 · 호버 시 최신{% if chart_n_day %} · 노란 세로선=N({{ chart_n_day[5:7] }}/{{ chart_n_day[8:10] }}){% endif %}</span>
  </span>
</span>
{%- endmacro %}
__ACTUAL_RET_CELL_MACRO__
{% macro cumulative_accuracy_td(r, meta) -%}
<td class="cumulative-accuracy-td" style="white-space:nowrap;font-variant-numeric:tabular-nums">
  {% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
  <span class="cumulative-sort-keys" aria-hidden="true">
    <span data-sort-col="cumulative_a" data-sort-value="{{ r.cumulative_accuracy_avg }}"></span>
    {% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %}
    <span data-sort-col="cumulative_b" data-sort-value="{{ r.cumulative_nonneg_rate_pct }}"></span>
    {% endif %}
  </span>
  <span class="gap-tip cumulative-hist-tip">
    <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="누적 정확도·{{ meta.threshold }} 이상 예측 이력">{{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%{% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %} vs {{ "%.2f"|format(r.cumulative_nonneg_rate_pct) }}%{% endif %}{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} ({{ r.cumulative_hit_x }} {% if r.cumulative_hit_z is defined and r.cumulative_hit_z is not none %}{{ r.cumulative_hit_z }}{% else %}0{% endif %} <span class="bad">{% if r.cumulative_hit_neg is defined and r.cumulative_hit_neg is not none %}{{ r.cumulative_hit_neg }}{% else %}0{% endif %}</span> / {{ r.cumulative_hit_y }}){% endif %}</span>
    <div class="gap-tip-popup cumulative-hist-popup" role="tooltip">
      <div class="combo-tip-body">
        <strong>관측일 T별 · 예측 ≥ {{ meta.threshold }}</strong>
        {% if r.pred_high_history|default([])|length > 0 %}
        <ul style="margin:8px 0 0 0;padding-left:18px">
        {% for h in r.pred_high_history %}
          <li><span class="pill">{{ h.t }}</span> 예측 {{ "%.2f"|format(h.pred_pct) }}%
            {% if h.actual_pct is not none %} · 실제 {{ "%.2f"|format(h.actual_pct) }}%{% else %} · 실적 미확정{% endif %}
          </li>
        {% endfor %}
        </ul>
        {% else %}
        <p class="combo-tip-empty" style="margin:8px 0 0 0">저장된 {{ meta.threshold }} 이상 예측 이력이 없습니다.</p>
        {% endif %}
        <p style="font-size:0.76rem;color:var(--muted);margin:8px 0 0 0;line-height:1.35">앞 {{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%: 예측≥{{ meta.threshold }}·실적 확정 건 중 실제≥{{ meta.threshold }} 적중 비율(맞춘 건수÷전체, a/d와 동일).{% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %} vs {{ "%.2f"|format(r.cumulative_nonneg_rate_pct) }}%: 예측≥{{ meta.threshold }}·실적 확정 건 중 실제가 0% 이상인 비율.{% endif %}{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} 괄호 (a b c / d): 예측≥{{ meta.threshold }}·실적 확정 건 중 a=실제≥{{ meta.threshold }}, b=0&lt;실제&lt;{{ meta.threshold }}, c=실제&lt;0(빨간색), d=전체.{% endif %}</p>
      </div>
    </div>
  </span>
  {% else %}—{% endif %}
</td>
{%- endmacro %}
{% macro market_filter_radios(suffix, forward_day=false) -%}
<div class="market-filter-radios" role="radiogroup" aria-label="표 시장 구분">
  <span class="market-filter-title">시장 -</span>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="all" checked="checked"/> 전체</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kospi"/> KOSPI</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kosdaq"/> KOSDAQ</label>
</div>
<div class="rise-filter-radios" role="radiogroup" aria-label="상승률 구간">
  <span class="rise-filter-title">상승률 -</span>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="all" checked="checked"/> 예측후보 전체</label>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="high"/> 20%이상</label>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="mid"/> 10%~20%</label>
</div>
{%- endmacro %}
{% macro prediction_signal_cell(r) -%}
{{ format_prediction_signal_cell(r) | safe }}
{%- endmacro %}
{% macro disclosure_tip(r, trading_day=none) -%}
<span class="gap-tip combo-tip disclosure-tip{% if trading_day is not none %} gap-tip-end{% endif %}">
  <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="당일 종목 공시 목록 보기">공시</span>
  <div class="gap-tip-popup combo-tip-popup disclosure-tip-popup" role="tooltip">
    <h4 class="combo-tip-h">{% if trading_day is not none %}종목 공시 · {{ trading_day.isoformat() }}{% else %}종목 공시{% endif %}</h4>
    <ul class="nl disclosure-tip-list">
    {% for h in r.disclosure_hits|default([]) %}
      <li>
        {% if h.day %}<span class="pill">{{ h.day }}</span>{% endif %}
        <code class="disc-kind">{{ h.kind }}</code>
        {% if h.link %}
        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
        {% else %}
        {{ h.title }}
        {% endif %}
      </li>
    {% else %}
      <li class="muted">{% if trading_day is not none %}이 거래일({{ trading_day.isoformat() }})에 등록된 공시가 없습니다.{% else %}공시 목록이 없습니다.{% endif %}</li>
    {% endfor %}
    </ul>
  </div>
</span>
{%- endmacro %}
{% macro pred_ret_cell(d, r) -%}
{% if r.pred_ret is not none %}{{ "%.2f"|format(r.pred_ret) }}{% else %}—{% endif %}{% if d.forward_observation | default(false) %}<span class="pred-live-intraday" hidden aria-hidden="true"> · <span class="pred-live-intraday-pct" data-stock-code="{{ (r.code|string).zfill(6) }}">…</span>%</span>{% endif %}
{%- endmacro %}
{% macro market_theme_panel(d) -%}
{% if d.market_theme_html or (d.forward_observation | default(false)) %}
<div class="market-theme-ref" style="margin:12px 0 16px;padding:12px 14px;background:#152232;border:1px solid #2a4a6a;border-radius:8px">
  <div class="market-theme-heading-row" style="display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin:0 0 8px">
    <h3 style="font-size:0.95rem;color:var(--ok);margin:0">당일 테마 요약</h3>
    {% if d.forward_observation | default(false) %}
    <button type="button" class="pill live-intraday-toggle" data-live-scope="day-{{ d.trading_day.isoformat() }}" aria-pressed="false" title="클릭 시 예측 상승률 옆에 장중 실시간 등락률을 표시합니다">실시간 상승률</button>
    {% endif %}
  </div>
  {% if d.market_theme_html %}{{ d.market_theme_html | safe }}{% endif %}
</div>
{% endif %}
{%- endmacro %}
{% macro forward_pred_rationale_panel(d, meta) -%}
{# 장 마감 전 관측일: 예측 근거는 표 ``예측 근거`` 열에 전문 표시 #}
{%- endmacro %}
{% macro stock_ret_chain_cell(d, r) -%}
{% if d.forward_observation | default(false) or d.show_stock_ret_column | default(false) %}
<td class="stock-ret-chain-col" style="vertical-align:top">{{ format_stock_ret_column_lines(r) | safe }}</td>
{% endif %}
{%- endmacro %}
{% macro pred_rationale_cell(d, r) -%}
<td class="pred-reason-forward" style="vertical-align:top;{% if d.forward_observation | default(false) %}white-space:normal;max-width:28rem;line-height:1.45{% else %}white-space:nowrap{% endif %}">
  {% if r.pred_ret is not none %}
    {% set pred_reason_body = r.pred_reason_tooltip_html or r.pred_reason_detail_html %}
    {% if d.forward_observation | default(false) %}
      {% if pred_reason_body and pred_reason_body != '—' %}
      <div class="pred-reason-inline">{{ pred_reason_body | safe }}</div>
      {% else %}—{% endif %}
    {% else %}
      {% if pred_reason_body and pred_reason_body != '—' %}
      <span class="gap-tip pred-reason-tip">
        <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 근거">근거</span>
        <div class="gap-tip-popup pred-reason-popup" role="tooltip">
          <div class="combo-tip-body">{{ pred_reason_body | safe }}</div>
        </div>
      </span>
      {% else %}—{% endif %}
      {% if r.pred_miss_tooltip_html %}
      <span class="gap-tip pred-miss-tip" style="margin-left:8px">
        <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="틀린 이유">틀린 이유</span>
        <div class="gap-tip-popup pred-miss-popup" role="tooltip">
          <div class="combo-tip-body">{{ r.pred_miss_tooltip_html | safe }}</div>
        </div>
      </span>
      {% endif %}
    {% endif %}
  {% else %}—{% endif %}
</td>
{%- endmacro %}
{% macro hit_at_k_panel(d, meta) -%}
{% if d.hit_at_k_metrics and not (d.forward_observation | default(false)) %}
<div class="hit-at-k-panel" style="margin:8px 0 14px;padding:10px 12px;background:#1a2433;border:1px solid #334;border-radius:8px;font-size:0.88rem">
  <strong style="color:var(--ok)">순위 평가 Hit@K</strong>
  <span class="sub" style="margin-left:8px">실제 {{ meta.threshold }} {{ d.hit_at_k_metrics.actual_big_count }}종 · 무작위 대비 lift</span>
  <div style="margin-top:6px;display:flex;flex-wrap:wrap;gap:8px">
    {% for k in [5,10,20,40] %}
    {% if d.hit_at_k_metrics['hit_at_' ~ k] is defined %}
    <span class="pill">@{{ k }}: {{ d.hit_at_k_metrics['hit_at_' ~ k] }}/{{ k }}
      {% if d.hit_at_k_metrics['lift_at_' ~ k] is not none %} (×{{ "%.1f"|format(d.hit_at_k_metrics['lift_at_' ~ k]) }}){% endif %}
    </span>
    {% endif %}
    {% endfor %}
  </div>
</div>
{% endif %}
{%- endmacro %}
{% macro day_pred_accuracy_banner(d, meta) -%}
{% set s = d.pred_accuracy_summary | default(none) %}
{% if s and (s.n_pred_high | default(0)) > 0 and (s.n_with_actual | default(0)) > 0 %}
<p class="sub" style="margin:6px 0 10px;padding:8px 10px;background:#1a2230;border:1px solid #3a4a5c;border-radius:6px;font-size:0.88rem;line-height:1.45">
  <strong>당일 예측≥{{ meta.threshold }}</strong> {{ s.n_pred_high }}건 · 실적 확정 {{ s.n_with_actual }}건 ·
  임계 적중 <strong class="{% if (s.n_hit_threshold | default(0)) == 0 %}bad{% else %}ok{% endif %}">{{ s.n_hit_threshold }}</strong>/{{ s.n_with_actual }}
  {% if s.mean_accuracy_ratio is not none %}
  · 당일 달성률 <strong class="{% if s.mean_accuracy_ratio < 0.01 %}bad{% endif %}">{{ "%.2f"|format(s.mean_accuracy_ratio * 100) }}%</strong>
  {% endif %}
  {% if s.n_negative | default(0) > 0 %} · <span class="bad">음수 {{ s.n_negative }}건</span>{% endif %}
</p>
{% endif %}
{%- endmacro %}
{% macro day_panel(d, meta) -%}
  <section id="day-{{ d.trading_day.isoformat() }}" class="day-market-block">
    <div class="day-heading-row">
      <h2>{{ d.trading_day.isoformat() }} (거래일)</h2>
      {{ market_filter_radios(d.trading_day.isoformat(), d.forward_observation | default(false)) }}
    </div>
    {{ day_pred_accuracy_banner(d, meta) }}
    {{ forward_pred_rationale_panel(d, meta) }}
    {{ hit_at_k_panel(d, meta) }}
    <p class="sub">{% if meta.use_decision_cutoff %}N-1 거래일 {{ meta.cutoff_kst }}(KST)까지 반영한 {% endif %}예측 입력 뉴스 하이라이트 키워드 예시:
      {% for t in d.news_highlight_terms[:20] %}
      <span class="pill">{{ t }}</span>
      {% endfor %}
    </p>

    <h3 style="font-size:1rem;color:var(--ok);margin:16px 0 8px;">{% if d.forward_observation | default(false) %}예측 10% 이상 후보{% else %}실제·예측 10% 이상 포함 종목{% endif %}</h3>
    <p class="sub" style="margin-top:0">{% if d.forward_observation | default(false) %}모델 <strong>예측 상승률</strong> 10% 이상 후보입니다. 장 마감 전이므로 실제 상승률은 표시하지 않습니다.{% else %}당일 <strong>실제</strong> 10% 이상 상승 종목과, 모델 <strong>예측 상승률</strong> 10% 이상 후보(중복 제거)를 함께 표시합니다.{% endif %} 위 라디오로 20%이상 / 10~20% 구간을 전환할 수 있습니다.</p>
    {% if d.rows_compare %}
    <table class="rows-compare">
      <colgroup><col class="col-group"/></colgroup>
      <thead>
        <tr>
          <th class="sortable-col col-group" data-sort="group" scope="col" title="구분 우선순위 정렬: 실제+예측 > 실제만 > 예측만">구분</th>
          <th class="sortable-col" data-sort="stock" scope="col" title="종목명/코드 오름차순·내림차순 정렬">종목</th>
          {% if not (d.forward_observation | default(false)) %}
          <th class="sortable-col" data-sort="actual" scope="col" title="종가 확정 후 일봉 기준">실제 상승률(%)</th>
          {% endif %}
          <th class="sortable-col" data-sort="pred" scope="col">예측 상승률(%)</th>
          {% if d.forward_observation | default(false) or d.show_stock_ret_column | default(false) %}
          <th scope="col" title="N-3·N-2·N-1·N 일봉 등락률">N일봉(%)</th>
          {% endif %}
          <th scope="col" title="{% if d.forward_observation | default(false) %}모델·키워드·모멘텀·섹터 요약{% else %}모델·키워드·모멘텀·섹터 요약 tooltip{% endif %}">예측 근거</th>
          <th>보정(%)</th>
          <th scope="col" title="예측≥임계 후보만. 앞: 예측≥임계·실적 확정 건 중 실제≥임계 적중 비율(a/d). vs: 예측≥임계·실적 확정 건 중 실제 0% 이상 비율. 괄호: a=실제≥임계, b=0&lt;실제&lt;임계, c=실제&lt;0, d=예측≥임계·실적 확정 전체 (a b c / d)">누적 정확도<br/><span style="font-size:0.68rem;font-weight:500;color:var(--muted);line-height:1.35;display:block;margin-top:2px">(<span class="sortable-col" data-sort="cumulative_a" title="적중% 정렬">A%</span> <span style="color:var(--muted)">vs</span> <span class="sortable-col" data-sort="cumulative_b" title="실제 0%+ 비율 정렬">B%</span> · a b c / d)</span></th>
          <th>누적정확도(10~20)</th>
          <th>누적정확도(전체)</th>
          <th>이유/차이</th>
          <th scope="col" title="키워드 교집합·종목명 언급·ML 확률·예측 순위·확신 구간·25%↑ 테마">예측 신호</th>
        </tr>
      </thead>
      <tbody>
        {% for r in d.rows_compare %}
        <tr data-market="{{ r.market_segment|default('other') }}" data-rise-band="{{ r.rise_band|default('low') }}">
          <td class="col-group" data-sort-col="group" data-sort-value="{% if r.actual_big and (r.pred_high | default(false)) %}3{% elif r.actual_big %}2{% elif r.pred_high | default(false) %}1{% else %}0{% endif %}">
            {% if not (d.forward_observation | default(false)) and r.actual_big %}<span class="pill" style="background:#1e3d2f;color:var(--ok)">실제≥{{ meta.threshold }}</span>{% endif %}
            {% if r.pred_high | default(false) %}<span class="pill" style="margin-top:4px;display:inline-block">{% if meta.ranking_mode | default(false) %}고확신{% else %}예측≥{{ meta.threshold }}{% endif %}</span>{% elif (r.confidence_tier | default('')) == 'mid' %}<span class="pill" style="margin-top:4px;display:inline-block">중확신</span>{% endif %}
          </td>
          <td data-sort-col="stock" data-sort-value="{{ r.name }} {{ r.code }}">
            {{ stock_name_link(r.code, r.name, r) }}
            {# <div class="pill">{{ r.code }}</div> #}
          </td>
          {% if not (d.forward_observation | default(false)) %}
          <td class="{% if r.actual_big %}ok{% elif r.actual_ret is not none and r.actual_ret < 0 %}bad{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none and r.actual_ret_intraday_pct < 0 %}bad{% endif %}" data-sort-col="actual" data-sort-value="{% if r.actual_cell_pre_close_snapshot | default(false) and r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% elif r.actual_ret is not none %}{{ r.actual_ret }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% endif %}">
            {{ actual_ret_cell(r, d.forward_observation | default(false)) }}
          </td>
          {% endif %}
          <td class="{% if r.pred_high | default(false) %}warn{% endif %}" style="vertical-align:top;{% if r.pred_high | default(false) %}color:var(--warn);font-weight:600{% endif %}" data-sort-col="pred" data-sort-value="{% if r.pred_ret is not none %}{{ r.pred_ret }}{% endif %}">
            {{ pred_ret_cell(d, r) }}
          </td>
          {{ stock_ret_chain_cell(d, r) }}
          {{ pred_rationale_cell(d, r) }}
          <td style="vertical-align:top">
            {% if r.pred_ret is not none and r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
            {% if r.cumulative_accuracy_from_hist | default(false) %}—{% else %}{{ "%.2f"|format(r.pred_ret * r.cumulative_accuracy_avg) }}{% endif %}
            {% else %}—{% endif %}
          </td>
          {{ cumulative_accuracy_td(r, meta) | safe }}
          <td class="num">
            {% if r.cumulative_accuracy_10_20_avg is defined and r.cumulative_accuracy_10_20_avg is not none %}{{ "%.2f"|format(r.cumulative_accuracy_10_20_avg * 100) }}%{% else %}—{% endif %}
          </td>
          <td class="num">
            {% if r.cumulative_accuracy_all_avg is defined and r.cumulative_accuracy_all_avg is not none %}{{ "%.2f"|format(r.cumulative_accuracy_all_avg * 100) }}%{% else %}—{% endif %}
          </td>
          <td class="pred-reason">
            <span class="gap-tip combo-tip integrate-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 이유, 예측·실제 차이, 상·하락 참고를 함께 보기">통합 보기</span>
              <div class="gap-tip-popup combo-tip-popup integrate-tip-popup" role="tooltip">
                <div class="combo-tip-inner">
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측 이유</h4>
                    <div class="combo-tip-body">{{ r.pred_reason_detail_html | default('') | safe }}</div>
                  </div>
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측·실제 차이</h4>
                    <div class="combo-tip-body">
                      {% if r.gap_analysis_html %}
                      {{ r.gap_analysis_html | safe }}
                      {% else %}
                      <p class="combo-tip-empty">해당 설명이 없습니다.</p>
                      {% endif %}
                    </div>
                  </div>
                  <div class="combo-tip-rise">
                    <h4 class="combo-tip-h">상·하락 참고 (특징·추세·수급·시장·의견)</h4>
                    <div class="combo-tip-body">{{ r.rise_reason_html | default('') | safe }}</div>
                  </div>
                </div>
              </div>
            </span>
            <span style="margin-left:10px">{{ disclosure_tip(r, d.trading_day) }}</span>
            <span class="pred-reason-plain" style="margin-left:10px">{% if not (d.forward_observation | default(false)) %}{{ r.pred_reason_hit_line | default(r.pred_reason_summary) | default('—') | safe }}{% endif %}</span>
          </td>
          <td>{{ prediction_signal_cell(r) }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% else %}
    <p class="sub">당일 실제·예측 {{ meta.threshold }} 이상 해당 종목 없음.</p>
    {% endif %}

    {% if d.false_negatives %}
    <h3 style="font-size:1rem;color:var(--bad);margin:20px 0 8px;">예측했으나 실제 음수 수익 — 집중 점검</h3>
    {% for fn in d.false_negatives %}
    <div class="fn-block">
      <strong>{{ stock_name_link(fn.code, fn.name) }}</strong>
      {# ({{ fn.code }}) · #}
      예측 {{ "%.2f"|format(fn.pred_ret) }}% · 실제
      <span class="bad">{{ "%.2f"|format(fn.actual_ret * 100) }}%</span>
      <p class="reasons" style="margin:8px 0;">{{ fn.analysis_html | default(fn.analysis) | safe }}</p>
      <p class="reasons"><em>예측 시 참고한 키워드:</em>
        {% for k in fn.keywords[:15] %}<span class="pill">{{ k }}</span>{% endfor %}
      </p>
    </div>
    {% endfor %}
    {% endif %}

    <h3 style="font-size:1rem;color:var(--muted);margin:20px 0 8px;">참고 뉴스 제목 (일부)</h3>
    <ul class="news">
      {% for t in d.news_titles_sample[:12] %}
      <li>{{ highlight_terms(t, d.news_highlight_terms) | safe }}</li>
      {% endfor %}
    </ul>
  </section>
{%- endmacro %}

  <h1>{{ title }}</h1>
  <p class="sub">
    생성 기준: 훈련 구간 {{ meta.train_range }} · 테스트 구간 {{ meta.test_range }} ·
    급등 기준 {{ meta.threshold }} · 뉴스 출처: {{ meta.news_source }}
  </p>

  <section>
    <h2>주의사항 · 매수 시나리오와 뉴스 시각</h2>
    {% if meta.use_decision_cutoff %}
    <ul class="news" style="margin-top:8px;line-height:1.6">
      <li>한국 현물시장은 <strong>14:30</strong>에 장이 마감됩니다. 본 리포트는 <strong>N 거래일 장 마감 직전(약 14:00~14:50)</strong>에 매수 주문을 넣어 <strong>N+1 거래일</strong>에 20% 이상 상승할 종목을 고르는 전제에 맞춥니다.</li>
      <li>그에 따라 예측·훈련에 쓰는 뉴스는 <strong>N-1 거래일 {{ meta.cutoff_kst }}(KST)까지</strong>로 제한합니다. (N = 익일 T의 직전 거래일, N-1은 그 이전 거래일.) <strong>N-1일 {{ meta.cutoff_kst }} 이후</strong> 기사와 <strong>N일·주말</strong> 등 그 다음 캘린더 구간 뉴스는 예측 입력에서 빠집니다.</li>
      <li><strong>N-1일 {{ meta.cutoff_kst }} 이후</strong> 뉴스와, 예측 종목이 <strong>실제로 20% 미만</strong>으로 마감한 경우의 겹침(간단 문자열 매칭)은 아래 &quot;탐색&quot; 표로 따로 집계합니다. 인과 검증이 아니라 후속 분석용입니다.</li>
      <li>기사에 시각이 없거나 옛 캐시면 해당 캘린더일은 <strong>09:00 KST</strong>로 간주해 early/late를 나눕니다. 시각이 중요하면 <code>data/cache/news/naver</code> 또는 <code>…/google</code> 아래 해당 월·일 JSON 삭제 후 재수집을 권장합니다.</li>
    </ul>
    {% else %}
    <p class="sub" style="margin:0">
      일자 단위 전통 뉴스 윈도우만 사용 중입니다. N-1 거래일 {{ meta.cutoff_kst }}(KST) 컷오프·지연 뉴스 탐색을 쓰려면 <code>USE_DECISION_NEWS_INTRADAY_CUTOFF=1</code>로 실행하세요.
    </p>
    {% endif %}
  </section>

  <section>
    <h2>요약</h2>
    <p class="sub" style="margin:0">
      테스트 일수 {{ meta.n_days }} · 예측 종목 총건수 {{ meta.total_preds }} ·
      실제 20% 이상 급등 {{ meta.total_actual_big }}건 ·
      <span class="bad">예측했으나 실제 음수 수익</span> {{ meta.n_false_neg }}건
    </p>
  </section>

  {% if meta.correlation_rows %}
  <section>
    <h2>급등 라벨 풀: 뉴스 키워드·20% 급등 공출현 요약</h2>
    <p class="sub" style="margin-top:0">
      이번 배치 최초 관측일 직전까지의 급등 사건에서, 당일 뉴스 키워드가 몇 번 등장했는지 집계했습니다.
      (예측 입력은 14:30까지 뉴스·테마·시세 등 전체이며, 본 표는 보조 프로필용 공출현입니다.)
    </p>
    <table>
      <thead><tr><th>키워드</th><th>급등 사건 수(종목·일)</th></tr></thead>
      <tbody>
        {% for word, cnt in meta.correlation_rows %}
        <tr><td>{{ word }}</td><td>{{ cnt }}</td></tr>
        {% endfor %}
      </tbody>
    </table>
  </section>
  {% endif %}

  {% if meta.late_news_probe %}
  <section>
    <h2>N-1일 {{ meta.cutoff_kst }} 이후 뉴스 vs 실제 20% 미만 (탐색)</h2>
    <p class="sub" style="margin-top:0">
      예측 상위 종목마다 &quot;지연 구간&quot; 뉴스(위 주의사항 정의)에 <em>예측 시 일치한 키워드</em>가 등장했는지 단순 포함 여부로 집계했습니다.
    </p>
    <table>
      <thead><tr><th>구분</th><th>표본 수</th><th>지연 뉴스에 키워드 겹침</th><th>비율</th></tr></thead>
      <tbody>
        <tr>
          <td>실제 상승률 20% 미만</td>
          <td>{{ meta.late_news_probe.below_n }}</td>
          <td>{{ meta.late_news_probe.below_kw }}</td>
          <td>{{ meta.late_news_probe.below_pct }}</td>
        </tr>
        <tr>
          <td>실제 상승률 20% 이상</td>
          <td>{{ meta.late_news_probe.gte_n }}</td>
          <td>{{ meta.late_news_probe.gte_kw }}</td>
          <td>{{ meta.late_news_probe.gte_pct }}</td>
        </tr>
      </tbody>
    </table>
  </section>
  {% endif %}

  {% if tabbed and days %}
  <section class="tabs-wrap">
    <h2>거래일별 보기 (탭)</h2>
    {% if week_note %}
    <p class="sub" style="margin-top:0">{{ week_note }}</p>
    {% endif %}
    <div class="tab-bar" role="tablist">
      {% for d in days %}
      <button type="button" class="tab-btn{% if loop.first %} active{% endif %}" role="tab"
              aria-selected="{{ 'true' if loop.first else 'false' }}" data-tab-idx="{{ loop.index0 }}">{{ d.trading_day.isoformat() }}</button>
      {% endfor %}
    </div>
    {% for d in days %}
    <div class="tab-panel{% if loop.first %} active{% endif %}" role="tabpanel" data-tab-panel="{{ loop.index0 }}">
      {{ day_panel(d, meta) }}
    </div>
    {% endfor %}
  </section>
  <script>
  (function () {
    var bar = document.querySelector(".tabs-wrap .tab-bar");
    if (!bar) return;
    var wrap = bar.closest(".tabs-wrap");
    var btns = bar.querySelectorAll(".tab-btn");
    var panels = wrap.querySelectorAll(".tab-panel");
    function show(i) {
      btns.forEach(function (b, j) {
        b.classList.toggle("active", j === i);
        b.setAttribute("aria-selected", j === i ? "true" : "false");
      });
      panels.forEach(function (p, j) { p.classList.toggle("active", j === i); });
    }
    btns.forEach(function (b, i) { b.addEventListener("click", function () { show(i); }); });
  })();
  </script>
  {% else %}
  {% for d in days %}
  {{ day_panel(d, meta) }}
  {% endfor %}
  {% endif %}
{{ interaction_snippet | safe }}
</body>
</html>
"""

_COMPACT_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko" data-live-quotes-base="http://127.0.0.1:8765">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root {
      --bg: #0f1419; --card: #1a2332; --text: #e7ecf3; --muted: #8b9cb3;
      --accent: #3d9cf5; --ok: #3ecf8e; --bad: #f07178; --warn: #e6c07b;
    }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.5; }
    h1 { font-size: 1.45rem; margin-bottom: 0.35rem; }
    .sub { color: var(--muted); font-size: 0.88rem; margin-bottom: 1rem; }
    section { background: var(--card); border-radius: 12px; padding: 18px 20px; margin-bottom: 18px;
              border: 1px solid #243044; }
    h2 { font-size: 1.05rem; margin: 0 0 10px 0; color: var(--accent); }
    table { width: 100%; border-collapse: collapse; font-size: 0.86rem; }
    th, td { padding: 9px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    tr:hover td { background: #1e2a3d; }
    a.stock { color: var(--accent); text-decoration: none; font-weight: 600; }
    a.stock:hover { text-decoration: underline; }
    .ok { color: var(--ok); font-weight: 600; }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); font-weight: 600; }
    .pill { display: inline-block; padding: 1px 6px; border-radius: 6px; font-size: 0.72rem;
             margin: 1px 3px 1px 0; background: #243044; color: var(--muted); }
    .tab-bar { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 14px; }
    .tab-bar.tab-bar-bottom { margin-top: 20px; margin-bottom: 0; }
    .tab-btn { font: inherit; cursor: pointer; padding: 8px 12px; border-radius: 8px; border: 1px solid #2a3548;
              background: #131b28; color: var(--muted); }
    .tab-btn.active { background: var(--accent); color: #0f1419; border-color: var(--accent); font-weight: 600; }
    .tab-panel { display: none; }
    .tab-panel.active { display: block; }
    .note { font-size: 0.82rem; color: var(--muted); margin-top: 10px; line-height: 1.45; }
    .day-stack { margin-bottom: 28px; padding-bottom: 4px; border-bottom: 1px solid #2a3f5c; }
    .day-stack:last-of-type { border-bottom: none; margin-bottom: 8px; }
    .day-stack > h2 { font-size: 1.08rem; margin: 0 0 12px 0; color: #8ec5f6; }
    table.rows-compare { table-layout: fixed; }
    table.rows-compare col.col-group,
    table.rows-compare th.col-group,
    table.rows-compare td.col-group,
    table.rows-compare th:nth-child(1),
    table.rows-compare td:nth-child(1) {
      width: 5.75rem; min-width: 5.75rem; max-width: 5.75rem;
    }
    table.rows-compare td.col-group,
    table.rows-compare td:nth-child(1) { white-space: nowrap; vertical-align: top; }
    .gap-tip { position: relative; display: inline-block; margin-top: 4px; max-width: 100%; }
    .gap-tip.gap-tip-inline { margin-top: 0; margin-left: 8px; vertical-align: baseline; }
    .gap-tip.gap-tip-end .gap-tip-popup { left: auto; right: 0; }
    .gap-tip-trigger {
      cursor: help; border-bottom: 1px dotted var(--accent); color: var(--accent);
      font-size: 0.72rem; font-weight: 600; outline: none;
    }
    .gap-tip-trigger:hover, .gap-tip-trigger:focus { color: #7ec4ff; }
    .gap-tip-popup {
      display: none; position: absolute; z-index: 500; left: 0; top: calc(100% + 6px);
      min-width: 360px; max-width: min(900px, 96vw); max-height: 92vh;
      overflow: auto; padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e;
      border-radius: 8px; box-shadow: 0 8px 24px rgba(0,0,0,0.45);
      font-size: 0.8rem; line-height: 1.55; color: #d0dce8; text-align: left;
    }
    .gap-tip-popup p { margin: 0 0 6px 0; }
    .gap-tip-popup ul { margin: 4px 0 0 14px; padding: 0; }
    .gap-tip-popup li { margin-bottom: 4px; }
    .gap-tip:not(.integrate-tip):hover .gap-tip-popup,
    .gap-tip:not(.integrate-tip):focus-within .gap-tip-popup { display: block; }
    .gap-tip.integrate-tip:hover .gap-tip-trigger,
    .gap-tip.integrate-tip:focus-within .gap-tip-trigger { color: #7ec4ff; border-bottom-color: #7ec4ff; }
    .gap-tip.integrate-tip:hover .integrate-tip-popup,
    .gap-tip.integrate-tip:focus-within .integrate-tip-popup { display: block !important; }
    .kw-count-tip { display: inline; vertical-align: baseline; margin: 0 1px; }
    .kw-count-tip .gap-tip-trigger { font-size: inherit; font-weight: 700; color: var(--warn); border-bottom-color: var(--warn); }
    .gap-tip.pred-miss-tip .gap-tip-trigger { color: var(--bad); border-bottom-color: rgba(248,113,113,0.85); }
    .gap-tip.pred-miss-tip .gap-tip-trigger:hover,
    .gap-tip.pred-miss-tip .gap-tip-trigger:focus { color: #ffa8a8; border-bottom-color: #ffa8a8; }
    .kw-list-popup { min-width: 280px; max-width: min(520px, 92vw) !important; width: auto !important; max-height: 70vh; z-index: 4100; }
    .integrate-tip-floating .kw-list-popup { z-index: 4100; }
    .integrate-tip-popup.integrate-tip-floating {
      display: block !important;
      position: fixed !important;
      z-index: 4000 !important;
      transform: none !important;
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      max-height: 90vh !important;
      overflow: auto !important;
      box-sizing: border-box;
      padding: 14px 16px !important;
      background: #1a2838;
      border: 1px solid #3d6a9e;
      border-radius: 8px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.5);
    }
    .pred-reason-plain { font-size: 0.82rem; color: var(--muted); line-height: 1.45; max-width: 26em; display: inline-block; vertical-align: middle; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
    .combo-tip { margin-left: 6px; vertical-align: middle; white-space: nowrap; }
    .combo-tip-popup {
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      left: 0 !important;
      right: auto !important;
      transform: none !important;
      top: calc(100% + 8px) !important;
      box-sizing: border-box;
      max-height: 90vh;
      overflow: auto;
      padding: 14px 16px !important;
    }
    .combo-tip-inner {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0 20px;
      align-items: start;
      width: 100%;
    }
    .combo-tip-col { min-width: 0; border-left: 1px solid #2d4a6a; padding-left: 16px; }
    .combo-tip-col:first-child { border-left: none; padding-left: 0; padding-right: 4px; }
    .combo-tip-h {
      margin: 0 0 8px 0;
      font-size: 0.76rem;
      font-weight: 700;
      color: #8ec5f6;
    }
    .combo-tip-body {
      font-size: 0.8rem; line-height: 1.6; color: #d0dce8;
      overflow-wrap: anywhere; word-wrap: break-word; word-break: break-word;
      white-space: normal;
    }
    .combo-tip-body p { margin: 0 0 6px 0; }
    .combo-tip-empty { margin: 0; color: var(--muted); font-style: italic; font-size: 0.8rem; }
    .disclosure-tip-popup { min-width: 300px; max-width: min(560px, 92vw) !important; width: auto !important; }
    .disclosure-tip-list { margin: 6px 0 0; padding-left: 0; list-style: none; }
    .disclosure-tip-list li { margin-bottom: 8px; line-height: 1.45; }
    .disc-kind { font-size: 0.75rem; color: #9fd3ff; margin-right: 4px; }
    .combo-tip-rise {
      grid-column: 1 / -1;
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid #2d4a6a;
      width: 100%;
    }
    ul.nl { margin: 4px 0 0 0; padding-left: 18px; }
    ul.nl li { margin-bottom: 6px; }
    @media (max-width: 760px) {
      .combo-tip-inner { grid-template-columns: 1fr; }
      .combo-tip-col { border-left: none; padding-left: 0; border-top: 1px solid #2d4a6a; padding-top: 12px; margin-top: 10px; }
      .combo-tip-col:first-child { border-top: none; padding-top: 0; margin-top: 0; }
    }
    .kw-pills { display: inline-flex; flex-wrap: wrap; gap: 4px 4px; align-items: center; vertical-align: middle; }
    .kw-pills .pill { font-size: 0.68rem; padding: 1px 6px; }
    .movers-data-note { background: #2a1f18; border: 1px solid #8b5a2b; border-radius: 10px;
                        padding: 12px 14px; margin-bottom: 14px; font-size: 0.86rem; line-height: 1.55;
                        color: #e8c9a8; }
    .day-heading-row { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-start; gap: 8px 14px; margin-bottom: 6px; }
    .day-heading-row h2 { margin: 0; }
    .market-filter-radios, .rise-filter-radios { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; font-size: 0.82rem; color: var(--muted); }
    .market-filter-title, .rise-filter-title { font-weight: 600; color: var(--muted); margin-right: 2px; }
    .market-filter-label, .rise-filter-label { cursor: pointer; display: inline-flex; align-items: center; gap: 5px; margin: 0; font-weight: 500; }
    .market-filter-label input, .rise-filter-label input { accent-color: var(--accent); vertical-align: middle; }
    table.rows-compare th.sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th.sortable-col:hover { text-decoration: underline; }
    table.rows-compare th.sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th.sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th .sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th .sortable-col:hover { text-decoration: underline; }
    table.rows-compare th .sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th .sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    td.forward-actual-ret { white-space: nowrap; }
    .forward-ret-chain { white-space: nowrap; }
    .live-intraday-toggle { cursor: pointer; border: 1px solid #3d6a9e; background: #1a2838; color: var(--accent); font-size: 0.78rem; }
    .live-intraday-toggle:hover { border-color: var(--accent); }
    .live-intraday-toggle.active { background: var(--accent); color: #0f1419; border-color: var(--accent); }
    .pred-live-intraday { margin-left: 2px; font-size: 0.92em; color: var(--muted); white-space: nowrap; }
    .pred-live-intraday-pct.ok { color: var(--ok); font-weight: 600; }
    .pred-live-intraday-pct.bad { color: var(--bad); font-weight: 600; }
    .cumulative-accuracy-td { position: relative; }
    .cumulative-accuracy-td .cumulative-sort-keys { position: absolute; left: -9999px; top: 0; width: 1px; height: 1px; overflow: hidden; }
    .gap-tip.cumulative-hist-tip { margin-top: 0; vertical-align: middle; }
    .gap-tip.cumulative-hist-tip .gap-tip-trigger {
      font-size: inherit;
      font-variant-numeric: tabular-nums;
    }
    .gap-tip.cumulative-hist-tip .gap-tip-popup.cumulative-hist-popup {
      min-width: 280px;
      max-width: min(440px, 94vw);
      z-index: 600;
    }
    .stock-chart-tip { position: relative; display: inline-block; vertical-align: baseline; max-width: 100%; }
    .stock-chart-tip .stock { position: relative; z-index: 1; }
    .stock-chart-popup {
      display: none; position: absolute; left: 0; top: calc(100% + 6px); z-index: 850;
      padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e; border-radius: 10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.55);
      min-width: min(720px, calc(100vw - 32px)); max-width: min(720px, calc(100vw - 24px));
      box-sizing: border-box;
    }
    .stock-chart-tip:hover .stock-chart-popup,
    .stock-chart-tip:focus-within .stock-chart-popup { display: block; }
    .stock-chart-frame { position: relative; display: block; width: 100%; max-width: 700px; }
    .stock-chart-frame .stock-chart-img {
      display: block; width: 100%; max-width: 700px; height: auto;
      background: #0f1419; border-radius: 6px;
    }
    .stock-chart-n-marker {
      display: none; position: absolute; width: 2px; margin-left: -1px;
      background: #e6c07b; box-shadow: 0 0 8px rgba(230, 192, 123, 0.9);
      pointer-events: none; z-index: 2;
    }
    .stock-chart-n-label {
      display: none; position: absolute; transform: translate(-50%, 0);
      font-size: 0.66rem; font-weight: 700; line-height: 1.2;
      color: #1a1a1a; background: #e6c07b; padding: 1px 5px; border-radius: 3px;
      pointer-events: none; z-index: 3; white-space: nowrap;
    }
    .stock-chart-frame.has-n-marker .stock-chart-n-marker,
    .stock-chart-frame.has-n-marker .stock-chart-n-label { display: block; }
    .stock-chart-caption { display: block; margin-top: 8px; font-size: 0.74rem; color: var(--muted); text-align: center; line-height: 1.4; }
    .stock-ret-lines {
      display: flex; flex-direction: row; flex-wrap: wrap; gap: 0 1.25em; justify-content: center; align-items: center;
      margin-bottom: 10px; padding: 8px 10px; background: #151c24; border-radius: 6px;
      font-size: 0.78rem; font-variant-numeric: tabular-nums; line-height: 1.45;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .stock-ret-line { white-space: nowrap; }
    .stock-ret-lbl { color: var(--text); font-weight: 600; }
    .stock-ret-line.ok .stock-ret-pct { color: var(--ok); }
    .stock-ret-line.bad .stock-ret-pct { color: var(--bad); }
    .stock-ret-dt { color: var(--muted); }
    .stock-ret-col-lines {
      display: flex; flex-direction: column; gap: 3px;
      font-size: 0.74rem; font-variant-numeric: tabular-nums; line-height: 1.35;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      min-width: 8.5rem;
    }
    .stock-ret-col-lines .stock-ret-line { display: flex; justify-content: space-between; gap: 10px; }
    .pred-reason-inline { font-size: 0.82rem; line-height: 1.45; white-space: normal; max-width: 28rem; }
  </style>
</head>
<body>
{% macro stock_name_link(code, name, r=none) -%}
<span class="stock-chart-tip" tabindex="0"{% if code %} data-stock-code="{{ (code|string).zfill(6) }}"{% endif %}>
  <a class="stock" target="_blank" rel="noopener" href="{{ naver_chart_url(code) }}"{# title="클릭: 네이버 차트 · 호버: 일봉·최근 등락률" #}>{{ name }}</a>
  <span class="stock-chart-popup" role="tooltip">
    {% if r and not (r.forward_observation | default(false)) %}{{ format_stock_ret_tooltip_lines(r) | safe }}{% endif %}
    {% set chart_n_day = stock_chart_n_day_iso(r) if r else "" %}
    {% set chart_n_off = stock_chart_n_bar_offset(r) if r else none %}
    <div class="stock-chart-frame"{% if chart_n_day %} data-n-day="{{ chart_n_day }}"{% endif %}{% if chart_n_off is not none %} data-n-offset="{{ chart_n_off }}"{% endif %}>
      <img class="stock-chart-img" data-chart-base="{{ naver_chart_day_img_url(code) }}" alt="{{ name }} 일봉 캔들 차트" width="700" height="289" decoding="async" referrerpolicy="no-referrer-when-downgrade"/>
      <span class="stock-chart-n-marker" aria-hidden="true"></span>
      <span class="stock-chart-n-label" aria-hidden="true">N</span>
    </div>
    <span class="stock-chart-caption">일봉 캔들 · 호버 시 최신{% if chart_n_day %} · 노란 세로선=N({{ chart_n_day[5:7] }}/{{ chart_n_day[8:10] }}){% endif %}</span>
  </span>
</span>
{%- endmacro %}
__ACTUAL_RET_CELL_MACRO_MONTHLY__
{% macro compact_cumulative_td(r, meta) -%}
<td class="cumulative-accuracy-td" style="white-space:nowrap;font-variant-numeric:tabular-nums">
  {% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
  <span class="cumulative-sort-keys" aria-hidden="true">
    <span data-sort-col="cumulative_a" data-sort-value="{{ r.cumulative_accuracy_avg }}"></span>
    {% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %}
    <span data-sort-col="cumulative_b" data-sort-value="{{ r.cumulative_nonneg_rate_pct }}"></span>
    {% endif %}
  </span>
  <span class="gap-tip cumulative-hist-tip">
    <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="누적 정확도·{{ meta.threshold }} 이상 예측 이력">{{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%{% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %} vs {{ "%.2f"|format(r.cumulative_nonneg_rate_pct) }}%{% endif %}{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} ({{ r.cumulative_hit_x }} {% if r.cumulative_hit_z is defined and r.cumulative_hit_z is not none %}{{ r.cumulative_hit_z }}{% else %}0{% endif %} <span class="bad">{% if r.cumulative_hit_neg is defined and r.cumulative_hit_neg is not none %}{{ r.cumulative_hit_neg }}{% else %}0{% endif %}</span> / {{ r.cumulative_hit_y }}){% endif %}</span>
    <div class="gap-tip-popup cumulative-hist-popup" role="tooltip">
      <div class="combo-tip-body">
        <strong>관측일 T별 · 예측 ≥ {{ meta.threshold }}</strong>
        {% if r.pred_high_history|default([])|length > 0 %}
        <ul style="margin:8px 0 0 0;padding-left:18px">
        {% for h in r.pred_high_history %}
          <li><span class="pill">{{ h.t }}</span> 예측 {{ "%.2f"|format(h.pred_pct) }}%
            {% if h.actual_pct is not none %} · 실제 {{ "%.2f"|format(h.actual_pct) }}%{% else %} · 실적 미확정{% endif %}
          </li>
        {% endfor %}
        </ul>
        {% else %}
        <p class="combo-tip-empty" style="margin:8px 0 0 0">저장된 {{ meta.threshold }} 이상 예측 이력이 없습니다.</p>
        {% endif %}
        <p style="font-size:0.76rem;color:var(--muted);margin:8px 0 0 0;line-height:1.35">앞 {{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%: 예측≥{{ meta.threshold }}·실적 확정 건 중 실제≥{{ meta.threshold }} 적중 비율(맞춘 건수÷전체, a/d와 동일).{% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %} vs {{ "%.2f"|format(r.cumulative_nonneg_rate_pct) }}%: 예측≥{{ meta.threshold }}·실적 확정 건 중 실제가 0% 이상인 비율.{% endif %}{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} 괄호 (a b c / d): 예측≥{{ meta.threshold }}·실적 확정 건 중 a=실제≥{{ meta.threshold }}, b=0&lt;실제&lt;{{ meta.threshold }}, c=실제&lt;0(빨간색), d=전체.{% endif %}</p>
      </div>
    </div>
  </span>
  {% else %}—{% endif %}
</td>
{%- endmacro %}
{% macro market_filter_radios(suffix, forward_day=false) -%}
<div class="market-filter-radios" role="radiogroup" aria-label="표 시장 구분">
  <span class="market-filter-title">시장 -</span>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="all" checked="checked"/> 전체</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kospi"/> KOSPI</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kosdaq"/> KOSDAQ</label>
</div>
<div class="rise-filter-radios" role="radiogroup" aria-label="상승률 구간">
  <span class="rise-filter-title">상승률 -</span>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="all" checked="checked"/> 예측후보 전체</label>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="high"/> 20%이상</label>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="mid"/> 10%~20%</label>
</div>
{%- endmacro %}
{% macro prediction_signal_cell(r) -%}
{{ format_prediction_signal_cell(r) | safe }}
{%- endmacro %}
{% macro disclosure_tip(r, trading_day=none) -%}
<span class="gap-tip combo-tip disclosure-tip{% if trading_day is not none %} gap-tip-end{% endif %}">
  <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="당일 종목 공시 목록 보기">공시</span>
  <div class="gap-tip-popup combo-tip-popup disclosure-tip-popup" role="tooltip">
    <h4 class="combo-tip-h">{% if trading_day is not none %}종목 공시 · {{ trading_day.isoformat() }}{% else %}종목 공시{% endif %}</h4>
    <ul class="nl disclosure-tip-list">
    {% for h in r.disclosure_hits|default([]) %}
      <li>
        {% if h.day %}<span class="pill">{{ h.day }}</span>{% endif %}
        <code class="disc-kind">{{ h.kind }}</code>
        {% if h.link %}
        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
        {% else %}
        {{ h.title }}
        {% endif %}
      </li>
    {% else %}
      <li class="muted">{% if trading_day is not none %}이 거래일({{ trading_day.isoformat() }})에 등록된 공시가 없습니다.{% else %}공시 목록이 없습니다.{% endif %}</li>
    {% endfor %}
    </ul>
  </div>
</span>
{%- endmacro %}
{% macro pred_ret_cell(d, r) -%}
{% if r.pred_ret is not none %}{{ "%.2f"|format(r.pred_ret) }}{% else %}—{% endif %}{% if d.forward_observation | default(false) %}<span class="pred-live-intraday" hidden aria-hidden="true"> · <span class="pred-live-intraday-pct" data-stock-code="{{ (r.code|string).zfill(6) }}">…</span>%</span>{% endif %}
{%- endmacro %}
{% macro market_theme_panel(d) -%}
{% if d.market_theme_html or (d.forward_observation | default(false)) %}
<div class="market-theme-ref" style="margin:12px 0 16px;padding:12px 14px;background:#152232;border:1px solid #2a4a6a;border-radius:8px">
  <div class="market-theme-heading-row" style="display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin:0 0 8px">
    <h3 style="font-size:0.95rem;color:var(--ok);margin:0">당일 테마 요약</h3>
    {% if d.forward_observation | default(false) %}
    <button type="button" class="pill live-intraday-toggle" data-live-scope="day-{{ d.trading_day.isoformat() }}" aria-pressed="false" title="클릭 시 예측 상승률 옆에 장중 실시간 등락률을 표시합니다">실시간 상승률</button>
    {% endif %}
  </div>
  {% if d.market_theme_html %}{{ d.market_theme_html | safe }}{% endif %}
</div>
{% endif %}
{%- endmacro %}
{% macro forward_pred_rationale_panel(d, meta) -%}
{# 장 마감 전 관측일: 예측 근거는 표 ``예측 근거`` 열에 전문 표시 #}
{%- endmacro %}
{% macro stock_ret_chain_cell(d, r) -%}
{% if d.forward_observation | default(false) or d.show_stock_ret_column | default(false) %}
<td class="stock-ret-chain-col" style="vertical-align:top">{{ format_stock_ret_column_lines(r) | safe }}</td>
{% endif %}
{%- endmacro %}
{% macro pred_rationale_cell(d, r) -%}
<td class="pred-reason-forward" style="vertical-align:top;{% if d.forward_observation | default(false) %}white-space:normal;max-width:28rem;line-height:1.45{% else %}white-space:nowrap{% endif %}">
  {% if r.pred_ret is not none %}
    {% set pred_reason_body = r.pred_reason_tooltip_html or r.pred_reason_detail_html %}
    {% if d.forward_observation | default(false) %}
      {% if pred_reason_body and pred_reason_body != '—' %}
      <div class="pred-reason-inline">{{ pred_reason_body | safe }}</div>
      {% else %}—{% endif %}
    {% else %}
      {% if pred_reason_body and pred_reason_body != '—' %}
      <span class="gap-tip pred-reason-tip">
        <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 근거">근거</span>
        <div class="gap-tip-popup pred-reason-popup" role="tooltip">
          <div class="combo-tip-body">{{ pred_reason_body | safe }}</div>
        </div>
      </span>
      {% else %}—{% endif %}
      {% if r.pred_miss_tooltip_html %}
      <span class="gap-tip pred-miss-tip" style="margin-left:8px">
        <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="틀린 이유">틀린 이유</span>
        <div class="gap-tip-popup pred-miss-popup" role="tooltip">
          <div class="combo-tip-body">{{ r.pred_miss_tooltip_html | safe }}</div>
        </div>
      </span>
      {% endif %}
    {% endif %}
  {% else %}—{% endif %}
</td>
{%- endmacro %}
{% macro hit_at_k_panel(d, meta) -%}
{% if d.hit_at_k_metrics and not (d.forward_observation | default(false)) %}
<div class="hit-at-k-panel" style="margin:8px 0 14px;padding:10px 12px;background:#1a2433;border:1px solid #334;border-radius:8px;font-size:0.88rem">
  <strong style="color:var(--ok)">순위 평가 Hit@K</strong>
  <span class="sub" style="margin-left:8px">실제 {{ meta.threshold }} {{ d.hit_at_k_metrics.actual_big_count }}종 · 무작위 대비 lift</span>
  <div style="margin-top:6px;display:flex;flex-wrap:wrap;gap:8px">
    {% for k in [5,10,20,40] %}
    {% if d.hit_at_k_metrics['hit_at_' ~ k] is defined %}
    <span class="pill">@{{ k }}: {{ d.hit_at_k_metrics['hit_at_' ~ k] }}/{{ k }}
      {% if d.hit_at_k_metrics['lift_at_' ~ k] is not none %} (×{{ "%.1f"|format(d.hit_at_k_metrics['lift_at_' ~ k]) }}){% endif %}
    </span>
    {% endif %}
    {% endfor %}
  </div>
</div>
{% endif %}
{%- endmacro %}
{% macro week_tabs_bar(week_panels, extra_class='') -%}
<div class="tab-bar{% if extra_class %} {{ extra_class }}{% endif %}" role="tablist">
  {% for w in week_panels %}
  <button type="button" class="tab-btn{% if loop.first %} active{% endif %}" role="tab"
          aria-selected="{{ 'true' if loop.first else 'false' }}" data-tab-idx="{{ loop.index0 }}">{{ w.label }}</button>
  {% endfor %}
</div>
{%- endmacro %}
{% macro day_pred_accuracy_banner(d, meta) -%}
{% set s = d.pred_accuracy_summary | default(none) %}
{% if s and (s.n_pred_high | default(0)) > 0 and (s.n_with_actual | default(0)) > 0 %}
<p class="sub" style="margin:6px 0 10px;padding:8px 10px;background:#1a2230;border:1px solid #3a4a5c;border-radius:6px;font-size:0.88rem;line-height:1.45">
  <strong>당일 예측≥{{ meta.threshold }}</strong> {{ s.n_pred_high }}건 · 실적 확정 {{ s.n_with_actual }}건 ·
  임계 적중 <strong class="{% if (s.n_hit_threshold | default(0)) == 0 %}bad{% else %}ok{% endif %}">{{ s.n_hit_threshold }}</strong>/{{ s.n_with_actual }}
  {% if s.mean_accuracy_ratio is not none %}
  · 당일 달성률 <strong class="{% if s.mean_accuracy_ratio < 0.01 %}bad{% endif %}">{{ "%.2f"|format(s.mean_accuracy_ratio * 100) }}%</strong>
  {% endif %}
  {% if s.n_negative | default(0) > 0 %} · <span class="bad">음수 {{ s.n_negative }}건</span>{% endif %}
</p>
{% endif %}
{%- endmacro %}
{% macro compact_day_table(d, meta, empty_extra='') -%}
{% if d.rows_compare %}
<table class="rows-compare">
  <colgroup><col class="col-group"/></colgroup>
  <thead>
    <tr>
      <th class="sortable-col col-group" data-sort="group" scope="col" title="구분 우선순위 정렬: 실제+예측 > 실제만 > 예측만">구분</th>
      <th class="sortable-col" data-sort="stock" scope="col" title="종목명/코드 오름차순·내림차순 정렬">종목</th>
      {% if not (d.forward_observation | default(false)) %}
      <th class="sortable-col" data-sort="actual" scope="col" title="종가 확정 후 일봉 기준">실제 상승률(%)</th>
      {% endif %}
      <th class="sortable-col" data-sort="pred" scope="col">예측 상승률(%)</th>
      {% if d.forward_observation | default(false) or d.show_stock_ret_column | default(false) %}
      <th scope="col" title="N-3·N-2·N-1·N 일봉 등락률">N일봉(%)</th>
      {% endif %}
      <th scope="col" title="{% if d.forward_observation | default(false) %}예측 근거 전문{% else %}예측 근거·미적중 시 틀린 이유 tooltip{% endif %}">예측 근거</th>
      <th>보정(%)</th>
      <th scope="col" title="예측≥임계 후보만. 앞: 예측≥임계·실적 확정 건 중 실제≥임계 적중 비율(a/d). vs: 예측≥임계·실적 확정 건 중 실제 0% 이상 비율. 괄호: a=실제≥임계, b=0&lt;실제&lt;임계, c=실제&lt;0, d=예측≥임계·실적 확정 전체 (a b c / d)">누적 정확도<br/><span style="font-size:0.65rem;font-weight:500;color:var(--muted);line-height:1.35;display:block;margin-top:2px">(<span class="sortable-col" data-sort="cumulative_a" title="적중% 정렬">A%</span> <span style="color:var(--muted)">vs</span> <span class="sortable-col" data-sort="cumulative_b" title="실제 0%+ 비율 정렬">B%</span> · a b c / d)</span></th>
      <th>누적정확도(10~20)</th>
      <th>누적정확도(전체)</th>
      <th>이유/차이</th>
      <th scope="col" title="키워드 교집합·종목명 언급·ML 확률·예측 순위·확신 구간·25%↑ 테마">예측 신호</th>
    </tr>
  </thead>
  <tbody>
    {% for r in d.rows_compare %}
    <tr data-market="{{ r.market_segment|default('other') }}" data-rise-band="{{ r.rise_band|default('low') }}">
      <td class="col-group" data-sort-col="group" data-sort-value="{% if r.actual_big and (r.pred_high | default(false)) %}3{% elif r.actual_big %}2{% elif r.pred_high | default(false) %}1{% else %}0{% endif %}">
        {% if not (d.forward_observation | default(false)) and r.actual_big %}<span class="pill" style="background:#1e3d2f;color:var(--ok)">실제</span>{% endif %}
        {% if r.pred_high | default(false) %}<span class="pill" style="margin-top:4px;display:inline-block">{% if meta.ranking_mode | default(false) %}고확신{% else %}예측{% endif %}</span>{% elif (r.confidence_tier | default('')) == 'mid' %}<span class="pill" style="margin-top:4px;display:inline-block">중확신</span>{% endif %}
      </td>
      <td data-sort-col="stock" data-sort-value="{{ r.name }} {{ r.code }}">
        {{ stock_name_link(r.code, r.name, r) }}
        {# <div class="pill">{{ r.code }}</div> #}
      </td>
      {% if not (d.forward_observation | default(false)) %}
      <td class="{% if r.actual_big %}ok{% elif r.actual_ret is not none and r.actual_ret < 0 %}bad{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none and r.actual_ret_intraday_pct < 0 %}bad{% endif %}" data-sort-col="actual" data-sort-value="{% if r.actual_cell_pre_close_snapshot | default(false) and r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% elif r.actual_ret is not none %}{{ r.actual_ret }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% endif %}">
        {{ actual_ret_cell_monthly(r, d.forward_observation | default(false)) }}
      </td>
      {% endif %}
      <td class="{% if r.pred_high | default(false) %}warn{% endif %}" style="vertical-align:top" data-sort-col="pred" data-sort-value="{% if r.pred_ret is not none %}{{ r.pred_ret }}{% endif %}">
        {{ pred_ret_cell(d, r) }}
      </td>
      {{ stock_ret_chain_cell(d, r) }}
      {{ pred_rationale_cell(d, r) }}
      <td style="vertical-align:top">
        {% if r.pred_ret is not none and r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
        {% if r.cumulative_accuracy_from_hist | default(false) %}—{% else %}{{ "%.2f"|format(r.pred_ret * r.cumulative_accuracy_avg) }}{% endif %}
        {% else %}—{% endif %}
      </td>
      {{ compact_cumulative_td(r, meta) | safe }}
      <td class="num">
        {% if r.cumulative_accuracy_10_20_avg is defined and r.cumulative_accuracy_10_20_avg is not none %}{{ "%.2f"|format(r.cumulative_accuracy_10_20_avg * 100) }}%{% else %}—{% endif %}
      </td>
      <td class="num">
        {% if r.cumulative_accuracy_all_avg is defined and r.cumulative_accuracy_all_avg is not none %}{{ "%.2f"|format(r.cumulative_accuracy_all_avg * 100) }}%{% else %}—{% endif %}
      </td>
      <td class="pred-reason">
        <span class="gap-tip combo-tip integrate-tip">
          <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 이유, 예측·실제 차이, 상·하락 참고를 함께 보기">통합 보기</span>
          <div class="gap-tip-popup combo-tip-popup integrate-tip-popup" role="tooltip">
            <div class="combo-tip-inner">
              <div class="combo-tip-col">
                <h4 class="combo-tip-h">예측 이유</h4>
                <div class="combo-tip-body">{{ r.pred_reason_detail_html | default('') | safe }}</div>
              </div>
              <div class="combo-tip-col">
                <h4 class="combo-tip-h">예측·실제 차이</h4>
                <div class="combo-tip-body">
                  {% if r.gap_analysis_html %}
                  {{ r.gap_analysis_html | safe }}
                  {% else %}
                  <p class="combo-tip-empty">해당 설명이 없습니다.</p>
                  {% endif %}
                </div>
              </div>
              <div class="combo-tip-rise">
                <h4 class="combo-tip-h">상·하락 참고 (특징·추세·수급·시장·의견)</h4>
                <div class="combo-tip-body">{{ r.rise_reason_html | default('') | safe }}</div>
              </div>
            </div>
          </div>
        </span>
        <span style="margin-left:10px">{{ disclosure_tip(r, d.trading_day) }}</span>
        <span class="pred-reason-plain" style="margin-left:10px">{% if not (d.forward_observation | default(false)) %}{{ r.pred_reason_hit_line | default(r.pred_reason_summary) | default('—') | safe }}{% endif %}</span>
      </td>
      <td>{{ prediction_signal_cell(r) }}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
{% else %}
<p class="sub">{% if d.forward_observation | default(false) %}예측 전용 거래일 — 상위 예측 후보가 없습니다(모델·키워드 필터 결과).{% else %}해당일 실제·예측 {{ meta.threshold }} 이상 해당 종목 없음{% endif %}{% if empty_extra %} ({{ empty_extra }}){% endif %}.</p>
{% endif %}
{%- endmacro %}

  <h1>{{ title }}</h1>
  <p class="sub">
    {% if meta.run_subtitle %}{{ meta.run_subtitle }} · {% endif %}
    급등 기준 {{ meta.threshold }} · 뉴스: {{ meta.news_source }}
    {% if meta.use_decision_cutoff %} · 예측 입력 뉴스 N-1일 {{ meta.cutoff_kst }}(KST)까지{% endif %}
  </p>

  {% if meta.movers_data_note %}
  <p class="movers-data-note">{{ meta.movers_data_note }}</p>
  {% endif %}

  {% if week_note %}
  <p class="sub" style="margin-top:-6px">{{ week_note }}</p>
  {% endif %}

  {% if week_tabs_stack_days and week_panels %}
  <section class="tabs-wrap week-tabs-wrap">
    <h2>주간별 (탭 · ISO 주 월요일 기준)</h2>
    <p class="sub" style="margin-top:0">각 탭은 한 주(월~금)를 <strong>월요일 날짜</strong>로 묶었습니다. 탭 안에서는 해당 주의 거래일을 <strong>일자 순</strong>으로 위에서 아래에 표시합니다. 앵커: <code>#day-YYYY-MM-DD</code></p>
    {{ week_tabs_bar(week_panels) }}
    {% for w in week_panels %}
    <div class="tab-panel{% if loop.first %} active{% endif %}" role="tabpanel" data-tab-panel="{{ loop.index0 }}" data-chart-view-end="{{ w.chart_view_end }}">
      {% for day in w.days %}
      {% if day.preserved_html %}
      {{ day.preserved_html | safe }}
      {% else %}
      {% set d = day.report %}
      <section class="day-stack day-market-block{% if d.forward_observation | default(false) %} day-forward-obs{% endif %}" id="day-{{ d.trading_day.isoformat() }}">
        <div class="day-heading-row">
          <h2>{{ d.trading_day.isoformat() }}{% if d.forward_observation | default(false) %} <span class="pill" style="font-size:0.72rem;font-weight:500;color:var(--warn)">예측 전용</span>{% endif %}</h2>
          {{ market_filter_radios(d.trading_day.isoformat() ~ "-" ~ w.monday.isoformat(), d.forward_observation | default(false)) }}
        </div>
        {{ day_pred_accuracy_banner(d, meta) }}
        {{ market_theme_panel(d) }}
        {{ forward_pred_rationale_panel(d, meta) }}
        {{ hit_at_k_panel(d, meta) }}
        {{ compact_day_table(d, meta) }}
      </section>
      {% endif %}
      {% endfor %}
    </div>
    {% endfor %}
    {{ week_tabs_bar(week_panels, 'tab-bar-bottom') }}
  </section>
  <script>
  (function () {
    var wrap = document.querySelector(".week-tabs-wrap");
    if (!wrap) return;
    var bars = wrap.querySelectorAll(":scope > .tab-bar");
    if (!bars.length) bars = wrap.querySelectorAll(".tab-bar");
    var panels = wrap.querySelectorAll(":scope > .tab-panel");
    if (!panels.length) panels = wrap.querySelectorAll(".tab-panel");
    function show(i) {
      bars.forEach(function (bar) {
        bar.querySelectorAll(":scope > .tab-btn").forEach(function (b, j) {
          b.classList.toggle("active", j === i);
          b.setAttribute("aria-selected", j === i ? "true" : "false");
        });
      });
      panels.forEach(function (p, j) { p.classList.toggle("active", j === i); });
    }
    bars.forEach(function (bar) {
      bar.querySelectorAll(":scope > .tab-btn").forEach(function (b, i) {
        b.addEventListener("click", function () { show(i); });
      });
    });
  })();
  </script>
  {% elif stack_days %}
  <p class="sub" style="margin-top:0">거래일을 <strong>일자 순</strong>으로 위에서 아래에 이어 붙였습니다. 앵커: <code>#day-YYYY-MM-DD</code></p>
  {% for d in days %}
  <section class="day-stack day-market-block" id="day-{{ d.trading_day.isoformat() }}">
    <div class="day-heading-row">
      <h2>{{ d.trading_day.isoformat() }}</h2>
      {{ market_filter_radios(d.trading_day.isoformat()) }}
    </div>
    {{ forward_pred_rationale_panel(d, meta) }}
    {{ hit_at_k_panel(d, meta) }}
    {{ compact_day_table(d, meta) }}
  </section>
  {% endfor %}
  {% elif days|length > 1 %}
  <section class="tabs-wrap">
    <h2>거래일별 (탭)</h2>
    <p class="sub" style="margin-top:0">각 탭: <strong>실제</strong> {{ meta.threshold }} 이상 급등 종목 + 모델 <strong>예측</strong> {{ meta.threshold }} 이상 후보(상위 예측·중복 제외).</p>
    <div class="tab-bar" role="tablist">
      {% for d in days %}
      <button type="button" class="tab-btn{% if loop.first %} active{% endif %}" role="tab"
              aria-selected="{{ 'true' if loop.first else 'false' }}" data-tab-idx="{{ loop.index0 }}">{{ d.trading_day.isoformat() }}</button>
      {% endfor %}
    </div>
    {% for d in days %}
    <div class="tab-panel{% if loop.first %} active{% endif %}" role="tabpanel" data-tab-panel="{{ loop.index0 }}">
      <div class="day-market-block">
        <div class="day-heading-row">
          <h2>{{ d.trading_day.isoformat() }}</h2>
          {{ market_filter_radios(d.trading_day.isoformat() ~ "-daytab-" ~ loop.index0|string) }}
        </div>
        {{ forward_pred_rationale_panel(d, meta) }}
        {{ hit_at_k_panel(d, meta) }}
        {{ compact_day_table(d, meta) }}
      </div>
    </div>
    {% endfor %}
  </section>
  <script>
  (function () {
    var bar = document.querySelector(".tabs-wrap .tab-bar");
    if (!bar) return;
    var wrap = bar.closest(".tabs-wrap");
    var btns = bar.querySelectorAll(".tab-btn");
    var panels = wrap.querySelectorAll(".tab-panel");
    function show(i) {
      btns.forEach(function (b, j) {
        b.classList.toggle("active", j === i);
        b.setAttribute("aria-selected", j === i ? "true" : "false");
      });
      panels.forEach(function (p, j) { p.classList.toggle("active", j === i); });
    }
    btns.forEach(function (b, i) { b.addEventListener("click", function () { show(i); }); });
  })();
  </script>
  {% else %}
  {% for d in days %}
  <section class="day-market-block">
    <div class="day-heading-row">
      <h2>{{ d.trading_day.isoformat() }} · 실제·예측 {{ meta.threshold }} 이상</h2>
      {{ market_filter_radios(d.trading_day.isoformat() ~ "-single") }}
    </div>
    {{ forward_pred_rationale_panel(d, meta) }}
    {{ hit_at_k_panel(d, meta) }}
    {{ compact_day_table(d, meta, '장 전 실행 시 데이터 없음') }}
  </section>
  {% endfor %}
  {% endif %}

  <p class="note">
    실제≥{{ meta.threshold }}·예측≥{{ meta.threshold }} 뱃지로 행 구분. 종목명 클릭 시 네이버 차트. 예측만 해당인 종목은 상위 예측(top_n) 중 예측 상승률 기준이며, 실제 급등과 겹치면 한 행으로 합칩니다.
  </p>
{{ interaction_snippet | safe }}
</body>
</html>
"""

_INDEX_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko" data-live-quotes-base="http://127.0.0.1:8765">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{{ title }}</title>
  <style>
    :root { --bg: #0f1419; --card: #1a2332; --text: #e7ecf3; --muted: #8b9cb3; --accent: #3d9cf5; }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.55; }
    h1 { font-size: 1.5rem; }
    .sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 1.25rem; }
    ul { list-style: none; padding: 0; margin: 0; }
    li { margin-bottom: 10px; }
    a { color: var(--accent); text-decoration: none; font-weight: 600; }
    a:hover { text-decoration: underline; }
    section { background: var(--card); border-radius: 12px; padding: 20px; border: 1px solid #243044; }
  </style>
</head>
<body>
  <h1>{{ title }}</h1>
  <p class="sub">월별 파일을 열면 <strong>ISO 주(월요일 기준)</strong> 단위 탭으로 구분되고, 탭 안에서는 거래일이 일자 순으로 나열됩니다.</p>
  <section>
    <ul>
      {% for href, label in week_links %}
      <li><a href="{{ href }}">{{ label }}</a></li>
      {% endfor %}
    </ul>
  </section>
</body>
</html>
"""

_DATED_N_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ko" data-live-quotes-base="http://127.0.0.1:8765">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>N={{ n_day.isoformat() }} · T={{ t_day.isoformat() }} 리포트</title>
  <style>
    :root {
      --bg: #0c1017; --card: #151d2a; --text: #e8edf5; --muted: #8b9cb3;
      --accent: #4da3ff; --ok: #3ecf8e; --bad: #f07178; --warn: #e6c07b;
      --banner: #1a2740;
    }
    body { font-family: "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
           background: var(--bg); color: var(--text); margin: 0; padding: 24px; line-height: 1.55; }
    h1 { font-size: 1.5rem; margin-bottom: 0.4rem; }
    h2 { font-size: 1.12rem; color: var(--accent); margin: 0 0 12px 0; }
    h3 { font-size: 1.05rem; margin: 0 0 8px 0; }
    h4 { font-size: 0.92rem; color: #a8c7ef; margin: 0 0 6px 0; }
    .sub, .hint, .muted { color: var(--muted); font-size: 0.86rem; }
    .hint { margin: 0 0 8px 0; line-height: 1.45; }
    .muted { font-style: italic; }
    section { background: var(--card); border-radius: 14px; padding: 20px 22px; margin-bottom: 20px;
              border: 1px solid #243044; }
    .banner { background: var(--banner); border-left: 4px solid var(--accent); padding: 14px 16px;
              border-radius: 10px; margin-bottom: 18px; font-size: 0.92rem; line-height: 1.55; }
    .banner.hist { border-left-color: var(--ok); }
    .pill { display: inline-block; padding: 2px 8px; border-radius: 8px; font-size: 0.72rem;
            margin: 2px 4px 2px 0; background: #243044; color: var(--muted); }
    .table-wrap { overflow-x: auto; margin-top: 10px; -webkit-overflow-scrolling: touch; }
    table.rows-compare { width: 100%; border-collapse: collapse; font-size: 0.86rem; min-width: 720px; table-layout: fixed; }
    table.rows-compare col.col-group,
    table.rows-compare th.col-group,
    table.rows-compare td.col-group,
    table.rows-compare th:nth-child(1),
    table.rows-compare td:nth-child(1) {
      width: 5.75rem; min-width: 5.75rem; max-width: 5.75rem;
    }
    table.rows-compare td.col-group,
    table.rows-compare td:nth-child(1) { white-space: nowrap; vertical-align: top; }
    table.rows-compare th, table.rows-compare td {
      padding: 10px 8px; text-align: left; border-bottom: 1px solid #2a3548; vertical-align: top;
    }
    table.rows-compare th { color: var(--muted); font-weight: 600; white-space: nowrap; }
    table.rows-compare tr:hover td { background: #1a2434; }
    table.rows-compare td.td-center { text-align: center; vertical-align: middle; }
    table.rows-compare td.num { font-variant-numeric: tabular-nums; white-space: nowrap; }
    table.rows-compare td.ok { color: var(--ok); font-weight: 600; }
    table.rows-compare td.bad { color: var(--bad); }
    table.rows-compare td.warn { color: var(--warn); font-weight: 600; }
    ul.nl { margin: 0; padding-left: 18px; font-size: 0.84rem; }
    ul.nl li { margin-bottom: 8px; }
    a { color: var(--accent); text-decoration: none; font-weight: 600; }
    a:hover { text-decoration: underline; }
    .reasons { margin-top: 12px; padding-top: 12px; border-top: 1px solid #2a3548; font-size: 0.84rem;
               color: var(--muted); line-height: 1.5; }
    .fn-block { margin-top: 12px; padding: 12px; background: #131b28; border-radius: 8px;
                border-left: 3px solid var(--bad); font-size: 0.86rem; }
    .gap-tip { position: relative; display: inline-block; vertical-align: middle; }
    .gap-tip.gap-tip-inline { margin-left: 6px; }
    .gap-tip.gap-tip-end .gap-tip-popup { left: auto; right: 0; }
    .gap-tip-trigger {
      cursor: help; border-bottom: 1px dotted var(--accent); color: var(--accent);
      font-size: 0.85rem; font-weight: 600; outline: none; margin-left: 0;
    }
    .gap-tip-trigger:hover, .gap-tip-trigger:focus { color: #7ec4ff; }
    .gap-tip-popup {
      display: none; position: absolute; z-index: 500; left: 0; top: calc(100% + 8px);
      min-width: 380px; max-width: min(920px, 96vw); max-height: 92vh;
      overflow: auto; padding: 14px 16px; background: #1a2838; border: 1px solid #3d6a9e;
      border-radius: 10px; box-shadow: 0 10px 28px rgba(0,0,0,0.5);
      font-size: 0.86rem; line-height: 1.55; color: #d0dce8; text-align: left;
    }
    .gap-tip-popup p { margin: 0 0 8px 0; }
    .gap-tip-popup ul { margin: 6px 0 0 16px; padding: 0; }
    .gap-tip-popup li { margin-bottom: 6px; }
    .gap-tip:not(.integrate-tip):hover .gap-tip-popup,
    .gap-tip:not(.integrate-tip):focus-within .gap-tip-popup { display: block; }
    .gap-tip.integrate-tip:hover .gap-tip-trigger,
    .gap-tip.integrate-tip:focus-within .gap-tip-trigger { color: #7ec4ff; border-bottom-color: #7ec4ff; }
    .gap-tip.integrate-tip:hover .integrate-tip-popup,
    .gap-tip.integrate-tip:focus-within .integrate-tip-popup { display: block !important; }
    .kw-count-tip { display: inline; vertical-align: baseline; margin: 0 1px; }
    .kw-count-tip .gap-tip-trigger { font-size: inherit; font-weight: 700; color: var(--warn); border-bottom-color: var(--warn); }
    .gap-tip.pred-miss-tip .gap-tip-trigger { color: var(--bad); border-bottom-color: rgba(248,113,113,0.85); }
    .gap-tip.pred-miss-tip .gap-tip-trigger:hover,
    .gap-tip.pred-miss-tip .gap-tip-trigger:focus { color: #ffa8a8; border-bottom-color: #ffa8a8; }
    .kw-list-popup { min-width: 280px; max-width: min(520px, 92vw) !important; width: auto !important; max-height: 70vh; z-index: 4100; }
    .integrate-tip-floating .kw-list-popup { z-index: 4100; }
    .integrate-tip-popup.integrate-tip-floating {
      display: block !important;
      position: fixed !important;
      z-index: 4000 !important;
      transform: none !important;
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      max-height: 90vh !important;
      overflow: auto !important;
      box-sizing: border-box;
      padding: 14px 16px !important;
      background: #1a2838;
      border: 1px solid #3d6a9e;
      border-radius: 10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.5);
      text-align: center;
    }
    .integrate-tip-popup.integrate-tip-floating .combo-tip-inner { text-align: left; margin: 0 auto; }
    .integrate-tip-popup.integrate-tip-floating .combo-tip-h { text-align: center; }
    .pred-reason-plain { font-size: 0.86rem; color: var(--muted); line-height: 1.5; display: inline-block; max-width: 100%; vertical-align: middle; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
    .combo-tip { margin-left: 0; vertical-align: middle; white-space: nowrap; }
    .combo-tip-popup {
      width: min(920px, calc(100vw - 24px)) !important;
      max-width: min(920px, calc(100vw - 24px)) !important;
      min-width: 320px !important;
      left: 0 !important;
      right: auto !important;
      transform: none !important;
      top: calc(100% + 8px) !important;
      box-sizing: border-box;
      max-height: 90vh;
      overflow: auto;
      padding: 14px 16px !important;
    }
    .combo-tip-inner {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0 20px;
      align-items: start;
      width: 100%;
    }
    .combo-tip-col { min-width: 0; border-left: 1px solid #2d4a6a; padding-left: 16px; }
    .combo-tip-col:first-child { border-left: none; padding-left: 0; padding-right: 4px; }
    .combo-tip-h {
      margin: 0 0 8px 0;
      font-size: 0.8rem;
      font-weight: 700;
      color: #8ec5f6;
    }
    .combo-tip-body {
      font-size: 0.84rem; line-height: 1.6; color: #d0dce8;
      overflow-wrap: anywhere; word-wrap: break-word; word-break: break-word;
      white-space: normal;
    }
    .combo-tip-body p { margin: 0 0 8px 0; }
    .combo-tip-empty { margin: 0; color: var(--muted); font-style: italic; font-size: 0.84rem; }
    .disclosure-tip-popup { min-width: 300px; max-width: min(560px, 92vw) !important; width: auto !important; }
    .disclosure-tip-list { margin: 6px 0 0; padding-left: 0; list-style: none; }
    .disclosure-tip-list li { margin-bottom: 8px; line-height: 1.45; }
    .disc-kind { font-size: 0.75rem; color: #9fd3ff; margin-right: 4px; }
    .combo-tip-rise {
      grid-column: 1 / -1;
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid #2d4a6a;
      width: 100%;
    }
    .integrate-tip-popup .combo-tip-rise .combo-tip-h { text-align: left; }
    @media (max-width: 760px) {
      .combo-tip-inner { grid-template-columns: 1fr; }
      .combo-tip-col { border-left: none; padding-left: 0; border-top: 1px solid #2d4a6a; padding-top: 12px; margin-top: 12px; }
      .combo-tip-col:first-child { border-top: none; padding-top: 0; margin-top: 0; }
    }
    .pred-reason-cell { max-width: 22em; font-size: 0.84rem; color: var(--muted); line-height: 1.45;
                        overflow-wrap: anywhere; word-break: break-word; }
    .kw-pills { display: inline-flex; flex-wrap: wrap; gap: 4px 6px; align-items: center; vertical-align: middle; }
    .kw-pills .pill { font-size: 0.68rem; padding: 1px 6px; }
    .news-tip-hint { font-size: 0.76rem; color: var(--muted); line-height: 1.4; margin: 0 0 8px 0; font-weight: 500; }
    .movers-data-note { background: #2a1f18; border: 1px solid #8b5a2b; border-radius: 10px;
                        padding: 12px 14px; margin-bottom: 16px; font-size: 0.86rem; line-height: 1.55;
                        color: #e8c9a8; }
    .day-heading-row { display: flex; flex-wrap: wrap; align-items: center; justify-content: flex-start; gap: 8px 14px; margin-bottom: 8px; }
    .day-heading-row h2 { margin: 0; font-size: 1.12rem; }
    .market-filter-radios, .rise-filter-radios { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 14px; font-size: 0.82rem; color: var(--muted); }
    .market-filter-title, .rise-filter-title { font-weight: 600; color: var(--muted); margin-right: 2px; }
    .market-filter-label, .rise-filter-label { cursor: pointer; display: inline-flex; align-items: center; gap: 5px; margin: 0; font-weight: 500; }
    .market-filter-label input, .rise-filter-label input { accent-color: var(--accent); vertical-align: middle; }
    table.rows-compare th.sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th.sortable-col:hover { text-decoration: underline; }
    table.rows-compare th.sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th.sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th .sortable-col { cursor: pointer; user-select: none; color: var(--accent); }
    table.rows-compare th .sortable-col:hover { text-decoration: underline; }
    table.rows-compare th .sortable-col.sort-asc::after { content: " ▲"; font-size: 0.65em; opacity: 0.85; }
    table.rows-compare th .sortable-col.sort-desc::after { content: " ▼"; font-size: 0.65em; opacity: 0.85; }
    td.forward-actual-ret { white-space: nowrap; }
    .forward-ret-chain { white-space: nowrap; }
    .live-intraday-toggle { cursor: pointer; border: 1px solid #3d6a9e; background: #1a2838; color: var(--accent); font-size: 0.78rem; }
    .live-intraday-toggle:hover { border-color: var(--accent); }
    .live-intraday-toggle.active { background: var(--accent); color: #0f1419; border-color: var(--accent); }
    .pred-live-intraday { margin-left: 2px; font-size: 0.92em; color: var(--muted); white-space: nowrap; }
    .pred-live-intraday-pct.ok { color: var(--ok); font-weight: 600; }
    .pred-live-intraday-pct.bad { color: var(--bad); font-weight: 600; }
    .cumulative-accuracy-td { position: relative; }
    .cumulative-accuracy-td .cumulative-sort-keys { position: absolute; left: -9999px; top: 0; width: 1px; height: 1px; overflow: hidden; }
    .gap-tip.cumulative-hist-tip { margin-top: 0; vertical-align: middle; }
    .gap-tip.cumulative-hist-tip .gap-tip-trigger {
      font-size: inherit;
      font-variant-numeric: tabular-nums;
    }
    .gap-tip.cumulative-hist-tip .gap-tip-popup.cumulative-hist-popup {
      min-width: 280px;
      max-width: min(440px, 94vw);
      z-index: 600;
    }
    .stock-chart-tip { position: relative; display: inline-block; vertical-align: baseline; max-width: 100%; }
    .stock-chart-tip .stock { position: relative; z-index: 1; }
    .stock-chart-popup {
      display: none; position: absolute; left: 0; top: calc(100% + 6px); z-index: 850;
      padding: 12px 14px; background: #1a2838; border: 1px solid #3d6a9e; border-radius: 10px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.55);
      min-width: min(720px, calc(100vw - 32px)); max-width: min(720px, calc(100vw - 24px));
      box-sizing: border-box;
    }
    .stock-chart-tip:hover .stock-chart-popup,
    .stock-chart-tip:focus-within .stock-chart-popup { display: block; }
    .stock-chart-frame { position: relative; display: block; width: 100%; max-width: 700px; }
    .stock-chart-frame .stock-chart-img {
      display: block; width: 100%; max-width: 700px; height: auto;
      background: #0f1419; border-radius: 6px;
    }
    .stock-chart-n-marker {
      display: none; position: absolute; width: 2px; margin-left: -1px;
      background: #e6c07b; box-shadow: 0 0 8px rgba(230, 192, 123, 0.9);
      pointer-events: none; z-index: 2;
    }
    .stock-chart-n-label {
      display: none; position: absolute; transform: translate(-50%, 0);
      font-size: 0.66rem; font-weight: 700; line-height: 1.2;
      color: #1a1a1a; background: #e6c07b; padding: 1px 5px; border-radius: 3px;
      pointer-events: none; z-index: 3; white-space: nowrap;
    }
    .stock-chart-frame.has-n-marker .stock-chart-n-marker,
    .stock-chart-frame.has-n-marker .stock-chart-n-label { display: block; }
    .stock-chart-caption { display: block; margin-top: 8px; font-size: 0.74rem; color: var(--muted); text-align: center; line-height: 1.4; }
    .stock-ret-lines {
      display: flex; flex-direction: row; flex-wrap: wrap; gap: 0 1.25em; justify-content: center; align-items: center;
      margin-bottom: 10px; padding: 8px 10px; background: #151c24; border-radius: 6px;
      font-size: 0.78rem; font-variant-numeric: tabular-nums; line-height: 1.45;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .stock-ret-line { white-space: nowrap; }
    .stock-ret-lbl { color: var(--text); font-weight: 600; }
    .stock-ret-line.ok .stock-ret-pct { color: var(--ok); }
    .stock-ret-line.bad .stock-ret-pct { color: var(--bad); }
    .stock-ret-dt { color: var(--muted); }
    .stock-ret-col-lines {
      display: flex; flex-direction: column; gap: 3px;
      font-size: 0.74rem; font-variant-numeric: tabular-nums; line-height: 1.35;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      min-width: 8.5rem;
    }
    .stock-ret-col-lines .stock-ret-line { display: flex; justify-content: space-between; gap: 10px; }
    .pred-reason-inline { font-size: 0.82rem; line-height: 1.45; white-space: normal; max-width: 28rem; }
  </style>
</head>
<body>
{% macro market_filter_radios(suffix, forward_day=false) -%}
<div class="market-filter-radios" role="radiogroup" aria-label="표 시장 구분">
  <span class="market-filter-title">시장 -</span>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="all" checked="checked"/> 전체</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kospi"/> KOSPI</label>
  <label class="market-filter-label"><input type="radio" name="market-scope-{{ suffix }}" value="kosdaq"/> KOSDAQ</label>
</div>
<div class="rise-filter-radios" role="radiogroup" aria-label="상승률 구간">
  <span class="rise-filter-title">상승률 -</span>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="all" checked="checked"/> 예측후보 전체</label>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="high"/> 20%이상</label>
  <label class="rise-filter-label"><input type="radio" name="rise-scope-{{ suffix }}" value="mid"/> 10%~20%</label>
</div>
{%- endmacro %}
{% macro prediction_signal_cell(r) -%}
{{ format_prediction_signal_cell(r) | safe }}
{%- endmacro %}
{% macro disclosure_tip(r, trading_day=none) -%}
<span class="gap-tip combo-tip disclosure-tip gap-tip-end">
  <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="당일 종목 공시 목록 보기">공시</span>
  <div class="gap-tip-popup combo-tip-popup disclosure-tip-popup" role="tooltip">
    <h4 class="combo-tip-h">{% if trading_day is not none %}종목 공시 · {{ trading_day.isoformat() }}{% else %}종목 공시{% endif %}</h4>
    <ul class="nl disclosure-tip-list">
    {% for h in r.disclosure_hits|default([]) %}
      <li>
        {% if h.day %}<span class="pill">{{ h.day }}</span>{% endif %}
        <code class="disc-kind">{{ h.kind }}</code>
        {% if h.link %}
        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
        {% else %}
        {{ h.title }}
        {% endif %}
      </li>
    {% else %}
      <li class="muted">{% if trading_day is not none %}이 거래일({{ trading_day.isoformat() }})에 등록된 공시가 없습니다.{% else %}공시 목록이 없습니다.{% endif %}</li>
    {% endfor %}
    </ul>
  </div>
</span>
{%- endmacro %}
{% macro day_pred_accuracy_banner(d, meta) -%}
{% set s = d.pred_accuracy_summary | default(none) %}
{% if s and (s.n_pred_high | default(0)) > 0 and (s.n_with_actual | default(0)) > 0 %}
<p class="sub" style="margin:6px 0 10px;padding:8px 10px;background:#1a2230;border:1px solid #3a4a5c;border-radius:6px;font-size:0.88rem;line-height:1.45">
  <strong>당일 예측≥{{ meta.threshold }}</strong> {{ s.n_pred_high }}건 · 실적 확정 {{ s.n_with_actual }}건 ·
  임계 적중 <strong class="{% if (s.n_hit_threshold | default(0)) == 0 %}bad{% else %}ok{% endif %}">{{ s.n_hit_threshold }}</strong>/{{ s.n_with_actual }}
  {% if s.mean_accuracy_ratio is not none %}
  · 당일 달성률 <strong class="{% if s.mean_accuracy_ratio < 0.01 %}bad{% endif %}">{{ "%.2f"|format(s.mean_accuracy_ratio * 100) }}%</strong>
  {% endif %}
  {% if s.n_negative | default(0) > 0 %} · <span class="bad">음수 {{ s.n_negative }}건</span>{% endif %}
</p>
{% endif %}
{%- endmacro %}
{% macro forward_pred_rationale_panel(d, meta) -%}
{# 장 마감 전 관측일: 예측 근거는 표 ``예측 근거`` 열에 전문 표시 #}
{%- endmacro %}
{% macro pred_ret_cell(d, r) -%}
{% if r.pred_ret is not none %}{{ "%.2f"|format(r.pred_ret) }}{% else %}—{% endif %}{% if d.forward_observation | default(false) %}<span class="pred-live-intraday" hidden aria-hidden="true"> · <span class="pred-live-intraday-pct" data-stock-code="{{ (r.code|string).zfill(6) }}">…</span>%</span>{% endif %}
{%- endmacro %}
{% macro market_theme_panel(d) -%}
{% if d.market_theme_html or (d.forward_observation | default(false)) %}
<div class="market-theme-ref" style="margin:12px 0 16px;padding:12px 14px;background:#152232;border:1px solid #2a4a6a;border-radius:8px">
  <div class="market-theme-heading-row" style="display:flex;align-items:center;flex-wrap:wrap;gap:8px;margin:0 0 8px">
    <h3 style="font-size:0.95rem;color:var(--ok);margin:0">당일 테마 요약</h3>
    {% if d.forward_observation | default(false) %}
    <button type="button" class="pill live-intraday-toggle" data-live-scope="day-{{ d.trading_day.isoformat() }}" aria-pressed="false" title="클릭 시 예측 상승률 옆에 장중 실시간 등락률을 표시합니다">실시간 상승률</button>
    {% endif %}
  </div>
  {% if d.market_theme_html %}{{ d.market_theme_html | safe }}{% endif %}
</div>
{% endif %}
{%- endmacro %}
{% macro stock_ret_chain_cell(d, r) -%}
{% if d.forward_observation | default(false) or d.show_stock_ret_column | default(false) %}
<td class="stock-ret-chain-col" style="vertical-align:top">{{ format_stock_ret_column_lines(r) | safe }}</td>
{% endif %}
{%- endmacro %}
{% macro pred_rationale_cell(d, r) -%}
<td class="pred-reason-forward" style="vertical-align:top;{% if d.forward_observation | default(false) %}white-space:normal;max-width:28rem;line-height:1.45{% else %}white-space:nowrap{% endif %}">
  {% if r.pred_ret is not none %}
    {% set pred_reason_body = r.pred_reason_tooltip_html or r.pred_reason_detail_html %}
    {% if d.forward_observation | default(false) %}
      {% if pred_reason_body and pred_reason_body != '—' %}
      <div class="pred-reason-inline">{{ pred_reason_body | safe }}</div>
      {% else %}—{% endif %}
    {% else %}
      {% if pred_reason_body and pred_reason_body != '—' %}
      <span class="gap-tip pred-reason-tip">
        <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 근거">근거</span>
        <div class="gap-tip-popup pred-reason-popup" role="tooltip">
          <div class="combo-tip-body">{{ pred_reason_body | safe }}</div>
        </div>
      </span>
      {% else %}—{% endif %}
      {% if r.pred_miss_tooltip_html %}
      <span class="gap-tip pred-miss-tip" style="margin-left:8px">
        <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="틀린 이유">틀린 이유</span>
        <div class="gap-tip-popup pred-miss-popup" role="tooltip">
          <div class="combo-tip-body">{{ r.pred_miss_tooltip_html | safe }}</div>
        </div>
      </span>
      {% endif %}
    {% endif %}
  {% else %}—{% endif %}
</td>
{%- endmacro %}
{% macro stock_name_link(code, name, r=none) -%}
<span class="stock-chart-tip" tabindex="0"{% if code %} data-stock-code="{{ (code|string).zfill(6) }}"{% endif %}>
  <a class="stock" target="_blank" rel="noopener" href="{{ naver_chart_url(code) }}"{# title="클릭: 네이버 차트 · 호버: 일봉·최근 등락률" #}>{{ name }}</a>
  <span class="stock-chart-popup" role="tooltip">
    {% if r and not (r.forward_observation | default(false)) %}{{ format_stock_ret_tooltip_lines(r) | safe }}{% endif %}
    {% set chart_n_day = stock_chart_n_day_iso(r) if r else "" %}
    {% set chart_n_off = stock_chart_n_bar_offset(r) if r else none %}
    <div class="stock-chart-frame"{% if chart_n_day %} data-n-day="{{ chart_n_day }}"{% endif %}{% if chart_n_off is not none %} data-n-offset="{{ chart_n_off }}"{% endif %}>
      <img class="stock-chart-img" data-chart-base="{{ naver_chart_day_img_url(code) }}" alt="{{ name }} 일봉 캔들 차트" width="700" height="289" decoding="async" referrerpolicy="no-referrer-when-downgrade"/>
      <span class="stock-chart-n-marker" aria-hidden="true"></span>
      <span class="stock-chart-n-label" aria-hidden="true">N</span>
    </div>
    <span class="stock-chart-caption">일봉 캔들 · 호버 시 최신{% if chart_n_day %} · 노란 세로선=N({{ chart_n_day[5:7] }}/{{ chart_n_day[8:10] }}){% endif %}</span>
  </span>
</span>
{%- endmacro %}
__ACTUAL_RET_CELL_MACRO_DATED__
  <h1>기준일 N={{ n_day.isoformat() }} → 관측일 T={{ t_day.isoformat() }}</h1>
  <p class="sub">
    {{ meta.run_subtitle }} · 급등 기준 {{ meta.threshold }} · 뉴스: {{ meta.news_source }}
    {% if meta.use_decision_cutoff %} · 예측 입력: T 직전 KRX 거래일 {{ meta.cutoff_kst }}(KST)까지{% endif %}
    · 예측 후보 수 {{ meta.total_preds }}
  </p>

  {% if meta.movers_data_note %}
  <p class="movers-data-note">{{ meta.movers_data_note }}</p>
  {% endif %}

  {% if meta.prediction_only and is_live_n and before_open_n %}
  <div class="banner">
    <strong>당일(N) · 장 시작 전 실행.</strong> 예측 입력 뉴스는 <strong>전 거래일까지</strong> 반영합니다(당일 장중 뉴스는 포함하지 않음).
    <strong>T={{ t_day.isoformat() }}</strong> 관측일 전이거나 장이 끝나기 전이면 <strong>실제 상승률·누적 정확도</strong>는 표에서 <strong>빈 칸</strong>으로 둡니다. 아래는 <strong>예측 상승률 {{ meta.threshold }} 이상 후보</strong> 위주입니다.
  </div>
  {% elif meta.prediction_only and is_live_n %}
  <div class="banner">
    <strong>당일(N) 실행 모드.</strong> 예측에는 위 시각까지 반영된 뉴스가 쓰였습니다.
    <strong>T={{ t_day.isoformat() }}</strong> 가 예측 전용이거나 일봉이 아직 확정되지 않았으면 <strong>실제 상승률</strong>은 빈 칸이거나, 당일 장 마감 전에는 pykrx·네이버 실시간 등락률을 <strong>— (xx%)</strong> 형태로만 참고합니다. <strong>누적 정확도</strong>는 실적이 없으면 빈 칸일 수 있습니다. 표는 <strong>예측/실제 10% 이상 후보</strong>를 포함하며, 라디오로 20%이상 / 10~20%를 전환할 수 있습니다.
    과거 기준일로 다시 실행하면 시장 20%↑ 종목과 예측을 함께 비교할 수 있습니다.
  </div>
  {% elif meta.prediction_only %}
  <div class="banner">
    <strong>예측 전용 리포트.</strong> 비거래일·미래 N·당일 실행 등으로 <strong>실제 상승률·누적 정확도</strong>를 알 수 없으면 표에서 <strong>빈 칸</strong>으로 둡니다.
  </div>
  {% else %}
  <div class="banner hist">
    <strong>과거 N일 기준 리포트.</strong> T일 종가 기준 실제 상승률과 모델 예측을 함께 표시합니다.
    예측 근거는 <em>예측에 사용한 뉴스 구간</em>에서 종목명·키워드가 들어간 기사만 골랐고,
    오른쪽은 <em>컷오프 이후·T일</em> 보도를 참고용으로 묶었습니다(인과 단정 아님).
  </div>
  {% endif %}

  {{ day_pred_accuracy_banner(day, meta) }}
  {{ forward_pred_rationale_panel(day, meta) }}
  {{ market_theme_panel(day) }}

  <section class="day-market-block" id="day-{{ day.trading_day.isoformat() }}">
    <div class="day-heading-row">
      <h2>종목별 상세 <span style="font-size:0.82rem;font-weight:500;color:var(--muted)">(관측일 {{ t_day.isoformat() }})</span></h2>
      {{ market_filter_radios(n_day.strftime("%Y%m%d")) }}
    </div>
    <p class="sub" style="margin-top:0">한 줄이 한 종목입니다. <strong>통합 보기</strong>·<strong>공시</strong>·<strong>뉴스</strong>에 마우스를 올리면 상세를 볼 수 있습니다. <strong>예측 신호</strong> 열은 교집합·ML·순위·<strong>25%↑</strong> 종목 당일 테마 요약입니다.</p>
    {% if day.rows_compare|length > 0 %}
    <div class="table-wrap">
    <table class="rows-compare">
      <colgroup><col class="col-group"/></colgroup>
      <thead>
        <tr>
          <th class="sortable-col col-group" data-sort="group" scope="col" title="구분 우선순위 정렬: 실제+예측 > 실제만 > 예측만">구분</th>
          <th class="sortable-col" data-sort="stock" scope="col" title="종목명/코드 오름차순·내림차순 정렬">종목</th>
          {% if not (day.forward_observation | default(false)) %}
          <th class="sortable-col" data-sort="actual" scope="col" title="종가 확정 후 일봉 기준">실제 상승률(%)</th>
          {% endif %}
          <th class="sortable-col" data-sort="pred" scope="col">예측 상승률(%)</th>
          {% if day.forward_observation | default(false) or day.show_stock_ret_column | default(false) %}
          <th scope="col" title="N-3·N-2·N-1·N 일봉 등락률">N일봉(%)</th>
          {% endif %}
          <th scope="col" title="{% if day.forward_observation | default(false) %}예측 근거 전문{% else %}예측 근거·미적중 시 틀린 이유 tooltip{% endif %}">예측 근거</th>
          <th>보정(%)</th>
          <th scope="col" title="예측≥임계 후보만. 앞: 예측≥임계·실적 확정 건 중 실제≥임계 적중 비율(a/d). vs: 예측≥임계·실적 확정 건 중 실제 0% 이상 비율. 괄호: a=실제≥임계, b=0&lt;실제&lt;임계, c=실제&lt;0, d=예측≥임계·실적 확정 전체 (a b c / d)">누적 정확도<br/><span style="font-size:0.68rem;font-weight:500;color:var(--muted);line-height:1.35;display:block;margin-top:2px">(<span class="sortable-col" data-sort="cumulative_a" title="적중% 정렬">A%</span> <span style="color:var(--muted)">vs</span> <span class="sortable-col" data-sort="cumulative_b" title="실제 0%+ 비율 정렬">B%</span> · a b c / d)</span></th>
          <th>누적정확도(10~20)</th>
          <th>누적정확도(전체)</th>
          <th>통합 보기</th>
          <th>공시</th>
          <th>이유/차이</th>
          <th>뉴스</th>
          <th scope="col" title="키워드 교집합·종목명 언급·ML 확률·예측 순위·확신 구간·25%↑ 테마">예측 신호</th>
        </tr>
      </thead>
      <tbody>
        {% for r in day.rows_compare %}
        <tr id="code-{{ row_id_prefix }}{{ r.code }}" data-market="{{ r.market_segment|default('other') }}" data-rise-band="{{ r.rise_band|default('low') }}">
          <td class="col-group" data-sort-col="group" data-sort-value="{% if (not meta.prediction_only) and r.actual_big and (r.pred_high | default(false)) %}3{% elif (not meta.prediction_only) and r.actual_big %}2{% elif r.pred_high | default(false) %}1{% else %}0{% endif %}">
            {% if not meta.prediction_only and r.actual_big %}<span class="pill" style="background:#1e3d2f;color:var(--ok)">실제≥{{ meta.threshold }}</span>{% endif %}
            {% if r.pred_high | default(false) %}<span class="pill" style="margin-top:4px;display:inline-block;color:var(--warn)">예측≥{{ meta.threshold }}</span>{% endif %}
          </td>
          <td data-sort-col="stock" data-sort-value="{{ r.name }} {{ r.code }}">
            {{ stock_name_link(r.code, r.name, r) }}
            {# <div class="pill">{{ r.code }}</div> #}
          </td>
          {% if not (day.forward_observation | default(false)) %}
          <td class="num {% if not meta.prediction_only and r.actual_big %}ok{% elif r.actual_ret is not none and r.actual_ret < 0 %}bad{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none and r.actual_ret_intraday_pct < 0 %}bad{% endif %}" data-sort-col="actual" data-sort-value="{% if r.actual_cell_pre_close_snapshot | default(false) and r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% elif r.actual_ret is not none %}{{ r.actual_ret }}{% elif r.actual_ret_intraday_pct is defined and r.actual_ret_intraday_pct is not none %}{{ r.actual_ret_intraday_pct / 100.0 }}{% endif %}">
            {{ actual_ret_cell_dated(r, day.forward_observation | default(false)) }}
          </td>
          {% endif %}
          <td class="num {% if r.pred_high | default(false) %}warn{% endif %}" data-sort-col="pred" data-sort-value="{% if r.pred_ret is not none %}{{ r.pred_ret }}{% endif %}">
            {{ pred_ret_cell(day, r) }}
          </td>
          {{ stock_ret_chain_cell(day, r) }}
          {{ pred_rationale_cell(day, r) }}
          <td class="num">
            {% if r.pred_ret is not none and r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
            {% if r.cumulative_accuracy_from_hist | default(false) %}—{% else %}{{ "%.2f"|format(r.pred_ret * r.cumulative_accuracy_avg) }}{% endif %}
            {% else %}—{% endif %}
          </td>
          <td class="num cumulative-accuracy-td" style="white-space:nowrap;font-variant-numeric:tabular-nums">
            {% if r.cumulative_accuracy_avg is defined and r.cumulative_accuracy_avg is not none %}
            <span class="cumulative-sort-keys" aria-hidden="true">
              <span data-sort-col="cumulative_a" data-sort-value="{{ r.cumulative_accuracy_avg }}"></span>
              {% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %}
              <span data-sort-col="cumulative_b" data-sort-value="{{ r.cumulative_nonneg_rate_pct }}"></span>
              {% endif %}
            </span>
            <span class="gap-tip cumulative-hist-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="누적 정확도·{{ meta.threshold }} 이상 예측 이력"{% if meta.cumulative_track_hint is defined %} title="{{ meta.cumulative_track_hint | e }}"{% endif %}>{{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%{% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %} vs {{ "%.2f"|format(r.cumulative_nonneg_rate_pct) }}%{% endif %}{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} : ({{ r.cumulative_hit_x }} {% if r.cumulative_hit_z is defined and r.cumulative_hit_z is not none %}{{ r.cumulative_hit_z }}{% else %}0{% endif %} <span class="bad">{% if r.cumulative_hit_neg is defined and r.cumulative_hit_neg is not none %}{{ r.cumulative_hit_neg }}{% else %}0{% endif %}</span> / {{ r.cumulative_hit_y }}){% endif %}</span>
              <div class="gap-tip-popup cumulative-hist-popup" role="tooltip">
                <div class="combo-tip-body">
                  <strong>관측일 T별 · 예측 ≥ {{ meta.threshold }}</strong>
                  {% if r.pred_high_history|default([])|length > 0 %}
                  <ul class="nl" style="margin:8px 0 0 0">
                  {% for h in r.pred_high_history %}
                    <li><span class="pill">{{ h.t }}</span> 예측 {{ "%.2f"|format(h.pred_pct) }}%
                      {% if h.actual_pct is not none %} · 실제 {{ "%.2f"|format(h.actual_pct) }}%{% else %} · 실적 미확정{% endif %}
                    </li>
                  {% endfor %}
                  </ul>
                  {% else %}
                  <p class="combo-tip-empty" style="margin:8px 0 0 0">저장된 {{ meta.threshold }} 이상 예측 이력이 없습니다.</p>
                  {% endif %}
                  <p style="font-size:0.76rem;color:var(--muted);margin:8px 0 0 0;line-height:1.35">앞 {{ "%.2f"|format(r.cumulative_accuracy_avg * 100) }}%: 예측≥{{ meta.threshold }}·실적 확정 건 중 실제≥{{ meta.threshold }} 적중 비율(맞춘 건수÷전체, a/d와 동일).{% if r.cumulative_nonneg_rate_pct is defined and r.cumulative_nonneg_rate_pct is not none %} vs {{ "%.2f"|format(r.cumulative_nonneg_rate_pct) }}%: 예측≥{{ meta.threshold }}·실적 확정 건 중 실제가 0% 이상인 비율.{% endif %}{% if r.cumulative_hit_x is defined and r.cumulative_hit_x is not none and r.cumulative_hit_y is defined and r.cumulative_hit_y is not none %} 괄호 (a b c / d): 예측≥{{ meta.threshold }}·실적 확정 건 중 a=실제≥{{ meta.threshold }}, b=0&lt;실제&lt;{{ meta.threshold }}, c=실제&lt;0(빨간색), d=전체.{% endif %}</p>
                </div>
              </div>
            </span>
            {% endif %}
          </td>
          <td class="num">
            {% if r.cumulative_accuracy_10_20_avg is defined and r.cumulative_accuracy_10_20_avg is not none %}{{ "%.2f"|format(r.cumulative_accuracy_10_20_avg * 100) }}%{% else %}—{% endif %}
          </td>
          <td class="num">
            {% if r.cumulative_accuracy_all_avg is defined and r.cumulative_accuracy_all_avg is not none %}{{ "%.2f"|format(r.cumulative_accuracy_all_avg * 100) }}%{% else %}—{% endif %}
          </td>
          <td class="td-center">
            <span class="gap-tip combo-tip integrate-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 이유, 예측·실제 차이, 상·하락 참고를 함께 보기">통합 보기</span>
              <div class="gap-tip-popup combo-tip-popup integrate-tip-popup" role="tooltip">
                <div class="combo-tip-inner">
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측 이유</h4>
                    <div class="combo-tip-body">{{ r.pred_reason_detail_html | default('') | safe }}</div>
                  </div>
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">{% if meta.prediction_only %}참고{% else %}예측·실제 차이{% endif %}</h4>
                    <div class="combo-tip-body">
                      {% if r.gap_analysis_html %}
                      {{ r.gap_analysis_html | safe }}
                      {% else %}
                      <p class="combo-tip-empty">해당 설명이 없습니다.</p>
                      {% endif %}
                    </div>
                  </div>
                  <div class="combo-tip-rise">
                    <h4 class="combo-tip-h">상·하락 참고 (특징·추세·수급·시장·의견)</h4>
                    <div class="combo-tip-body">{{ r.rise_reason_html | default('') | safe }}</div>
                  </div>
                </div>
              </div>
            </span>
          </td>
          <td class="td-center">
            {{ disclosure_tip(r, t_day) }}
          </td>
          <td class="pred-reason-cell">{% if not (day.forward_observation | default(false)) %}{{ r.pred_reason_hit_line | default(r.pred_reason_summary) | default('—') | safe }}{% else %}—{% endif %}</td>
          <td class="td-center">
            <span class="gap-tip combo-tip">
              <span class="gap-tip-trigger" tabindex="0" role="button" aria-label="예측 입력 구간 뉴스와 참고 뉴스를 함께 보기">뉴스</span>
              <div class="gap-tip-popup combo-tip-popup" role="tooltip">
                <div class="combo-tip-inner">
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">예측 상승률이 높게 나온 데 기여한 뉴스(예측 입력 구간)</h4>
                    <p class="news-tip-hint">종목명 또는 예측 시 맞춘 키워드가 제목·요약에 포함된 기사입니다.</p>
                    <ul class="nl">
                    {% for h in r.pred_news_hits|default([]) %}
                      <li><span class="pill">{{ h.day.isoformat() }}</span> <code style="font-size:0.75rem;color:var(--warn)">{{ h.matched }}</code>
                        {% if h.link %}
                        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
                        {% else %}
                        {{ h.title }}
                        {% endif %}
                      </li>
                    {% else %}
                      <li class="muted">매칭된 기사 없음(키워드·종목명이 뉴스 제목·요약에 직접 나타난 경우만 표시).</li>
                    {% endfor %}
                    </ul>
                  </div>
                  <div class="combo-tip-col">
                    <h4 class="combo-tip-h">실제 등락과 시기가 겹치는 뉴스(참고)</h4>
                    <p class="news-tip-hint">예측 컷오프 이후 구간 + T({{ t_day.isoformat() }}) 당일 기사 중 같은 방식으로 매칭한 목록입니다. 상승 원인으로 단정하지 않습니다.</p>
                    <ul class="nl">
                    {% for h in r.actual_news_hits|default([]) %}
                      <li><span class="pill">{{ h.day.isoformat() }}</span> <code style="font-size:0.75rem;color:var(--ok)">{{ h.matched }}</code>
                        {% if h.link %}
                        <a href="{{ h.link }}" target="_blank" rel="noopener">{{ h.title }}</a>
                        {% else %}
                        {{ h.title }}
                        {% endif %}
                      </li>
                    {% else %}
                      <li class="muted">매칭된 기사 없음.</li>
                    {% endfor %}
                    </ul>
                  </div>
                </div>
              </div>
            </span>
          </td>
          <td>{{ prediction_signal_cell(r) }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    </div>
    {% else %}
    <p class="sub">실제·예측 {{ meta.threshold }} 이상으로 표에 올린 종목이 없습니다.</p>
    {% endif %}
  </section>

  {% if (not meta.prediction_only) and day.false_negatives and day.false_negatives|length > 0 %}
  <section>
    <h2>예측했으나 실제 음수 수익</h2>
    {% for fn in day.false_negatives %}
    <div class="fn-block">
      <strong>{{ stock_name_link(fn.code, fn.name) }}</strong>
      {# ({{ fn.code }}) · #}
      예측 {{ "%.2f"|format(fn.pred_ret) }}% · 실제
      <span class="bad">{{ "%.2f"|format(fn.actual_ret * 100) }}%</span>
      <p style="margin:8px 0 0 0;color:var(--muted);">{{ fn.analysis_html | default(fn.analysis) | safe }}</p>
    </div>
    {% endfor %}
  </section>
  {% endif %}

  <section>
    <h2>예측 입력 뉴스 제목 샘플</h2>
    <ul class="nl">
      {% for t in day.news_titles_sample[:16] %}
      <li>{{ t }}</li>
      {% else %}
      <li class="muted">없음</li>
      {% endfor %}
    </ul>
  </section>
</body>
</html>
"""

_TEMPLATE = _TEMPLATE.replace("__ACTUAL_RET_CELL_MACRO__", _actual_ret_cell_macro("actual_ret_cell"))
_COMPACT_TEMPLATE = _COMPACT_TEMPLATE.replace(
    "__ACTUAL_RET_CELL_MACRO_MONTHLY__", _actual_ret_cell_macro("actual_ret_cell_monthly")
)
_DATED_N_TEMPLATE = _DATED_N_TEMPLATE.replace(
    "__ACTUAL_RET_CELL_MACRO_DATED__", _actual_ret_cell_macro("actual_ret_cell_dated")
)

_m_dated_style = re.search(r"<style>\s*(.*?)\s*</style>", _DATED_N_TEMPLATE, re.DOTALL)
