import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.import_freeze_from_report import _section_html, parse_day_section

path = Path(sys.argv[1])
day_id = sys.argv[2] if len(sys.argv) > 2 else "day-2026-07-06"
html = path.read_text(encoding="utf-8")
sec = _section_html(html, day_id)
tbody = re.search(r"<tbody>(.*?)</tbody>", sec, re.S)
rows = re.findall(r"<tr[^>]*>", tbody.group(1) if tbody else "")
codes = re.findall(r'data-stock-code="([^"]+)"', sec)
items = parse_day_section(sec)
print(path.name, "tr", len(rows), "parsed_pred", len(items), "stock_codes", len(codes))
for x in items:
    print(f"  {x['code']} {x['name']} {x['predicted_return_pct']:.2f}")
