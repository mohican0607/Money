"""[deprecated] → ``python scripts/diag_prediction.py rank-hits 20260629``"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.diag_prediction import main

if __name__ == "__main__":
    raise SystemExit(main(["rank-hits", "20260629"]))
