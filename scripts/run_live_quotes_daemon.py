"""
로그온·아침 스케줄용 장중 등락률 데몬.

이미 ``127.0.0.1:8765`` 가 응답하면 즉시 종료합니다.
꺼져 있으면 ``pythonw`` 데몬을 IDE와 분리해 띄운 뒤 이 프로세스는 끝냅니다.
작업 스케줄러·로그온 Run 은 ``pythonw.exe`` 로 이 파일을 호출합니다.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)


def main() -> int:
    from src.report.live_quotes import run_if_needed

    run_if_needed()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
