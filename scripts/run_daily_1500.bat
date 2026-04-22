@echo off
REM 거래일에만 main.py 실행 후 output 폴더·리포트 HTML 자동 오픈 (run_daily_1500.ps1, 15:00 스케줄 예시)
cd /d "%~dp0.."
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_daily_1500.ps1" -RepoRoot "%~dp0.."
exit /b %ERRORLEVEL%
