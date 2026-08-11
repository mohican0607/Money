#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 15:00 — 14:30 실행 보강 (main.py --append-rebuild-learning --n-day N).

.DESCRIPTION
  - 14:30 스케줄이 끝난 뒤 freeze·리포트를 다시 확인·갱신 (이메일 없음).
  - 14:30 lock 이 남아 있으면 최대 25분 대기.
  - 스케줄러: register_daily_email_tasks.ps1 의 MoneyKRX_Daily1500_Append.
#>
param(
    [string] $RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [switch] $ShowWindow
)

$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$PythonW = Join-Path $RepoRoot ".venv\Scripts\pythonw.exe"
$RunnerPy = Join-Path $RepoRoot "scripts\run_daily_email.py"

if (-not (Test-Path $PythonExe)) {
    Write-Error "가상환경 Python 없음: $PythonExe"
    exit 1
}

Set-Location -LiteralPath $RepoRoot
if ($ShowWindow) {
    & $PythonExe $RunnerPy --slot 1500
} else {
    if (-not (Test-Path $PythonW)) {
        Write-Error "pythonw.exe 없음: $PythonW"
        exit 1
    }
    & $PythonW $RunnerPy --slot 1500
}
exit $LASTEXITCODE
