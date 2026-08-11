#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 16:00 — 15:30 실행 이후 보강 (main.py --append-rebuild-learning --n-day N).

.DESCRIPTION
  - 14:30·15:30 선행 실행 lock 종료 후 freeze·리포트·학습 보강 (이메일 없음).
  - 스케줄러: register_daily_email_tasks.ps1 의 MoneyKRX_Daily1600_Append.
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
    & $PythonExe $RunnerPy --slot 1600
} else {
    if (-not (Test-Path $PythonW)) {
        Write-Error "pythonw.exe 없음: $PythonW"
        exit 1
    }
    & $PythonW $RunnerPy --slot 1600
}
exit $LASTEXITCODE
