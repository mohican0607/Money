#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 16:00 — 장마감된 당일(T=오늘) 확정 (main.py --append-rebuild-learning YYYYMMDD).

.DESCRIPTION
  - 15:30 선행 lock 종료 후, 당일 관측 블록을 actual·테마로 확정 후 이메일 발송.
  - 고·중 확신 없는 none-tier 더미는 신규 freeze에 채우지 않음.
  - 스케줄러: MoneyKRX_Daily1600_Append
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
