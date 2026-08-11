#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 15:30 — N→N+1 예측(main.py) 실행 후 이메일 발송.

.DESCRIPTION
  - 오늘이 거래일이 아니면 종료(스케줄러는 성공 코드).
  - 장마감(15:30) 직후 완성 일봉·뉴스 기준 리포트.
#>
param(
    [string] $RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [switch] $ShowWindow
)

$ErrorActionPreference = "Stop"
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$PythonW = Join-Path $RepoRoot ".venv\Scripts\pythonw.exe"
$RunnerPy = Join-Path $RepoRoot "scripts\run_daily_email.py"

if (-not (Test-Path $PythonExe)) {
    Write-Error "가상환경 Python 없음: $PythonExe"
    exit 1
}

Set-Location -LiteralPath $RepoRoot
if ($ShowWindow) {
    & $PythonExe $RunnerPy --slot 1530
} else {
    if (-not (Test-Path $PythonW)) {
        Write-Error "pythonw.exe 없음: $PythonW"
        exit 1
    }
    & $PythonW $RunnerPy --slot 1530
}
exit $LASTEXITCODE
