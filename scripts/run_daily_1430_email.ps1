#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 14:30 — N→N+1 예측(main.py) 실행 후 이메일 발송.

.DESCRIPTION
  - 오늘이 거래일이 아니면 종료(스케줄러는 성공 코드).
  - .env 의 EMAIL_* 설정 필요(SMTP 계정·수신자).
#>
param(
    [string] $RepoRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$RunnerPy = Join-Path $RepoRoot "scripts\run_daily_email.py"

if (-not (Test-Path $PythonExe)) {
    Write-Error "가상환경 Python 없음: $PythonExe"
    exit 1
}

Set-Location -LiteralPath $RepoRoot
& $PythonExe $RunnerPy --slot 1430
exit $LASTEXITCODE
