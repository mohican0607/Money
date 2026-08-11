#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 14:30 — N→N+1 예측(main.py) 실행 후 이메일 발송.

.DESCRIPTION
  - 오늘이 거래일이 아니면 종료(스케줄러는 성공 코드).
  - .env 의 EMAIL_* 설정 필요(SMTP 계정·수신자).
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
# 스케줄러: register_daily_email_tasks.ps1 가 pythonw 로 직접 호출(창 없음).
# 수동 실행 시 -ShowWindow 로 콘솔 로그 확인 가능.
if ($ShowWindow) {
    & $PythonExe $RunnerPy --slot 1430
} else {
    if (-not (Test-Path $PythonW)) {
        Write-Error "pythonw.exe 없음: $PythonW"
        exit 1
    }
    & $PythonW $RunnerPy --slot 1430
}
exit $LASTEXITCODE
