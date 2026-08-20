#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 16:00 — 장마감된 당일(T=오늘) 학습 진단 병합
  (main.py --append-rebuild-learning YYYYMMDD, 리포트 HTML 미작성).

.DESCRIPTION
  - 15:30 선행 lock 종료 후, rebuild_learning·스냅샷만 갱신하고 이메일을 발송합니다.
  - 리포트 HTML 은 첨부하지 않습니다(15:30에서 이미 갱신·발송).
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
