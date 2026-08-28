#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 16:30 — 16:00 append 이후 ML joblib 강제 재학습
  (main.py --force-ml-retrain T, 리포트 HTML 미작성).

.DESCRIPTION
  - 16:00 선행 lock 종료 후, 다음 거래일 T용 ML 모델만 재학습하고 이메일을 발송합니다.
  - 리포트 HTML 은 첨부하지 않습니다.
  - 스케줄러: MoneyKRX_Daily1630_MLRetrain
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
    & $PythonExe $RunnerPy --slot 1630
} else {
    if (-not (Test-Path $PythonW)) {
        Write-Error "pythonw.exe 없음: $PythonW"
        exit 1
    }
    & $PythonW $RunnerPy --slot 1630
}
exit $LASTEXITCODE
