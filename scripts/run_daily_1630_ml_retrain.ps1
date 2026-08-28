#Requires -Version 5.1
<#
.SYNOPSIS
  ML joblib 강제 재학습만 수동 재실행 (main.py --force-ml-retrain T).

.DESCRIPTION
  - 16:00 append·연쇄 ML 이 끝난 뒤 lock 해제 후에만 실행됩니다.
  - 정상 흐름은 16:00 슬롯이 append 성공 직후 자동으로 ML 재학습합니다.
  - 스케줄러 등록 대상이 아닙니다(실패 시 재시도용).
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
