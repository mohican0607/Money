#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 15:00 자동 실행용: MOCK_NEWS=0 으로 main.py 실행 후(기본) 생성 리포트 HTML만 자동 오픈(main.py 동작).

.DESCRIPTION
  - 오늘이 XKRX 거래일이 아니면 종료(스케줄러는 성공 코드).
  - T = N 다음 거래일이 관측일인데, T도 거래일인지 한 번 더 확인(휴장만 있는 달 등 예외 대비).
  - 완료 후 리포트 열기는 main.py가 담당(NO_AUTO_OPEN_OUTPUT=1 이면 생략).
#>
param(
    [string] $RepoRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"
$OutputDir = Join-Path $RepoRoot "output"
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$MainPy = Join-Path $RepoRoot "main.py"

if (-not (Test-Path $PythonExe)) {
    Write-Error "가상환경 Python 없음: $PythonExe"
    exit 1
}

Set-Location -LiteralPath $RepoRoot

$env:MOCK_NEWS = "0"
$env:PYTHONUTF8 = "1"

$CheckPy = Join-Path $PSScriptRoot "check_trading_day_for_daily.py"
& $PythonExe $CheckPy
$code = $LASTEXITCODE
if ($code -eq 2) {
    Write-Host "[run_daily_1500] 오늘은 거래일이 아니어서 건너뜀."
    exit 0
}
if ($code -eq 3) {
    Write-Host "[run_daily_1500] 다음 거래일(T)이 휴장으로만 잡혀 건너뜀."
    exit 0
}
if ($code -eq 4) {
    Write-Host "[run_daily_1500] 다음 거래일을 찾을 수 없어 건너뜀."
    exit 0
}
if ($code -ne 0) {
    Write-Error "거래일 검사 실패 (exit $code)"
    exit $code
}

& $PythonExe $MainPy
if ($LASTEXITCODE -ne 0) {
    Write-Error "main.py 실패 (exit $LASTEXITCODE)"
    exit $LASTEXITCODE
}

if (-not (Test-Path -LiteralPath $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

$todayTag = Get-Date -Format "yyyyMMdd"
$reportPath = Join-Path $OutputDir "report_$todayTag.html"

if (-not (Test-Path -LiteralPath $reportPath)) {
    Write-Warning "리포트 파일이 아직 없음: $reportPath"
    exit 0
}

Write-Host "[run_daily_1500] 완료: $reportPath (브라우저 열기는 main.py 기본 동작)"
exit 0
