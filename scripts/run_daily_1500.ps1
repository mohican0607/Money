#Requires -Version 5.1
<#
.SYNOPSIS
  KRX 거래일 장 마감 후(15:40) 자동 실행용: 완성 일봉으로 N+1 예측 리포트를 생성합니다.

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
# 장후 N+1 예측 + 마감된 「예측 전용」 일자 확정 갱신
$env:ML_REUSE_PRIOR_FOR_FORWARD = "1"
$env:PIPELINE_AUTO_SUPPLEMENT_STALE_FORWARD = "1"
if (-not $env:MONEY_TEMP_DIR) { $env:MONEY_TEMP_DIR = "F:\temp\Money\tmp" }
if (-not $env:MONEY_CACHE_DIR) { $env:MONEY_CACHE_DIR = "F:\temp\Money\cache" }
$env:TEMP = $env:MONEY_TEMP_DIR
$env:TMP = $env:MONEY_TEMP_DIR
$env:JOBLIB_TEMP_FOLDER = $env:MONEY_TEMP_DIR
New-Item -ItemType Directory -Force -Path $env:MONEY_TEMP_DIR, $env:MONEY_CACHE_DIR | Out-Null

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

# 학습은 완성된 N일 일봉을 사용한다. 15:30 이전 부분 일봉으로 실행하면
# train/serve skew가 생기므로, 기존 14:30~15:00 스케줄도 15:40까지 기다린다.
$now = Get-Date
$postClose = Get-Date -Hour 15 -Minute 40 -Second 0
if ($now -lt $postClose) {
    $waitSeconds = [int][Math]::Ceiling(($postClose - $now).TotalSeconds)
    Write-Host "[run_daily_1500] 완성 일봉 대기: $waitSeconds 초 (15:40 실행)"
    Start-Sleep -Seconds $waitSeconds
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
