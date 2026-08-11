# 작업 스케줄러에 거래일 14:30 / 15:30 이메일 리포트 등록
#
# - 매일 14:30: N→N+1 예측 + 이메일
# - 매일 15:30: N→N+1 예측 + 이메일 (장마감 직후 actual·테마 갱신)
# - 토·일·공휴일은 스크립트가 즉시 종료(exit 0)
# - PowerShell 창 없이 pythonw.exe 로 실행 (콘솔 클릭으로 멈추는 문제 방지)
#
# 사용 전 .env 에 EMAIL_SMTP_* / EMAIL_RECIPIENTS 를 설정하세요.
#
# 사용:
#   powershell -ExecutionPolicy Bypass -File scripts\register_daily_email_tasks.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\register_daily_email_tasks.ps1 -UpdateOnly

param(
    [switch] $UpdateOnly
)

$Repo = if ($PSScriptRoot) { Split-Path -Parent $PSScriptRoot } else { "E:\Git\Money" }
$PythonW = Join-Path $Repo ".venv\Scripts\pythonw.exe"
$RunnerPy = Join-Path $Repo "scripts\run_daily_email.py"

if (-not (Test-Path -LiteralPath $PythonW)) {
    Write-Error "pythonw.exe 없음: $PythonW (가상환경 생성 후 다시 실행)"
    exit 1
}

function New-DailyEmailTaskTr([string] $Slot) {
    # schtasks /TR: pythonw — 콘솔 창 없음. stdout/stderr는 run_daily_email.py 가 파일·이메일로 처리.
    return "`"$PythonW`" `"$RunnerPy`" --slot $Slot"
}

$Tr1430 = New-DailyEmailTaskTr "1430"
$Tr1530 = New-DailyEmailTaskTr "1530"

$Tasks = @(
    @{ Name = "MoneyKRX_Daily1430_Email"; Time = "14:30"; Tr = $Tr1430 }
    @{ Name = "MoneyKRX_Daily1530_Email"; Time = "15:30"; Tr = $Tr1530 }
)

foreach ($t in $Tasks) {
    $exists = schtasks /Query /TN $t.Name 2>$null
    if ($LASTEXITCODE -eq 0) {
        schtasks /Change /TN $t.Name /TR $t.Tr | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "작업 변경 실패: $($t.Name)"
            exit 1
        }
        Write-Host "변경: $($t.Name) -> pythonw (창 없음)"
    }
    elseif (-not $UpdateOnly) {
        schtasks /Create /TN $t.Name /TR $t.Tr /SC DAILY /ST $t.Time /RL HIGHEST /F | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "작업 등록 실패: $($t.Name)"
            exit 1
        }
        Write-Host "등록: $($t.Name) (매일 $($t.Time)) -> pythonw (창 없음)"
    }
    else {
        Write-Warning "작업 없음(건너뜀): $($t.Name)"
    }
}

Write-Host ""
Write-Host "확인: schtasks /Query /TN MoneyKRX_Daily1430_Email /V /FO LIST"
Write-Host "로그: scripts/logs/run_daily_YYYYMMDD.log"
Write-Host "수동(콘솔 확인): .venv\Scripts\python.exe scripts\run_daily_email.py --slot 1430"
