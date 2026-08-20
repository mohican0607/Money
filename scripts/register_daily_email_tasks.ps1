# 작업 스케줄러에 거래일 14:30 / 15:30 / 16:00 리포트 등록
#
# - 매일 14:30: N→N+1 예측 + 이메일
# - 매일 15:30: 장마감 직후 리포트(actual·테마) 갱신 + 이메일
# - 매일 16:00: main.py --append-rebuild-learning YYYYMMDD (학습 진단만, 리포트 미작성·미첨부) + 이메일
# - 토·일·공휴일은 스크립트가 즉시 종료(exit 0)
# - PowerShell 창 없이 pythonw.exe 로 실행 (콘솔 클릭으로 멈추는 문제 방지)
#
# 사용:
#   powershell -ExecutionPolicy Bypass -File scripts\register_daily_email_tasks.ps1
#   powershell -ExecutionPolicy Bypass -File scripts\register_daily_email_tasks.ps1 -UpdateOnly
#   (-UpdateOnly 여도 없는 슬롯은 등록. 구 15:00 은 16:00 등록 성공 후에만 삭제)

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
    return "`"$PythonW`" `"$RunnerPy`" --slot $Slot"
}

function Test-SchTask([string] $Name) {
    cmd /c "schtasks /Query /TN `"$Name`" >nul 2>&1"
    return ($LASTEXITCODE -eq 0)
}

$Tr1430 = New-DailyEmailTaskTr "1430"
$Tr1530 = New-DailyEmailTaskTr "1530"
$Tr1600 = New-DailyEmailTaskTr "1600"

$Tasks = @(
    @{ Name = "MoneyKRX_Daily1430_Email"; Time = "14:30"; Tr = $Tr1430 }
    @{ Name = "MoneyKRX_Daily1530_Email"; Time = "15:30"; Tr = $Tr1530 }
    @{ Name = "MoneyKRX_Daily1600_Append"; Time = "16:00"; Tr = $Tr1600 }
)

foreach ($t in $Tasks) {
    if (Test-SchTask $t.Name) {
        schtasks /Change /TN $t.Name /TR $t.Tr | Out-Null
        if ($LASTEXITCODE -ne 0) {
            Write-Error "작업 변경 실패: $($t.Name)"
            exit 1
        }
        Write-Host "변경: $($t.Name) -> pythonw (창 없음)"
    }
    else {
        # UpdateOnly 여도 없는 슬롯은 등록한다.
        # (15:00→16:00 이름 변경 후 -UpdateOnly 가 1600 을 건너뛰던 버그 방지)
        schtasks /Create /TN $t.Name /TR $t.Tr /SC DAILY /ST $t.Time /RL HIGHEST /F | Out-Null
        if ($LASTEXITCODE -ne 0) {
            # /RL HIGHEST 는 관리자 필요. 일반 권한으로라도 등록해 로그온 세션에서 돌게 한다.
            schtasks /Create /TN $t.Name /TR $t.Tr /SC DAILY /ST $t.Time /F | Out-Null
        }
        if ($LASTEXITCODE -ne 0) {
            Write-Error "작업 등록 실패: $($t.Name) (관리자 PowerShell 필요할 수 있음)"
            exit 1
        }
        Write-Host "등록: $($t.Name) (매일 $($t.Time)) -> pythonw (창 없음)"
    }
}

# 1600 이 실제로 있을 때만 구 15:00 삭제 (교체 전에 지워 슬롯이 사라지던 문제 방지)
if (Test-SchTask "MoneyKRX_Daily1600_Append") {
    if (Test-SchTask "MoneyKRX_Daily1500_Append") {
        schtasks /Delete /TN "MoneyKRX_Daily1500_Append" /F 2>$null | Out-Null
        Write-Host "삭제: MoneyKRX_Daily1500_Append (15:00 → 16:00 이전)"
    }
}

Write-Host ""
Write-Host "확인: schtasks /Query /TN MoneyKRX_Daily1600_Append /V /FO LIST"
Write-Host "로그: scripts/logs/run_daily_YYYYMMDD.log"
Write-Host "수동(콘솔): .venv\Scripts\python.exe scripts\run_daily_email.py --slot 1600"
