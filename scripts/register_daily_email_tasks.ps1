# 작업 스케줄러에 거래일 14:30 / 15:30 / 16:00 리포트 + 장중 등락률 데몬 등록
#
# - 로그온 + 매일 08:50: 실시간 상승률 데몬(127.0.0.1:8765). PC 재부팅 후에도 유지
# - 매일 14:30: N→N+1 예측 + 이메일
# - 매일 15:30: 장마감 직후 리포트(actual·테마) 갱신 + 이메일
# - 매일 16:00: append-rebuild-learning 후 성공 시 즉시 force-ml-retrain
#   (RUN_DAILY_AUTO_1630=1 일 때, 리포트 미작성·미첨부) + 이메일 1통
# - 토·일·공휴일은 리포트 스크립트가 즉시 종료(exit 0). 등락률 데몬은 떠 있어도 무해
# - PowerShell 창 없이 pythonw.exe 로 실행 (콘솔 클릭으로 멈추는 문제 방지)
# - 작업 이름은 목록 상단 정렬을 위해 ``0.`` 접두사 (예: 0.MoneyKRX_Daily1430_Email)
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
$TaskPrefix = "0."

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

function Remove-SchTaskIfExists([string] $Name) {
    if (-not $Name) { return }
    if (-not (Test-SchTask $Name)) { return }
    schtasks /Delete /TN $Name /F 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "삭제(구이름): $Name"
    }
}

function Move-SchTaskToPrefixed([string] $OldName, [string] $NewName) {
    if (-not $OldName -or $OldName -eq $NewName) { return }
    if (-not (Test-SchTask $OldName)) { return }
    if (Test-SchTask $NewName) {
        Remove-SchTaskIfExists $OldName
        return
    }
    $xmlPath = Join-Path $env:TEMP ("money_schtask_" + [guid]::NewGuid().ToString("N") + ".xml")
    $raw = schtasks /Query /TN $OldName /XML 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $raw) {
        Write-Host "XML 내보내기 실패: $OldName"
        return
    }
    $xml = ($raw | Out-String).Replace("\" + $OldName, "\" + $NewName)
    Set-Content -LiteralPath $xmlPath -Value $xml -Encoding Unicode
    schtasks /Create /TN $NewName /XML $xmlPath /F | Out-Null
    Remove-Item -LiteralPath $xmlPath -Force -ErrorAction SilentlyContinue
    if ($LASTEXITCODE -ne 0) {
        Write-Host "이름 이전 실패(XML): $OldName -> $NewName"
        return
    }
    Remove-SchTaskIfExists $OldName
    Write-Host "이름 이전: $OldName -> $NewName"
}

$Tr1430 = New-DailyEmailTaskTr "1430"
$Tr1530 = New-DailyEmailTaskTr "1530"
$Tr1600 = New-DailyEmailTaskTr "1600"

$Tasks = @(
    @{ Name = "${TaskPrefix}MoneyKRX_Daily1430_Email"; OldName = "MoneyKRX_Daily1430_Email"; Time = "14:30"; Tr = $Tr1430 }
    @{ Name = "${TaskPrefix}MoneyKRX_Daily1530_Email"; OldName = "MoneyKRX_Daily1530_Email"; Time = "15:30"; Tr = $Tr1530 }
    @{ Name = "${TaskPrefix}MoneyKRX_Daily1600_Append"; OldName = "MoneyKRX_Daily1600_Append"; Time = "16:00"; Tr = $Tr1600 }
)

foreach ($t in $Tasks) {
    Move-SchTaskToPrefixed $t.OldName $t.Name
    if (Test-SchTask $t.Name) {
        Write-Host "유지: $($t.Name)"
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

# 1600 이 실제로 있을 때만 구 15:00·16:30 삭제
if (Test-SchTask "${TaskPrefix}MoneyKRX_Daily1600_Append") {
    foreach ($legacy in @(
            "MoneyKRX_Daily1500_Append",
            "MoneyKRX_Daily1630_MLRetrain",
            "MoneyKRX_Daily1600_Append"
        )) {
        Remove-SchTaskIfExists $legacy
    }
}

function Register-OneSchTask([string] $Name, [string] $Tr, [string] $Schedule, [string] $StartTime) {
    if (Test-SchTask $Name) {
        Write-Host "유지: $Name"
        return
    }
    if ($Schedule -eq "ONLOGON") {
        schtasks /Create /TN $Name /TR $Tr /SC ONLOGON /F | Out-Null
    }
    else {
        schtasks /Create /TN $Name /TR $Tr /SC DAILY /ST $StartTime /F | Out-Null
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "작업 등록 실패: $Name (관리자 PowerShell 필요할 수 있음)"
        exit 1
    }
    if ($Schedule -eq "ONLOGON") {
        Write-Host "등록: $Name (로그온) -> pythonw 데몬 (창 없음)"
    }
    else {
        Write-Host "등록: $Name (매일 $StartTime) -> pythonw 데몬 (창 없음)"
    }
}

function Register-LiveQuotesDaemonTask {
    $DaemonPy = Join-Path $Repo "scripts\run_live_quotes_daemon.py"
    if (-not (Test-Path -LiteralPath $DaemonPy)) {
        Write-Error "없음: $DaemonPy"
        exit 1
    }
    $Tr = "`"$PythonW`" `"$DaemonPy`""
    # 로그온: HKCU Run (관리자 불필요). ONLOGON schtasks 는 종종 액세스가 거부됨.
    $runKey = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
    New-ItemProperty -Path $runKey -Name "MoneyKRX_LiveQuotes" -Value $Tr -PropertyType String -Force | Out-Null
    Write-Host "등록: HKCU Run\MoneyKRX_LiveQuotes (로그온) -> pythonw 데몬 (창 없음)"

    $morningNew = "${TaskPrefix}MoneyKRX_LiveQuotes_Morning"
    Move-SchTaskToPrefixed "MoneyKRX_LiveQuotes_Morning" $morningNew
    Register-OneSchTask $morningNew $Tr "DAILY" "08:50"

    $logonNew = "${TaskPrefix}MoneyKRX_LiveQuotes_Logon"
    try {
        schtasks /Create /TN $logonNew /TR $Tr /SC ONLOGON /F 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "등록: $logonNew (ONLOGON schtasks)"
        }
    }
    catch {
        # 무시. HKCU Run 이 로그온 기동을 담당.
    }
    Remove-SchTaskIfExists "MoneyKRX_LiveQuotes_Logon"
}

Register-LiveQuotesDaemonTask

Write-Host ""
Write-Host "확인: reg query HKCU\Software\Microsoft\Windows\CurrentVersion\Run /v MoneyKRX_LiveQuotes"
Write-Host "확인: schtasks /Query /TN ${TaskPrefix}MoneyKRX_LiveQuotes_Morning /FO LIST"
Write-Host "확인: schtasks /Query /TN ${TaskPrefix}MoneyKRX_Daily1600_Append /V /FO LIST"
Write-Host "로그: scripts/logs/run_daily_YYYYMMDD.log"
Write-Host "수동(콘솔): .venv\Scripts\python.exe scripts\run_daily_email.py --slot 1600"
Write-Host "수동 ML만: .venv\Scripts\python.exe scripts\run_daily_email.py --slot 1630 --force"
Write-Host "수동 등락률 데몬: .venv\Scripts\pythonw.exe scripts\run_live_quotes_daemon.py"
