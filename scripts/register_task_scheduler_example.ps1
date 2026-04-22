# 작업 스케줄러에 "거래일 15:00" 등록 예시
# - 매일 15:00 (토·일·공휴일은 스크립트가 즉시 종료)
# - MOCK_NEWS=0, main.py 실행 후 output 폴더 + report_YYYYMMDD.html 자동 오픈
#
# 실제 등록(schtasks)은 아래에서 주석 처리되어 있습니다. 사용할 때만 주석을 해제하세요.
#
# 아래 $Repo 를 본인 경로로 바꾼 뒤 PowerShell에서 실행 (관리자 권한 권장).

$Repo = "E:\Git\Money"
$Ps1  = Join-Path $Repo "scripts\run_daily_1500.ps1"
$Tr   = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$Ps1`" -RepoRoot `"$Repo`""

# schtasks /Create /TN "MoneyKRX_Daily1500" /TR $Tr /SC DAILY /ST 15:00 /RL HIGHEST /F

Write-Host "[예시] 작업 스케줄 등록은 비활성화되어 있습니다. 활성화하려면 schtasks 줄의 선행 # 을 제거하세요."
Write-Host "등록 예정: MoneyKRX_Daily1500 (매일 15:00) -> $Ps1"
Write-Host "확인(등록 후): schtasks /Query /TN MoneyKRX_Daily1500 /V /FO LIST"
Write-Host "삭제(등록 후): schtasks /Delete /TN MoneyKRX_Daily1500 /F"
