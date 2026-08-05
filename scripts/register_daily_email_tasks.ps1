# 작업 스케줄러에 거래일 14:30 / 15:30 이메일 리포트 등록 예시
#
# - 매일 14:30: N→N+1 예측 + 이메일
# - 매일 15:30: N→N+1 예측 + 이메일 (장마감 직후)
# - 토·일·공휴일은 스크립트가 즉시 종료(exit 0)
#
# 사용 전 .env 에 EMAIL_SMTP_* / EMAIL_RECIPIENTS 를 설정하세요.
#
# 아래 schtasks 줄은 주석 처리되어 있습니다. 사용할 때만 # 을 제거하세요.

$Repo = "E:\Git\Money"
$Ps1430 = Join-Path $Repo "scripts\run_daily_1430_email.ps1"
$Ps1530 = Join-Path $Repo "scripts\run_daily_1530_email.ps1"
$Tr1430 = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$Ps1430`" -RepoRoot `"$Repo`""
$Tr1530 = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$Ps1530`" -RepoRoot `"$Repo`""

# schtasks /Create /TN "MoneyKRX_Daily1430_Email" /TR $Tr1430 /SC DAILY /ST 14:30 /RL HIGHEST /F
# schtasks /Create /TN "MoneyKRX_Daily1530_Email" /TR $Tr1530 /SC DAILY /ST 15:30 /RL HIGHEST /F
#
# 등록 후 실행 로그: scripts/logs/run_daily_YYYYMMDD.log
# 작업 스케줄러 '마지막 실행 결과' 1 = main.py 는 됐는데 이메일 SMTP 실패일 수 있음

Write-Host "[예시] 작업 스케줄 등록은 비활성화되어 있습니다. 활성화하려면 schtasks 줄의 선행 # 을 제거하세요."
Write-Host "등록 예정:"
Write-Host "  MoneyKRX_Daily1430_Email (매일 14:30) -> $Ps1430"
Write-Host "  MoneyKRX_Daily1530_Email (매일 15:30) -> $Ps1530"
Write-Host "확인(등록 후): schtasks /Query /TN MoneyKRX_Daily1430_Email /V /FO LIST"
Write-Host "삭제(등록 후): schtasks /Delete /TN MoneyKRX_Daily1430_Email /F"
