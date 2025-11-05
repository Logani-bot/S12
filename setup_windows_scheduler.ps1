# Windows 작업 스케줄러 자동 설정 스크립트
# S12 트레이딩 시스템용

# PowerShell 관리자 권한 체크
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")
if (-not $isAdmin) {
    Write-Host "⚠️  관리자 권한이 필요합니다. PowerShell을 관리자 권한으로 실행해주세요." -ForegroundColor Red
    exit 1
}

# 현재 스크립트 위치
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$batFile1 = Join-Path $scriptDir "RUN_DAILY_SYSTEM.bat"
$batFile2 = Join-Path $scriptDir "run_real_time_monitor.bat"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Windows 작업 스케줄러 설정 시작" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# 작업 스케줄러 서비스 체크
try {
    $schedule = New-Object -ComObject Schedule.Service
    $schedule.Connect()
} catch {
    Write-Host "❌ 작업 스케줄러에 연결할 수 없습니다." -ForegroundColor Red
    exit 1
}

# 기존 작업 삭제 (있다면)
$taskNames = @("S2_Daily_Trading_Signal", "S2_Realtime_Monitor")
foreach ($taskName in $taskNames) {
    try {
        $schedule.GetFolder("\").DeleteTask($taskName, 0)
        Write-Host "✓ 기존 작업 삭제: $taskName" -ForegroundColor Yellow
    } catch {
        # 작업이 없으면 에러 무시
    }
}

# 작업 1: Daily Turnover + Signal (매일 20:10)
Write-Host "`n작업 1 생성 중..." -ForegroundColor Green
$action1 = New-ScheduledTaskAction -Execute $batFile1
$trigger1 = New-ScheduledTaskTrigger -Daily -At "20:10"
$settings1 = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName "S2_Daily_Trading_Signal" -Action $action1 -Trigger $trigger1 -Settings $settings1 -Description "S12 매일 거래대금 + 시그널 분석 (20:10)" -Force | Out-Null
Write-Host "✓ 작업 1 생성 완료: S2_Daily_Trading_Signal (매일 20:10)" -ForegroundColor Green

# 작업 2: Real-Time Monitor (평일 08:00) - 백그라운드 실행
Write-Host "`n작업 2 생성 중... (백그라운드)" -ForegroundColor Green
$action2 = New-ScheduledTaskAction -Execute $batFile2
$trigger2 = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At "08:00"
$settings2 = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName "S2_Realtime_Monitor" -Action $action2 -Trigger $trigger2 -Settings $settings2 -Description "S12 실시간 주식 모니터링 (평일 08:00)" -Force | Out-Null
Write-Host "✓ 작업 2 생성 완료: S2_Realtime_Monitor (평일 08:00, 백그라운드)" -ForegroundColor Green

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "✅ 작업 스케줄러 설정 완료!" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "생성된 작업:" -ForegroundColor Yellow
Write-Host "  1. S2_Daily_Trading_Signal - 매일 20:10 실행 (백그라운드)" -ForegroundColor White
Write-Host "  2. S2_Realtime_Monitor - 평일 08:00 실행 (백그라운드)" -ForegroundColor White

Write-Host "`n백그라운드 실행 특징:" -ForegroundColor Yellow
Write-Host "  - 모든 작업이 백그라운드에서 실행됩니다" -ForegroundColor White
Write-Host "  - 로그는 파일로 저장됩니다: logs/ 폴더" -ForegroundColor White
Write-Host "  - 터미널 창이 표시되지 않습니다" -ForegroundColor White

Write-Host "`n로그 확인 방법:" -ForegroundColor Yellow
Write-Host "  1. 로그 파일 위치: logs/s12_daily_YYYYMMDD.log" -ForegroundColor White
Write-Host "  2. 실시간 모니터 로그: logs/realtime_monitor_YYYYMMDD.log" -ForegroundColor White
Write-Host "  3. 작업 스케줄러에서 실행 결과 확인 가능" -ForegroundColor White
Write-Host "`n" -ForegroundColor White

