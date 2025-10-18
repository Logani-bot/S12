# S12 Trading System - 고급 스케줄러 설정
# 화면보호기, 잠금 상태에서도 실행되도록 설정

Write-Host "=========================================" -ForegroundColor Green
Write-Host "S12 Trading System - 고급 스케줄러 설정" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""

# 기존 작업 삭제 (있다면)
Write-Host "기존 작업 삭제 중..." -ForegroundColor Yellow
try {
    schtasks /delete /tn "S12_Trading_Signal_Daily" /f
    Write-Host "✓ 기존 작업 삭제 완료" -ForegroundColor Green
} catch {
    Write-Host "기존 작업이 없습니다." -ForegroundColor Gray
}

Write-Host ""

# 새로운 고급 작업 생성
Write-Host "고급 스케줄러 작업 생성 중..." -ForegroundColor Yellow

$taskName = "S12_Trading_Signal_Daily"
$taskDescription = "S12 Trading System - Daily Signal Generation (20:10)"
$taskPath = "C:\Coding\S12\run_trading_signal.bat"
$workingDir = "C:\Coding\S12"

# XML 설정 생성
$taskXml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Date>2025-10-18T00:00:00</Date>
    <Author>S12 Trading System</Author>
    <Description>$taskDescription</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2025-10-18T20:10:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>S-1-5-18</UserId>
      <RunLevel>HighestAvailable</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT2H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>$taskPath</Command>
      <WorkingDirectory>$workingDir</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"@

# XML 파일로 저장
$xmlFile = "C:\Coding\S12\s12_task.xml"
$taskXml | Out-File -FilePath $xmlFile -Encoding UTF8

# 작업 등록
Write-Host "작업 등록 중..." -ForegroundColor Yellow
schtasks /create /tn $taskName /xml $xmlFile

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ 고급 스케줄러 작업 생성 완료!" -ForegroundColor Green
} else {
    Write-Host "✗ 작업 생성 실패" -ForegroundColor Red
    exit 1
}

# 설정 확인
Write-Host ""
Write-Host "설정 확인 중..." -ForegroundColor Yellow
schtasks /query /tn $taskName /v /fo list | Select-String -Pattern "실행 수준|Run Level|사용자|User|사용자 계정|User Account"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host "설정 완료!" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""
Write-Host "주요 설정 사항:" -ForegroundColor Cyan
Write-Host "• 실행 수준: 최고 권한 (HighestAvailable)" -ForegroundColor White
Write-Host "• 배터리 모드: 실행 허용" -ForegroundColor White
Write-Host "• 네트워크 없어도 실행" -ForegroundColor White
Write-Host "• 화면보호기/잠금 상태에서도 실행" -ForegroundColor White
Write-Host "• 실행 시간 제한: 2시간" -ForegroundColor White
Write-Host "• 우선순위: 높음 (7)" -ForegroundColor White
Write-Host ""
Write-Host "다음 실행 예정: 매일 오후 8시 10분" -ForegroundColor Yellow

# XML 파일 정리
Remove-Item $xmlFile -Force
Write-Host ""
Write-Host "Temporary file cleanup completed" -ForegroundColor Gray