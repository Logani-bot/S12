# 기존 작업을 표시 모드로 변경

$taskName = "S2_Realtime_Monitor"

try {
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
    
    Write-Host "작업 찾음: $taskName" -ForegroundColor Green
    
    # 표시 모드로 설정
    $task.Settings.Hidden = $false
    Set-ScheduledTask -InputObject $task
    
    Write-Host "표시 모드로 변경 완료!" -ForegroundColor Green
    Write-Host "이제 실행 시 화면에 표시됩니다." -ForegroundColor Yellow
}
catch {
    Write-Host "작업을 찾을 수 없습니다: $taskName" -ForegroundColor Red
}

Write-Host ""
Read-Host "엔터 키를 눌러 종료..."


