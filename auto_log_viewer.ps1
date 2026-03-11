# 자동 로그 모니터링 스크립트
# 3초마다 업데이트

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

while ($true) {
    Clear-Host
    
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "S12 실시간 로그 모니터링" -ForegroundColor Yellow
    Write-Host "시간: " -NoNewline
    Write-Host (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
    
    try {
        $latestLog = Get-ChildItem -Path "$scriptDir\logs" -Filter "realtime_monitor_*.log" -ErrorAction SilentlyContinue | 
                     Sort-Object LastWriteTime -Descending | 
                     Select-Object -First 1
        
        if ($latestLog) {
            Get-Content -Path $latestLog.FullName -Tail 35 -Encoding UTF8 -ErrorAction SilentlyContinue
        } else {
            Write-Host "로그 파일을 찾는 중..." -ForegroundColor Gray
        }
    }
    catch {
        Write-Host "로그를 읽는 중 오류 발생" -ForegroundColor Red
    }
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "실시간 업데이트 중... (Ctrl+C로 종료)" -ForegroundColor Green
    Write-Host ""
    
    Start-Sleep -Seconds 3
}


