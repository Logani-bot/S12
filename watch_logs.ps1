# S12 실시간 로그 모니터링 (PowerShell 버전)
# 더 나은 성능과 기능 제공

param(
    [ValidateSet('daily','realtime','all','today')]
    [string]$Type = 'today'
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$logsDir = Join-Path $scriptDir "logs"

if (-not (Test-Path $logsDir)) {
    Write-Host "로그 폴더를 찾을 수 없습니다: $logsDir" -ForegroundColor Red
    exit 1
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "S12 실시간 로그 모니터링" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

function Show-LogMonitor {
    param(
        [string]$Pattern,
        [string]$Label
    )
    
    Write-Host "$Label 모니터링 시작...`n" -ForegroundColor Yellow
    Write-Host "(Ctrl+C로 종료)`n" -ForegroundColor Gray
    Write-Host "========================================`n" -ForegroundColor Cyan
    
    # 무한 루프로 실시간 모니터링
    $lastPosition = 0
    
    while ($true) {
        try {
            # 로그 파일 찾기
            $logFiles = Get-ChildItem -Path $logsDir -Filter $Pattern -ErrorAction SilentlyContinue | 
                        Sort-Object LastWriteTime -Descending
            
            if ($logFiles.Count -eq 0) {
                Write-Host "[대기 중] 로그 파일을 찾는 중... $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
                Start-Sleep -Seconds 5
                continue
            }
            
            # 가장 최근 파일
            $latestLog = $logFiles[0]
            
            # 파일이 새로 생성되었거나 크기가 변경되었으면
            $currentSize = (Get-Item $latestLog.FullName).Length
            
            if ($currentSize -ne $lastPosition) {
                # 새 내용 읽기
                $content = Get-Content -Path $latestLog.FullName -Tail 30 -Encoding UTF8 -ErrorAction SilentlyContinue
                Clear-Host
                
                Write-Host "========================================" -ForegroundColor Cyan
                Write-Host "File: $($latestLog.Name)" -ForegroundColor Yellow
                Write-Host "Size: $([math]::Round($currentSize/1KB, 2)) KB" -ForegroundColor Gray
                Write-Host "Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
                Write-Host "========================================`n" -ForegroundColor Cyan
                
                Write-Host $content -ForegroundColor White
                Write-Host "`n========================================" -ForegroundColor Cyan
                Write-Host "실시간 업데이트 중... (Ctrl+C로 종료)" -ForegroundColor Green
                
                $lastPosition = $currentSize
            }
            
            Start-Sleep -Seconds 2
        }
        catch {
            Write-Host "`n[에러] $($_.Exception.Message)" -ForegroundColor Red
            Start-Sleep -Seconds 5
        }
    }
}

# 메인 로직
switch ($Type.ToLower()) {
    'daily' {
        Show-LogMonitor -Pattern "s12_daily_*.log" -Label "일일 리포트 로그"
    }
    'realtime' {
        Show-LogMonitor -Pattern "realtime_monitor_*.log" -Label "실시간 모니터링 로그"
    }
    'today' {
        Write-Host "오늘 날짜의 모든 로그 모니터링...`n" -ForegroundColor Yellow
        $today = Get-Date -Format "yyyyMMdd"
        
        # 오늘 날짜의 로그 파일 찾기
        $todayLogs = Get-ChildItem -Path $logsDir -Filter "*$today.log" -ErrorAction SilentlyContinue
        
        if ($todayLogs.Count -eq 0) {
            Write-Host "오늘의 로그 파일이 아직 없습니다." -ForegroundColor Yellow
            Write-Host "로그가 생성되면 자동으로 표시됩니다...`n" -ForegroundColor Gray
        }
        
        Write-Host "========================================`n" -ForegroundColor Cyan
        
        # 오늘 날짜의 모든 로그를 시간순으로 보여줌
        while ($true) {
            $todayLogs = Get-ChildItem -Path $logsDir -Filter "*$today.log" -ErrorAction SilentlyContinue
            
            if ($todayLogs.Count -gt 0) {
                Clear-Host
                Write-Host "========================================" -ForegroundColor Cyan
                Write-Host "오늘의 로그 ($today)" -ForegroundColor Yellow
                Write-Host "시간: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
                Write-Host "========================================`n" -ForegroundColor Cyan
                
                # 각 로그 파일의 최근 내용 표시
                foreach ($log in $todayLogs | Sort-Object Name) {
                    Write-Host "--- $($log.Name) ---" -ForegroundColor Magenta
                    $content = Get-Content -Path $log.FullName -Tail 15 -Encoding UTF8 -ErrorAction SilentlyContinue
                    Write-Host $content -ForegroundColor White
                    Write-Host ""
                }
                
                Write-Host "========================================" -ForegroundColor Cyan
                Write-Host "실시간 업데이트 중... (Ctrl+C로 종료)" -ForegroundColor Green
                Start-Sleep -Seconds 3
            } else {
                Write-Host "[대기 중] 로그 파일을 찾는 중... $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Gray
                Start-Sleep -Seconds 5
            }
        }
    }
    'all' {
        Write-Host "모든 로그 통합 모니터링...`n" -ForegroundColor Yellow
        # 모든 로그를 하나로 합쳐서 실시간 표시
        while ($true) {
            Clear-Host
            Write-Host "========================================" -ForegroundColor Cyan
            Write-Host "전체 로그 통합 뷰" -ForegroundColor Yellow
            Write-Host "시간: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
            Write-Host "========================================`n" -ForegroundColor Cyan
            
            $allLogs = Get-ChildItem -Path $logsDir -Filter "*.log" -ErrorAction SilentlyContinue | 
                       Sort-Object LastWriteTime -Descending
            
            foreach ($log in $allLogs | Select-Object -First 5) {
                Write-Host "--- $($log.Name) ---" -ForegroundColor Magenta
                $content = Get-Content -Path $log.FullName -Tail 10 -Encoding UTF8 -ErrorAction SilentlyContinue
                Write-Host $content -ForegroundColor White
                Write-Host ""
            }
            
            Write-Host "========================================" -ForegroundColor Cyan
            Write-Host "실시간 업데이트 중... (Ctrl+C로 종료)" -ForegroundColor Green
            Start-Sleep -Seconds 3
        }
    }
}


