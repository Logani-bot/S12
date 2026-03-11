@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ========================================================
echo S12 실시간 주식 모니터링 시작 (표시 모드)
echo ========================================================
echo.
echo 모니터링 설정:
echo   - 거래일: 08:00 ~ 20:00
echo   - 간격: 60초 (1분)
echo   - Summary 탭의 종목만 모니터링
echo   - 매수선 접근 시 텔레그램 알림
echo.
echo 종료하려면 Ctrl+C를 누르세요.
echo ========================================================
echo.

REM 로그 모니터링 창 자동 열기
start "S12 로그 모니터" powershell -NoExit -Command "cd '%~dp0'; while ($true) { Clear-Host; Write-Host '========================================' -ForegroundColor Cyan; Write-Host 'S12 실시간 로그 모니터링' -ForegroundColor Yellow; Write-Host '시간: ' -NoNewline; Write-Host (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') -ForegroundColor Green; Write-Host '========================================' -ForegroundColor Cyan; Write-Host ''; Get-ChildItem logs\realtime_monitor_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content -Tail 30 -Encoding UTF8 -ErrorAction SilentlyContinue; Write-Host '========================================' -ForegroundColor Cyan; Write-Host '실시간 업데이트 중... (Ctrl+C로 종료)' -ForegroundColor Green; Write-Host ''; Start-Sleep -Seconds 3 }"

REM 메인 모니터링 시작
python Real_Time_Monitor.py ^
  --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU ^
  --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs ^
  --interval 60

pause


