@echo off
REM ========================================
REM S12 로그 모니터링 (최소 전력 버전)
REM ========================================
chcp 65001 > nul
cd /d "%~dp0"

echo.
echo === S12 로그 최신 내용 표시 ===
echo (한 번만 표시, 전력 절약 모드)
echo.

set /p type="로그 종류 (daily/realtime/all): "

if "%type%"=="daily" (
    echo.
    echo === 일일 리포트 로그 ===
    powershell -Command "Get-ChildItem logs\s12_daily_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content -Tail 50 -Encoding UTF8"
) else if "%type%"=="realtime" (
    echo.
    echo === 실시간 모니터링 로그 ===
    powershell -Command "Get-ChildItem logs\realtime_monitor_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content -Tail 50 -Encoding UTF8"
) else (
    echo.
    echo === 전체 로그 ===
    powershell -Command "Get-ChildItem logs\*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 3 | ForEach-Object { Write-Host \"`n--- $($_.Name) ---\" -ForegroundColor Yellow; Get-Content $_.FullName -Tail 20 -Encoding UTF8 }"
)

echo.
echo.
echo (완료 - 한 번만 표시되었습니다)
echo.
pause


