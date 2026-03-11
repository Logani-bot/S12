@echo off
chcp 65001 > nul
cd /d "%~dp0"

echo ========================================
echo S12 실시간 로그 모니터링
echo ========================================
echo.
echo 모니터링할 로그를 선택하세요:
echo.
echo 1. 일일 리포트 로그 (Daily)
echo 2. 실시간 모니터링 로그 (Real-Time)
echo 3. 전체 로그 통합 뷰
echo 4. 종료
echo.
set /p choice="선택 (1-4): "

if "%choice%"=="1" goto daily
if "%choice%"=="2" goto realtime
if "%choice%"=="3" goto all
if "%choice%"=="4" goto end
goto menu

:daily
echo.
echo ========================================
echo 일일 리포트 로그 모니터링
echo ========================================
echo.
echo 최신 로그를 실시간으로 표시합니다...
echo (Ctrl+C로 종료)
echo ========================================
echo.
powershell -Command "Get-Content 'logs\s12_daily_*.log' -Wait -Tail 50 -Encoding UTF8"
goto menu

:realtime
echo.
echo ========================================
echo 실시간 모니터링 로그
echo ========================================
echo.
echo 최신 로그를 실시간으로 표시합니다...
echo (Ctrl+C로 종료)
echo ========================================
echo.
powershell -Command "Get-Content 'logs\realtime_monitor_*.log' -Wait -Tail 50 -Encoding UTF8"
goto menu

:all
echo.
echo ========================================
echo 전체 로그 통합 뷰
echo ========================================
echo.
echo 모든 로그를 시간순으로 실시간 표시...
echo (Ctrl+C로 종료)
echo ========================================
echo.
powershell -Command "Get-ChildItem logs\*.log | Sort-Object LastWriteTime | Get-Content -Wait -Tail 50 -Encoding UTF8"
goto menu

:end
echo.
echo 종료합니다.
pause


