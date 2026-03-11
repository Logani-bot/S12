@echo off
echo ========================================
echo S12 실시간 모니터링 표시 모드 테스트
echo ========================================
echo.
echo 실시간 모니터링 창이 열립니다...
echo.
echo.

REM 기존 run_real_time_monitor.bat 실행
start "S12 실시간 모니터링" cmd /k "%~dp0run_real_time_monitor.bat"

timeout /t 2 > nul
echo.
echo 모니터링 창이 열렸습니다!
echo.
pause


