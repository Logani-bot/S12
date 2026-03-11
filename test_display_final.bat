@echo off
echo 테스트 시작...
echo.
start "S12 로그 모니터" cmd /k "powershell -ExecutionPolicy Bypass -File auto_log_viewer.ps1"
timeout /t 2 > nul
echo.
echo 로그 창이 열렸습니다!
echo.


