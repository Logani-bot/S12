@echo off
REM 실시간 모니터링 백그라운드 실행 (새 창에서)
cd /d "%~dp0"

echo ========================================
echo 실시간 모니터링을 백그라운드로 시작합니다.
echo ========================================
echo.
echo 새 창이 열리면서 10분마다 자동 모니터링됩니다.
echo.
echo 종료하려면:
echo   1. 새로 열린 창에서 Ctrl+C
echo   또는
echo   2. 작업 관리자에서 python 프로세스 종료
echo.
pause

REM 새 창에서 백그라운드로 실행 (최소화)
start "실시간 주식 모니터링" /MIN python Real_Time_Monitor.py ^
  --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU ^
  --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs ^
  --interval 10

echo.
echo ========================================
echo 백그라운드 실행 시작됨!
echo 작업 관리자에서 확인 가능합니다.
echo ========================================
pause


