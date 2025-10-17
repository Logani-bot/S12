@echo off
REM 실시간 모니터링 실행 배치 파일 (10분 간격 반복)
cd /d "%~dp0"

echo ========================================
echo 실시간 주식 모니터링 시작
echo 시작 시간: %date% %time%
echo ========================================
echo.
echo 모니터링 시간: 08:00-20:00
echo 모니터링 간격: 10분
echo 알람 조건: 매수선 5%% 이내 접근
echo.
echo 종료하려면 Ctrl+C를 누르세요.
echo ========================================
echo.

python Real_Time_Monitor.py ^
  --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU ^
  --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs ^
  --interval 10

echo.
echo ========================================
echo 모니터링 종료: %date% %time%
echo ========================================
pause

