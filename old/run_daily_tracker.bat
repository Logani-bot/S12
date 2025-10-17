@echo off
REM 거래대금 5000억+ 종목 추적기 - 자동 실행 배치 파일
REM Windows 작업 스케줄러에 등록하여 매일 자동 실행 가능

cd /d "%~dp0"

echo ========================================
echo 거래대금 추적기 실행 중...
echo 시작 시간: %date% %time%
echo ========================================
echo.

python Daily_Turnover_Tracker.py

echo.
echo ========================================
echo 완료 시간: %date% %time%
echo ========================================

REM 로그 파일로 저장하려면 다음과 같이 실행:
REM python Daily_Turnover_Tracker.py >> tracker_log.txt 2>&1

REM 창을 5초간 유지 (오류 확인용)
timeout /t 5 /nobreak > nul


