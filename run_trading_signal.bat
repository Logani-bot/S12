@echo off
REM Trading Signal System 실행 배치 파일 (20:10 자동 실행)
REM Windows 작업 스케줄러에서 호출됨

cd /d "%~dp0"

echo ========================================
echo S12 Trading System - Daily Run (20:10)
echo 시작 시간: %date% %time%
echo ========================================
echo.

REM ===== 1단계: 거래대금 5000억+ 종목 수집 =====
echo [1/2] 거래대금 추적 중...
echo ========================================
python Daily_Turnover_Tracker.py
echo.

REM ===== 2단계: 매매 시그널 생성 및 텔레그램 리포트 =====
echo [2/2] 매매 시그널 생성 중...
echo ========================================
python Trading_Signal_System.py ^
  --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU ^
  --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs ^
  --alert-threshold 10.0

echo.
echo ========================================
echo 완료 시간: %date% %time%
echo ========================================

REM 로그 저장 (선택)
REM python Trading_Signal_System.py ... >> signal_log.txt 2>&1






