@echo off
REM S12 Trading System 자동 실행 등록 (매일 20:10)
echo ========================================
echo S12 Trading System 자동 실행 등록
echo ========================================
echo.
echo [주의] 이 작업은 관리자 권한이 필요합니다.
echo.
echo 다음 작업이 등록됩니다:
echo - 작업 이름: S12_Trading_Signal_Daily
echo - 실행 시간: 매일 20:10 (장 마감 후)
echo - 실행 내용:
echo   1. 거래대금 5000억+ 종목 수집 (Daily_Turnover_Tracker.py)
echo   2. 매매 시그널 생성 및 텔레그램 리포트 (Trading_Signal_System.py)
echo.
pause

REM 작업 스케줄러 등록
schtasks /create ^
  /tn "S12_Trading_Signal_Daily" ^
  /tr "\"%~dp0run_trading_signal.bat\"" ^
  /sc daily ^
  /st 20:10 ^
  /f

if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo [SUCCESS] 자동 실행이 등록되었습니다!
    echo ========================================
    echo.
    echo 매일 20:10에 자동으로 실행됩니다.
    echo 텔레그램으로 알람을 받게 됩니다.
    echo.
) else (
    echo.
    echo ========================================
    echo [ERROR] 등록 실패
    echo ========================================
    echo.
    echo 관리자 권한으로 다시 시도해주세요.
    echo (배치 파일 우클릭 -> 관리자 권한으로 실행)
    echo.
)

pause






