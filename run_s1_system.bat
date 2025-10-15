@echo off
REM S1 시스템 전체 실행
REM 1. 시총 필터링 → 2. 트레이딩 시그널 생성

echo ========================================
echo S1 시스템 전체 실행 시작
echo ========================================

echo [1/2] 시총 기반 종목 필터링...
call run_market_cap_filter.bat

if %ERRORLEVEL% NEQ 0 (
    echo ✗ 시총 필터링 실패로 중단
    pause
    exit /b 1
)

echo.
echo [2/2] S1 트레이딩 시그널 생성...
call run_trading_signal_s1.bat

if %ERRORLEVEL% NEQ 0 (
    echo ✗ S1 트레이딩 시그널 생성 실패
    pause
    exit /b 1
)

echo ========================================
echo S1 시스템 전체 실행 완료
echo ========================================
