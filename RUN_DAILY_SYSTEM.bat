@echo off
chcp 65001 > nul
REM ========================================
REM S12 Trading System - Manual Run
REM ========================================
REM 이 파일을 더블클릭하면 일일 리포트가 실행됩니다
REM 20:10에 자동 실행되는 것과 동일합니다
REM ========================================

echo ========================================
echo S12 Trading System - Manual Run
echo ========================================
echo.

cd /d "%~dp0"

"%~dp0run_trading_signal.bat"

pause







