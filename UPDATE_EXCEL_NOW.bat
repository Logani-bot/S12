@echo off
chcp 65001 > nul
REM ========================================
REM 엑셀 파일 즉시 업데이트
REM ========================================

cd /d "%~dp0"

echo ========================================
echo 엑셀 파일 업데이트 중...
echo ========================================
echo.

REM Step 1: Daily Turnover Tracking
echo [1/2] turnover_universe.xlsx 업데이트 중...
"C:\Program Files (x86)\Python311\python.exe" Daily_Turnover_Tracker.py --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs

if %ERRORLEVEL% neq 0 (
    echo ERROR: Daily Turnover Tracker failed!
    pause
    exit /b 1
)

echo.
echo ========================================
echo [2/2] trading_signals.xlsx 업데이트 중...
"C:\Program Files (x86)\Python311\python.exe" Trading_Signal_System.py --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs --alert-threshold 10.0

if %ERRORLEVEL% neq 0 (
    echo ERROR: Trading Signal System failed!
    pause
    exit /b 1
)

echo.
echo ========================================
echo 업데이트 완료!
echo ========================================
echo.
echo output\turnover_universe.xlsx
echo output\trading_signals.xlsx
echo.

pause









