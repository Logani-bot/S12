@echo off
REM Trading Signal System Batch File (20:10 Auto Execution)
REM Called by Windows Task Scheduler
REM Can run in screensaver/locked state

REM Set working directory
cd /d "%~dp0"

REM Set log file
set LOG_FILE=%~dp0logs\s12_daily_%date:~0,4%%date:~5,2%%date:~8,2%.log
if not exist "%~dp0logs" mkdir "%~dp0logs"

REM Start logging
echo ======================================== >> "%LOG_FILE%"
echo S12 Trading System - Daily Run (20:10) >> "%LOG_FILE%"
echo Start Time: %date% %time% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"

echo ========================================
echo S12 Trading System - Daily Run (20:10)
echo Start Time: %date% %time%
echo ========================================
echo.

REM ===== Step 1: Daily Turnover Tracking =====
echo [1/2] Daily Turnover Tracking...
echo ========================================
echo [1/2] Daily Turnover Tracking... >> "%LOG_FILE%"

python Daily_Turnover_Tracker.py --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% neq 0 (
    echo ERROR: Daily Turnover Tracker failed! >> "%LOG_FILE%"
    echo ERROR: Daily Turnover Tracker failed!
    goto :error_exit
)

echo Step 1 completed successfully >> "%LOG_FILE%"
echo.

REM ===== Step 2: Trading Signal Generation =====
echo [2/2] Trading Signal Generation...
echo ========================================
echo [2/2] Trading Signal Generation... >> "%LOG_FILE%"

python Trading_Signal_System.py --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs --alert-threshold 10.0 >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% neq 0 (
    echo ERROR: Trading Signal System failed! >> "%LOG_FILE%"
    echo ERROR: Trading Signal System failed!
    goto :error_exit
)

echo Step 2 completed successfully >> "%LOG_FILE%"

echo.
echo ========================================
echo Completion Time: %date% %time%
echo ========================================
echo Completion Time: %date% %time% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"

echo SUCCESS: All steps completed successfully >> "%LOG_FILE%"
exit /b 0

:error_exit
echo ========================================
echo ERROR: Process failed at %date% %time%
echo ========================================
echo ERROR: Process failed at %date% %time% >> "%LOG_FILE%"
echo ======================================== >> "%LOG_FILE%"
exit /b 1






