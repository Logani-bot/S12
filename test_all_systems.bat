@echo off
echo ======================================
echo Testing All Systems
echo ======================================

echo.
echo 1. Testing Daily Turnover Tracker...
python Daily_Turnover_Tracker.py
if %errorlevel% neq 0 (
    echo ERROR: Turnover tracker failed
    pause
    exit /b 1
)

echo.
echo 2. Testing Trading Signal System...
python Trading_Signal_System.py --appkey IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU --secret eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs
if %errorlevel% neq 0 (
    echo ERROR: Signal system failed
    pause
    exit /b 1
)

echo.
echo ======================================
echo All tests completed successfully!
echo ======================================
pause

