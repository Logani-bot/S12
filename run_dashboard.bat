@echo off
chcp 65001 > nul
cd /d "%~dp0"

echo ========================================
echo   Real-time Monitor Dashboard
echo ========================================
echo.

"C:\Program Files (x86)\Python311\python.exe" monitor_dashboard.py

if errorlevel 1 (
    echo.
    echo Error: Dashboard execution failed
    pause
)
