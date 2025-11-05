@echo off
chcp 65001 > nul
echo 업비트 시장 하락 감시 시스템 시작...
echo.

cd /d "%~dp0"

python upbit_alert_monitor.py

pause




















