@echo off
chcp 65001 > nul
cd /d "%~dp0"

echo 시작...

REM 로그 모니터링 창 열기
start "S12 로그 모니터" powershell -ExecutionPolicy Bypass -File "%~dp0auto_log_viewer.ps1"

timeout /t 1 > nul
echo 완료


