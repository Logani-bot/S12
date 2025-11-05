@echo off
chcp 65001 > nul
echo 업비트 알람 시스템 스케줄러 설정
echo.

REM 관리자 권한 확인
net session >nul 2>&1
if %errorLevel% == 0 (
    echo ✓ 관리자 권한으로 실행 중
) else (
    echo ❌ 관리자 권한이 필요합니다.
    echo 이 파일을 우클릭하여 "관리자 권한으로 실행"을 선택해주세요.
    pause
    exit /b 1
)

echo.
echo Windows 작업 스케줄러에 업비트 알람 작업을 등록합니다...
echo.

REM 현재 스크립트 경로
set SCRIPT_PATH=%~dp0upbit_alert_monitor.py
set BATCH_PATH=%~dp0run_upbit_alert.bat

echo 스크립트 경로: %SCRIPT_PATH%
echo 배치 파일 경로: %BATCH_PATH%
echo.

REM 작업 스케줄러 등록
schtasks /create /tn "Upbit_Alert_Monitor" /tr "\"%BATCH_PATH%\"" /sc minute /mo 10 /st 09:00 /et 18:00 /f

if %errorLevel% == 0 (
    echo.
    echo ✓ 업비트 알람 스케줄러 등록 완료!
    echo.
    echo 작업 정보:
    echo - 작업 이름: Upbit_Alert_Monitor
    echo - 실행 간격: 10분마다
    echo - 실행 시간: 09:00 - 18:00
    echo - 실행 파일: %BATCH_PATH%
    echo.
    echo 확인 방법:
    echo 1. 작업 스케줄러 열기
    echo 2. "Upbit_Alert_Monitor" 작업 확인
    echo.
) else (
    echo.
    echo ❌ 스케줄러 등록 실패
    echo 오류 코드: %errorLevel%
    echo.
)

pause




















