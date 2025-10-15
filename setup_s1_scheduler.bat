@echo off
REM S1 시스템 스케줄러 설정
REM 매일 20:10에 일일 트레이딩 리포트 S1 전송

echo ========================================
echo S1 시스템 스케줄러 설정
echo ========================================

echo 기존 S1 스케줄러 제거 중...
schtasks /delete /tn "S1_Trading_System" /f 2>nul

echo.
echo 새로운 S1 스케줄러 생성 중...

REM 현재 디렉토리 경로 가져오기
set "CURRENT_DIR=%CD%"

REM 매일 20:10에 실행하는 작업 스케줄 생성
schtasks /create ^
    /tn "S1_Trading_System" ^
    /tr "\"%CURRENT_DIR%\run_s1_system.bat\"" ^
    /sc daily ^
    /st 20:10 ^
    /f

if %ERRORLEVEL% EQU 0 (
    echo ✓ S1 스케줄러 생성 완료
    echo   - 작업명: S1_Trading_System
    echo   - 실행시간: 매일 20:10
    echo   - 실행파일: run_s1_system.bat
) else (
    echo ✗ S1 스케줄러 생성 실패
    pause
    exit /b 1
)

echo.
echo ========================================
echo S1 스케줄러 설정 완료
echo ========================================
echo.
echo 확인 방법:
echo   schtasks /query /tn "S1_Trading_System"
echo.
echo 수동 실행 방법:
echo   run_s1_system.bat
echo.
pause
