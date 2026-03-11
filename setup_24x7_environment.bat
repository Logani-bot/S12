@echo off
chcp 65001 > nul
REM ========================================
REM 24/7 실행 환경 설정 스크립트
REM ========================================

echo ========================================
echo S12 24/7 실행 환경 설정
echo ========================================
echo.
echo 이 스크립트는 시스템 전원 설정을 변경합니다.
echo 관리자 권한이 필요할 수 있습니다.
echo.
pause

REM 절전 모드 비활성화
echo.
echo [1/3] 절전 모드 비활성화 중...
powercfg /change monitor-timeout-ac 0
powercfg /change monitor-timeout-dc 0
powercfg /change standby-timeout-ac 0
powercfg /change standby-timeout-dc 0
powercfg /change hibernate-timeout-ac 0
powercfg /change hibernate-timeout-dc 0
echo ✓ 완료
timeout /t 1 > nul

REM Never Sleep 설정
echo.
echo [2/3] "절전 안 함" 전원 스키마 설정 중...
powercfg /change monitor-timeout-ac 0
powercfg /change monitor-timeout-dc 0
powercfg /change standby-timeout-ac 0
powercfg /change standby-timeout-dc 0
powercfg /change hibernate-timeout-ac 0
powercfg /change hibernate-timeout-dc 0
echo ✓ 완료
timeout /t 1 > nul

REM 디스크 전원 관리 비활성화
echo.
echo [3/3] 디스크 전원 절약 모드 비활성화 중...
powercfg /change disk-timeout-ac 0
powercfg /change disk-timeout-dc 0
echo ✓ 완료

echo.
echo ========================================
echo ✅ 전원 설정 완료!
echo ========================================
echo.
echo 추가로 수동 설정이 필요한 항목:
echo   1. Windows 업데이트 자동 재시작 방지
echo   2. 네트워크 어댑터 전원 절약 모드 해제
echo.
echo 자세한 내용은 SETUP_SCHEDULER_MANUAL.md를 참조하세요.
echo.
pause


