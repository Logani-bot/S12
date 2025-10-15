@echo off
REM Trading Signal System S1 실행
REM 시총 기반 매매 시그널 생성 및 텔레그램 알림

echo ========================================
echo Trading Signal System S1 시작
echo ========================================

python Trading_Signal_System_S1.py --appkey %KIWOOM_APPKEY% --secret %KIWOOM_SECRET%

if %ERRORLEVEL% EQU 0 (
    echo ✓ S1 트레이딩 시그널 생성 완료
) else (
    echo ✗ S1 트레이딩 시그널 생성 실패
    pause
    exit /b 1
)

echo ========================================
echo S1 트레이딩 시그널 완료
echo ========================================
