@echo off
REM 시총 기반 종목 필터링 실행
REM 매일 시총 1조 5천억 이상 종목들을 가져와서 market_cap_universe.xlsx 생성

echo ========================================
echo 시총 기반 종목 필터링 시작
echo ========================================

python market_cap_filter.py --appkey %KIWOOM_APPKEY% --secret %KIWOOM_SECRET%

if %ERRORLEVEL% EQU 0 (
    echo ✓ 시총 필터링 완료
) else (
    echo ✗ 시총 필터링 실패
    pause
    exit /b 1
)

echo ========================================
echo 시총 필터링 완료
echo ========================================
