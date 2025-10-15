@echo off
REM S1 시스템 테스트
REM 모든 S1 컴포넌트 테스트

echo ========================================
echo S1 시스템 테스트 시작
echo ========================================

echo [1/4] 시총 필터링 테스트...
python market_cap_filter.py --appkey %KIWOOM_APPKEY% --secret %KIWOOM_SECRET% --output output/test_market_cap_universe.xlsx

if %ERRORLEVEL% EQU 0 (
    echo ✓ 시총 필터링 테스트 성공
) else (
    echo ✗ 시총 필터링 테스트 실패
    pause
    exit /b 1
)

echo.
echo [2/4] S1 트레이딩 시그널 테스트...
python Trading_Signal_System_S1.py --appkey %KIWOOM_APPKEY% --secret %KIWOOM_SECRET% --universe output/test_market_cap_universe.xlsx --signal output/test_trading_signals_s1.xlsx

if %ERRORLEVEL% EQU 0 (
    echo ✓ S1 트레이딩 시그널 테스트 성공
) else (
    echo ✗ S1 트레이딩 시그널 테스트 실패
    pause
    exit /b 1
)

echo.
echo [3/4] 텔레그램 알림 S1 테스트...
python telegram_notifier_s1.py

if %ERRORLEVEL% EQU 0 (
    echo ✓ 텔레그램 알림 S1 테스트 성공
) else (
    echo ✗ 텔레그램 알림 S1 테스트 실패
    pause
    exit /b 1
)

echo.
echo [4/4] 파일 생성 확인...
if exist "output\test_market_cap_universe.xlsx" (
    echo ✓ 시총 유니버스 파일 생성 확인
) else (
    echo ✗ 시총 유니버스 파일 없음
)

if exist "output\test_trading_signals_s1.xlsx" (
    echo ✓ S1 트레이딩 시그널 파일 생성 확인
) else (
    echo ✗ S1 트레이딩 시그널 파일 없음
)

echo.
echo ========================================
echo S1 시스템 테스트 완료
echo ========================================
echo.
echo 생성된 테스트 파일들:
echo   - output\test_market_cap_universe.xlsx
echo   - output\test_trading_signals_s1.xlsx
echo.
echo 실제 운영 시에는 다음 파일들을 사용:
echo   - output\market_cap_universe.xlsx
echo   - output\trading_signals_s1.xlsx
echo.
pause
