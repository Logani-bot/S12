@echo off
chcp 65001 > nul
echo 업비트 시장 하락 감시 시스템 (최적화 버전)
echo.

cd /d "%~dp0"

echo 설정 정보:
echo - 모니터링 간격: 30분
echo - API 호출 최적화: 배치 처리
echo - Google Cloud 지원
echo.

python upbit_alert_optimized.py

pause




















