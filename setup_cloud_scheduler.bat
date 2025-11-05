@echo off
chcp 65001 > nul
echo Google Cloud Scheduler 설정
echo.

REM 프로젝트 ID 확인
set /p PROJECT_ID="Google Cloud 프로젝트 ID를 입력하세요: "
if "%PROJECT_ID%"=="" (
    echo ❌ 프로젝트 ID가 필요합니다.
    pause
    exit /b 1
)

REM 함수 URL 확인
echo.
echo Cloud Functions URL 확인 중...
for /f "tokens=*" %%i in ('gcloud functions describe upbit-alert-monitor --region=asia-northeast3 --format="value(httpsTrigger.url)"') do set FUNCTION_URL=%%i

if "%FUNCTION_URL%"=="" (
    echo ❌ Cloud Functions URL을 찾을 수 없습니다.
    echo 먼저 deploy_to_google_cloud.bat을 실행해주세요.
    pause
    exit /b 1
)

echo ✓ 함수 URL: %FUNCTION_URL%
echo.

REM Cloud Scheduler 작업 생성
echo Cloud Scheduler 작업 생성 중...
gcloud scheduler jobs create http upbit-alert-schedule ^
  --schedule="*/30 9-18 * * 1-5" ^
  --uri="%FUNCTION_URL%" ^
  --http-method=GET ^
  --time-zone="Asia/Seoul" ^
  --description="업비트 시장 하락 감시 (30분 간격, 평일 09:00-18:00)"

if %errorLevel% neq 0 (
    echo ❌ Cloud Scheduler 작업 생성 실패
    pause
    exit /b 1
)

echo ✓ Cloud Scheduler 작업 생성 완료
echo.

echo 작업 정보:
echo - 작업 이름: upbit-alert-schedule
echo - 실행 간격: 30분마다
echo - 실행 시간: 평일 09:00-18:00
echo - 타임존: Asia/Seoul
echo.

echo 다음 단계:
echo 1. 환경 변수 설정
echo 2. 수동 테스트 실행
echo 3. 스케줄러 활성화
echo.

pause




















