@echo off
chcp 65001 > nul
echo Google Cloud 업비트 알람 시스템 배포
echo.

REM Google Cloud CLI 설치 확인
gcloud --version >nul 2>&1
if %errorLevel% neq 0 (
    echo ❌ Google Cloud CLI가 설치되지 않았습니다.
    echo https://cloud.google.com/sdk/docs/install 에서 설치해주세요.
    pause
    exit /b 1
)

echo ✓ Google Cloud CLI 확인 완료
echo.

REM 프로젝트 ID 확인
echo 현재 프로젝트:
gcloud config get-value project
echo.

set /p PROJECT_ID="Google Cloud 프로젝트 ID를 입력하세요: "
if "%PROJECT_ID%"=="" (
    echo ❌ 프로젝트 ID가 필요합니다.
    pause
    exit /b 1
)

REM 프로젝트 설정
echo.
echo 프로젝트 설정 중...
gcloud config set project %PROJECT_ID%

REM Cloud Functions 배포
echo.
echo Cloud Functions 배포 중...
gcloud functions deploy upbit-alert-monitor ^
  --runtime python39 ^
  --trigger-http ^
  --entry-point cloud_function_handler ^
  --memory 256MB ^
  --timeout 300s ^
  --region asia-northeast3 ^
  --source . ^
  --allow-unauthenticated

if %errorLevel% neq 0 (
    echo ❌ Cloud Functions 배포 실패
    pause
    exit /b 1
)

echo ✓ Cloud Functions 배포 완료
echo.

REM 함수 URL 확인
echo 함수 URL:
gcloud functions describe upbit-alert-monitor --region=asia-northeast3 --format="value(httpsTrigger.url)"

echo.
echo 다음 단계:
echo 1. 환경 변수 설정 (TELEGRAM_TOKEN, TELEGRAM_CHAT_ID_ME)
echo 2. Cloud Scheduler 설정
echo 3. 테스트 실행
echo.

pause




















