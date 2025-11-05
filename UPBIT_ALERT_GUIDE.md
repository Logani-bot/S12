# 🚨 업비트 알람 시스템 가이드

## 📋 시스템 개요

업비트에서 전일대비 15% 이상 하락한 종목이 15개 이상일 때 텔레그램으로 알람을 보내는 시스템입니다.

## 🎯 주요 기능

- **실시간 모니터링**: 30분 간격으로 업비트 시장 감시
- **스마트 알람**: 15개 이상 하락 시에만 알람 발송
- **중복 방지**: 같은 날 한 번만 알람
- **Google Cloud 지원**: 클라우드 환경에서 안정적 실행

## 📱 알람 메시지 형식

```
코인 저점 시그널 알림
──────────────────

전일대비 15% 이상 하락: 18개
하락 종목 : BTC ETH ADA DOT LINK MATIC SOL AVAX LUNA ATOM
```

## 🚀 배포 방법

### 1. 로컬 실행 (테스트용)

```bash
# 최적화 버전 실행
python upbit_alert_optimized.py

# 또는 배치 파일 실행
run_upbit_optimized.bat
```

### 2. Google Cloud 배포

#### **2-1. Cloud Functions 배포**
```bash
# 자동 배포 스크립트 실행
deploy_to_google_cloud.bat

# 또는 수동 배포
gcloud functions deploy upbit-alert-monitor \
  --runtime python39 \
  --trigger-http \
  --entry-point cloud_function_handler \
  --memory 256MB \
  --timeout 300s \
  --region asia-northeast3
```

#### **2-2. Cloud Scheduler 설정**
```bash
# 자동 스케줄러 설정
setup_cloud_scheduler.bat

# 또는 수동 설정
gcloud scheduler jobs create http upbit-alert-schedule \
  --schedule="*/30 9-18 * * 1-5" \
  --uri="YOUR_FUNCTION_URL" \
  --http-method=GET \
  --time-zone="Asia/Seoul"
```

#### **2-3. 환경 변수 설정**
```bash
gcloud functions deploy upbit-alert-monitor \
  --update-env-vars \
  TELEGRAM_TOKEN=your_telegram_bot_token,\
  TELEGRAM_CHAT_ID_ME=your_telegram_chat_id
```

### 3. 테스트

```bash
# Cloud Function 테스트
python test_cloud_function.py
```

## ⚙️ 설정 정보

### **모니터링 설정**
- **실행 간격**: 30분
- **실행 시간**: 09:00 - 18:00 (평일)
- **하락 임계값**: 15% 이상
- **알람 조건**: 15개 이상 하락

### **API 최적화**
- **배치 처리**: 100개씩 나누어 조회
- **호출 간격**: 100ms 대기
- **하루 API 호출**: 36회 (30분 간격)

### **비용 최적화**
- **예상 월 비용**: $0.50 이하
- **API 호출 75% 감소**
- **Google Cloud 무료 티어 활용**

## 📊 모니터링

### **API 호출 통계**
시스템이 자동으로 다음 통계를 로깅합니다:
- 총 API 호출 횟수
- 성공/실패 횟수
- 마지막 호출 시간

### **로그 확인**
```bash
# Google Cloud 로그 확인
gcloud functions logs read upbit-alert-monitor --region=asia-northeast3

# 로컬 로그 확인
# 콘솔 출력 또는 로그 파일
```

## 🔧 문제 해결

### **일반적인 문제**

1. **텔레그램 알람이 오지 않음**
   - 환경 변수 확인 (TELEGRAM_TOKEN, TELEGRAM_CHAT_ID_ME)
   - 텔레그램 봇 토큰 유효성 확인

2. **API 호출 실패**
   - 네트워크 연결 확인
   - 업비트 API 상태 확인

3. **Cloud Function 실행 실패**
   - 함수 URL 확인
   - 권한 설정 확인

### **디버깅**

```bash
# 로컬 디버깅
python upbit_alert_optimized.py

# Cloud Function 로그 확인
gcloud functions logs read upbit-alert-monitor --region=asia-northeast3 --limit=50
```

## 📈 성능 최적화

### **권장 설정**

1. **개발/테스트**: 30분 간격
2. **운영**: 30분 간격 (09:00-18:00)
3. **긴급 모니터링**: 15분 간격 (시장 급변 시)

### **확장 가능성**

- **다중 알람 조건**: 다양한 하락률 임계값
- **다중 수신자**: 여러 텔레그램 채팅방
- **다른 거래소**: 바이낸스, 코인원 등 추가

## 🎉 완료!

이제 업비트 시장 급락을 실시간으로 감지하고 텔레그램으로 알람을 받을 수 있습니다!




















