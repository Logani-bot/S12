# Google Cloud 업비트 알람 시스템 설정 가이드

## 🎯 최적화된 설정

### ⚙️ **주요 개선사항**

1. **API 호출 최적화**
   - 모니터링 간격: 10분 → **30분** (하루 28회 → 18회)
   - 배치 처리: 100개씩 나누어 조회 (API 제한 고려)
   - 호출 간격: 100ms 대기로 과도한 요청 방지

2. **Google Cloud 최적화**
   - Cloud Functions 지원
   - Cloud Scheduler 연동
   - 환경 변수 자동 감지
   - API 호출 통계 모니터링

3. **비용 최적화**
   - 하루 API 호출: 144회 → **36회** (75% 감소)
   - 실행 시간: 09:00-18:00 (9시간)
   - 중복 알람 방지

## 🚀 Google Cloud 배포 방법

### 1. Cloud Functions 배포

```bash
# requirements.txt 생성
echo "requests==2.31.0" > requirements.txt

# Cloud Functions 배포
gcloud functions deploy upbit-alert-monitor \
  --runtime python39 \
  --trigger-http \
  --entry-point cloud_function_handler \
  --memory 256MB \
  --timeout 300s \
  --set-env-vars TELEGRAM_TOKEN=your_token,TELEGRAM_CHAT_ID_ME=your_chat_id
```

### 2. Cloud Scheduler 설정

```bash
# 30분마다 실행 (09:00-18:00)
gcloud scheduler jobs create http upbit-alert-schedule \
  --schedule="*/30 9-18 * * 1-5" \
  --uri="https://your-region-your-project.cloudfunctions.net/upbit-alert-monitor" \
  --http-method=GET \
  --time-zone="Asia/Seoul"
```

### 3. 환경 변수 설정

```bash
# 환경 변수 설정
gcloud functions deploy upbit-alert-monitor \
  --update-env-vars \
  TELEGRAM_TOKEN=your_telegram_bot_token,\
  TELEGRAM_CHAT_ID_ME=your_telegram_chat_id
```

## 📊 비용 분석

### **API 호출 비용**
- **기존**: 10분 간격 = 144회/일
- **최적화**: 30분 간격 = 36회/일
- **절약**: 75% 감소

### **Google Cloud 비용**
- **Cloud Functions**: 월 $0.40 (무료 티어 포함)
- **Cloud Scheduler**: 월 $0.10
- **총 예상 비용**: 월 $0.50 이하

## 🔧 로컬 테스트

```bash
# 최적화 버전 테스트
python upbit_alert_optimized.py

# API 호출 통계 확인
# 로그에서 "API 호출 통계" 확인
```

## 📱 알람 메시지 (동일)

```
코인 저점 시그널 알림
──────────────────

전일대비 15% 이상 하락: 18개
하락 종목 : BTC ETH ADA DOT LINK MATIC SOL AVAX LUNA ATOM
```

## 🎯 권장 설정

1. **개발/테스트**: 30분 간격
2. **운영**: 30분 간격 (09:00-18:00)
3. **긴급 모니터링**: 15분 간격 (시장 급변 시)

## 📈 모니터링

- API 호출 통계 자동 로깅
- 실패율 모니터링
- 알람 발송 이력 추적
- Google Cloud 로그 확인




















