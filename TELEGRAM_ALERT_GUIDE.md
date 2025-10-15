# 📱 텔레그램 알람 사용 가이드

## 🎯 알람이 오는 경우

Trading Signal System은 다음 조건에서 알람을 보냅니다:

### 1️⃣ **1차 매수선 접근** 🟡
- 현재가가 1차 매수선까지 **0~10% 이내**로 접근했을 때
- 예: 1차 매수선 10,000원, 현재가 10,800원 → 이격도 +8% → **알람!**

### 2️⃣ **2차/3차 매수선 접근** 🟠🔴
- 1차 매수 후, 2차 매수선까지 0~10% 이내
- 2차 매수 후, 3차 매수선까지 0~10% 이내

### 3️⃣ **매도선 접근** 🟢
- 매수 후, +3%/+5%/+7% 매도선까지 10% 이내로 접근했을 때

---

## 📊 현재 알람 대상 (2025-10-14 기준)

현재 **2개 종목**이 알람 조건을 만족하고 있습니다:

| 종목 | 상태 | 메시지 |
|------|------|--------|
| **HJ중공업** | READY_BUY1 | 1차 매수선까지 8.8% (접근 중!) |
| **파인엠텍** | READY_BUY1 | 1차 매수선까지 9.2% (접근 중!) |

---

## 🚀 사용 방법

### 방법 1: 수동 실행 (즉시 알람 받기)

```bash
# 기본 실행 (임계값 10%)
python Trading_Signal_System.py --appkey [YOUR_KEY] --secret [YOUR_SECRET]

# 민감도 높이기 (임계값 3% - 더 강한 조건)
python Trading_Signal_System.py --appkey [YOUR_KEY] --secret [YOUR_SECRET] --alert-threshold 3

# 민감도 낮추기 (임계값 20% - 더 많은 알람)
python Trading_Signal_System.py --appkey [YOUR_KEY] --secret [YOUR_SECRET] --alert-threshold 20
```

또는 간단하게:
```bash
run_trading_signal.bat
```

### 방법 2: 자동 실행 (매일 20:10)

**1단계: 스케줄러 등록**
```bash
# 관리자 권한으로 실행 (우클릭 -> 관리자 권한으로 실행)
setup_scheduler.bat
```

**2단계: 등록 확인**
```bash
# Windows 작업 스케줄러 열기
작업 스케줄러 -> 작업 스케줄러 라이브러리 -> "S12_Trading_Signal_Daily" 확인
```

**3단계: 완료!**
- 매일 20:10에 자동으로 분석
- 텔레그램으로 알람 수신

---

## 📱 텔레그램 메시지 형식

### 알람 대상이 있을 때

```
📊 일일 트레이딩 리포트
🕐 2025-10-14 20:10
───────────────

🟡 1차 매수 접근 중 (2개)
  • HJ중공업
    현재가: 6,520원
    매수가: 6,000원
    이격도: 8.8%

  • 파인엠텍
    현재가: 12,450원
    매수가: 11,400원
    이격도: 9.2%
```

### 알람 대상이 없을 때

```
📊 일일 트레이딩 리포트
🕐 2025-10-14 20:10
───────────────

✅ 총 66개 종목 분석
🔕 알람 대상 없음
```

---

## ⚙️ 알람 임계값 조정

`--alert-threshold` 옵션으로 알람 민감도를 조정할 수 있습니다:

| 임계값 | 의미 | 추천 용도 |
|--------|------|-----------|
| **3%** | 매우 가까움 (강한 조건) | 즉각 대응이 필요한 경우 |
| **10%** (기본) | 접근 중 (표준) | 일반적인 모니터링 |
| **20%** | 여유 있음 (약한 조건) | 조기 경고가 필요한 경우 |

**예시:**
```bash
# 3% 이내만 알람 (강함)
python Trading_Signal_System.py --appkey ... --alert-threshold 3

# 10% 이내 알람 (기본)
python Trading_Signal_System.py --appkey ... --alert-threshold 10

# 20% 이내 알람 (약함)
python Trading_Signal_System.py --appkey ... --alert-threshold 20
```

---

## 🔧 문제 해결

### 텔레그램 알람이 안 올 때

1. **환경 변수 확인**
   - `.env` 파일에 `TELEGRAM_TOKEN`과 `TELEGRAM_CHAT_ID_ME`가 설정되어 있는지 확인

2. **telegram_notifier.py 확인**
   ```bash
   python telegram_notifier.py
   ```
   테스트 메시지가 오는지 확인

3. **로그 확인**
   - 콘솔에 `✓ 텔레그램 일일 리포트 전송 완료` 메시지가 나오는지 확인

### 자동 실행이 안 될 때

1. **작업 스케줄러 확인**
   - Windows 작업 스케줄러에서 "S12_Trading_Signal_Daily" 작업이 있는지 확인
   - 실행 기록 확인 (우클릭 -> 속성 -> 기록)

2. **수동 실행 테스트**
   ```bash
   run_trading_signal.bat
   ```
   수동으로 실행되는지 먼저 확인

---

## 📝 참고

- **실행 시간**: 매일 20:10 (장 마감 후)
- **분석 대상**: `turnover_universe.xlsx`의 66개 종목
- **결과 저장**: `trading_signals.xlsx` (Summary + History)
- **알람 방식**: 텔레그램 메시지 (HTML 포맷)
- **수신자**: 기본적으로 본인만 (`recipients=["me"]`)

---

## 🎓 추가 정보

더 자세한 내용은 다음 문서를 참고하세요:
- `TRADING_SYSTEM_GUIDE.md` - 전체 트레이딩 시스템 가이드
- `README.md` - 프로젝트 개요
- `telegram_notifier.py` - 텔레그램 알람 모듈 소스 코드






