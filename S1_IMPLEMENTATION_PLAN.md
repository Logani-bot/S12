# S1 시스템 구현 계획

## 시스템 개요

**S1**: 시가총액 기준 고유동성 종목 매매 시스템
**S2 (현재)**: 거래대금 기준 고회전율 종목 매매 시스템

---

## 핵심 차이점

| 구분 | S1 | S2 (현재) |
|------|----|----|
| **종목 선정 기준** | 시가총액 상위 N개 | 거래대금 5000억+ |
| **Universe 파일** | `s1_universe.xlsx` | `turnover_universe.xlsx` |
| **Signal 파일** | `s1_signals.xlsx` | `trading_signals.xlsx` |
| **Telegram 표시** | `[S1]` 접두사 | `[S2]` 접두사 |
| **로그 파일** | `logs/s1_daily_*.log` | `logs/s12_daily_*.log` |

**매매 로직**: S2와 100% 동일
- 20일 이평선 엔벨로프
- 3단계 분할 매수/매도
- 익일 기준 매수선 계산 (`predict_next_day_buy_price`)

---

## 구현 단계

### Step 1: S1 폴더 구조 생성

```bash
mkdir C:\Coding\S1
mkdir C:\Coding\S1\output
mkdir C:\Coding\S1\logs
```

### Step 2: 기존 S2 코드 복사 및 수정

#### 2.1 공통 모듈 복사 (수정 없음)
- `telegram_notifier.py`
- `trading_day_utils.py`
- `.env`

#### 2.2 Daily Tracker 수정
**파일명**: `Daily_Tracker_S1.py`

**변경 사항**:
```python
# 기존: Daily_Turnover_Tracker.py
# 변경: API 엔드포인트를 시가총액 조회로 변경

# Before
API_ENDPOINT = "/api/dostk/rkinfo"  # 거래대금 순위
API_ID = "ka10032"

# After
API_ENDPOINT = "/api/dostk/rkinfo"  # 시가총액 순위
API_ID = "ka10025"  # 시가총액 순위 API (키움 문서 확인 필요)

# 파일명 변경
OUTPUT_FILE = "output/s1_universe.xlsx"

# 로그 파일명
LOG_FILE = f"logs/s1_daily_{datetime.now().strftime('%Y%m%d')}.log"

# Telegram 메시지에 [S1] 표시
send_daily_report(f"[S1] 시가총액 기준 종목 업데이트 완료")
```

#### 2.3 Trading Signal System 수정
**파일명**: `Trading_Signal_S1.py`

**변경 사항**:
```python
# 입력 파일
DEFAULT_UNIVERSE_FILE = "output/s1_universe.xlsx"

# 출력 파일
DEFAULT_SIGNAL_FILE = "output/s1_signals.xlsx"

# 로그 파일
LOG_FILE = f"logs/s1_signal_{datetime.now().strftime('%Y%m%d')}.log"

# Telegram 메시지 수정
def send_daily_report(alerts, total_count, recipients):
    message = f"🔔 [S1] 일일 트레이딩 시그널 리포트\n\n"
    # ... 기존 로직 동일
```

#### 2.4 Real-Time Monitor 수정
**파일명**: `Real_Time_Monitor_S1.py`

**변경 사항**:
```python
DEFAULT_SIGNAL_FILE = "output/s1_signals.xlsx"
LOG_FILE = f"logs/s1_realtime_{datetime.now().strftime('%Y%m%d')}.log"

# Telegram 알람에 [S1] 표시
send_alert(f"[S1] {ticker} {종목명}: {알람내용}")
```

### Step 3: Batch 파일 생성

#### `C:\Coding\S1\RUN_S1_DAILY.bat`
```batch
@echo off
cd /d C:\Coding\S1
"C:\Program Files (x86)\Python311\python.exe" Daily_Tracker_S1.py --appkey [KEY] --secret [SECRET]
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

"C:\Program Files (x86)\Python311\python.exe" Trading_Signal_S1.py --appkey [KEY] --secret [SECRET] --alert-threshold 10.0
```

#### `C:\Coding\S1\run_s1_realtime.bat`
```batch
@echo off
cd /d C:\Coding\S1
"C:\Program Files (x86)\Python311\python.exe" Real_Time_Monitor_S1.py --appkey [KEY] --secret [SECRET]
```

### Step 4: Windows 작업 스케줄러 등록

**작업 1**: S1_Daily_Signal
- 시간: 매일 20:15 (S2보다 5분 늦게)
- 스크립트: `C:\Coding\S1\RUN_S1_DAILY.bat`

**작업 2**: S1_Realtime_Monitor
- 시간: 월-금 08:05 (S2보다 5분 늦게)
- 스크립트: `C:\Coding\S1\run_s1_realtime.bat`

---

## Telegram 메시지 포맷

### S1 메시지 예시
```
🔔 [S1] 일일 트레이딩 시그널 리포트

📊 총 50개 종목 분석
⚡ 5개 알람

🔴 [S1] 삼성전자 (005930)
1차 매수선까지 3.2% (접근 중!)
현재가: 107,700원
1차 매수선: 104,300원

...
```

### S2 메시지 예시 (기존)
```
🔔 [S2] 일일 트레이딩 시그널 리포트

📊 총 79개 종목 분석
⚡ 8개 알람

🔴 [S2] 카카오 (035720)
1차 매수선까지 2.1% (접근 중!)
현재가: 65,500원
1차 매수선: 64,200원

...
```

---

## API 엔드포인트 조사 필요

### 시가총액 순위 API
- **API ID**: `ka10025` (추정, 키움 문서 확인 필요)
- **엔드포인트**: `/api/dostk/rkinfo`
- **파라미터**:
  ```json
  {
    "sort_tp": "1",  // 시가총액 정렬
    "mk_tp": "0",    // 전체 시장
    "stt_idx": "1",  // 시작 인덱스
    "end_idx": "100" // 종료 인덱스 (상위 100개)
  }
  ```

**⚠️ 주의**: 실제 API 사양은 키움증권 API 문서 확인 필요

---

## 테스트 계획

### 1단계: 로컬 테스트 (현재 컴퓨터)
- [ ] Daily_Tracker_S1.py 실행
- [ ] s1_universe.xlsx 생성 확인
- [ ] Trading_Signal_S1.py 실행 (--force)
- [ ] s1_signals.xlsx 생성 확인
- [ ] Telegram 메시지 [S1] 표시 확인

### 2단계: 병렬 실행 테스트
- [ ] S2 Daily (20:10) 실행
- [ ] S1 Daily (20:15) 실행
- [ ] 양쪽 모두 Telegram 메시지 수신 확인
- [ ] 파일 충돌 없음 확인

### 3단계: 새 컴퓨터 이전
- [ ] S2 안정화 완료 후
- [ ] S1 폴더 전체 복사
- [ ] 스케줄러 등록
- [ ] 1주일 모니터링

---

## 예상 일정

| 단계 | 작업 | 소요시간 | 의존성 |
|------|------|---------|--------|
| 1 | 새 컴퓨터 S2 세팅 | 1일 | - |
| 2 | S2 안정화 확인 | 2-3일 | 1 완료 |
| 3 | S1 코드 작성 | 2-3시간 | 2 완료 |
| 4 | S1 로컬 테스트 | 1-2시간 | 3 완료 |
| 5 | S1 병렬 실행 테스트 | 1일 | 4 완료 |
| 6 | S1 새 컴퓨터 이전 | 1시간 | 5 완료 |
| 7 | S1 안정화 확인 | 1주일 | 6 완료 |

**총 예상 기간**: 약 2주

---

## 리스크 관리

### 높은 리스크
- ❌ S2 코드 수정 → **절대 수정 안 함**
- ❌ 동일 파일 동시 쓰기 → **별도 폴더/파일 사용**

### 중간 리스크
- ⚠️ API 호출 중복 → **5분 간격 실행으로 회피**
- ⚠️ Telegram 메시지 혼동 → **[S1]/[S2] 명확히 표시**

### 낮은 리스크
- ✅ 코드 중복 관리 → **변경 빈도 낮음, 수동 동기화 가능**

---

## 향후 확장 계획

### S3, S4 추가 시
동일한 패턴으로 폴더 복사:
```
C:\Coding\S3\  # 변동성 기반 시스템
C:\Coding\S4\  # 모멘텀 기반 시스템
```

각 시스템별 독립 실행, Telegram에서 통합 모니터링

---

**문서 작성일**: 2025-11-02
**작성자**: Claude Code
