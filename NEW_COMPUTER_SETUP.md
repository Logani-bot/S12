# 새 컴퓨터 24/7 실행 환경 구축 가이드

## 1단계: 기본 소프트웨어 설치

### Python 설치
- [ ] Python 3.11 설치 (현재 사용 중인 버전과 동일)
  - 설치 경로: `C:\Program Files (x86)\Python311\`
  - ⚠️ "Add Python to PATH" 체크
  - ⚠️ "Install for all users" 체크

### Git 설치 (선택사항, 코드 동기화용)
- [ ] Git for Windows 설치
- [ ] GitHub 계정 연동 (private repo 권장)

---

## 2단계: 프로젝트 파일 이전

### 방법 A: USB/외장하드 복사 (권장)
```
전체 S12 폴더를 C:\Coding\S12\로 복사
```

### 방법 B: Git 동기화
```bash
cd C:\Coding
git clone [repository-url] S12
cd S12
```

### 필수 파일 확인
- [ ] `.env` 파일 (API 키, Telegram 토큰)
- [ ] `output/turnover_universe.xlsx`
- [ ] `output/trading_signals.xlsx`
- [ ] 모든 `.py`, `.bat` 파일들

---

## 3단계: Python 패키지 설치

```bash
cd C:\Coding\S12
pip install -r requirements.txt
```

만약 requirements.txt가 없다면:
```bash
pip install requests python-dotenv pandas openpyxl holidays
```

---

## 4단계: .env 파일 설정

`C:\Coding\S12\.env` 파일 생성 및 확인:

```env
# Kiwoom API
KIWOOM_APPKEY=IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU
KIWOOM_SECRET=eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs

# Telegram Bot
TELEGRAM_TOKEN=[현재 사용 중인 봇 토큰]
TELEGRAM_CHAT_ID_ME=[현재 Chat ID]
TELEGRAM_CHAT_ID_YOONJOO=[현재 Chat ID]
TELEGRAM_CHAT_ID_MINJEONG=[현재 Chat ID]
TELEGRAM_CHAT_ID_JUMEONI=[현재 Chat ID]
```

---

## 5단계: 테스트 실행

### 5.1 거래일 체크 테스트
```bash
python trading_day_utils.py
```
예상 출력: 오늘 날짜, 거래일 여부, 다음 거래일

### 5.2 Daily Turnover Tracker 테스트
```bash
python Daily_Turnover_Tracker.py --appkey [KEY] --secret [SECRET] --test
```

### 5.3 Trading Signal System 테스트 (force 모드)
```bash
python Trading_Signal_System.py --appkey [KEY] --secret [SECRET] --alert-threshold 10.0 --force
```

### 5.4 Telegram 알람 테스트
```bash
python test_telegram.py  # 테스트 스크립트 필요 시 생성
```

---

## 6단계: Windows 작업 스케줄러 설정

### 6.1 작업 스케줄러 열기
- Win + R → `taskschd.msc` → Enter

### 6.2 새 작업 만들기

**작업 1: Daily Turnover + Signal (매일 20:10)**
- 이름: `S2_Daily_Trading_Signal`
- 트리거: 매일 20:10
- 동작: `C:\Coding\S12\RUN_DAILY_SYSTEM.bat`
- 조건:
  - [ ] "컴퓨터의 전원이 켜져 있을 때만 작업 시작" 해제
  - [ ] "AC 전원일 때만 시작" 해제
- 설정:
  - [ ] "작업이 실패하면 다시 시작" 활성화 (3번, 1분 간격)

**작업 2: Real-Time Monitor (평일 08:00)**
- 이름: `S2_Realtime_Monitor`
- 트리거: 월-금 08:00
- 동작: `C:\Coding\S12\run_real_time_monitor.bat`
- 조건: 위와 동일

### 6.3 작업 검증
- [ ] 작업 목록에서 "실행" 버튼으로 수동 테스트
- [ ] 로그 파일 확인 (`logs/` 폴더)
- [ ] Telegram 메시지 도착 확인

---

## 7단계: 24/7 실행 환경 설정

### Windows 절전 모드 비활성화
- 설정 → 시스템 → 전원 및 절전
  - [ ] "화면 끄기": 안 함
  - [ ] "절전 모드": 안 함

### Windows 업데이트 자동 재시작 방지
- 설정 → 업데이트 및 보안 → Windows Update → 고급 옵션
  - [ ] "활성 시간" 설정: 00:00 - 23:59
  - [ ] "자동 재시작" 해제

### 네트워크 연결 유지
- 제어판 → 네트워크 및 공유 센터 → 어댑터 설정 변경
- 네트워크 어댑터 우클릭 → 속성 → 구성
  - [ ] 전원 관리 → "전원 절약을 위해 컴퓨터가 이 장치를 끌 수 있음" 해제

### Excel 자동 잠금 방지
- Excel 실행 안 함 (스크립트 실행 중 Excel 파일 열지 않기)
- 필요 시 Excel Online으로 확인

---

## 8단계: 모니터링 및 백업

### 로그 모니터링
- [ ] 매일 `logs/s12_daily_YYYYMMDD.log` 확인
- [ ] 에러 발생 시 Telegram 알람 확인

### 데이터 백업
- [ ] 주 1회 `output/` 폴더 전체 백업
- [ ] Google Drive / Dropbox 자동 백업 설정 권장

### 원격 접근 설정 (선택사항)
- [ ] Windows 원격 데스크톱 활성화
- [ ] TeamViewer / AnyDesk 설치

---

## 9단계: 장애 대응 체크리스트

### API 토큰 만료
- 증상: "토큰 획득 실패" 로그
- 해결: Kiwoom 계정 로그인 상태 확인, API 키 재발급

### Excel 파일 잠김
- 증상: "Permission denied" 에러
- 해결: Excel 프로그램 종료 후 재실행

### 인터넷 연결 끊김
- 증상: "Connection timeout" 로그
- 해결: 네트워크 재연결, 라우터 재부팅

### Telegram 알람 미도착
- 증상: 스크립트는 성공했으나 메시지 없음
- 해결: `.env` 파일의 TELEGRAM_TOKEN, CHAT_ID 확인

---

## 10단계: 최종 검증

### 48시간 연속 실행 테스트
- [ ] 수동으로 2일간 모니터링
- [ ] 매일 20:10 알람 도착 확인
- [ ] Real-time 알람 정상 작동 확인
- [ ] 로그 파일 에러 없음 확인

### 성공 기준
- ✅ 2일 연속 에러 없이 실행
- ✅ Telegram 알람 정상 수신
- ✅ Excel 파일 정상 업데이트
- ✅ 시스템 재부팅 후에도 자동 실행

---

## 문제 해결 연락처

- Kiwoom API 지원: [고객센터 번호]
- Telegram Bot 이슈: @BotFather
- Python 환경 이슈: ChatGPT / Claude Code

---

**구축 완료 예상 시간: 2-4시간**
**안정화 기간: 2-3일**
