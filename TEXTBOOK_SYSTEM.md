# 📚 S12 Trading System - 교과서 (TEXTBOOK)

**이 문서를 외워서 매번 물어보지 말고 여기를 보세요!**

## 🎯 빠른 참조

### Q: 엑셀 파일 업데이트하려면?
**A: `UPDATE_EXCEL_NOW.bat` 더블클릭하세요**

### Q: 20:10에 뭐가 자동 실행되나?
**A: `run_trading_signal.bat`가 두 개의 Python 스크립트를 실행:**
1. `Daily_Turnover_Tracker.py` → `turnover_universe.xlsx` 업데이트
2. `Trading_Signal_System.py` → `trading_signals.xlsx` 생성/업데이트

### Q: 텔레그램으로 뭐가 전송되나?
**A: 일일 리포트 (매일 20:10 자동 실행 후)**
- 1차 매수 접근 종목 (10% 이내)
- 매수 완료 종목 (수익률 포함)
- 매도선 접근 종목
- 전체 종목 수, 알람 대상 수

---

## 📂 파일 구조

### 실행 파일 (이것들만 기억하세요!)

| 파일 | 용도 | 언제 사용? |
|------|------|-----------|
| `UPDATE_EXCEL_NOW.bat` | **엑셀 바로 업데이트** | 필요할 때 수동 실행 |
| `run_trading_signal.bat` | 20:10 자동 실행 | 자동 (스케줄러가 실행) |

### Python 스크립트

| 파일 | 설명 |
|------|------|
| `Daily_Turnover_Tracker.py` | 거래대금 5000억+ 종목 추적 |
| `Trading_Signal_System.py` | 매매 시그널 생성 + 텔레그램 전송 |
| `telegram_notifier.py` | 텔레그램 전송 모듈 |

### 출력 파일

| 파일 | 설명 | 업데이트 주기 |
|------|------|--------------|
| `output/turnover_universe.xlsx` | 거래대금 5000억+ 종목 리스트 | 매일 20:10 |
| `output/trading_signals.xlsx` | 매매 시그널 (Summary + History) | 매일 20:10 |

---

## ⚙️ 20:10 자동 실행 시스템

### 실행 순서
1. **Daily_Turnover_Tracker.py**
   - 키움 API로 거래대금 순위 조회
   - 5000억 이상 종목 필터링
   - `turnover_universe.xlsx` 업데이트

2. **Trading_Signal_System.py**
   - `turnover_universe.xlsx` 읽기
   - 종목별 20일선 분석, 매수/매도선 계산
   - `trading_signals.xlsx` 생성/업데이트
   - **텔레그램 일일 리포트 전송** ← 여기!

### 텔레그램 전송 내용
```
📊 일일 트레이딩 리포트
🕐 2025-10-27 20:10
───────────────

🟡 1차 매수 접근 중 (2개)
  • 삼성전자
    현재가: 65,000원
    매수가: 62,000원
    이격도: 4.8%

🔴 매수 완료 종목 (1개)
  • 현대차
    현재가: 240,000원
    평균가: 220,000원
    이격도: +9.1%

🟢 매도선 접근 (1개)
  • SK하이닉스
    현재가: 120,000원
    목표가: 123,000원
    이격도: +2.5%
```

---

## 🔧 문제 해결 체크리스트

### Q: 패키지 오류?
```bash
pip install -r requirements.txt
```

### Q: 실행이 안 될 때?
1. Python 설치 확인: `C:\Program Files (x86)\Python311\python.exe`
2. 패키지 설치: `pip install -r requirements.txt`
3. API 키 확인: `Daily_Turnover_Tracker.py` 파일 상단

### Q: 텔레그램이 안 올 때?
1. `.env` 파일 확인
2. Telegram Token, Chat IDs 설정 확인
3. 로그 파일 확인: `logs/s12_daily_YYYYMMDD.log`

---

## 📊 주요 파일 위치

```
C:\Coding\S12\
├── Daily_Turnover_Tracker.py      (거래대금 추적)
├── Trading_Signal_System.py        (시그널 생성 + 텔레그램 전송)
├── telegram_notifier.py            (텔레그램 모듈)
├── run_trading_signal.bat          (20:10 자동 실행)
├── UPDATE_EXCEL_NOW.bat            (수동 실행용)
├── trading_day_utils.py            (거래일 체크)
├── .env                             (환경 변수)
├── output/
│   ├── turnover_universe.xlsx       (거래대금 유니버스)
│   └── trading_signals.xlsx        (매매 시그널)
└── logs/
    └── s12_daily_YYYYMMDD.log      (실행 로그)
```

---

## 🎓 핵심 정리

### 1. 매일 20:10에 자동으로
- `turnover_universe.xlsx` 업데이트
- `trading_signals.xlsx` 생성
- 텔레그램 일일 리포트 전송

### 2. 수동 업데이트하려면
- `UPDATE_EXCEL_NOW.bat` 실행

### 3. 문제 생기면
- `README_DAILY_SYSTEM.md` 확인
- 이 파일(`TEXTBOOK_SYSTEM.md`) 확인

---

## 💡 자주 묻는 질문

**Q: 왜 업데이트가 안 되나요?**
A: Python 패키지가 설치 안 되어 있거나, API 키가 잘못되었거나, 인터넷 연결이 안 됨

**Q: 텔레그램이 왜 안 오나요?**
A: `.env` 파일의 Chat ID가 잘못되었거나, 텔레그램 봇 토큰이 없거나, 실행 에러 발생

**Q: 수동으로 실행하려면?**
A: `UPDATE_EXCEL_NOW.bat` 더블클릭

---

**이제 이 파일을 북마크하세요! 매번 물어보지 마세요!** 📌









