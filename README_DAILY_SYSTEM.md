# S12 Trading System - 일일 리포트 가이드

## 📌 무엇을 실행해야 하나요?

### 🎯 빠른 실행
**`RUN_DAILY_SYSTEM.bat` 파일을 더블클릭하면 끝입니다!**

이 파일이 20:10에 자동 실행되는 것과 동일합니다.

---

## ⏰ 자동 실행 스케줄
- **매일 20:10**에 자동 실행됩니다
- `turnover_universe.xlsx` 업데이트
- `trading_signals.xlsx` 생성/업데이트

---

## 🔧 문제 해결

### Q: 패키지 오류가 발생합니다
```bash
pip install -r requirements.txt
```

### Q: 수동으로 실행하려면?
**`RUN_DAILY_SYSTEM.bat` 파일을 더블클릭하세요**

### Q: 20:10에 실행이 안 되는 이유는?
1. Windows 작업 스케줄러에 등록되어 있는지 확인
2. Python 패키지가 설치되어 있는지 확인 (`pip install -r requirements.txt`)
3. API 키가 올바른지 확인

---

## 📁 주요 파일

| 파일 | 설명 |
|------|------|
| `RUN_DAILY_SYSTEM.bat` | **👈 수동 실행용 (이 파일을 실행하세요)** |
| `run_trading_signal.bat` | 20:10 자동 실행 스크립트 |
| `Daily_Turnover_Tracker.py` | 거래대금 5000억+ 종목 추적 |
| `Trading_Signal_System.py` | 매매 시그널 생성 |
| `output/turnover_universe.xlsx` | 거래대금 유니버스 |
| `output/trading_signals.xlsx` | 매매 시그널 (Summary + History) |

---

## 🚀 처음 설치하기

```bash
# 1. Python 패키지 설치
pip install -r requirements.txt

# 2. 수동 실행 테스트
RUN_DAILY_SYSTEM.bat

# 3. 20:10 자동 실행 확인
# 로그 파일: logs/s12_daily_YYYYMMDD.log
```

---

## 📊 출력 파일

### `output/turnover_universe.xlsx`
- 거래대금 5000억 이상 종목
- 첫주도주, 최근주도주, 티커, 종목명, 거래대금(억), 누적횟수
- 매일 업데이트되는 유니버스

### `output/trading_signals.xlsx`
**Summary 탭:**
- 현재 추적 중인 종목들의 시그널
- 매수선/매도선, 이격도, 알람 상태

**History 탭:**
- 매도 완료된 종목들의 기록
- 실현수익률, 종료사유 포함

---

## 📝 로그 확인

로그 파일 위치: `logs/s12_daily_YYYYMMDD.log`

```bash
# 오늘 로그 확인
type logs\s12_daily_20251027.log

# 최근 로그 확인 (PowerShell)
Get-Content logs\s12_daily_*.log -Tail 50
```

---

## ⚙️ 스케줄러 관리

### 작업 스케줄러에서 확인
1. Windows 작업 스케줄러 열기
2. 작업 이름: `S12_Debug_Test` 또는 `S12*` 검색
3. 실행 시간, 최근 실행 결과 확인

### 스케줄러 재등록 (필요시)
아직 스케줄러가 설정되지 않았다면, 관리자 권한으로 실행:
```cmd
# 작업 스케줄러 등록 (20:10 실행)
schtasks /create /tn "S12_Daily" /tr "C:\Coding\S12\run_trading_signal.bat" /sc daily /st 20:10 /f
```

---

## 🆘 문제 해결 체크리스트

✅ Python이 설치되어 있나요? (`C:\Program Files (x86)\Python311\python.exe`)  
✅ 필수 패키지가 설치되어 있나요? (`pip install -r requirements.txt`)  
✅ API 키가 올바른가요? (Daily_Turnover_Tracker.py에서 확인)  
✅ 인터넷 연결이 되어 있나요?  
✅ output 폴더가 존재하나요? (`output/` 디렉토리)

---

## 📞 더 자세한 정보

- `S12_SYSTEM_GUIDE.md` - 전체 시스템 가이드
- `UPBIT_ALERT_GUIDE.md` - 업비트 알람 가이드
- `google_cloud_setup.md` - Google Cloud 설정










