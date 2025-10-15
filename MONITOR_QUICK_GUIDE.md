# 실시간 모니터링 빠른 가이드

## ❓ Cursor를 끄면 모니터링도 꺼지나요?

**아니요! Cursor를 종료해도 모니터링은 계속 실행됩니다.** ✅

- Cursor는 단순히 코드 편집기입니다
- 실시간 모니터링은 독립적인 Python 프로세스로 실행됩니다
- **컴퓨터를 끄면** 모니터링도 종료됩니다

---

## 📊 상태 확인

### 간단 확인
```powershell
tasklist | Select-String "python"
```
- 결과 있음 = ✅ 실행 중
- 결과 없음 = ❌ 중지됨

### 상세 확인 (최근 로그)
```powershell
Get-Content realtime_monitor_*.log -Tail 20
```

---

## 🚀 시작하기

### 백그라운드 실행 (추천)
```
start_realtime_monitor_background.bat (더블클릭)
```

### 포그라운드 실행 (로그 보면서)
```
run_realtime_monitor.bat (더블클릭)
```

---

## 🛑 중지하기

### PowerShell 명령어
```powershell
Stop-Process -Name python -Force
```

### 작업 관리자
`Ctrl + Shift + Esc` → python.exe 찾기 → 작업 끝내기

---

## 🔄 자동 실행 여부

### 실시간 모니터링
- ❌ 컴퓨터 부팅 시 **자동 시작 안 됨**
- ✅ 한 번 실행하면 **다음날까지 계속 실행**
- ⏰ 08:00-20:00만 모니터링, 그 외 시간은 대기

### 일일 리포트
- ✅ 매일 20:10 **자동 실행** (Windows 작업 스케줄러)
- 컴퓨터가 켜져 있어야 함

---

## 📝 참고사항

- **Cursor 종료**: 모니터링 계속 실행 ✅
- **컴퓨터 종료**: 모니터링 중지 ❌
- **컴퓨터 재부팅**: 수동으로 다시 시작 필요 🔄

---

## 🎯 핵심 정리

1. **확인**: `tasklist | Select-String "python"`
2. **시작**: `start_realtime_monitor_background.bat`
3. **중지**: `Stop-Process -Name python -Force`

---

## 📞 문제 발생 시

1. 로그 파일 확인: `realtime_monitor_YYYYMMDD.log`
2. 프로세스 강제 종료 후 재시작
3. 컴퓨터 재부팅



