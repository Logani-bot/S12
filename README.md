# Upbit 1515 - 업비트 급락 알림 시스템

업비트 전체 시장 모니터링 시스템: **15개 이상 코인이 15% 이상 하락**하면 텔레그램 알림 전송

## 시스템 개요

- **모니터링 대상**: 업비트 원화(KRW) 페어 전체 (~500개 코인)
- **알림 조건**: -15% 이하 하락 종목이 15개 이상일 때
- **실행 주기**: 1시간마다 정각에 체크
- **알림 제한**: 하루 최대 1회 시작 알림 + 1회 종료 알림

## 빠른 시작

### 1. 설치

```bash
cd /c/Coding/Upbit1515
pip install -r requirements.txt
```

### 2. 설정

`config.json` 파일 수정:

```json
{
  "telegram_bot_token": "YOUR_BOT_TOKEN",
  "telegram_chat_id": "YOUR_CHAT_ID",
  "decline_threshold": -15.0,
  "min_coin_count": 15,
  "check_interval_hours": 1,
  "log_level": "INFO"
}
```

### 3. 테스트 실행

```bash
# 연결 테스트
python test_connection.py

# 1회 테스트 실행
python run_test.py
```

### 4. 정식 실행

```bash
# Windows
run_monitor.bat

# Linux/Mac
python run_monitor.py
```

## 알림 로직

### 시작 알림 (START)
- 조건: -15% 이하 하락 종목 ≥ 15개
- 하루 최대 1회만 전송
- 예시:
```
🚨 업비트 급락 알림 🚨
-15% 이하 하락 종목: 23개

급락 종목 TOP 10:
1. BTC -18.5%
2. ETH -17.2%
...
```

### 종료 알림 (END)
- 조건: 하락 종목 < 15개 (시작 알림 후)
- 하루 최대 1회만 전송
- 시작 알림 없이는 전송 안 됨

### 중복 방지
- `alert_status.json` 파일에 날짜별 알림 상태 저장
- 자정(00:00)마다 자동 초기화

## 파일 구조

```
Upbit1515/
├── upbit_monitor.py          # 메인 모니터링 로직
├── telegram_notifier.py      # 텔레그램 전송 모듈
├── run_monitor.py            # 1시간 스케줄러 (정식)
├── run_test.py               # 1회 실행 테스트
├── test_connection.py        # 업비트/텔레그램 연결 테스트
├── config.json               # 설정 파일
├── requirements.txt          # 의존성 목록
├── alert_status.json         # 알림 상태 추적 (자동 생성)
└── upbit_monitor_YYYYMMDD.log # 일별 로그 (자동 생성)
```

## 로그 확인

```bash
# 오늘 로그 보기
cat upbit_monitor_$(date +%Y%m%d).log

# 실시간 로그 모니터링
tail -f upbit_monitor_$(date +%Y%m%d).log
```

## 문제 해결

### 텔레그램 연결 실패
```bash
python test_connection.py
```
- 봇 토큰 확인
- 채팅 ID 확인
- 봇과 대화 시작했는지 확인

### API 오류
- 인터넷 연결 확인
- 업비트 API 서버 상태 확인: https://status.upbit.com

### 알림이 안 옴
- `alert_status.json` 확인 (하루 1회 제한)
- 로그 파일에서 에러 메시지 확인

## 주의사항

⚠️ **투자 주의**: 이 시스템은 참고용 정보 제공이며, 투자 권유가 아닙니다.
⚠️ **API 제한**: 업비트 API 호출 제한을 준수하세요.
⚠️ **보안**: config.json은 git에 커밋하지 마세요.

## 관련 프로젝트

- [CoinRedS](../CoinRedS) - 45일 이동평균 엔벨로프 알림 시스템
- [omg](../omg) - Phase 1.5 트레이딩 시뮬레이션 시스템
