# S1 시스템 사용 가이드

## 개요
S1 시스템은 기존 turnover signals 시스템과 별도로 운영되는 시총 기반 트레이딩 시스템입니다.

## 주요 특징
- **시총 기준**: 매일 시총 1조 5천억 이상 종목들을 대상으로 분석
- **5조 이상 별도 표시**: 시총 5조 이상 종목은 별도 구분
- **독립 운영**: 기존 시스템과 완전히 분리된 파일 및 알림 관리
- **동일한 로직**: 기존 Trading_Signal_System.py와 동일한 매매 로직 사용

## 파일 구조

### S1 전용 파일들
```
market_cap_filter.py              # 시총 기반 종목 필터링
Trading_Signal_System_S1.py       # S1 트레이딩 시그널 시스템
telegram_notifier_s1.py           # S1 텔레그램 알림
run_market_cap_filter.bat         # 시총 필터링 실행
run_trading_signal_s1.bat         # S1 트레이딩 시그널 실행
run_s1_system.bat                 # S1 시스템 전체 실행
setup_s1_scheduler.bat            # S1 스케줄러 설정
test_s1_system.bat                # S1 시스템 테스트
```

### 출력 파일들
```
output/market_cap_universe.xlsx    # 시총 기반 종목 리스트
output/trading_signals_s1.xlsx    # S1 트레이딩 시그널 (Summary + History)
```

## 사용 방법

### 1. 환경 설정
기존과 동일한 환경 변수 사용:
- `KIWOOM_APPKEY`: 키움 API 앱키
- `KIWOOM_SECRET`: 키움 API 시크릿키
- `TELEGRAM_TOKEN`: 텔레그램 봇 토큰
- `TELEGRAM_CHAT_ID_*`: 텔레그램 채팅 ID들

### 2. 스케줄러 설정
```bash
setup_s1_scheduler.bat
```
- 매일 20:10에 자동 실행
- 작업명: `S1_Trading_System`

### 3. 수동 실행
```bash
# 전체 S1 시스템 실행
run_s1_system.bat

# 개별 실행
run_market_cap_filter.bat      # 시총 필터링만
run_trading_signal_s1.bat      # S1 트레이딩 시그널만
```

### 4. 테스트
```bash
test_s1_system.bat
```

## 실행 순서

### 자동 실행 (매일 20:10)
1. **시총 필터링**: `market_cap_filter.py`
   - 전체 종목 조회
   - 시총 1조 5천억 이상 필터링
   - `market_cap_universe.xlsx` 생성

2. **트레이딩 시그널 생성**: `Trading_Signal_System_S1.py`
   - 시총 유니버스 종목들 분석
   - 매매 시그널 생성
   - `trading_signals_s1.xlsx` 생성
   - 텔레그램 일일 리포트 전송

## 텔레그램 알림

### 일일 리포트 (20:10)
- **제목**: "일일 트레이딩 리포트 S1 (시총 기반)"
- **내용**: 
  - 1차 매수 접근 중 종목
  - 매수 완료 종목
  - 매도선 접근 종목
- **수신자**: 모든 설정된 사용자

### 실시간 알림
- 기존과 동일한 로직
- 메시지에 "(S1)" 표시로 구분

## 기존 시스템과의 차이점

| 구분 | 기존 시스템 | S1 시스템 |
|------|-------------|-----------|
| 대상 종목 | turnover_universe.xlsx | market_cap_universe.xlsx |
| 기준 | 거래량 기반 | 시총 기반 (1.5조 이상) |
| 시그널 파일 | trading_signals.xlsx | trading_signals_s1.xlsx |
| 텔레그램 모듈 | telegram_notifier.py | telegram_notifier_s1.py |
| 알림 제목 | "일일 트레이딩 리포트" | "일일 트레이딩 리포트 S1" |
| 스케줄러 | 기존 작업 | S1_Trading_System |

## 주의사항

1. **독립 운영**: S1 시스템은 기존 시스템과 완전히 분리되어 운영됩니다.
2. **파일 충돌 방지**: 각각 다른 파일명을 사용하여 충돌을 방지합니다.
3. **동시 실행 가능**: 기존 시스템과 S1 시스템을 동시에 실행할 수 있습니다.
4. **별도 모니터링**: S1 시스템용 실시간 모니터링은 별도로 구현해야 합니다.

## 문제 해결

### 시총 필터링 실패
- API 키 확인
- 네트워크 연결 확인
- 키움 API 서버 상태 확인

### 트레이딩 시그널 생성 실패
- 시총 유니버스 파일 존재 확인
- API 토큰 발급 확인
- 차트 데이터 조회 권한 확인

### 텔레그램 전송 실패
- 텔레그램 토큰 확인
- 채팅 ID 설정 확인
- 네트워크 연결 확인

## 확장 가능성

1. **실시간 모니터링**: S1용 실시간 모니터링 시스템 추가
2. **다양한 시총 기준**: 시총 기준을 변경 가능하도록 설정화
3. **백테스팅**: S1 시스템용 백테스팅 기능 추가
4. **성과 분석**: S1 시스템 전용 성과 분석 도구 추가
