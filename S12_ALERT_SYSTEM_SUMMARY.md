# S12 알람 시스템 정리

**작성일**: 2025-01-XX  
**프로젝트**: S12 Trading System

---

## 📋 알람 시스템 개요

S12 시스템은 **텔레그램 알람만 활성화**되어 있으며, **슬랙 알람은 코드는 있지만 사용되지 않고 있습니다.**

---

## 🔔 텔레그램 알람 시스템

### 1. 알람 모듈: `telegram_notifier.py`

#### 주요 함수
- `send_telegram_message()`: 기본 텔레그램 메시지 전송
- `send_daily_report()`: 일일 리포트 전송
- `send_realtime_alert()`: 실시간 알람 전송
- `send_error_alert()`: 에러 알람 전송

#### 수신자 설정
```python
CHAT_IDS = {
    "me": os.getenv("TELEGRAM_CHAT_ID_ME"),
    "yoonjoo": os.getenv("TELEGRAM_CHAT_ID_YOONJOO"),
    "minjeong": os.getenv("TELEGRAM_CHAT_ID_MINJEONG"),
    "jumeoni": os.getenv("TELEGRAM_CHAT_ID_JUMEONI")
}
```

- 기본값: `["me"]` (본인만)
- `["all"]` 사용 시: 모든 수신자에게 전송
- 개별 지정: `["me", "yoonjoo"]` 등

---

## 📤 실제 알람 전송 위치

### 1. 일일 리포트 (Trading_Signal_System.py)

**전송 위치**: `Trading_Signal_System.py` 1136줄  
**실행 시점**: 매일 20:10 (거래일만)  
**전송 함수**: `send_daily_report()`  
**수신자**: `["all"]` (모든 사람에게)

**알람 내용**:
- 📊 일일 트레이딩 리포트
- 🟡 1차 매수 접근 중 (10% 이내)
- 🟠 2차 매수 접근 중 (10% 이내)
- 🟤 3차 매수 접근 중 (10% 이내)
- 🔴 매수 완료 종목 (수익률 포함)
- 🟢 매도선 접근

**전송 조건**:
- `alert_status`가 `WATCHING`, `WAITING`이 아닌 경우
- 알람 대상 종목이 있는 경우

**코드 위치**:
```python
# Trading_Signal_System.py 1133-1139줄
if TELEGRAM_AVAILABLE:
    try:
        send_daily_report(alerts, len(df_summary), recipients=["all"])
        logger.info("✓ 텔레그램 일일 리포트 전송 완료")
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")
```

---

### 2. 실시간 알람 (Real_Time_Monitor.py)

**전송 위치**: `Real_Time_Monitor.py` `check_simplified_alert()` 함수 내부  
**실행 시점**: 실시간 모니터링 중 (거래일 08:00-20:00, 설정된 간격마다)  
**전송 함수**: `send_realtime_alert()`  
**수신자**: `["all"]` (모든 사람에게)

**알람 타입**:
- ✅ 1차 매수 체결!
- ✅✅ 2차 매수 체결!
- ✅✅✅ 3차 매수 체결!

**알람 내용**:
- 종목명, 티커
- 현재가, 저가
- 목표가 (매수선)
- 이격도
- 매도가 정보 (매수 체결 시)

**전송 조건**:
- 저가가 매수선에 도달한 경우 (이격도 ≤ 0%)
- 하루 1회 제한 (중복 방지)

**코드 위치**:
```python
# Real_Time_Monitor.py 563-643줄
def check_simplified_alert(...):
    # ...
    if low_dist_buy1 <= 0:  # 저가가 매수선 도달
        alert_key = "BUY1_EXECUTED"
        if not ticker_alerts.get(alert_key, False):
            send_realtime_alert(
                alert_type="1차 매수 체결!",
                stock_name=stock_name,
                ticker=ticker,
                current_price=current_price,
                target_price=buy1,
                distance_pct=low_dist_buy1,
                recipients=["all"],
                sell_prices=sell_prices,
                system_label=system_label,
                low_price=low_price
            )
```

---

### 3. 에러 알람

**전송 위치**: 
- `Trading_Signal_System.py`: 에러 발생 시
- `Real_Time_Monitor.py`: 시스템 오류 발생 시 (773줄)

**전송 함수**: `send_error_alert()`  
**수신자**: `["me"]` (본인만)

**알람 내용**:
- ❌ 시스템 에러 발생
- 스크립트 이름
- 에러 메시지

**코드 위치**:
```python
# Real_Time_Monitor.py 768-775줄
except Exception as e:
    logger.error(f"[ERROR] 시스템 오류: {e}")
    try:
        from telegram_notifier import send_error_alert
        send_error_alert(str(e), "Real_Time_Monitor", recipients=["me"])
    except:
        pass
```

---

## 📭 슬랙 알람 시스템 (현재 미사용)

### 1. 알람 모듈: `slack_notifier.py`

#### 주요 함수
- `send_slack_message()`: 기본 슬랙 메시지 전송
- `send_slack_realtime_alert()`: 실시간 알람 슬랙 전송
- `send_slack_daily_report()`: 일일 리포트 슬랙 전송

#### 설정
- 환경 변수: `SLACK_WEBHOOK_URL`
- 현재 상태: **코드는 있지만 실제로 호출하는 곳이 없음**

### 2. 실제 사용 현황

**❌ 사용되지 않음**
- `Trading_Signal_System.py`: 슬랙 import 없음
- `Real_Time_Monitor.py`: 슬랙 import 없음
- 다른 파일에서도 슬랙 호출 없음

---

## 📊 알람 흐름 정리

### 일일 리포트 (20:10 실행)

```
Trading_Signal_System.py 실행
  ↓
turnover_universe.xlsx 읽기
  ↓
각 종목 분석
  ↓
알람 대상 종목 수집
  ↓
send_daily_report(alerts, total_stocks, recipients=["all"])
  ↓
텔레그램 전송 (모든 사람에게)
```

### 실시간 알람 (08:00-20:00, 설정 간격마다)

```
Real_Time_Monitor.py 실행
  ↓
trading_signals.xlsx Summary 탭 읽기
  ↓
각 종목 현재가/저가 조회
  ↓
저가 기준 매수선 도달 체크
  ↓
check_simplified_alert() 호출
  ↓
send_realtime_alert(...)
  ↓
텔레그램 전송 (모든 사람에게)
```

---

## 🔧 현재 설정

### 텔레그램
- ✅ **활성화됨**
- 모듈: `telegram_notifier.py`
- 환경 변수: `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID_*`
- 수신자: 4명 (me, yoonjoo, minjeong, jumeoni)

### 슬랙
- ❌ **비활성화됨** (코드만 존재)
- 모듈: `slack_notifier.py`
- 환경 변수: `SLACK_WEBHOOK_URL`
- 실제 호출: 없음

---

## 📝 알람 타입 상세

### 일일 리포트 알람 타입

| 알람 상태 | 이모지 | 설명 |
|----------|--------|------|
| `READY_BUY1` | 🟡 | 1차 매수선 접근 중 (10% 이내) |
| `READY_BUY2` | 🟠 | 2차 매수선 접근 중 (10% 이내) |
| `READY_BUY3` | 🟤 | 3차 매수선 접근 중 (10% 이내) |
| `BOUGHT_1/2/3` | 🔴 | 매수 완료 종목 (수익률 포함) |
| `READY_SELL` | 🟢 | 매도선 접근 (3%, 5%, 7%) |

### 실시간 알람 타입

| 알람 타입 | 이모지 | 설명 |
|----------|--------|------|
| `1차 매수 체결!` | ✅ | 저가가 1차 매수선 도달 |
| `2차 매수 체결!` | ✅✅ | 저가가 2차 매수선 도달 |
| `3차 매수 체결!` | ✅✅✅ | 저가가 3차 매수선 도달 |

---

## 🎯 수정 필요 사항

### 슬랙 알람 활성화 필요

1. **Trading_Signal_System.py**에 슬랙 알람 추가
   - 일일 리포트 전송 시 슬랙도 함께 전송
   - `send_slack_daily_report()` 호출 추가

2. **Real_Time_Monitor.py**에 슬랙 알람 추가
   - 실시간 알람 전송 시 슬랙도 함께 전송
   - `send_slack_realtime_alert()` 호출 추가

3. **에러 알람** 슬랙 추가 (선택적)

---

## 📌 참고사항

- 텔레그램은 HTML 포맷 사용
- 슬랙은 마크다운 포맷 사용 (HTML → 마크다운 자동 변환)
- 모든 알람은 중복 방지 로직 포함 (하루 1회 제한)
- 실시간 알람은 저가 기준으로 판단 (매수선 터치 감지)

