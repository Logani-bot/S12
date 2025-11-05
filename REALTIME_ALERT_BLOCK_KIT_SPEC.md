# 실시간 알람 Block Kit 양식

**작성일**: 2025-01-XX  
**목적**: S12 실시간 모니터링 알람 Block Kit 형식 스펙

---

## 📋 알람 타입

### 1. 매수선 인접 알람 (현재 구현 없음, 향후 추가 가능)
- 1차 매수선 5% 인접
- 1차 매수선 3% 인접
- 1차 매수선 1% 인접
- 2차 매수선 5% 인접
- 2차 매수선 3% 인접
- 2차 매수선 1% 인접
- 3차 매수선 5% 인접
- 3차 매수선 3% 인접
- 3차 매수선 1% 인접

### 2. 매수 체결 알람 (현재 구현됨)
- 1차 매수 체결!
- 2차 매수 체결!
- 3차 매수 체결!

---

## 🔔 알람 1: 매수선 인접 알람

### 전송 위치
- **파일**: `Real_Time_Monitor.py`
- **함수**: `check_simplified_alert()` (향후 추가 예정)
- **현재 상태**: 구현 없음

### 사용 가능한 데이터
```python
{
    "alert_type": "1차 매수선 5% 인접",  # 문자열
    "stock_name": "삼성전자",            # 종목명 (문자열)
    "ticker": "005930",                  # 티커 (문자열, 6자리)
    "current_price": 75000,              # 현재가 (float)
    "low_price": 74000,                  # 저가 (float, 선택적)
    "target_price": 74000,                # 매수선 (float)
    "distance_pct": 1.35,                # 이격도 (%) (float)
    "system_label": "S2"                 # "S1" 또는 "S2" (문자열)
}
```

### Block Kit 구조 예시

**예시 데이터**:
```python
{
    "alert_type": "1차 매수선 5% 인접",
    "stock_name": "삼성전자",
    "ticker": "005930",
    "current_price": 75000,
    "low_price": 74000,
    "target_price": 74000,
    "distance_pct": 1.35,
    "system_label": "S2"
}
```

**Block Kit JSON**:
```json
{
    "blocks": [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🟡 [S2] 1차 매수선 5% 인접",
                "emoji": true
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "🕐 14:30:25"
                }
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*종목명     :* 삼성전자 (005930)\n*현재가     :* 75,000원\n*저가       :* 74,000원\n*매수선     :* 74,000원\n*이격도     :* 1.35%"
            }
        },
        {
            "type": "divider"
        }
    ]
}
```

---

## ✅ 알람 2: 매수 체결 알람

### 전송 위치
- **파일**: `Real_Time_Monitor.py`
- **함수**: `check_simplified_alert()`
- **줄 번호**: 622줄
- **현재 상태**: 텔레그램만 전송 (슬랙 추가 필요)

### 사용 가능한 데이터
```python
{
    "alert_type": "1차 매수 체결!",      # 문자열
    "stock_name": "삼성전자",            # 종목명 (문자열)
    "ticker": "005930",                  # 티커 (문자열, 6자리)
    "current_price": 75000,              # 현재가 (float)
    "low_price": 74000,                  # 저가 (float)
    "target_price": 74000,               # 매수선 (float)
    "distance_pct": -0.50,               # 이격도 (%) (float, 마이너스)
    "sell_prices": {                     # 매도가 정보 (dict)
        "sell1": 76220,                  # 3% 매도가
        "sell2": 77700,                  # 5% 매도가
        "sell3": 79180                   # 7% 매도가
    },
    "system_label": "S2"                 # "S1" 또는 "S2" (문자열)
}
```

### Block Kit 구조 예시

**예시 데이터**:
```python
{
    "alert_type": "1차 매수 체결!",
    "stock_name": "삼성전자",
    "ticker": "005930",
    "current_price": 75000,
    "low_price": 74000,
    "target_price": 74000,
    "distance_pct": -0.50,
    "sell_prices": {
        "sell1": 76220,
        "sell2": 77700,
        "sell3": 79180
    },
    "system_label": "S2"
}
```

**Block Kit JSON**:
```json
{
    "blocks": [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "✅ [S2] 1차 매수 체결!",
                "emoji": true
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "🕐 14:30:25"
                }
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*종목명     :* 삼성전자 (005930)\n*현재가     :* 75,000원\n*저가       :* 74,000원\n*매수선     :* 74,000원\n*이격도     :* -0.50%"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*매도가 정보:*\n*3% 매도가 :* 76,220원\n*5% 매도가 :* 77,700원\n*7% 매도가 :* 79,180원"
            }
        },
        {
            "type": "divider"
        }
    ]
}
```

---

## 📊 알람 타입별 이모지 매핑

| 알람 타입 | 이모지 | 설명 |
|----------|--------|------|
| 1차 매수선 5% 인접 | 🟡 | 1차 매수선 5% 이내 |
| 1차 매수선 3% 인접 | 🟠 | 1차 매수선 3% 이내 |
| 1차 매수선 1% 인접 | 🔴 | 1차 매수선 1% 이내 |
| 2차 매수선 5% 인접 | 🟡 | 2차 매수선 5% 이내 |
| 2차 매수선 3% 인접 | 🟠 | 2차 매수선 3% 이내 |
| 2차 매수선 1% 인접 | 🔴 | 2차 매수선 1% 이내 |
| 3차 매수선 5% 인접 | 🟡 | 3차 매수선 5% 이내 |
| 3차 매수선 3% 인접 | 🟠 | 3차 매수선 3% 이내 |
| 3차 매수선 1% 인접 | 🔴 | 3차 매수선 1% 이내 |
| 1차 매수 체결! | ✅ | 저가가 1차 매수선 도달 |
| 2차 매수 체결! | ✅✅ | 저가가 2차 매수선 도달 |
| 3차 매수 체결! | ✅✅✅ | 저가가 3차 매수선 도달 |

---

## 🔧 수정 필요 사항

### slack_notifier.py에 추가할 함수

**함수명**: `send_slack_realtime_alert_block_kit()`

**파라미터**:
```python
def send_slack_realtime_alert_block_kit(
    alert_type: str,           # "1차 매수선 5% 인접", "1차 매수 체결!" 등
    stock_name: str,           # 종목명
    ticker: str,               # 티커
    current_price: float,      # 현재가
    target_price: float,       # 매수선
    distance_pct: float,       # 이격도 (%)
    sell_prices: dict = None,  # 매도가 정보 (매수 체결 시만)
    system_label: str = "S2",  # 시스템 라벨
    low_price: float = None    # 저가 (선택적)
) -> bool:
```

### Real_Time_Monitor.py 수정 위치

**현재 코드** (622줄):
```python
send_realtime_alert(
    alert_type=alert_type,
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

**추가할 코드**:
```python
# 텔레그램 전송 (기존 - 그대로 유지)
send_realtime_alert(...)

# 슬랙 Block Kit 전송 (추가)
from slack_notifier import send_slack_realtime_alert_block_kit
send_slack_realtime_alert_block_kit(
    alert_type=alert_type,
    stock_name=stock_name,
    ticker=ticker,
    current_price=current_price,
    target_price=buy1,
    distance_pct=low_dist_buy1,
    sell_prices=sell_prices,
    system_label=system_label,
    low_price=low_price
)
```

---

## 📌 참고사항

- 가격 포맷: 천 단위 콤마 (예: 75,000원)
- 이격도 포맷: 소수점 2자리 (예: 1.35%, -0.50%)
- 저가는 선택적 (매수 체결 알람에는 필수)
- 매도가 정보는 매수 체결 시에만 표시
- 시스템 라벨은 자동 감지 (S1 또는 S2)

