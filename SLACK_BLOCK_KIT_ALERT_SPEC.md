# S12 슬랙 Block Kit 알람 수정 사항

**작성일**: 2025-01-XX  
**목적**: S12 시스템에 슬랙 Block Kit 알람 추가를 위한 스펙

---

## 📋 수정해야 하는 위치

### 1. 실시간 알람 (Real_Time_Monitor.py)
**파일**: `Real_Time_Monitor.py`  
**위치**: 622줄  
**현재**: 텔레그램만 전송  
**추가 필요**: 슬랙 Block Kit 알람

### 2. 일일 리포트 (Trading_Signal_System.py)
**파일**: `Trading_Signal_System.py`  
**위치**: 1136줄  
**현재**: 텔레그램만 전송  
**추가 필요**: 슬랙 Block Kit 알람

---

## 🔔 알람 1: 실시간 매수 체결 알람

### 전송 위치
- **파일**: `Real_Time_Monitor.py`
- **함수**: `check_simplified_alert()` 
- **줄 번호**: 622줄 (`send_realtime_alert()` 호출 직후 또는 같이 호출)

### 전송 조건
- 저가가 1차 매수선에 도달한 경우 (`low_dist_buy1 <= 0`)
- 하루 1회 제한 (중복 방지)

### 사용 가능한 데이터
```python
{
    "alert_type": "1차 매수 체결!",  # 문자열
    "stock_name": stock_name,        # 종목명 (문자열)
    "ticker": ticker,                # 티커 (문자열, 6자리)
    "current_price": current_price,  # 현재가 (float)
    "low_price": low_price,          # 저가 (float)
    "target_price": buy1,            # 목표가/매수선 (float)
    "distance_pct": low_dist_buy1,   # 이격도 (%) (float)
    "sell_prices": {                 # 매도가 정보 (dict)
        "sell1": float,              # 3% 매도가
        "sell2": float,              # 5% 매도가
        "sell3": float               # 7% 매도가
    },
    "system_label": system_label     # "S1" 또는 "S2" (문자열)
}
```

### 알람 형태 (Block Kit)

**예시 데이터**:
```python
{
    "alert_type": "1차 매수 체결!",
    "stock_name": "삼성전자",
    "ticker": "005930",
    "current_price": 75000,
    "low_price": 74000,
    "target_price": 74000,
    "distance_pct": -0.5,
    "sell_prices": {
        "sell1": 76220,  # 3% 매도가
        "sell2": 77700,  # 5% 매도가
        "sell3": 79180   # 7% 매도가
    },
    "system_label": "S2"
}
```

**Block Kit 구조**:
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

## 📊 알람 2: 일일 트레이딩 리포트

### 전송 위치
- **파일**: `Trading_Signal_System.py`
- **함수**: `main()`
- **줄 번호**: 1136줄 (`send_daily_report()` 호출 직후 또는 같이 호출)

### 전송 조건
- 매일 20:10 실행 (거래일만)
- 알람 대상 종목이 있는 경우

### 사용 가능한 데이터
```python
{
    "alerts": [  # 알람 대상 종목 리스트
        {
            "티커": "005930",
            "종목명": "삼성전자",
            "알람상태": "READY_BUY1_5%",  # 또는 "READY_BUY2_5%", "READY_BUY3_5%", "BOUGHT_1", "READY_SELL_3%"
            "매수상태": "NONE",  # 또는 "BOUGHT_1", "BOUGHT_2", "BOUGHT_3"
            "종가": 75000,
            "1차매수선(익일)": 74000,
            "1차매수선이격도(%)": 1.35,
            "2차매수선(익일)": 66600,
            "2차매수선이격도(%)": 12.61,
            "3차매수선(익일)": 59940,
            "3차매수선이격도(%)": 25.14,
            "평균매수가": 74000,
            "1차매도선(+3%)": 76220,
            "1차매도선이격도(%)": -1.61,
            "2차매도선(+5%)": 77700,
            "2차매도선이격도(%)": -3.53,
            "3차매도선(+7%)": 79180,
            "3차매도선이격도(%)": -5.25,
            "상태메시지": "1차 매수선 5% 이내 접근"
        },
        // ... 더 많은 종목들
    ],
    "total_stocks": 150,  # 총 분석 종목 수
    "system_label": "S2"  # "S1" 또는 "S2"
}
```

### 알람 형태 (Block Kit)

**예시 데이터**:
```python
{
    "alerts": [
        {
            "티커": "005930",
            "종목명": "삼성전자",
            "알람상태": "READY_BUY1_5%",
            "종가": 75000,
            "1차매수선(익일)": 74000,
            "1차매수선이격도(%)": 1.35
        },
        {
            "티커": "000660",
            "종목명": "SK하이닉스",
            "알람상태": "BOUGHT_1",
            "종가": 150000,
            "평균매수가": 145000,
            # 이격도 계산: ((150000 - 145000) / 145000) * 100 = 3.45%
        },
        {
            "티커": "035420",
            "종목명": "NAVER",
            "알람상태": "READY_SELL_3%",
            "종가": 220000,
            "1차매도선(+3%)": 228000,
            "1차매도선이격도(%)": -3.51
        }
    ],
    "total_stocks": 150,
    "system_label": "S2"
}
```

**Block Kit 구조**:
```json
{
    "blocks": [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📊 [S2] 일일 트레이딩 리포트",
                "emoji": true
            }
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "🕐 2025-01-15 20:10"
                }
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*총 분석 종목: 150개*"
            }
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*🟡 1차 매수 접근 중 (1개)*"
            }
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_preformatted",
                    "elements": [
                        {
                            "type": "text",
                            "text": "삼성전자 (005930)\n현재가: 75,000원 / 매수가: 74,000원 / 이격도: 1.35%"
                        }
                    ]
                }
            ]
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*🔴 매수 완료 종목 (1개)*"
            }
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_preformatted",
                    "elements": [
                        {
                            "type": "text",
                            "text": "SK하이닉스 (000660)\n현재가: 150,000원 / 평균가: 145,000원 / 이격도: +3.45%"
                        }
                    ]
                }
            ]
        },
        {
            "type": "divider"
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*🟢 매도선 접근 (1개)*"
            }
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_preformatted",
                    "elements": [
                        {
                            "type": "text",
                            "text": "NAVER (035420)\n현재가: 220,000원 / 목표가: 228,000원 / 이격도: -3.51%"
                        }
                    ]
                }
            ]
        },
        {
            "type": "divider"
        }
    ]
}
```

---

## 📝 알람 상태별 분류

### 일일 리포트 알람 상태

| 알람 상태 | 카테고리 | 설명 |
|----------|---------|------|
| `READY_BUY1_5%` | 🟡 1차 매수 접근 중 | 1차 매수선 10% 이내 |
| `READY_BUY2_5%` | 🟠 2차 매수 접근 중 | 2차 매수선 10% 이내 |
| `READY_BUY3_5%` | 🟤 3차 매수 접근 중 | 3차 매수선 10% 이내 |
| `BOUGHT_1`, `BOUGHT_2`, `BOUGHT_3` | 🔴 매수 완료 종목 | 매수 체결 완료 |
| `READY_SELL_3%`, `READY_SELL_5%`, `READY_SELL_7%` | 🟢 매도선 접근 | 매도선 근접 |

---

## 🔧 수정 필요 사항

### 1. slack_notifier.py 수정

**추가할 함수**:
1. `send_slack_realtime_alert_block_kit()` - 실시간 매수 체결 알람 (Block Kit)
2. `send_slack_daily_report_block_kit()` - 일일 리포트 (Block Kit)

**기존 함수 수정**:
- `send_slack_message()` 함수에 `blocks` 파라미터 추가 (OMG 방식과 동일)

### 2. Real_Time_Monitor.py 수정

**위치**: 622줄 이후
```python
# 텔레그램 전송 (기존)
send_realtime_alert(...)

# 슬랙 Block Kit 전송 (추가)
from slack_notifier import send_slack_realtime_alert_block_kit
send_slack_realtime_alert_block_kit(...)
```

### 3. Trading_Signal_System.py 수정

**위치**: 1136줄 이후
```python
# 텔레그램 전송 (기존)
send_daily_report(alerts, len(df_summary), recipients=["all"])

# 슬랙 Block Kit 전송 (추가)
from slack_notifier import send_slack_daily_report_block_kit
send_slack_daily_report_block_kit(alerts, len(df_summary), system_label="S2")
```

---

## 📌 참고사항

- 텔레그램 알람은 그대로 유지 (변경 없음)
- 슬랙 알람만 Block Kit 형식으로 추가
- 가격 포맷: 천 단위 콤마 (예: 75,000원)
- 이격도 포맷: 소수점 2자리 (예: 1.35%, -0.50%)
- 시스템 라벨: "S1" 또는 "S2" (자동 감지)

