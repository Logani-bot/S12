import pandas as pd

print("=== NAVER와 한미반도체 알람상태 수정 ===")

# 1. 현재 상태 확인
df = pd.read_excel('output/trading_signals.xlsx', sheet_name='Summary')

test_tickers = ['035420', '042700']  # NAVER, 한미반도체

print("현재 상태 확인:")
for ticker in test_tickers:
    stock = df[df['티커'] == ticker]
    if len(stock) > 0:
        s = stock.iloc[0]
        print(f"\n{ticker} ({s['종목명']}):")
        print(f"  매수상태: {s['매수상태']}")
        print(f"  알람상태: {s['알람상태']}")
        print(f"  상태메시지: {s['상태메시지']}")
        print(f"  종가: {s['종가']:,.0f}원")
        print(f"  1차매수선: {s['1차매수선']:,.0f}원")
        print(f"  1차매수선이격도: {s['1차매수선이격도(%)']:.1f}%")

# 2. 알람상태와 상태메시지 수정
df_clean = df.copy()

for ticker in test_tickers:
    mask = df_clean['티커'] == ticker
    if mask.any():
        stock = df_clean[mask].iloc[0]
        
        # 1차 매수선 이격도 계산
        close = stock['종가']
        buy1 = stock['1차매수선']
        dist_buy1 = stock['1차매수선이격도(%)']
        
        # 알람상태와 상태메시지 결정
        if dist_buy1 is not None and 0 < dist_buy1 <= 10.0:
            alert_status = "READY_BUY1"
            status_msg = f"1차 매수선까지 {dist_buy1:.1f}% (접근 중!)"
        else:
            alert_status = "WATCHING"
            status_msg = f"1차 매수선까지 {dist_buy1:.1f}%"
        
        # 업데이트
        df_clean.loc[mask, '알람상태'] = alert_status
        df_clean.loc[mask, '상태메시지'] = status_msg
        
        print(f"\n{ticker} 수정:")
        print(f"  알람상태: {alert_status}")
        print(f"  상태메시지: {status_msg}")

# 3. 수정된 파일 저장
with pd.ExcelWriter('output/trading_signals.xlsx', engine='openpyxl') as writer:
    df_clean.to_excel(writer, index=False, sheet_name='Summary')
    # History는 비어있는 상태로 유지
    pd.DataFrame(columns=df.columns).to_excel(writer, index=False, sheet_name='History')

print(f"\n=== 수정 완료 ===")
print("trading_signals.xlsx 업데이트 완료")

# 4. 수정 결과 확인
print(f"\n=== 수정 결과 확인 ===")
for ticker in test_tickers:
    stock = df_clean[df_clean['티커'] == ticker]
    if len(stock) > 0:
        s = stock.iloc[0]
        print(f"{ticker} ({s['종목명']}):")
        print(f"  알람상태: {s['알람상태']}")
        print(f"  상태메시지: {s['상태메시지']}")
        print(f"  1차매수선이격도: {s['1차매수선이격도(%)']:.1f}%")




