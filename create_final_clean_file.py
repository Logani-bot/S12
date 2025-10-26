import pandas as pd

print("=== 최종 정리된 파일 생성 ===")

# 1. 원본 파일 읽기
df_summary = pd.read_excel('output/trading_signals.xlsx', sheet_name='Summary')
df_history = pd.read_excel('output/trading_signals.xlsx', sheet_name='History')

print(f"원본 Summary: {len(df_summary)}개 종목")
print(f"원본 History: {len(df_history)}개 종목")

# 2. NAVER와 한미반도체의 더미 거래 데이터 초기화
test_tickers = ['035420', '042700']

df_summary_clean = df_summary.copy()

for ticker in test_tickers:
    mask = df_summary_clean['티커'] == ticker
    if mask.any():
        # 거래 관련 데이터만 초기화 (종목 정보는 유지)
        df_summary_clean.loc[mask, '매수상태'] = 'NONE'
        df_summary_clean.loc[mask, '평균매수가'] = None
        df_summary_clean.loc[mask, '총투자금액'] = 0
        df_summary_clean.loc[mask, '총보유수량'] = 0
        df_summary_clean.loc[mask, '1차매수일'] = None
        df_summary_clean.loc[mask, '1차매수가'] = None
        df_summary_clean.loc[mask, '1차매수량'] = None
        df_summary_clean.loc[mask, '2차매수일'] = None
        df_summary_clean.loc[mask, '2차매수가'] = None
        df_summary_clean.loc[mask, '2차매수량'] = None
        df_summary_clean.loc[mask, '3차매수일'] = None
        df_summary_clean.loc[mask, '3차매수가'] = None
        df_summary_clean.loc[mask, '3차매수량'] = None
        df_summary_clean.loc[mask, '최고도달선'] = None
        print(f"{ticker} 더미 거래 데이터 초기화 완료")

# 3. History는 그대로 유지 (더미 데이터가 없었음)
df_history_clean = df_history.copy()

# 4. 최종 파일 저장
with pd.ExcelWriter('output/trading_signals_final.xlsx', engine='openpyxl') as writer:
    df_summary_clean.to_excel(writer, index=False, sheet_name='Summary')
    df_history_clean.to_excel(writer, index=False, sheet_name='History')

print(f"\n=== 최종 파일 생성 완료 ===")
print(f"파일명: output/trading_signals_final.xlsx")
print(f"Summary: {len(df_summary_clean)}개 종목")
print(f"History: {len(df_history_clean)}개 종목")

# 5. 정리 결과 확인
print(f"\n=== 정리 결과 확인 ===")
for ticker in test_tickers:
    stock = df_summary_clean[df_summary_clean['티커'] == ticker]
    if len(stock) > 0:
        s = stock.iloc[0]
        print(f"{ticker}: 매수상태={s['매수상태']}, 2차매수일={s['2차매수일']}")




