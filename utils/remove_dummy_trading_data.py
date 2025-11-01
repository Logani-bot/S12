import pandas as pd

print("=== 더미 거래 데이터 제거 ===")

# 1. trading_signals.xlsx 읽기
df_summary = pd.read_excel('output/trading_signals.xlsx', sheet_name='Summary')
df_history = pd.read_excel('output/trading_signals.xlsx', sheet_name='History')

print(f"Summary 정리 전: {len(df_summary)}개 종목")
print(f"History 정리 전: {len(df_history)}개 종목")

# 2. NAVER와 한미반도체의 더미 거래 데이터 확인
test_tickers = ['035420', '042700']  # NAVER, 한미반도체

print("\n=== NAVER (035420) 더미 데이터 확인 ===")
naver_summary = df_summary[df_summary['티커'] == '035420']
if len(naver_summary) > 0:
    naver = naver_summary.iloc[0]
    print(f"매수상태: {naver['매수상태']}")
    print(f"1차매수일: {naver['1차매수일']}")
    print(f"2차매수일: {naver['2차매수일']}")
    print(f"3차매수일: {naver['3차매수일']}")
    print(f"평균매수가: {naver['평균매수가']}")

print("\n=== 한미반도체 (042700) 더미 데이터 확인 ===")
hanmi_summary = df_summary[df_summary['티커'] == '042700']
if len(hanmi_summary) > 0:
    hanmi = hanmi_summary.iloc[0]
    print(f"매수상태: {hanmi['매수상태']}")
    print(f"1차매수일: {hanmi['1차매수일']}")
    print(f"2차매수일: {hanmi['2차매수일']}")
    print(f"3차매수일: {hanmi['3차매수일']}")
    print(f"평균매수가: {hanmi['평균매수가']}")

# 3. History에서 해당 종목들 확인
print("\n=== History 탭 더미 데이터 확인 ===")
naver_history = df_history[df_history['티커'] == '035420']
hanmi_history = df_history[df_history['티커'] == '042700']

print(f"NAVER History: {len(naver_history)}개 기록")
if len(naver_history) > 0:
    print(naver_history[['티커', '종목명', '매수상태', '종료일']].to_string())

print(f"\n한미반도체 History: {len(hanmi_history)}개 기록")
if len(hanmi_history) > 0:
    print(hanmi_history[['티커', '종목명', '매수상태', '종료일']].to_string())

print("\n=== 더미 데이터 제거 시작 ===")

# 4. Summary에서 더미 거래 데이터 초기화
df_summary_clean = df_summary.copy()

for ticker in test_tickers:
    mask = df_summary_clean['티커'] == ticker
    if mask.any():
        # 거래 관련 컬럼들을 초기화
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
        print(f"✓ {ticker} Summary 더미 거래 데이터 초기화")

# 5. History에서 해당 종목들 완전 제거
df_history_clean = df_history[~df_history['티커'].isin(test_tickers)]
print(f"✓ History에서 {len(df_history) - len(df_history_clean)}개 더미 기록 제거")

# 6. 정리된 파일 저장
with pd.ExcelWriter('output/trading_signals_cleaned.xlsx', engine='openpyxl') as writer:
    df_summary_clean.to_excel(writer, index=False, sheet_name='Summary')
    df_history_clean.to_excel(writer, index=False, sheet_name='History')

print(f"\n=== 정리 완료 ===")
print(f"Summary: {len(df_summary_clean)}개 종목")
print(f"History: {len(df_history_clean)}개 종목")
print("파일 저장: output/trading_signals_cleaned.xlsx")




