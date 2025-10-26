import pandas as pd

print("=== 테스트 데이터 정리 시작 ===")

# 1. turnover_universe.xlsx에서 테스트 데이터 제거
print("\n1. turnover_universe.xlsx 정리 중...")
df_turnover = pd.read_excel('output/turnover_universe.xlsx', sheet_name='universe')
print(f"정리 전 종목 수: {len(df_turnover)}")

# NAVER와 한미반도체 제거
test_tickers = ['035420', '042700']  # NAVER, 한미반도체
df_turnover_clean = df_turnover[~df_turnover['티커'].isin(test_tickers)]
print(f"정리 후 종목 수: {len(df_turnover_clean)}")

# 저장
with pd.ExcelWriter('output/turnover_universe.xlsx', engine='openpyxl') as writer:
    df_turnover_clean.to_excel(writer, index=False, sheet_name='universe')

print("✓ turnover_universe.xlsx 정리 완료")

# 2. trading_signals.xlsx에서 테스트 데이터 제거
print("\n2. trading_signals.xlsx 정리 중...")
df_summary = pd.read_excel('output/trading_signals.xlsx', sheet_name='Summary')
df_history = pd.read_excel('output/trading_signals.xlsx', sheet_name='History')

print(f"Summary 정리 전: {len(df_summary)}개")
print(f"History 정리 전: {len(df_history)}개")

# Summary에서 테스트 데이터 제거
df_summary_clean = df_summary[~df_summary['티커'].isin(test_tickers)]
print(f"Summary 정리 후: {len(df_summary_clean)}개")

# History에서도 테스트 데이터 제거
df_history_clean = df_history[~df_history['티커'].isin(test_tickers)]
print(f"History 정리 후: {len(df_history_clean)}개")

# 저장
with pd.ExcelWriter('output/trading_signals.xlsx', engine='openpyxl') as writer:
    df_summary_clean.to_excel(writer, index=False, sheet_name='Summary')
    df_history_clean.to_excel(writer, index=False, sheet_name='History')

print("✓ trading_signals.xlsx 정리 완료")

# 3. 정리 결과 확인
print("\n=== 정리 결과 확인 ===")
print(f"turnover_universe.xlsx: {len(df_turnover_clean)}개 종목")
print(f"trading_signals.xlsx Summary: {len(df_summary_clean)}개 종목")
print(f"trading_signals.xlsx History: {len(df_history_clean)}개 종목")

# 제거된 종목 확인
removed_turnover = df_turnover[df_turnover['티커'].isin(test_tickers)]
removed_summary = df_summary[df_summary['티커'].isin(test_tickers)]
removed_history = df_history[df_history['티커'].isin(test_tickers)]

print(f"\n제거된 종목:")
if len(removed_turnover) > 0:
    print("turnover_universe에서 제거:", removed_turnover[['티커', '종목명']].values.tolist())
if len(removed_summary) > 0:
    print("Summary에서 제거:", removed_summary[['티커', '종목명']].values.tolist())
if len(removed_history) > 0:
    print("History에서 제거:", removed_history[['티커', '종목명']].values.tolist())

print("\n=== 테스트 데이터 정리 완료 ===")



