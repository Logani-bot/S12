import pandas as pd

print("=== Cleaned files verification ===")

# Check turnover_universe_clean.xlsx
print("\n1. turnover_universe_clean.xlsx:")
df_turnover = pd.read_excel('output/turnover_universe_clean.xlsx')
print(f"Total stocks: {len(df_turnover)}")
print("First 5 stocks:")
print(df_turnover[['티커', '종목명']].head())

# Check if NAVER and Hanmi are removed
naver = df_turnover[df_turnover['티커'] == '035420']
hanmi = df_turnover[df_turnover['티커'] == '042700']
print(f"\nNAVER (035420): {'Found' if len(naver) > 0 else 'Removed'}")
print(f"Hanmi (042700): {'Found' if len(hanmi) > 0 else 'Removed'}")

# Check trading_signals_clean.xlsx
print("\n2. trading_signals_clean.xlsx:")
df_summary = pd.read_excel('output/trading_signals_clean.xlsx', sheet_name='Summary')
df_history = pd.read_excel('output/trading_signals_clean.xlsx', sheet_name='History')
print(f"Summary stocks: {len(df_summary)}")
print(f"History stocks: {len(df_history)}")

# Check if test data is removed from Summary
naver_summary = df_summary[df_summary['티커'] == '035420']
hanmi_summary = df_summary[df_summary['티커'] == '042700']
print(f"\nSummary - NAVER: {'Found' if len(naver_summary) > 0 else 'Removed'}")
print(f"Summary - Hanmi: {'Found' if len(hanmi_summary) > 0 else 'Removed'}")

print("\n=== Verification completed ===")




