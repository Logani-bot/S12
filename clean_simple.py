import pandas as pd
import os

print("=== Test data cleanup ===")

# 1. Clean turnover_universe.xlsx
print("\n1. Cleaning turnover_universe.xlsx...")
df_turnover = pd.read_excel('output/turnover_universe.xlsx', sheet_name='universe')
print(f"Before: {len(df_turnover)} stocks")

# Remove NAVER and Hanmi
test_tickers = ['035420', '042700']
df_turnover_clean = df_turnover[~df_turnover['티커'].isin(test_tickers)]
print(f"After: {len(df_turnover_clean)} stocks")

# Save to temp file
df_turnover_clean.to_excel('output/turnover_universe_temp.xlsx', index=False, sheet_name='universe')
print("Temp file created")

# 2. Clean trading_signals.xlsx
print("\n2. Cleaning trading_signals.xlsx...")
df_summary = pd.read_excel('output/trading_signals.xlsx', sheet_name='Summary')
df_history = pd.read_excel('output/trading_signals.xlsx', sheet_name='History')

print(f"Summary before: {len(df_summary)} stocks")
print(f"History before: {len(df_history)} stocks")

# Remove test data
df_summary_clean = df_summary[~df_summary['티커'].isin(test_tickers)]
df_history_clean = df_history[~df_history['티커'].isin(test_tickers)]

print(f"Summary after: {len(df_summary_clean)} stocks")
print(f"History after: {len(df_history_clean)} stocks")

# Save to temp file
with pd.ExcelWriter('output/trading_signals_temp.xlsx', engine='openpyxl') as writer:
    df_summary_clean.to_excel(writer, index=False, sheet_name='Summary')
    df_history_clean.to_excel(writer, index=False, sheet_name='History')

print("Temp file created")

# 3. Show removed stocks
removed_turnover = df_turnover[df_turnover['티커'].isin(test_tickers)]
removed_summary = df_summary[df_summary['티커'].isin(test_tickers)]
removed_history = df_history[df_history['티커'].isin(test_tickers)]

print(f"\nRemoved stocks:")
if len(removed_turnover) > 0:
    print("From turnover:", removed_turnover[['티커', '종목명']].values.tolist())
if len(removed_summary) > 0:
    print("From Summary:", removed_summary[['티커', '종목명']].values.tolist())
if len(removed_history) > 0:
    print("From History:", removed_history[['티커', '종목명']].values.tolist())

print("\n=== Cleanup completed ===")
print("Please manually replace the original files with temp files if needed.")



