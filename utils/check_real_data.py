import pandas as pd

print("=== 실제 데이터 vs 테스트 데이터 확인 ===")

# 1. turnover_universe.xlsx 확인
print("\n1. turnover_universe.xlsx 분석:")
df_turnover = pd.read_excel('output/turnover_universe.xlsx', sheet_name='universe')
print(f"총 종목 수: {len(df_turnover)}")

# 거래대금 순위 확인
print("\n거래대금 상위 10개:")
print(df_turnover[['티커', '종목명', '거래대금(억)']].head(10))

# NAVER와 한미반도체의 거래대금 확인
naver = df_turnover[df_turnover['티커'] == '035420']
hanmi = df_turnover[df_turnover['티커'] == '042700']

print(f"\nNAVER (035420):")
if len(naver) > 0:
    print(f"  거래대금: {naver.iloc[0]['거래대금(억)']:,.0f}억")
    print(f"  순위: {df_turnover[df_turnover['티커'] == '035420'].index[0] + 1}위")
else:
    print("  없음")

print(f"\n한미반도체 (042700):")
if len(hanmi) > 0:
    print(f"  거래대금: {hanmi.iloc[0]['거래대금(억)']:,.0f}억")
    print(f"  순위: {df_turnover[df_turnover['티커'] == '042700'].index[0] + 1}위")
else:
    print("  없음")

# 2. trading_signals.xlsx 확인
print("\n2. trading_signals.xlsx 분석:")
df_summary = pd.read_excel('output/trading_signals.xlsx', sheet_name='Summary')
print(f"Summary 종목 수: {len(df_summary)}")

# 이격도가 비정상적으로 높은 종목들 확인 (테스트 데이터 가능성)
print("\n1차 매수선 이격도가 50% 이상인 종목들 (테스트 데이터 의심):")
high_distance = df_summary[df_summary['1차매수선이격도(%)'] > 50]
print(high_distance[['티커', '종목명', '1차매수선이격도(%)']].to_string())

print("\n=== 분석 완료 ===")




