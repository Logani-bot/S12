import pandas as pd

print("=== 테스트용 더미 데이터 찾기 ===")

# trading_signals.xlsx에서 비정상적인 이격도를 가진 종목들 확인
df_summary = pd.read_excel('output/trading_signals.xlsx', sheet_name='Summary')

print("1. 1차 매수선 이격도가 50% 이상인 종목들:")
high_distance = df_summary[df_summary['1차매수선이격도(%)'] > 50]
print(high_distance[['티커', '종목명', '1차매수선이격도(%)', '종가', '1차매수선']].to_string())

print("\n2. 1차 매수선 이격도가 100% 이상인 종목들 (확실한 더미 데이터):")
very_high_distance = df_summary[df_summary['1차매수선이격도(%)'] > 100]
print(very_high_distance[['티커', '종목명', '1차매수선이격도(%)', '종가', '1차매수선']].to_string())

print("\n3. 종가가 1차 매수선보다 2배 이상 높은 종목들:")
df_summary['가격비율'] = df_summary['종가'] / df_summary['1차매수선']
very_high_ratio = df_summary[df_summary['가격비율'] > 2.0]
print(very_high_ratio[['티커', '종목명', '종가', '1차매수선', '가격비율']].to_string())

print("\n=== 분석 완료 ===")




