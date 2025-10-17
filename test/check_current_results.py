import pandas as pd

# 현재 결과 확인
df = pd.read_excel('output/market_cap_universe.xlsx')

print(f"현재 확인된 종목: {len(df)}개")
print(f"5조 이상: {len(df[df['시총구분'] == '5조이상'])}개")
print(f"1.5조~5조: {len(df[df['시총구분'] == '1.5조이상'])}개")

print("\n상위 20개 종목:")
print(df.head(20)[['티커', '종목명', '시총(원)', '시총구분']].to_string(index=False))

print(f"\n문제: 실제로는 시총 1.5조 이상인 종목이 239개인데, 현재는 {len(df)}개만 확인됨")
print("해결방법: 전체 종목 리스트를 가져와서 모든 종목의 시총을 확인해야 함")

