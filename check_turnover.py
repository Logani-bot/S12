import pandas as pd

# turnover_universe.xlsx 읽기
df = pd.read_excel('output/turnover_universe.xlsx', sheet_name='universe')
print(f'총 종목 수: {len(df)}')
print('\n상위 10개 종목:')
print(df[['티커', '종목명', '거래대금(억)']].head(10))

# NAVER와 한미반도체가 있는지 확인
naver_stocks = df[df['종목명'].str.contains('NAVER', na=False)]
hanmi_stocks = df[df['종목명'].str.contains('한미', na=False)]

print(f'\nNAVER 관련 종목: {len(naver_stocks)}개')
if len(naver_stocks) > 0:
    print(naver_stocks[['티커', '종목명']].to_string())

print(f'\n한미 관련 종목: {len(hanmi_stocks)}개')
if len(hanmi_stocks) > 0:
    print(hanmi_stocks[['티커', '종목명']].to_string())



