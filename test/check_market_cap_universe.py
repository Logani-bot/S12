"""
market_cap_universe.xlsx의 종목명 확인
"""

import pandas as pd

def check_market_cap_universe():
    print("=== market_cap_universe.xlsx 종목명 확인 ===")
    
    try:
        df = pd.read_excel('output/market_cap_universe.xlsx')
        
        print(f"총 종목 수: {len(df)}개")
        print(f"컬럼명: {df.columns.tolist()}")
        
        # 종목명이 "종목_xxxxx" 형태인 것들 찾기
        print(f"\n종목명이 '종목_xxxxx' 형태인 종목들:")
        invalid_names = df[df['종목명'].str.contains('종목_', na=False)]
        print(f"  총 개수: {len(invalid_names)}개")
        
        for idx, row in invalid_names.head(10).iterrows():
            ticker = row['티커']
            name = row['종목명']
            print(f"  {ticker}: {name}")
        
        # 정상적인 종목명들도 확인
        print(f"\n정상적인 종목명들 (처음 10개):")
        valid_names = df[~df['종목명'].str.contains('종목_', na=False)]
        for idx, row in valid_names.head(10).iterrows():
            ticker = row['티커']
            name = row['종목명']
            print(f"  {ticker}: {name}")
        
        # 특정 티커들 확인
        target_tickers = ['456040', '457550', '5930', '660']
        print(f"\n특정 티커들의 종목명:")
        for ticker in target_tickers:
            match = df[df['티커'] == ticker]
            if not match.empty:
                name = match.iloc[0]['종목명']
                print(f"  {ticker}: {name}")
            else:
                print(f"  {ticker}: 찾을 수 없음")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_market_cap_universe()

