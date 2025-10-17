"""
market_cap_universe.xlsx의 구조 확인
"""

import pandas as pd

def check_universe_structure():
    print("=== market_cap_universe.xlsx 구조 확인 ===")
    
    try:
        df = pd.read_excel('output/market_cap_universe.xlsx')
        
        print(f"총 종목 수: {len(df)}개")
        print(f"컬럼명: {df.columns.tolist()}")
        
        # 첫 5개 종목의 모든 데이터 확인
        print(f"\n첫 5개 종목의 모든 데이터:")
        for idx, row in df.head(5).iterrows():
            print(f"\n{idx+1}. {row['종목명']} ({row['티커']}):")
            for col in df.columns:
                print(f"  {col}: {row[col]} (타입: {type(row[col])})")
        
        # 특정 티커들 확인
        target_tickers = ['456040', '457550']
        print(f"\n특정 티커들의 데이터:")
        for ticker in target_tickers:
            match = df[df['티커'] == ticker]
            if not match.empty:
                row = match.iloc[0]
                print(f"\n{ticker}:")
                for col in df.columns:
                    print(f"  {col}: {row[col]} (타입: {type(row[col])})")
            else:
                print(f"  {ticker}: 찾을 수 없음")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_universe_structure()

