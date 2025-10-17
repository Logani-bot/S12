"""
최종 결과 확인
"""

import pandas as pd

def check_results():
    try:
        df = pd.read_excel('output/market_cap_universe.xlsx')
        
        print(f"=== 최종 결과 확인 ===")
        print(f"총 종목 수: {len(df)}개")
        print(f"5조 이상: {len(df[df['시총구분'] == '5조이상'])}개")
        print(f"1.5조~5조: {len(df[df['시총구분'] == '1.5조이상'])}개")
        
        print(f"\n컬럼 정보:")
        print(f"컬럼명: {df.columns.tolist()}")
        
        print(f"\n상위 10개 종목:")
        for idx, row in df.head(10).iterrows():
            ticker = row.iloc[0]  # 첫 번째 컬럼 (티커)
            market_cap = row.iloc[1]  # 두 번째 컬럼 (시총)
            current_price = row.iloc[2]  # 세 번째 컬럼 (현재가)
            volume = row.iloc[3]  # 네 번째 컬럼 (거래량)
            market = row.iloc[4]  # 다섯 번째 컬럼 (시장)
            category = row.iloc[5]  # 여섯 번째 컬럼 (시총구분)
            print(f"  {idx+1:2d}. {ticker} - {market_cap:,.0f}원 [{category}] {market}")
        
        print(f"\n파일 저장 완료: output/market_cap_universe.xlsx")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_results()
