"""
pykrx 데이터 구조 확인
"""

import pandas as pd
from pykrx import stock
from datetime import datetime

def check_data_structure():
    print("=== pykrx 데이터 구조 확인 ===")
    
    # 오늘 날짜
    today = datetime.now().strftime("%Y%m%d")
    print(f"조회 날짜: {today}")
    
    try:
        # KOSPI 데이터 확인
        print("\n1. KOSPI 데이터 구조:")
        kospi_df = stock.get_market_cap_by_ticker(today, market="KOSPI")
        print(f"  Shape: {kospi_df.shape}")
        print(f"  Columns: {kospi_df.columns.tolist()}")
        print(f"  Index: {kospi_df.index.name}")
        print(f"  Sample data:")
        print(kospi_df.head(3))
        
        # KOSDAQ 데이터 확인
        print("\n2. KOSDAQ 데이터 구조:")
        kosdaq_df = stock.get_market_cap_by_ticker(today, market="KOSDAQ")
        print(f"  Shape: {kosdaq_df.shape}")
        print(f"  Columns: {kosdaq_df.columns.tolist()}")
        print(f"  Index: {kosdaq_df.index.name}")
        print(f"  Sample data:")
        print(kosdaq_df.head(3))
        
        # 시가총액 컬럼 확인
        print("\n3. 시가총액 데이터 확인:")
        if '시가총액' in kospi_df.columns:
            print("  KOSPI 시가총액 상위 5개:")
            print(kospi_df['시가총액'].nlargest(5))
            
            # 1.5조 이상 필터링 테스트
            threshold = 1_500_000_000_000
            kospi_filtered = kospi_df[kospi_df['시가총액'] >= threshold]
            print(f"\n  KOSPI에서 1.5조 이상: {len(kospi_filtered)}개")
            
            if len(kospi_filtered) > 0:
                print("  상위 종목들:")
                for idx, row in kospi_filtered.head(5).iterrows():
                    print(f"    {idx}: {row['시가총액']:,.0f}원")
        
        if '시가총액' in kosdaq_df.columns:
            kosdaq_filtered = kosdaq_df[kosdaq_df['시가총액'] >= threshold]
            print(f"\n  KOSDAQ에서 1.5조 이상: {len(kosdaq_filtered)}개")
        
        # 어제 데이터도 시도
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        print(f"\n4. 어제 날짜({yesterday}) 데이터 확인:")
        
        try:
            kospi_yesterday = stock.get_market_cap_by_ticker(yesterday, market="KOSPI")
            print(f"  KOSPI 어제: {kospi_yesterday.shape}")
            if '시가총액' in kospi_yesterday.columns:
                kospi_filtered_y = kospi_yesterday[kospi_yesterday['시가총액'] >= threshold]
                print(f"  KOSPI 어제 1.5조 이상: {len(kospi_filtered_y)}개")
        except Exception as e:
            print(f"  어제 KOSPI 데이터 실패: {e}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_data_structure()

