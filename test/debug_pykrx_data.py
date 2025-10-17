"""
pykrx 데이터 구조 디버깅
"""

import pandas as pd
from datetime import datetime, timedelta

try:
    from pykrx import stock
    HAS_PYKRX = True
except ImportError:
    HAS_PYKRX = False
    print("pykrx 라이브러리가 설치되지 않았습니다.")
    exit(1)

def debug_pykrx_data():
    print("=== pykrx 데이터 구조 디버깅 ===")
    
    try:
        # 오늘 날짜
        today = datetime.now()
        date_str = today.strftime("%Y%m%d")
        print(f"조회 날짜: {date_str}")
        
        # KOSPI 데이터 조회
        print(f"\nKOSPI 데이터 조회 중...")
        try:
            kospi_df = stock.get_market_cap_by_ticker(date_str, market="KOSPI")
            print(f"KOSPI 데이터 조회 성공: {len(kospi_df)}개 종목")
            print(f"컬럼명: {kospi_df.columns.tolist()}")
            
            if not kospi_df.empty:
                print(f"\nKOSPI 첫 5개 종목:")
                for i, (ticker, row) in enumerate(kospi_df.head(5).iterrows()):
                    print(f"  {i+1}. {ticker}: {dict(row)}")
                
                # 시총 컬럼 확인
                market_cap_cols = [col for col in kospi_df.columns if '시총' in col or '총액' in col or 'market' in col.lower()]
                print(f"\n시총 관련 컬럼: {market_cap_cols}")
                
                if market_cap_cols:
                    market_cap_col = market_cap_cols[0]
                    print(f"시총 컬럼 '{market_cap_col}' 샘플 데이터:")
                    print(kospi_df[market_cap_col].head(10))
                    
                    # 1.5조 이상 종목 확인
                    threshold = 1_500_000_000_000
                    large_cap = kospi_df[kospi_df[market_cap_col] >= threshold]
                    print(f"\n1.5조 이상 종목: {len(large_cap)}개")
                    
                    if len(large_cap) > 0:
                        print("상위 5개:")
                        for i, (ticker, row) in enumerate(large_cap.head(5).iterrows()):
                            market_cap = row[market_cap_col]
                            print(f"  {i+1}. {ticker}: {market_cap:,.0f}원")
                
        except Exception as e:
            print(f"KOSPI 조회 실패: {e}")
        
        # 어제 날짜로 재시도
        print(f"\n어제 날짜로 재시도...")
        yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")
        print(f"조회 날짜: {yesterday}")
        
        try:
            kospi_df = stock.get_market_cap_by_ticker(yesterday, market="KOSPI")
            print(f"어제 KOSPI 데이터 조회 성공: {len(kospi_df)}개 종목")
            print(f"컬럼명: {kospi_df.columns.tolist()}")
            
            if not kospi_df.empty:
                # 시총 컬럼 확인
                market_cap_cols = [col for col in kospi_df.columns if '시총' in col or '총액' in col or 'market' in col.lower()]
                print(f"시총 관련 컬럼: {market_cap_cols}")
                
                if market_cap_cols:
                    market_cap_col = market_cap_cols[0]
                    threshold = 1_500_000_000_000
                    large_cap = kospi_df[kospi_df[market_cap_col] >= threshold]
                    print(f"1.5조 이상 종목: {len(large_cap)}개")
                    
                    if len(large_cap) > 0:
                        print("상위 5개:")
                        for i, (ticker, row) in enumerate(large_cap.head(5).iterrows()):
                            market_cap = row[market_cap_col]
                            print(f"  {i+1}. {ticker}: {market_cap:,.0f}원")
                
        except Exception as e:
            print(f"어제 KOSPI 조회 실패: {e}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_pykrx_data()

