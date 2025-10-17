"""
pykrx 종목명 조회 테스트
"""

from pykrx import stock
from datetime import datetime, timedelta

def test_ticker_names():
    print("=== pykrx 종목명 조회 테스트 ===")
    
    # 테스트 티커들
    test_tickers = ["005930", "000660", "035420", "207940", "373220"]
    
    # 어제 날짜
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    print(f"조회 날짜: {yesterday}")
    
    print("\n1. get_market_ticker_name 테스트:")
    for ticker in test_tickers:
        try:
            name = stock.get_market_ticker_name(ticker, yesterday)
            print(f"  {ticker}: {name}")
        except Exception as e:
            print(f"  {ticker}: 오류 - {e}")
    
    print("\n2. get_market_ticker_list 테스트:")
    try:
        # KOSPI 전체 티커 리스트
        kospi_tickers = stock.get_market_ticker_list(yesterday, market="KOSPI")
        print(f"  KOSPI 전체 티커: {len(kospi_tickers)}개")
        print(f"  처음 10개: {kospi_tickers[:10]}")
        
        # KOSDAQ 전체 티커 리스트  
        kosdaq_tickers = stock.get_market_ticker_list(yesterday, market="KOSDAQ")
        print(f"  KOSDAQ 전체 티커: {len(kosdaq_tickers)}개")
        print(f"  처음 10개: {kosdaq_tickers[:10]}")
        
    except Exception as e:
        print(f"  티커 리스트 조회 오류: {e}")
    
    print("\n3. 다른 방법으로 종목명 조회:")
    try:
        # OHLCV 데이터에서 종목명 확인
        ohlcv = stock.get_market_ohlcv_by_ticker(yesterday, market="KOSPI")
        print(f"  OHLCV 데이터 컬럼: {ohlcv.columns.tolist()}")
        print(f"  OHLCV 샘플:")
        print(ohlcv.head(3))
        
        # 시가총액 데이터에서 종목명 확인
        market_cap = stock.get_market_cap_by_ticker(yesterday, market="KOSPI")
        print(f"  시가총액 데이터 컬럼: {market_cap.columns.tolist()}")
        print(f"  시가총액 샘플:")
        print(market_cap.head(3))
        
    except Exception as e:
        print(f"  데이터 조회 오류: {e}")

if __name__ == "__main__":
    test_ticker_names()

