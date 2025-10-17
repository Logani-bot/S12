"""
매수 신호 종목 찾기
"""

import pandas as pd

def find_buy_signals():
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        print(f"=== 매수 신호 종목 찾기 ===")
        print(f"총 종목 수: {len(df)}개")
        
        # 로그에서 확인한 매수 신호 종목들
        target_stocks = ['097230', '082270']  # HJ중공업, 하나금융지주
        
        print(f"\n로그에서 확인한 매수 신호 종목 찾기:")
        for target_ticker in target_stocks:
            # 티커로 찾기
            stock_row = df[df.iloc[:, 0].astype(str) == target_ticker]
            if len(stock_row) > 0:
                row = stock_row.iloc[0]
                ticker = row.iloc[0]
                name = row.iloc[1]
                signal_info = row.iloc[4]  # 신호정보 컬럼
                print(f"  {name} ({ticker}): {signal_info}")
            else:
                print(f"  {target_ticker}: 찾을 수 없음")
        
        # 거리(%) 컬럼에서 매수 신호 찾기 (8% 이하)
        distance_col_idx = 11  # 1차매수라인거리(%) 컬럼
        if len(df.columns) > distance_col_idx:
            print(f"\n거리 8% 이하 종목 찾기:")
            buy_candidates = df[df.iloc[:, distance_col_idx] <= 8.0]
            if len(buy_candidates) > 0:
                print(f"  총 {len(buy_candidates)}개 종목:")
                for idx, row in buy_candidates.iterrows():
                    ticker = row.iloc[0]
                    name = row.iloc[1]
                    distance = row.iloc[distance_col_idx]
                    print(f"    {name} ({ticker}): 거리 {distance:.1f}%")
            else:
                print("  거리 8% 이하 종목이 없습니다.")
        
        # 신호정보 컬럼에서 매수 관련 텍스트 찾기
        signal_info_col_idx = 4  # 신호정보 컬럼
        if len(df.columns) > signal_info_col_idx:
            print(f"\n신호정보에서 매수 관련 텍스트 찾기:")
            for idx, row in df.iterrows():
                signal_info = str(row.iloc[signal_info_col_idx])
                if '매수' in signal_info and '신호' in signal_info:
                    ticker = row.iloc[0]
                    name = row.iloc[1]
                    print(f"  {name} ({ticker}): {signal_info}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_buy_signals()

