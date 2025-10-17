"""
정확한 거리값 확인
"""

import pandas as pd

def check_exact_distances():
    print("=== 정확한 거리값 확인 ===")
    
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        # 매수 신호 종목들 (거리 1% 이하)
        buy_signals = df[df.iloc[:, 11] <= 0.01]
        
        print(f"매수 신호 종목 수: {len(buy_signals)}개")
        print(f"\n상위 10개 종목의 정확한 거리값:")
        
        top_10 = buy_signals.nsmallest(10, df.columns[11])
        
        for idx, row in top_10.iterrows():
            ticker = row.iloc[0]
            name = row.iloc[1]
            distance = row.iloc[11]
            current_price = row.iloc[5]
            buy_line = row.iloc[10]
            
            # 정확한 거리 계산
            exact_distance = ((current_price - buy_line) / buy_line) * 100
            
            print(f"\n{name} ({ticker}):")
            print(f"  현재가: {current_price:,}원")
            print(f"  매수라인: {buy_line:,}원")
            print(f"  엑셀 거리값: {distance:.10f}")
            print(f"  엑셀 거리(%): {distance*100:.10f}%")
            print(f"  수동 계산 거리: {exact_distance:.10f}%")
            
            # 매수라인 대비 현재가 상태
            if current_price > buy_line:
                print(f"  상태: 매수라인 돌파 (+{exact_distance:.2f}%)")
            elif current_price < buy_line:
                print(f"  상태: 매수라인 미달 (-{abs(exact_distance):.2f}%)")
            else:
                print(f"  상태: 매수라인 동일")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_exact_distances()

