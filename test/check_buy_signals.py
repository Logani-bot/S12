"""
매수 신호 기준 확인
"""

import pandas as pd

def check_buy_signals():
    print("=== 매수 신호 기준 확인 ===")
    
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        # 거리 컬럼 (11번째)
        distance_col = df.iloc[:, 11]
        
        print(f"총 종목 수: {len(df)}개")
        print(f"거리 통계:")
        print(f"  최소값: {distance_col.min():.6f} ({distance_col.min()*100:.3f}%)")
        print(f"  최대값: {distance_col.max():.6f} ({distance_col.max()*100:.3f}%)")
        print(f"  평균값: {distance_col.mean():.6f} ({distance_col.mean()*100:.3f}%)")
        print(f"  중간값: {distance_col.median():.6f} ({distance_col.median()*100:.3f}%)")
        
        # 다양한 기준으로 매수 신호 개수 확인
        criteria = [0.0001, 0.001, 0.01, 0.05, 0.1]  # 0.01%, 0.1%, 1%, 5%, 10%
        
        print(f"\n매수 신호 기준별 종목 수:")
        for criterion in criteria:
            count = len(df[distance_col <= criterion])
            print(f"  거리 <= {criterion:.4f} ({criterion*100:.2f}%): {count}개")
        
        # 가장 가까운 10개 종목 확인
        print(f"\n가장 가까운 10개 종목:")
        closest_10 = df.nsmallest(10, df.columns[11])
        
        for idx, row in closest_10.iterrows():
            ticker = row.iloc[0]
            name = row.iloc[1]
            distance = row.iloc[11]
            current_price = row.iloc[5]
            buy_line = row.iloc[10]
            
            print(f"  {name} ({ticker}): 거리 {distance*100:.3f}%, 현재가 {current_price:,}원, 매수라인 {buy_line:,}원")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_buy_signals()

