"""
거리 계산 디버깅
"""

import pandas as pd

def debug_distance_calculation():
    print("=== 거리 계산 디버깅 ===")
    
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        print(f"총 종목 수: {len(df)}개")
        print(f"컬럼명: {df.columns.tolist()}")
        
        # 처음 5개 종목의 거리 계산 관련 데이터 확인
        print(f"\n처음 5개 종목의 거리 계산 데이터:")
        for idx, row in df.head(5).iterrows():
            ticker = row.iloc[0]
            name = row.iloc[1]
            current_price = row.iloc[5]  # 현재가
            buy_line_1 = row.iloc[10]    # 1차매수라인
            distance = row.iloc[11]      # 거리(%)
            
            print(f"\n{name} ({ticker}):")
            print(f"  현재가: {current_price}")
            print(f"  1차매수라인: {buy_line_1}")
            print(f"  거리: {distance}")
            
            # 수동으로 거리 계산
            if current_price > 0 and buy_line_1 > 0:
                manual_distance = ((current_price - buy_line_1) / buy_line_1) * 100
                print(f"  수동 계산 거리: {manual_distance:.2f}%")
            else:
                print(f"  계산 불가: 현재가={current_price}, 매수라인={buy_line_1}")
        
        # 거리 컬럼의 고유값 확인
        distance_col = df.iloc[:, 11]
        unique_distances = distance_col.unique()
        print(f"\n거리 컬럼 고유값: {sorted(unique_distances)}")
        
        # 거리가 0이 아닌 종목들 찾기
        non_zero_distances = df[distance_col != 0]
        print(f"\n거리가 0이 아닌 종목 수: {len(non_zero_distances)}개")
        
        if len(non_zero_distances) > 0:
            print("거리가 0이 아닌 종목들:")
            for idx, row in non_zero_distances.head(10).iterrows():
                ticker = row.iloc[0]
                name = row.iloc[1]
                distance = row.iloc[11]
                print(f"  {name} ({ticker}): 거리 {distance}%")
        
        # 거리 컬럼의 데이터 타입 확인
        print(f"\n거리 컬럼 데이터 타입: {distance_col.dtype}")
        print(f"거리 컬럼 통계:")
        print(f"  평균: {distance_col.mean()}")
        print(f"  최소값: {distance_col.min()}")
        print(f"  최대값: {distance_col.max()}")
        print(f"  표준편차: {distance_col.std()}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_distance_calculation()

