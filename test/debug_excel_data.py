"""
엑셀 데이터 정확한 분석
"""

import pandas as pd
import numpy as np

def debug_excel_data():
    print("=== 엑셀 데이터 정확한 분석 ===")
    
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        print(f"총 종목 수: {len(df)}개")
        print(f"컬럼명: {df.columns.tolist()}")
        
        # 거리 컬럼 (11번째) 상세 분석
        distance_col = df.iloc[:, 11]
        print(f"\n거리 컬럼 상세 분석:")
        print(f"  데이터 타입: {distance_col.dtype}")
        print(f"  최소값: {distance_col.min()}")
        print(f"  최대값: {distance_col.max()}")
        print(f"  평균값: {distance_col.mean()}")
        print(f"  표준편차: {distance_col.std()}")
        
        # 처음 5개 종목의 모든 거리 관련 컬럼 확인
        print(f"\n처음 5개 종목의 모든 데이터:")
        for idx, row in df.head(5).iterrows():
            ticker = row.iloc[0]
            name = row.iloc[1]
            current_price = row.iloc[5]  # 현재가
            buy_line = row.iloc[10]     # 1차매수라인
            distance = row.iloc[11]     # 1차매수라인거리(%)
            
            print(f"\n{name} ({ticker}):")
            print(f"  현재가: {current_price} (타입: {type(current_price)})")
            print(f"  1차매수라인: {buy_line} (타입: {type(buy_line)})")
            print(f"  엑셀 거리값: {distance} (타입: {type(distance)})")
            
            # 수동으로 거리 계산
            if pd.notna(current_price) and pd.notna(buy_line) and buy_line != 0:
                manual_distance = ((current_price - buy_line) / buy_line) * 100
                print(f"  수동 계산 거리: {manual_distance:.6f}%")
                
                # 엑셀값과 수동 계산값 비교
                if pd.notna(distance):
                    diff = abs(distance - manual_distance)
                    print(f"  차이: {diff:.10f}")
                    
                    # 엑셀값이 소수인지 확인
                    if distance < 1:
                        print(f"  엑셀값이 소수: {distance:.10f} → 실제값: {distance:.10f}%")
                    else:
                        print(f"  엑셀값이 퍼센트: {distance:.10f}%")
        
        # 거리 컬럼의 고유값 샘플
        print(f"\n거리 컬럼 고유값 샘플 (처음 20개):")
        unique_values = sorted(distance_col.dropna().unique())
        for i, val in enumerate(unique_values[:20]):
            print(f"  {i+1:2d}: {val:.10f}")
        
        # 거리가 0이 아닌 값들 확인
        non_zero = distance_col[distance_col != 0]
        print(f"\n거리가 0이 아닌 값들 (처음 10개):")
        for i, val in enumerate(non_zero.head(10)):
            print(f"  {i+1:2d}: {val:.10f}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_excel_data()

