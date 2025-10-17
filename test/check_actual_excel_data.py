"""
실제 엑셀 데이터 정확한 확인
"""

import pandas as pd

def check_actual_excel_data():
    print("=== 실제 엑셀 데이터 정확한 확인 ===")
    
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        print(f"총 종목 수: {len(df)}개")
        print(f"컬럼명: {df.columns.tolist()}")
        
        # 중복 제거
        df_unique = df.drop_duplicates(subset=['티커'], keep='first')
        print(f"중복 제거 후: {len(df_unique)}개")
        
        # 매수 신호 종목들 (거리 0.01 이하)
        buy_signals = df_unique[df_unique.iloc[:, 11] <= 0.01]
        print(f"매수 신호 종목 수: {len(buy_signals)}개")
        
        print(f"\n매수 신호 종목들의 상세 데이터:")
        for idx, row in buy_signals.iterrows():
            ticker = row.iloc[0]
            name = row.iloc[1]
            distance = row.iloc[11]
            current_price = row.iloc[5]
            buy_line = row.iloc[10]
            
            print(f"\n{name} ({ticker}):")
            print(f"  현재가: {current_price}")
            print(f"  매수라인: {buy_line}")
            print(f"  엑셀 거리값: {distance}")
            print(f"  엑셀 거리값 타입: {type(distance)}")
            
            # 수동 계산
            if buy_line > 0:
                manual_distance = ((current_price - buy_line) / buy_line) * 100
                print(f"  수동 계산 거리: {manual_distance:.6f}%")
                
                # 엑셀값이 퍼센트인지 소수인지 확인
                if abs(distance - manual_distance) < 0.000001:
                    print(f"  → 엑셀값이 퍼센트 (정상)")
                elif abs(distance * 100 - manual_distance) < 0.000001:
                    print(f"  → 엑셀값이 소수 (100 곱해야 함)")
                else:
                    print(f"  → 엑셀값이 다른 형태")
        
        # 거리가 0이 아닌 종목들도 확인
        print(f"\n거리가 0이 아닌 종목들 (처음 5개):")
        non_zero = df_unique[df_unique.iloc[:, 11] > 0.01].head(5)
        
        for idx, row in non_zero.iterrows():
            ticker = row.iloc[0]
            name = row.iloc[1]
            distance = row.iloc[11]
            current_price = row.iloc[5]
            buy_line = row.iloc[10]
            
            print(f"\n{name} ({ticker}):")
            print(f"  현재가: {current_price}")
            print(f"  매수라인: {buy_line}")
            print(f"  엑셀 거리값: {distance}")
            
            if buy_line > 0:
                manual_distance = ((current_price - buy_line) / buy_line) * 100
                print(f"  수동 계산 거리: {manual_distance:.6f}%")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_actual_excel_data()

