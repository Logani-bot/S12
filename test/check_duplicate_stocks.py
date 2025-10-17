"""
동일 종목 중복 및 이격도 0 수렴 문제 확인
"""

import pandas as pd

def check_duplicate_stocks():
    print("=== 동일 종목 중복 및 이격도 0 수렴 문제 확인 ===")
    
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        print(f"총 종목 수: {len(df)}개")
        
        # 종목별 그룹핑
        ticker_groups = df.groupby('티커')
        
        print(f"\n중복 종목 확인:")
        duplicates = []
        
        for ticker, group in ticker_groups:
            if len(group) > 1:
                duplicates.append((ticker, len(group)))
                print(f"\n{ticker} ({group.iloc[0]['종목명']}): {len(group)}개")
                
                for idx, row in group.iterrows():
                    current_price = row.iloc[5]
                    buy_line = row.iloc[10]
                    distance = row.iloc[11]
                    
                    print(f"  - 현재가: {current_price:,}원, 매수라인: {buy_line:,}원, 거리: {distance:.6f}%")
                    
                    # 이격도가 0에 수렴하는 경우 확인
                    if distance < 0.000001:  # 0.0001% 미만
                        print(f"    [WARNING] 이격도 0 수렴: {distance:.10f}%")
                        
                        # 수동 계산
                        if buy_line > 0:
                            manual_distance = ((current_price - buy_line) / buy_line) * 100
                            print(f"    수동 계산: {manual_distance:.10f}%")
        
        print(f"\n중복 종목 요약:")
        print(f"  총 중복 종목 수: {len(duplicates)}개")
        
        for ticker, count in duplicates:
            print(f"  {ticker}: {count}개")
        
        # 이격도가 0에 수렴하는 종목들 확인
        print(f"\n이격도 0 수렴 종목들:")
        zero_distance_stocks = df[df.iloc[:, 11] < 0.000001]
        
        print(f"  총 개수: {len(zero_distance_stocks)}개")
        
        for idx, row in zero_distance_stocks.head(10).iterrows():
            ticker = row.iloc[0]
            name = row.iloc[1]
            current_price = row.iloc[5]
            buy_line = row.iloc[10]
            distance = row.iloc[11]
            
            print(f"  {name} ({ticker}): 거리 {distance:.10f}%")
            print(f"    현재가: {current_price:,}원, 매수라인: {buy_line:,}원")
            
            # 수동 계산
            if buy_line > 0:
                manual_distance = ((current_price - buy_line) / buy_line) * 100
                print(f"    수동 계산: {manual_distance:.10f}%")
        
        # 전체 데이터에서 거리 분포 확인
        distance_col = df.iloc[:, 11]
        print(f"\n거리 분포:")
        print(f"  최소값: {distance_col.min():.10f}")
        print(f"  최대값: {distance_col.max():.10f}")
        print(f"  평균값: {distance_col.mean():.10f}")
        print(f"  중간값: {distance_col.median():.10f}")
        
        # 거리 구간별 개수
        print(f"\n거리 구간별 개수:")
        ranges = [
            (0, 0.000001, "0 수렴"),
            (0.000001, 0.01, "0.0001%~1%"),
            (0.01, 0.1, "1%~10%"),
            (0.1, 1.0, "10%~100%"),
            (1.0, float('inf'), "100% 이상")
        ]
        
        for min_val, max_val, label in ranges:
            if max_val == float('inf'):
                count = len(distance_col[distance_col >= min_val])
            else:
                count = len(distance_col[(distance_col >= min_val) & (distance_col < max_val)])
            print(f"  {label}: {count}개")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_duplicate_stocks()
