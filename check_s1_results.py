"""
S1 트레이딩 시그널 결과 확인
"""

import pandas as pd

def check_s1_results():
    try:
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        print(f"=== S1 트레이딩 시그널 결과 ===")
        print(f"총 종목 수: {len(df)}개")
        print(f"컬럼명: {df.columns.tolist()}")
        
        # 신호 컬럼 찾기 (한글 깨짐 고려)
        signal_col = None
        for i, col in enumerate(df.columns):
            if '신호' in str(col) or 'signal' in str(col).lower():
                signal_col = col
                break
        
        if signal_col:
            signal_counts = df[signal_col].value_counts()
            print(f"\n신호별 개수:")
            for signal, count in signal_counts.items():
                print(f"  {signal}: {count}개")
            
            # 매수 신호만 확인
            buy_signals = df[df[signal_col] == '매수']
            if len(buy_signals) > 0:
                print(f"\n매수 신호 종목 ({len(buy_signals)}개):")
                for idx, row in buy_signals.iterrows():
                    ticker = row.iloc[0] if len(row) > 0 else 'N/A'
                    name = row.iloc[1] if len(row) > 1 else 'N/A'
                    current_price = row.iloc[5] if len(row) > 5 else 0  # 현재가 컬럼
                    buy_line_1 = row.iloc[10] if len(row) > 10 else 0  # 1차매수라인 컬럼
                    distance = row.iloc[11] if len(row) > 11 else 0    # 거리 컬럼
                    print(f"  {name} ({ticker}): 현재가 {current_price:,}원, 1차매수라인 {buy_line_1:,}원, 거리 {distance:.1f}%")
        else:
            print("신호 컬럼을 찾을 수 없습니다.")
            
            # 매수 신호를 다른 방법으로 찾기
            print("\n매수 신호 찾기 시도...")
            for i, col in enumerate(df.columns):
                if '매수' in str(col):
                    print(f"매수 관련 컬럼 발견: {col} (인덱스 {i})")
                    unique_values = df[col].unique()
                    print(f"  고유값: {unique_values}")
                    
                    # 매수 신호가 있는 행 찾기 (문자열이 아닌 경우 고려)
                    try:
                        if df[col].dtype == 'object':
                            buy_rows = df[df[col].astype(str).str.contains('매수', na=False)]
                        else:
                            # 숫자 컬럼의 경우 특정 값으로 매수 신호 찾기
                            buy_rows = df[df[col] > 0]  # 예: 거리 > 0인 경우
                        
                        if len(buy_rows) > 0:
                            print(f"\n매수 신호 종목 ({len(buy_rows)}개):")
                            for idx, row in buy_rows.iterrows():
                                ticker = row.iloc[0] if len(row) > 0 else 'N/A'
                                name = row.iloc[1] if len(row) > 1 else 'N/A'
                                print(f"  {name} ({ticker})")
                    except Exception as e:
                        print(f"  오류: {e}")
        
        print(f"\n파일 저장 완료: output/trading_signals_s1.xlsx")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_s1_results()
