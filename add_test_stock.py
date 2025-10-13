"""
테스트용 종목을 turnover_universe.xlsx에 간편하게 추가하는 스크립트

사용법:
    python add_test_stock.py 005380 현대차
    python add_test_stock.py 051910 LG화학 035720 카카오
"""
import sys
from datetime import date
from Daily_Turnover_Tracker import read_existing_data, save_to_excel

def add_test_stocks(ticker_name_pairs):
    """테스트용 종목 추가"""
    if len(ticker_name_pairs) < 2 or len(ticker_name_pairs) % 2 != 0:
        print("❌ 사용법: python add_test_stock.py [티커] [종목명] [티커] [종목명] ...")
        print("예시: python add_test_stock.py 005380 현대차 051910 LG화학")
        return
    
    # 기존 데이터 읽기
    df = read_existing_data('turnover_universe.xlsx')
    print(f"📊 기존 종목: {len(df)}개")
    
    # 새 종목 추가
    added = []
    skipped = []
    
    for i in range(0, len(ticker_name_pairs), 2):
        ticker = ticker_name_pairs[i].zfill(6)  # 6자리로 패딩
        name = ticker_name_pairs[i + 1]
        
        # 이미 있는지 확인
        if ticker in df['티커'].values:
            skipped.append(f"{name}({ticker})")
            continue
        
        # 새 행 추가
        today = date.today()
        new_row = {
            '첫주도주': today,
            '최근주도주': today,
            '티커': ticker,
            '종목명': name,
            '거래대금(억)': 1000.0,  # 더미 값
            '누적횟수': 1
        }
        
        import pandas as pd
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        added.append(f"{name}({ticker})")
    
    # 저장
    if added:
        save_to_excel('turnover_universe.xlsx', df, date.today().strftime("%Y-%m-%d"))
        print(f"\n✅ 추가 완료: {', '.join(added)}")
    
    if skipped:
        print(f"⚠️  이미 존재: {', '.join(skipped)}")
    
    print(f"\n📊 최종 종목: {len(df)}개")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("=" * 60)
        print("🧪 테스트용 종목 추가 스크립트")
        print("=" * 60)
        print("\n사용법:")
        print("  python add_test_stock.py [티커] [종목명] [티커] [종목명] ...")
        print("\n예시:")
        print("  python add_test_stock.py 005380 현대차")
        print("  python add_test_stock.py 051910 LG화학 035720 카카오")
        print("  python add_test_stock.py 005930 삼성전자 000660 SK하이닉스")
        print("\n" + "=" * 60)
        sys.exit(1)
    
    add_test_stocks(sys.argv[1:])


