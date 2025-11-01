"""
업비트 알람 시스템 데모 (임계값 낮춰서 테스트)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from upbit_alert_monitor import (
    get_krw_markets, 
    get_ticker_data, 
    analyze_market_dropdown
)
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_with_lower_threshold():
    """낮은 임계값으로 테스트"""
    
    print("=" * 60)
    print("업비트 알람 시스템 데모 (임계값: 5% 하락)")
    print("=" * 60)
    
    try:
        # 1. KRW 마켓 목록 조회
        print("\n1. KRW 마켓 목록 조회 중...")
        krw_markets = get_krw_markets()
        print(f"✓ KRW 마켓 {len(krw_markets)}개 조회 완료")
        
        if not krw_markets:
            print("❌ KRW 마켓 목록을 가져올 수 없습니다.")
            return False
        
        # 2. 티커 데이터 조회 (처음 50개)
        print("\n2. 티커 데이터 조회 중...")
        test_markets = krw_markets[:50]  # 50개로 늘림
        ticker_data = get_ticker_data(test_markets)
        print(f"✓ 티커 데이터 {len(ticker_data)}개 조회 완료")
        
        if not ticker_data:
            print("❌ 티커 데이터를 가져올 수 없습니다.")
            return False
        
        # 3. 하락 분석 (5% 이상 하락으로 변경)
        print("\n3. 하락 분석 중... (5% 이상 하락)")
        
        significant_drops = []
        total_markets = len(ticker_data)
        
        for ticker in ticker_data:
            try:
                market = ticker.get('market', '')
                korean_name = ticker.get('korean_name', '')
                trade_price = ticker.get('trade_price', 0)
                prev_closing_price = ticker.get('prev_closing_price', 0)
                
                if not prev_closing_price or prev_closing_price == 0:
                    continue
                    
                # 전일대비 변동률 계산
                change_rate = ((trade_price - prev_closing_price) / prev_closing_price) * 100
                
                # 5% 이상 하락한 종목 (데모용)
                if change_rate <= -5.0:
                    significant_drops.append({
                        'market': market,
                        'korean_name': korean_name,
                        'trade_price': trade_price,
                        'prev_closing_price': prev_closing_price,
                        'change_rate': change_rate
                    })
                    
            except Exception as e:
                continue
        
        # 하락률 순으로 정렬
        significant_drops.sort(key=lambda x: x['change_rate'])
        
        drop_count = len(significant_drops)
        print(f"✓ 분석 완료: {drop_count}개 하락 (전체 {total_markets}개)")
        
        # 하락 종목 상세 정보 출력
        if significant_drops:
            print(f"\n📉 5% 이상 하락한 종목 ({len(significant_drops)}개):")
            for i, drop in enumerate(significant_drops[:10], 1):  # 상위 10개만
                market = drop['market'].replace('KRW-', '')
                korean_name = drop['korean_name']
                change_rate = drop['change_rate']
                trade_price = drop['trade_price']
                
                print(f"  {i:2d}. {korean_name} ({market})")
                print(f"      현재가: {trade_price:,.0f}원")
                print(f"      하락률: {change_rate:+.1f}%")
        else:
            print("📈 5% 이상 하락한 종목이 없습니다.")
        
        # 4. 알람 메시지 형식 데모
        if drop_count > 0:
            print(f"\n4. 알람 메시지 형식 데모:")
            print("=" * 40)
            
            # 심볼만 나열
            symbols = []
            for drop in significant_drops:
                symbol = drop['market'].replace('KRW-', '')
                symbols.append(symbol)
            
            message = f"🚨 코인 저점 시그널 알림 🚨\n"
            message += f"──────────────────\n\n"
            message += f"📊 전일대비 5% 이상 하락: {drop_count}개\n"
            message += f"하락 종목 : {' '.join(symbols)}"
            
            print(message)
            print("=" * 40)
        
        print("\n" + "=" * 60)
        print("데모 완료!")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ 데모 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_with_lower_threshold()

















