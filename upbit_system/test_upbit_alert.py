"""
업비트 알람 시스템 테스트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from upbit_alert_monitor import (
    get_krw_markets, 
    get_ticker_data, 
    analyze_market_dropdown,
    send_dropdown_alert
)
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_upbit_alert():
    """업비트 알람 시스템 테스트"""
    
    print("=" * 60)
    print("업비트 알람 시스템 테스트")
    print("=" * 60)
    
    try:
        # 1. KRW 마켓 목록 조회 테스트
        print("\n1. KRW 마켓 목록 조회 중...")
        krw_markets = get_krw_markets()
        print(f"✓ KRW 마켓 {len(krw_markets)}개 조회 완료")
        
        if not krw_markets:
            print("❌ KRW 마켓 목록을 가져올 수 없습니다.")
            return False
        
        # 2. 티커 데이터 조회 테스트 (처음 10개만)
        print("\n2. 티커 데이터 조회 중...")
        test_markets = krw_markets[:10]  # 테스트용으로 10개만
        ticker_data = get_ticker_data(test_markets)
        print(f"✓ 티커 데이터 {len(ticker_data)}개 조회 완료")
        
        if not ticker_data:
            print("❌ 티커 데이터를 가져올 수 없습니다.")
            return False
        
        # 3. 하락 분석 테스트
        print("\n3. 하락 분석 중...")
        analysis_result = analyze_market_dropdown(ticker_data)
        
        drop_count = analysis_result['drop_count']
        total_markets = analysis_result['total_markets']
        significant_drops = analysis_result['significant_drops']
        
        print(f"✓ 분석 완료: {drop_count}개 하락 (전체 {total_markets}개)")
        
        # 하락 종목 상세 정보 출력
        if significant_drops:
            print(f"\n📉 15% 이상 하락한 종목 ({len(significant_drops)}개):")
            for i, drop in enumerate(significant_drops[:5], 1):  # 상위 5개만
                market = drop['market'].replace('KRW-', '')
                korean_name = drop['korean_name']
                change_rate = drop['change_rate']
                trade_price = drop['trade_price']
                
                print(f"  {i}. {korean_name} ({market})")
                print(f"     현재가: {trade_price:,.0f}원")
                print(f"     하락률: {change_rate:+.1f}%")
        else:
            print("📈 15% 이상 하락한 종목이 없습니다.")
        
        # 4. 알람 조건 확인
        print(f"\n4. 알람 조건 확인:")
        print(f"   하락 종목: {drop_count}개")
        print(f"   임계값: 15개")
        print(f"   알람 발송: {'예' if analysis_result['should_alert'] else '아니오'}")
        
        # 5. 텔레그램 알람 테스트 (조건 충족 시에만)
        if analysis_result['should_alert']:
            print(f"\n5. 텔레그램 알람 테스트 중...")
            success = send_dropdown_alert(analysis_result)
            if success:
                print("✓ 텔레그램 알람 전송 성공")
            else:
                print("❌ 텔레그램 알람 전송 실패")
        else:
            print(f"\n5. 알람 조건 미충족으로 텔레그램 테스트 생략")
        
        print("\n" + "=" * 60)
        print("테스트 완료!")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_upbit_alert()

















