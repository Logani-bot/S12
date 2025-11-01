"""
업비트 알람 메시지 형식 데모 (간단 버전)
"""

def demo_alert_message():
    """알람 메시지 형식 데모"""
    
    print("=" * 60)
    print("업비트 알람 메시지 형식 데모")
    print("=" * 60)
    
    # 가상의 하락 종목 데이터
    demo_drops = [
        {'market': 'KRW-BTC', 'korean_name': '비트코인', 'change_rate': -18.5},
        {'market': 'KRW-ETH', 'korean_name': '이더리움', 'change_rate': -16.2},
        {'market': 'KRW-ADA', 'korean_name': '에이다', 'change_rate': -15.8},
        {'market': 'KRW-DOT', 'korean_name': '폴카닷', 'change_rate': -15.3},
        {'market': 'KRW-LINK', 'korean_name': '체인링크', 'change_rate': -15.1},
        {'market': 'KRW-MATIC', 'korean_name': '폴리곤', 'change_rate': -14.9},
        {'market': 'KRW-SOL', 'korean_name': '솔라나', 'change_rate': -14.7},
        {'market': 'KRW-AVAX', 'korean_name': '아발란체', 'change_rate': -14.5},
        {'market': 'KRW-LUNA', 'korean_name': '루나', 'change_rate': -14.2},
        {'market': 'KRW-ATOM', 'korean_name': '코스모스', 'change_rate': -14.0},
        {'market': 'KRW-NEAR', 'korean_name': '니어', 'change_rate': -13.8},
        {'market': 'KRW-FTM', 'korean_name': '판텀', 'change_rate': -13.5},
        {'market': 'KRW-ALGO', 'korean_name': '알고랜드', 'change_rate': -13.2},
        {'market': 'KRW-SAND', 'korean_name': '샌드박스', 'change_rate': -13.0},
        {'market': 'KRW-MANA', 'korean_name': '디센트럴랜드', 'change_rate': -12.8},
        {'market': 'KRW-CHZ', 'korean_name': '칠리즈', 'change_rate': -12.5},
        {'market': 'KRW-ENJ', 'korean_name': '엔진코인', 'change_rate': -12.2},
        {'market': 'KRW-BAT', 'korean_name': '베이직어텐션토큰', 'change_rate': -12.0},
        {'market': 'KRW-ZRX', 'korean_name': '제로엑스', 'change_rate': -11.8},
        {'market': 'KRW-COMP', 'korean_name': '컴파운드', 'change_rate': -11.5}
    ]
    
    drop_count = len(demo_drops)
    
    print(f"\n가상 시나리오: {drop_count}개 종목이 15% 이상 하락")
    print("\n" + "=" * 60)
    print("실제 텔레그램으로 전송될 메시지:")
    print("=" * 60)
    
    # 실제 알람 메시지 형식
    message = f"코인 저점 시그널 알림\n"
    message += f"──────────────────\n\n"
    message += f"전일대비 15% 이상 하락: {drop_count}개\n"
    
    if drop_count > 0:
        message += f"하락 종목 : "
        
        # 심볼만 나열 (하락률 순으로 정렬된 상태)
        symbols = []
        for drop in demo_drops:
            symbol = drop['market'].replace('KRW-', '')
            symbols.append(symbol)
        
        # 심볼들을 공백으로 구분하여 나열
        message += " ".join(symbols)
    
    print(message)
    print("=" * 60)
    
    print(f"\n하락 종목 상세 정보 (상위 10개):")
    for i, drop in enumerate(demo_drops[:10], 1):
        market = drop['market'].replace('KRW-', '')
        korean_name = drop['korean_name']
        change_rate = drop['change_rate']
        
        print(f"  {i:2d}. {korean_name} ({market}) - {change_rate:+.1f}%")
    
    if drop_count > 10:
        print(f"  ... 외 {drop_count - 10}개 종목")
    
    print("\n" + "=" * 60)
    print("데모 완료!")
    print("=" * 60)

if __name__ == "__main__":
    demo_alert_message()

















