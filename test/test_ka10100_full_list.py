"""
제공받은 ka10100 API 테스트
전체 종목 리스트를 가져올 수 있는지 확인
"""
import requests
import json

# 환경 변수 설정
APPKEY = "IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU"
SECRETKEY = "eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs"

def get_token():
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APPKEY,
        "secretkey": SECRETKEY
    }
    
    response = requests.post("https://api.kiwoom.com/oauth2/token", headers=headers, json=body, timeout=20)
    response.raise_for_status()
    data = response.json()
    return data.get("token") or data.get("access_token")

def test_ka10100_stock_list():
    """ka10100 API로 전체 종목 리스트 조회 테스트"""
    token = get_token()
    
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': 'N',
        'next-key': '',
        'api-id': 'ka10100',
    }
    
    # 전체 종목 조회를 위한 다양한 파라미터 시도
    test_cases = [
        {},  # 빈 파라미터
        {"stk_cd": ""},  # 빈 종목코드
        {"stex_tp": "3"},  # 통합 시장
        {"stex_tp": "1"},  # KRX
        {"stex_tp": "2"},  # NXT
        {"market": "ALL"},  # 전체 시장
        {"list_type": "ALL"},  # 전체 리스트
    ]
    
    url = 'https://api.kiwoom.com/api/dostk/stkinfo'
    
    for i, params in enumerate(test_cases):
        print(f"\n=== 테스트 {i+1}: {params} ===")
        
        try:
            response = requests.post(url, headers=headers, json=params, timeout=20)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"Return Code: {result.get('return_code')}")
                print(f"Return Msg: {result.get('return_msg')}")
                
                if result.get("return_code") == 0:
                    print("SUCCESS! 응답 구조:")
                    for key, value in result.items():
                        if key not in ["return_code", "return_msg"]:
                            print(f"  {key}: {type(value)}")
                            if isinstance(value, list):
                                print(f"    List length: {len(value)}")
                                if len(value) > 0 and isinstance(value[0], dict):
                                    print(f"    First item keys: {list(value[0].keys())}")
                                    print(f"    First item sample: {value[0]}")
                            elif isinstance(value, dict):
                                print(f"    Dict keys: {list(value.keys())}")
                    
                    # 종목 리스트가 있다면 샘플 출력
                    if 'list' in result and isinstance(result['list'], list):
                        print(f"\n총 {len(result['list'])}개 종목 발견!")
                        print("처음 5개 종목:")
                        for stock in result['list'][:5]:
                            print(f"  {stock}")
                    
                    return True
                else:
                    print(f"API Error: {result.get('return_msg')}")
            else:
                print(f"HTTP Error: {response.text[:200]}")
                
        except Exception as e:
            print(f"Exception: {e}")
    
    print("\nka10100 API로 전체 종목 리스트를 가져올 수 없습니다.")
    print("다른 방법을 시도해야 합니다.")
    return False

if __name__ == '__main__':
    test_ka10100_stock_list()

