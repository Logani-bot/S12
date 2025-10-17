"""
키움 API 전체 종목 리스트 조회 테스트
다양한 API 엔드포인트와 ID 조합 시도
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

def test_stock_list_apis():
    """전체 종목 리스트 조회 API 테스트"""
    token = get_token()
    
    # 다양한 API 조합 시도
    test_cases = [
        # (endpoint, api_id, body)
        ("/api/dostk/stklist", "ka10001", {"stex_tp": "3"}),  # 통합 종목 리스트
        ("/api/dostk/stklist", "ka10001", {"stex_tp": "1"}),  # KRX 종목 리스트
        ("/api/dostk/stklist", "ka10001", {"stex_tp": "2"}),  # NXT 종목 리스트
        ("/api/dostk/stklist", "ka10001", {}),  # 빈 body
        ("/api/dostk/stklist", "ka10002", {"stex_tp": "3"}),  # 다른 API ID
        ("/api/dostk/stklist", "ka10003", {"stex_tp": "3"}),  # 또 다른 API ID
        ("/api/dostk/stock", "ka10001", {"stex_tp": "3"}),  # 다른 엔드포인트
        ("/api/dostk/list", "ka10001", {"stex_tp": "3"}),   # 또 다른 엔드포인트
        ("/api/dostk/market", "ka10001", {"stex_tp": "3"}), # 시장 정보
        ("/api/dostk/stklist", "ka10001", {"market": "ALL"}),  # 시장 전체
        ("/api/dostk/stklist", "ka10001", {"list_type": "ALL"}),  # 리스트 전체
        ("/api/dostk/stklist", "ka10001", {"page_no": "1", "page_cnt": "1000"}),  # 페이징
    ]
    
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': 'N',
        'next-key': '',
    }
    
    for endpoint, api_id, body in test_cases:
        print(f"\n=== {endpoint} + {api_id} 테스트 ===")
        print(f"Body: {body}")
        
        headers["api-id"] = api_id
        url = f'https://api.kiwoom.com{endpoint}'
        
        try:
            response = requests.post(url, headers=headers, json=body, timeout=20)
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
                            if isinstance(value, list) and len(value) > 0:
                                print(f"    List length: {len(value)}")
                                if isinstance(value[0], dict):
                                    print(f"    First item keys: {list(value[0].keys())}")
                                    # 첫 번째 항목 샘플 출력
                                    print(f"    First item sample: {value[0]}")
                            elif isinstance(value, dict):
                                print(f"    Dict keys: {list(value.keys())}")
                    print("✅ 유효한 API 발견!")
                    return endpoint, api_id, body
                else:
                    print(f"❌ API Error: {result.get('return_msg')}")
            else:
                print(f"❌ HTTP Error: {response.text[:100]}")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
    
    print("\n❌ 유효한 전체 종목 리스트 API를 찾지 못했습니다.")
    return None, None, None

if __name__ == '__main__':
    test_stock_list_apis()

