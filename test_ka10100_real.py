"""
키움 API ka10100 테스트 - 실제 응답 확인
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

def test_ka10100():
    """ka10100 API 실제 응답 확인"""
    token = get_token()
    
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
        'authorization': f'Bearer {token}',
        'cont-yn': 'N',
        'next-key': '',
        'api-id': 'ka10100'
    }
    
    data = {
        'stk_cd': '005930'  # 삼성전자
    }
    
    url = 'https://api.kiwoom.com/api/dostk/stkinfo'
    
    print("=== ka10100 API 테스트 ===")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print(f"Data: {data}")
    print()
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=20)
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print()
        
        result = response.json()
        print("Response Body:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        # 응답 구조 분석
        if result:
            print("\n=== 응답 구조 분석 ===")
            for key, value in result.items():
                print(f"{key}: {type(value)}")
                if isinstance(value, dict):
                    print(f"  Keys: {list(value.keys())}")
                elif isinstance(value, list) and len(value) > 0:
                    print(f"  List length: {len(value)}")
                    if isinstance(value[0], dict):
                        print(f"  First item keys: {list(value[0].keys())}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    test_ka10100()
