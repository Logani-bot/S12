"""
키움 API 종목 정보 조회 테스트
시가총액을 직접 가져오는 방법 확인
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

def test_stock_info_api():
    """종목 정보 조회 API 테스트"""
    token = get_token()
    
    # 다양한 엔드포인트와 API ID 조합 시도
    endpoints = [
        ("/api/dostk/chart", "ka10081"),  # 차트 (이미 확인됨)
        ("/api/dostk/stock", "ka10001"),  # 종목 정보
        ("/api/dostk/stock", "ka10002"),  # 종목 상세
        ("/api/dostk/stock", "ka10003"),  # 종목 리스트
        ("/api/dostk/stock", "ka10004"),  # 시가총액
        ("/api/dostk/stock", "ka10005"),  # 종목 기본정보
        ("/api/dostk/info", "ka10001"),   # 정보 조회
        ("/api/dostk/info", "ka10002"),   # 정보 상세
        ("/api/dostk/list", "ka10001"),   # 리스트 조회
        ("/api/dostk/market", "ka10001"), # 시장 정보
    ]
    
    headers = {
        "authorization": f"Bearer {token}",
        "Content-Type": "application/json;charset=UTF-8",
        "cont-yn": "N",
        "next-key": ""
    }
    
    for endpoint, api_id in endpoints:
        print(f"\n=== {endpoint} + {api_id} 테스트 ===")
        headers["api-id"] = api_id
        
        # 다양한 body 형태 시도
        test_bodies = [
            {"stk_cd": "005930"},  # 삼성전자
            {"stk_cd": "005930", "stex_tp": "3"},  # 통합 차트
            {"stk_cd": "005930_AL"},  # _AL 접미사
            {"stk_cd": "005930_AL", "stex_tp": "3"},  # 통합 + _AL
            {},  # 빈 body
            {"stex_tp": "3"},  # 통합만
        ]
        
        success_found = False
        for i, body in enumerate(test_bodies):
            try:
                print(f"  Body {i+1}: {body}")
                response = requests.post(f"https://api.kiwoom.com{endpoint}", headers=headers, json=body, timeout=10)
                print(f"  Status: {response.status_code}")
                
                if response.status_code == 200:
                    result = response.json()
                    print(f"  Success! Keys: {list(result.keys()) if result else 'Empty'}")
                    
                    # 시가총액 관련 필드 찾기
                    if result and result.get("return_code") == 0:
                        for key, value in result.items():
                            if isinstance(value, (list, dict)):
                                if isinstance(value, list) and len(value) > 0:
                                    first_item = value[0]
                                    if isinstance(first_item, dict):
                                        print(f"    {key} 첫 번째 항목 키들: {list(first_item.keys())}")
                                        # 시가총액 관련 키 찾기
                                        market_cap_keys = [k for k in first_item.keys() if any(word in k.lower() for word in ['market', 'cap', '시총', '총액', 'mkt', 'shares', '주식', '발행'])]
                                        if market_cap_keys:
                                            print(f"    시가총액 관련 키: {market_cap_keys}")
                                else:
                                    print(f"    {key}: {type(value)}")
                            else:
                                print(f"    {key}: {value}")
                        success_found = True
                        break  # 성공하면 다음 엔드포인트로
                    else:
                        print(f"  API Error: {result.get('return_msg', 'Unknown error')}")
                else:
                    print(f"  HTTP Error: {response.text[:100]}")
                    
            except Exception as e:
                print(f"  Exception: {e}")
            
            if i < len(test_bodies) - 1:
                print()
        
        if success_found:
            print("  [SUCCESS] 유효한 API 발견!")
        else:
            print("  [FAILED] 유효한 API 없음")

if __name__ == "__main__":
    test_stock_info_api()
