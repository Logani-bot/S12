"""
Google Cloud Function 테스트
"""

import requests
import json
import sys

def test_cloud_function():
    """Cloud Function 테스트"""
    
    print("=" * 60)
    print("Google Cloud Function 테스트")
    print("=" * 60)
    
    # 함수 URL 입력
    function_url = input("Cloud Function URL을 입력하세요: ").strip()
    
    if not function_url:
        print("❌ 함수 URL이 필요합니다.")
        return False
    
    if not function_url.startswith("https://"):
        print("❌ 올바른 HTTPS URL을 입력해주세요.")
        return False
    
    print(f"\n테스트 대상: {function_url}")
    print("테스트 실행 중...")
    
    try:
        # HTTP GET 요청
        response = requests.get(function_url, timeout=60)
        
        print(f"\n응답 상태 코드: {response.status_code}")
        
        if response.status_code == 200:
            print("✓ Cloud Function 실행 성공!")
            
            # 응답 내용 확인
            try:
                response_data = response.json()
                print(f"응답 데이터: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
            except:
                print(f"응답 내용: {response.text}")
                
        else:
            print(f"❌ Cloud Function 실행 실패: {response.status_code}")
            print(f"오류 내용: {response.text}")
            
    except requests.exceptions.Timeout:
        print("❌ 요청 시간 초과 (60초)")
    except requests.exceptions.ConnectionError:
        print("❌ 연결 실패 - 함수 URL을 확인해주세요")
    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
    
    print("\n" + "=" * 60)
    print("테스트 완료!")
    print("=" * 60)

if __name__ == "__main__":
    test_cloud_function()

















