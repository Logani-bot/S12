# S12 | Rank API 최소 패치 (api_id 본문 + 500 재시도 + 캐시 폴백)
# - 요청 방식: POST /api/dostk/rkinfo  (본문에 api_id 사용)
# - 500 오류 시 재시도: 0.5s -> 1.0s -> 2.0s (최대 3회)
# - 실패 시 캐시 폴백: /Users/lll/Documents/Macoding/S12/rest/*.json 중 최신 파일 사용
# - 출력: 성공 응답을 YYYY-MM-DD 기반 파일명으로 저장, 캐시 사용 시 로그 표기
#
# 사용법 (venv 활성화 상태):
#   python s12_rank_patch.py --token "<BEARER_TOKEN>" \
#       --base-url "https://api.kiwoom.com" --market ALL --count 100 \
#       --out-dir "/Users/lll/Documents/Macoding/S12/rest"
#
# 필요 패키지: requests

import argparse
import datetime as dt
import glob
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests


def _ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fetch_rank(
    base_url: str,
    token: str,
    market: str = "ALL",
    count: int = 100,
    api_id: str = "ka10031",
    retry_intervals = (0.5, 1.0, 2.0),
) -> Tuple[Optional[Dict[str, Any]], Optional[requests.Response], str]:
    """POST /api/dostk/rkinfo 로 랭크 데이터 요청.

    반환: (json_dict | None, last_response | None, status)
        - status: 'live' (성공) | 'retry' (재시도 후 성공) | 'fail' (완전 실패)
    """
    url = f"{base_url.rstrip('/')}/api/dostk/rkinfo"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "api_id": api_id,  # 핵심: tr_cd -> api_id 로 전환
        "market": market,
        "count": int(count),
    }

    last_resp: Optional[requests.Response] = None

    # 최초 시도
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        last_resp = resp
        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception:
                data = None
            # 정상 응답으로 간주
            if data is not None:
                return data, resp, 'live'
        elif 500 <= resp.status_code < 600:
            # 아래에서 재시도
            pass
        else:
            # 비 2xx & 비 5xx 은 재시도 이점 낮음 -> 바로 실패 리턴
            return None, resp, 'fail'
    except Exception:
        # 네트워크 예외 -> 재시도 루프로 진입
        pass

    # 5xx 또는 예외면 재시도 (지수 백오프)
    for i, sec in enumerate(retry_intervals):
        time.sleep(sec)
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            last_resp = resp
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = None
                if data is not None:
                    return data, resp, 'retry'
            elif 500 <= resp.status_code < 600:
                # 다음 루프로
                continue
            else:
                return None, resp, 'fail'
        except Exception:
            continue

    return None, last_resp, 'fail'


def load_latest_cache(out_dir: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """out_dir 내 JSON 파일 중 가장 최근 파일을 로드.
    반환: (json_dict | None, 파일경로 | None)
    """
    pattern = os.path.join(out_dir, "*.json")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data, path
        except Exception:
            continue
    return None, None


def save_json(data: Dict[str, Any], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="S12 Rank API 최소 패치")
    parser.add_argument("--token", required=True, help="Bearer Access Token")
    parser.add_argument("--base-url", default="https://api.kiwoom.com")
    parser.add_argument("--market", default="ALL")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--api-id", default="ka10031")
    parser.add_argument("--out-dir", default="/Users/lll/Documents/Macoding/S12/rest")
    args = parser.parse_args()

    today = dt.datetime.now().strftime("%Y-%m-%d")
    out_file = os.path.join(args.out_dir, f"ka10031_{today}.json")

    print(f"[{_ts()}] [INFO] Fetch rank -> POST /api/dostk/rkinfo (api_id in body)")
    data, resp, status = fetch_rank(
        base_url=args.base_url,
        token=args.token,
        market=args.market,
        count=args.count,
        api_id=args.api_id,
    )

    if data is not None:
        save_json(data, out_file)
        src = "live" if status == 'live' else 'retry'
        print(f"[{_ts()}] [OK] {src} response saved: {out_file}")
        return

    # 완전 실패 -> 캐시 폴백
    print(f"[{_ts()}] [ERROR] Rank API failed. status={resp.status_code if resp else 'NO_RESP'}")
    cache, cache_path = load_latest_cache(args.out_dir)
    if cache is not None:
        # 캐시를 오늘 파일명으로도 저장하여 다운스트림 파이프라인을 유지
        save_json(cache, out_file)
        print(f"[{_ts()}] [WARN] Using cache: {cache_path} -> saved as {out_file}")
    else:
        print(f"[{_ts()}] [FATAL] No cache available in: {args.out_dir}")
        exit(2)


if __name__ == "__main__":
    main()


# ==============================================
# rest_probe.py 통합 패치 (diff)
# ==============================================
# 목적: 기존 rkinfo 호출부를 s12_rank_patch의 fetch_rank()로 대체
# 전제: 같은 디렉토리에 s12_rank_patch.py 존재 (이미 생성 완료)
# 적용 난이도: 매우 낮음 (import 1줄 + 호출부 교체 10~20줄)
#
# ⚠️ 사용자의 요청 기준(모델 컨텍스트 #23):
#  - "수정 전 코드를 명시하고 그 자리를 삭제한 뒤 수정 후 코드를 그대로 붙여넣을 수 있도록" 제공
#  → 아래 A/B 블록을 그대로 사용하세요.


## [PATCH A] 상단 import 정리

### (삭제)
# (아래와 유사한 기존 import들 중 rkinfo 전용 유틸이 없다면 그대로 두고, 별도 삭제할 것은 없음)
# import requests
# import time

### (신규 추가)
from s12_rank_patch import fetch_rank


## [PATCH B] rkinfo 호출부 교체
# 아래 "기존 rkinfo 호출부"와 유사한 블록을 찾아 전체 삭제하고, 바로 아래의 "신규 rkinfo 호출부"로 교체하십시오.
# 힌트: 기존 코드 내에서 "rkinfo" 문자열을 검색하면 해당 구간을 쉽게 찾을 수 있습니다.

### (삭제: 기존 rkinfo 호출부 예시)
"""
# 예시) 기존에 GET/POST를 혼용하며 호출하던 블록 (실제 코드와 다를 수 있음)
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}
# 1) GET 시도
url_get = f"{base_url}/api/dostk/rkinfo/ka10031"
resp = requests.get(url_get, headers=headers, timeout=15)
if resp.status_code != 200:
    # 2) POST 시도 (tr_cd 사용)
    url_post = f"{base_url}/api/dostk/rkinfo"
    payload = {"tr_cd": "ka10031", "market": market, "count": count}
    resp = requests.post(url_post, headers=headers, json=payload, timeout=15)

if resp.status_code == 200:
    data = resp.json()
    save_json(data, out_file)
else:
    # 실패 처리 ...
"""

### (신규: rkinfo 호출부 대체 — fetch_rank 사용)
# 필요한 변수 (기존에 이미 존재하면 그대로 사용하세요):
#   base_url: str, access_token: str, market: str, count: int, out_dir: str
#   out_file: str (오늘자 저장 경로) — 기존 로직 유지 권장

# 1) 랭크 데이터 요청 (api_id 본문 + 500 재시도 내장)
rank_json, rank_resp, rank_status = fetch_rank(
    base_url=base_url,
    token=access_token,
    market=market,
    count=count,
    api_id="ka10031",
)

# 2) 저장/폴백 처리: 기존 save_json()·캐시 로더가 이미 있다면 그대로 재사용하세요.
if rank_json is not None:
    save_json(rank_json, out_file)
    print(f"[OK] {'live' if rank_status=='live' else 'retry'} response saved: {out_file}")
else:
    # 완전 실패 시, 기존 캐시 폴백 함수를 사용하세요 (예: load_latest_cache)
    cache, cache_path = load_latest_cache(out_dir)
    if cache is not None:
        save_json(cache, out_file)
        print(f"[WARN] Using cache: {cache_path} -> saved as {out_file}")
    else:
        print(f"[FATAL] No cache available in: {out_dir}")
        raise SystemExit(2)


## [참고] 변수 연결 가이드
# - base_url      : 기존에 토큰 요청에 사용하신 값 (예: "https://api.kiwoom.com")
# - access_token  : 기존 토큰 취득부에서 받은 Bearer 액세스 토큰 문자열
# - market        : 문자열 (예: "ALL"/"KOSPI"/"KOSDAQ")
# - count         : 정수 (예: 100)
# - out_dir       : "/Users/lll/Documents/Macoding/S12/rest"
# - out_file      : f"{out_dir}/ka10031_{today}.json"  (기존과 동일 포맷 유지 권장)


## [체크리스트]
# [ ] 코드 상단에 "from s12_rank_patch import fetch_rank" 추가했는가?
# [ ] 기존 rkinfo 호출부 전체를 삭제했는가?
# [ ] 신규 호출부로 교체하고, 변수명(base_url, access_token, market, count, out_dir, out_file) 연결을 맞췄는가?
# [ ] 실행 후 "[OK] live/retry response saved" 로그가 출력되는가?
# [ ] 실패 시 "[WARN] Using cache:" 또는 "[FATAL] No cache available" 로그가 정상 출력되는가?


# ==============================================
# rest_probe.py (단일 파일 버전, s12_rank_patch 통합)
# ==============================================
# - 별도 파일 없이 이 파일 하나로 동작합니다.
# - 기능: api_id 본문 방식 POST 호출 + 500 재시도 + 캐시 폴백 + 저장
# - 사용법:
#     python rest_probe.py --token "<BEARER_TOKEN>" \
#         --base-url "https://api.kiwoom.com" --market ALL --count 100 \
#         --out-dir "/Users/lll/Documents/Macoding/S12/rest"

import argparse
import datetime as dt
import glob
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests


def _ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_json(data: Dict[str, Any], out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_latest_cache(out_dir: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    pattern = os.path.join(out_dir, "*.json")
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data, path
        except Exception:
            continue
    return None, None


def fetch_rank(
    base_url: str,
    token: str,
    market: str = "ALL",
    count: int = 100,
    api_id: str = "ka10031",
    retry_intervals = (0.5, 1.0, 2.0),
) -> Tuple[Optional[Dict[str, Any]], Optional[requests.Response], str]:
    """POST /api/dostk/rkinfo 로 랭크 데이터 요청.

    반환: (json_dict | None, last_response | None, status)
        - status: 'live' | 'retry' | 'fail'
    """
    url = f"{base_url.rstrip('/')}/api/dostk/rkinfo"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {
        "api_id": api_id,  # 핵심: REST는 tr_cd 대신 api_id 사용
        "market": market,
        "count": int(count),
    }

    last_resp: Optional[requests.Response] = None

    # 최초 시도
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        last_resp = resp
        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception:
                data = None
            if data is not None:
                return data, resp, 'live'
        elif 500 <= resp.status_code < 600:
            pass  # 재시도 진입
        else:
            return None, resp, 'fail'
    except Exception:
        pass

    # 재시도 (지수 백오프)
    for sec in retry_intervals:
        time.sleep(sec)
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            last_resp = resp
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = None
                if data is not None:
                    return data, resp, 'retry'
            elif 500 <= resp.status_code < 600:
                continue
            else:
                return None, resp, 'fail'
        except Exception:
            continue

    return None, last_resp, 'fail'


def main():
    parser = argparse.ArgumentParser(description="S12 Rank API (단일 파일)")
    parser.add_argument("--token", required=True, help="Bearer Access Token")
    parser.add_argument("--base-url", default="https://api.kiwoom.com")
    parser.add_argument("--market", default="ALL")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--api-id", default="ka10031")
    parser.add_argument("--out-dir", default="/Users/lll/Documents/Macoding/S12/rest")
    args = parser.parse_args()

    today = dt.datetime.now().strftime("%Y-%m-%d")
    out_file = os.path.join(args.out_dir, f"{args.api_id}_{today}.json")

    print(f"[{_ts()}] [INFO] Fetch rank -> POST /api/dostk/rkinfo (api_id in body)")
    data, resp, status = fetch_rank(
        base_url=args.base_url,
        token=args.token,
        market=args.market,
        count=args.count,
        api_id=args.api_id,
    )

    if data is not None:
        save_json(data, out_file)
        src = "live" if status == 'live' else 'retry'
        print(f"[{_ts()}] [OK] {src} response saved: {out_file}")
        return

    # 실패 -> 캐시 폴백
    scode = resp.status_code if resp else 'NO_RESP'
    print(f"[{_ts()}] [ERROR] Rank API failed. status={scode}")
    cache, cache_path = load_latest_cache(args.out_dir)
    if cache is not None:
        save_json(cache, out_file)
        print(f"[{_ts()}] [WARN] Using cache: {cache_path} -> saved as {out_file}")
    else:
        print(f"[{_ts()}] [FATAL] No cache available in: {args.out_dir}")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
