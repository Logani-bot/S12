import os, json, traceback, requests
from datetime import datetime
from pathlib import Path
import yaml

def load_cfg(path: str) -> dict:
    print(f"[DEBUG] loading config: {path}")
    with open(path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise RuntimeError("rest_config.yml 파싱 실패: 내용이 비었거나 문법 오류입니다.")
    for k in ("api", "auth", "paths"):
        if k not in cfg:
            raise RuntimeError(f"rest_config.yml에 '{k}' 섹션이 없습니다.")
    return cfg

def get_token(base: str, token_path: str, appkey: str, secretkey: str, grant_type: str = 'client_credentials'):
    """
    키움 REST 토큰: JSON 바디 + appkey/secretkey
    - 응답 키는 'token' (access_token 아님)
    """
    url = base.rstrip('/') + token_path
    print(f"[DEBUG] token URL: {url}")

    headers_json = {"Content-Type": "application/json"}
    payload_json = {"grant_type": grant_type, "appkey": appkey, "secretkey": secretkey}
    r = requests.post(url, headers=headers_json, json=payload_json, timeout=15)
    print(f"[DEBUG] token(json) status={r.status_code}, preview={r.text[:200]}")
    r.raise_for_status()
    js = r.json()

    # 키움 포맷 대응: 'token' 우선, 그 외 키도 백업 탐색
    token = js.get("token") or js.get("access_token") or js.get("accessToken")
    if not token:
        raise RuntimeError(f"Token missing in response: {js}")
    print("[DEBUG] token acquired")
    return token

def fetch_rank(base: str, ep: str, token: str, params: dict, timeout: int):
    url = base.rstrip('/') + ep
    headers = {'Authorization': f'Bearer {token}'}
    print(f"[DEBUG] rank URL: {url}")
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    print(f"[DEBUG] rank status={r.status_code}, preview={r.text[:200]}")
    r.raise_for_status()
    return r.json()

def main():
    try:
        cfg = load_cfg('rest_config.yml')
        base = cfg['api']['base_url']
        token_url = cfg['api']['token_url']
        ep = cfg['api']['rank_endpoint']
        timeout = int(cfg['api'].get('timeout_sec', 15))

        appkey = cfg['auth']['client_id']       # rest_config.yml에 넣어둔 AppKey
        secretkey = cfg['auth']['client_secret']# rest_config.yml에 넣어둔 SecretKey
        grant_type = cfg['auth'].get('grant_type', 'client_credentials')
        params = cfg.get('params', {})

        token = get_token(base, token_url, appkey, secretkey, grant_type)
        data = fetch_rank(base, ep, token, params, timeout)

        save_dir = Path(cfg['paths']['save_dir'])
        save_dir.mkdir(parents=True, exist_ok=True)
        out = save_dir / f"ka10031_{datetime.now().strftime('%Y-%m-%d')}.json"
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[OK] saved: {out}")
    except Exception:
        print("[ERROR] rest_probe failed:")
        traceback.print_exc()

if __name__ == '__main__':
    main()
