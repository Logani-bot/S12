"""
Real-time Stock Monitoring System

실시간 주식 모니터링 시스템 (08:00-20:00, 10분 간격)
- Summary 탭의 종목만 모니터링
- 현재가 기반 동적 20일선 계산
- 매수선 5% 이내 접근 시 알람
- 상태별 하루 1회 알람 (중복 방지)
"""

import sys
import logging
import requests
import pandas as pd
from datetime import datetime, time
from pathlib import Path
import json
from typing import Dict, List, Tuple, Optional
import argparse

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 상수
SIGNAL_FILE = "trading_signals.xlsx"
ALERT_HISTORY_FILE = "alert_history.json"
MONITORING_START_TIME = time(8, 0)  # 08:00
MONITORING_END_TIME = time(20, 0)   # 20:00
DISTANCE_THRESHOLD = 5.0  # 5% 이내 접근 시 알람

# 키움 API 설정
KIWOOM_BASE_URL = "https://openapi.kiwoom.com/api/oauth2"
KIWOOM_TOKEN = None
APPKEY = None
SECRETKEY = None


def get_access_token(appkey: str, secretkey: str) -> Optional[str]:
    """
    키움 API 접근 토큰 발급
    """
    try:
        url = f"{KIWOOM_BASE_URL}/v1/token"
        
        body = {
            "grant_type": "client_credentials",
            "appkey": appkey,
            "appsecret": secretkey
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        response = requests.post(url, data=body, headers=headers, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        token = result.get("access_token")
        
        if token:
            logger.info("✓ 접근 토큰 발급 성공")
            return token
        else:
            logger.error("✗ 접근 토큰 발급 실패")
            return None
    
    except Exception as e:
        logger.error(f"✗ 토큰 발급 중 오류: {e}")
        return None


def get_current_price(ticker: str, token: str) -> Optional[float]:
    """
    현재가 조회 (호가 API 사용)
    
    Args:
        ticker: 종목 코드
        token: 접근 토큰
    
    Returns:
        현재가 (실패 시 None)
    """
    try:
        url = f"{KIWOOM_BASE_URL}/v1/domestic/ka10024"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        body = {
            "stk_cd": ticker
        }
        
        response = requests.post(url, headers=headers, json=body, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        
        # 현재가 추출 (여러 필드 시도)
        data = result.get("output", {})
        
        # 우선순위: cur_pric > stck_prpr > prpr
        current_price = None
        for key in ["cur_pric", "stck_prpr", "prpr", "price"]:
            if key in data:
                try:
                    current_price = float(data[key])
                    if current_price > 0:
                        break
                except (ValueError, TypeError):
                    continue
        
        if current_price and current_price > 0:
            return current_price
        else:
            logger.warning(f"⚠ {ticker}: 현재가 데이터 없음")
            return None
    
    except Exception as e:
        logger.error(f"✗ {ticker} 현재가 조회 실패: {e}")
        return None


def fetch_chart_data(ticker: str, token: str, days: int = 20) -> Optional[pd.DataFrame]:
    """
    차트 데이터 조회 (과거 N일)
    
    Args:
        ticker: 종목 코드
        token: 접근 토큰
        days: 조회 일수
    
    Returns:
        DataFrame (날짜, 종가) 또는 None
    """
    try:
        url = f"{KIWOOM_BASE_URL}/v1/domestic/ka10081"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # 오늘 날짜
        today = datetime.now().strftime("%Y%m%d")
        
        body = {
            "stk_cd": ticker,
            "base_dt": today,
            "upd_stkpc_tp": "1"
        }
        
        response = requests.post(url, headers=headers, json=body, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        output = result.get("output", [])
        
        if not output:
            logger.warning(f"⚠ {ticker}: 차트 데이터 없음")
            return None
        
        # DataFrame 생성
        records = []
        for item in output:
            date_str = item.get("base_dt") or item.get("stck_bsop_date") or item.get("date")
            
            # 종가 추출 (우선순위: END_PRC > stck_clpr > close > cur_pric)
            close_price = None
            for key in ["END_PRC", "stck_clpr", "close", "cur_pric"]:
                if key in item:
                    try:
                        close_price = float(item[key])
                        if close_price > 0:
                            break
                    except (ValueError, TypeError):
                        continue
            
            if date_str and close_price:
                records.append({
                    "날짜": pd.to_datetime(date_str, format="%Y%m%d"),
                    "종가": close_price
                })
        
        if not records:
            logger.warning(f"⚠ {ticker}: 유효한 차트 데이터 없음")
            return None
        
        df = pd.DataFrame(records)
        
        # 날짜 기준 내림차순 정렬 후 최근 N일 선택
        df = df.sort_values("날짜", ascending=False).head(days)
        
        # 시간순으로 다시 정렬 (MA 계산용)
        df = df.sort_values("날짜", ascending=True).reset_index(drop=True)
        
        return df
    
    except Exception as e:
        logger.error(f"✗ {ticker} 차트 조회 실패: {e}")
        return None


def calculate_tick_unit(price: float) -> int:
    """
    가격대별 호가 단위 계산
    """
    if price < 1000:
        return 1
    elif price < 5000:
        return 5
    elif price < 10000:
        return 10
    elif price < 50000:
        return 50
    elif price < 100000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000


def calculate_dynamic_ma20_and_buy_lines(ticker: str, token: str, current_price: float) -> Optional[Dict]:
    """
    동적 20일선 및 매수선 계산
    
    과거 19일 종가 + 오늘 현재가로 20일선 계산
    
    Args:
        ticker: 종목 코드
        token: 접근 토큰
        current_price: 현재가
    
    Returns:
        {
            "ma20": 20일선,
            "envelope": -20% 엔벨로프,
            "buy1": 1차 매수선,
            "buy2": 2차 매수선,
            "buy3": 3차 매수선,
            "dist_buy1": 1차 매수선 이격도(%),
            "dist_buy2": 2차 매수선 이격도(%),
            "dist_buy3": 3차 매수선 이격도(%)
        }
    """
    # 과거 19일 데이터 조회
    df_chart = fetch_chart_data(ticker, token, days=19)
    
    if df_chart is None or len(df_chart) < 19:
        logger.warning(f"⚠ {ticker}: 과거 데이터 부족 (19일 필요)")
        return None
    
    # 과거 19일 종가 + 오늘 현재가
    past_19_closes = df_chart["종가"].tolist()
    all_20_closes = past_19_closes + [current_price]
    
    # 20일 이동평균
    ma20 = sum(all_20_closes) / 20
    
    # -20% 엔벨로프 지지선
    envelope = ma20 * 0.8
    
    # 1차 매수선: 엔벨로프 + 1틱
    tick = calculate_tick_unit(envelope)
    buy1 = envelope + tick
    
    # 2차 매수선: 1차에서 -10%
    buy2 = buy1 * 0.9
    
    # 3차 매수선: 2차에서 -10%
    buy3 = buy2 * 0.9
    
    # 이격도 계산
    dist_buy1 = ((current_price - buy1) / buy1) * 100
    dist_buy2 = ((current_price - buy2) / buy2) * 100
    dist_buy3 = ((current_price - buy3) / buy3) * 100
    
    return {
        "ma20": ma20,
        "envelope": envelope,
        "buy1": buy1,
        "buy2": buy2,
        "buy3": buy3,
        "dist_buy1": dist_buy1,
        "dist_buy2": dist_buy2,
        "dist_buy3": dist_buy3
    }


def load_summary_stocks() -> pd.DataFrame:
    """
    Summary 탭에서 모니터링 대상 종목 로드
    """
    try:
        if not Path(SIGNAL_FILE).exists():
            logger.warning(f"⚠ {SIGNAL_FILE} 파일이 없습니다.")
            return pd.DataFrame()
        
        df = pd.read_excel(SIGNAL_FILE, sheet_name="Summary")
        
        if df.empty:
            logger.info("ℹ Summary 탭에 종목이 없습니다.")
            return pd.DataFrame()
        
        logger.info(f"✓ Summary 탭에서 {len(df)}개 종목 로드")
        return df
    
    except Exception as e:
        logger.error(f"✗ Summary 탭 로드 실패: {e}")
        return pd.DataFrame()


def load_alert_history() -> Dict:
    """
    알람 히스토리 로드 (오늘자)
    
    Returns:
        {
            "date": "2025-10-14",
            "alerts": {
                "005930": {
                    "READY_BUY1_5%": True,
                    "BOUGHT_1": True
                }
            }
        }
    """
    today = datetime.now().strftime("%Y-%m-%d")
    
    if not Path(ALERT_HISTORY_FILE).exists():
        return {
            "date": today,
            "alerts": {}
        }
    
    try:
        with open(ALERT_HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
        
        # 날짜가 다르면 초기화
        if history.get("date") != today:
            return {
                "date": today,
                "alerts": {}
            }
        
        return history
    
    except Exception as e:
        logger.error(f"✗ 알람 히스토리 로드 실패: {e}")
        return {
            "date": today,
            "alerts": {}
        }


def save_alert_history(history: Dict):
    """
    알람 히스토리 저장
    """
    try:
        with open(ALERT_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    
    except Exception as e:
        logger.error(f"✗ 알람 히스토리 저장 실패: {e}")


def check_and_send_alert(
    ticker: str,
    stock_name: str,
    current_price: float,
    buy_status: str,
    buy_lines: Dict,
    history: Dict
) -> bool:
    """
    알람 조건 체크 및 텔레그램 전송
    
    Args:
        ticker: 종목 코드
        stock_name: 종목명
        current_price: 현재가
        buy_status: 매수 상태 (NONE, BOUGHT_1, BOUGHT_2, BOUGHT_3)
        buy_lines: 매수선 정보
        history: 알람 히스토리
    
    Returns:
        알람 전송 여부
    """
    from telegram_notifier import send_realtime_alert
    
    alerts = history.get("alerts", {})
    ticker_alerts = alerts.get(ticker, {})
    
    # 1차 매수선 5% 이내 (NONE 상태일 때만)
    if buy_status == "NONE":
        dist_buy1 = buy_lines["dist_buy1"]
        
        if 0 < dist_buy1 <= DISTANCE_THRESHOLD:
            alert_key = "READY_BUY1_5%"
            
            if not ticker_alerts.get(alert_key, False):
                # 알람 전송
                send_realtime_alert(
                    alert_type="1차 매수선 5% 인접",
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    target_price=buy_lines["buy1"],
                    distance_pct=dist_buy1,
                    recipients=["me"]
                )
                
                # 히스토리 업데이트
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🔔 {stock_name} ({ticker}): 1차 매수선 5% 인접 알람 전송")
                return True
    
    # 2차 매수선 5% 이내 (BOUGHT_1 상태일 때만)
    elif buy_status == "BOUGHT_1":
        dist_buy2 = buy_lines["dist_buy2"]
        
        if 0 < dist_buy2 <= DISTANCE_THRESHOLD:
            alert_key = "READY_BUY2_5%"
            
            if not ticker_alerts.get(alert_key, False):
                send_realtime_alert(
                    alert_type="2차 매수선 5% 인접",
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    target_price=buy_lines["buy2"],
                    distance_pct=dist_buy2,
                    recipients=["me"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🔔 {stock_name} ({ticker}): 2차 매수선 5% 인접 알람 전송")
                return True
    
    # 3차 매수선 5% 이내 (BOUGHT_2 상태일 때만)
    elif buy_status == "BOUGHT_2":
        dist_buy3 = buy_lines["dist_buy3"]
        
        if 0 < dist_buy3 <= DISTANCE_THRESHOLD:
            alert_key = "READY_BUY3_5%"
            
            if not ticker_alerts.get(alert_key, False):
                send_realtime_alert(
                    alert_type="3차 매수선 5% 인접",
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    target_price=buy_lines["buy3"],
                    distance_pct=dist_buy3,
                    recipients=["me"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🔔 {stock_name} ({ticker}): 3차 매수선 5% 인접 알람 전송")
                return True
    
    return False


def is_monitoring_time() -> bool:
    """
    모니터링 시간대 체크 (08:00-20:00)
    """
    now = datetime.now().time()
    return MONITORING_START_TIME <= now <= MONITORING_END_TIME


def main():
    """
    메인 함수
    """
    global APPKEY, SECRETKEY, KIWOOM_TOKEN
    
    # 인자 파싱
    parser = argparse.ArgumentParser(description="실시간 주식 모니터링")
    parser.add_argument("--appkey", required=True, help="키움 APPKEY")
    parser.add_argument("--secret", required=True, help="키움 SECRETKEY")
    args = parser.parse_args()
    
    APPKEY = args.appkey
    SECRETKEY = args.secret
    
    logger.info("=" * 80)
    logger.info("🔍 실시간 주식 모니터링 시작")
    logger.info("=" * 80)
    
    # 모니터링 시간대 체크
    if not is_monitoring_time():
        logger.info("ℹ 모니터링 시간대가 아닙니다 (08:00-20:00)")
        return
    
    try:
        # 1. 접근 토큰 발급
        KIWOOM_TOKEN = get_access_token(APPKEY, SECRETKEY)
        if not KIWOOM_TOKEN:
            logger.error("✗ 토큰 발급 실패로 종료")
            return
        
        # 2. Summary 종목 로드
        df_summary = load_summary_stocks()
        if df_summary.empty:
            logger.info("ℹ 모니터링 대상 종목이 없습니다.")
            return
        
        # 3. 알람 히스토리 로드
        alert_history = load_alert_history()
        
        # 4. 각 종목 모니터링
        alert_count = 0
        
        for idx, row in df_summary.iterrows():
            ticker = str(row.get("티커", "")).zfill(6)
            stock_name = row.get("종목명", "")
            buy_status = row.get("매수상태", "NONE")
            
            logger.info(f"\n[{idx+1}/{len(df_summary)}] {stock_name} ({ticker}) 모니터링 중...")
            
            # 현재가 조회
            current_price = get_current_price(ticker, KIWOOM_TOKEN)
            if not current_price:
                logger.warning(f"⚠ {stock_name}: 현재가 조회 실패, 스킵")
                continue
            
            logger.info(f"  💰 현재가: {current_price:,.0f}원")
            
            # 동적 매수선 계산
            buy_lines = calculate_dynamic_ma20_and_buy_lines(ticker, KIWOOM_TOKEN, current_price)
            if not buy_lines:
                logger.warning(f"⚠ {stock_name}: 매수선 계산 실패, 스킵")
                continue
            
            logger.info(f"  📊 20일선: {buy_lines['ma20']:,.0f}원")
            logger.info(f"  📉 1차 매수선: {buy_lines['buy1']:,.0f}원 (이격도: {buy_lines['dist_buy1']:.1f}%)")
            logger.info(f"  📉 2차 매수선: {buy_lines['buy2']:,.0f}원 (이격도: {buy_lines['dist_buy2']:.1f}%)")
            logger.info(f"  📉 3차 매수선: {buy_lines['buy3']:,.0f}원 (이격도: {buy_lines['dist_buy3']:.1f}%)")
            
            # 알람 체크 및 전송
            if check_and_send_alert(ticker, stock_name, current_price, buy_status, buy_lines, alert_history):
                alert_count += 1
        
        logger.info("\n" + "=" * 80)
        logger.info("✓ 모니터링 완료")
        logger.info(f"  모니터링 종목: {len(df_summary)}개")
        logger.info(f"  전송 알람: {alert_count}개")
        logger.info("=" * 80)
    
    except Exception as e:
        logger.error(f"✗ 시스템 오류: {e}")
        
        from telegram_notifier import send_error_alert
        send_error_alert(str(e), "Real_Time_Monitor", recipients=["me"])


if __name__ == "__main__":
    main()

