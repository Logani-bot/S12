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
from datetime import datetime, time as time_type, timedelta
from pathlib import Path
import json
from typing import Dict, List, Tuple, Optional
import argparse
import time

# 로깅 설정
log_filename = f"realtime_monitor_{datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 상수
SIGNAL_FILE = "trading_signals.xlsx"
ALERT_HISTORY_FILE = "alert_history.json"
MONITORING_START_TIME = time_type(8, 0)  # 08:00
MONITORING_END_TIME = time_type(20, 0)   # 20:00
DISTANCE_THRESHOLD = 5.0  # 5% 이내 접근 시 알람

# 키움 API 설정
KIWOOM_BASE_URL = "https://api.kiwoom.com"
KIWOOM_TOKEN_URL = "https://api.kiwoom.com/oauth2/token"
KIWOOM_TOKEN = None
APPKEY = None
SECRETKEY = None


def get_access_token(appkey: str, secretkey: str) -> Optional[str]:
    """
    키움 API 접근 토큰 발급
    """
    try:
        headers = {"Content-Type": "application/json;charset=UTF-8"}
        body = {
            "grant_type": "client_credentials",
            "appkey": appkey,
            "secretkey": secretkey
        }
        
        response = requests.post(KIWOOM_TOKEN_URL, headers=headers, json=body, timeout=20)
        response.raise_for_status()
        
        result = response.json()
        token = result.get("token") or result.get("access_token")
        
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
    현재가 조회 (차트 API로 최신 데이터 조회)
    
    Args:
        ticker: 종목 코드
        token: 접근 토큰
    
    Returns:
        현재가 (실패 시 None)
    """
    try:
        url = f"{KIWOOM_BASE_URL}/api/dostk/chart"
        
        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "ka10081",
            "cont-yn": "N",
            "next-key": ""
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
        
        # 데이터 추출
        records = result.get("stk_dt_pole_chart_qry")
        
        if not records or len(records) == 0:
            logger.warning(f"⚠ {ticker}: 현재가 데이터 없음")
            return None
        
        # 가장 최근 데이터 (첫 번째 항목)
        latest = records[0]
        
        # 현재가 추출 (첫 번째 키가 cur_pric)
        # Note: 'cur_pric' in latest가 작동하지 않는 버그가 있어 직접 접근 사용
        all_keys = list(latest.keys())
        if len(all_keys) > 0:
            first_key = all_keys[0]  # cur_pric
            try:
                current_price = float(str(latest[first_key]).replace(",", ""))
                if current_price > 0:
                    return current_price
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠ {ticker}: 현재가 파싱 실패 ({e})")
                return None
        
        logger.warning(f"⚠ {ticker}: 현재가 파싱 실패 (키 없음)")
        return None
    
    except Exception as e:
        logger.error(f"✗ {ticker} 현재가 조회 실패: {e}")
        return None


def get_enhanced_price_data(ticker: str, token: str) -> Optional[Dict]:
    """
    확장된 가격 데이터 조회 (현재가, 저가, 고가 포함)
    
    Args:
        ticker: 종목 코드
        token: 접근 토큰
    
    Returns:
        가격 데이터 딕셔너리 또는 None
    """
    try:
        url = f"{KIWOOM_BASE_URL}/api/dostk/chart"
        
        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "ka10081",
            "cont-yn": "N",
            "next-key": ""
        }
        
        # 오늘 날짜
        today = datetime.now().strftime("%Y%m%d")
        
        # KRX+NXT 통합 기준: 종목코드에 _AL 접미사 추가
        integrated_ticker = f"{ticker}_AL"
        
        body = {
            "stk_cd": integrated_ticker,  # 통합 종목코드 사용
            "base_dt": today,
            "upd_stkpc_tp": "1",  # 수정주가
            "stex_tp": "3"  # 통합 (KRX+NXT)
        }
        
        response = requests.post(url, headers=headers, json=body, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        
        # 데이터 추출
        records = result.get("stk_dt_pole_chart_qry")
        
        if not records or len(records) == 0:
            logger.warning(f"⚠ {ticker}: 가격 데이터 없음")
            return None
        
        # 가장 최근 데이터 (첫 번째 항목)
        latest = records[0]
        
        # 모든 키 추출 (cur_pric, stck_lwpr, stck_hgpr, stck_oprc, acml_vol 순서)
        all_keys = list(latest.keys())
        
        data = {}
        if len(all_keys) > 0:
            data['current'] = float(str(latest[all_keys[0]]).replace(",", ""))  # cur_pric
        if len(all_keys) > 1:
            data['low'] = float(str(latest[all_keys[1]]).replace(",", ""))     # stck_lwpr
        if len(all_keys) > 2:
            data['high'] = float(str(latest[all_keys[2]]).replace(",", ""))    # stck_hgpr
        if len(all_keys) > 3:
            data['open'] = float(str(latest[all_keys[3]]).replace(",", ""))    # stck_oprc
        if len(all_keys) > 4:
            data['volume'] = int(str(latest[all_keys[4]]).replace(",", ""))    # acml_vol
        
        # 필수 데이터 확인
        if 'current' not in data or data['current'] <= 0:
            logger.warning(f"⚠ {ticker}: 현재가 데이터 없음")
            return None
        
        return data
    
    except Exception as e:
        logger.error(f"✗ {ticker} 가격 데이터 조회 실패: {e}")
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
        url = f"{KIWOOM_BASE_URL}/api/dostk/chart"
        
        headers = {
            "authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "ka10081",
            "cont-yn": "N",
            "next-key": ""
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
        
        # 데이터 추출
        output = result.get("stk_dt_pole_chart_qry")
        
        if not output or len(output) == 0:
            logger.warning(f"⚠ {ticker}: 차트 데이터 없음")
            return None
        
        # DataFrame 생성
        records = []
        for item in output:
            # 날짜 추출 (첫 번째 키에서 4번째 키가 dt)
            item_keys = list(item.keys())
            date_str = None
            close_price = None
            
            # 날짜는 보통 4번째 키 (dt)
            if len(item_keys) > 3:
                date_str = item[item_keys[3]]  # dt
            
            # 종가는 첫 번째 키 (cur_pric)
            if len(item_keys) > 0:
                try:
                    close_price = float(str(item[item_keys[0]]).replace(",", ""))
                except (ValueError, TypeError):
                    pass
            
            if date_str and close_price and close_price > 0:
                try:
                    records.append({
                        "날짜": pd.to_datetime(date_str, format="%Y%m%d"),
                        "종가": close_price
                    })
                except:
                    pass
        
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
    price_data: Dict,
    buy_status: str,
    buy_lines: Dict,
    history: Dict
) -> bool:
    """
    알람 조건 체크 및 텔레그램 전송
    
    Args:
        ticker: 종목 코드
        stock_name: 종목명
        price_data: 가격 데이터 (current, low, high, open, volume)
        buy_status: 매수 상태 (NONE, BOUGHT_1, BOUGHT_2, BOUGHT_3)
        buy_lines: 매수선 정보
        history: 알람 히스토리
    
    Returns:
        알람 전송 여부
    """
    from telegram_notifier import send_enhanced_alert
    
    current_price = price_data.get('current', 0)
    low_price = price_data.get('low', 0)
    
    alerts = history.get("alerts", {})
    ticker_alerts = alerts.get(ticker, {})
    
    # 1차 매수선 저가 기준 이격도 계산
    if buy_status == "NONE":
        # 저가 기준 이격도 계산
        low_dist_buy1 = calculate_low_price_distance(low_price, buy_lines["buy1"])
        
        # 저가가 매수선에 도달한 경우 (마이너스 이격도)
        if low_dist_buy1 <= 0:
            alert_key = "BUY1_PRICE_REACHED"
            alert_type = "1차 매수가 도달"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy1"],
                    current_distance=buy_lines["dist_buy1"],
                    low_distance=low_dist_buy1,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🚨 {stock_name} ({ticker}): 1차 매수가 도달 알람 전송")
                return True
        
        # 저가가 매수선 5% 이내 접근한 경우
        elif 0 < low_dist_buy1 <= 5.0:
            alert_key = "READY_BUY1_5%"
            alert_type = "1차 매수선 5% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy1"],
                    current_distance=buy_lines["dist_buy1"],
                    low_distance=low_dist_buy1,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🟡 {stock_name} ({ticker}): 1차 매수선 저가 기준 5% 인접 알람 전송")
                return True
        
        # 저가가 매수선 3% 이내 접근한 경우
        elif 0 < low_dist_buy1 <= 3.0:
            alert_key = "READY_BUY1_3%"
            alert_type = "1차 매수선 3% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy1"],
                    current_distance=buy_lines["dist_buy1"],
                    low_distance=low_dist_buy1,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🟠 {stock_name} ({ticker}): 1차 매수선 저가 기준 3% 인접 알람 전송")
                return True
        
        # 저가가 매수선 1% 이내 접근한 경우
        elif 0 < low_dist_buy1 <= 1.0:
            alert_key = "READY_BUY1_1%"
            alert_type = "1차 매수선 1% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy1"],
                    current_distance=buy_lines["dist_buy1"],
                    low_distance=low_dist_buy1,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🔴 {stock_name} ({ticker}): 1차 매수선 저가 기준 1% 인접 알람 전송")
                return True
    
    # 2차 매수선 저가 기준 이격도 계산 (BOUGHT_1 상태일 때만)
    elif buy_status == "BOUGHT_1":
        low_dist_buy2 = calculate_low_price_distance(low_price, buy_lines["buy2"])
        
        # 저가가 매수선에 도달한 경우 (마이너스 이격도)
        if low_dist_buy2 <= 0:
            alert_key = "BUY2_PRICE_REACHED"
            alert_type = "2차 매수가 도달"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy2"],
                    current_distance=buy_lines["dist_buy2"],
                    low_distance=low_dist_buy2,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🚨🚨 {stock_name} ({ticker}): 2차 매수가 도달 알람 전송")
                return True
        
        # 저가가 매수선 5% 이내 접근한 경우
        elif 0 < low_dist_buy2 <= 5.0:
            alert_key = "READY_BUY2_5%"
            alert_type = "2차 매수선 5% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy2"],
                    current_distance=buy_lines["dist_buy2"],
                    low_distance=low_dist_buy2,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🟡 {stock_name} ({ticker}): 2차 매수선 저가 기준 5% 인접 알람 전송")
                return True
        
        # 저가가 매수선 3% 이내 접근한 경우
        elif 0 < low_dist_buy2 <= 3.0:
            alert_key = "READY_BUY2_3%"
            alert_type = "2차 매수선 3% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy2"],
                    current_distance=buy_lines["dist_buy2"],
                    low_distance=low_dist_buy2,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🟠 {stock_name} ({ticker}): 2차 매수선 저가 기준 3% 인접 알람 전송")
                return True
        
        # 저가가 매수선 1% 이내 접근한 경우
        elif 0 < low_dist_buy2 <= 1.0:
            alert_key = "READY_BUY2_1%"
            alert_type = "2차 매수선 1% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy2"],
                    current_distance=buy_lines["dist_buy2"],
                    low_distance=low_dist_buy2,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🔴 {stock_name} ({ticker}): 2차 매수선 저가 기준 1% 인접 알람 전송")
                return True
    
    # 3차 매수선 저가 기준 이격도 계산 (BOUGHT_2 상태일 때만)
    elif buy_status == "BOUGHT_2":
        low_dist_buy3 = calculate_low_price_distance(low_price, buy_lines["buy3"])
        
        # 저가가 매수선에 도달한 경우 (마이너스 이격도)
        if low_dist_buy3 <= 0:
            alert_key = "BUY3_PRICE_REACHED"
            alert_type = "3차 매수가 도달"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy3"],
                    current_distance=buy_lines["dist_buy3"],
                    low_distance=low_dist_buy3,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🚨🚨🚨 {stock_name} ({ticker}): 3차 매수가 도달 알람 전송")
                return True
        
        # 저가가 매수선 5% 이내 접근한 경우
        elif 0 < low_dist_buy3 <= 5.0:
            alert_key = "READY_BUY3_5%"
            alert_type = "3차 매수선 5% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy3"],
                    current_distance=buy_lines["dist_buy3"],
                    low_distance=low_dist_buy3,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🟡 {stock_name} ({ticker}): 3차 매수선 저가 기준 5% 인접 알람 전송")
                return True
        
        # 저가가 매수선 3% 이내 접근한 경우
        elif 0 < low_dist_buy3 <= 3.0:
            alert_key = "READY_BUY3_3%"
            alert_type = "3차 매수선 3% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy3"],
                    current_distance=buy_lines["dist_buy3"],
                    low_distance=low_dist_buy3,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🟠 {stock_name} ({ticker}): 3차 매수선 저가 기준 3% 인접 알람 전송")
                return True
        
        # 저가가 매수선 1% 이내 접근한 경우
        elif 0 < low_dist_buy3 <= 1.0:
            alert_key = "READY_BUY3_1%"
            alert_type = "3차 매수선 1% 인접"
            
            if not ticker_alerts.get(alert_key, False):
                send_enhanced_alert(
                    alert_type=alert_type,
                    stock_name=stock_name,
                    ticker=ticker,
                    current_price=current_price,
                    low_price=low_price,
                    target_price=buy_lines["buy3"],
                    current_distance=buy_lines["dist_buy3"],
                    low_distance=low_dist_buy3,
                    recipients=["all"]
                )
                
                ticker_alerts[alert_key] = True
                alerts[ticker] = ticker_alerts
                history["alerts"] = alerts
                save_alert_history(history)
                
                logger.info(f"🔴 {stock_name} ({ticker}): 3차 매수선 저가 기준 1% 인접 알람 전송")
                return True
    
    return False


def is_monitoring_time() -> bool:
    """
    모니터링 시간대 체크 (08:00-20:00)
    """
    now = datetime.now().time()
    return MONITORING_START_TIME <= now <= MONITORING_END_TIME


def run_dynamic_monitoring_cycle(next_check_times: dict):
    """
    동적 간격 모니터링 사이클 실행
    """
    global KIWOOM_TOKEN
    
    try:
        # 1. 접근 토큰 발급 (또는 재사용)
        if not KIWOOM_TOKEN:
            KIWOOM_TOKEN = get_access_token(APPKEY, SECRETKEY)
            if not KIWOOM_TOKEN:
                logger.error("✗ 토큰 발급 실패")
                return False
        
        # 2. Summary 종목 로드
        df_summary = load_summary_stocks()
        if df_summary.empty:
            logger.info("ℹ 모니터링 대상 종목이 없습니다.")
            return True
        
        # 3. 알람 히스토리 로드
        alert_history = load_alert_history()
        
        # 4. 동적 간격으로 종목별 모니터링
        current_time = datetime.now()
        alert_count = 0
        checked_count = 0
        
        for idx, row in df_summary.iterrows():
            ticker = str(row.get("티커", "")).zfill(6)
            stock_name = row.get("종목명", "")
            buy_status = row.get("매수상태", "NONE")
            
            # 종목별 다음 체크 시간 확인
            next_check_time = next_check_times.get(ticker)
            if next_check_time and current_time < next_check_time:
                continue  # 아직 체크할 시간이 안 됨
            
            checked_count += 1
            logger.info(f"\n[{checked_count}] {stock_name} ({ticker}) 모니터링 중...")
            
            # API 호출 제한 방지 (0.5초 대기)
            if checked_count > 1:
                time.sleep(0.5)
            
            # 확장된 가격 데이터 조회
            price_data = get_enhanced_price_data(ticker, KIWOOM_TOKEN)
            if not price_data:
                logger.warning(f"⚠ {stock_name}: 가격 데이터 조회 실패, 스킵")
                # 실패한 종목은 10분 후 재시도
                next_check_times[ticker] = current_time + timedelta(minutes=10)
                continue
            
            current_price = price_data.get('current', 0)
            low_price = price_data.get('low', 0)
            high_price = price_data.get('high', 0)
            
            logger.info(f"  💰 현재가: {current_price:,.0f}원")
            logger.info(f"  📉 저가: {low_price:,.0f}원")
            logger.info(f"  📈 고가: {high_price:,.0f}원")
            
            # 동적 매수선 계산
            buy_lines = calculate_dynamic_ma20_and_buy_lines(ticker, KIWOOM_TOKEN, current_price)
            if not buy_lines:
                logger.warning(f"⚠ {stock_name}: 매수선 계산 실패, 스킵")
                # 실패한 종목은 10분 후 재시도
                next_check_times[ticker] = current_time + timedelta(minutes=10)
                continue
            
            logger.info(f"  📊 20일선: {buy_lines['ma20']:,.0f}원")
            logger.info(f"  📉 1차 매수선: {buy_lines['buy1']:,.0f}원 (이격도: {buy_lines['dist_buy1']:.1f}%)")
            logger.info(f"  📉 2차 매수선: {buy_lines['buy2']:,.0f}원 (이격도: {buy_lines['dist_buy2']:.1f}%)")
            logger.info(f"  📉 3차 매수선: {buy_lines['buy3']:,.0f}원 (이격도: {buy_lines['dist_buy3']:.1f}%)")
            
            # 동적 간격 계산
            interval = calculate_monitoring_interval(current_price, buy_lines['envelope'])
            next_check_times[ticker] = current_time + timedelta(seconds=interval)
            
            logger.info(f"  ⏰ 다음 모니터링 간격: {interval}초 ({interval//60}분)")
            
            # 저가 기준 인접 알림 (5%, 3%, 1%)
            if check_and_send_alert(ticker, stock_name, price_data, buy_status, buy_lines, alert_history):
                alert_count += 1
        
        logger.info("\n" + "=" * 80)
        logger.info("✓ 동적 모니터링 사이클 완료")
        logger.info(f"  전체 종목: {len(df_summary)}개")
        logger.info(f"  체크한 종목: {checked_count}개")
        logger.info(f"  전송 알람: {alert_count}개")
        logger.info("=" * 80)
        
        return True
    
    except Exception as e:
        logger.error(f"✗ 시스템 오류: {e}")
        
        try:
            from telegram_notifier import send_error_alert
            send_error_alert(str(e), "Real_Time_Monitor", recipients=["me"])  # 에러는 본인만
        except:
            pass
        
        return False


def run_monitoring_cycle():
    """
    1회 모니터링 사이클 실행 (기존 방식 - 호환성 유지)
    """
    # 기존 방식으로 동작하도록 임시 구현
    next_check_times = {}
    return run_dynamic_monitoring_cycle(next_check_times)


def main():
    """
    메인 함수 - 동적 간격 실시간 모니터링
    """
    global APPKEY, SECRETKEY, KIWOOM_TOKEN
    
    # 인자 파싱
    parser = argparse.ArgumentParser(description="실시간 주식 모니터링 (동적 간격)")
    parser.add_argument("--appkey", required=True, help="키움 APPKEY")
    parser.add_argument("--secret", required=True, help="키움 SECRETKEY")
    parser.add_argument("--interval", type=int, default=60, help="기본 모니터링 간격 (초, 기본값: 60)")
    args = parser.parse_args()
    
    APPKEY = args.appkey
    SECRETKEY = args.secret
    base_interval = args.interval
    
    logger.info("=" * 80)
    logger.info("🔍 실시간 주식 모니터링 시작 (개선된 버전)")
    logger.info(f"⏰ 기본 모니터링 간격: {base_interval}초 ({base_interval//60}분)")
    logger.info("📊 동적 간격: 1% 이내(1분), 3% 이내(3분), 10% 이내(10분)")
    logger.info("🎯 저가 기준 터치 감지 활성화")
    logger.info("🕐 모니터링 시간: 08:00-20:00")
    logger.info("=" * 80)
    
    # 종목별 다음 체크 시간 관리
    next_check_times = {}  # {ticker: timestamp}
    
    cycle_count = 0
    
    while True:
        cycle_count += 1
        current_time = datetime.now()
        
        # 모니터링 시간대 체크
        if not is_monitoring_time():
            logger.info(f"\n[사이클 {cycle_count}] 모니터링 시간대가 아닙니다 (08:00-20:00)")
            logger.info(f"⏰ {base_interval}초 후 재확인...")
            time.sleep(base_interval)
            continue
        
        logger.info(f"\n{'=' * 80}")
        logger.info(f"[사이클 {cycle_count}] {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'=' * 80}")
        
        # 모니터링 실행 (동적 간격 적용)
        success = run_dynamic_monitoring_cycle(next_check_times)
        
        if not success:
            logger.warning("⚠ 모니터링 실패, 재시도...")
        
        # 다음 실행까지 대기 (기본 간격)
        logger.info(f"\n⏰ {base_interval}초 후 다음 사이클 실행...")
        logger.info(f"   종료하려면 Ctrl+C를 누르세요.")
        
        try:
            time.sleep(base_interval)
        except KeyboardInterrupt:
            logger.info("\n" + "=" * 80)
            logger.info("🛑 사용자가 모니터링을 중지했습니다.")
            logger.info("=" * 80)
            break


if __name__ == "__main__":
    main()

