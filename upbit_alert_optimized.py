"""
업비트 시장 하락 감시 시스템 (Google Cloud 최적화)
- 전일대비 15% 이상 하락한 종목이 15개 이상일 때 텔레그램 알람
- 30분 간격으로 모니터링 (API 호출 최적화)
- Google Cloud Functions/Scheduler 최적화
"""

import requests
import json
import time
import logging
from datetime import datetime, time as time_type, timedelta
from typing import List, Dict, Optional
import sys
import io
import os

# 기존 텔레그램 시스템 import
try:
    from telegram_notifier import send_telegram_message, send_error_alert
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    print("⚠️ telegram_notifier 모듈을 찾을 수 없습니다. 텔레그램 알람이 비활성화됩니다.")

# Windows 콘솔 인코딩 설정
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 업비트 API 설정
UPBIT_BASE_URL = "https://api.upbit.com/v1"
MARKET_ALL_URL = f"{UPBIT_BASE_URL}/market/all"
TICKER_URL = f"{UPBIT_BASE_URL}/ticker"

# 모니터링 설정 (최적화)
DROPDOWN_THRESHOLD = -15.0  # 15% 이상 하락
ALERT_COUNT_THRESHOLD = 15  # 15개 이상
MONITORING_INTERVAL = 1800  # 30분 간격 (초) - API 호출 최적화
MONITORING_START_TIME = time_type(9, 0)   # 09:00
MONITORING_END_TIME = time_type(18, 0)    # 18:00

# Google Cloud 환경 감지
IS_GOOGLE_CLOUD = os.getenv('GOOGLE_CLOUD_PROJECT') is not None

# 알람 히스토리 (중복 알람 방지)
alert_history = set()

# API 호출 통계
api_call_stats = {
    'total_calls': 0,
    'successful_calls': 0,
    'failed_calls': 0,
    'last_call_time': None
}


def log_api_call(success: bool):
    """API 호출 통계 로깅"""
    api_call_stats['total_calls'] += 1
    api_call_stats['last_call_time'] = datetime.now()
    
    if success:
        api_call_stats['successful_calls'] += 1
    else:
        api_call_stats['failed_calls'] += 1


def get_krw_markets() -> List[str]:
    """
    KRW 마켓의 모든 코인 목록 조회
    
    Returns:
        List[str]: KRW-XXX 형태의 마켓 코드 리스트
    """
    try:
        response = requests.get(MARKET_ALL_URL, timeout=10)
        response.raise_for_status()
        
        markets = response.json()
        krw_markets = [market['market'] for market in markets if market['market'].startswith('KRW-')]
        
        log_api_call(True)
        logger.info(f"✓ KRW 마켓 {len(krw_markets)}개 조회 완료")
        return krw_markets
        
    except Exception as e:
        log_api_call(False)
        logger.error(f"✗ 마켓 목록 조회 실패: {e}")
        return []


def get_ticker_data_batch(markets: List[str], batch_size: int = 100) -> List[Dict]:
    """
    여러 마켓의 티커 데이터 조회 (배치 처리)
    
    Args:
        markets: 마켓 코드 리스트
        batch_size: 한 번에 조회할 마켓 수 (업비트 API 제한 고려)
        
    Returns:
        List[Dict]: 티커 데이터 리스트
    """
    all_ticker_data = []
    
    try:
        # 배치 단위로 나누어 처리
        for i in range(0, len(markets), batch_size):
            batch_markets = markets[i:i + batch_size]
            markets_str = ','.join(batch_markets)
            url = f"{TICKER_URL}?markets={markets_str}"
            
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            
            batch_data = response.json()
            all_ticker_data.extend(batch_data)
            
            # API 호출 간격 조절 (과도한 요청 방지)
            if i + batch_size < len(markets):
                time.sleep(0.1)  # 100ms 대기
        
        log_api_call(True)
        logger.info(f"✓ 티커 데이터 {len(all_ticker_data)}개 조회 완료 (배치 처리)")
        return all_ticker_data
        
    except Exception as e:
        log_api_call(False)
        logger.error(f"✗ 티커 데이터 조회 실패: {e}")
        return []


def analyze_market_dropdown(ticker_data: List[Dict]) -> Dict:
    """
    시장 하락 분석
    
    Args:
        ticker_data: 티커 데이터 리스트
        
    Returns:
        Dict: 분석 결과
    """
    significant_drops = []
    total_markets = len(ticker_data)
    
    for ticker in ticker_data:
        try:
            market = ticker.get('market', '')
            korean_name = ticker.get('korean_name', '')
            trade_price = ticker.get('trade_price', 0)
            prev_closing_price = ticker.get('prev_closing_price', 0)
            
            if not prev_closing_price or prev_closing_price == 0:
                continue
                
            # 전일대비 변동률 계산
            change_rate = ((trade_price - prev_closing_price) / prev_closing_price) * 100
            
            # 15% 이상 하락한 종목
            if change_rate <= DROPDOWN_THRESHOLD:
                significant_drops.append({
                    'market': market,
                    'korean_name': korean_name,
                    'trade_price': trade_price,
                    'prev_closing_price': prev_closing_price,
                    'change_rate': change_rate
                })
                
        except Exception as e:
            logger.warning(f"티커 데이터 분석 중 오류: {e}")
            continue
    
    # 하락률 순으로 정렬
    significant_drops.sort(key=lambda x: x['change_rate'])
    
    return {
        'total_markets': total_markets,
        'significant_drops': significant_drops,
        'drop_count': len(significant_drops),
        'should_alert': len(significant_drops) >= ALERT_COUNT_THRESHOLD
    }


def send_dropdown_alert(analysis_result: Dict) -> bool:
    """
    하락 알람 전송
    
    Args:
        analysis_result: 분석 결과
        
    Returns:
        bool: 전송 성공 여부
    """
    if not TELEGRAM_AVAILABLE:
        logger.warning("텔레그램이 비활성화되어 있습니다.")
        return False
    
    drop_count = analysis_result['drop_count']
    significant_drops = analysis_result['significant_drops']
    
    # 알람 메시지 생성
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    message = f"코인 저점 시그널 알림\n"
    message += f"──────────────────\n\n"
    message += f"전일대비 15% 이상 하락: {drop_count}개\n"
    
    if drop_count > 0:
        message += f"하락 종목 : "
        
        # 심볼만 나열 (하락률 순으로 정렬된 상태)
        symbols = []
        for drop in significant_drops:
            symbol = drop['market'].replace('KRW-', '')
            symbols.append(symbol)
        
        # 심볼들을 공백으로 구분하여 나열
        message += " ".join(symbols)
    
    # 텔레그램 전송
    try:
        success = send_telegram_message(message, recipients=["me"])
        if success:
            logger.info("✓ 업비트 하락 알람 전송 완료")
        return success
    except Exception as e:
        logger.error(f"✗ 알람 전송 실패: {e}")
        return False


def is_monitoring_time() -> bool:
    """
    모니터링 시간 확인 (09:00-18:00)
    
    Returns:
        bool: 모니터링 시간 여부
    """
    now = datetime.now().time()
    return MONITORING_START_TIME <= now <= MONITORING_END_TIME


def run_monitoring_cycle() -> bool:
    """
    한 번의 모니터링 사이클 실행
    
    Returns:
        bool: 성공 여부
    """
    try:
        logger.info("=" * 60)
        logger.info("업비트 시장 하락 감시 시작 (최적화 버전)")
        logger.info("=" * 60)
        
        # 1. KRW 마켓 목록 조회
        krw_markets = get_krw_markets()
        if not krw_markets:
            logger.error("KRW 마켓 목록을 가져올 수 없습니다.")
            return False
        
        # 2. 티커 데이터 조회 (배치 처리)
        ticker_data = get_ticker_data_batch(krw_markets, batch_size=100)
        if not ticker_data:
            logger.error("티커 데이터를 가져올 수 없습니다.")
            return False
        
        # 3. 하락 분석
        analysis_result = analyze_market_dropdown(ticker_data)
        
        drop_count = analysis_result['drop_count']
        total_markets = analysis_result['total_markets']
        
        logger.info(f"📊 분석 결과: {drop_count}개 하락 (전체 {total_markets}개)")
        
        # 4. 알람 조건 확인
        if analysis_result['should_alert']:
            # 중복 알람 방지 (같은 날짜에는 한 번만)
            today = datetime.now().strftime("%Y-%m-%d")
            if today not in alert_history:
                logger.warning(f"🚨 알람 조건 충족! {drop_count}개 하락 (임계값: {ALERT_COUNT_THRESHOLD}개)")
                send_dropdown_alert(analysis_result)
                alert_history.add(today)
            else:
                logger.info(f"오늘 이미 알람을 보냈습니다. ({drop_count}개 하락)")
        else:
            logger.info(f"알람 조건 미충족 ({drop_count}개 < {ALERT_COUNT_THRESHOLD}개)")
        
        # 5. API 호출 통계 출력
        stats = api_call_stats
        logger.info(f"📈 API 호출 통계: 총 {stats['total_calls']}회 (성공: {stats['successful_calls']}, 실패: {stats['failed_calls']})")
        
        return True
        
    except Exception as e:
        logger.error(f"모니터링 사이클 실행 중 오류: {e}")
        if TELEGRAM_AVAILABLE:
            send_error_alert(str(e), "upbit_alert_optimized")
        return False


def main():
    """
    메인 실행 함수
    """
    logger.info("업비트 시장 하락 감시 시스템 시작 (Google Cloud 최적화)")
    logger.info(f"설정: {DROPDOWN_THRESHOLD}% 이상 하락, {ALERT_COUNT_THRESHOLD}개 이상 시 알람")
    logger.info(f"모니터링 시간: {MONITORING_START_TIME} - {MONITORING_END_TIME}")
    logger.info(f"모니터링 간격: {MONITORING_INTERVAL}초 (30분)")
    logger.info(f"Google Cloud 환경: {'예' if IS_GOOGLE_CLOUD else '아니오'}")
    
    try:
        while True:
            # 모니터링 시간 확인
            if is_monitoring_time():
                run_monitoring_cycle()
            else:
                current_time = datetime.now().strftime("%H:%M:%S")
                logger.info(f"모니터링 시간이 아닙니다. ({current_time})")
            
            # 대기
            logger.info(f"{MONITORING_INTERVAL}초 대기 중...")
            time.sleep(MONITORING_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단되었습니다.")
    except Exception as e:
        logger.error(f"시스템 오류: {e}")
        if TELEGRAM_AVAILABLE:
            send_error_alert(str(e), "upbit_alert_optimized")


# Google Cloud Functions용 엔트리 포인트
def cloud_function_handler(request):
    """
    Google Cloud Functions용 핸들러
    """
    try:
        logger.info("Google Cloud Functions에서 실행 중...")
        return run_monitoring_cycle()
    except Exception as e:
        logger.error(f"Cloud Function 실행 중 오류: {e}")
        return False


if __name__ == "__main__":
    main()




















