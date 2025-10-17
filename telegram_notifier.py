"""
텔레그램 알람 전송 모듈
"""
import os
import requests
import logging
from typing import List, Optional
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 텔레그램 설정
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

# Chat IDs
CHAT_IDS = {
    "me": os.getenv("TELEGRAM_CHAT_ID_ME"),
    "yoonjoo": os.getenv("TELEGRAM_CHAT_ID_YOONJOO"),
    "minjeong": os.getenv("TELEGRAM_CHAT_ID_MINJEONG"),
    "jumeoni": os.getenv("TELEGRAM_CHAT_ID_JUMEONI")
}


def send_telegram_message(message: str, recipients: List[str] = None, parse_mode: str = "HTML") -> bool:
    """
    텔레그램 메시지 전송
    
    Args:
        message: 전송할 메시지
        recipients: 수신자 리스트 (기본값: ["me"] - 본인만)
                   예: ["me", "yoonjoo"] 또는 ["all"]
        parse_mode: 메시지 포맷 ("Markdown" 또는 "HTML")
    
    Returns:
        bool: 전송 성공 여부
    """
    if not TELEGRAM_TOKEN:
        logger.error("텔레그램 토큰이 설정되지 않았습니다.")
        return False
    
    # 기본값: 본인만
    if recipients is None:
        recipients = ["me"]
    
    # "all" 이면 모든 사람에게
    if "all" in recipients:
        recipients = list(CHAT_IDS.keys())
    
    success = True
    for recipient in recipients:
        chat_id = CHAT_IDS.get(recipient)
        if not chat_id:
            logger.warning(f"알 수 없는 수신자: {recipient}")
            continue
        
        try:
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": parse_mode
            }
            
            response = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"✓ 텔레그램 전송 성공: {recipient}")
        
        except Exception as e:
            logger.error(f"✗ 텔레그램 전송 실패 ({recipient}): {e}")
            success = False
    
    return success


def send_daily_report(alerts: List[dict], total_stocks: int, recipients: List[str] = None):
    """
    일일 리포트 전송 (20:10 실행 시)
    
    Args:
        alerts: 알람 대상 종목 리스트
        total_stocks: 총 종목 수
        recipients: 수신자 리스트
    """
    from datetime import datetime
    
    # 헤더
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    message = f"📊 <b>일일 트레이딩 리포트</b>\n"
    message += f"🕐 {now}\n"
    message += f"───────────────\n\n"
    
    if not alerts:
        message += f"✅ 총 {total_stocks}개 종목 분석\n"
        message += f"🔕 알람 대상 없음\n"
        send_telegram_message(message, recipients)
        return
    
    # 상태별 그룹화
    ready_buy1 = []
    bought_stocks = []
    ready_sell = []
    
    for alert in alerts:
        status = alert.get("알람상태", "")
        if "READY_BUY1" in status:
            ready_buy1.append(alert)
        elif "BOUGHT" in alert.get("매수상태", ""):
            bought_stocks.append(alert)
        elif "READY_SELL" in status:
            ready_sell.append(alert)
    
    # 1차 매수 접근 중 (10% 이내) - 이격도 낮은 순으로 정렬
    if ready_buy1:
        message += f"🟡 <b>1차 매수 접근 중</b> ({len(ready_buy1)}개)\n"
        
        # 이격도 낮은 순으로 정렬
        ready_buy1.sort(key=lambda x: x.get("1차매수선이격도(%)", 999))
        
        for stock in ready_buy1:
            name = stock.get("종목명", "")
            close = stock.get("종가", 0)
            buy1 = stock.get("1차매수선", 0)
            dist = stock.get("1차매수선이격도(%)", 0)
            
            message += f"  • {name}\n"
            message += f"    현재가: {int(close):,}원\n"
            message += f"    매수가: {int(round(buy1)):,}원\n"
            message += f"    이격도: {dist:.1f}%\n\n"
        
        message += "\n"
    
    # 매수 완료 종목 - 수익률 높은 순으로 정렬
    if bought_stocks:
        message += f"🔴 <b>매수 완료 종목</b> ({len(bought_stocks)}개)\n"
        
        # 수익률 높은 순으로 정렬
        bought_stocks.sort(key=lambda x: ((x.get("종가", 0) - x.get("평균매수가", 0)) / x.get("평균매수가", 1)) * 100 if x.get("평균매수가", 0) else -999, reverse=True)
        
        for stock in bought_stocks:
            name = stock.get("종목명", "")
            close = stock.get("종가", 0)
            avg_price = stock.get("평균매수가", 0)
            
            message += f"  • {name}\n"
            message += f"    현재가: {int(close):,}원\n"
            
            if avg_price and close:
                dist = ((close - avg_price) / avg_price) * 100
                message += f"    평균가: {int(round(avg_price)):,}원\n"
                message += f"    이격도: {dist:+.1f}%\n\n"
            else:
                message += f"    평균가: -\n"
                message += f"    이격도: -\n\n"
        
        message += "\n"
    
    # 매도선 접근 - 이격도 낮은 순으로 정렬
    if ready_sell:
        message += f"🟢 <b>매도선 접근</b> ({len(ready_sell)}개)\n"
        
        # 이격도 낮은 순으로 정렬
        ready_sell.sort(key=lambda x: min(
            abs(x.get("1차매도선이격도(%)", 999)),
            abs(x.get("2차매도선이격도(%)", 999)),
            abs(x.get("3차매도선이격도(%)", 999))
        ))
        
        for stock in ready_sell:
            name = stock.get("종목명", "")
            close = stock.get("종가", 0)
            msg = stock.get("상태메시지", "")
            
            # 매도선 찾기
            if "+3%" in msg:
                target = stock.get("1차매도선(+3%)", 0)
                dist = stock.get("1차매도선이격도(%)", 0)
            elif "+5%" in msg:
                target = stock.get("2차매도선(+5%)", 0)
                dist = stock.get("2차매도선이격도(%)", 0)
            elif "+7%" in msg:
                target = stock.get("3차매도선(+7%)", 0)
                dist = stock.get("3차매도선이격도(%)", 0)
            else:
                target = 0
                dist = 0
            
            message += f"  • {name}\n"
            message += f"    현재가: {int(close):,}원\n"
            message += f"    목표가: {int(round(target)):,}원\n"
            message += f"    이격도: {dist:+.1f}%\n\n"
        
        message += "\n"
    
    send_telegram_message(message, recipients)


def send_realtime_alert(alert_type: str, stock_name: str, ticker: str, 
                       current_price: float, target_price: float, 
                       distance_pct: float, recipients: List[str] = None):
    """
    실시간 알람 전송
    
    Args:
        alert_type: "1차 매수선 5% 인접", "2차 매수선 5% 인접", "1차 매수 체결" 등
        stock_name: 종목명
        ticker: 티커
        current_price: 현재가
        target_price: 목표가 (매수선 또는 매도선)
        distance_pct: 이격도 (%)
        recipients: 수신자 리스트
    """
    from datetime import datetime
    
    now = datetime.now().strftime("%H:%M:%S")
    
    # 알람 타입별 이모지
    emoji_map = {
        "1차 매수선 5% 인접": "🟡",
        "2차 매수선 5% 인접": "🟠",
        "3차 매수선 5% 인접": "🔴",
        "1차 매수 체결": "✅",
        "2차 매수 체결": "✅✅",
        "3차 매수 체결": "✅✅✅",
        "1차 매도선 5% 인접": "🟢",
        "2차 매도선 5% 인접": "💚",
        "3차 매도선 5% 인접": "💰"
    }
    
    emoji = emoji_map.get(alert_type, "🔔")
    
    message = f"{emoji} <b>{alert_type}</b>\n"
    message += f"🕐 {now}\n"
    message += f"───────────────\n"
    message += f"종목: {stock_name}\n"
    message += f"현재가: {int(current_price):,}원\n"
    message += f"목표가: {int(round(target_price)):,}원\n"
    message += f"이격도: {distance_pct:+.2f}%\n"
    
    send_telegram_message(message, recipients)


def send_error_alert(error_message: str, script_name: str = None, recipients: List[str] = None):
    """
    에러 알람 전송
    
    Args:
        error_message: 에러 메시지
        script_name: 스크립트 이름
        recipients: 수신자 리스트 (기본값: 본인만)
    """
    from datetime import datetime
    
    if recipients is None:
        recipients = ["me"]  # 에러는 본인에게만
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    message = f"❌ <b>시스템 에러 발생</b>\n"
    message += f"🕐 {now}\n"
    if script_name:
        message += f"📝 스크립트: {script_name}\n"
    message += f"───────────────\n"
    message += f"<pre>{error_message}</pre>"
    
    send_telegram_message(message, recipients)


# 테스트용
if __name__ == "__main__":
    # 간단한 테스트 메시지
    test_msg = "🤖 <b>텔레그램 봇 테스트</b>\n테스트 메시지입니다!"
    
    # 본인에게만 테스트
    print("본인에게 테스트 메시지 전송 중...")
    send_telegram_message(test_msg, recipients=["me"])

