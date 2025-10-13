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


def send_telegram_message(message: str, recipients: List[str] = None, parse_mode: str = "Markdown") -> bool:
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
    message = f"📊 *일일 트레이딩 리포트*\n"
    message += f"🕐 {now}\n"
    message += f"━━━━━━━━━━━━━━━━━\n\n"
    
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
    
    # 1차 매수 접근 중 (10% 이내)
    if ready_buy1:
        message += f"🟡 *1차 매수 접근 중* ({len(ready_buy1)}개)\n"
        for stock in ready_buy1:
            name = stock.get("종목명", "")
            dist = stock.get("1차매수선이격도(%)", 0)
            message += f"  • {name}: {dist:.1f}% 남음\n"
        message += "\n"
    
    # 매수 완료 종목
    if bought_stocks:
        message += f"🔴 *매수 완료 종목* ({len(bought_stocks)}개)\n"
        for stock in bought_stocks:
            name = stock.get("종목명", "")
            status = stock.get("매수상태", "")
            avg_price = stock.get("평균매수가", 0)
            if avg_price:
                message += f"  • {name} ({status}): 평균 {avg_price:,.0f}원\n"
            else:
                message += f"  • {name} ({status})\n"
        message += "\n"
    
    # 매도선 접근
    if ready_sell:
        message += f"🟢 *매도선 접근* ({len(ready_sell)}개)\n"
        for stock in ready_sell:
            name = stock.get("종목명", "")
            msg = stock.get("상태메시지", "")
            message += f"  • {name}: {msg}\n"
        message += "\n"
    
    message += f"━━━━━━━━━━━━━━━━━\n"
    message += f"📈 총 {total_stocks}개 종목 추적 중\n"
    message += f"🔔 알람: {len(alerts)}개"
    
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
    
    message = f"{emoji} *{alert_type}*\n"
    message += f"🕐 {now}\n"
    message += f"━━━━━━━━━━━━━━━━━\n"
    message += f"📌 종목: *{stock_name}* ({ticker})\n"
    message += f"💵 현재가: `{current_price:,.0f}원`\n"
    message += f"🎯 목표가: `{target_price:,.0f}원`\n"
    message += f"📊 이격도: `{distance_pct:.2f}%`\n"
    
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
    
    message = f"❌ *시스템 에러 발생*\n"
    message += f"🕐 {now}\n"
    if script_name:
        message += f"📝 스크립트: {script_name}\n"
    message += f"━━━━━━━━━━━━━━━━━\n"
    message += f"```\n{error_message}\n```"
    
    send_telegram_message(message, recipients)


# 테스트용
if __name__ == "__main__":
    # 간단한 테스트 메시지
    test_msg = "🤖 텔레그램 봇 테스트\n테스트 메시지입니다!"
    
    # 본인에게만 테스트
    print("본인에게 테스트 메시지 전송 중...")
    send_telegram_message(test_msg, recipients=["me"])

