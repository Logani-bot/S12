"""
Slack 알람 전송 모듈 (S12 시스템용)
"""
import os
import requests
import logging
from typing import Optional, List
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

# Slack Webhook URL (환경 변수에서만 읽기)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def convert_html_to_slack_markdown(html_text: str) -> str:
    """
    HTML 태그를 Slack 마크다운으로 변환
    
    Args:
        html_text: HTML 형식의 텍스트
    
    Returns:
        str: Slack 마크다운 형식의 텍스트
    """
    import re
    
    # <b>태그 → *bold*
    text = re.sub(r'<b>(.*?)</b>', r'*\1*', html_text)
    
    # <tg-spoiler>태그 → _spoiler_ (이탤릭체로)
    text = re.sub(r'<tg-spoiler>(.*?)</tg-spoiler>', r'_\1_', text)
    
    # <pre>태그 → ```code block```
    text = re.sub(r'<pre>(.*?)</pre>', r'```\1```', text, flags=re.DOTALL)
    
    # HTML 엔티티 디코딩
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&amp;', '&')
    
    return text


def send_slack_message(message: str, parse_html: bool = True) -> bool:
    """
    Slack 메시지 전송 (Incoming Webhook 사용)
    
    Args:
        message: 전송할 메시지 (HTML 태그 포함 가능)
        parse_html: HTML 태그를 Slack 마크다운으로 변환할지 여부
    
    Returns:
        bool: 전송 성공 여부
    """
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack Webhook URL이 설정되지 않았습니다. Slack 알림을 건너뜁니다.")
        return False
    
    try:
        # HTML 태그를 Slack 마크다운으로 변환
        if parse_html:
            slack_message = convert_html_to_slack_markdown(message)
        else:
            slack_message = message
        
        payload = {
            "text": slack_message
        }
        
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        
        logger.info("✓ Slack 전송 성공")
        return True
        
    except Exception as e:
        logger.error(f"✗ Slack 전송 실패: {e}")
        return False


def send_slack_realtime_alert(alert_type: str, stock_name: str, ticker: str,
                             current_price: float, target_price: float,
                             distance_pct: float, sell_prices: dict = None,
                             system_label: str = "S1", low_price: float = None) -> bool:
    """
    실시간 알람을 Slack으로 전송
    
    Args:
        alert_type: "1차 매수선 5% 인접", "2차 매수선 5% 인접", "1차 매수 체결" 등
        stock_name: 종목명
        ticker: 티커
        current_price: 현재가
        target_price: 목표가 (매수선 또는 매도선)
        distance_pct: 이격도 (%)
        sell_prices: 매도가 정보 {"sell1": 가격, "sell2": 가격, "sell3": 가격}
        system_label: 시스템 라벨 (기본값: "S1")
        low_price: 저가 (선택적)
    
    Returns:
        bool: 전송 성공 여부
    """
    from datetime import datetime
    
    try:
        now = datetime.now().strftime("%H:%M:%S")
        
        # 알람 타입별 이모지
        emoji_map = {
            "1차 매수선 5% 인접": "🟡",
            "1차 매수선 3% 인접": "🟠",
            "1차 매수선 1% 인접": "🔴",
            "2차 매수선 5% 인접": "🟡",
            "2차 매수선 3% 인접": "🟠",
            "2차 매수선 1% 인접": "🔴",
            "3차 매수선 5% 인접": "🟡",
            "3차 매수선 3% 인접": "🟠",
            "3차 매수선 1% 인접": "🔴",
            "1차 매수 체결": "✅",
            "1차 매수 체결!": "✅",
            "2차 매수 체결": "✅✅",
            "2차 매수 체결!": "✅✅",
            "3차 매수 체결": "✅✅✅",
            "3차 매수 체결!": "✅✅✅",
        }
        
        emoji = emoji_map.get(alert_type, "🔔")
        
        message = f"{emoji} *[{system_label}] {alert_type}*\n"
        message += f"🕐 {now}\n"
        message += f"────────────\n"
        message += f"종목: {stock_name} ({ticker})\n"
        message += f"현재가: {int(current_price):,}원\n"
        if low_price is not None:
            message += f"저가: {int(low_price):,}원\n"
        message += f"목표가: {int(round(target_price)):,}원\n"
        message += f"이격도: {distance_pct:+.2f}%\n"
        
        # 매수 체결 시 매도가 정보 추가
        if "매수 체결" in alert_type and sell_prices:
            message += f"\n*매도가 정보:*\n"
            if sell_prices.get('sell1'):
                message += f"  • 3% 매도가: {int(round(sell_prices.get('sell1', 0))):,}원\n"
            if sell_prices.get('sell2'):
                message += f"  • 5% 매도가: {int(round(sell_prices.get('sell2', 0))):,}원\n"
            if sell_prices.get('sell3'):
                message += f"  • 7% 매도가: {int(round(sell_prices.get('sell3', 0))):,}원\n"
            message += f"────────────\n"
        
        return send_slack_message(message, parse_html=False)
        
    except Exception as e:
        logger.error(f"Slack 알림 포맷팅 실패: {e}")
        return False


def send_slack_daily_report(alerts: List[dict], total_stocks: int, system_label: str = "S2") -> bool:
    """
    일일 리포트를 Slack으로 전송
    
    Args:
        alerts: 알람 대상 종목 리스트
        total_stocks: 총 종목 수
        system_label: 시스템 라벨 (기본값: "S2")
    
    Returns:
        bool: 전송 성공 여부
    """
    from datetime import datetime
    
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        
        message = f"📊 *[{system_label}] 일일 트레이딩 리포트*\n"
        message += f"🕐 {now}\n"
        message += f"────────────\n\n"
        
        if not alerts:
            message += f"✅ 총 {total_stocks}개 종목 분석\n"
            message += f"🔕 알람 대상 없음\n"
            return send_slack_message(message, parse_html=False)
        
        # 상태별 그룹화
        ready_buy1 = []
        ready_buy2 = []
        ready_buy3 = []
        bought_stocks = []
        ready_sell = []
        
        for alert in alerts:
            status = alert.get("알람상태", "")
            if "READY_BUY1" in status:
                ready_buy1.append(alert)
            elif "READY_BUY2" in status:
                ready_buy2.append(alert)
            elif "READY_BUY3" in status:
                ready_buy3.append(alert)
            elif "BOUGHT" in alert.get("매수상태", ""):
                bought_stocks.append(alert)
            elif "READY_SELL" in status:
                ready_sell.append(alert)
        
        # 1차 매수 접근 중
        if ready_buy1:
            message += f"🟡 *1차 매수 접근 중* ({len(ready_buy1)}개)\n"
            ready_buy1.sort(key=lambda x: x.get("1차매수선이격도(%)", 999))
            for stock in ready_buy1:
                name = stock.get("종목명", "")
                close = stock.get("종가", 0)
                buy1 = stock.get("1차매수선(익일)", 0)
                dist = stock.get("1차매수선이격도(%)", 0)
                message += f"  • {name}\n"
                message += f"    현재가: {int(close):,}원 / 매수가: {int(round(buy1)):,}원 / 이격도: {dist:.1f}%\n"
            message += "\n"
        
        # 2차 매수 접근 중
        if ready_buy2:
            message += f"🟠 *2차 매수 접근 중* ({len(ready_buy2)}개)\n"
            ready_buy2.sort(key=lambda x: x.get("2차매수선이격도(%)", 999))
            for stock in ready_buy2:
                name = stock.get("종목명", "")
                close = stock.get("종가", 0)
                buy2 = stock.get("2차매수선(익일)", 0)
                dist = stock.get("2차매수선이격도(%)", 0)
                message += f"  • {name}\n"
                message += f"    현재가: {int(close):,}원 / 매수가: {int(round(buy2)):,}원 / 이격도: {dist:.1f}%\n"
            message += "\n"
        
        # 3차 매수 접근 중
        if ready_buy3:
            message += f"🟤 *3차 매수 접근 중* ({len(ready_buy3)}개)\n"
            ready_buy3.sort(key=lambda x: x.get("3차매수선이격도(%)", 999))
            for stock in ready_buy3:
                name = stock.get("종목명", "")
                close = stock.get("종가", 0)
                buy3 = stock.get("3차매수선(익일)", 0)
                dist = stock.get("3차매수선이격도(%)", 0)
                message += f"  • {name}\n"
                message += f"    현재가: {int(close):,}원 / 매수가: {int(round(buy3)):,}원 / 이격도: {dist:.1f}%\n"
            message += "\n"
        
        # 매수 완료 종목
        if bought_stocks:
            message += f"🔴 *매수 완료 종목* ({len(bought_stocks)}개)\n"
            bought_stocks.sort(key=lambda x: ((x.get("종가", 0) - x.get("평균매수가", 0)) / x.get("평균매수가", 1)) * 100 if x.get("평균매수가", 0) else -999, reverse=True)
            for stock in bought_stocks:
                name = stock.get("종목명", "")
                close = stock.get("종가", 0)
                avg_price = stock.get("평균매수가", 0)
                if avg_price and close:
                    dist = ((close - avg_price) / avg_price) * 100
                    message += f"  • {name}\n"
                    message += f"    현재가: {int(close):,}원 / 평균가: {int(round(avg_price)):,}원 / 이격도: {dist:+.1f}%\n"
                else:
                    message += f"  • {name}\n"
                    message += f"    현재가: {int(close):,}원\n"
            message += "\n"
        
        # 매도선 접근
        if ready_sell:
            message += f"🟢 *매도선 접근* ({len(ready_sell)}개)\n"
            ready_sell.sort(key=lambda x: min(
                abs(x.get("1차매도선이격도(%)", 999)),
                abs(x.get("2차매도선이격도(%)", 999)),
                abs(x.get("3차매도선이격도(%)", 999))
            ))
            for stock in ready_sell:
                name = stock.get("종목명", "")
                close = stock.get("종가", 0)
                msg = stock.get("상태메시지", "")
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
                message += f"    현재가: {int(close):,}원 / 목표가: {int(round(target)):,}원 / 이격도: {dist:+.1f}%\n"
            message += "\n"
        
        return send_slack_message(message, parse_html=False)
        
    except Exception as e:
        logger.error(f"Slack 일일 리포트 포맷팅 실패: {e}")
        return False


# 테스트용
if __name__ == "__main__":
    # 간단한 테스트 메시지
    test_msg = "🤖 *Slack 봇 테스트 (S2)*\n테스트 메시지입니다!"
    
    print("Slack 테스트 메시지 전송 중...")
    if send_slack_message(test_msg, parse_html=False):
        print("[SUCCESS] Slack 테스트 메시지 전송 성공!")
    else:
        print("[FAILED] Slack 테스트 메시지 전송 실패")
