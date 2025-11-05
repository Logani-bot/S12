"""
중복 종목 제거 후 올바른 S1 리포트 생성
"""

import pandas as pd
from datetime import datetime
import sys
import os

# 상대 경로로 telegram_notifier_s1 모듈 임포트
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import telegram_notifier_s1 as telegram_s1
except ImportError:
    print("telegram_notifier_s1 모듈을 찾을 수 없습니다.")
    sys.exit(1)

def generate_fixed_s1_report():
    """중복 제거 후 S1 일일 트레이딩 리포트 생성"""
    
    print("=== 중복 제거 후 S1 일일 트레이딩 리포트 생성 ===")
    
    try:
        # 트레이딩 시그널 데이터 로드
        df = pd.read_excel('output/trading_signals_s1.xlsx')
        
        print(f"원본 데이터: {len(df)}개 종목")
        
        # 중복 제거: 동일 티커의 첫 번째 항목만 유지
        df_unique = df.drop_duplicates(subset=['티커'], keep='first')
        
        print(f"중복 제거 후: {len(df_unique)}개 종목")
        
        # 현재 시간
        now = datetime.now()
        report_date = now.strftime('%Y-%m-%d %H:%M:%S')
        
        # 기본 통계
        total_stocks = len(df_unique)
        
        # 매수 신호 종목 찾기 (거리 0.01 이하 = 1% 이하)
        # 엑셀값이 소수로 저장되어 있으므로 0.01 (1%) 기준
        buy_signals = df_unique[df_unique.iloc[:, 11] <= 0.01]  # 거리 컬럼 (11번째) - 소수값
        buy_count = len(buy_signals)
        
        print(f"\n[S1 트레이딩 리포트] ({report_date})")
        print(f"총 분석 종목: {total_stocks}개 (중복 제거 후)")
        print(f"매수 신호: {buy_count}개")
        
        # 상위 매수 신호 종목들 (거리 순으로 정렬)
        if buy_count > 0:
            top_buy_signals = buy_signals.nsmallest(10, df_unique.columns[11])  # 거리가 작은 순으로 10개
            
            print(f"\n[상위 매수 신호 종목] (거리 순):")
            for idx, row in top_buy_signals.iterrows():
                ticker = row.iloc[0]
                name = row.iloc[1]
                distance = row.iloc[11]
                current_price = row.iloc[5] if len(row) > 5 else 0
                buy_line = row.iloc[10] if len(row) > 10 else 0
                
                print(f"  {name} ({ticker}): 현재가 {current_price:,}원, 매수라인 {buy_line:,}원, 거리 {distance*100:.2f}%")
        
        # 텔레그램 리포트 전송
        print(f"\n[텔레그램 리포트 전송 중...]")
        
        # 리포트 메시지 생성 (S12와 동일하게 항상 리포트 전송)
        report_message = f"""📊 **일일 트레이딩 리포트 S1 (시총 기반)**
📅 {report_date}

**📈 분석 결과**
• 총 분석 종목: {total_stocks:,}개 (중복 제거 후)
• 매수 신호: {buy_count:,}개"""

        if buy_count > 0:
            report_message += f"""

**🔍 주요 매수 신호 종목**"""
            # 상위 5개 매수 신호만 포함
            top_5_signals = buy_signals.nsmallest(5, df_unique.columns[11])
            
            for idx, row in top_5_signals.iterrows():
                ticker = row.iloc[0]
                name = row.iloc[1]
                distance = row.iloc[11]
                current_price = row.iloc[5] if len(row) > 5 else 0
                buy_line = row.iloc[10] if len(row) > 10 else 0
                
                report_message += f"""
• {name} ({ticker})
  현재가: {current_price:,}원
  매수라인: {buy_line:,}원
  거리: {distance*100:.2f}%"""
        else:
            report_message += f"""

🔕 매수 신호 대상 없음"""
        
        report_message += f"""

**💡 S1 시스템 특징**
• 시총 1.5조원 이상 종목 분석
• 239개 대형주 대상
• 현재가 기준 매수라인 거리 분석
• 중복 제거 후 정확한 분석

---
🤖 S1 Trading System"""
        
        # 텔레그램 전송
        success = telegram_s1.send_telegram_message(report_message, recipients=["me"])
        
        if success:
            print("[SUCCESS] S1 일일 트레이딩 리포트 전송 완료!")
        else:
            print("[FAILED] 리포트 전송 실패")
            
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_fixed_s1_report()
