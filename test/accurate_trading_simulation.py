"""
실제 Trading_Signal_System.py 로직을 복사한 정확한 시뮬레이션 테스트
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
import shutil
import os
import math

class AccurateTradingSimulation:
    def __init__(self):
        self.signal_file = "output/trading_signals.xlsx"
        self.backup_file = "output/trading_signals_backup.xlsx"
        
        # 실제 시스템과 동일한 상수들
        self.BUY_LEVEL_GAP = 10.0  # 매수선 간격 10%
        self.SELL_LEVELS = [3.0, 5.0, 7.0]  # 매도선 레벨
        self.SELL_LEVEL_1_GAP = 3.0
        self.SELL_LEVEL_2_GAP = 5.0
        self.SELL_LEVEL_3_GAP = 7.0
        
        # 매수/매도 상태
        self.BuyStatus = type('BuyStatus', (), {
            'NONE': 'NONE',
            'BOUGHT_1': 'BOUGHT_1',
            'BOUGHT_2': 'BOUGHT_2',
            'BOUGHT_3': 'BOUGHT_3',
            'SOLD': 'SOLD'
        })
        
        # 알람 상태
        self.AlertStatus = type('AlertStatus', (), {
            'WATCHING': 'WATCHING',
            'READY_BUY1': 'READY_BUY1',
            'READY_BUY2': 'READY_BUY2',
            'READY_BUY3': 'READY_BUY3',
            'READY_SELL1': 'READY_SELL1',
            'READY_SELL2': 'READY_SELL2',
            'READY_SELL3': 'READY_SELL3',
            'WAITING': 'WAITING',
            'COMPLETED': 'COMPLETED'
        })
    
    def backup_current_data(self):
        """백업 생성"""
        try:
            shutil.copy2(self.signal_file, self.backup_file)
            print(f"Backup completed: {self.backup_file}")
            return True
        except Exception as e:
            print(f"Backup failed: {e}")
            return False
    
    def restore_backup(self):
        """백업 복원"""
        try:
            shutil.copy2(self.backup_file, self.signal_file)
            print(f"Restore completed: {self.signal_file}")
            return True
        except Exception as e:
            print(f"Restore failed: {e}")
            return False
    
    # ==================== 실제 시스템과 동일한 함수들 ====================
    
    def get_tick_unit(self, price: float) -> int:
        """한국 주식시장 정확한 호가 단위 반환"""
        if price < 2000:
            return 1
        elif price < 5000:
            return 5
        elif price < 20000:
            return 10
        elif price < 50000:
            return 50
        elif price < 200000:
            return 100
        elif price < 500000:
            return 500
        else:
            return 1000
    
    def get_nearest_tick_price(self, price: float) -> float:
        """가장 가까운 정규 호가 가격 계산 (항상 윗 호가)"""
        tick_unit = self.get_tick_unit(price)
        
        if price % tick_unit == 0:
            return price
        
        lower_tick = (price // tick_unit) * tick_unit
        upper_tick = lower_tick + tick_unit
        
        return upper_tick
    
    def get_one_tick_up_price(self, price: float) -> float:
        """한 호가 위 가격 계산"""
        nearest_tick = self.get_nearest_tick_price(price)
        tick_unit = self.get_tick_unit(nearest_tick)
        return nearest_tick + tick_unit
    
    def ceil_tick(self, price: float) -> float:
        """호가 단위에 맞춰 윗 호가로 올림"""
        delta = self.get_tick_unit(price)
        return math.ceil(price / delta) * delta
    
    def floor_tick(self, price: float) -> float:
        """호가 단위에 맞춰 아래 호가로 내림 (매도선용)"""
        delta = self.get_tick_unit(price)
        return math.floor(price / delta) * delta
    
    def predict_next_day_buy_price(self, S19_next: float) -> int:
        """다음날 매수 목표가 예측 (실시간 감시와 100% 동일한 로직)"""
        if S19_next is None or S19_next <= 0:
            return None
        
        x_star = S19_next / 24.0
        p = self.ceil_tick(x_star)
        
        while True:
            delta = self.get_tick_unit(p)
            upper = (S19_next + 25.0 * delta) / 24.0
            
            if p < upper:
                return int(p)
            else:
                p = p + delta
    
    def calculate_buy_line_1(self, S19_next: float) -> float:
        """1차 매수선: 다음날 실시간 감시와 100% 동일한 매수 목표가"""
        if S19_next is None or S19_next <= 0:
            return None
        return self.predict_next_day_buy_price(S19_next)
    
    def calculate_buy_line_2(self, buy1: float) -> float:
        """2차 매수선: 1차 매수선에서 10% 하락 후 한 호가 위"""
        if buy1 is None:
            return None
        base_price = buy1 * (1 - self.BUY_LEVEL_GAP / 100)
        return self.get_one_tick_up_price(base_price)
    
    def calculate_buy_line_3(self, buy2: float) -> float:
        """3차 매수선: 2차 매수선에서 10% 하락 후 한 호가 위"""
        if buy2 is None:
            return None
        base_price = buy2 * (1 - self.BUY_LEVEL_GAP / 100)
        return self.get_one_tick_up_price(base_price)
    
    def calculate_sell_line_1(self, avg_buy_price: float) -> float:
        """1차 매도선: 평균 매수가에서 3% 상승 후 아래 호가"""
        if avg_buy_price is None:
            return None
        target_price = avg_buy_price * (1 + self.SELL_LEVEL_1_GAP / 100)
        return self.floor_tick(target_price)
    
    def calculate_sell_line_2(self, avg_buy_price: float) -> float:
        """2차 매도선: 평균 매수가에서 5% 상승 후 아래 호가"""
        if avg_buy_price is None:
            return None
        target_price = avg_buy_price * (1 + self.SELL_LEVEL_2_GAP / 100)
        return self.floor_tick(target_price)
    
    def calculate_sell_line_3(self, avg_buy_price: float) -> float:
        """3차 매도선: 평균 매수가에서 7% 상승 후 아래 호가"""
        if avg_buy_price is None:
            return None
        target_price = avg_buy_price * (1 + self.SELL_LEVEL_3_GAP / 100)
        return self.floor_tick(target_price)
    
    def calculate_distance_pct(self, current: float, target: float) -> float:
        """이격도 계산"""
        if target is None or target == 0:
            return None
        return ((current - target) / target) * 100
    
    def check_buy_signal(self, low: float, buy_line: float) -> bool:
        """매수 시그널 체크 (저가가 매수선에 도달)"""
        if buy_line is None:
            return False
        return low <= buy_line
    
    def check_sell_retouch(self, high: float, close: float, sell_line: float, max_high_line: float) -> bool:
        """매도 재터치 체크"""
        if sell_line is None or max_high_line is None:
            return False
        
        # 고가가 매도선에 도달했고, 현재가가 매도선 아래로 떨어졌을 때
        return high >= sell_line and close < sell_line
    
    def determine_alert_status(self, buy_status: str, close: float,
                              buy1: float, buy2: float, buy3: float,
                              sell1: float, sell2: float, sell3: float,
                              dist_buy1: float, dist_buy2: float, dist_buy3: float,
                              dist_sell1: float, dist_sell2: float, dist_sell3: float,
                              threshold: float) -> tuple:
        """알람 상태 및 메시지 결정"""
        
        if buy_status == self.BuyStatus.SOLD:
            return self.AlertStatus.COMPLETED, "매도 완료"
        
        # 매수 전
        if buy_status == self.BuyStatus.NONE:
            if dist_buy1 is not None and 0 < dist_buy1 <= threshold:
                return self.AlertStatus.READY_BUY1, f"1차 매수선까지 {dist_buy1:.1f}% (접근 중!)"
            else:
                return self.AlertStatus.WATCHING, f"1차 매수선까지 {dist_buy1:.1f}%"
        
        # 1차 매수 후
        elif buy_status == self.BuyStatus.BOUGHT_1:
            if dist_sell1 is not None and dist_sell1 <= threshold:
                return self.AlertStatus.READY_SELL1, f"+3% 매도선까지 {abs(dist_sell1):.1f}%"
            elif dist_buy2 is not None and 0 < dist_buy2 <= threshold:
                return self.AlertStatus.READY_BUY2, f"2차 매수선까지 {dist_buy2:.1f}%"
            else:
                return self.AlertStatus.WAITING, f"대기 중 (2차선까지 {dist_buy2:.1f}%)"
        
        # 2차 매수 후
        elif buy_status == self.BuyStatus.BOUGHT_2:
            if dist_sell2 is not None and dist_sell2 <= threshold:
                return self.AlertStatus.READY_SELL2, f"+5% 매도선까지 {abs(dist_sell2):.1f}%"
            elif dist_buy3 is not None and 0 < dist_buy3 <= threshold:
                return self.AlertStatus.READY_BUY3, f"3차 매수선까지 {dist_buy3:.1f}%"
            else:
                return self.AlertStatus.WAITING, f"대기 중 (3차선까지 {dist_buy3:.1f}%)"
        
        # 3차 매수 후
        elif buy_status == self.BuyStatus.BOUGHT_3:
            if dist_sell3 is not None and dist_sell3 <= threshold:
                return self.AlertStatus.READY_SELL3, f"+7% 매도선까지 {abs(dist_sell3):.1f}%"
            else:
                return self.AlertStatus.WAITING, f"대기 중 (+7%까지 {dist_sell3:.1f}%)"
        
        return self.AlertStatus.WATCHING, "상태 확인 중"
    
    # ==================== 시뮬레이션용 함수들 ====================
    
    def simulate_stock_processing(self, ticker: str, name: str, 
                                 close: float, low: float, high: float,
                                 ma20: float, S20: float, Close_D_19: float,
                                 date_str: str = None, existing_buy_status: str = None,
                                 existing_buy1_price: float = None) -> dict:
        """실제 시스템과 동일한 종목 처리 로직"""
        
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        # 다음날 기준 19일 종가 합계 계산
        S19_next = S20 - Close_D_19
        
        # 엔벨로프 지지선 (-20%) - 기존 로직 유지 (참고용)
        envelope = (S20 / 20) * 0.8  # MA20 * 0.8
        
        # 새로운 정확한 매수선 계산
        buy1 = self.calculate_buy_line_1(S19_next)
        buy2 = self.calculate_buy_line_2(buy1)
        buy3 = self.calculate_buy_line_3(buy2)
        
        # 이격도 계산
        dist_buy1 = self.calculate_distance_pct(close, buy1)
        dist_buy2 = self.calculate_distance_pct(close, buy2)
        dist_buy3 = self.calculate_distance_pct(close, buy3)
        
        print(f"  [{date_str}] 종가: {close:,.0f}원, 20일선: {ma20:,.0f}원")
        print(f"  S20: {S20:,.0f}, Close_D_19: {Close_D_19:,.0f}, S19_next: {S19_next:,.0f}")
        print(f"  정확한 매수선: 1차 {buy1:,.0f}, 2차 {buy2:,.0f}, 3차 {buy3:,.0f}")
        print(f"  기존 엔벨로프: {envelope:,.0f}원 (참고용)")
        
        # 기존 데이터 확인
        df_summary = pd.read_excel(self.signal_file, sheet_name='Summary')
        existing = df_summary[df_summary["티커"] == ticker] if not df_summary.empty and "티커" in df_summary.columns else pd.DataFrame()
        
        if existing.empty:
            # 신규 종목 또는 시뮬레이션용 기존 상태 설정
            if existing_buy_status:
                # 시뮬레이션용 기존 상태 설정
                buy_status = existing_buy_status
                if existing_buy_status == self.BuyStatus.BOUGHT_1:
                    buy1_price = existing_buy1_price
                    buy1_qty = 100
                    total_qty = 100
                    total_amount = buy1_price * 100
                    avg_price = buy1_price
                    buy1_date = date_str
                else:
                    buy1_price = None
                    buy1_qty = None
                    total_qty = 0
                    total_amount = 0
                    avg_price = None
                    buy1_date = None
                
                buy2_date = None
                buy2_price = None
                buy2_qty = None
                buy3_date = None
                buy3_price = None
                buy3_qty = None
                max_high_line = None
            else:
                # 완전 신규 종목
                buy_status = self.BuyStatus.NONE
                avg_price = None
                total_qty = 0
                total_amount = 0
                buy1_date = None
                buy1_price = None
                buy1_qty = None
                buy2_date = None
                buy2_price = None
                buy2_qty = None
                buy3_date = None
                buy3_price = None
                buy3_qty = None
                max_high_line = None
        else:
            # 기존 종목
            row = existing.iloc[0]
            buy_status = row.get("매수상태", self.BuyStatus.NONE)
            avg_price = row.get("평균매수가")
            total_qty = row.get("총보유수량", 0)
            total_amount = row.get("총투자금액", 0)
            buy1_date = row.get("1차매수일")
            buy1_price = row.get("1차매수가")
            buy1_qty = row.get("1차매수량")
            buy2_date = row.get("2차매수일")
            buy2_price = row.get("2차매수가")
            buy2_qty = row.get("2차매수량")
            buy3_date = row.get("3차매수일")
            buy3_price = row.get("3차매수가")
            buy3_qty = row.get("3차매수량")
            max_high_line = row.get("최고도달선")
        
        # 매수 시그널 체크
        if buy_status == self.BuyStatus.NONE and self.check_buy_signal(low, buy1):
            buy_status = self.BuyStatus.BOUGHT_1
            buy1_date = date_str
            buy1_price = buy1
            buy1_qty = 100  # 예시: 100주
            total_qty = 100
            total_amount = buy1 * 100
            avg_price = buy1
            print(f"  1차 매수 체결! {buy1:,.0f}원 x 100주")
        
        elif buy_status == self.BuyStatus.BOUGHT_1 and self.check_buy_signal(low, buy2):
            buy_status = self.BuyStatus.BOUGHT_2
            buy2_date = date_str
            buy2_price = buy2
            buy2_qty = 100
            total_qty += 100
            total_amount += buy2 * 100
            avg_price = total_amount / total_qty
            print(f"  2차 매수 체결! {buy2:,.0f}원 x 100주")
        
        elif buy_status == self.BuyStatus.BOUGHT_2 and self.check_buy_signal(low, buy3):
            buy_status = self.BuyStatus.BOUGHT_3
            buy3_date = date_str
            buy3_price = buy3
            buy3_qty = 100
            total_qty += 100
            total_amount += buy3 * 100
            avg_price = total_amount / total_qty
            print(f"  3차 매수 체결! {buy3:,.0f}원 x 100주")
        
        # 매도선 계산 (매수 후에만)
        sell1 = None
        sell2 = None
        sell3 = None
        dist_sell1 = None
        dist_sell2 = None
        dist_sell3 = None
        
        if buy_status in [self.BuyStatus.BOUGHT_1, self.BuyStatus.BOUGHT_2, self.BuyStatus.BOUGHT_3] and avg_price:
            sell1 = self.calculate_sell_line_1(avg_price)
            sell2 = self.calculate_sell_line_2(avg_price)
            sell3 = self.calculate_sell_line_3(avg_price)
            
            dist_sell1 = self.calculate_distance_pct(close, sell1)
            dist_sell2 = self.calculate_distance_pct(close, sell2)
            dist_sell3 = self.calculate_distance_pct(close, sell3)
            
            # 최고도달선 업데이트
            if max_high_line is None:
                max_high_line = high
            else:
                max_high_line = max(max_high_line, high)
            
            # 매도 시그널 체크
            # +7% 즉시 매도
            if high >= sell3:
                buy_status = self.BuyStatus.SOLD
                print(f"  +7% 도달! 전량 매도!")
            
            # +5% 재터치
            elif self.check_sell_retouch(high, close, sell2, max_high_line):
                buy_status = self.BuyStatus.SOLD
                print(f"  +5% 재터치! 전량 매도!")
            
            # +3% 재터치
            elif self.check_sell_retouch(high, close, sell1, max_high_line):
                buy_status = self.BuyStatus.SOLD
                print(f"  +3% 재터치! 전량 매도!")
        
        # 알람 상태 결정
        alert_status, alert_msg = self.determine_alert_status(
            buy_status, close, buy1, buy2, buy3, sell1, sell2, sell3,
            dist_buy1, dist_buy2, dist_buy3, dist_sell1, dist_sell2, dist_sell3,
            10.0  # threshold
        )
        
        # 결과 반환
        result = {
            "티커": ticker,
            "종목명": name,
            "매수상태": buy_status,
            "알람상태": alert_status,
            "상태메시지": alert_msg,
            "종가": close,
            "저가": low,
            "고가": high,
            "20일선": ma20,
            "-20%엔벨로프": envelope,
            "1차매수선": buy1,
            "1차매수선이격도(%)": dist_buy1,
            "1차매수일": buy1_date,
            "1차매수가": buy1_price,
            "1차매수량": buy1_qty,
            "2차매수선": buy2,
            "2차매수선이격도(%)": dist_buy2,
            "2차매수일": buy2_date,
            "2차매수가": buy2_price,
            "2차매수량": buy2_qty,
            "3차매수선": buy3,
            "3차매수선이격도(%)": dist_buy3,
            "3차매수일": buy3_date,
            "3차매수가": buy3_price,
            "3차매수량": buy3_qty,
            "평균매수가": avg_price,
            "총투자금액": total_amount,
            "총보유수량": total_qty,
            "1차매도선(+3%)": sell1,
            "1차매도선이격도(%)": dist_sell1,
            "2차매도선(+5%)": sell2,
            "2차매도선이격도(%)": dist_sell2,
            "3차매도선(+7%)": sell3,
            "3차매도선이격도(%)": dist_sell3,
            "최고도달선": max_high_line,
        }
        
        return result
    
    def update_excel_with_result(self, result: dict):
        """Excel 파일을 결과로 업데이트"""
        try:
            df_summary = pd.read_excel(self.signal_file, sheet_name='Summary')
            df_history = pd.read_excel(self.signal_file, sheet_name='History') if 'History' in pd.ExcelFile(self.signal_file).sheet_names else pd.DataFrame()
            
            ticker = result['티커']
            
            # Summary에서 해당 종목 찾기
            mask = df_summary['티커'] == ticker
            
            if mask.any():
                # 기존 종목 업데이트
                for col, value in result.items():
                    if col in df_summary.columns:
                        df_summary.loc[mask, col] = value
            else:
                # 신규 종목 추가
                new_row = pd.DataFrame([result])
                df_summary = pd.concat([df_summary, new_row], ignore_index=True)
            
            # 매도 완료된 종목을 History로 이동
            sold_mask = df_summary['매수상태'] == self.BuyStatus.SOLD
            if sold_mask.any():
                sold_stocks = df_summary[sold_mask].copy()
                
                # History에 추가할 데이터 준비
                for idx, row in sold_stocks.iterrows():
                    history_record = row.copy()
                    history_record['종료일'] = datetime.now().strftime('%Y-%m-%d')
                    history_record['매도가'] = row['종가']  # 현재가를 매도가로 설정
                    
                    # 수익률 계산
                    avg_price = row.get('평균매수가', 0)
                    if avg_price and avg_price > 0:
                        profit_rate = ((row['종가'] - avg_price) / avg_price) * 100
                        history_record['실현수익률(%)'] = profit_rate
                    
                    # 매도 단계 결정
                    if row.get('고가', 0) >= row.get('3차매도선(+7%)', 0):
                        history_record['매도단계'] = "3차 매도 (+7% 도달)"
                    elif row.get('고가', 0) >= row.get('2차매도선(+5%)', 0):
                        history_record['매도단계'] = "2차 매도 (+5% 재터치)"
                    else:
                        history_record['매도단계'] = "1차 매도 (+3% 재터치)"
                    
                    df_history = pd.concat([df_history, pd.DataFrame([history_record])], ignore_index=True)
                
                # Summary에서 매도 완료된 종목 제거
                df_summary = df_summary[~sold_mask].reset_index(drop=True)
            
            # Excel 파일 저장
            with pd.ExcelWriter(self.signal_file, engine='openpyxl') as writer:
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
                df_history.to_excel(writer, sheet_name='History', index=False)
            
            print(f"Excel updated successfully for {result['종목명']} ({ticker})")
            
        except Exception as e:
            print(f"Excel update failed: {e}")
    
    def create_test_scenarios(self):
        """테스트 시나리오 생성"""
        scenarios = {
            "scenario_1": {
                "name": "1st Buy Execution Test",
                "description": "Test 1st buy execution with accurate calculations",
                "stocks": [
                    {"ticker": "005930", "name": "삼성전자", "close": 75000, "low": 74000, "high": 76000, "ma20": 78000, "S20": 1560000, "Close_D_19": 80000},
                    {"ticker": "000660", "name": "SK하이닉스", "close": 300000, "low": 295000, "high": 305000, "ma20": 320000, "S20": 6400000, "Close_D_19": 330000},
                ]
            },
            "scenario_2": {
                "name": "2nd Buy Execution Test",
                "description": "Test 2nd buy execution after 1st buy",
                "stocks": [
                    {"ticker": "035420", "name": "NAVER", "close": 180000, "low": 175000, "high": 185000, "ma20": 200000, "S20": 4000000, "Close_D_19": 210000, "existing_buy_status": "BOUGHT_1", "existing_buy1_price": 158000},
                ]
            },
            "scenario_3": {
                "name": "Sell Execution Test",
                "description": "Test sell execution after multiple buys",
                "stocks": [
                    {"ticker": "034020", "name": "두산에너빌리티", "close": 55000, "low": 54000, "high": 58000, "ma20": 50000, "S20": 1000000, "Close_D_19": 48000},
                ]
            }
        }
        return scenarios
    
    def run_scenario(self, scenario_name: str):
        """시나리오 실행"""
        scenarios = self.create_test_scenarios()
        
        if scenario_name not in scenarios:
            print(f"Scenario '{scenario_name}' not found.")
            return False
        
        scenario = scenarios[scenario_name]
        print(f"\nSimulation Start: {scenario['name']}")
        print(f"Description: {scenario['description']}")
        
        success_count = 0
        
        for stock_data in scenario['stocks']:
            print(f"\nProcessing {stock_data['name']} ({stock_data['ticker']})...")
            
            try:
                result = self.simulate_stock_processing(
                    ticker=stock_data['ticker'],
                    name=stock_data['name'],
                    close=stock_data['close'],
                    low=stock_data['low'],
                    high=stock_data['high'],
                    ma20=stock_data['ma20'],
                    S20=stock_data['S20'],
                    Close_D_19=stock_data['Close_D_19'],
                    existing_buy_status=stock_data.get('existing_buy_status'),
                    existing_buy1_price=stock_data.get('existing_buy1_price')
                )
                
                self.update_excel_with_result(result)
                success_count += 1
                
            except Exception as e:
                print(f"Processing failed for {stock_data['ticker']}: {e}")
        
        print(f"\nSimulation completed: {success_count}/{len(scenario['stocks'])} successful")
        return success_count > 0
    
    def run_full_test(self):
        """전체 테스트 실행"""
        print("Accurate Trading System Simulation Test")
        print("=" * 60)
        
        if not self.backup_current_data():
            print("Full test aborted due to backup failure.")
            return
        
        scenarios = self.create_test_scenarios()
        
        for i, (key, scenario) in enumerate(scenarios.items()):
            print(f"\n--- Running Scenario {i+1}: {scenario['name']} ---")
            self.run_scenario(key)
            self.print_current_status()
        
        print("\n" + "=" * 60)
        print("Full Test Completed. Check the Excel file for changes.")
        print("Use restore_backup() to restore original data if needed.")
        print("=" * 60)
    
    def print_current_status(self):
        """현재 상태 출력"""
        try:
            df_summary = pd.read_excel(self.signal_file, sheet_name='Summary')
            df_history = pd.read_excel(self.signal_file, sheet_name='History') if 'History' in pd.ExcelFile(self.signal_file).sheet_names else pd.DataFrame()
            
            print(f"\nCurrent Status:")
            print(f"  Summary: {len(df_summary)} stocks")
            print(f"  History: {len(df_history)} stocks")
            
            if not df_summary.empty and '매수상태' in df_summary.columns:
                buy_status_counts = df_summary['매수상태'].value_counts()
                print(f"  Buy Status:")
                for status, count in buy_status_counts.items():
                    print(f"    {status}: {count} stocks")
                    
        except Exception as e:
            print(f"Status print failed: {e}")

def main():
    """메인 함수"""
    simulation = AccurateTradingSimulation()
    
    print("Accurate Trading System Simulation Test")
    print("=" * 50)
    print("1. Run full test")
    print("2. Run individual scenario")
    print("3. Check current status")
    print("4. Restore backup")
    print("0. Exit")
    
    while True:
        choice = input("\nSelect option (0-4): ").strip()
        
        if choice == '1':
            simulation.run_full_test()
        elif choice == '2':
            scenarios = simulation.create_test_scenarios()
            print("\nAvailable scenarios:")
            for key, scenario in scenarios.items():
                print(f"  {key}: {scenario['name']}")
            
            scenario_name = input("\nEnter scenario name: ").strip()
            simulation.run_scenario(scenario_name)
        elif choice == '3':
            simulation.print_current_status()
        elif choice == '4':
            simulation.restore_backup()
        elif choice == '0':
            print("Exiting simulation...")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()
