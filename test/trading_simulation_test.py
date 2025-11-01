"""
S12 Trading System 시뮬레이션 테스트
- 가상 매수/매도 시나리오 생성
- 전체 플로우 테스트 (매수 → 매도 → History 이동)
- 실시간 모니터링 알림 테스트
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
from pathlib import Path

class TradingSimulation:
    def __init__(self):
        self.signal_file = "output/trading_signals.xlsx"
        self.backup_file = "output/trading_signals_backup.xlsx"
        
    def backup_current_data(self):
        """Backup current data"""
        try:
            import shutil
            shutil.copy2(self.signal_file, self.backup_file)
            print(f"Backup completed: {self.backup_file}")
            return True
        except Exception as e:
            print(f"Backup failed: {e}")
            return False
    
    def restore_backup(self):
        """Restore backup data"""
        try:
            import shutil
            shutil.copy2(self.backup_file, self.signal_file)
            print(f"Restore completed: {self.signal_file}")
            return True
        except Exception as e:
            print(f"Restore failed: {e}")
            return False
    
    def create_simulation_scenarios(self):
        """시뮬레이션 시나리오 생성"""
        scenarios = {
            "scenario_1": {
                "name": "1차 매수 체결 시뮬레이션",
                "description": "현재가가 1차 매수선에 도달한 종목들을 가상 매수",
                "stocks": ["005930", "000660", "034020"],  # 삼성전자, SK하이닉스, 네이버웍스
                "buy_level": 1,
                "simulate_price": "current_at_buy1"
            },
            "scenario_2": {
                "name": "2차 매수 체결 시뮬레이션", 
                "description": "1차 매수 후 2차 매수선에 도달한 종목들",
                "stocks": ["035420", "042700"],  # NAVER, 삼성바이오로직스
                "buy_level": 2,
                "simulate_price": "current_at_buy2"
            },
            "scenario_3": {
                "name": "매도 조건 도달 시뮬레이션",
                "description": "매수된 종목들이 매도선에 도달",
                "stocks": ["005930", "000660"],  # 이미 매수된 종목들
                "sell_level": 1,
                "simulate_price": "current_at_sell1"
            },
            "scenario_4": {
                "name": "매도 완료 시뮬레이션",
                "description": "매도 완료되어 History로 이동할 종목들",
                "stocks": ["034020"],  # 네이버웍스
                "sell_level": 3,
                "simulate_price": "current_at_sell3"
            }
        }
        return scenarios
    
    def simulate_buy_execution(self, ticker, buy_level, current_price):
        """가상 매수 체결 시뮬레이션"""
        try:
            # Excel 파일 읽기
            df_summary = pd.read_excel(self.signal_file, sheet_name='Summary')
            
            # 해당 종목 찾기
            stock_row = df_summary[df_summary['티커'] == ticker]
            if stock_row.empty:
                print(f"Stock {ticker} not found.")
                return False
            
            idx = stock_row.index[0]
            
            # Update buy status
            if buy_level == 1:
                df_summary.at[idx, '매수상태'] = 'BOUGHT_1'
                df_summary.at[idx, '1차매수일'] = datetime.now().strftime('%Y-%m-%d')
                df_summary.at[idx, '1차매수가'] = current_price
                print(f"Buy 1 executed for {ticker} (price: {current_price:,} KRW)")
            elif buy_level == 2:
                df_summary.at[idx, '매수상태'] = 'BOUGHT_2'
                df_summary.at[idx, '2차매수일'] = datetime.now().strftime('%Y-%m-%d')
                df_summary.at[idx, '2차매수가'] = current_price
                print(f"Buy 2 executed for {ticker} (price: {current_price:,} KRW)")
            elif buy_level == 3:
                df_summary.at[idx, '매수상태'] = 'BOUGHT_3'
                df_summary.at[idx, '3차매수일'] = datetime.now().strftime('%Y-%m-%d')
                df_summary.at[idx, '3차매수가'] = current_price
                print(f"Buy 3 executed for {ticker} (price: {current_price:,} KRW)")
            
            # Save Excel file
            with pd.ExcelWriter(self.signal_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            return True
            
        except Exception as e:
            print(f"Buy simulation failed ({ticker}): {e}")
            return False
    
    def simulate_sell_execution(self, ticker, sell_level, current_price):
        """가상 매도 체결 시뮬레이션"""
        try:
            # Excel 파일 읽기
            df_summary = pd.read_excel(self.signal_file, sheet_name='Summary')
            df_history = pd.read_excel(self.signal_file, sheet_name='History') if 'History' in pd.ExcelFile(self.signal_file).sheet_names else pd.DataFrame()
            
            # 해당 종목 찾기
            stock_row = df_summary[df_summary['티커'] == ticker]
            if stock_row.empty:
                print(f"Stock {ticker} not found.")
                return False
            
            idx = stock_row.index[0]
            stock_data = stock_row.iloc[0]
            
            # 매도 완료 처리
            df_summary.at[idx, '매수상태'] = 'SOLD'
            df_summary.at[idx, '종료일'] = datetime.now().strftime('%Y-%m-%d')
            
            # 평균 매수가 계산
            avg_buy_price = 0
            buy_count = 0
            
            if pd.notna(stock_data.get('1차매수가', 0)) and stock_data.get('1차매수가', 0) > 0:
                avg_buy_price += stock_data['1차매수가']
                buy_count += 1
            if pd.notna(stock_data.get('2차매수가', 0)) and stock_data.get('2차매수가', 0) > 0:
                avg_buy_price += stock_data['2차매수가']
                buy_count += 1
            if pd.notna(stock_data.get('3차매수가', 0)) and stock_data.get('3차매수가', 0) > 0:
                avg_buy_price += stock_data['3차매수가']
                buy_count += 1
            
            if buy_count > 0:
                avg_buy_price = avg_buy_price / buy_count
            
            # 수익률 계산
            profit_rate = ((current_price - avg_buy_price) / avg_buy_price) * 100 if avg_buy_price > 0 else 0
            
            # History로 이동
            history_record = {
                '티커': ticker,
                '종목명': stock_data['종목명'],
                '시작일': stock_data.get('1차매수일', ''),
                '종료일': datetime.now().strftime('%Y-%m-%d'),
                '평균매수가': avg_buy_price,
                '매도가': current_price,
                '수익률(%)': profit_rate,
                '매도단계': f'{sell_level}차 매도'
            }
            
            df_history = pd.concat([df_history, pd.DataFrame([history_record])], ignore_index=True)
            
            # Summary에서 제거
            df_summary = df_summary.drop(idx).reset_index(drop=True)
            
            # Excel 파일 저장
            with pd.ExcelWriter(self.signal_file, engine='openpyxl') as writer:
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
                df_history.to_excel(writer, sheet_name='History', index=False)
            
            print(f"Sell {sell_level} completed for {ticker} -> History (profit: {profit_rate:.1f}%)")
            return True
            
        except Exception as e:
            print(f"Sell simulation failed ({ticker}): {e}")
            return False
    
    def run_simulation(self, scenario_name):
        """시뮬레이션 실행"""
        scenarios = self.create_simulation_scenarios()
        
        if scenario_name not in scenarios:
            print(f"Scenario '{scenario_name}' not found.")
            return False
        
        scenario = scenarios[scenario_name]
        print(f"\nSimulation Start: {scenario['name']}")
        print(f"Description: {scenario['description']}")
        print(f"Target stocks: {scenario['stocks']}")
        
        success_count = 0
        
        for ticker in scenario['stocks']:
            try:
                # 현재 데이터 읽기
                df_summary = pd.read_excel(self.signal_file, sheet_name='Summary')
                stock_row = df_summary[df_summary['티커'] == ticker]
                
                if stock_row.empty:
                    print(f"Stock {ticker} not found.")
                    continue
                
                stock_data = stock_row.iloc[0]
                
                if 'buy_level' in scenario:
                    # 매수 시뮬레이션
                    buy_level = scenario['buy_level']
                    if buy_level == 1:
                        current_price = stock_data['1차매수선']
                    elif buy_level == 2:
                        current_price = stock_data['2차매수선']
                    elif buy_level == 3:
                        current_price = stock_data['3차매수선']
                    
                    if self.simulate_buy_execution(ticker, buy_level, current_price):
                        success_count += 1
                
                elif 'sell_level' in scenario:
                    # 매도 시뮬레이션
                    sell_level = scenario['sell_level']
                    if sell_level == 1:
                        current_price = stock_data['1차매도선(+3%)']
                    elif sell_level == 2:
                        current_price = stock_data['2차매도선(+5%)']
                    elif sell_level == 3:
                        current_price = stock_data['3차매도선(+7%)']
                    
                    if self.simulate_sell_execution(ticker, sell_level, current_price):
                        success_count += 1
                
            except Exception as e:
                print(f"Processing failed for {ticker}: {e}")
        
        print(f"\nSimulation completed: {success_count}/{len(scenario['stocks'])} successful")
        return success_count > 0
    
    def run_full_simulation(self):
        """전체 시뮬레이션 실행"""
        print("🚀 S12 Trading System 전체 시뮬레이션 시작")
        print("=" * 60)
        
        # 백업 생성
        if not self.backup_current_data():
            return False
        
        try:
            # 시나리오 순차 실행
            scenarios = ["scenario_1", "scenario_2", "scenario_3", "scenario_4"]
            
            for scenario in scenarios:
                print(f"\n{'='*60}")
                if not self.run_simulation(scenario):
                    print(f"❌ 시나리오 {scenario} 실패")
                    break
                
                # 각 시나리오 후 현재 상태 출력
                self.print_current_status()
                
                input("\n⏸️  다음 시나리오로 진행하려면 Enter를 누르세요...")
            
            print(f"\n{'='*60}")
            print("🎉 전체 시뮬레이션 완료!")
            
        except KeyboardInterrupt:
            print("\n⏹️  시뮬레이션이 중단되었습니다.")
        except Exception as e:
            print(f"\n❌ 시뮬레이션 오류: {e}")
        finally:
            # 복원 여부 확인
            restore = input("\n🔄 원본 데이터로 복원하시겠습니까? (y/n): ").lower()
            if restore == 'y':
                self.restore_backup()
                print("✅ 원본 데이터로 복원되었습니다.")
    
    def print_current_status(self):
        """Print current status"""
        try:
            df_summary = pd.read_excel(self.signal_file, sheet_name='Summary')
            df_history = pd.read_excel(self.signal_file, sheet_name='History') if 'History' in pd.ExcelFile(self.signal_file).sheet_names else pd.DataFrame()
            
            print(f"\nCurrent Status:")
            print(f"  Summary: {len(df_summary)} stocks")
            print(f"  History: {len(df_history)} stocks")
            
            # Buy status statistics
            buy_status_counts = df_summary['매수상태'].value_counts()
            print(f"  Buy Status:")
            for status, count in buy_status_counts.items():
                print(f"    {status}: {count} stocks")
                
        except Exception as e:
            print(f"Status print failed: {e}")

def main():
    """Main function"""
    simulation = TradingSimulation()
    
    print("S12 Trading System Simulation Test")
    print("=" * 50)
    print("1. Run full simulation")
    print("2. Run individual scenario")
    print("3. Check current status")
    print("4. Restore backup")
    print("0. Exit")
    
    while True:
        choice = input("\nSelect option (0-4): ").strip()
        
        if choice == '1':
            simulation.run_full_simulation()
        elif choice == '2':
            scenarios = simulation.create_simulation_scenarios()
            print("\nAvailable scenarios:")
            for key, scenario in scenarios.items():
                print(f"  {key}: {scenario['name']}")
            
            scenario_name = input("\nEnter scenario name: ").strip()
            simulation.run_simulation(scenario_name)
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
