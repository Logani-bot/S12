#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
암호화폐 실시간 모니터링 시스템

기능:
1. 00:00에 DEBUG/ANALYSIS 파일 생성
2. 00:00에 ANALYSIS 파일에서 B1~B7 값 저장
3. 30분 간격으로 실시간 가격과 비교하여 알람 전송
4. 중복 알람 방지 (코인별, 매수목표별 하루 1회)
"""

import os
import sys
import pandas as pd
import requests
import time
import json
import schedule
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import subprocess
import pathlib

# S12 디렉토리의 모듈 import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from telegram_notifier import send_telegram_message

class CryptoRealtimeMonitor:
    def __init__(self):
        self.omg_dir = pathlib.Path("C:/Coding/OMG")
        self.analysis_file = None
        self.monitoring_data = {}  # {symbol: {next_target, buy_levels, rank, name}}
        self.alert_history = {}  # {symbol: {target: sent_date}}
        self.alert_history_file = "alert_history.json"
        
        # 알람 이력 로드
        self.load_alert_history()
        
    def load_alert_history(self):
        """알람 이력 로드"""
        try:
            if os.path.exists(self.alert_history_file):
                with open(self.alert_history_file, 'r', encoding='utf-8') as f:
                    self.alert_history = json.load(f)
        except Exception as e:
            print(f"알람 이력 로드 실패: {e}")
            self.alert_history = {}
    
    def save_alert_history(self):
        """알람 이력 저장"""
        try:
            with open(self.alert_history_file, 'w', encoding='utf-8') as f:
                json.dump(self.alert_history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"알람 이력 저장 실패: {e}")
    
    def run_daily_update(self):
        """00:00에 실행되는 일일 업데이트"""
        print(f"[{datetime.now()}] 일일 업데이트 시작...")
        
        try:
            # OMG 디렉토리로 이동하여 DEBUG/ANALYSIS 파일 생성
            os.chdir(self.omg_dir)
            
            # DEBUG 파일 생성
            print("DEBUG 파일 생성 중...")
            result = subprocess.run([
                "python", "auto_debug_builder.py", "--limit-days", "1200"
            ], capture_output=True, text=True, encoding='cp949')
            
            if result.returncode != 0:
                print(f"DEBUG 파일 생성 실패: {result.stderr}")
                return False
            
            # ANALYSIS 파일 생성
            print("ANALYSIS 파일 생성 중...")
            result = subprocess.run([
                "python", "coin_analysis_excel.py"
            ], capture_output=True, text=True, encoding='cp949')
            
            if result.returncode != 0:
                print(f"ANALYSIS 파일 생성 실패: {result.stderr}")
                return False
            
            # 최신 ANALYSIS 파일 찾기
            output_dir = self.omg_dir / "output"
            analysis_files = list(output_dir.glob("coin_analysis_*.xlsx"))
            if not analysis_files:
                print("ANALYSIS 파일을 찾을 수 없습니다.")
                return False
            
            # 가장 최신 파일 선택
            self.analysis_file = max(analysis_files, key=os.path.getctime)
            print(f"ANALYSIS 파일 선택: {self.analysis_file.name}")
            
            # ANALYSIS 파일에서 모니터링 데이터 로드
            self.load_monitoring_data()
            
            # 알람 이력 초기화 (새로운 날)
            today = datetime.now().strftime("%Y-%m-%d")
            for symbol in list(self.alert_history.keys()):
                if isinstance(self.alert_history[symbol], dict):
                    for target in list(self.alert_history[symbol].keys()):
                        if self.alert_history[symbol][target] != today:
                            del self.alert_history[symbol][target]
                    # 빈 딕셔너리 제거
                    if not self.alert_history[symbol]:
                        del self.alert_history[symbol]
            
            print(f"[{datetime.now()}] 일일 업데이트 완료!")
            return True
            
        except Exception as e:
            print(f"일일 업데이트 실패: {e}")
            return False
        finally:
            # S12 디렉토리로 복귀
            os.chdir("C:/Coding/S12")
    
    def load_monitoring_data(self):
        """ANALYSIS 파일에서 모니터링 데이터 로드"""
        if not self.analysis_file or not self.analysis_file.exists():
            print("ANALYSIS 파일이 없습니다.")
            return
        
        try:
            df = pd.read_excel(self.analysis_file)
            self.monitoring_data = []
            
            for _, row in df.iterrows():
                symbol = row['심볼']
                next_target = row['다음매수목표']
                
                # 모니터링 제외 조건
                if pd.isna(next_target) or next_target in ['', 'STOP LOSS (실행됨)']:
                    continue
                
                # B1~B7 값 추출
                buy_levels = {}
                for i in range(1, 8):
                    level_key = f'B{i}'
                    if level_key in row and pd.notna(row[level_key]):
                        try:
                            # 콤마 제거 후 변환
                            value_str = str(row[level_key]).replace(',', '')
                            buy_levels[level_key] = float(value_str)
                        except (ValueError, TypeError):
                            continue
                
                # Stop_Loss 값 추출
                if 'Stop_Loss' in row and pd.notna(row['Stop_Loss']):
                    try:
                        value_str = str(row['Stop_Loss']).replace(',', '')
                        buy_levels['Stop_Loss'] = float(value_str)
                    except (ValueError, TypeError):
                        pass
                
                # 현재가 처리
                current_price = 0
                if pd.notna(row['현재가']):
                    try:
                        current_price_str = str(row['현재가']).replace(',', '')
                        current_price = float(current_price_str)
                    except (ValueError, TypeError):
                        current_price = 0
                
                self.monitoring_data.append({
                    'symbol': symbol,
                    'next_target': next_target,
                    'buy_levels': buy_levels,
                    'rank': int(row['순위']) if pd.notna(row['순위']) else 0,
                    'name': row['코인명'],
                    'current_price': current_price
                })
            
            print(f"모니터링 데이터 로드 완료: {len(self.monitoring_data)}개 코인")
            
        except Exception as e:
            print(f"모니터링 데이터 로드 실패: {e}")
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """실시간 가격 조회 (Binance API)"""
        try:
            # Binance API 사용
            url = "https://api.binance.com/api/v3/ticker/price"
            params = {"symbol": f"{symbol}USDT"}
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            return float(data['price'])
            
        except Exception as e:
            print(f"{symbol} 가격 조회 실패: {e}")
            return None
    
    def calculate_divergence(self, current_price: float, target_price: float) -> float:
        """이격도 계산 (현재가 기준)"""
        if target_price == 0:
            return float('inf')
        return abs((current_price - target_price) / target_price) * 100
    
    def get_allowed_targets(self, next_target: str) -> List[str]:
        """다음 매수 목표에 따른 허용 알람 목표 반환"""
        if next_target.startswith('B'):
            # B1~B7인 경우
            level_num = int(next_target[1])
            return [f'B{i}' for i in range(level_num, 8)] + ['STOP LOSS (실행 전)']
        elif next_target == 'STOP LOSS (실행 전)':
            return ['STOP LOSS (실행 전)']
        else:
            return []
    
    def check_alert_condition(self, coin_data: Dict, current_price: float) -> List[Dict]:
        """알람 조건 확인"""
        symbol = coin_data['symbol']
        next_target = coin_data['next_target']
        buy_levels = coin_data['buy_levels']
        
        # 허용되는 알람 목표들
        allowed_targets = self.get_allowed_targets(next_target)
        
        alerts = []
        
        for target in allowed_targets:
            if target not in buy_levels:
                continue
            
            target_price = buy_levels[target]
            divergence = self.calculate_divergence(current_price, target_price)
            
            # 5% 이내 접근 시 알람
            if divergence <= 5.0:
                # 중복 알람 확인
                today = datetime.now().strftime("%Y-%m-%d")
                if (symbol not in self.alert_history or 
                    not isinstance(self.alert_history[symbol], dict) or
                    target not in self.alert_history[symbol] or
                    self.alert_history[symbol][target] != today):
                    
                    alerts.append({
                        'symbol': symbol,
                        'target': target,
                        'target_price': target_price,
                        'current_price': current_price,
                        'divergence': divergence,
                        'rank': coin_data['rank'],
                        'name': coin_data['name']
                    })
        
        return alerts
    
    def send_alert(self, alert: Dict):
        """텔레그램 알람 전송"""
        try:
            # 알람 메시지 포맷팅
            message = (
                f"🪙 <b>매수 목표 접근 알림</b>\n\n"
                f"코인명: {alert['name']}\n"
                f"심볼: {alert['symbol']}\n"
                f"시총 순위: {alert['rank']}\n"
                f"현재가: ${alert['current_price']:,.4f}\n"
                f"매수목표: {alert['target']}\n"
                f"목표가격: ${alert['target_price']:,.4f}\n"
                f"이격도: {alert['divergence']:.2f}%"
            )
            
            # 텔레그램 전송 (모든 수신자에게)
            success = send_telegram_message(message, recipients=["all"])
            
            if success:
                # 알람 이력 업데이트
                today = datetime.now().strftime("%Y-%m-%d")
                if alert['symbol'] not in self.alert_history:
                    self.alert_history[alert['symbol']] = {}
                if not isinstance(self.alert_history[alert['symbol']], dict):
                    self.alert_history[alert['symbol']] = {}
                self.alert_history[alert['symbol']][alert['target']] = today
                self.save_alert_history()
                
                print(f"알람 전송 성공: {alert['symbol']} {alert['target']}")
            else:
                print(f"알람 전송 실패: {alert['symbol']} {alert['target']}")
                
        except Exception as e:
            print(f"알람 전송 오류: {e}")
    
    def run_monitoring_cycle(self):
        """30분 간격 모니터링 사이클"""
        if not self.monitoring_data:
            print("모니터링 데이터가 없습니다.")
            return
        
        print(f"[{datetime.now()}] 모니터링 사이클 시작...")
        
        for coin_data in self.monitoring_data:
            try:
                symbol = coin_data['symbol']
                # 실시간 가격 조회
                current_price = self.get_current_price(symbol)
                if current_price is None:
                    continue
                
                # 알람 조건 확인
                alerts = self.check_alert_condition(coin_data, current_price)
                
                # 알람 전송
                for alert in alerts:
                    self.send_alert(alert)
                
                # API 제한 방지
                time.sleep(0.1)
                
            except Exception as e:
                print(f"{symbol} 모니터링 오류: {e}")
        
        print(f"[{datetime.now()}] 모니터링 사이클 완료")
    
    def start_monitoring(self):
        """모니터링 시작"""
        print("암호화폐 실시간 모니터링 시스템 시작...")
        
        # 스케줄 설정
        schedule.every().day.at("00:00").do(self.run_daily_update)
        schedule.every(30).minutes.do(self.run_monitoring_cycle)
        
        # 초기 실행 (테스트용)
        print("초기 데이터 로드...")
        if self.run_daily_update():
            print("초기 데이터 로드 완료")
        else:
            print("초기 데이터 로드 실패")
            return
        
        # 메인 루프
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 1분마다 스케줄 확인
        except KeyboardInterrupt:
            print("모니터링 중단")
        except Exception as e:
            print(f"모니터링 오류: {e}")

def main():
    monitor = CryptoRealtimeMonitor()
    monitor.start_monitoring()

if __name__ == "__main__":
    main()
