#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
호가 단위 및 매수선 계산 테스트
"""

from Trading_Signal_System import get_tick_unit, calculate_buy_line_1, calculate_buy_line_2, calculate_buy_line_3

def test_tick_unit():
    """호가 단위 테스트"""
    print("=== Tick Unit Test ===")
    test_prices = [500, 999, 1000, 4999, 5000, 9999, 10000, 49999, 50000, 99999, 100000, 499999, 500000, 1000000]
    
    for price in test_prices:
        tick = get_tick_unit(price)
        print(f"{price:>8,} KRW -> Tick: {tick:>4} KRW")

def test_buy_line_calculation():
    """매수선 계산 테스트"""
    print("\n=== Buy Line Calculation Test ===")
    
    # 테스트 케이스들
    test_cases = [
        {"name": "Low Price Stock", "envelope": 800, "close": 1000},
        {"name": "Mid Price Stock", "envelope": 15000, "close": 20000},
        {"name": "High Price Stock", "envelope": 150000, "close": 200000},
        {"name": "Very High Price Stock", "envelope": 800000, "close": 1000000}
    ]
    
    for case in test_cases:
        print(f"\n--- {case['name']} ---")
        print(f"Envelope: {case['envelope']:,} KRW")
        print(f"Close: {case['close']:,} KRW")
        
        # 매수선 계산
        buy1 = calculate_buy_line_1(case['envelope'], case['close'])
        buy2 = calculate_buy_line_2(buy1)
        buy3 = calculate_buy_line_3(buy2)
        
        print(f"Buy Line 1: {buy1:,} KRW (envelope + {get_tick_unit(case['envelope'])} KRW)")
        print(f"Buy Line 2: {buy2:,} KRW (buy1 - 10% + {get_tick_unit(buy1 * 0.9)} KRW)")
        print(f"Buy Line 3: {buy3:,} KRW (buy2 - 10% + {get_tick_unit(buy2 * 0.9)} KRW)")
        
        # 이격도 계산
        dist1 = ((case['close'] - buy1) / buy1) * 100
        dist2 = ((case['close'] - buy2) / buy2) * 100
        dist3 = ((case['close'] - buy3) / buy3) * 100
        
        print(f"Distance 1: {dist1:.1f}%")
        print(f"Distance 2: {dist2:.1f}%")
        print(f"Distance 3: {dist3:.1f}%")

if __name__ == "__main__":
    test_tick_unit()
    test_buy_line_calculation()
