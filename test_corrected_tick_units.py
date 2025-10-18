#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 호가 단위 테스트
"""

from Trading_Signal_System import get_tick_unit
from Real_Time_Monitor import calculate_tick_unit

def test_corrected_tick_units():
    print("=== Corrected Tick Unit Test ===")
    print()
    
    # Test various price ranges according to Korean stock market standards
    test_prices = [
        500, 999, 1999, 2000, 4999, 5000, 19999, 20000, 49999, 50000, 199999, 200000, 499999, 500000, 1000000
    ]
    
    print("Price Range Test:")
    for price in test_prices:
        tick1 = get_tick_unit(price)
        tick2 = calculate_tick_unit(price)
        match = "YES" if tick1 == tick2 else "NO"
        print(f"{price:>8,} KRW → Tick: {tick1:>4} KRW (Trading={tick1}, RealTime={tick2}, Match={match})")
    
    print()
    print("=== Korean Stock Market Standards ===")
    print("2,000원 미만     : 1원")
    print("2,000~5,000원   : 5원")
    print("5,000~20,000원  : 10원")
    print("20,000~50,000원 : 50원")
    print("50,000~200,000원: 100원")
    print("200,000~500,000원: 500원")
    print("500,000원 이상  : 1,000원")
    
    print()
    print("=== Analysis of Previous Buy Lines ===")
    
    # Analyze previous buy lines with corrected tick units
    previous_buy_lines = [
        {"name": "Samsung 1st Buy Line", "price": 70004, "old_tick": 100, "new_tick": 100},
        {"name": "SK Hynix 1st Buy Line", "price": 306560, "old_tick": 500, "new_tick": 500},
        {"name": "Doosan Energy 1st Buy Line", "price": 55052, "old_tick": 100, "new_tick": 100},
        {"name": "NAVER 1st Buy Line", "price": 200740, "old_tick": 500, "new_tick": 500},
        {"name": "Hanmi Semi 1st Buy Line", "price": 86932, "old_tick": 100, "new_tick": 100},
        {"name": "Ecopro BM 1st Buy Line", "price": 98756, "old_tick": 100, "new_tick": 100},
        {"name": "Samsung SDI 1st Buy Line", "price": 171580, "old_tick": 500, "new_tick": 100},
        {"name": "Ecopro 1st Buy Line", "price": 41394, "old_tick": 50, "new_tick": 50},
        {"name": "Ecopro Materials 1st Buy Line", "price": 41420, "old_tick": 50, "new_tick": 50},
        {"name": "LG Energy 1st Buy Line", "price": 295720, "old_tick": 500, "new_tick": 500},
        {"name": "Hanwha Ocean 1st Buy Line", "price": 87440, "old_tick": 100, "new_tick": 100},
    ]
    
    for case in previous_buy_lines:
        new_tick = get_tick_unit(case["price"])
        changed = "CHANGED" if case["old_tick"] != new_tick else "SAME"
        print(f"{case['name']:25} | {case['price']:>8,} KRW → Old: {case['old_tick']:>4} KRW, New: {new_tick:>4} KRW ({changed})")

if __name__ == "__main__":
    test_corrected_tick_units()
