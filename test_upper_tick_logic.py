#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 호가 계산 로직 테스트 (윗 호가 우선)
"""

from Trading_Signal_System import get_tick_unit, get_nearest_tick_price, get_one_tick_up_price

def test_upper_tick_logic():
    print("=== 수정된 호가 계산 로직 테스트 (항상 윗 호가) ===")
    print()
    
    # 사용자가 지적한 문제 케이스들
    test_cases = [
        {"price": 301860, "description": "SK하이닉스 엔벨로프", "expected": 302000},
        {"price": 55052, "description": "두산에너빌리티 엔벨로프", "expected": 55100},
        {"price": 95431, "description": "사용자 예시", "expected": 95500},
        {"price": 69344, "description": "삼성전자 엔벨로프", "expected": 69400},
        {"price": 200740, "description": "NAVER 엔벨로프", "expected": 201000},
        {"price": 171580, "description": "삼성SDI 엔벨로프", "expected": 171600},
    ]
    
    print("테스트 케이스:")
    for case in test_cases:
        price = case["price"]
        expected = case["expected"]
        tick_unit = get_tick_unit(price)
        nearest_tick = get_nearest_tick_price(price)
        one_tick_up = get_one_tick_up_price(price)
        
        # 정확한지 확인
        is_correct = nearest_tick == expected
        status = "CORRECT" if is_correct else "ERROR"
        
        print(f"{case['description']:20} | {price:>8,} KRW")
        print(f"{'':20} | Tick Unit: {tick_unit:>4} KRW")
        print(f"{'':20} | Nearest Tick: {nearest_tick:>8,} KRW (Expected: {expected:>8,} KRW) {status}")
        print(f"{'':20} | One Tick Up: {one_tick_up:>8,} KRW")
        print()
    
    print("=== 경계값 테스트 ===")
    print()
    
    # 경계값 테스트
    boundary_cases = [
        {"price": 2000, "description": "2,000원 (경계값)", "expected": 2000},
        {"price": 2001, "description": "2,001원 (1원 위)", "expected": 2005},
        {"price": 1999, "description": "1,999원 (1원 아래)", "expected": 1999},
        {"price": 5000, "description": "5,000원 (경계값)", "expected": 5000},
        {"price": 5001, "description": "5,001원 (1원 위)", "expected": 5010},
        {"price": 4999, "description": "4,999원 (1원 아래)", "expected": 4999},
    ]
    
    for case in boundary_cases:
        price = case["price"]
        expected = case["expected"]
        nearest_tick = get_nearest_tick_price(price)
        
        is_correct = nearest_tick == expected
        status = "CORRECT" if is_correct else "ERROR"
        
        print(f"{case['description']:20} | {price:>8,} KRW → {nearest_tick:>8,} KRW (Expected: {expected:>8,} KRW) {status}")
    
    print()
    print("=== Logic Explanation ===")
    print("When current price is between lower and upper tick:")
    print("- ALWAYS choose upper tick (no distance comparison)")
    print("Example: 301,860 KRW (500 KRW tick)")
    print("  - Lower tick: 301,500 KRW")
    print("  - Upper tick: 302,000 KRW")
    print("  - Always choose upper tick 302,000 KRW")

if __name__ == "__main__":
    test_upper_tick_logic()
