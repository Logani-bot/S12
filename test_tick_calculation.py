#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
수정된 호가 계산 방식 테스트
"""

from Trading_Signal_System import get_tick_unit, get_nearest_tick_price, get_one_tick_up_price

def test_tick_calculation():
    print("=== 수정된 호가 계산 방식 테스트 ===")
    print()
    
    # 테스트 케이스들
    test_cases = [
        {"price": 95431, "description": "사용자 예시"},
        {"price": 69344, "description": "삼성전자 엔벨로프"},
        {"price": 301860, "description": "SK하이닉스 엔벨로프"},
        {"price": 55052, "description": "두산에너빌리티 엔벨로프"},
        {"price": 200740, "description": "NAVER 엔벨로프"},
        {"price": 171580, "description": "삼성SDI 엔벨로프"},
        {"price": 41394, "description": "에코프로 엔벨로프"},
        {"price": 295720, "description": "LG에너지솔루션 엔벨로프"},
    ]
    
    for case in test_cases:
        price = case["price"]
        tick_unit = get_tick_unit(price)
        nearest_tick = get_nearest_tick_price(price)
        one_tick_up = get_one_tick_up_price(price)
        
        print(f"{case['description']:20} | {price:>8,}원")
        print(f"{'':20} | 호가단위: {tick_unit:>4}원")
        print(f"{'':20} | 가장 가까운 정규호가: {nearest_tick:>8,}원")
        print(f"{'':20} | 한 호가 위: {one_tick_up:>8,}원")
        print()
    
    print("=== 비교: 기존 방식 vs 수정된 방식 ===")
    print()
    
    # 사용자 예시로 비교
    envelope_price = 95431
    
    # 기존 방식
    old_tick_unit = get_tick_unit(envelope_price)
    old_buy_line = envelope_price + old_tick_unit
    
    # 수정된 방식
    new_buy_line = get_one_tick_up_price(envelope_price)
    
    print(f"엔벨로프 가격: {envelope_price:,}원")
    print(f"호가 단위: {old_tick_unit}원")
    print()
    print(f"기존 방식: {envelope_price:,} + {old_tick_unit} = {old_buy_line:,}원")
    print(f"수정된 방식: 가장 가까운 정규호가({get_nearest_tick_price(envelope_price):,}원) + {old_tick_unit} = {new_buy_line:,}원")
    print()
    print(f"차이: {abs(old_buy_line - new_buy_line):,}원")
    
    # 정규 호가인지 확인
    def is_valid_tick_price(price, tick_unit):
        return price % tick_unit == 0
    
    print()
    print(f"기존 방식 결과가 정규 호가인가? {is_valid_tick_price(old_buy_line, old_tick_unit)}")
    print(f"수정된 방식 결과가 정규 호가인가? {is_valid_tick_price(new_buy_line, old_tick_unit)}")

if __name__ == "__main__":
    test_tick_calculation()
