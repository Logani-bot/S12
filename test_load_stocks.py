#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from Real_Time_Monitor import load_summary_stocks_with_buy_lines

print("=" * 80)
print("S12 실시간 모니터링 - 종목 로드 테스트")
print("=" * 80)

df = load_summary_stocks_with_buy_lines()

print(f"\n로드된 종목 수: {len(df)}")

if not df.empty:
    print("\n컬럼 목록:")
    print(list(df.columns))
    
    print(f"\n상위 5개 종목:")
    print(df[['티커', '종목명', '1차매수선', '2차매수선', '3차매수선']].head(5).to_string())
else:
    print("\n❌ 종목을 로드할 수 없습니다.")

print("\n" + "=" * 80)


