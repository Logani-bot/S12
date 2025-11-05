"""
다나와(064260) 종목 당일/익일 기준 매수선 확인 테스트
"""
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, r'C:\Coding\S12')

from Trading_Signal_System import get_api_token, fetch_chart_data, calculate_ma, calculate_buy_line_1, calculate_buy_line_2, calculate_buy_line_3

# API 인증
appkey = "IweTdkYa8JWDUOa8NohVSVeOiJ1THDGd_2x050A8XcU"
secret = "eazu-jPNJpAsIVkaUTh3_88gUvXrCMJCwGF2AYRtBJs"

print("=" * 80)
print("DANAWA (064260) Analysis")
print("=" * 80)

# API 토큰 획득
token = get_api_token(appkey, secret)
print("OK: API token acquired\n")

# 차트 데이터 조회
ticker = "064260"
df_chart = fetch_chart_data(token, ticker, days=60)

if df_chart.empty:
    print("❌ 차트 데이터 없음")
    sys.exit(1)

print(f"✓ 차트 데이터 {len(df_chart)}일 조회 완료\n")

# 최신 데이터
latest = df_chart.iloc[-1]
close = latest["종가"]
low = latest["저가"]
high = latest["고가"]
date_str = latest["날짜"].strftime("%Y-%m-%d")

print(f"📅 데이터 날짜: {date_str}")
print(f"종가: {close:,.0f}원")
print(f"저가: {low:,.0f}원")
print(f"고가: {high:,.0f}원\n")

# 당일 기준 계산
ma20_today = calculate_ma(df_chart, 20)
S20_today = ma20_today * 20
Close_D_20 = df_chart.iloc[-20]["종가"]
S19_today = S20_today - Close_D_20

buy1_today = calculate_buy_line_1(S19_today)
buy2_today = calculate_buy_line_2(buy1_today)
buy3_today = calculate_buy_line_3(buy2_today)

print("=" * 80)
print("당일(D일) 기준 계산")
print("=" * 80)
print(f"20일선: {ma20_today:,.0f}원")
print(f"S20_today: {S20_today:,.0f}")
print(f"Close_D_20: {Close_D_20:,.0f}")
print(f"S19_today: {S19_today:,.0f}")
print(f"\n1차 매수선: {buy1_today:,.0f}원")
print(f"2차 매수선: {buy2_today:,.0f}원")
print(f"3차 매수선: {buy3_today:,.0f}원\n")

# 당일 체결 여부 확인
print("=" * 80)
print("당일 체결 여부 확인")
print("=" * 80)
print(f"저가({low:,.0f}원) vs 1차매수선({buy1_today:,.0f}원)")
if low <= buy1_today:
    print("✓ 1차 매수 체결!")
else:
    print("✗ 1차 매수 미체결")
print()

# 익일 기준 계산
S19_next = S20_today - close

buy1_next = calculate_buy_line_1(S19_next)
buy2_next = calculate_buy_line_2(buy1_next)
buy3_next = calculate_buy_line_3(buy2_next)

print("=" * 80)
print("익일(D+1일) 기준 계산")
print("=" * 80)
print(f"S19_next: {S19_next:,.0f}")
print(f"\n1차 매수선: {buy1_next:,.0f}원")
print(f"2차 매수선: {buy2_next:,.0f}원")
print(f"3차 매수선: {buy3_next:,.0f}원\n")

# 이격도 계산
dist_buy1 = ((close - buy1_next) / buy1_next) * 100

print("=" * 80)
print("익일 기준 이격도")
print("=" * 80)
print(f"종가({close:,.0f}원) vs 익일 1차매수선({buy1_next:,.0f}원)")
print(f"이격도: {dist_buy1:.1f}%")
print("=" * 80)
