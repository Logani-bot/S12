# 거래대금 5000억+ 종목 추적기

키움증권 REST API를 활용하여 매일 거래대금 5000억 이상 종목을 자동으로 수집하고 엑셀에 축적하는 스크립트입니다.

## ⚠️ 중요: API 제한사항

키움 REST API는 **과거 날짜의 거래대금 순위를 제공하지 않습니다**. `bas_dd` 파라미터에 과거 날짜를 입력해도 항상 최신 데이터만 반환합니다.

따라서 **매일 1회 실행**하여 데이터를 축적하는 방식만 가능합니다.

## 📋 주요 기능

- ✅ 키움 REST API로 최신 거래대금 순위 조회
- ✅ 5000억 이상 종목만 자동 필터링
- ✅ ETF/ETN/인버스/레버리지 자동 제외
- ✅ KRX + KOSDAQ 통합 조회
- ✅ (날짜, 티커) 중복 자동 제거
- ✅ 엑셀 자동 서식 (열 너비, 테두리)

## 🚀 사용 방법

### 기본 실행 (5000억 이상)

```bash
python Daily_Turnover_Tracker.py
```

### 임계값 변경 (예: 3000억 이상)

```bash
python Daily_Turnover_Tracker.py --threshold 3000
```

### 출력 파일명 지정

```bash
python Daily_Turnover_Tracker.py --out my_stocks.xlsx
```

### 전체 옵션

```bash
python Daily_Turnover_Tracker.py --help
```

## 📊 출력 형식

엑셀 파일(`turnover_universe.xlsx`)에 다음 형식으로 저장됩니다:

| 날짜       | 티커   | 종목명           | 거래대금(억) | 누적횟수 |
|------------|--------|------------------|--------------|----------|
| 2025-10-12 | 005930 | 삼성전자         | 50,101       | 15       |
| 2025-10-12 | 000660 | SK하이닉스       | 41,636       | 12       |
| 2025-10-11 | 035420 | NAVER            | 15,602       | 3        |
| ...        | ...    | ...              | ...          | ...      |

**동작 방식:**
- **종목별 1행만 유지** (같은 종목은 중복 저장 안 함)
- **누적횟수 카운트** - 같은 종목이 다시 5천억 이상이면 횟수만 +1
- **최신 정보 갱신** - 날짜와 거래대금은 가장 최근 값으로 자동 업데이트
- **누적횟수 내림차순 정렬** - 자주 등장한 종목이 위로

## ⏰ 자동화 방법

### Windows 작업 스케줄러

1. **작업 스케줄러** 실행 (`taskschd.msc`)
2. **기본 작업 만들기** 클릭
3. 설정:
   - 이름: `거래대금 수집`
   - 트리거: **매일** (오전 9시 이후 권장)
   - 작업: **프로그램 시작**
     - 프로그램: `python`
     - 인수: `C:\Coding\S12\Daily_Turnover_Tracker.py`
     - 시작 위치: `C:\Coding\S12`

### Linux/Mac (cron)

```bash
# crontab -e
# 매일 오전 9시 30분 실행
30 9 * * * cd /path/to/S12 && python Daily_Turnover_Tracker.py
```

## 🔧 고급 설정

### 환경변수로 API 키 관리 (보안 강화)

```bash
# Windows PowerShell
$env:KIWOOM_APPKEY = "your_app_key"
$env:KIWOOM_SECRET = "your_secret"

# Linux/Mac
export KIWOOM_APPKEY="your_app_key"
export KIWOOM_SECRET="your_secret"
```

설정 후 스크립트에서 자동으로 환경변수를 사용합니다.

### 명령행으로 API 키 지정

```bash
python Daily_Turnover_Tracker.py --appkey "YOUR_KEY" --secret "YOUR_SECRET"
```

## 📝 수동 데이터 추가

엑셀 파일에 직접 데이터를 추가할 수 있습니다:

1. `turnover_universe.xlsx` 열기
2. 마지막 행에 데이터 추가 (형식 맞춰서)
3. 저장 후 스크립트 실행 → 자동으로 중복 제거 및 정렬

## ⚙️ 시스템 요구사항

```bash
pip install requests pandas openpyxl
```

## 📂 파일 설명

- **`Daily_Turnover_Tracker.py`**: 메인 스크립트 (최적화 버전)
- **`Run_Turnover_Universe.py`**: 구버전 (복잡한 로직 포함, 백업용)
- **`test_past_rank_api.py`**: API 테스트 스크립트
- **`turnover_universe.xlsx`**: 데이터 저장 파일

## 🐛 문제 해결

### API 토큰 오류

```
RuntimeError: 토큰 획득 실패
```

→ `APPKEY`와 `SECRET`이 올바른지 확인하세요.

### Rate Limit 오류 (429)

```
Rate limit - X초 대기 중...
```

→ 자동으로 재시도합니다. 잠시 기다리세요.

### 데이터가 비어있음

```
조회된 데이터가 없습니다.
```

→ 장 마감 후(15:30 이후) 실행하세요.

## 📈 코드 최적화 내역

기존 437줄 → **391줄**로 대폭 간소화:

1. ✅ 불필요한 `reconstruct` 로직 제거 (과거 API 미지원)
2. ✅ 불필요한 차트 API 호출 제거
3. ✅ 복잡한 날짜 범위 처리 제거 (오늘만 수집)
4. ✅ 함수 구조 단순화 및 명확화
5. ✅ 주석 및 docstring 추가
6. ✅ 에러 처리 개선
7. ✅ Rate limit 처리 강화

## 💡 팁

- **장 마감 후** 실행하는 것을 권장 (15:30 이후)
- **매일 실행**이 중요합니다 (과거 데이터 조회 불가)
- 임계값은 `--threshold` 옵션으로 자유롭게 변경 가능
- 여러 날의 데이터가 필요하면 엑셀에 직접 추가 가능

## 📞 문의

문제가 발생하면 로그를 확인하세요:

```bash
python Daily_Turnover_Tracker.py --verbose
```

