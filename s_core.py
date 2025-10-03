"""
S-CORE 공통 라이브러리 (v0.1)
- 공통: 시세 로더(일봉), 20일선 ±20% 엔벨로프, 시가총액 필터/플래그
- S1/S2에서 공용으로 사용 (S1은 스냅샷, S2는 일상 운영)

의존성: pandas>=2.0, numpy
권장 타임존: Asia/Seoul
파일 인코딩: UTF-8

사용 예:
    from s_core import Config, load_prices_csv, enrich_with_envelope, filter_by_market_cap
    cfg = Config()
    df = load_prices_csv("/path/ohlcv.csv")
    df = enrich_with_envelope(df, cfg)
    df = filter_by_market_cap(df, cfg)
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Iterable
import pandas as pd
import numpy as np

# ===========================
# 0) 설정
# ===========================
@dataclass
class Config:
    ma_window: int = 20                # 이동평균 기간
    band_pct: float = 0.20             # 엔벨로프 밴드 폭 (±20%)
    min_mcap_won: float = 1.3e12       # S1 필터: 시총 ≥ 1.3조(원)
    highlight_mcap_won: float = 5.0e12 # 강조: 시총 ≥ 5조(원)
    tz: str = "Asia/Seoul"
    
    # 컬럼명 규약 (입력 데이터에 맞게 조정 가능)
    col_date: str = "date"
    col_open: str = "open"
    col_high: str = "high"
    col_low: str = "low"
    col_close: str = "close"
    col_volume: str = "volume"
    col_ticker: str = "ticker"
    col_mcap: str = "market_cap"      # 원화 기준 시가총액(원)

# ===========================
# 1) 데이터 로딩
# ===========================
def load_prices_csv(
    path: str,
    cfg: Optional[Config] = None,
    dtypes: Optional[dict] = None,
    parse_dates: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """CSV에서 일봉 시세 로드.
    기대 컬럼: [date, open, high, low, close, volume, ticker, market_cap]
    - date: 날짜/시각 가능(naive면 로컬 해석), 가능한 한 날짜로 파싱
    - market_cap: 원 단위 시총(숫자)
    """
    cfg = cfg or Config()
    parse_dates = parse_dates or [cfg.col_date]

    df = pd.read_csv(path, dtype=dtypes, low_memory=False)
    # 날짜 파싱
    if cfg.col_date in df.columns:
        df[cfg.col_date] = pd.to_datetime(df[cfg.col_date], errors="coerce")
    # 정렬
    df = df.sort_values([cfg.col_ticker, cfg.col_date]).reset_index(drop=True)
    return df

# ===========================
# 2) 지표 계산 (20MA ±20% 엔벨로프)
# ===========================
def enrich_with_envelope(df: pd.DataFrame, cfg: Optional[Config] = None) -> pd.DataFrame:
    """각 티커별 20일 이동평균과 엔벨로프(±band_pct) 계산.
    추가 컬럼:
      - ma20: 이동평균
      - env_upper: ma20 * (1 + band_pct)
      - env_lower: ma20 * (1 - band_pct)  # 하단 지지선
    """
    cfg = cfg or Config()
    req = [cfg.col_date, cfg.col_close, cfg.col_ticker]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"필수 컬럼 누락: {c}")

    def _calc(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        g["ma20"] = g[cfg.col_close].rolling(cfg.ma_window, min_periods=cfg.ma_window).mean()
        g["env_upper"] = g["ma20"] * (1 + cfg.band_pct)
        g["env_lower"] = g["ma20"] * (1 - cfg.band_pct)
        return g

    out = df.groupby(cfg.col_ticker, group_keys=False).apply(_calc)
    return out

# ===========================
# 3) 시가총액 필터/플래그
# ===========================
DEF_MC_FLAG = "is_ge_5trn"


def filter_by_market_cap(df: pd.DataFrame, cfg: Optional[Config] = None) -> pd.DataFrame:
    """시가총액 기준 필터(S1용).
    - 조건: market_cap ≥ 1.3조(원)
    - 플래그: market_cap ≥ 5조(원) → is_ge_5trn=True
    """
    cfg = cfg or Config()
    if cfg.col_mcap not in df.columns:
        raise ValueError(f"시가총액 컬럼 '{cfg.col_mcap}' 이(가) 없습니다.")

    f = df[cfg.col_mcap] >= cfg.min_mcap_won
    out = df.loc[f].copy()
    out[DEF_MC_FLAG] = out[cfg.col_mcap] >= cfg.highlight_mcap_won
    return out

# ===========================
# 4) 유틸리티
# ===========================
def pct_gap(a: float, b: float) -> float:
    """a와 b의 상대 괴리율(%).
    정의: (b - a) / a * 100
    """
    if a is None or b is None or pd.isna(a) or pd.isna(b) or a == 0:
        return np.nan
    return (b - a) / a * 100.0


def latest_snapshot(df: pd.DataFrame, cfg: Optional[Config] = None) -> pd.DataFrame:
    """티커별 가장 최신 일자 레코드만 반환(스냅샷용)."""
    cfg = cfg or Config()
    idx = df.groupby(cfg.col_ticker)[cfg.col_date].idxmax()
    return df.loc[idx].reset_index(drop=True)

# ===========================
# 5) S1 보조: A/B/C 레벨 산출(스냅샷)
# ===========================
S1_A = "s1_A_env_lower"
S1_B = "s1_B_minus10pct"
S1_C = "s1_C_minus10pct"
S1_GAP_A = "gap_to_A_pct"
S1_GAP_B = "gap_to_B_pct"
S1_GAP_C = "gap_to_C_pct"


def s1_compute_levels(snapshot_df: pd.DataFrame, cfg: Optional[Config] = None) -> pd.DataFrame:
    """스냅샷 데이터에서 S1 레벨 A/B/C 및 괴리율(%) 산출.
    전제: enrich_with_envelope()가 선행되어 env_lower가 존재해야 함.
      - A = env_lower
      - B = A * 0.9
      - C = B * 0.9
      - gap_to_X_pct = (X - close)/close * 100  (현재가 대비 X까지의 거리, +값이면 하방)
    """
    cfg = cfg or Config()
    need = [cfg.col_close, "env_lower"]
    for c in need:
        if c not in snapshot_df.columns:
            raise ValueError(f"S1 레벨 계산 전 '{c}' 컬럼이 필요합니다. 먼저 enrich_with_envelope() 호출 여부 확인.")

    df = snapshot_df.copy()
    df[S1_A] = df["env_lower"]
    df[S1_B] = df[S1_A] * 0.9
    df[S1_C] = df[S1_B] * 0.9
    # 현재가 대비 괴리율(양수면 아래쪽에 있음 = 더 내려가야 닿음)
    df[S1_GAP_A] = (df[S1_A] - df[cfg.col_close]) / df[cfg.col_close] * 100.0
    df[S1_GAP_B] = (df[S1_B] - df[cfg.col_close]) / df[cfg.col_close] * 100.0
    df[S1_GAP_C] = (df[S1_C] - df[cfg.col_close]) / df[cfg.col_close] * 100.0
    return df

# ===========================
# 6) 포맷팅 헬퍼
# ===========================
def format_market_cap_krw(mcap_won: float) -> str:
    """원 단위 시총을 사람이 읽기 쉽게(조/억) 표현."""
    if pd.isna(mcap_won):
        return ""
    if mcap_won >= 1e12:
        return f"{mcap_won/1e12:.2f}조"
    elif mcap_won >= 1e8:
        return f"{mcap_won/1e8:.0f}억"
    return f"{mcap_won:.0f}원"


# ===========================
# 7) 간단 검증용 유닛 테스트
# ===========================
def _self_test() -> None:
    data = {
        "date": pd.to_datetime([
            "2025-01-01","2025-01-02","2025-01-03","2025-01-04","2025-01-05",
            "2025-01-01","2025-01-02","2025-01-03","2025-01-04","2025-01-05",
        ]),
        "ticker": ["AAA"]*5 + ["BBB"]*5,
        "close": [100, 102, 101, 98, 97, 50, 52, 55, 53, 54],
        "open":  [100, 101, 101, 99, 98, 50, 51, 53, 54, 54],
        "high":  [101, 103, 102, 101, 99, 51, 53, 56, 55, 55],
        "low":   [ 99, 100, 100,  97, 96, 49, 50, 52, 52, 53],
        "volume":[1000]*10,
        "market_cap": [1.4e12]*5 + [6.0e12]*5,
    }
    df = pd.DataFrame(data)
    cfg = Config()

    df = enrich_with_envelope(df, cfg)
    df_f = filter_by_market_cap(df, cfg)
    snap = latest_snapshot(df_f, cfg)
    snap = s1_compute_levels(snap, cfg)

    assert DEF_MC_FLAG in snap.columns
    assert {S1_A, S1_B, S1_C}.issubset(snap.columns)
    assert snap.shape[0] == 2
    print("[SELF-TEST] OK — rows:", snap.shape[0])


if __name__ == "__main__":
    _self_test()

