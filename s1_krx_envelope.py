"""
S1 KRX Fetch & Envelope (v1.1.4)
- Step 1: 기준일 탐색(시총 + OHLCV 모두 유효한 최근 영업일)
- Step 2: 해당 기준일의 시가총액 기준 대상 리스트 생성 (≥ 1.3조, ≥ 5조 플래그)
- Step 3: 대상 종목만 '넉넉히' 수집 → 20MA 및 ±20% 엔벨로프 계산
- Step 4: 저장 직전에 종목별 최근 N(기본 120)봉만 '표시'로 슬라이스
- Step 5: raw data에 buy1~3, pos_close/pos_low, gap_close_pct/gap_low_pct(문자열 %) 추가
- Step 6: CSV 저장 (utf-8-sig)
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
import time
import warnings

import numpy as np
import pandas as pd
from pykrx import stock

# pykrx 경고 억제 (원치 않으면 주석 처리)
warnings.filterwarnings("ignore", category=UserWarning, module="pykrx")
warnings.filterwarnings("ignore", category=FutureWarning, module="pykrx")

MIN_MCAP = 1.3e12   # 1조 3천억
FLAG_MCAP = 5.0e12  # 5조 (플래그)


# -------------------------
# 1) 기준일 탐색 (시총 + OHLCV 동시 유효)
# -------------------------
def find_latest_trading_date_with_ohlcv(max_back_days: int = 30, probe_ticker: str = "005930") -> str:
    """오늘부터 과거로 역탐색하며 시총(cap)과 OHLCV가 동시에 존재하는 가장 최신 거래일(YYYYMMDD)을 반환."""
    base = datetime.now().date()
    for i in range(max_back_days + 1):
        d = base - timedelta(days=i)
        ds = d.strftime("%Y%m%d")
        try:
            cap = stock.get_market_cap_by_ticker(ds)
            if cap is None or cap.empty:
                continue
            ohlcv = stock.get_market_ohlcv_by_date(ds, ds, probe_ticker)
            if ohlcv is not None and not ohlcv.empty:
                return ds
        except Exception:
            continue
    raise RuntimeError("최근 기간 내에 cap+OHLCV가 모두 유효한 기준일을 찾지 못했습니다.")


# -------------------------
# 2) 대상 리스트업 (시총 필터)
# -------------------------
def build_target_list(ref_yyyymmdd: str) -> pd.DataFrame:
    """시총 ≥ 1.3조, ≥ 5조 플래그 추가. KOSPI/KOSDAQ 전체에서 필터."""
    cap = stock.get_market_cap_by_ticker(ref_yyyymmdd)
    if cap is None or cap.empty:
        raise RuntimeError(f"시총 데이터가 비어 있습니다: {ref_yyyymmdd}")

    cap = cap.reset_index().rename(columns={"티커": "ticker"})
    cap = cap[["ticker", "시가총액"]].copy().rename(columns={"시가총액": "market_cap"})
    cap["name"] = cap["ticker"].apply(stock.get_market_ticker_name)

    cap = cap.loc[cap["market_cap"] >= MIN_MCAP].copy()
    cap["is_ge_5trn"] = cap["market_cap"] >= FLAG_MCAP

    cap = cap.sort_values(["is_ge_5trn", "market_cap"], ascending=[False, False]).reset_index(drop=True)
    return cap


# -------------------------
# 3) 일봉 수집 + 엔벨로프 계산
# -------------------------
def fetch_ohlcv(ticker: str, start: str, end: str) -> pd.DataFrame:
    df = stock.get_market_ohlcv_by_date(start, end, ticker)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index().rename(
        columns={
            "날짜": "date",
            "시가": "open",
            "고가": "high",
            "저가": "low",
            "종가": "close",
            "거래량": "volume",
        }
    )
    df["ticker"] = ticker
    try:
        df["name"] = stock.get_market_ticker_name(ticker)
    except Exception:
        df["name"] = ""
    return df[["date", "ticker", "name", "open", "high", "low", "close", "volume"]]


def add_ma_envelope(df: pd.DataFrame, ma_window: int = 20, band_pct: float = 0.20) -> pd.DataFrame:
    if df.empty:
        return df

    def _calc(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("date").copy()
        g["ma20"] = g["close"].rolling(ma_window, min_periods=ma_window).mean()
        g["env_upper"] = g["ma20"] * (1 + band_pct)
        g["env_lower"] = g["ma20"] * (1 - band_pct)
        return g

    return df.groupby("ticker", group_keys=False).apply(_calc)


# -------------------------
# 4) 메인
# -------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="S1 KRX Fetch & Envelope v1.1.4")
    ap.add_argument("--outdir", required=True, help="출력 폴더 경로")
    ap.add_argument("--days", type=int, default=120, help="표시할 최근 거래일 수 (기본 120)")
    ap.add_argument("--band", type=float, default=0.20, help="엔벨로프 밴드 폭 (예: 0.20 = ±20%)")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # 기준일 확정
    ref = find_latest_trading_date_with_ohlcv(max_back_days=30)
    print(f"[S1] 기준일(ref) 확정 = {ref}")

    # 대상 리스트업
    targets = build_target_list(ref)
    print(f"[S1] 대상종목 = {len(targets)}개  샘플티커: {targets['ticker'].head(5).tolist()}")

    tgt_path = outdir / f"s1_targets_{ref}.csv"
    targets.to_csv(tgt_path, index=False, encoding="utf-8-sig")

    # OHLCV 수집: 계산 안정성을 위해 표시일수의 2배(최소 60달력일)로 버퍼 확보
    ref_dt = datetime.strptime(ref, "%Y%m%d").date()
    start_dt = ref_dt - timedelta(days=max(60, int(args.days * 2.0)))  # 달력일 기준 버퍼
    start = start_dt.strftime("%Y%m%d")

    frames: List[pd.DataFrame] = []
    fail = 0
    for i, tkr in enumerate(targets["ticker"].tolist(), 1):
        try:
            df = fetch_ohlcv(tkr, start, ref)
            if not df.empty:
                frames.append(df)
            else:
                print(f"[WARN] OHLCV empty: {tkr}")
        except Exception as e:
            fail += 1
            print(f"[ERR] {tkr}: {e}")
        if i % 20 == 0:
            print(f"[S1] 진행 {i}/{len(targets)}")
        time.sleep(0.25)  # 레이트리밋 완화

    print(f"[S1] OHLCV 성공 {len(frames)}종 / 실패 {fail}종")
    if not frames:
        raise RuntimeError("대상 종목 OHLCV를 수집하지 못했습니다.")

    # 엔벨로프 계산 (풀히스토리로 MA/밴드 먼저 계산)
    ohlcv_all = pd.concat(frames, ignore_index=True)
    env = add_ma_envelope(ohlcv_all, ma_window=20, band_pct=args.band)

    # === 표시 슬라이스: 종목별 최근 N(=args.days) 봉만 남김 (계산은 위에서 이미 전체 구간으로 수행) ===
    env = env.groupby("ticker", group_keys=False).apply(lambda g: g.sort_values("date").tail(args.days))

    # ===== 추가 컬럼 (buy1~3, pos, gap) =====
    # 매수선
    env["buy1"] = env["env_lower"]
    env["buy2"] = env["buy1"] * 0.9
    env["buy3"] = env["buy2"] * 0.9

    # 라벨링 함수 (pos_close/pos_low 동일 기준, ma20 없으면 공란)
    def label_stage(price: float, b1: float, b2: float, b3: float) -> str:
        if pd.isna(price) or pd.isna(b1) or pd.isna(b2) or pd.isna(b3):
            return ""
        if price >= b1:
            return "1차 매수 대기"
        elif price >= b2:
            return "2차 매수 대기"
        elif price >= b3:
            return "3차 매수 대기"
        else:
            return "3차 매수 완료"

    env["pos_close"] = env.apply(
        lambda r: label_stage(r["close"], r["buy1"], r["buy2"], r["buy3"]) if pd.notna(r.get("ma20")) else "",
        axis=1,
    )
    env["pos_low"] = env.apply(
        lambda r: label_stage(r["low"], r["buy1"], r["buy2"], r["buy3"]) if pd.notna(r.get("ma20")) else "",
        axis=1,
    )

    # gap 계산 (문자열 % 표기, 절댓값, 소수 1자리)
    def gap_to_next(price: float, stage: str, b1: float, b2: float, b3: float) -> str:
        if pd.isna(price) or pd.isna(b1) or pd.isna(b2) or pd.isna(b3) or not stage:
            return ""
        if stage == "1차 매수 대기":
            target = b1
        elif stage == "2차 매수 대기":
            target = b2
        else:  # "3차 매수 대기" 또는 "3차 매수 완료"는 모두 3차 매수선 기준
            target = b3
        pct = abs((target - price) / target * 100.0)
        return f"{pct:.1f}%"

    env["gap_close_pct"] = env.apply(
        lambda r: gap_to_next(r["close"], r["pos_close"], r["buy1"], r["buy2"], r["buy3"]),
        axis=1,
    )
    env["gap_low_pct"] = env.apply(
        lambda r: gap_to_next(r["low"], r["pos_low"], r["buy1"], r["buy2"], r["buy3"]),
        axis=1,
    )

    # ===== 저장 직전 보강/정리/저장 =====
    env_path = outdir / f"s1_envelope_{ref}.csv"

    # name 컬럼 보강 (없으면 티커→이름 매핑)
    if "name" not in env.columns:
        try:
            from pykrx import stock as _stock
            env["name"] = env["ticker"].map(lambda t: _stock.get_market_ticker_name(t) or "")
        except Exception:
            env["name"] = ""

    # 컬럼 순서 안전 재정렬 (있는 컬럼만 사용)
    preferred = [
        "date", "ticker", "name", "open", "high", "low", "close", "volume",
        "ma20", "env_upper", "env_lower", "buy1", "buy2", "buy3",
        "pos_close", "pos_low", "gap_close_pct", "gap_low_pct"
    ]
    existing = [c for c in preferred if c in env.columns]
    others   = [c for c in env.columns if c not in existing]
    env = env[existing + others]

    # CSV 저장 (엑셀 한글 깨짐 방지)
    env.to_csv(env_path, index=False, encoding="utf-8-sig")

    print(f"[S1] 대상 리스트: {tgt_path}")
    print(f"[S1] 엔벨로프 데이터(+buy1~3,pos,gap% | 최근 {args.days}봉 표시): {len(env):,} rows → {env_path}")


if __name__ == "__main__":
    main()
