"""
S1 KRX Fetch & Envelope (v1.1)
- Step 1: 기준일 탐색(시총 + OHLCV 모두 유효한 최근 영업일)
- Step 2: 해당 기준일의 시가총액 기준 대상 리스트 생성 (≥ 1.3조, ≥ 5조 플래그)
- Step 3: 대상 종목만 최근 N(기본 120)영업일 일봉 수집 → 20MA 및 ±20% 엔벨로프 계산
- Step 4: raw data에 buy1~3, pos_close/pos_low, gap% 컬럼 추가
- Step 5: CSV 저장 (utf-8-sig)
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

warnings.filterwarnings("ignore", category=UserWarning, module="pykrx")
warnings.filterwarnings("ignore", category=FutureWarning, module="pykrx")

MIN_MCAP = 1.3e12   # 1조 3천억
FLAG_MCAP = 5.0e12  # 5조 (플래그)

# -------------------------
# 1) 기준일 탐색 (시총 + OHLCV 동시 유효)
# -------------------------
def find_latest_trading_date_with_ohlcv(max_back_days: int = 30, probe_ticker: str = "005930") -> str:
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
    ap = argparse.ArgumentParser(description="S1 KRX Fetch & Envelope v1.1")
    ap.add_argument("--outdir", required=True, help="출력 폴더 경로")
    ap.add_argument("--days", type=int, default=120, help="최근 영업일 수(엔벨로프 계산용)")
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

    # OHLCV 수집
    ref_dt = datetime.strptime(ref, "%Y%m%d").date()
    start_dt = ref_dt - timedelta(days=max(60, int(args.days * 2.0)))
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
        time.sleep(0.25)

    print(f"[S1] OHLCV 성공 {len(frames)}종 / 실패 {fail}종")
    if not frames:
        raise RuntimeError("대상 종목 OHLCV를 수집하지 못했습니다.")

    ohlcv_all = pd.concat(frames, ignore_index=True)
    env = add_ma_envelope(ohlcv_all, ma_window=20, band_pct=args.band)

    # ===== 추가 컬럼 (buy1~3, pos, gap%) =====
    env["buy1"] = env["env_lower"]
    env["buy2"] = env["buy1"] * 0.9
    env["buy3"] = env["buy2"] * 0.9

    def mark_position(row, price):
        if price >= row["buy1"]:
            return "상단~1차 사이"
        elif price >= row["buy2"]:
            return "1차~2차 사이"
        elif price >= row["buy3"]:
            return "2차~3차 사이"
        else:
            return "3차 하회"

    env["pos_close"] = env.apply(lambda r: mark_position(r, r["close"]), axis=1)
    env["pos_low"]   = env.apply(lambda r: mark_position(r, r["low"]), axis=1)

    def calc_gap(row, price):
        if price >= row["buy1"]:
            return (row["buy1"]-price)/row["buy1"]*100
        elif price >= row["buy2"]:
            return (row["buy2"]-price)/row["buy2"]*100
        elif price >= row["buy3"]:
            return (row["buy3"]-price)/row["buy3"]*100
        else:
            return 0.0

    env["gap%"] = env.apply(lambda r: calc_gap(r, r["close"]), axis=1)

    # ===== 저장 =====
    env_path = outdir / f"s1_envelope_{ref}.csv"
    env.to_csv(env_path, index=False, encoding="utf-8-sig")

    print(f"[S1] 대상 리스트: {tgt_path}")
    print(f"[S1] 엔벨로프 데이터(+buy1~3,pos,gap): {len(env):,} rows → {env_path}")


if __name__ == "__main__":
    main()
