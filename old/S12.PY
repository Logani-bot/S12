"""
S1 Snapshot Scanner (v0.1)
- 클릭 시점 스냅샷 기준으로 S1 테이블 생성
- 입력: KRX 일봉 CSV (OHLCV, ticker, market_cap)
- 출력: 엑셀(.xlsx) 1시트 (S1_SNAPSHOT)

의존성: pandas>=2.0, numpy, openpyxl
파일 구성 가정:
  - s_core.py (S-CORE 공통 라이브러리 v0.1)
  - s1_snapshot.py (본 파일)

실행 예시:
  python s1_snapshot.py \
    --input /path/ohlcv.csv \
    --output /path/output/S1_snapshot_YYYYMMDD.xlsx

정렬 규칙:
  1) is_ge_5trn (True 우선)
  2) gap_to_A_pct (오름차순)
"""
from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd

# 내부 모듈
import s_core as SC

SHEET_NAME = "S1_SNAPSHOT"

# ===========================
# 1) 메인 로직
# ===========================

def build_s1_snapshot(df: pd.DataFrame, cfg: SC.Config) -> pd.DataFrame:
    """S1 스냅샷 테이블 생성: 시총 필터 → 지표 → 스냅샷 → A/B/C 및 괴리율 계산 → 정렬/표시용 가공."""
    # 지표
    df_i = SC.enrich_with_envelope(df, cfg)
    # 시총 필터
    df_f = SC.filter_by_market_cap(df_i, cfg)
    # 스냅샷(티커별 최신)
    snap = SC.latest_snapshot(df_f, cfg)
    # A/B/C 및 괴리율
    snap = SC.s1_compute_levels(snap, cfg)

    # 표시 컬럼 가공
    snap["mcap_억원"] = (snap[cfg.col_mcap] / 1e8).round(0).astype("Int64")
    snap["is_ge_5trn"] = snap[SC.DEF_MC_FLAG]
    # 닿음 여부 플래그(현재가가 하단 지지선 이하이면 True)
    snap["touch_env_lower"] = snap[cfg.col_close] <= snap["env_lower"]

    # 정렬
    snap = snap.sort_values(["is_ge_5trn", SC.S1_GAP_A], ascending=[False, True]).reset_index(drop=True)

    # 출력 컬럼 선정
    out_cols = [
        cfg.col_ticker,
        "mcap_억원",
        "is_ge_5trn",
        cfg.col_close,
        "ma20",
        "env_lower",             # A
        SC.S1_B,                  # B
        SC.S1_C,                  # C
        SC.S1_GAP_A,
        SC.S1_GAP_B,
        SC.S1_GAP_C,
        "touch_env_lower",
    ]
    # 누락 방지
    out_cols = [c for c in out_cols if c in snap.columns]

    out = snap[out_cols].copy()
    # 포맷(표시용): 소수점 2자리
    for col in [cfg.col_close, "ma20", "env_lower", SC.S1_B, SC.S1_C]:
        if col in out.columns:
            out[col] = out[col].round(2)
    for col in [SC.S1_GAP_A, SC.S1_GAP_B, SC.S1_GAP_C]:
        if col in out.columns:
            out[col] = out[col].round(2)

    return out


# ===========================
# 2) I/O
# ===========================

def load_input(path: Path, cfg: SC.Config) -> pd.DataFrame:
    return SC.load_prices_csv(str(path), cfg)


def save_to_excel(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=SHEET_NAME)


# ===========================
# 3) CLI
# ===========================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="S1 Snapshot Scanner")
    p.add_argument("--input", required=True, help="입력 CSV 경로 (OHLCV + ticker + market_cap)")
    p.add_argument("--output", required=False, help="출력 엑셀 경로(.xlsx). 미지정 시 날짜 기반 자동 이름")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = SC.Config()

    input_path = Path(args.input)
    if args.output:
        output_path = Path(args.output)
    else:
        stamp = datetime.now().strftime("%Y%m%d")
        output_path = input_path.parent / f"S1_snapshot_{stamp}.xlsx"

    df = load_input(input_path, cfg)
    out = build_s1_snapshot(df, cfg)
    save_to_excel(out, output_path)

    print(f"[S1] Done → {output_path}")
    print(f"rows={len(out)}  cols={len(out.columns)}")


if __name__ == "__main__":
    main()
