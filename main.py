# ------------------------------
# main.py
# ------------------------------
from __future__ import annotations
import sys
import yaml
from pathlib import Path
from datetime import datetime
import sqlite3
import pandas as pd

from db_utils import ensure_dirs, get_conn, init_db, upsert_many
from core import load_replay_daily, split_leaders_and_universe, build_rows_for_tables, TURNOVER_THRESHOLD_EOK


DEFAULT_CFG = {
    "mode": "replay",  # replay | normal
    "test_date": "2025-09-30",
    "paths": {
        "db": "/Users/lll/Documents/Macoding/S12/s2.sqlite",
        "backup": "/Users/lll/Documents/Macoding/S12/backup/",
        "replay_csv": "/Users/lll/Documents/Macoding/S12/replay/ohlcv_2025-09-30.csv",
    },
    "seed_days": 0,
}


def load_config(cfg_path: Path) -> dict:
    if not cfg_path.exists():
        return DEFAULT_CFG
    with cfg_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    # shallow merge defaults
    def merge(d, default):
        out = default.copy()
        for k, v in d.items():
            if isinstance(v, dict) and k in out and isinstance(out[k], dict):
                out[k] = merge(v, out[k])
            else:
                out[k] = v
        return out
    return merge(data, DEFAULT_CFG)


def increment_times_above_5k(conn: sqlite3.Connection, wuniv_rows: list, today_leaders: pd.DataFrame, date_str: str) -> int:
    # read existing to add counts
    cur = conn.cursor()
    existing = {row[0]: row for row in cur.execute("SELECT ticker, times_above_5k, first_seen FROM watch_universe")}
    out = []
    leaders_set = set(today_leaders["ticker"].tolist())
    for r in wuniv_rows:
        t = r["ticker"]
        prev = existing.get(t)
        prev_cnt = prev[1] if prev else 0
        add = 1 if t in leaders_set else 0
        r["times_above_5k"] = prev_cnt + add
        # preserve first_seen if exists
        if prev and prev[2]:
            r["first_seen"] = prev[2]
        # last_seen only if leader today
        if t not in leaders_set:
            r["last_seen"] = prev[2] if prev else None
        out.append(r)
    return upsert_many(conn, "watch_universe", out)


def backup_today_csv(df_leaders: pd.DataFrame, backup_dir: Path, date_str: str) -> None:
    backup_dir.mkdir(parents=True, exist_ok=True)
    out = backup_dir / f"leaders_{date_str.replace('-', '')}.csv"
    cols = ["date","ticker","name","market","close","volume","turnover_억원"]
    df_leaders[cols].to_csv(out, index=False, encoding="utf-8-sig")


def run_replay(cfg: dict) -> None:
    date_str = cfg["test_date"]
    paths = cfg["paths"]
    db_path = Path(paths["db"]).expanduser()
    backup_dir = Path(paths["backup"]).expanduser()
    replay_csv = Path(paths.get("replay_csv", "")).expanduser()

    ensure_dirs(db_path.parent, backup_dir)

    # load csv
    df = load_replay_daily(replay_csv)
    if df["date"].nunique() != 1 or df["date"].iloc[0] != date_str:
        raise ValueError(f"CSV date mismatch. expected {date_str}, got {sorted(df['date'].unique())}")

    leaders, universe = split_leaders_and_universe(df, date_str)

    # build rows
    lhist, levents, wuniv, pdaily = build_rows_for_tables(leaders, universe, mktcaps=None, date_str=date_str)

    # db ops
    conn = get_conn(db_path)
    init_db(conn)

    n1 = upsert_many(conn, "leaders_history", lhist)
    n2 = upsert_many(conn, "leaders_events", levents)
    n3 = upsert_many(conn, "prices_daily", pdaily)
    n4 = increment_times_above_5k(conn, wuniv, leaders, date_str)

    # backup leaders snapshot
    backup_today_csv(leaders, backup_dir, date_str)

    # summary
    print("==== REPLAY SUMMARY ====")
    print(f"date              : {date_str}")
    print(f"leaders threshold : {TURNOVER_THRESHOLD_EOK} 억원")
    print(f"leaders count     : {len(leaders)} / universe: {len(universe)}")
    print(f"upsert leaders_history : {n1}")
    print(f"upsert leaders_events  : {n2}")
    print(f"upsert prices_daily    : {n3}")
    print(f"upsert watch_universe  : {n4}")


def main():
    root = Path(__file__).resolve().parent
    cfg_path = root / "config.yml"
    cfg = load_config(cfg_path)

    mode = cfg.get("mode", "replay").lower()
    if mode == "replay":
        run_replay(cfg)
    else:
        raise NotImplementedError("normal(OpenAPI) mode will be added in Phase 1.0 (Windows + Kiwoom)")


if __name__ == "__main__":
    main()