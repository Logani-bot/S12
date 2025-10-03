# ------------------------------
# db_utils.py
# ------------------------------
from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any

DDL = {
    "leaders_history": (
        """
        CREATE TABLE IF NOT EXISTS leaders_history (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            name TEXT,
            market TEXT,
            close REAL,
            volume INTEGER,
            turnover_억원 REAL,
            mktcap_억원 REAL,
            first_seen TEXT,
            last_seen TEXT,
            PRIMARY KEY(date, ticker)
        )
        """
    ),
    "leaders_events": (
        """
        CREATE TABLE IF NOT EXISTS leaders_events (
            ticker TEXT NOT NULL,
            date TEXT NOT NULL,
            turnover_억원 REAL,
            close REAL,
            high REAL,
            low REAL,
            volume INTEGER,
            PRIMARY KEY(ticker, date)
        )
        """
    ),
    "watch_universe": (
        """
        CREATE TABLE IF NOT EXISTS watch_universe (
            ticker TEXT PRIMARY KEY,
            name TEXT,
            market TEXT,
            first_seen TEXT,
            last_seen TEXT,
            times_above_5k INTEGER DEFAULT 0,
            last_turnover_억원 REAL
        )
        """
    ),
    "prices_daily": (
        """
        CREATE TABLE IF NOT EXISTS prices_daily (
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            turnover_억원 REAL,
            PRIMARY KEY(date, ticker)
        )
        """
    )
}

UPSERT_SQL = {
    "leaders_history": (
        """
        INSERT INTO leaders_history
        (date, ticker, name, market, close, volume, turnover_억원, mktcap_억원, first_seen, last_seen)
        VALUES (:date, :ticker, :name, :market, :close, :volume, :turnover_억원, :mktcap_억원, :first_seen, :last_seen)
        ON CONFLICT(date, ticker) DO UPDATE SET
            name=excluded.name,
            market=excluded.market,
            close=excluded.close,
            volume=excluded.volume,
            turnover_억원=excluded.turnover_억원,
            mktcap_억원=excluded.mktcap_억원,
            first_seen=MIN(leaders_history.first_seen, excluded.first_seen),
            last_seen=excluded.last_seen
        """
    ),
    "leaders_events": (
        """
        INSERT INTO leaders_events
        (ticker, date, turnover_억원, close, high, low, volume)
        VALUES (:ticker, :date, :turnover_억원, :close, :high, :low, :volume)
        ON CONFLICT(ticker, date) DO UPDATE SET
            turnover_억원=excluded.turnover_억원,
            close=excluded.close,
            high=excluded.high,
            low=excluded.low,
            volume=excluded.volume
        """
    ),
    "watch_universe": (
        """
        INSERT INTO watch_universe
        (ticker, name, market, first_seen, last_seen, times_above_5k, last_turnover_억원)
        VALUES (:ticker, :name, :market, :first_seen, :last_seen, :times_above_5k, :last_turnover_억원)
        ON CONFLICT(ticker) DO UPDATE SET
            name=excluded.name,
            market=excluded.market,
            first_seen=COALESCE(watch_universe.first_seen, excluded.first_seen),
            last_seen=excluded.last_seen,
            times_above_5k=excluded.times_above_5k,
            last_turnover_억원=excluded.last_turnover_억원
        """
    ),
    "prices_daily": (
        """
        INSERT INTO prices_daily
        (date, ticker, open, high, low, close, volume, turnover_억원)
        VALUES (:date, :ticker, :open, :high, :low, :close, :volume, :turnover_억원)
        ON CONFLICT(date, ticker) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            volume=excluded.volume,
            turnover_억원=excluded.turnover_억원
        """
    )
}


def ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def get_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for sql in DDL.values():
        cur.execute(sql)
    conn.commit()


def upsert_many(conn: sqlite3.Connection, table: str, rows: Iterable[Dict[str, Any]]) -> int:
    rows = list(rows)
    if not rows:
        return 0
    cur = conn.cursor()
    cur.executemany(UPSERT_SQL[table], rows)
    conn.commit()
    return cur.rowcount



