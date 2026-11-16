"""Load instrument reference data into Postgres."""
from __future__ import annotations

import csv
import os
import sys
import time

import psycopg2

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_PATH = os.path.join(ROOT, "data", "ref", "instruments.csv")


def connect(retries: int = 30, delay: float = 1.0):
    host = os.environ.get("PGHOST", "localhost")
    port = int(os.environ.get("PGPORT", "5432"))
    db = os.environ.get("PGDATABASE", "marketdata")
    user = os.environ.get("PGUSER", "etl")
    password = os.environ.get("PGPASSWORD", "etlpass")
    last = None
    for _ in range(retries):
        try:
            return psycopg2.connect(
                host=host, port=port, dbname=db, user=user, password=password
            )
        except psycopg2.OperationalError as exc:
            last = exc
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Postgres: {last}")


def main() -> int:
    if not os.path.exists(CSV_PATH):
        print(f"Missing {CSV_PATH}; run generate_data first", file=sys.stderr)
        return 1
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS instruments (
            symbol VARCHAR(16) PRIMARY KEY,
            asset_class VARCHAR(32) NOT NULL,
            currency CHAR(3) NOT NULL,
            tick_size NUMERIC(12, 6) NOT NULL,
            lot_size INTEGER NOT NULL,
            exchange VARCHAR(16) NOT NULL,
            is_active BOOLEAN NOT NULL DEFAULT TRUE
        )
        """
    )
    cur.execute("TRUNCATE instruments")
    with open(CSV_PATH, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = [
            (
                r["symbol"],
                r["asset_class"],
                r["currency"],
                r["tick_size"],
                int(r["lot_size"]),
                r["exchange"],
                r["is_active"].lower() in ("1", "true", "t", "yes"),
            )
            for r in reader
        ]
    cur.executemany(
        """
        INSERT INTO instruments
            (symbol, asset_class, currency, tick_size, lot_size, exchange, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} instruments into Postgres")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
