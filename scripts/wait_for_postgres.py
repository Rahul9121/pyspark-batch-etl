"""Block until Postgres accepts connections."""
from __future__ import annotations

import os
import sys
import time

import psycopg2


def main() -> int:
    host = os.environ.get("PGHOST", "localhost")
    port = int(os.environ.get("PGPORT", "5432"))
    db = os.environ.get("PGDATABASE", "marketdata")
    user = os.environ.get("PGUSER", "etl")
    password = os.environ.get("PGPASSWORD", "etlpass")
    for i in range(40):
        try:
            conn = psycopg2.connect(
                host=host, port=port, dbname=db, user=user, password=password
            )
            conn.close()
            print(f"Postgres ready at {host}:{port}/{db}")
            return 0
        except Exception as exc:
            print(f"waiting ({i+1}/40): {exc}")
            time.sleep(1.5)
    print("Postgres did not become ready", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
