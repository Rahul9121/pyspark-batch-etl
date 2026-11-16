"""Download PostgreSQL JDBC driver into jars/."""
from __future__ import annotations

import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JARS = ROOT / "jars"
URL = (
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/"
    "postgresql-42.7.3.jar"
)
NAME = "postgresql-42.7.3.jar"


def main() -> int:
    JARS.mkdir(parents=True, exist_ok=True)
    dest = JARS / NAME
    if dest.exists() and dest.stat().st_size > 1000:
        print(f"JDBC jar present: {dest}")
    else:
        print(f"Downloading {URL}")
        urllib.request.urlretrieve(URL, dest)
        print(f"Saved {dest}")
    # export path hint for Spark
    os.environ["SPARK_JARS"] = str(dest)
    env_file = ROOT / ".spark_jars"
    env_file.write_text(str(dest), encoding="utf-8")
    print(f"SPARK_JARS={dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
