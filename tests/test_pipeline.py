"""Core pipeline unit/integration tests (JVM CSV path)."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.etl.bronze import ingest_trades_csv
from src.etl.gold import apply_business_rules, build_gold_tables, load_instruments_csv
from src.etl.reconcile import run_reconciliation
from src.etl.silver import clean_trades
from src.generate_data import generate_trades, write_instruments, write_trades
from src.utils.spark_session import build_spark


@pytest.fixture(scope="module")
def spark():
    s = build_spark(app_name="pytest-etl", shuffle_partitions=4, aqe=True)
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture
def sample_csv(tmp_path):
    trades = generate_trades(
        n=200, seed=7, duplicate_rate=0.05, null_rate=0.05, bad_type_rate=0.05
    )
    tpath = tmp_path / "trades.csv"
    ipath = tmp_path / "instruments.csv"
    write_trades(tpath, trades)
    write_instruments(ipath)
    return tpath, ipath


def test_normalize_side_via_csv(spark, tmp_path):
    path = tmp_path / "sides.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["trade_id", "trade_ts", "symbol", "side", "qty", "price", "venue", "account_id", "trader_id", "notional", "source_file"])
        rows = [
            ("T1", "2024-11-01 10:00:00", "AAPL", "buy", "10", "100.0", "XNAS", "A1", "TR01", "1000", "t"),
            ("T2", "2024-11-01 10:00:00", "AAPL", "S", "10", "100.0", "XNAS", "A1", "TR01", "1000", "t"),
            ("T3", "2024-11-01 10:00:00", "AAPL", "SELL", "10", "100.0", "XNAS", "A1", "TR01", "1000", "t"),
            ("T4", "2024-11-01 10:00:00", "AAPL", "B", "10", "100.0", "XNAS", "A1", "TR01", "1000", "t"),
        ]
        w.writerows(rows)
    bronze = ingest_trades_csv(spark, str(path))
    silver = clean_trades(bronze)
    sides = sorted(r["side"] for r in silver.select("side").collect())
    assert sides == ["BUY", "BUY", "SELL", "SELL"]


def test_silver_dedupes_and_types(spark, sample_csv):
    tpath, _ = sample_csv
    bronze = ingest_trades_csv(spark, str(tpath))
    silver = clean_trades(bronze)
    assert silver.count() <= bronze.count()
    assert silver.select("trade_id").distinct().count() == silver.count()
    sides = {r["side"] for r in silver.select("side").distinct().collect()}
    assert sides.issubset({"BUY", "SELL"})


def test_gold_broadcast_and_rules(spark, sample_csv):
    tpath, ipath = sample_csv
    bronze = ingest_trades_csv(spark, str(tpath))
    silver = clean_trades(bronze)
    inst = load_instruments_csv(spark, str(ipath))
    enriched = apply_business_rules(silver, inst)
    tables = build_gold_tables(enriched)
    assert tables["trades_enriched"].count() <= silver.count()
    assert "asset_class" in tables["trades_enriched"].columns
    assert tables["venue_summary"].count() > 0


def test_reconciliation_passes(spark, sample_csv):
    tpath, ipath = sample_csv
    bronze = ingest_trades_csv(spark, str(tpath))
    silver = clean_trades(bronze)
    inst = load_instruments_csv(spark, str(ipath))
    gold = build_gold_tables(apply_business_rules(silver, inst))["trades_enriched"]
    checks = run_reconciliation(spark, bronze, silver, gold, rejected_estimate=0)
    assert all(c["passed"] for c in checks if c["name"] != "rejected_rows_tracked")


def test_generator_is_seeded():
    a = generate_trades(n=50, seed=42)
    b = generate_trades(n=50, seed=42)
    assert [r["trade_id"] for r in a] == [r["trade_id"] for r in b]
