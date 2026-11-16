"""Run bronze -> silver -> gold, reconcile, tune, emit dashboard JSON."""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.etl.bronze import ingest_trades_csv, write_bronze
from src.etl.gold import (
    apply_business_rules,
    build_gold_tables,
    load_instruments,
    write_gold,
)
from src.etl.reconcile import run_reconciliation
from src.etl.silver import clean_trades, write_silver
from src.utils.config import abspath, load_config
from src.utils.spark_session import build_spark


def _rows(df) -> list[dict]:
    return [r.asDict(recursive=True) for r in df.collect()]


def _serialize(obj):
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="replace")
    return str(obj)


def run_once(spark, cfg: dict) -> dict:
    out_root = abspath(cfg, cfg.get("output_root", "data/output"))
    bronze_path = f"{out_root}/bronze/trades"
    silver_path = f"{out_root}/silver/trades"
    gold_path = f"{out_root}/gold"

    t0 = time.perf_counter()
    bronze = ingest_trades_csv(spark, abspath(cfg, cfg["raw_csv"]))
    write_bronze(bronze, bronze_path)
    bronze_cnt = bronze.count()
    t_bronze = time.perf_counter() - t0

    t1 = time.perf_counter()
    silver = clean_trades(bronze)
    write_silver(silver, silver_path)
    silver_cnt = silver.count()
    t_silver = time.perf_counter() - t1

    t2 = time.perf_counter()
    instruments = load_instruments(spark, cfg)
    inst_cnt = instruments.count()
    enriched = apply_business_rules(silver, instruments)
    tables = build_gold_tables(enriched)
    write_gold(tables, gold_path)
    gold_cnt = tables["trades_enriched"].count()
    t_gold = time.perf_counter() - t2

    checks = run_reconciliation(
        spark,
        bronze,
        silver,
        tables["trades_enriched"],
        rejected_estimate=int(bronze_cnt - silver_cnt),
    )

    daily = tables["daily_symbol"].limit(50)
    venue = tables["venue_summary"].limit(50)
    sample_trades = tables["trades_enriched"].orderBy("trade_ts").limit(25)

    ref_mode = os.environ.get("REF_SOURCE", "jdbc")
    try:
        # detect actual source used
        _ = instruments.take(1)
    except Exception:
        ref_mode = "csv"

    return {
        "counts": {
            "bronze": int(bronze_cnt),
            "silver": int(silver_cnt),
            "gold": int(gold_cnt),
            "instruments": int(inst_cnt),
            "daily_symbol_rows": int(tables["daily_symbol"].count()),
            "venue_summary_rows": int(tables["venue_summary"].count()),
        },
        "stage_timings_sec": {
            "bronze": round(t_bronze, 3),
            "silver": round(t_silver, 3),
            "gold": round(t_gold, 3),
            "total": round(t_bronze + t_silver + t_gold, 3),
        },
        "reconciliation": checks,
        "gold_daily_symbol": _rows(daily),
        "gold_venue_summary": _rows(venue),
        "gold_trades_sample": _rows(sample_trades),
        "ref_source": ref_mode if os.environ.get("REF_SOURCE") else "jdbc_with_csv_fallback",
    }


def run_tuning(cfg: dict) -> list[dict]:
    """Compare baseline vs tuned shuffle/AQE on a gold-style aggregation."""
    results = []
    for name, opts in cfg.get("tuning", {}).items():
        spark = build_spark(
            app_name=f"tune-{name}",
            shuffle_partitions=int(opts.get("shuffle_partitions", 8)),
            aqe=bool(opts.get("aqe", True)),
        )
        spark.catalog.clearCache()
        bronze = ingest_trades_csv(spark, abspath(cfg, cfg["raw_csv"]))
        silver = clean_trades(bronze)
        instruments = load_instruments(spark, cfg)
        enriched = apply_business_rules(silver, instruments)
        # force a shuffle-heavy job
        t0 = time.perf_counter()
        out = (
            enriched.groupBy("trade_date", "symbol", "venue", "side")
            .agg({"notional": "sum", "qty": "sum", "trade_id": "count"})
            .count()
        )
        elapsed = time.perf_counter() - t0
        results.append(
            {
                "label": name,
                "shuffle_partitions": int(opts.get("shuffle_partitions", 8)),
                "aqe": bool(opts.get("aqe", True)),
                "seconds": round(elapsed, 3),
                "result_groups": int(out),
            }
        )
        spark.stop()
    return results


def _ensure_jars() -> None:
    if os.environ.get("SPARK_JARS"):
        return
    hint = ROOT / ".spark_jars"
    jar = ROOT / "jars" / "postgresql-42.7.3.jar"
    if hint.exists():
        os.environ["SPARK_JARS"] = hint.read_text(encoding="utf-8").strip()
    elif jar.exists():
        os.environ["SPARK_JARS"] = str(jar)


def main() -> int:
    _ensure_jars()
    cfg = load_config()
    spark = build_spark(
        shuffle_partitions=int(cfg.get("tuning", {}).get("tuned", {}).get("shuffle_partitions", 8)),
        aqe=True,
    )
    try:
        payload = run_once(spark, cfg)
    finally:
        spark.stop()

    tuning = run_tuning(cfg)
    payload["tuning"] = tuning
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    payload["project"] = "pyspark-batch-etl"
    payload["author"] = "Rahul Ega"

    dash_dir = ROOT / "dashboard"
    dash_dir.mkdir(parents=True, exist_ok=True)
    out_json = dash_dir / "metrics.json"

    def default(o):
        return _serialize(o)

    out_json.write_text(json.dumps(payload, indent=2, default=default), encoding="utf-8")
    print(json.dumps({"counts": payload["counts"], "tuning": tuning, "checks_passed": all(c["passed"] for c in payload["reconciliation"])}, indent=2))
    print(f"Wrote {out_json}")
    return 0 if all(c["passed"] for c in payload["reconciliation"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
