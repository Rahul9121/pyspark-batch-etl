"""SQL reconciliation checks across bronze / silver / gold."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def run_reconciliation(
    spark: SparkSession,
    bronze: DataFrame,
    silver: DataFrame,
    gold_trades: DataFrame,
    rejected_estimate: int | None = None,
) -> list[dict]:
    bronze.createOrReplaceTempView("bronze_trades")
    silver.createOrReplaceTempView("silver_trades")
    gold_trades.createOrReplaceTempView("gold_trades")

    checks: list[dict] = []

    bronze_cnt = spark.sql("SELECT COUNT(*) AS c FROM bronze_trades").collect()[0]["c"]
    silver_cnt = spark.sql("SELECT COUNT(*) AS c FROM silver_trades").collect()[0]["c"]
    gold_cnt = spark.sql("SELECT COUNT(*) AS c FROM gold_trades").collect()[0]["c"]
    bronze_distinct = spark.sql(
        "SELECT COUNT(DISTINCT trade_id) AS c FROM bronze_trades"
    ).collect()[0]["c"]
    silver_distinct = spark.sql(
        "SELECT COUNT(DISTINCT trade_id) AS c FROM silver_trades"
    ).collect()[0]["c"]

    checks.append(
        {
            "name": "bronze_row_count",
            "metric": int(bronze_cnt),
            "expected": "raw ingest rows",
            "passed": bronze_cnt > 0,
            "detail": f"bronze={bronze_cnt}",
        }
    )
    checks.append(
        {
            "name": "silver_lte_bronze_distinct",
            "metric": int(silver_cnt),
            "expected": "silver <= distinct bronze trade_ids",
            "passed": silver_cnt <= bronze_distinct,
            "detail": f"silver={silver_cnt}, bronze_distinct={bronze_distinct}",
        }
    )
    checks.append(
        {
            "name": "silver_unique_trade_id",
            "metric": int(silver_distinct),
            "expected": "silver_cnt == distinct trade_id",
            "passed": silver_cnt == silver_distinct,
            "detail": f"cnt={silver_cnt}, distinct={silver_distinct}",
        }
    )
    checks.append(
        {
            "name": "gold_lte_silver",
            "metric": int(gold_cnt),
            "expected": "gold <= silver after business rules",
            "passed": gold_cnt <= silver_cnt,
            "detail": f"gold={gold_cnt}, silver={silver_cnt}",
        }
    )

    # notional continuity: gold gross notional matches sum of abs signed
    row = spark.sql(
        """
        SELECT
          ROUND(SUM(notional), 2) AS gross,
          ROUND(SUM(ABS(signed_notional)), 2) AS abs_signed
        FROM gold_trades
        """
    ).collect()[0]
    gross = float(row["gross"] or 0)
    abs_signed = float(row["abs_signed"] or 0)
    notional_ok = abs(gross - abs_signed) < 0.1
    checks.append(
        {
            "name": "gold_notional_identity",
            "metric": abs(gross - abs_signed),
            "expected": "sum(notional) ~= sum(abs(signed_notional))",
            "passed": notional_ok,
            "detail": f"gross={gross}, abs_signed={abs_signed}",
        }
    )

    # no null keys in gold
    null_keys = spark.sql(
        """
        SELECT COUNT(*) AS c FROM gold_trades
        WHERE trade_id IS NULL OR symbol IS NULL OR trade_date IS NULL
        """
    ).collect()[0]["c"]
    checks.append(
        {
            "name": "gold_no_null_keys",
            "metric": int(null_keys),
            "expected": 0,
            "passed": null_keys == 0,
            "detail": f"null_key_rows={null_keys}",
        }
    )

    # side domain
    bad_side = spark.sql(
        "SELECT COUNT(*) AS c FROM gold_trades WHERE side NOT IN ('BUY','SELL')"
    ).collect()[0]["c"]
    checks.append(
        {
            "name": "gold_side_domain",
            "metric": int(bad_side),
            "expected": 0,
            "passed": bad_side == 0,
            "detail": f"bad_side_rows={bad_side}",
        }
    )

    # silver symbols subset of bronze symbols (upper)
    orphan = spark.sql(
        """
        SELECT COUNT(*) AS c FROM (
          SELECT DISTINCT UPPER(TRIM(symbol)) s FROM silver_trades
          EXCEPT
          SELECT DISTINCT UPPER(TRIM(symbol)) s FROM bronze_trades
        )
        """
    ).collect()[0]["c"]
    checks.append(
        {
            "name": "silver_symbols_subset_bronze",
            "metric": int(orphan),
            "expected": 0,
            "passed": orphan == 0,
            "detail": f"orphan_symbols={orphan}",
        }
    )

    if rejected_estimate is not None:
        checks.append(
            {
                "name": "rejected_rows_tracked",
                "metric": int(rejected_estimate),
                "expected": "bronze - approx valid path",
                "passed": rejected_estimate >= 0,
                "detail": f"rejected_or_deduped_approx={rejected_estimate}",
            }
        )

    return checks
