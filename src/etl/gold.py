"""Gold: broadcast join to instruments, business rules, aggregates."""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_instruments_jdbc(spark: SparkSession, cfg: dict) -> DataFrame:
    pg = cfg["postgres"]
    url = pg.get("jdbc_url") or (
        f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['database']}"
    )
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", pg.get("table", "instruments"))
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def load_instruments_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def load_instruments(spark: SparkSession, cfg: dict) -> DataFrame:
    mode = os.environ.get("REF_SOURCE", "jdbc").lower()
    if mode == "csv":
        from src.utils.config import abspath

        return load_instruments_csv(spark, abspath(cfg, cfg["instruments_csv"]))
    try:
        return load_instruments_jdbc(spark, cfg)
    except Exception:
        # local fallback when Postgres/JDBC jar is unavailable
        from src.utils.config import abspath

        return load_instruments_csv(spark, abspath(cfg, cfg["instruments_csv"]))


def apply_business_rules(silver: DataFrame, instruments: DataFrame) -> DataFrame:
    inst = F.broadcast(
        instruments.select(
            F.upper(F.col("symbol")).alias("symbol"),
            "asset_class",
            "currency",
            F.col("tick_size").cast("double").alias("tick_size"),
            F.col("lot_size").cast("long").alias("lot_size"),
            "exchange",
            F.col("is_active").cast("boolean").alias("is_active"),
        )
    )
    joined = silver.join(inst, on="symbol", how="left")

    # business rules:
    # 1) drop inactive instruments
    # 2) qty must be multiple of lot_size when lot_size present
    # 3) signed notional: BUY positive, SELL negative
    # 4) mark late trades (trade_date after 2024-11-10 business window end)
    rules = (
        joined.filter(F.col("is_active").isNull() | F.col("is_active"))
        .withColumn(
            "lot_ok",
            F.when(F.col("lot_size").isNull(), F.lit(True)).otherwise(
                (F.col("qty") % F.col("lot_size")) == 0
            ),
        )
        .filter(F.col("lot_ok"))
        .withColumn(
            "signed_notional",
            F.when(F.col("side") == "BUY", F.col("notional")).otherwise(-F.col("notional")),
        )
        .withColumn(
            "is_late",
            F.col("trade_date") > F.lit("2024-11-10").cast("date"),
        )
        .withColumn(
            "trade_bucket",
            F.when(F.col("notional") >= 100000, F.lit("block"))
            .when(F.col("notional") >= 25000, F.lit("large"))
            .otherwise(F.lit("standard")),
        )
    )
    return rules.drop("lot_ok")


def build_gold_tables(enriched: DataFrame) -> dict[str, DataFrame]:
    trades = enriched.select(
        "trade_id",
        "trade_ts",
        "trade_date",
        "symbol",
        "asset_class",
        "currency",
        "side",
        "qty",
        "price",
        "venue",
        "account_id",
        "trader_id",
        "notional",
        "signed_notional",
        "trade_bucket",
        "is_late",
        "tick_size",
        "lot_size",
        "exchange",
    )

    daily_symbol = (
        enriched.groupBy("trade_date", "symbol", "asset_class", "side")
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("qty").alias("total_qty"),
            F.round(F.sum("notional"), 2).alias("gross_notional"),
            F.round(F.sum("signed_notional"), 2).alias("net_notional"),
            F.round(F.avg("price"), 6).alias("avg_price"),
        )
        .orderBy("trade_date", "symbol", "side")
    )

    venue_summary = (
        enriched.groupBy("venue", "asset_class")
        .agg(
            F.count("*").alias("trade_count"),
            F.round(F.sum("notional"), 2).alias("gross_notional"),
            F.countDistinct("symbol").alias("symbol_count"),
            F.countDistinct("account_id").alias("account_count"),
        )
        .orderBy(F.col("gross_notional").desc())
    )

    return {
        "trades_enriched": trades,
        "daily_symbol": daily_symbol,
        "venue_summary": venue_summary,
    }


def write_gold(tables: dict[str, DataFrame], out_root: str) -> None:
    trades = tables["trades_enriched"]
    (
        trades.write.mode("overwrite")
        .partitionBy("trade_date", "venue")
        .option("compression", "snappy")
        .parquet(f"{out_root}/trades_enriched")
    )
    tables["daily_symbol"].write.mode("overwrite").parquet(f"{out_root}/daily_symbol")
    tables["venue_summary"].write.mode("overwrite").parquet(f"{out_root}/venue_summary")
