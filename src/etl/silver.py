"""Silver: type normalization, side mapping, dedupe, quality flags."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def normalize_side(col):
    c = F.upper(F.trim(col))
    return (
        F.when(c.isin("BUY", "B"), "BUY")
        .when(c.isin("SELL", "S"), "SELL")
        .otherwise(F.lit(None).cast("string"))
    )


def clean_trades(bronze: DataFrame) -> DataFrame:
    price_clean = F.regexp_replace(F.col("price"), ",", "")
    qty_num = F.col("qty").cast(T.DoubleType())
    price_num = price_clean.cast(T.DoubleType())

    typed = (
        bronze.withColumn("trade_id", F.trim(F.col("trade_id")))
        .withColumn("symbol", F.upper(F.trim(F.col("symbol"))))
        .withColumn("side_norm", normalize_side(F.col("side")))
        .withColumn("qty_num", qty_num)
        .withColumn("price_num", price_num)
        .withColumn("venue", F.upper(F.trim(F.col("venue"))))
        .withColumn("trade_ts_parsed", F.to_timestamp("trade_ts", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("trade_date", F.to_date("trade_ts_parsed"))
        .withColumn(
            "notional_calc",
            F.round(F.col("qty_num") * F.col("price_num"), 4),
        )
    )

    flagged = (
        typed.withColumn(
            "is_valid",
            F.col("trade_id").isNotNull()
            & (F.length(F.col("trade_id")) > 0)
            & F.col("symbol").isNotNull()
            & F.col("side_norm").isNotNull()
            & F.col("qty_num").isNotNull()
            & (F.col("qty_num") > 0)
            & F.col("price_num").isNotNull()
            & (F.col("price_num") > 0)
            & F.col("trade_ts_parsed").isNotNull()
            & F.col("venue").isNotNull()
            & (F.length(F.col("venue")) > 0),
        )
        .withColumn(
            "dq_reason",
            F.when(F.col("is_valid"), F.lit(None).cast("string")).otherwise(
                F.concat_ws(
                    ",",
                    F.when(F.col("side_norm").isNull(), F.lit("bad_side")),
                    F.when(
                        F.col("qty_num").isNull() | (F.col("qty_num") <= 0),
                        F.lit("bad_qty"),
                    ),
                    F.when(
                        F.col("price_num").isNull() | (F.col("price_num") <= 0),
                        F.lit("bad_price"),
                    ),
                    F.when(F.col("trade_ts_parsed").isNull(), F.lit("bad_ts")),
                    F.when(
                        F.col("venue").isNull() | (F.length(F.col("venue")) == 0),
                        F.lit("bad_venue"),
                    ),
                )
            ),
        )
    )

    valid = flagged.filter(F.col("is_valid"))
    w = Window.partitionBy("trade_id").orderBy(F.col("ingest_ts").desc())
    deduped = (
        valid.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    return deduped.select(
        "trade_id",
        F.col("trade_ts_parsed").alias("trade_ts"),
        "trade_date",
        "symbol",
        F.col("side_norm").alias("side"),
        F.col("qty_num").cast(T.LongType()).alias("qty"),
        F.col("price_num").alias("price"),
        "venue",
        "account_id",
        "trader_id",
        F.col("notional_calc").alias("notional"),
        "source_file",
        "ingest_ts",
        "source_system",
        "is_valid",
        "dq_reason",
    )


def write_silver(df: DataFrame, out_path: str) -> DataFrame:
    (
        df.write.mode("overwrite")
        .partitionBy("trade_date")
        .option("compression", "snappy")
        .parquet(out_path)
    )
    return df
