"""Bronze: raw ingest from CSV, minimal typing, land as Parquet."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ingest_trades_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(path)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_system", F.lit("csv_feed"))
    )


def write_bronze(df: DataFrame, out_path: str) -> DataFrame:
    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )
    return df
