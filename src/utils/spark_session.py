"""Spark session factory with optional tuning knobs."""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]


def _windows_hadoop_home() -> None:
    if os.name != "nt":
        return
    if os.environ.get("HADOOP_HOME"):
        return
    candidate = ROOT / "hadoop_home"
    if (candidate / "bin").exists():
        os.environ["HADOOP_HOME"] = str(candidate)


def build_spark(
    app_name: str = "pyspark-batch-etl",
    shuffle_partitions: int | None = None,
    aqe: bool | None = None,
) -> SparkSession:
    _windows_hadoop_home()
    import sys

    # keep driver/worker on the same interpreter (Windows-friendly)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or 8))
        .config(
            "spark.sql.adaptive.enabled",
            "true" if (aqe if aqe is not None else True) else "false",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.python.worker.reuse", "true")
    )
    jars = os.environ.get("SPARK_JARS", "")
    if jars:
        builder = builder.config("spark.jars", jars)
    return builder.getOrCreate()
