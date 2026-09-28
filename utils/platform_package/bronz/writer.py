"""Local Spark CSV reader and Parquet Bronze Layer writer."""

import csv
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .config import SourceTable


def read_source_files(spark: SparkSession, files: list[Path], table: SourceTable, ingested_at: datetime) -> tuple[DataFrame | None, list[str]]:
    """Read each Source File with its own string schema, preserving malformed text."""
    combined = None
    columns = []
    headers = []
    for path in sorted(files, key=lambda file: file.name):
        with path.open(newline="", encoding="utf-8-sig") as handle:
            header = next(csv.reader(handle), [])
        seen = {}
        for name in header:
            if name.startswith("_"):
                raise ValueError(f"Source File {path.name}: reserved header {name!r} starts with '_'")
            key = name.strip().casefold()
            if key in seen:
                raise ValueError(f"Source File {path.name}: colliding headers {seen[key]!r} and {name!r}")
            seen[key] = name
        headers.append((path, header))
    for path, header in headers:
        if not header:
            continue
        columns.extend(name for name in header if name not in columns)
        schema = StructType([StructField(name, StringType()) for name in header] + [StructField("_corrupt_record", StringType())])
        frame = (
            spark.read.option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(schema)
            .csv(str(path))
            .withColumn("_source_system", F.lit(table.source_system))
            .withColumn("_source_table", F.lit(table.source_table))
            .withColumn("_source_file", F.lit(path.name))
            .withColumn("_ingested_at", F.lit(ingested_at))
        )
        combined = frame if combined is None else combined.unionByName(frame, allowMissingColumns=True)
    return combined, columns


def write_bronze_run(frame: DataFrame | None, destination_root: Path, table: SourceTable, run_id: str) -> tuple[int, int]:
    """Store a Run in its own Bronze Layer partition (without a stored _run_id)."""
    if frame is None:
        return 0, 0
    frame = frame.persist()
    try:
        counts = frame.agg(F.count("*").alias("total"), F.count("_corrupt_record").alias("corrupt")).first()
        if counts.total:
            partition = destination_root / table.source_system / table.source_table / f"_run_id={run_id}"
            frame.write.mode("errorifexists").parquet(str(partition))
        return counts.total, counts.corrupt
    finally:
        frame.unpersist()
