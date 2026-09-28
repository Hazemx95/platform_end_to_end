"""Public ingest entry point for Source Table Runs."""

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

from pyspark.sql import SparkSession

from .config import configured_source_tables
from .run_log import RunLogRepository, RunStatus, connection_from_environment
from .selector import select_source_files
from .writer import read_source_files, write_bronze_run


@dataclass(frozen=True)
class RunOutcome:
    """Outcome of one Source Table Run."""

    run_id: UUID
    source_system: str
    source_table: str
    status: RunStatus
    rows_written: int = 0
    error_message: str | None = None


def create_spark_session(disable_ui: bool = False) -> SparkSession:
    """Start local Spark with the same Python interpreter for driver and workers."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return (
        SparkSession.builder.master("local[*]")
        .appName("bronze-ingest")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", str(not disable_ui).lower())
        .getOrCreate()
    )


def source_file_watermark(files: list[Path]) -> datetime | None:
    """Floor Source File mtimes to UTC microseconds without floating-point rounding."""
    if not files:
        return None
    microseconds = max(path.stat().st_mtime_ns // 1000 for path in files)
    seconds, remaining = divmod(microseconds, 1_000_000)
    return datetime.fromtimestamp(seconds, tz=timezone.utc) + timedelta(microseconds=remaining)


def ingest(
    source_root: Path | str = Path("datasets"),
    destination_root: Path | str = Path("Destination/bronze"),
    run_log_connection: dict | None = None,
    run_log_schema: str = "platform_meta",
    source_table: str | None = None,
    spark: SparkSession | None = None,
) -> list[RunOutcome]:
    """Ingest all current Source Files of configured Source Tables and record each Run."""
    tables = configured_source_tables(source_table)
    repository = RunLogRepository(run_log_connection or connection_from_environment(), run_log_schema)
    repository.ensure_exists()
    spark = spark or create_spark_session()
    outcomes = []
    for table in tables:
        run_id = uuid4()
        repository.start(run_id, table.source_system, table.source_table)
        try:
            files = select_source_files(Path(source_root), table)
            frame, columns = read_source_files(spark, files, table, datetime.now(timezone.utc))
            rows = write_bronze_run(frame, Path(destination_root), table, str(run_id))
            watermark = source_file_watermark(files)
            repository.success(run_id, [path.name for path in files], rows, watermark, columns)
            outcomes.append(RunOutcome(run_id, table.source_system, table.source_table, RunStatus.SUCCESS, rows))
        except Exception as exc:
            repository.fail(run_id, str(exc))
            outcomes.append(RunOutcome(run_id, table.source_system, table.source_table, RunStatus.FAILED, error_message=str(exc)))
    return outcomes
