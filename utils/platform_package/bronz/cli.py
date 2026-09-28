"""Command-line adapter for local Bronze Layer ingestion."""

import argparse
from pathlib import Path

from .ingest import ingest
from .run_log import RunStatus


def main() -> int:
    """Ingest all current Source Files for one or all configured Source Tables."""
    parser = argparse.ArgumentParser(prog="bronze-ingest", description="Ingest current Source Files into the Bronze Layer")
    parser.add_argument("--source-table", help="Source Table name (default: all configured tables)")
    parser.add_argument("--source-root", type=Path, default=Path("datasets"), help="Source File root (default: datasets)")
    parser.add_argument("--destination-root", type=Path, default=Path("Destination/bronze"), help="Bronze Layer root (default: Destination/bronze)")
    parser.add_argument("--run-log-schema", default="platform_meta", help="Run Log schema (default: platform_meta)")
    args = parser.parse_args()
    try:
        outcomes = ingest(source_root=args.source_root, destination_root=args.destination_root, run_log_schema=args.run_log_schema, source_table=args.source_table)
    except Exception as exc:
        parser.exit(1, f"bronze-ingest: {exc}\n")
    for outcome in outcomes:
        print(f"{outcome.source_system}/{outcome.source_table}: {outcome.status.value} ({outcome.rows_written} rows)")
    if any(outcome.status != RunStatus.SUCCESS for outcome in outcomes):
        parser.exit(1, "bronze-ingest: one or more Runs failed\n")
    return 0
