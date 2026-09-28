"""Source File discovery and incremental selection."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import SourceTable


def select_source_files(source_root: Path, table: SourceTable) -> list[Path]:
    """Select current, non-hidden CSV Source Files directly in a Source Table folder."""
    folder = source_root / table.source_system / table.source_table
    if not folder.is_dir():
        raise FileNotFoundError(f"Source Table folder does not exist: {folder}")
    return sorted(
        path for path in folder.iterdir()
        if path.is_file() and not path.name.startswith(".") and path.suffix.lower() == ".csv"
    )


def select_incremental_files(files: list[Path], watermark: datetime | None, loaded_names: set[str]) -> list[Path]:
    """Keep new names at or after the Watermark, using floored UTC microseconds."""
    if watermark is None:
        return [path for path in files if path.name not in loaded_names]
    watermark_us = (watermark - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)
    return [path for path in files if path.name not in loaded_names and path.stat().st_mtime_ns // 1000 >= watermark_us]
