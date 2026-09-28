"""Source File discovery; Watermark filtering is reserved for a later ticket."""

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
