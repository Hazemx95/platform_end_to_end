"""Validated Source Table declarations for the Bronze Layer."""

from pydantic import BaseModel, ConfigDict


class SourceTable(BaseModel):
    model_config = ConfigDict(frozen=True)

    source_system: str
    source_table: str


SOURCE_TABLES = tuple(
    SourceTable(source_system=source_system, source_table=table)
    for source_system, tables in (
        ("source_crm", ("cust_info", "prd_info", "sales_details")),
        ("source_erp", ("CUST_AZ12", "LOC_A101", "PX_CAT_G1V2")),
    )
    for table in tables
)


def configured_source_tables(source_table: str | None = None) -> tuple[SourceTable, ...]:
    """Resolve an optional Source Table name before starting any Run."""
    if source_table is None:
        return SOURCE_TABLES
    selected = tuple(table for table in SOURCE_TABLES if table.source_table == source_table)
    if not selected:
        raise ValueError(f"Unknown Source Table: {source_table}")
    return selected
