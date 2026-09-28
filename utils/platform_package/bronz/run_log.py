"""Postgres Run Log repository: the sole owner of Run persistence."""

import os
from contextlib import contextmanager
from datetime import datetime
from enum import StrEnum
from uuid import UUID

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import Json


class RunStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


def connection_from_environment() -> dict:
    """Load Postgres settings without exposing credentials in diagnostics."""
    load_dotenv()
    return {
        "host": os.environ["POSTGRES_HOST"],
        "port": os.environ["POSTGRES_PORT"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "dbname": os.environ.get("POSTGRES_DB", "mydb"),
        "connect_timeout": 5,
    }


class RunLogRepository:
    """Persist the lifecycle and Watermark of each Source Table Run."""

    def __init__(self, connection: dict, schema: str):
        self.connection = connection
        self.table = sql.SQL("{}.bronze_run_log").format(sql.Identifier(schema))
        self.schema = schema

    @contextmanager
    def _cursor(self):
        with psycopg2.connect(**self.connection) as connection:
            with connection.cursor() as cursor:
                yield cursor

    def ensure_exists(self):
        """Preflight Postgres and initialize the Run Log before loading Source Files."""
        with self._cursor() as cursor:
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
            cursor.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
                    run_id uuid PRIMARY KEY,
                    source_system text NOT NULL,
                    source_table text NOT NULL,
                    status text NOT NULL,
                    started_at timestamptz NOT NULL,
                    finished_at timestamptz,
                    files_loaded text[],
                    rows_read bigint,
                    rows_written bigint,
                    corrupt_rows bigint,
                    watermark_value timestamptz,
                    schema_columns jsonb,
                    schema_change jsonb,
                    error_message text
            )""").format(self.table))

    def start(self, run_id: UUID, source_system: str, source_table: str):
        """Record a RUNNING Run before touching the Bronze Layer."""
        with self._cursor() as cursor:
            cursor.execute(
                sql.SQL("INSERT INTO {} (run_id, source_system, source_table, status, started_at) VALUES (%s, %s, %s, %s, now())").format(self.table),
                (str(run_id), source_system, source_table, RunStatus.RUNNING.value),
            )

    def success(self, run_id: UUID, files: list[str], rows: int, watermark: datetime | None, columns: list[str]):
        """Finish a Run with its Source Files, counts and reached Watermark."""
        with self._cursor() as cursor:
            cursor.execute(
                sql.SQL("""UPDATE {} SET status = %s, finished_at = now(),
                    files_loaded = %s, rows_read = %s, rows_written = %s,
                    watermark_value = %s, schema_columns = %s WHERE run_id = %s""").format(self.table),
                (RunStatus.SUCCESS.value, files, rows, rows, watermark, Json(columns), str(run_id)),
            )

    def fail(self, run_id: UUID, error: str):
        """Record a failed Run without advancing its Watermark."""
        with self._cursor() as cursor:
            cursor.execute(
                sql.SQL("UPDATE {} SET status = %s, finished_at = now(), error_message = %s WHERE run_id = %s").format(self.table),
                (RunStatus.FAILED.value, error, str(run_id)),
            )
