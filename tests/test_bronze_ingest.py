import csv
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from utils.platform_package.bronz.cli import main
from utils.platform_package.bronz.ingest import ingest


def write_source_file(source_root: Path, source_system: str, source_table: str, name: str, content: str, mtime_ns: int):
    directory = source_root / source_system / source_table
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(content)
    os.utime(path, ns=(mtime_ns, mtime_ns))
    return path


def expected_watermark(mtime_ns):
    seconds, microseconds = divmod(mtime_ns // 1000, 1_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc) + timedelta(microseconds=microseconds)


def run_log_rows(settings, schema, run_ids):
    with psycopg2.connect(**settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT source_system, source_table, status, files_loaded, rows_read, rows_written, watermark_value, started_at, finished_at FROM {}.bronze_run_log WHERE run_id = ANY(%s::uuid[]) ORDER BY source_system, source_table").format(sql.Identifier(schema)),
                ([str(run_id) for run_id in run_ids],),
            )
            return cursor.fetchall()


def test_one_source_table_lands_in_bronze_with_run_log(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root = tmp_path / "source"
    destination = tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    source_file = write_source_file(source_root, "source_crm", "cust_info", "cust_info.csv", "id,name\n1,Ada\n2,Lin\n", mtime_ns)
    with source_file.open(newline="") as handle:
        expected_count = sum(1 for _ in csv.reader(handle)) - 1

    outcomes = ingest(source_root=source_root, destination_root=destination, run_log_connection=settings, run_log_schema=schema, source_table="cust_info", spark=spark)

    assert len(outcomes) == 1
    assert outcomes[0].status == "SUCCESS"
    rows = run_log_rows(settings, schema, [outcome.run_id for outcome in outcomes])
    assert len(rows) == 1
    source_system, table, status, files, rows_read, rows_written, watermark, started, finished = rows[0]
    assert (source_system, table, status, files) == ("source_crm", "cust_info", "SUCCESS", ["cust_info.csv"])
    assert (rows_read, rows_written) == (expected_count, expected_count)
    assert watermark == expected_watermark(mtime_ns)
    assert started is not None and finished is not None

    table_root = destination / source_system / table
    assert len(list(table_root.glob("_run_id=*"))) == 1
    bronze = spark.read.option("mergeSchema", "true").parquet(str(table_root))
    assert bronze.count() == expected_count
    assert bronze.schema["id"].dataType.simpleString() == "string"
    assert bronze.schema["name"].dataType.simpleString() == "string"
    for row in bronze.collect():
        assert row["_run_id"] and row["_source_system"] == source_system
        assert row["_source_table"] == table and row["_source_file"] == source_file.name
        assert row["_ingested_at"] is not None


def test_all_source_tables_ingest_current_source_files_with_mixed_headers(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root = tmp_path / "source"
    destination = tmp_path / "bronze"
    contents = {
        ("source_crm", "cust_info"): "id,name\n1,Ada\n",
        ("source_crm", "prd_info"): "id,name\n2,Bike\n",
        ("source_crm", "sales_details"): "id,amount\n3,10\n",
        ("source_erp", "CUST_AZ12"): "id,name\n4,Lin\n",
        ("source_erp", "LOC_A101"): "id,country\nA1,US\nA3,US,EXTRA\n",
        ("source_erp", "PX_CAT_G1V2"): "id,category\n5,Parts\n",
    }
    expected = {}
    first_ns = 1_700_000_100_000_000_999
    for index, ((source_system, table), content) in enumerate(contents.items()):
        path = write_source_file(source_root, source_system, table, f"{table}.csv", content, first_ns + index * 1001)
        with path.open(newline="") as handle:
            expected[table] = sum(1 for _ in csv.reader(handle)) - 1
    extra = write_source_file(source_root, "source_crm", "sales_details", "delivery.CSV", "amount,id,discount\n20,6,3\n", first_ns + 9_876_543)
    with extra.open(newline="") as handle:
        expected["sales_details"] += sum(1 for _ in csv.reader(handle)) - 1
    write_source_file(source_root, "source_crm", "sales_details", ".hidden.csv", "id,amount\n7,30\n", first_ns)
    nested = source_root / "source_crm" / "sales_details" / "nested"
    nested.mkdir()
    (nested / "unseen.csv").write_text("id\n8\n")

    outcomes = ingest(source_root=source_root, destination_root=destination, run_log_connection=settings, run_log_schema=schema, spark=spark)

    assert len(outcomes) == 6
    assert all(outcome.status == "SUCCESS" for outcome in outcomes)
    rows = run_log_rows(settings, schema, [outcome.run_id for outcome in outcomes])
    assert len(rows) == 6
    for source_system, table, status, files, rows_read, rows_written, watermark, started, finished in rows:
        assert status == "SUCCESS"
        assert rows_read == rows_written == expected[table]
        assert files == (["delivery.CSV", f"{table}.csv"] if table == "sales_details" else [f"{table}.csv"])
        expected_ns = first_ns + (9_876_543 if table == "sales_details" else list(contents).index((source_system, table)) * 1001)
        assert watermark == expected_watermark(expected_ns)
        root = destination / source_system / table
        assert len(list(root.glob("_run_id=*"))) == 1
        bronze = spark.read.option("mergeSchema", "true").parquet(str(root))
        assert bronze.count() == expected[table]
        assert all(field.dataType.simpleString() == "string" for field in bronze.schema if field.name not in ("_ingested_at", "_run_id"))
        assert all(row["_run_id"] and row["_source_file"] and row["_ingested_at"] for row in bronze.collect())
        if table == "LOC_A101":
            assert bronze.where("id = 'A3'").select("_corrupt_record").first()[0] == "A3,US,EXTRA"
        if table == "sales_details":
            values = {row["id"]: (row["amount"], row["discount"]) for row in bronze.collect()}
            assert values == {"3": ("10", None), "6": ("20", "3")}
            assert bronze.select("_ingested_at").distinct().count() == 1


def test_unknown_source_table_fails_before_creating_a_run(tmp_path, spark, run_log_db):
    settings, _ = run_log_db
    schema = f"platform_meta_test_{uuid4().hex[:12]}"
    try:
        with pytest.raises(ValueError, match="Unknown Source Table: typo"):
            ingest(tmp_path, tmp_path / "bronze", run_log_connection=settings, run_log_schema=schema, source_table="typo", spark=spark)
        with psycopg2.connect(**settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regnamespace(%s)", (schema,))
                assert cursor.fetchone()[0] is None
    finally:
        with psycopg2.connect(**settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def test_unreachable_postgres_fails_before_any_bronze_run(tmp_path, spark):
    write_source_file(tmp_path / "source", "source_crm", "cust_info", "cust_info.csv", "id\n1\n", 1_700_000_000_000_000_000)
    with pytest.raises(psycopg2.OperationalError):
        ingest(
            source_root=tmp_path / "source", destination_root=tmp_path / "bronze",
            source_table="cust_info", spark=spark,
            run_log_connection={"host": "127.0.0.1", "port": 1, "dbname": "mydb", "connect_timeout": 1},
        )
    assert not (tmp_path / "bronze").exists()


def test_first_run_creates_run_log_in_fresh_schema(tmp_path, spark, run_log_db):
    settings, _ = run_log_db
    schema = f"platform_meta_test_{uuid4().hex[:12]}"
    source_root = tmp_path / "source"
    write_source_file(source_root, "source_crm", "cust_info", "delivery.csv", "id\n1\n", 1_700_000_000_000_000_000)
    try:
        with psycopg2.connect(**settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT to_regnamespace(%s)", (schema,))
                assert cursor.fetchone()[0] is None
        outcomes = ingest(source_root=source_root, destination_root=tmp_path / "bronze", run_log_connection=settings, run_log_schema=schema, source_table="cust_info", spark=spark)
        assert len(outcomes) == 1 and outcomes[0].status == "SUCCESS"
        assert run_log_rows(settings, schema, [outcomes[0].run_id])[0][5] == 1
    finally:
        with psycopg2.connect(**settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


def test_bom_header_is_not_a_bronze_column(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root = tmp_path / "source"
    write_source_file(source_root, "source_crm", "cust_info", "delivery.csv", "\ufeffid,name\n1,Ada\n", 1_700_000_000_000_000_000)
    outcomes = ingest(source_root=source_root, destination_root=tmp_path / "bronze", run_log_connection=settings, run_log_schema=schema, source_table="cust_info", spark=spark)
    assert outcomes[0].status == "SUCCESS"
    bronze = spark.read.option("mergeSchema", "true").parquet(str(tmp_path / "bronze" / "source_crm" / "cust_info"))
    assert "\ufeffid" not in bronze.columns
    assert bronze.select("id").first()[0] == "1"


def test_cli_ingests_source_table_to_selected_destination(tmp_path, spark, run_log_db, monkeypatch, capsys):
    settings, schema = run_log_db
    source_root = tmp_path / "source"
    destination = tmp_path / "bronze"
    write_source_file(source_root, "source_crm", "cust_info", "delivery.csv", "id\n1\n", 1_700_000_000_000_000_000)
    monkeypatch.setattr(sys, "argv", ["bronze-ingest", "--source-root", str(source_root), "--destination-root", str(destination), "--run-log-schema", schema, "--source-table", "cust_info"])
    assert main() == 0
    assert "source_crm/cust_info: SUCCESS (1 rows)" in capsys.readouterr().out
    assert len(list((destination / "source_crm" / "cust_info").glob("_run_id=*"))) == 1
    with psycopg2.connect(**settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT status, rows_written FROM {}.bronze_run_log WHERE source_table = 'cust_info'").format(sql.Identifier(schema)))
            assert ("SUCCESS", 1) in cursor.fetchall()
