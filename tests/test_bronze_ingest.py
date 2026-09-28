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


def run_details(settings, schema, run_ids):
    with psycopg2.connect(**settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT run_id, status, files_loaded, rows_written, watermark_value, schema_columns FROM {}.bronze_run_log WHERE run_id = ANY(%s::uuid[])").format(sql.Identifier(schema)),
                ([str(run_id) for run_id in run_ids],),
            )
            return {str(row[0]): row[1:] for row in cursor.fetchall()}


def run_corrupt_rows(settings, schema, run_ids):
    with psycopg2.connect(**settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT run_id, corrupt_rows FROM {}.bronze_run_log WHERE run_id = ANY(%s::uuid[])").format(sql.Identifier(schema)),
                ([str(run_id) for run_id in run_ids],),
            )
            return {str(run_id): count for run_id, count in cursor.fetchall()}


def run_schema(settings, schema, run_ids):
    with psycopg2.connect(**settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT run_id, schema_columns, schema_change FROM {}.bronze_run_log WHERE run_id = ANY(%s::uuid[])").format(sql.Identifier(schema)),
                ([str(run_id) for run_id in run_ids],),
            )
            return {str(run_id): (columns, change) for run_id, columns, change in cursor.fetchall()}


def test_added_and_removed_columns_are_tracked_against_previous_success(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    base = 1_700_000_000_000_000_000
    write_source_file(source_root, "source_crm", "sales_details", "first.csv", "sls_id,sls_amount\n1,10\n", base)
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "cst_id,cst_marital_status\n1,Single\n", base)
    for system, table in (("source_crm", "prd_info"), ("source_erp", "CUST_AZ12"), ("source_erp", "LOC_A101"), ("source_erp", "PX_CAT_G1V2")):
        (source_root / system / table).mkdir(parents=True)
    first = ingest(source_root, destination, settings, schema, spark=spark)
    write_source_file(source_root, "source_crm", "sales_details", "second.csv", "sls_id,sls_amount,sls_discount\n2,20,3\n", base + 1_000_000)
    write_source_file(source_root, "source_crm", "cust_info", "second.csv", "cst_id\n2\n", base + 1_000_000)

    second = ingest(source_root, destination, settings, schema, spark=spark)

    assert all(run.status == "SUCCESS" for run in first + second)
    by_table = {run.source_table: run for run in second if run.source_table in ("sales_details", "cust_info")}
    changes = run_schema(settings, schema, [run.run_id for run in first + second])
    assert all(changes[str(run.run_id)][1] is None for run in first)
    assert changes[str(by_table["sales_details"].run_id)] == (
        ["sls_id", "sls_amount", "sls_discount"], {"added": ["sls_discount"], "removed": []},
    )
    assert changes[str(by_table["cust_info"].run_id)] == (
        ["cst_id"], {"added": [], "removed": ["cst_marital_status"]},
    )
    sales = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_crm" / "sales_details"))
    assert {(row["sls_id"], row["sls_discount"]) for row in sales.collect()} == {("1", None), ("2", "3")}
    customers = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_crm" / "cust_info"))
    assert {(row["cst_id"], row["cst_marital_status"]) for row in customers.collect()} == {("1", "Single"), ("2", None)}


def test_unchanged_and_idle_runs_have_null_schema_change(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    base = 1_700_000_000_000_000_000
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id,name\n1,Ada\n", base)
    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    write_source_file(source_root, "source_crm", "cust_info", "second.csv", "name,id\nLin,2\n", base + 1_000_000)
    unchanged = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    idle = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert all(run.status == "SUCCESS" for run in (first, unchanged, idle))
    assert run_schema(settings, schema, [first.run_id, unchanged.run_id, idle.run_id]) == {
        str(run.run_id): (columns, None)
        for run, columns in ((first, ["id", "name"]), (unchanged, ["name", "id"]), (idle, ["name", "id"]))
    }
    bronze = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_crm" / "cust_info"))
    assert {(row["id"], row["name"]) for row in bronze.collect()} == {("1", "Ada"), ("2", "Lin")}


def test_mixed_headers_use_sorted_file_order_and_union_by_name(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    base = 1_700_000_000_000_000_000
    write_source_file(source_root, "source_crm", "sales_details", "z.csv", "sls_id,sls_amount\n2,20\n", base)
    write_source_file(source_root, "source_crm", "sales_details", "a.csv", "sls_discount,sls_id\n5,1\n", base + 1_000_000)

    run = ingest(source_root, destination, settings, schema, "sales_details", spark)[0]

    assert run.status == "SUCCESS", run.error_message
    assert run_schema(settings, schema, [run.run_id])[str(run.run_id)] == (["sls_discount", "sls_id", "sls_amount"], None)
    bronze = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_crm" / "sales_details"))
    assert {(row["sls_id"], row["sls_amount"], row["sls_discount"]) for row in bronze.collect()} == {
        ("1", None, "5"), ("2", "20", None),
    }


def test_renamed_column_is_one_removal_and_one_addition(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    base = 1_700_000_000_000_000_000
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id,old_name\n1,Ada\n", base)
    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    write_source_file(source_root, "source_crm", "cust_info", "second.csv", "id,new_name\n2,Lin\n", base + 1_000_000)

    renamed = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert first.status == renamed.status == "SUCCESS"
    assert run_schema(settings, schema, [renamed.run_id])[str(renamed.run_id)] == (
        ["id", "new_name"], {"added": ["new_name"], "removed": ["old_name"]},
    )
    bronze = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_crm" / "cust_info"))
    assert {(row["id"], row["old_name"], row["new_name"]) for row in bronze.collect()} == {
        ("1", "Ada", None), ("2", None, "Lin"),
    }


def test_header_names_are_preserved_without_trimming(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    write_source_file(source_root, "source_crm", "cust_info", "spaces.csv", " id ,name\n1,Ada\n", 1_700_000_000_000_000_000)

    run = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert run.status == "SUCCESS", run.error_message
    assert run_schema(settings, schema, [run.run_id])[str(run.run_id)] == ([" id ", "name"], None)
    bronze = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_crm" / "cust_info"))
    assert " id " in bronze.columns
    assert [(row[" id "], row["name"]) for row in bronze.collect()] == [("1", "Ada")]


@pytest.mark.parametrize("header,offending", [
    ("cst_id, CST_ID", ("cst_id", " CST_ID")),
    ("cst_id,_source_file", ("_source_file",)),
])
def test_ambiguous_or_reserved_header_fails_only_its_source_table(tmp_path, spark, run_log_db, header, offending):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    base = 1_700_000_000_000_000_000
    write_source_file(source_root, "source_crm", "cust_info", "a.csv", "cst_id\n0\n", base)
    write_source_file(source_root, "source_crm", "cust_info", "bad.csv", f"{header}\n1,Ada\n", base)
    write_source_file(source_root, "source_crm", "prd_info", "good.csv", "id\n2\n", base)
    for system, table in (("source_crm", "sales_details"), ("source_erp", "CUST_AZ12"), ("source_erp", "LOC_A101"), ("source_erp", "PX_CAT_G1V2")):
        (source_root / system / table).mkdir(parents=True)

    outcomes = ingest(source_root, destination, settings, schema, spark=spark)

    bad = next(run for run in outcomes if run.source_table == "cust_info")
    good = next(run for run in outcomes if run.source_table == "prd_info")
    assert bad.status == "FAILED"
    assert "bad.csv" in bad.error_message
    assert all(name in bad.error_message for name in offending)
    assert good.status == "SUCCESS", good.error_message
    with psycopg2.connect(**settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT status, error_message FROM {}.bronze_run_log WHERE run_id = %s").format(sql.Identifier(schema)), (str(bad.run_id),))
            assert cursor.fetchone() == ("FAILED", bad.error_message)
    assert run_schema(settings, schema, [good.run_id])[str(good.run_id)] == (["id"], None)
    bronze = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_crm" / "prd_info"))
    assert [row["id"] for row in bronze.collect()] == ["2"]
    assert not (destination / "source_crm" / "cust_info").exists()


def test_malformed_source_row_is_kept_and_counted_in_successful_run(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    write_source_file(source_root, "source_erp", "LOC_A101", "delivery.csv", "id,country\nA1,US\nA3,US,EXTRA\n", 1_700_000_000_123_456_789)

    run = ingest(source_root, destination, settings, schema, "LOC_A101", spark)[0]

    assert run.status == "SUCCESS", run.error_message
    assert run_corrupt_rows(settings, schema, [run.run_id]) == {str(run.run_id): 1}
    assert run_details(settings, schema, [run.run_id])[str(run.run_id)][2] == 2
    bronze = spark.read.option("mergeSchema", "true").parquet(str(destination / "source_erp" / "LOC_A101"))
    rows = {row["id"]: row for row in bronze.collect()}
    assert len(rows) == 2
    assert rows["A1"]["_corrupt_record"] is None
    assert rows["A3"]["_corrupt_record"] == "A3,US,EXTRA"
    for row in rows.values():
        assert row["_run_id"] == str(run.run_id)
        assert row["_source_system"] == "source_erp"
        assert row["_source_table"] == "LOC_A101"
        assert row["_source_file"] == "delivery.csv"
        assert row["_ingested_at"] is not None


def test_idle_run_carries_watermark_and_schema_without_partition(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root = tmp_path / "source"
    destination = tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id,name\n1,Ada\n", mtime_ns)

    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    idle = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert (first.status, idle.status, idle.rows_written) == ("SUCCESS", "SUCCESS", 0)
    details = run_details(settings, schema, [first.run_id, idle.run_id])
    assert details[str(first.run_id)] == ("SUCCESS", ["first.csv"], 1, expected_watermark(mtime_ns), ["id", "name"])
    assert details[str(idle.run_id)] == ("SUCCESS", [], 0, expected_watermark(mtime_ns), ["id", "name"])
    assert run_schema(settings, schema, [first.run_id, idle.run_id]) == {
        str(first.run_id): (["id", "name"], None),
        str(idle.run_id): (["id", "name"], None),
    }
    assert run_corrupt_rows(settings, schema, [first.run_id, idle.run_id]) == {
        str(first.run_id): 0, str(idle.run_id): 0,
    }
    table_root = destination / "source_crm" / "cust_info"
    assert len(list(table_root.glob("_run_id=*"))) == 1
    assert [(row["id"], row["_source_file"]) for row in spark.read.option("mergeSchema", "true").parquet(str(table_root)).collect()] == [("1", "first.csv")]


def test_second_all_table_run_is_idle_without_new_partitions(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    tables = {
        "source_crm": ("cust_info", "prd_info", "sales_details"),
        "source_erp": ("CUST_AZ12", "LOC_A101", "PX_CAT_G1V2"),
    }
    mtime_ns = 1_700_000_000_123_456_789
    for source_system, names in tables.items():
        for name in names:
            write_source_file(source_root, source_system, name, "delivery.csv", "id\n1\n", mtime_ns)

    first = ingest(source_root, destination, settings, schema, spark=spark)
    second = ingest(source_root, destination, settings, schema, spark=spark)

    assert len(first) == len(second) == 6
    assert all(run.status == "SUCCESS" for run in first + second)
    details = run_details(settings, schema, [run.run_id for run in first + second])
    for initial, idle in zip(first, second):
        assert details[str(initial.run_id)] == ("SUCCESS", ["delivery.csv"], 1, expected_watermark(mtime_ns), ["id"])
        assert details[str(idle.run_id)] == ("SUCCESS", [], 0, expected_watermark(mtime_ns), ["id"])
        root = destination / initial.source_system / initial.source_table
        assert len(list(root.glob("_run_id=*"))) == 1
        assert [(row["id"], row["_source_file"]) for row in spark.read.option("mergeSchema", "true").parquet(str(root)).collect()] == [("1", "delivery.csv")]


def test_new_source_file_only_advances_watermark(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    first_ns = 1_700_000_000_123_456_789
    second_ns = first_ns + 2_000_000
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id\n1\n", first_ns)
    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    write_source_file(source_root, "source_crm", "cust_info", "second.csv", "id\n2\n", second_ns)

    second = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert second.status == "SUCCESS" and second.rows_written == 1
    assert run_details(settings, schema, [second.run_id])[str(second.run_id)] == ("SUCCESS", ["second.csv"], 1, expected_watermark(second_ns), ["id"])
    root = destination / "source_crm" / "cust_info"
    assert len(list(root.glob("_run_id=*"))) == 2
    assert {(row["id"], row["_source_file"], row["_run_id"]) for row in spark.read.option("mergeSchema", "true").parquet(str(root)).collect()} == {
        ("1", "first.csv", str(first.run_id)), ("2", "second.csv", str(second.run_id))
    }


def test_touched_loaded_source_file_is_not_loaded_again(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    path = write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id\n1\n", mtime_ns)
    ingest(source_root, destination, settings, schema, "cust_info", spark)
    os.utime(path, ns=(mtime_ns + 10_000_000, mtime_ns + 10_000_000))

    idle = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert run_details(settings, schema, [idle.run_id])[str(idle.run_id)] == ("SUCCESS", [], 0, expected_watermark(mtime_ns), ["id"])
    root = destination / "source_crm" / "cust_info"
    assert len(list(root.glob("_run_id=*"))) == 1
    assert [(row["id"], row["_source_file"]) for row in spark.read.option("mergeSchema", "true").parquet(str(root)).collect()] == [("1", "first.csv")]


def test_new_source_file_at_watermark_is_loaded(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id\n1\n", mtime_ns)
    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    assert first.status == "SUCCESS", first.error_message
    write_source_file(source_root, "source_crm", "cust_info", "same-time.csv", "id\n2\n", mtime_ns)

    second = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert run_details(settings, schema, [second.run_id])[str(second.run_id)] == ("SUCCESS", ["same-time.csv"], 1, expected_watermark(mtime_ns), ["id"])
    root = destination / "source_crm" / "cust_info"
    assert len(list(root.glob("_run_id=*"))) == 2
    assert {(row["id"], row["_source_file"]) for row in spark.read.option("mergeSchema", "true").parquet(str(root)).collect()} == {("1", "first.csv"), ("2", "same-time.csv")}


def test_new_source_file_older_than_watermark_is_skipped(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id\n1\n", mtime_ns)
    ingest(source_root, destination, settings, schema, "cust_info", spark)
    write_source_file(source_root, "source_crm", "cust_info", "older.csv", "id\n2\n", mtime_ns - 1_000)

    idle = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert run_details(settings, schema, [idle.run_id])[str(idle.run_id)] == ("SUCCESS", [], 0, expected_watermark(mtime_ns), ["id"])
    root = destination / "source_crm" / "cust_info"
    assert len(list(root.glob("_run_id=*"))) == 1
    assert [(row["id"], row["_source_file"]) for row in spark.read.option("mergeSchema", "true").parquet(str(root)).collect()] == [("1", "first.csv")]


def test_two_source_files_within_one_microsecond_are_both_loaded(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_100
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id\n1\n", mtime_ns)
    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    write_source_file(source_root, "source_crm", "cust_info", "later.csv", "id\n2\n", mtime_ns + 899)

    second = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert run_details(settings, schema, [first.run_id, second.run_id])[str(second.run_id)] == ("SUCCESS", ["later.csv"], 1, expected_watermark(mtime_ns + 899), ["id"])
    root = destination / "source_crm" / "cust_info"
    assert len(list(root.glob("_run_id=*"))) == 2
    assert {(row["id"], row["_source_file"]) for row in spark.read.option("mergeSchema", "true").parquet(str(root)).collect()} == {("1", "first.csv"), ("2", "later.csv")}


def test_failed_run_does_not_advance_watermark_or_mark_source_file_loaded(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    first_ns = 1_700_000_000_123_456_000
    second_ns = first_ns + 1_000_000
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id\n1\n", first_ns)
    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    write_source_file(source_root, "source_crm", "cust_info", "retry.csv", "id\n2\n", second_ns)
    blocked_destination = tmp_path / "not-a-directory"
    blocked_destination.write_text("blocked")

    failed = ingest(source_root, blocked_destination, settings, schema, "cust_info", spark)[0]
    retried = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    assert failed.status == "FAILED"
    details = run_details(settings, schema, [first.run_id, failed.run_id, retried.run_id])
    assert details[str(first.run_id)][3] == expected_watermark(first_ns)
    assert details[str(failed.run_id)] == ("FAILED", None, None, None, None)
    assert details[str(retried.run_id)] == ("SUCCESS", ["retry.csv"], 1, expected_watermark(second_ns), ["id"])
    root = destination / "source_crm" / "cust_info"
    assert len(list(root.glob("_run_id=*"))) == 2
    assert {(row["id"], row["_source_file"]) for row in spark.read.option("mergeSchema", "true").parquet(str(root)).collect()} == {("1", "first.csv"), ("2", "retry.csv")}


def test_header_only_source_file_is_loaded_once_with_zero_rows(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    write_source_file(source_root, "source_crm", "cust_info", "header.csv", "id,name\n", mtime_ns)

    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    idle = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    details = run_details(settings, schema, [first.run_id, idle.run_id])
    assert details[str(first.run_id)] == ("SUCCESS", ["header.csv"], 0, expected_watermark(mtime_ns), ["id", "name"])
    assert details[str(idle.run_id)] == ("SUCCESS", [], 0, expected_watermark(mtime_ns), ["id", "name"])
    assert not list((destination / "source_crm" / "cust_info").glob("_run_id=*"))


def test_zero_byte_source_file_is_loaded_once_without_columns(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    write_source_file(source_root, "source_crm", "cust_info", "empty.csv", "", mtime_ns)

    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    idle = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    details = run_details(settings, schema, [first.run_id, idle.run_id])
    assert details[str(first.run_id)] == ("SUCCESS", ["empty.csv"], 0, expected_watermark(mtime_ns), [])
    assert details[str(idle.run_id)] == ("SUCCESS", [], 0, expected_watermark(mtime_ns), [])
    assert run_schema(settings, schema, [first.run_id, idle.run_id]) == {
        str(first.run_id): ([], None), str(idle.run_id): ([], None),
    }
    assert not list((destination / "source_crm" / "cust_info").glob("_run_id=*"))


def test_zero_byte_delivery_carries_previous_schema_columns(tmp_path, spark, run_log_db):
    settings, schema = run_log_db
    source_root, destination = tmp_path / "source", tmp_path / "bronze"
    mtime_ns = 1_700_000_000_123_456_789
    write_source_file(source_root, "source_crm", "cust_info", "first.csv", "id,name\n1,Ada\n", mtime_ns)
    first = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]
    write_source_file(source_root, "source_crm", "cust_info", "empty.csv", "", mtime_ns + 1_000)

    empty = ingest(source_root, destination, settings, schema, "cust_info", spark)[0]

    details = run_details(settings, schema, [first.run_id, empty.run_id])
    assert details[str(empty.run_id)] == ("SUCCESS", ["empty.csv"], 0, expected_watermark(mtime_ns + 1_000), ["id", "name"])
    assert run_schema(settings, schema, [empty.run_id])[str(empty.run_id)] == (["id", "name"], None)


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
