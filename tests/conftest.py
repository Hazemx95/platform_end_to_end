from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from utils.platform_package.bronz.ingest import create_spark_session
from utils.platform_package.bronz.run_log import connection_from_environment


class PostgresSettings(dict):
    def __repr__(self):
        return "<Postgres settings redacted>"


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session(disable_ui=True)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def run_log_db():
    settings = PostgresSettings(connection_from_environment())
    schema = f"platform_meta_test_{uuid4().hex[:12]}"
    try:
        yield settings, schema
    finally:
        with psycopg2.connect(**settings) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
