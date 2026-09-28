# Local Bronze Layer ingestion

Requires Python 3.13, Postgres (the `mydb` database), and JDK 17, 21, or 25. Spark 4.2 does not support the machine's default JDK 27. Set `JAVA_HOME` to a supported JDK and put `$JAVA_HOME/bin` first on `PATH` before running the command or tests. The Spark factory sets `PYSPARK_PYTHON` to the current Python interpreter and uses UTC.

Set `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` in the environment or in `.env`. Optionally set `POSTGRES_DB` (defaults to `mydb`). Do not commit `.env`.

```bash
export JAVA_HOME="$HOME/.local/jdk/jdk-21.0.12.1+1"
export PATH="$JAVA_HOME/bin:$PATH"
uv run bronze-ingest --source-root datasets
uv run bronze-ingest --source-table cust_info --source-root datasets
uv run pytest -q
```

Each Source Table has its own folder, for example `datasets/source_crm/cust_info/cust_info.csv`. Every non-hidden CSV directly inside that folder is a Source File, regardless of its name. The command ingests all current Source Files on every invocation. It writes Parquet to `Destination/bronze/<source_system>/<source_table>/_run_id=<uuid>/` and records each Source Table Run in `platform_meta.bronze_run_log`. Use `--destination-root` and `--run-log-schema` to target alternate output locations. Incremental Watermark-based selection and recovery of failed partitions are not implemented yet; use a fresh destination for exploratory runs.
