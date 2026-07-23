"""
Koalake write layer for the Unipile connector.

Reused (trimmed) from Koalitix/koalake-matomo-dashboard's koalake_io.py so we
inherit its hard-won workarounds for the koalake DuckDB extension. We keep only
what a full-refresh snapshot loader needs: connect + write_df.

Known extension behaviour this module works around:
- plain `CREATE TABLE ... AS SELECT` creates an EMPTY table then errors —
  always use `CREATE OR REPLACE TABLE ... AS SELECT`
- `INSERT ... BY NAME` is broken — always list columns explicitly
- DML affected-row counts are unreliable — verify effects with SELECT count(*)
"""
import logging
import re
from datetime import datetime, timezone

import duckdb


def utc_now() -> datetime:
    """Naive UTC now (TIMESTAMP columns, no tz)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def duckdb_config(memory_limit: str = "2GB", threads: int = 2) -> dict:
    """duckdb.connect config that bounds each connection's footprint."""
    return {
        "allow_unsigned_extensions": True,
        "memory_limit": memory_limit,
        "threads": threads,
    }


def get_connection(catalog: str, endpoint_url: str, token: str,
                   extension_path: str = "", memory_limit: str = "2GB",
                   threads: int = 2) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with the koalake extension loaded and the catalog attached."""
    con = duckdb.connect(config=duckdb_config(memory_limit, threads))
    if extension_path:
        con.execute(f"LOAD '{extension_path}'")
    else:
        con.execute("LOAD koalake")

    # In-memory secret: ATTACH uses it on this same connection immediately.
    secret_name = "koalake_" + re.sub(r"\W", "_", catalog)
    try:
        con.execute(f"DROP PERSISTENT SECRET IF EXISTS {secret_name}")
    except duckdb.Error:
        pass
    con.execute(f"""
        CREATE OR REPLACE SECRET {secret_name} (
            TYPE koalake,
            ENDPOINT '{endpoint_url}',
            TOKEN '{token}'
        )
    """)
    con.execute(f"ATTACH 'koalake:{secret_name}' AS {catalog} (TYPE koalake, CATALOG '{catalog}')")
    # The join-order optimizer asks the koalake multi-file scan for per-column
    # distinct-count stats; on schema-evolved tables that can index past a
    # file's column vector and crash. Our writes don't need it.
    con.execute("SET disabled_optimizers = 'join_order'")
    return con


def table_exists(con, catalog: str, schema: str, table: str) -> bool:
    # information_schema is accurate even when the extension's catalog cache
    # serves phantoms at CREATE time — trust it.
    return con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [catalog, schema, table],
    ).fetchone()[0] > 0


def write_snapshot(con, df, catalog: str, schema: str, table: str) -> int:
    """
    Full-refresh a koalake table from a pandas DataFrame.

    Connections are a full snapshot each run, so we rebuild the table with
    `CREATE OR REPLACE TABLE ... AS SELECT` (plain CTAS is broken in the
    extension). Idempotent by construction: re-running replaces, never appends.
    Returns the verified row count (DML counts are unreliable — we SELECT).
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    con.execute(f"CREATE OR REPLACE TABLE {full_table_name} AS SELECT * FROM df")
    written = con.execute(f"SELECT count(*) FROM {full_table_name}").fetchone()[0]
    logging.info(f"Wrote {written} rows to {full_table_name} (full refresh)")
    return written