"""
LinkedIn connections -> Koalake ingestion DAG.

Pulls the connected account's LinkedIn connections ("relations") from Unipile
and full-refreshes them into {koalake_catalog}.{schema}.connections. Connections
are a full snapshot, so the load is a CREATE OR REPLACE (idempotent: re-running
replaces, never duplicates).

Follows the Koalake connector convention from
Koalitix/koalake-matomo-dashboard: DAG in airflow/dags/, shared code in
airflow/src/, config via Koalake project variables (with env fallback for local
dev). The koalake DuckDB extension + the airflow provider are supplied by the
Koalake deployment image.

Required Koalake project variables:
  koalake_catalog     e.g. linkedin_unipile
  koalake_token       a Koalake personal access token (npat_...)
  unipile_api_key     Unipile API key
  unipile_account_id  the connected LinkedIn account id
Optional: koalake_schema (default main), koalake_endpoint, unipile_dsn.
"""
from airflow.sdk import dag, task

from datetime import datetime, timedelta
import logging
import os
import sys

# Make airflow/src importable (same pattern as internal-koalake / matomo).
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/.."))

import pandas as pd

from src import koalake_io
from src.connector_env import (
    DATABASE_CATALOG, DATABASE_SCHEMA,
    UNIPILE_API_KEY, UNIPILE_DSN, UNIPILE_ACCOUNT_ID,
    alerting_default_args, connect_koalake,
)
from src.unipile_client import UnipileClient, relation_to_row, message_to_row

CONNECTIONS_TABLE = "connections"
MESSAGES_TABLE = "messages"

CONNECTION_COLUMNS = [
    "member_id", "first_name", "last_name", "headline",
    "public_identifier", "public_profile_url", "profile_picture_url",
    "connection_urn", "connected_at", "ingested_at",
]
MESSAGE_COLUMNS = [
    "message_id", "chat_id", "account_id", "sender_id", "is_sender", "text",
    "message_type", "attendee_type", "seen", "delivered", "edited", "deleted",
    "has_attachments", "sent_at", "ingested_at",
]

default_args = {
    "owner": "natalie",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    **alerting_default_args(),
}


@dag(
    dag_id="linkedin_connections",
    default_args=default_args,
    description="Ingest LinkedIn connections from Unipile into Koalake",
    schedule="0 3 * * *",  # daily at 03:00
    catchup=False,
    max_active_runs=1,
    tags=["linkedin", "unipile", "koalake", "ingestion"],
)
def linkedin_connections():

    @task
    def sync_connections() -> dict:
        if not UNIPILE_API_KEY:
            raise ValueError("unipile_api_key is not configured. Set the project variable.")
        if not UNIPILE_ACCOUNT_ID:
            raise ValueError("unipile_account_id is not configured. Set the project variable.")

        ingested_at = koalake_io.utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")

        client = UnipileClient(dsn=UNIPILE_DSN, api_key=UNIPILE_API_KEY)
        rows = [relation_to_row(r, ingested_at) for r in client.iter_relations(UNIPILE_ACCOUNT_ID)]
        logging.info(f"Fetched {len(rows)} connection(s) from Unipile")

        df = pd.DataFrame(rows, columns=[
            "member_id", "first_name", "last_name", "headline",
            "public_identifier", "public_profile_url", "profile_picture_url",
            "connection_urn", "connected_at", "ingested_at",
        ])
        # Typed columns so the koalake table gets TIMESTAMP, not VARCHAR.
        df["connected_at"] = pd.to_datetime(df["connected_at"])
        df["ingested_at"] = pd.to_datetime(df["ingested_at"])

        con = connect_koalake()
        try:
            written = koalake_io.write_snapshot(
                con, df, DATABASE_CATALOG, DATABASE_SCHEMA, CONNECTIONS_TABLE
            )
        finally:
            con.close()

        if written != len(rows):
            raise RuntimeError(f"Wrote {written} rows but fetched {len(rows)}")
        return {
            "fetched": len(rows),
            "written": written,
            "table": f"{DATABASE_CATALOG}.{DATABASE_SCHEMA}.{CONNECTIONS_TABLE}",
        }

    @task
    def sync_messages() -> dict:
        if not UNIPILE_API_KEY:
            raise ValueError("unipile_api_key is not configured. Set the project variable.")
        if not UNIPILE_ACCOUNT_ID:
            raise ValueError("unipile_account_id is not configured. Set the project variable.")

        ingested_at = koalake_io.utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")

        client = UnipileClient(dsn=UNIPILE_DSN, api_key=UNIPILE_API_KEY)
        rows = [message_to_row(m, ingested_at) for m in client.iter_messages(UNIPILE_ACCOUNT_ID)]
        logging.info(f"Fetched {len(rows)} message(s) from Unipile")

        df = pd.DataFrame(rows, columns=MESSAGE_COLUMNS)
        # Typed columns so the koalake table gets TIMESTAMP, not VARCHAR.
        df["sent_at"] = pd.to_datetime(df["sent_at"])
        df["ingested_at"] = pd.to_datetime(df["ingested_at"])

        con = connect_koalake()
        try:
            written = koalake_io.write_snapshot(
                con, df, DATABASE_CATALOG, DATABASE_SCHEMA, MESSAGES_TABLE
            )
        finally:
            con.close()

        if written != len(rows):
            raise RuntimeError(f"Wrote {written} rows but fetched {len(rows)}")
        return {
            "fetched": len(rows),
            "written": written,
            "table": f"{DATABASE_CATALOG}.{DATABASE_SCHEMA}.{MESSAGES_TABLE}",
        }

    # Independent full-refresh loads — connections and messages don't depend on each other.
    sync_connections()
    sync_messages()


dag_obj = linkedin_connections()