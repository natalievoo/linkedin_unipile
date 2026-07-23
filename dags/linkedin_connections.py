"""
LinkedIn connections -> Koalake (production DAG).

Extracts the connected account's LinkedIn connections ("relations") from Unipile
on a schedule and full-refreshes them into the Koalake catalog. This is the
deploy artifact; the proven, laptop-runnable twin lives in
scripts/ingest_connections.py (same extract + load logic).

Koalake deploy notes (see koalitix-claude knowledge/airflow/airflow-koalake.md):
  * Deploy THROUGH the Koalake platform (workspace -> S3DagBundle). The AST
    rewrite turns environment_id=0 into the real env connection at deploy time;
    running this file directly (no deploy) fails at get_connection -- expected.
  * Secrets are synced Koalake Variables, read via KoalakeEnvironment.get(...).
    Nothing secret is hardcoded here.
  * Load = FULL REFRESH (create-if-missing -> delete-all -> insert). Idempotent:
    re-runs never duplicate; dropped connections fall out.
  * Verify operator import paths / constructor kwargs against the
    airflow-provider version on your platform before first deploy.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import requests
from airflow.sdk import dag, task

# Koalake airflow-provider surface (koalake.airflow_provider.*)
from koalake.airflow_provider import (
    KoalakeEnvironment,
    KoalakeQueryOperator,
    KoalakeInsertOperator,
)

CATALOG = "linkedin_unipile"
SCHEMA = "main"
TABLE = "connections"
FQTN = f"{CATALOG}.{SCHEMA}.{TABLE}"

DEFAULT_ARGS = {
    "owner": "natalie",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {FQTN} (
    member_id           VARCHAR,
    first_name          VARCHAR,
    last_name           VARCHAR,
    headline            VARCHAR,
    public_identifier   VARCHAR,
    public_profile_url  VARCHAR,
    profile_picture_url VARCHAR,
    connection_urn      VARCHAR,
    connected_at        TIMESTAMP,
    ingested_at         TIMESTAMP
)
"""


@dag(
    dag_id="linkedin_connections",
    schedule="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["linkedin", "unipile", "koalake"],
    doc_md=__doc__,
)
def linkedin_connections():
    env = KoalakeEnvironment(environment_id=0)  # rewritten to the real env at deploy

    @task
    def extract_relations() -> list[dict]:
        """Cursor-paginate Unipile relations, map to the target schema."""
        api_key = env.get("UNIPILE_API_KEY")
        dsn = env.get("UNIPILE_DSN", "api43.unipile.com:17362")
        account_id = env.get("UNIPILE_ACCOUNT_ID")

        base = f"https://{dsn}/api/v1/users/relations"
        headers = {"X-API-KEY": api_key, "accept": "application/json"}
        ingested_at = (
            datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        )

        rows: list[dict] = []
        cursor: str | None = None
        while True:
            params = {"account_id": account_id, "limit": 100}
            if cursor:
                params["cursor"] = cursor
            resp = requests.get(base, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            for r in payload.get("items", []):
                ms = r.get("created_at")
                connected_at = (
                    datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
                    .replace(tzinfo=None)
                    .isoformat(timespec="seconds")
                    if isinstance(ms, (int, float))
                    else None
                )
                rows.append(
                    {
                        "member_id": r.get("member_id"),
                        "first_name": r.get("first_name"),
                        "last_name": r.get("last_name"),
                        "headline": r.get("headline"),
                        "public_identifier": r.get("public_identifier"),
                        "public_profile_url": r.get("public_profile_url"),
                        "profile_picture_url": r.get("profile_picture_url"),
                        "connection_urn": r.get("connection_urn"),
                        "connected_at": connected_at,
                        "ingested_at": ingested_at,
                    }
                )
            cursor = payload.get("cursor")
            if not cursor or not payload.get("items"):
                break
        return rows

    # 1) table exists  2) clear it (full refresh)  3) insert the fresh pull
    create_table = KoalakeQueryOperator(
        task_id="create_table", environment=env, sql=CREATE_SQL, catalog=CATALOG
    )
    truncate = KoalakeQueryOperator(
        task_id="truncate", environment=env, sql=f"DELETE FROM {FQTN}", catalog=CATALOG
    )

    rows = extract_relations()

    load = KoalakeInsertOperator(
        task_id="load",
        environment=env,
        catalog=CATALOG,
        schema=SCHEMA,
        table=TABLE,
        rows=rows,
    )

    create_table >> truncate >> load
    rows >> load


linkedin_connections()