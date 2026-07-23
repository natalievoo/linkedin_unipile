#!/usr/bin/env python3
"""
Standalone LinkedIn -> Koalake ingest (no Airflow required).

Pulls the connected account's LinkedIn connections ("relations") from Unipile
and full-refreshes them into a Koalake table. This is the runnable/testable
twin of dags/linkedin_connections.py -- same extract + load logic, but it uses
the already-authenticated `koalake` CLI for the write instead of the Koalake
Airflow provider, so it runs on a laptop today.

Load strategy: FULL REFRESH (delete-all + insert). Idempotent by design --
re-running never duplicates rows, and connections that disappear drop out.

Config comes from environment variables (see .env.example). Nothing secret is
hardcoded. Run:  python scripts/ingest_connections.py
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Config (env-driven; load a local .env if present, without extra deps)
# ---------------------------------------------------------------------------

def _load_dotenv() -> None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()

UNIPILE_API_KEY = os.environ.get("UNIPILE_API_KEY", "")
UNIPILE_DSN = os.environ.get("UNIPILE_DSN", "api43.unipile.com:17362")
UNIPILE_ACCOUNT_ID = os.environ.get("UNIPILE_ACCOUNT_ID", "")

KOALAKE_CATALOG = os.environ.get("KOALAKE_CATALOG", "linkedin_unipile")
KOALAKE_SCHEMA = os.environ.get("KOALAKE_SCHEMA", "main")
KOALAKE_TABLE = os.environ.get("KOALAKE_TABLE", "connections")

PAGE_SIZE = int(os.environ.get("UNIPILE_PAGE_SIZE", "100"))
INSERT_BATCH = 500

FQTN = f"{KOALAKE_CATALOG}.{KOALAKE_SCHEMA}.{KOALAKE_TABLE}"

# Target columns, in insert order. Keep permissive (no NOT NULL) per Koalake rules.
COLUMNS = [
    "member_id",
    "first_name",
    "last_name",
    "headline",
    "public_identifier",
    "public_profile_url",
    "profile_picture_url",
    "connection_urn",
    "connected_at",   # TIMESTAMP, derived from Unipile created_at (epoch ms)
    "ingested_at",    # TIMESTAMP, this run's timestamp
]


# ---------------------------------------------------------------------------
# Extract -- Unipile relations, cursor-paginated
# ---------------------------------------------------------------------------

def fetch_all_relations() -> list[dict]:
    base = f"https://{UNIPILE_DSN}/api/v1/users/relations"
    rows: list[dict] = []
    cursor: str | None = None
    while True:
        url = f"{base}?account_id={UNIPILE_ACCOUNT_ID}&limit={PAGE_SIZE}"
        if cursor:
            url += f"&cursor={urllib.parse.quote(cursor)}"
        req = urllib.request.Request(
            url,
            headers={"X-API-KEY": UNIPILE_API_KEY, "accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise SystemExit(f"Unipile API error {exc.code}: {body}") from exc
        items = payload.get("items", [])
        rows.extend(items)
        cursor = payload.get("cursor")
        if not cursor or not items:
            break
    return rows


def transform(raw: list[dict]) -> list[dict]:
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
    out: list[dict] = []
    for r in raw:
        created_ms = r.get("created_at")
        connected_at = (
            datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
            .replace(tzinfo=None)
            .isoformat(timespec="seconds")
            if isinstance(created_ms, (int, float))
            else None
        )
        out.append(
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
    return out


# ---------------------------------------------------------------------------
# Load -- full refresh into Koalake via the authenticated `koalake` CLI
# ---------------------------------------------------------------------------

def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _run_sql(sql: str) -> str:
    """Send SQL to `koalake query` over stdin (UTF-8) -- avoids arg-encoding issues."""
    if not shutil.which("koalake"):
        raise SystemExit("`koalake` CLI not found on PATH. Install/auth it first.")
    proc = subprocess.run(
        f"koalake query - --catalog {KOALAKE_CATALOG}",
        input=sql,
        text=True,
        encoding="utf-8",
        shell=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise SystemExit(f"koalake query failed:\n{proc.stderr.strip()}")
    return proc.stdout.strip()


def ensure_table() -> None:
    _run_sql(
        f"""
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
    )


def load_full_refresh(rows: list[dict]) -> None:
    ensure_table()
    _run_sql(f"DELETE FROM {FQTN}")
    if not rows:
        return
    col_list = ", ".join(COLUMNS)
    for i in range(0, len(rows), INSERT_BATCH):
        batch = rows[i : i + INSERT_BATCH]
        values = ",\n".join(
            "(" + ", ".join(_sql_literal(row[c]) for c in COLUMNS) + ")" for row in batch
        )
        _run_sql(f"INSERT INTO {FQTN} ({col_list}) VALUES\n{values}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    missing = [k for k in ("UNIPILE_API_KEY", "UNIPILE_ACCOUNT_ID") if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)} (see .env.example)")

    print(f"Pulling relations for account {UNIPILE_ACCOUNT_ID} from {UNIPILE_DSN} ...")
    raw = fetch_all_relations()
    print(f"  fetched {len(raw)} connection(s)")

    rows = transform(raw)
    print(f"Loading (full refresh) into {FQTN} ...")
    load_full_refresh(rows)

    count = _run_sql(f"SELECT count(*) AS n FROM {FQTN}").splitlines()[-1].strip()
    print(f"Done. {FQTN} now holds {count} row(s).")


if __name__ == "__main__":
    main()