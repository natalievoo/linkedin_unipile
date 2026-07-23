# linkedin_unipile

Pulls LinkedIn **connections** from [Unipile](https://www.unipile.com/) and lands
them in **Koalake**, so the network can be analysed and dashboarded. Phase 1 of
the LinkedIn-in-Koalake project (Phase 2 = push to HubSpot, later).

LinkedIn's own API is heavily restricted, so we go through Unipile, which exposes
a connected account's connections as **"relations"**. Each relation carries a
connection **date** (`created_at`), which is what makes "connections over time"
possible.

> **Testing note:** all development runs against a **throwaway** LinkedIn account,
> never a real one. Real connections are third-party personal data (GDPR) — sort
> the Unipile DPA + EU data-residency check before pointing this at a real account.

## What's here

```
airflow/                         Koalake connector (this is what Koalake deploys)
├── dags/linkedin_connections.py   the ingestion DAG  (Koalake parses dags/ here)
├── src/
│   ├── koalake_io.py              DuckDB + koalake-extension write layer
│   ├── connector_env.py           config: Koalake project variables -> env fallback
│   └── unipile_client.py          paginated Unipile relations client
├── .airflowignore                 keeps src/ + tests/ out of DAG parsing
└── requirements.txt               task runtime deps (Airflow itself is in the image)
scripts/ingest_connections.py    Standalone twin -- same result, runs on a laptop via the koalake CLI
.env.example                     Local config template (copy to .env; .env is gitignored)
```

Structure mirrors `Koalitix/koalake-matomo-dashboard`, the reference Koalake
connector. Both paths do the same thing: **extract** relations from Unipile →
**full-refresh** them into `linkedin_unipile.main.connections`. Full refresh
(`CREATE OR REPLACE TABLE`) is idempotent: re-running replaces, never duplicates.

The DAG writes to Koalake the way Koalake actually supports it — a direct DuckDB
connection with the `koalake` extension (`LOAD koalake` → `CREATE OR REPLACE
SECRET` → `ATTACH`), **not** Airflow "Koalake operators" (which don't exist for
this).

### Target table

| column | type | notes |
|---|---|---|
| member_id | VARCHAR | stable person id (natural key) |
| first_name / last_name | VARCHAR | |
| headline | VARCHAR | |
| public_identifier | VARCHAR | LinkedIn vanity handle |
| public_profile_url | VARCHAR | |
| profile_picture_url | VARCHAR | |
| connection_urn | VARCHAR | |
| connected_at | TIMESTAMP | from Unipile `created_at` — drives growth-over-time |
| ingested_at | TIMESTAMP | when this run pulled the data |

The DAG also full-refreshes a **`messages`** table (LinkedIn message history):

| column | type | notes |
|---|---|---|
| message_id | VARCHAR | Unipile message id |
| chat_id | VARCHAR | conversation id |
| sender_id | VARCHAR | LinkedIn member id of the sender |
| is_sender | BOOLEAN | true = the connected account sent it |
| text | VARCHAR | message content (**sensitive** — private conversation data) |
| message_type / attendee_type | VARCHAR | |
| seen / delivered / edited / deleted | BOOLEAN | message flags |
| has_attachments | BOOLEAN | |
| sent_at | TIMESTAMP | from Unipile `timestamp` |
| ingested_at | TIMESTAMP | when this run pulled the data |

## Run it now (standalone, no Airflow)

Prereqs: the authenticated `koalake` CLI on PATH (`koalake auth status` → OK) and
Python 3.

```bash
cp .env.example .env          # then fill in UNIPILE_API_KEY and UNIPILE_ACCOUNT_ID
python scripts/ingest_connections.py
```

It prints how many connections it fetched and the final row count. Verify:

```bash
koalake query "SELECT count(*) FROM linkedin_unipile.main.connections" --catalog linkedin_unipile
koalake query "SELECT date_trunc('day', connected_at) AS day, count(*) new, \
  sum(count(*)) OVER (ORDER BY date_trunc('day', connected_at)) running_total \
  FROM linkedin_unipile.main.connections GROUP BY 1 ORDER BY 1" --catalog linkedin_unipile
```

## Deploy the DAG to Koalake

The Koalake project is **connected to this git repo** and clones from it. Koalake
parses DAGs out of **`airflow/dags/`**, so the DAG appears once the project syncs
a commit that contains it.

1. **Set these project variables** in Koalake (read by `connector_env.py`):

   | Variable | Value |
   |---|---|
   | `koalake_catalog` | `linkedin_unipile` |
   | `koalake_token` | a Koalake personal access token (`npat_...`) |
   | `unipile_api_key` | your Unipile API key |
   | `unipile_account_id` | the connected LinkedIn account id |

   Optional: `koalake_schema` (default `main`), `koalake_endpoint`, `unipile_dsn`.

2. **Re-sync** the project's git connection so it picks up the latest commit, then
   check the DAGs view -- `linkedin_connections` should appear.
3. **Trigger** it. On failure, read the failing task's log.

See koalitix-claude `knowledge/airflow/airflow-koalake.md` and the reference
connector `Koalitix/koalake-matomo-dashboard` for the full pattern.

## Secrets

Never commit secrets. Locally they live in `.env` (gitignored); in production they
are synced Koalake Variables. **Rotate** the Unipile API key used during early
testing before any real account is connected.

## Next

- dbt models on top: `stg_connections` → a clean `connections` mart + a
  `connections_growth` daily model, so dashboards read tidy tables.
- Dashboard: connections over time, new-per-week, cuts by headline/company.
- Messages ingest (Unipile chats) — groundwork for Phase 2 (HubSpot).