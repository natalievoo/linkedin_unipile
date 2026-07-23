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
dags/linkedin_connections.py     Airflow DAG -- the production pipeline (deploy to Koalake)
scripts/ingest_connections.py    Standalone twin -- same logic, runs on a laptop (no Airflow)
.env.example                     Config template (copy to .env; .env is gitignored)
requirements.txt                 Deps (stdlib-only script; DAG needs `requests`)
```

Both do the same thing: **extract** relations from Unipile → **full-refresh** them
into `linkedin_unipile.main.connections`. Full refresh (delete-all + insert) is
idempotent: re-running never duplicates, and connections that disappear drop out.

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

The DAG is the scheduled production form. On Koalake you **deploy through the
platform** (workspace → `S3DagBundle`), not by dropping a file in a `dags/` folder.
At deploy the platform rewrites `environment_id=0` into the real environment
connection — which is why running `dags/linkedin_connections.py` directly, undeployed,
fails at `get_connection` (expected).

1. Register the Unipile config as **Koalake Variables** on the project so they sync
   to Airflow (read in the DAG via `KoalakeEnvironment.get(...)`):
   `UNIPILE_API_KEY`, `UNIPILE_DSN`, `UNIPILE_ACCOUNT_ID`.
2. Deploy this repo's `dags/` to the project's workspace and publish the bundle.
3. Before first deploy, confirm the koalake `airflow-provider` operator imports and
   constructor kwargs (`KoalakeQueryOperator`, `KoalakeInsertOperator`,
   `KoalakeEnvironment`) match the provider version on the platform.

See koalitix-claude `knowledge/airflow/airflow-koalake.md` for the full deploy model.

## Secrets

Never commit secrets. Locally they live in `.env` (gitignored); in production they
are synced Koalake Variables. **Rotate** the Unipile API key used during early
testing before any real account is connected.

## Next

- dbt models on top: `stg_connections` → a clean `connections` mart + a
  `connections_growth` daily model, so dashboards read tidy tables.
- Dashboard: connections over time, new-per-week, cuts by headline/company.
- Messages ingest (Unipile chats) — groundwork for Phase 2 (HubSpot).