"""
Shared environment/config plumbing for the Unipile connector DAG.

Values come from the KoalakeEnvironment mapping when the airflow provider is
present (on the Koalake platform), falling back to uppercased environment
variables for local dev / tests. Mirrors the pattern in
Koalitix/koalake-matomo-dashboard so the two stay in sync.
"""
import logging
import os

from src import koalake_io

try:
    from koalake.airflow_provider.environment import KoalakeEnvironment
    _env = KoalakeEnvironment(environment_id=0)
except ImportError:
    _env = None


def get_config(key: str, default: str = "") -> str:
    """KoalakeEnvironment first (platform), then os.environ (local/tests)."""
    if _env is not None:
        value = _env.get(key, default="")
        if value:
            return value
    return os.environ.get(key.upper(), default)


# --- Koalake target (direct DuckDB connection via the koalake extension) ---
# Catalog MUST be set explicitly — silently writing to a default catalog is
# worse than failing, so connect_koalake() raises when it's empty.
DATABASE_CATALOG = get_config("koalake_catalog", "")
DATABASE_SCHEMA = get_config("koalake_schema", "main")
KOALAKE_ENDPOINT = get_config("koalake_endpoint", "https://api.dev.koalitix.koalake.cloud")
KOALAKE_TOKEN = get_config("koalake_token", "")
KOALAKE_EXTENSION_PATH = get_config("koalake_extension_path", "")  # optional on-platform

# --- Unipile source (LinkedIn) ---
UNIPILE_API_KEY = get_config("unipile_api_key", "")
UNIPILE_DSN = get_config("unipile_dsn", "api43.unipile.com:17362")
UNIPILE_ACCOUNT_ID = get_config("unipile_account_id", "")

# --- Per-connection DuckDB bounds ---
DUCKDB_MEMORY_LIMIT = get_config("koalake_duckdb_memory_limit", "2GB")
DUCKDB_THREADS = int(get_config("koalake_duckdb_threads", "2"))

# --- Failure alerting (optional) ---
ALERT_EMAIL = [e.strip() for e in get_config("alert_email", "").split(",") if e.strip()]


def alerting_default_args() -> dict:
    """default_args fragment wiring email-on-failure when alert_email is set."""
    if not ALERT_EMAIL:
        return {"email_on_failure": False, "email_on_retry": False}
    return {"email": ALERT_EMAIL, "email_on_failure": True, "email_on_retry": False}


def connect_koalake():
    if not DATABASE_CATALOG:
        raise ValueError(
            "koalake_catalog is not configured — refusing to write to a default "
            "catalog. Set the 'koalake_catalog' project variable (e.g. linkedin_unipile)."
        )
    if not KOALAKE_TOKEN:
        raise ValueError("koalake_token is not configured. Set the 'koalake_token' project variable.")
    logging.info(f"Connecting to Koalake catalog '{DATABASE_CATALOG}' at {KOALAKE_ENDPOINT}")
    return koalake_io.get_connection(
        DATABASE_CATALOG, KOALAKE_ENDPOINT, KOALAKE_TOKEN, KOALAKE_EXTENSION_PATH,
        memory_limit=DUCKDB_MEMORY_LIMIT, threads=DUCKDB_THREADS,
    )