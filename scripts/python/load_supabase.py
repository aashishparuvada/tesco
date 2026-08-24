"""Load every CSV in data/ into the raw_data schema of a Supabase Postgres.

Same behaviour as load_data.py -- the shared logic lives in loader.py -- but
pointed at the hosted database instead of the local container.

Reads two variables from .env:

    SUPABASE_CONNECTION_STRING  the project connection string; a literal
                                {SUPABASE_DB_PASSWORD} placeholder in it is
                                substituted at runtime, so the password is
                                never duplicated
    SUPABASE_DB_PASSWORD        the database password

Usage:
    uv run scripts/python/load_supabase.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent))

from loader import load_env, require_env, run_load

PASSWORD_PLACEHOLDER = "{SUPABASE_DB_PASSWORD}"


def build_dsn() -> str:
    """Resolve the connection string, substituting the password and forcing TLS."""
    require_env("SUPABASE_CONNECTION_STRING")
    dsn = os.environ["SUPABASE_CONNECTION_STRING"].strip()

    if PASSWORD_PLACEHOLDER in dsn:
        require_env("SUPABASE_DB_PASSWORD")
        # Percent-encode: passwords routinely contain characters that would
        # otherwise be parsed as URL delimiters.
        password = quote(os.environ["SUPABASE_DB_PASSWORD"], safe="")
        dsn = dsn.replace(PASSWORD_PLACEHOLDER, password)

    # Supabase requires TLS. psycopg2 would only "prefer" it by default, which
    # can silently fall back to an unencrypted connection.
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"
    if "connect_timeout=" not in dsn:
        dsn += "&connect_timeout=10"

    return dsn


def redact(dsn: str) -> str:
    """Describe the target without exposing the password in the logs."""
    parts = urlsplit(dsn)
    host = parts.hostname or "?"
    port = f":{parts.port}" if parts.port else ""
    user = f"{parts.username}@" if parts.username else ""
    return urlunsplit((parts.scheme, f"{user}{host}{port}", parts.path, "", ""))


CONNECTION_HELP = """
Could not reach Supabase.

Two things commonly cause this:

1. IPv6-only direct connection. db.<project-ref>.supabase.co now resolves to an
   IPv6 address only. On an IPv4-only network it cannot be reached at all, which
   shows up as "could not translate host name ... to address".

2. A paused project. Free-tier projects pause after a period of inactivity and
   refuse connections until resumed from the dashboard.

The fix for (1) is the connection pooler, which is IPv4-compatible. In the
Supabase dashboard go to Project Settings -> Database -> Connection string and
copy the *Session pooler* URI, then put it in .env as:

    SUPABASE_CONNECTION_STRING=postgresql://postgres.<project-ref>:{SUPABASE_DB_PASSWORD}@aws-<n>-<region>.pooler.supabase.com:5432/postgres

Note the username is postgres.<project-ref>, not postgres, and the host carries
your project's own region -- it cannot be guessed. Prefer the session pooler on
port 5432 over the transaction pooler on 6543: this script bulk-loads with
COPY inside one transaction, which session mode handles without caveats.
"""


def connect(dsn: str):
    try:
        return psycopg2.connect(dsn)
    except psycopg2.OperationalError as exc:
        raise SystemExit(f"{CONNECTION_HELP}\nUnderlying error: {exc}") from exc


def main() -> int:
    load_env()
    dsn = build_dsn()
    return run_load(lambda: connect(dsn), f"Supabase ({redact(dsn)})")


if __name__ == "__main__":
    raise SystemExit(main())
