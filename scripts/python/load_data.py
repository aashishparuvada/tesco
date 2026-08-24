"""Load every CSV in data/ into the raw_data schema of the local Postgres.

This targets the docker-compose instance defined at the repository root. The
table definitions are not hand-written -- see loader.py, which profiles each
CSV, infers a column type per column, and writes the generated DDL to
scripts/sql/raw_data/ddl.sql.

Adding a new CSV to data/ is the only step needed to get a new raw_data table.

Usage:
    docker compose up -d
    uv run scripts/python/load_data.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent))

from loader import load_env, require_env, run_load


def connect():
    require_env("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


def main() -> int:
    load_env()
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    return run_load(connect, f"local Postgres at {host}:{port}")


if __name__ == "__main__":
    raise SystemExit(main())
