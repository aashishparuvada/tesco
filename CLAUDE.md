# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A follow-along data engineering tutorial repo, not a library. Six synthetic UK
retail CSVs in `data/` (42,796 rows) are loaded into a `raw_data` schema in
Postgres — locally via Docker (Stage 0) and in a hosted Postgres (Stage 1) — and
Stage 2 (CDC into Databricks) is the stage currently being built. The README is
the deliverable as much as the code is: it is written to be executed step by
step by a reader, so changes to loader behaviour, env vars, or commands must be
reflected there and in `CHANGELOG.md` (Keep a Changelog format, SemVer, version
also lives in `pyproject.toml`).

## Commands

```bash
uv sync                                  # install (Python 3.12, pinned in .python-version)
uv sync --dev && uv run pre-commit install

docker compose up -d                     # local Postgres 16; reads .env
uv run scripts/python/load_data.py       # Stage 0: CSVs -> local Postgres raw_data
uv run scripts/python/load_supabase.py   # Stage 1: CSVs -> hosted Postgres raw_data

uv run pre-commit run --all-files        # the full quality gate; CI runs exactly this
uv run pre-commit run ruff --all-files   # single hook
```

There is no test suite. `.github/workflows/quality-checks.yml` runs the
pre-commit hooks plus a 5MB large-file guard against `origin/main`.

## Architecture

`scripts/python/loader.py` holds all the logic; the two `load_*.py` files are
thin entry points that differ only in how they build a psycopg2 connection and
pass it to `run_load(connect, target)` as a zero-arg callable. **Never fork
loading behaviour into an entry point** — the whole point of the split is that
the local and hosted targets cannot drift apart.

The load is schema-inferring and fully generated:

1. `discover_csvs()` — every `*.csv` in `data/` becomes a table named after the
   file. Adding a CSV is the only step needed to get a new table.
2. `profile_csv()` — one pass per file collecting `ColumnStats` (does every
   value parse as int/decimal/timestamp/date, max length, max int digits, max
   scale, distinct set).
3. `sql_type()` — narrowest type that fits: `TIMESTAMP` > `DATE` > `BIGINT`
   (for `*_id` or >int32) > `INTEGER` > `NUMERIC(p,s)` > `CHAR(1)` > bucketed
   `VARCHAR` > `TEXT`. Widths get deliberate headroom (`VARCHAR_BUCKETS`, +4
   numeric precision) so a longer value in a later extract does not break the load.
4. `primary_key()` — a leading `*_id` column that is unique and never empty.
5. `run_load()` — writes `scripts/sql/raw_data/ddl.sql`, then in **one
   transaction** does `ensure_schema` → `DROP`/`CREATE TABLE` → `COPY FROM
   STDIN` → row-count verification. A failure commits nothing; re-running is
   always safe.

Constraints worth knowing before editing this code:

- **`scripts/sql/raw_data/ddl.sql` is generated output and is committed.** Do
  not hand-edit it. Change the inference rules in `loader.py` and re-run
  `load_data.py` to regenerate.
- **Generated SQL must satisfy sqlfluff**, which lints it in pre-commit and CI
  with `dialect = databricks` and uppercase keywords. That is why `table_ddl()`
  emits single spaces and no column-alignment padding.
- **`raw_data` is a landing layer by design.** Only primary keys are `NOT NULL`;
  everything else stays nullable and permissive. Constraints, conformed types,
  and business rules belong downstream (`scripts/sql/bronze/` is the empty
  placeholder for that).
- **Identifiers are validated, then interpolated via `psycopg2.sql`.** Table and
  column names must match `IDENTIFIER_RE` (lower_snake_case) or the run aborts.
- **`load_env()` is a hand-rolled `.env` reader** using `os.environ.setdefault`,
  so real environment variables always win in CI. There is no `python-dotenv`
  dependency; the only runtime dependency is `psycopg2-binary`.

Every table carries `created_timestamp`, `updated_timestamp`, and an
`is_active` `Y`/`N` soft-delete flag. These exist specifically so Stage 2 can do
CDC sequencing and deletes (`AUTO CDC ... SEQUENCE BY updated_timestamp APPLY AS
DELETE WHEN is_active = 'N'`) without WAL access. Do not drop them from the
dataset or treat them as incidental.

## Connection gotchas that are load-bearing

Supabase's direct host `db.<ref>.supabase.co` is **IPv6-only**, so on an
IPv4 network only the *session pooler* (`aws-<n>-<region>.pooler.supabase.com`,
user `postgres.<ref>`, port 5432) works. `load_supabase.py` prints this as
`CONNECTION_HELP` on `OperationalError`. Session mode (5432), not transaction
mode (6543), because the loader COPYs inside one transaction. The connection
string keeps a literal `{SUPABASE_DB_PASSWORD}` placeholder that `build_dsn()`
substitutes and percent-encodes at runtime, and `sslmode=require` is appended
when absent because psycopg2 would otherwise only *prefer* TLS. `redact()` keeps
the password out of logs — preserve that when touching logging.

Stage 2 has two documented vendor blockers, both explained at length in the
README: Databricks Free Edition is serverless-only while the Lakeflow Connect
ingestion gateway needs classic compute, and Supabase free cannot carry a
replication connection (logical replication does not pass through Supavisor, and
the direct host needs the Pro-plan IPv4 add-on). Path B (federated read +
`AUTO CDC`) is the free route and the recommended one to build first.

## Orchestration (Airflow)

`airflow/` is a second, independent `docker compose` project (its own
`docker-compose.yaml`, `.env`, Postgres metadata DB) that runs the
`dbt_tesco_pipeline` DAG (`airflow/dags/orchestrate.py`):
`databricks_ingest_cdc` (triggers the existing Databricks bronze job) →
`source_freshness` → silver models/tests → the OBT/tests → the gold layer.
`LocalExecutor` via `command: standalone`, not the official `CeleryExecutor`
compose — deliberate, since there is exactly one DAG here.

- **`airflow/dags/utils.py` calls `databricks-sdk`'s `WorkspaceClient`
  directly** (`run_now` + poll `get_run` to a terminal state) rather than the
  `apache-airflow-providers-databricks` operator package. Keep it that way
  unless there's a real reason to add the provider dependency.
- **The triggered job ID is a hardcoded literal**
  (`utils.trigger_databricks_job(job_id=...)` in `orchestrate.py`) — a known
  rough edge, not settled design. Prefer moving it to `.env` or an Airflow
  Variable over leaving it as a literal if you touch this function.
- **`airflow/.env` changes need the container recreated, not just
  `up -d`.** `env_file:` values are read at container creation; editing
  `airflow/.env` and running `docker compose up -d` again does not reliably
  refresh an already-running container. Use `cd airflow && docker compose
  down && docker compose up -d --build` (`--build` too, whenever
  `airflow/Dockerfile` also changed — otherwise the container comes back from
  the stale image).
- `DATABRICKS_HOST` / `DATABRICKS_TOKEN` belong only in the git-ignored
  `airflow/.env`, never in `airflow/.env.example` — that template documents
  every other variable but deliberately omits these two.

## Data

Everything in `data/` is synthetic and safe to publish: Ofcom reserved drama
phone ranges, `example.*` email domains, GBP values. No value maps to a real
person.
