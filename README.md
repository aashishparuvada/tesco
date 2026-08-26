# <img src="assets/images/tesco-logo.png" alt="" height="30" valign="middle"> Data Engineering Project

A hands-on, end-to-end data engineering project built on a synthetic UK retail
dataset. You start with plain CSV files, land them in Postgres, push them to a
hosted database, and then stream changes into a Databricks catalog using Change
Data Capture (CDC).

The repository is meant to be **followed**, not just read. Every stage runs on
free tooling wherever that is possible, and where it genuinely is not, this
README says so plainly instead of letting you discover it halfway through.

> **Status:** Stages 0 and 1 are implemented and verified. Stage 2 (CDC into
> Databricks) is the stage currently being built — this README documents the
> design and the setup you need, and will be updated as the pipeline lands. The
> dbt silver layer that sits on top of it is written and reviewable now; it runs
> the moment bronze exists.

---

## Table of contents

- [What you will learn](#what-you-will-learn)
- [Architecture](#architecture)
- [The dataset](#the-dataset)
- [Prerequisites](#prerequisites)
- [Accounts you need, and when to create them](#accounts-you-need-and-when-to-create-them)
- [Stage 0 — Land the CSVs in local Postgres](#stage-0--land-the-csvs-in-local-postgres)
- [Stage 1 — Push to a hosted Postgres](#stage-1--push-to-a-hosted-postgres)
- [Stage 2 — CDC from Postgres into Databricks](#stage-2--cdc-from-postgres-into-databricks)
- [Setting up dbt for Databricks](#setting-up-dbt-for-databricks)
- [The silver layer in dbt](#the-silver-layer-in-dbt)
- [Repository layout](#repository-layout)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)

---

## What you will learn

| Topic | Where it shows up |
| --- | --- |
| Schema inference from raw files | `loader.py` profiles each CSV and generates DDL |
| Idempotent, transactional loads | The whole load is one transaction; re-running is safe |
| Raw / landing layer design | Permissive types, nullable columns, no business rules |
| Environment-based configuration | `.env` drives Docker and both loaders |
| Moving from local to hosted Postgres | Same code, different connection |
| Change Data Capture | Postgres logical replication (WAL) into Delta tables |
| Medallion architecture | `raw_data` → bronze → silver / gold in Unity Catalog |
| Incremental models and `MERGE` | Silver models merge on their primary key, not append |
| Watermarking an incremental load | `is_incremental()` plus a `COALESCE`d `MAX(updated_timestamp)` |
| Generating SQL with Jinja | The OBT declares its joins as data and loops over them |
| Data quality testing | Generic tests in YAML, singular tests as queries, `warn` vs `error` |
| The gap between docs and reality | Network, tier, and quota limits that block the happy path |

That last row is not filler. Most of the difficulty in this project is not
writing SQL — it is getting two managed services to talk to each other.

---

## Architecture

```mermaid
flowchart LR
    A[CSV files<br/>data/] -->|COPY| B[(Local Postgres<br/>Docker<br/>raw_data)]
    A -->|COPY| C[(Hosted Postgres<br/>Supabase / Neon<br/>raw_data)]
    C -->|CDC via WAL<br/>logical replication| D[Databricks<br/>Unity Catalog]
    D --> E[Bronze tables]
    E --> F[Silver / Gold<br/>models]

    style A fill:#1f6feb,color:#fff
    style C fill:#238636,color:#fff
    style D fill:#bd561d,color:#fff
```

Stage 0 and Stage 1 are the two `COPY` arrows. Stage 2 is the CDC arrow: the
hosted Postgres is the **source of truth**, and Databricks continuously applies
inserts, updates, and deletes from its write-ahead log.

---

## The dataset

Six CSVs in `data/` model a UK retail chain. All of it is synthetic — phone
numbers use the Ofcom reserved drama ranges, emails use `example.*` domains, so
no value maps to a real person.

| File | Rows | Grain |
| --- | ---: | --- |
| `stores.csv` | 25 | one row per store |
| `employees.csv` | 250 | one per employee, `store_id` FK |
| `customers.csv` | 2,000 | one per customer |
| `products.csv` | 500 | one per product |
| `orders.csv` | 10,000 | one per order, `customer_id` + `store_id` FKs |
| `order_items.csv` | 30,021 | one per order line, `order_id` + `product_id` FKs |

**42,796 rows total.** Small enough to load in seconds, large enough that
partitioning and incremental logic are not pointless.

Every table carries three columns that exist specifically to make CDC and
slowly-changing-dimension work possible:

- `created_timestamp` — when the row first appeared
- `updated_timestamp` — when it last changed
- `is_active` — `Y` / `N` soft-delete flag

Keep these in mind: they are what makes the free Stage 2 path in this README
work without log-based replication.

---

## Prerequisites

Local tooling:

| Tool | Why | Install |
| --- | --- | --- |
| Python 3.12 | Pinned in `.python-version` | via `uv` |
| [uv](https://docs.astral.sh/uv/) | Dependency + venv management | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker Desktop | Runs local Postgres 16 | [docker.com](https://www.docker.com/products/docker-desktop/) |
| `psql` *(optional)* | Poking at the database by hand | `brew install libpq` |

Clone and install:

```bash
git clone <your-fork-url> tesco
cd tesco
uv sync
cp .env.example .env      # then edit it
```

`.env` is git-ignored. `.env.example` documents every variable and is the file
to read first.

---

## Accounts you need, and when to create them

Do **not** sign up for everything on day one. Each account is only needed at the
stage that uses it, and some free tiers start expiring the moment you create
them.

### 1. Hosted Postgres — create this before Stage 1

Any publicly reachable Postgres works. This is **not** Supabase-specific; the
loader takes a standard connection string.

| Provider | Free tier | Logical replication (needed for real CDC) |
| --- | --- | --- |
| [Supabase](https://supabase.com) | Yes | `wal_level=logical` already on, but **not reachable for replication on the free tier** — see below |
| [Neon](https://neon.com) | Yes | Yes, enable in Console → Settings → Logical Replication |
| [Aiven](https://aiven.io) | Trial | Yes |
| Your own VPS | — | Yes, you control `postgresql.conf` |

**If you only want to finish Stage 1**, Supabase free is fine and is what this
repo is configured for.

**If you want log-based CDC in Stage 2**, prefer **Neon's free tier**. The
reasoning is in [Stage 2](#stage-2--cdc-from-postgres-into-databricks) — briefly,
Supabase's free tier cannot carry a replication connection.

### 2. Databricks — create this at the start of Stage 2

Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition).
It gives you a serverless workspace with Unity Catalog, which is what you need to
create a catalog and land tables.

Two Free Edition limits matter here, both from the
[official limitations page](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations):

- **Serverless compute only.** No classic compute, one 2X-Small SQL warehouse,
  one active pipeline per pipeline type.
- **Outbound internet access is restricted to a limited set of trusted
  domains.** Account verification unlocks broader outbound access — do this
  early, because without it your workspace cannot reach your database at all.

---

## Stage 0 — Land the CSVs in local Postgres

Start the database and load it:

```bash
docker compose up -d
uv run scripts/python/load_data.py
```

Expected output:

```
Found 6 CSV file(s) in /path/to/tesco/data

Profiling CSV files to infer column types...
  customers.csv        11 cols, 2,000 rows, pk=customer_id
  employees.csv        10 cols, 250 rows, pk=employee_id
  order_items.csv      9 cols, 30,021 rows, pk=order_item_id
  orders.csv           10 cols, 10,000 rows, pk=order_id
  products.csv         8 cols, 500 rows, pk=product_id
  stores.csv           8 cols, 25 rows, pk=store_id

Wrote schema definition to scripts/sql/raw_data/ddl.sql
...
All 6 table(s) loaded into raw_data on local Postgres at localhost:5432 (42,796 rows).
```

### What the loader actually does

This is the part worth understanding, because it is the reusable idea.

1. **Discovery** — every `*.csv` in `data/` becomes a table named after the file.
   Adding a CSV is the only step needed to get a new table.
2. **Profiling** — each file is read once, recording per column whether every
   value parses as an integer, decimal, timestamp, or date, plus the longest
   value, largest integer, maximum numeric scale, and distinct count.
3. **Type inference** — the narrowest type that still fits every value:
   `TIMESTAMP`, `DATE`, `BIGINT` for `*_id`, `INTEGER`, `NUMERIC(p, s)`,
   `CHAR(1)`, or a bucketed `VARCHAR`. Widths get deliberate headroom so a
   longer value in a later extract does not break the load.
4. **Key detection** — a leading `*_id` column that is unique and never empty
   becomes the `PRIMARY KEY`.
5. **Execution** — `CREATE SCHEMA IF NOT EXISTS raw_data`, then
   `DROP TABLE IF EXISTS` + `CREATE TABLE` per table, then `COPY ... FROM STDIN`.
6. **Verification** — every table is re-counted and the run fails if the count
   does not match the source CSV.

The generated DDL is written to `scripts/sql/raw_data/ddl.sql` and committed, so
schema changes show up in code review instead of drifting silently.

The whole run is **one transaction**. If any table fails, nothing is committed —
there is no half-loaded schema to clean up. Re-running is always safe.

> **Design note:** `raw_data` is a landing layer. Only primary keys are
> `NOT NULL`; everything else stays nullable and accepts whatever the source
> sends. Constraints, conformed types, and business rules belong downstream. A
> raw layer that rejects rows loses data you cannot get back.

---

## Stage 1 — Push to a hosted Postgres

CDC needs a source that Databricks can reach over the internet. Your laptop's
Docker container is not that, so the same data goes to a hosted Postgres.

Add your connection string to `.env`:

```bash
SUPABASE_CONNECTION_STRING=postgresql://postgres.YOUR_PROJECT_REF:{SUPABASE_DB_PASSWORD}@aws-0-YOUR_REGION.pooler.supabase.com:5432/postgres
SUPABASE_DB_PASSWORD=your_database_password
```

Leave the `{SUPABASE_DB_PASSWORD}` placeholder literal — the script substitutes
it at runtime and percent-encodes it, so the password lives in exactly one place
and special characters cannot break URL parsing.

```bash
uv run scripts/python/load_supabase.py
```

Same six tables, same row counts, same generated DDL — `loader.py` is shared, so
the local and hosted targets cannot drift apart. The only differences are that
`sslmode=require` is forced (psycopg2 would otherwise merely *prefer* TLS), and
the schema is created only if genuinely missing, since a hosted role may not have
permission to create schemas.

### Getting the connection string right

This trips up nearly everyone on Supabase. The URI shown most prominently in the
dashboard is the **direct** connection:

```
postgresql://postgres:[YOUR-PASSWORD]@db.<project-ref>.supabase.co:5432/postgres
```

That hostname resolves to an **IPv6 address only**:

```console
$ dig +short AAAA db.<project-ref>.supabase.co
2406:da14:1d62:b401:4281:9d41:890f:b5b0
$ dig +short A db.<project-ref>.supabase.co
                                    # nothing
```

On an IPv4-only network it fails with `could not translate host name ... to
address`. Use the **Session pooler** URI instead (Project Settings → Database →
Connection string). Two things change: the username becomes
`postgres.<project-ref>`, and the host becomes
`aws-<n>-<region>.pooler.supabase.com`. Both the `aws-<n>-` prefix and the region
are per-project — copy them, do not guess.

Prefer **session mode (port 5432)** over transaction mode (6543): this loader
bulk-copies inside a single transaction, which session mode handles without
caveats.

`load_supabase.py` prints this guidance automatically if the connection fails.

---

## Stage 2 — CDC from Postgres into Databricks

### What CDC is, and why bother

The naive way to keep Databricks in sync is to re-export all 42,796 rows every
night and overwrite the target. That works here and stops working immediately at
real scale, because cost grows with **table size** rather than with **how much
actually changed**.

Change Data Capture reads the changes themselves. Postgres already writes every
insert, update, and delete to its **write-ahead log** (WAL) for durability.
Logical replication lets an external consumer subscribe to that log, so
Databricks receives *"customer 1,432 changed city"* rather than re-reading two
thousand customers. Deletes come through as real events — which a
`WHERE updated_timestamp > last_run` query can never detect.

Databricks implements this with
[Lakeflow Connect's PostgreSQL connector](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/postgresql-pipeline),
using logical replication with the `pgoutput` plugin. Two pipelines are involved:

- an **ingestion gateway** that continuously reads the WAL into staging storage
- an **ingestion pipeline** that applies those changes into Delta tables

The gateway must run **continuously**, otherwise Postgres retains WAL segments
for your unconsumed replication slot until the disk fills. This is the single
most important operational fact about CDC and the most common way people break
their source database.

### Read this before you start: two blockers

Both are documented vendor limits, not bugs. Knowing them now saves hours.

**Blocker 1 — Databricks Free Edition cannot run the managed connector.**

The Lakeflow Connect docs state the ingestion gateway
[runs on classic compute](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/cdc-overview)
inside your workspace VPC. Free Edition
[provides serverless compute only](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).
Taken together, log-based CDC via the managed connector needs a paid or trial
workspace with classic compute. Databricks does not state this combination
explicitly in one place, so verify against current docs before spending money —
previews change.

**Blocker 2 — Supabase's free tier cannot carry a replication connection.**

Two independent reasons, both from [Supabase's own docs](https://supabase.com/docs/guides/database/postgres/setup-replication-external):

1. Logical replication needs a **direct** connection. Connections through
   Supavisor, the pooler, **will not work** for replication — and the pooler is
   the only IPv4 route into a free project.
2. The direct host is IPv6-only. Reaching it over IPv4 requires the **IPv4
   add-on**, which needs at least the **Pro plan**.

The database itself is ready — a free Supabase project already reports
`wal_level = logical`, Postgres 17.6, and 5 available replication slots. It is
purely a network-reachability wall.

### Pick your path

| | Path A — real log-based CDC | Path B — free federated CDC |
| --- | --- | --- |
| **Source** | Neon free tier, or Supabase Pro + IPv4 add-on | Any hosted Postgres, incl. Supabase free |
| **Databricks** | Paid / trial workspace (classic compute) | Free Edition |
| **Mechanism** | WAL logical replication | Federated read + `AUTO CDC` on `updated_timestamp` |
| **Catches deletes** | Yes, natively | Only via the `is_active` flag |
| **Latency** | Seconds | Whatever you schedule |
| **Cost** | Databricks compute; ~$29/mo if using Supabase | Free |
| **Teaches you** | Production CDC as actually deployed | CDC semantics, SCD modelling |

**Recommendation:** do **Path B first**, even if you intend to pay. It teaches
the modelling half of CDC — merge keys, sequencing, SCD Type 2 — on free
infrastructure. Path A then changes only how bytes arrive, not how you model
them.

### Path A — Lakeflow Connect (log-based)

Full walkthrough:
[Ingest data from PostgreSQL](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/postgresql-pipeline).
The source-side setup is:

```sql
-- 1. Confirm logical replication is on (Supabase/Neon: already 'logical')
SHOW wal_level;

-- 2. Dedicated replication role
CREATE USER databricks_replication WITH PASSWORD '<strong-password>';
GRANT CONNECT ON DATABASE postgres TO databricks_replication;
GRANT USAGE ON SCHEMA raw_data TO databricks_replication;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO databricks_replication;
ALTER USER databricks_replication WITH REPLICATION;

-- 3. Replica identity: DEFAULT is fine, every table here has a primary key
ALTER TABLE raw_data.customers REPLICA IDENTITY DEFAULT;
-- ... repeat per table, or use FULL for tables without a PK

-- 4. Publication — the set of tables to replicate
CREATE PUBLICATION databricks_publication FOR TABLE
    raw_data.stores, raw_data.employees, raw_data.customers,
    raw_data.products, raw_data.orders, raw_data.order_items;

-- 5. Replication slot — the consumer's bookmark into the WAL
SELECT pg_create_logical_replication_slot('databricks_slot', 'pgoutput');
```

Then in Databricks: create a Unity Catalog **connection** of type `postgresql`,
create the ingestion **gateway** pipeline, and create the **ingestion pipeline**,
naming `databricks_slot` and `databricks_publication`.

> [!IMPORTANT]
> **Supabase users: use the Session pooler URL in the Databricks connector, not
> the direct host.**
>
> The Databricks PostgreSQL connector will simply fail to connect if you give it
> the direct connection details from the Supabase dashboard. Use the **Session
> pooler** credentials instead:
>
> | Field | Direct — *does not connect* | Session pooler — **use this** |
> | --- | --- | --- |
> | Host | `db.<project-ref>.supabase.co` | `aws-<n>-<region>.pooler.supabase.com` |
> | Port | `5432` | `5432` |
> | User | `postgres` | `postgres.<project-ref>` |
>
> The reason is the same IPv4 / IPv6 split described in
> [Stage 1](#getting-the-connection-string-right): Supabase's direct host
> resolves to an **IPv6 address only**, and on a free account the pooler is the
> only IPv4 route into the database. Anything reaching your project over IPv4 —
> Databricks included — has to go through the pooler.
>
> Copy both the `aws-<n>-` prefix and the region from **Project Settings →
> Database → Connection string → Session pooler**. They are per-project and
> cannot be guessed. Use **session mode (5432)**, not transaction mode (6543).

> [!WARNING]
> Getting the connection to succeed is not the same as getting replication to
> work. Supabase
> [documents that logical replication does not work through Supavisor](https://supabase.com/docs/guides/database/postgres/setup-replication-external),
> so the gateway may still fail at the replication step even once the connection
> tests green. If that happens, the supported routes are the **IPv4 add-on**
> (Pro plan) so the direct host becomes reachable, or a source that serves
> logical replication over IPv4 on its free tier, such as **Neon**. Path B below
> avoids the problem entirely.

Operational warnings, all from the vendor docs:

- Keep the gateway running continuously or WAL will accumulate.
- Deleting a pipeline does **not** drop the replication slot. Drop it yourself
  with `pg_drop_replication_slot('databricks_slot')` or it retains WAL forever.
- Replication works only against a primary instance, never a read replica.
- Neon removes replication slots inactive for roughly 40 hours.

### Path B — Federated read plus `AUTO CDC`

This runs entirely on Databricks Free Edition and needs nothing from your
Postgres beyond a normal SQL login — the pooler is fine, because this is not
replication.

**Step 1 — Create a foreign catalog.** Register the connection
([query federation docs](https://docs.databricks.com/aws/en/query-federation/postgresql)):

```sql
CREATE CONNECTION supabase_pg TYPE postgresql OPTIONS (
  host 'aws-0-<region>.pooler.supabase.com',
  port '5432',
  user secret('tesco', 'pg_user'),
  password secret('tesco', 'pg_password')
);

CREATE FOREIGN CATALOG tesco_source USING CONNECTION supabase_pg
  OPTIONS (database 'postgres');
```

Now `tesco_source.raw_data.customers` is queryable from Databricks without
copying anything. Store credentials in a secret scope, never inline.

**Step 2 — Apply changes with `AUTO CDC`.** The
[`AUTO CDC` APIs](https://docs.databricks.com/aws/en/ldp/cdc) — which replace
`APPLY CHANGES` with identical syntax — handle out-of-order events, deletes, and
SCD Type 1 and 2 for you. Using `updated_timestamp` as the sequencing column and
`is_active` as the delete signal:

```sql
CREATE OR REFRESH STREAMING TABLE bronze.customers;

CREATE FLOW customers_cdc AS AUTO CDC INTO bronze.customers
FROM stream(tesco_source.raw_data.customers)
  KEYS (customer_id)
  APPLY AS DELETE WHEN is_active = 'N'
  SEQUENCE BY updated_timestamp
  COLUMNS * EXCEPT (is_active)
  STORED AS SCD TYPE 2;
```

`SCD TYPE 2` keeps history, so you can ask what a customer's city was *last
month* — the question a nightly overwrite permanently destroys. Switch to
`SCD TYPE 1` if you only want current state.

This is where those three audit columns earn their place: `updated_timestamp`
orders the changes and `is_active` expresses deletes, giving you real CDC
semantics without WAL access.

> **Free Edition caveat:** one active pipeline per pipeline type. Build one
> pipeline handling all six tables rather than six pipelines.

### Verify it worked

```sql
SELECT count(*) FROM bronze.customers;                     -- expect 2,000
SELECT count(*) FROM bronze.order_items;                   -- expect 30,021

-- Then change a row at the source and confirm it propagates:
--   UPDATE raw_data.customers
--   SET city = 'Manchester', updated_timestamp = now()
--   WHERE customer_id = 1;
```

---

## Setting up dbt for Databricks

The transformations on top of Stage 2 are built with
[dbt](https://docs.getdbt.com/), running against a Databricks SQL warehouse via
the `dbt-databricks` adapter. The dbt project lives in `dbt/`.

```bash
uv sync                  # dbt-core and dbt-databricks are already in pyproject.toml
cd dbt
uv run dbt debug         # must print "All checks passed!" before you write a model
```

What follows is how this project was actually set up, in order: the version pin
the adapter forces on you, how `dbt init` was run and the layout it leaves
behind, where the credentials live, the one macOS certificate problem that stands
between a fresh machine and a passing `dbt debug`, and the VS Code setup on top.

### 1. Pin `dbt-core` below 1.12, or resolution fails

Installing the two packages without a constraint does not work:

```bash
uv add dbt-core dbt-databricks          # fails to resolve
```

`dbt-databricks` trails `dbt-core` by design. Version 1.12.4 declares:

```
Requires-Dist: dbt-core<1.12.1,>=1.11.2
Requires-Dist: dbt-spark<1.11.0,>=1.10.0
```

Unconstrained, the resolver reaches for the newest `dbt-core` on PyPI — 1.12.3
at the time of writing — which is outside that range. The working install is:

```bash
uv add "dbt-core<1.12" dbt-databricks
```

which is what `pyproject.toml` records. It resolves to `dbt-core` 1.11.14,
`dbt-databricks` 1.12.4, `dbt-spark` 1.10.3.

> [!IMPORTANT]
> **`dbt --version` will tell you dbt-core is out of date. Ignore it.**
>
> ```
> Core:
>   - installed: 1.11.14
>   - latest:    1.12.3  - Update available!
> ```
>
> That warning compares against PyPI, not against what your adapter supports.
> Upgrading `dbt-core` to satisfy it breaks the adapter. Do not pin *downwards*
> either — `dbt-core<=1.7` also fails to resolve, because 1.11.2 is the floor
> `dbt-databricks` 1.12.4 accepts. The usable window is narrow: `>=1.11.2,<1.12.1`.

### 2. Scaffold the project, then flatten it

`dbt init` will not initialise a project *inside* an existing directory — it
always creates a new subdirectory named after the project. Run from `dbt/`, it
gives you `dbt/tesco/`, one level deeper than you want:

```
dbt/
└── tesco/               # <- the extra level
    ├── dbt_project.yml
    ├── models/
    └── ...
```

The prompts it asks are: which adapter (`databricks`), then the workspace host,
the SQL warehouse HTTP path, a personal access token, `catalog` (`tesco`),
`schema` (`dbt_schema`), and `threads` (`1`).

That nested layout was then flattened by hand, so the dbt project *is* `dbt/`:

```bash
cd dbt
uv run dbt init                 # project name: tesco  ->  creates dbt/tesco/
mv tesco/* tesco/.[!.]* .       # the dotfiles matter - .gitignore is one of them
rmdir tesco                     # refuses to run if anything was left behind
```

Use `rmdir`, not `rm -rf`. It fails loudly if the move missed a file, which is
exactly what you want — `dbt init` writes `.gitignore` into the project root and
a plain `mv tesco/* .` silently leaves it behind.

The result is `dbt/dbt_project.yml`, `dbt/models/`, `dbt/macros/`, and so on,
with no `tesco/` in the middle. `dbt_project.yml` still names both the project
and its profile `tesco`, which is what ties it to `profiles.yml` below.

### 3. Where `profiles.yml` lives

`dbt init` writes your credentials to `~/.dbt/profiles.yml`, outside the repo.
This project keeps them next to the dbt project instead, so the whole setup is
visible in one tree:

- `dbt/profiles.yml` — your real host, HTTP path, and token. **Git-ignored.**
- `dbt/profiles.yml.example` — the same file with placeholders, committed.

```bash
cp dbt/profiles.yml.example dbt/profiles.yml   # then fill in host, http_path, token
```

dbt looks for `profiles.yml` in the current directory before falling back to
`~/.dbt/`, which is why every dbt command in this README runs from `dbt/`. From
anywhere else, pass `--profiles-dir dbt`.

The token in that file is a live credential. `gitleaks` runs in pre-commit as a
backstop and `dbt/.gitignore` covers both `profiles.yml` and `.user.yml`, but if
a token ever does reach a remote, rotate it in Databricks rather than trying to
rewrite history.

### 4. The certificate error, and the one command that fixes it

On macOS with a python.org (framework) Python, TLS fails twice — once while
installing the packages, once on `dbt debug` — and the two look like unrelated
problems. They are the same problem, and it is neither dbt's fault nor your
network's.

**Symptom 1 — installing.** A certificate error while fetching from PyPI:
expired, unable to verify, or self-signed in chain, depending on which tool
reports it.

**Symptom 2 — `dbt debug`.** Every configuration check passes, then the
connection test blows up:

```
Connection:
  host: <your-workspace>.cloud.databricks.com
  http_path: /sql/1.0/warehouses/<id>
  catalog: tesco
  schema: dbt_schema
Registered adapter: databricks=1.12.4
...
  File ".../urllib3/util/ssl_.py", line 477, in _ssl_wrap_socket_impl
    return ssl_context.wrap_socket(sock, server_hostname=server_hostname)
  File ".../python3.12/ssl.py", line 1320, in do_handshake
    self._sslobj.do_handshake()
ssl.SSLCertVerificationError: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate in certificate chain (_ssl.c:1000)
```

> [!WARNING]
> **`dbt debug` can sit there for fifteen minutes before showing you any of
> that.** `databricks-sql-connector` treats the failed handshake as retryable and
> works through its retry policy first:
>
> ```
> Error during request to server. Retry request would exceed Retry policy
> max retry duration of 900.0 seconds
> Error properties: attempt=1/30, elapsed-seconds=858.7/900.0, method=OpenSession
> ```
>
> If `dbt debug` hangs on *"Opening a new connection"*, do not wait it out.
> Ctrl-C and check your certificate store.

**The cause.** The python.org installer ships **no CA bundle at all** until you
run its post-install script. The interpreter has nothing to validate any chain
against, and OpenSSL reports that missing trust anchor as *"self-signed
certificate in certificate chain"* — which sends everybody hunting for a
corporate proxy that does not exist. Two commands confirm it:

```bash
ls -l /Library/Frameworks/Python.framework/Versions/3.12/etc/openssl/
# empty - there is no cert.pem

uv run python -c "import ssl; print(ssl.get_default_verify_paths())"
# openssl_cafile='/Library/Frameworks/.../etc/openssl/cert.pem'   <- a file that does not exist
```

A venv built from that interpreter inherits the problem, which is why `uv` and
`dbt` fail identically. `curl` and your browser keep working, because they use
the system keychain rather than Python's store — and that asymmetry is what
makes it look like a dbt bug.

**The fix.** Run the installer's certificate script once:

```bash
open "/Applications/Python 3.12/Install Certificates.command"
```

It pip-installs `certifi` into the framework Python and symlinks the missing
store at it (`pip` carries its own bundled certificates, so it works even while
the store is broken). Verify:

```bash
ls -l /Library/Frameworks/Python.framework/Versions/3.12/etc/openssl/cert.pem
# cert.pem -> ../../lib/python3.12/site-packages/certifi/cacert.pem

cd dbt && uv run dbt debug
#   Connection test: [OK connection ok]
# All checks passed!
```

> [!IMPORTANT]
> **This is the whole fix. Do not set `SSL_CERT_FILE` in your shell profile.**
>
> One run repairs every venv, every project, and every tool on the machine —
> including VS Code, whose extension host does not read `~/.zshrc` and so cannot
> see a terminal-only workaround anyway. `export SSL_CERT_FILE=...` will get a
> single command working in a pinch, but as a permanent fix it is a trap:
> pointing it at a path inside a project's `.venv` makes every Python program on
> your machine depend on that venv, and rebuilding `.venv` breaks them all in
> unrelated places. Fix the interpreter instead and set nothing.

### 5. VS Code: the dbt Power User extension

dbt on its own is a CLI. [dbt Power
User](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user)
(`innoverio.vscode-dbt-power-user`) makes it usable in an editor: compiled-SQL
preview beside the model you are editing, a lineage graph built from your
`ref()` calls, go-to-definition across models, column-level autocomplete, and
run / test buttons per model. None of it is required — every command in this
README is CLI-first and stays that way — but on a project with a Jinja-heavy
model layer it saves a lot of `dbt compile && cat target/...`.

Installing it pulls in three extensions it depends on, so you do not install
them separately:

| Extension | Why |
| --- | --- |
| `samuelcolvin.jinjahtml` | Jinja-aware SQL syntax highlighting |
| `ms-python.python` | Supplies the interpreter the extension runs dbt with |
| `altimateai.vscode-altimate-mcp-server` | Optional Altimate AI features; harmless if unused |

**The one thing you must get right is the interpreter.** The extension does not
bundle dbt — it shells out to whichever Python the Python extension has
selected, so that interpreter has to be the one holding `dbt-core`. In this repo
that is the venv at the **repo root**, one level *above* the dbt project:

```
tesco/
├── .venv/bin/python        <- select this: it has dbt-core 1.11.14
└── dbt/dbt_project.yml     <- the dbt project the extension discovers
```

`.vscode/settings.json` is committed and already points at it:

```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "files.associations": { "dbt/**/*.sql": "jinja-sql" },
  "dbt.perspectiveTheme": "Pro Dark"
}
```

The `files.associations` glob is scoped to `dbt/` on purpose — `jinja-sql` is
right for models, but `scripts/sql/raw_data/ddl.sql` is generated plain SQL and
stays plain SQL. `.vscode/extensions.json` recommends the extension, so a
teammate opening the repo is prompted rather than told.

Two settings you should *not* need to touch: `dbt.dbtIntegration` already
defaults to `core`, which is correct here — this is dbt Core against a SQL
warehouse, not dbt Cloud. And there is no profiles-directory setting to
configure, because the extension runs dbt with the project directory as its
working directory, and `dbt/profiles.yml` sits right next to
`dbt/dbt_project.yml`. That is the practical reason the credentials were moved
out of `~/.dbt/` in [step 3](#3-where-profilesyml-lives) — the CLI and the
extension then find the same file with no configuration at all.

If the extension still reports that dbt is not installed, set
`dbt.dbtPythonPathOverride` to the **absolute** path of `.venv/bin/python`. VS
Code does not expand `${workspaceFolder}` in arbitrary extension settings, so a
variable will not work there:

```json
{ "dbt.dbtPythonPathOverride": "/Users/you/projects/tesco/.venv/bin/python" }
```

> [!IMPORTANT]
> **Fix the certificate store before you trust this extension's error
> messages.** It runs dbt itself, so a broken trust store
> ([step 4](#4-the-certificate-error-and-the-one-command-that-fixes-it)) surfaces
> here as a failed connection, a stalled lineage graph, or a query preview that
> never returns — with no terminal output to explain why. Get `uv run dbt debug`
> printing *"All checks passed!"* on the command line first, then open VS Code.

---

## The silver layer in dbt

With dbt connected, the project stops being a scaffold. This section is the
model layer: six incremental silver models over the bronze tables Stage 2 lands,
a generated one-big-table on top of them, and the tests that keep both honest.

> [!NOTE]
> These models read from `tesco.bronze`, which **Stage 2 produces**. Until the
> CDC pipeline has landed those tables, `dbt run` will fail with *"Table or view
> not found"* — that is expected, not a broken model. `dbt parse` and
> `dbt compile` work regardless, so you can develop and review the SQL before
> the data exists.

### 1. Declare the sources, do not hard-code the catalog

`dbt/models/source/sources.yml` names the six bronze tables once:

```yaml
sources:
  - name: tesco_databricks
    database: tesco
    schema: bronze
    tables:
      - name: orders
      - name: customers
      - name: products
      - name: order_items
      - name: employees
      - name: stores
```

Every model then reads `{{ source('tesco_databricks', 'orders') }}` rather than
`tesco.bronze.orders`. Two things fall out of that. dbt now knows bronze is
upstream, so `dbt run --select source:tesco_databricks+` and the lineage graph
work. And if bronze ever moves catalog or schema, it moves in this one file
instead of in six.

### 2. Silver: incremental models that merge on the primary key

All six silver models are the same shape. `dbt/models/silver/customers.sql` in
full:

```sql
{{
  config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'customer_id'
    )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS processed_at
FROM
    {{ source('tesco_databricks', 'customers') }}

{% if is_incremental() %}

    WHERE updated_timestamp > (
        SELECT COALESCE(
            MAX(updated_timestamp),
            '1900-01-01'
        )
        FROM {{ this }}
    )

{% endif %}
```

The other five differ only in the source table and the `unique_key`
(`order_id`, `product_id`, `order_item_id`, `employee_id`, `store_id`).

Four decisions worth understanding, because they are the whole pattern:

**`materialized = 'incremental'`.** On the first run — or any run with
`--full-refresh` — dbt creates the table from the full `SELECT`. On every run
after that it runs the same `SELECT` and merges the result into the existing
table. You are not rebuilding 30,000 order lines to pick up yesterday's changes.

**`incremental_strategy = 'merge'` with a `unique_key`.** The default strategy
would *append*, which turns every update into a duplicate row. `merge` compiles
to a Delta `MERGE INTO ... ON target.customer_id = source.customer_id`, so a
changed row updates in place and a new one inserts. This is the strategy that
makes an incremental model idempotent: re-running it after a partial failure
converges instead of doubling.

**The `is_incremental()` watermark.** The `WHERE` clause only exists on
incremental runs — on a full refresh the block is not rendered at all, because
`{{ this }}` does not yet hold anything to compare against. When it does render,
it limits the scan to rows whose `updated_timestamp` is newer than the newest
one already in the target. This is what turns "merge the whole table every time"
into "merge only what moved".

**Why `COALESCE(..., '1900-01-01')`.** On the first *incremental* run after a
full refresh the target could be empty, and `MAX(updated_timestamp)` over an
empty table returns `NULL`. `updated_timestamp > NULL` is not false — it is
`NULL`, which filters out **every** row, and the model silently loads nothing.
The sentinel date guarantees the comparison is always against a real value. This
is the single most common way a hand-written incremental model quietly does
nothing, and it produces no error to tell you.

`processed_at` is stamped with `CURRENT_TIMESTAMP()` on the way through, so
every silver row records when dbt touched it — distinct from
`created_timestamp` / `updated_timestamp`, which describe when the *source* row
changed. The `is_active` flag is carried through untouched: silver is still a
faithful copy, and deciding what a soft-deleted row means is a gold-layer
concern.

### 3. The macro that stops dbt renaming your schemas

`dbt_project.yml` routes the models to a `silver` schema:

```yaml
models:
  tesco:
    silver:
      +materialized: table
      +schema: silver
```

By default that does **not** produce a schema called `silver`. dbt's built-in
`generate_schema_name` concatenates, so `+schema: silver` against a target
schema of `default` gives you `default_silver`. That behaviour exists so several
developers can build into one warehouse without colliding, and it is exactly
wrong for a medallion layout where `bronze`, `silver`, and `gold` are fixed
names in the catalog.

`dbt/macros/custom_schema.sql` overrides it:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

A model with no `+schema` still lands in the target schema from
`profiles.yml`; a model with one lands in exactly that schema, verbatim. dbt
picks the macro up by name — there is nothing to register.

> [!TIP]
> Confirm it before you run anything expensive. `uv run dbt compile` then read
> the header of `target/run/tesco/models/silver/customers.sql` — the fully
> qualified name should be `tesco.silver.customers`, not
> `tesco.default_silver.customers`.

### 4. The OBT, generated instead of typed

`dbt/models/silver/obt.sql` flattens all six silver models into one wide table
for analysts who want to answer questions without writing a six-way join. Doing
that by hand is roughly 70 columns of aliased `SELECT` list and five `LEFT
JOIN`s — tedious to write and worse to review, because the column list and the
join list drift apart.

Instead the model declares the joins as data:

```sql
{% set configs = [
    {
        "model": "orders",
        "columns": """ o.order_id, o.store_id, o.order_timestamp, ... """,
        "alias": "o"
    },
    {
        "model": "customers",
        "columns": """ c.customer_id, c.first_name AS customer_first_name, ... """,
        "alias": "c",
        "join_condition": "o.customer_id = c.customer_id"
    },
    ...
] %}

SELECT
    {% for cfg in configs %}
        {{ cfg['columns'] }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM
    {% for cfg in configs %}
        {% if loop.first %}
            {{ ref(cfg['model']) }} AS {{ cfg['alias'] }}
        {% else %}
LEFT JOIN
        {{ ref(cfg['model']) }} AS {{ cfg['alias'] }}
        ON {{ cfg['join_condition'] }}
        {% endif %}
    {% endfor %}
```

The first entry is the anchor and gets no `join_condition`; every other entry
contributes one `LEFT JOIN`. Adding a dimension to the OBT is one dict, and the
column list and the join cannot fall out of sync because they are the same
object.

**The tables come from `ref()`, not from hard-coded names,** and that matters
more than it looks. dbt builds its DAG by instrumenting `ref()` during the parse
phase: every call is recorded as an edge and returns a `Relation` that renders
to the fully-qualified name. Because the `{% set %}` block and the loop always
evaluate, a `ref()` buried inside a loop over a dict registers exactly as a
top-level one would. Two consequences:

- **dbt orders the build for you.** `dbt run` builds the six silver models
  before the OBT; `dbt build --select +obt` selects the whole upstream chain;
  `dbt run --select customers+` rebuilds customers and everything downstream.
  Hard-coded names produce an orphan node that dbt will happily build *first*,
  against yesterday's data.
- **It resolves per target.** `tesco.silver.orders` written out literally means a
  dev run building into `tesco.dev_silver` still reads *production* silver —
  silently, with no error. `ref()` goes through the same
  `generate_schema_name` macro the silver models do, so dev reads dev.

One cost, worth knowing and not worth avoiding: `ref()` with a variable argument
defeats dbt's fast static parser, so this model falls back to full Jinja
rendering at parse time. That is milliseconds, and it changes nothing about the
compiled SQL.

> [!TIP]
> **Confirm the edges actually registered — before you need them.** A `ref()`
> that never renders produces no error, just a silently orphaned node, so check
> the DAG rather than trusting the SQL. Neither command touches the warehouse:
>
> ```bash
> cd dbt
> uv run dbt ls --select +obt --resource-type model --output name
> ```
>
> The `+` prefix means *"this node and everything upstream of it"*. If `ref()`
> is wired up, all six silver models come back alongside the OBT:
>
> ```
> customers
> employees
> obt
> order_items
> orders
> products
> stores
> ```
>
> If it prints `obt` on its own, the model is an orphan and dbt is free to build
> it before the tables it reads. Drop `--resource-type model` to see the whole
> upstream chain — the six bronze sources as `tesco_databricks.*`, and the tests
> that will run against them:
>
> ```bash
> uv run dbt ls --select +obt --output name
> ```
>
> To see the resolved SQL rather than the graph, `uv run dbt compile --select
> obt` and read `target/compiled/tesco/models/silver/obt.sql` — the `FROM` and
> `LEFT JOIN` clauses should name `` `tesco`.`silver`.`orders` `` and friends,
> fully qualified and backtick-quoted by the adapter.

The loop variable is `cfg`, not `config`, deliberately — `config` is dbt's own
context object, and shadowing it inside the loop would break any later
`{{ config.get(...) }}` in the same block in a way that is genuinely hard to
spot.

Two details in the column lists are deliberate:

- **Collisions are aliased by entity.** `customers`, `employees`, and `stores`
  all have a `city`; two have an `email` and a `first_name`. They come out as
  `customer_city` / `store_city`, `customer_email` / `employee_email`, and so
  on. Without this the model fails to build, or worse, builds with ambiguous
  duplicates.
- **The audit columns survive the flattening.** Each entity's
  `created_timestamp`, `updated_timestamp`, `is_active`, and `processed_at` are
  carried through prefixed (`order_is_active`, `product_processed_at`, …), and
  the last entry adds `CURRENT_TIMESTAMP() AS obt_processed_at`. A wide table
  that has lost its provenance cannot be reconciled against bronze; this one can.

The joins are `LEFT` on purpose. An order line whose product row has not
replicated yet should still appear in the OBT with `NULL` product columns,
rather than vanishing from the analysts' table because of a race in the CDC
feed. That choice is what the OBT test below exists to police.

### 5. Two kinds of test

**Generic tests**, in `dbt/models/silver/properties.yml`, are the declarative
ones dbt ships with:

```yaml
models:
  - name: products
    columns:
      - name: product_id
        data_tests:
          - not_null
          - unique:
              config:
                where: "price > 0"

  - name: orders
    columns:
      - name: order_id
        data_tests:
          - not_null
          - unique
```

The `where` on the products uniqueness check is worth noticing: it scopes the
test to priced rows, which is how you assert a rule that genuinely only holds
for part of the table without either weakening it everywhere or failing the run
on rows it was never meant to cover.

**Singular tests** are just a query that should return no rows.
`dbt/tests/test_obt.sql` checks that the OBT's keys all survived their joins:

```sql
{{ config(severity='warn') }}

SELECT 1
FROM {{ ref('obt') }} AS obt
WHERE obt.order_id IS NULL
   OR obt.product_id IS NULL
   OR obt.customer_id IS NULL
   OR obt.order_item_id IS NULL
   OR obt.employee_id IS NULL
   OR obt.store_id IS NULL
```

`severity='warn'` is the point of it. Because the joins are `LEFT`, a `NULL`
key means a real referential gap in the source data — an order whose customer
has not arrived, an order line pointing at a product that was deleted. You want
to *know* that, and you do not want it to fail the build and block every
downstream model, because the gap may well be legitimate mid-replication. Warn,
look, then decide.

Run them:

```bash
cd dbt
uv run dbt run                 # build silver, then the OBT
uv run dbt test                # generic + singular tests
uv run dbt build               # or: run and test model by model, in DAG order
```

`dbt build` is usually what you want day to day — it tests each model as soon as
it is built, so a broken model stops its own dependents instead of poisoning
them.

> [!TIP]
> `dbt run` on a model whose SQL you just changed will **merge into the old
> table**, because incremental models do not notice that their definition moved.
> After editing a silver model, run `uv run dbt run --full-refresh --select
> customers` to rebuild it from scratch. Forgetting this is why a column you
> just added shows up empty.

---

## Repository layout

```
tesco/
├── data/                          # Six source CSVs (42,796 rows)
├── scripts/
│   ├── python/
│   │   ├── loader.py              # Shared: profiling, inference, DDL, COPY
│   │   ├── load_data.py           # Entry point -> local Postgres
│   │   └── load_supabase.py       # Entry point -> hosted Postgres
│   └── sql/
│       └── raw_data/ddl.sql       # GENERATED - do not edit by hand
├── dbt/
│   ├── models/
│   │   ├── source/sources.yml     # The six bronze tables, declared once
│   │   └── silver/                # Incremental models + the generated OBT
│   │       ├── customers.sql      # (one per source table)
│   │       ├── obt.sql            # One Big Table, joins declared as data
│   │       └── properties.yml     # Generic tests: not_null, unique
│   ├── macros/custom_schema.sql   # generate_schema_name override
│   ├── tests/test_obt.sql         # Singular test: no NULL keys in the OBT
│   ├── dbt_project.yml
│   ├── profiles.yml.example       # Copy to profiles.yml - GIT-IGNORED
│   └── profiles.yml               # Your host / http_path / token - never committed
├── .vscode/                       # Interpreter + extension recommendations
├── docker-compose.yaml            # Local Postgres 16
├── .env.example                   # Every variable, documented
├── .pre-commit-config.yaml        # ruff, sqlfluff, gitleaks, hygiene
├── .github/workflows/             # CI runs the same hooks
├── CHANGELOG.md
└── README.md
```

`ddl.sql` is generated output. To change the schema, change the inference rules
in `loader.py` and re-run — do not hand-edit the SQL.

`dbt/profiles.yml` holds a live Databricks token and is git-ignored. Commit
changes to `profiles.yml.example` instead.

### Development

```bash
uv sync --dev
uv run pre-commit install
uv run pre-commit run --all-files
```

CI runs the same hooks, so a clean local run means a clean pull request.
`sqlfluff` lints the generated SQL too, which is why `loader.py` emits single
spaces and no column alignment padding.

---

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `could not translate host name ... to address` | IPv6-only Supabase direct host on an IPv4 network. Use the Session pooler URI. |
| `tenant or user not found` | Wrong pooler region, or username is `postgres` instead of `postgres.<project-ref>`. Copy the host from the dashboard. |
| `Connection refused` on localhost | Container is not up. `docker compose up -d`, then `docker ps` to confirm it is healthy. |
| `Missing environment variable(s)` | `.env` absent or incomplete. `cp .env.example .env` and fill it in. |
| `password authentication failed` | Free-tier projects pause after inactivity; resume from the dashboard. Also re-check the password after any reset. |
| Load fails partway | Nothing was committed — the run is one transaction. Fix the cause and re-run. |
| Databricks cannot reach your database | Free Edition restricts outbound access until the account is verified. Verify, then retry. |
| Databricks PostgreSQL connector will not connect to Supabase | You gave it the direct host. Use the Session pooler host `aws-<n>-<region>.pooler.supabase.com` with username `postgres.<project-ref>` — see [Path A](#path-a--lakeflow-connect-log-based). |
| Source disk filling up during CDC | A replication slot with no live consumer. Restart the gateway, or drop the slot with `pg_drop_replication_slot`. |
| `uv add dbt-core dbt-databricks` will not resolve | `dbt-databricks` 1.12.4 needs `dbt-core>=1.11.2,<1.12.1`. Install `"dbt-core<1.12"` — see [the version pin](#1-pin-dbt-core-below-112-or-resolution-fails). |
| `dbt --version` says dbt-core is out of date | Expected and correct. The pin exists because the adapter cannot use 1.12.x. Do not upgrade. |
| `dbt init` created `dbt/tesco/` instead of using `dbt/` | Expected — it always makes a subdirectory. Flatten it: `mv tesco/* tesco/.[!.]* . && rmdir tesco`. See [scaffolding](#2-scaffold-the-project-then-flatten-it). |
| `CERTIFICATE_VERIFY_FAILED ... self-signed certificate in certificate chain` | Framework Python has no CA bundle. Run `Install Certificates.command` — see [the certificate error](#4-the-certificate-error-and-the-one-command-that-fixes-it). Not a proxy, not your token. |
| `dbt debug` hangs for ~15 minutes, then `Retry policy max retry duration of 900.0 seconds` | Same certificate problem — the SQL connector retries the failed TLS handshake before reporting it. Fix the trust store. |
| dbt works in the terminal but fails inside VS Code | You patched `SSL_CERT_FILE` in your shell profile; the extension host never reads it. Fix the trust store instead — see [the certificate error](#4-the-certificate-error-and-the-one-command-that-fixes-it). |
| dbt Power User reports dbt is not installed | It runs dbt from the interpreter the Python extension selected. Pick `.venv/bin/python` at the repo root, or set `dbt.dbtPythonPathOverride` to its absolute path — see [VS Code](#5-vs-code-the-dbt-power-user-extension). |
| `Could not find profile named` / dbt cannot find credentials | Run dbt from `dbt/`, or pass `--profiles-dir dbt`. `dbt/profiles.yml` is git-ignored; copy it from `profiles.yml.example` — see [where it lives](#3-where-profilesyml-lives). |
| `Table or view not found: tesco.bronze.*` | Stage 2 has not landed the bronze tables yet. Expected until CDC runs — `dbt compile` still works. |
| Silver tables land in `<target_schema>_silver` | dbt's default `generate_schema_name` concatenates. The `custom_schema.sql` macro overrides it — see [the schema macro](#3-the-macro-that-stops-dbt-renaming-your-schemas). |
| An incremental model loads zero rows on its first incremental run | `MAX(updated_timestamp)` over an empty table is `NULL`, and `x > NULL` matches nothing. Keep the `COALESCE(..., '1900-01-01')` — see [the watermark](#2-silver-incremental-models-that-merge-on-the-primary-key). |
| A column you just added to a model is empty | Incremental models merge into the existing table. Rebuild with `dbt run --full-refresh --select <model>`. |
| Rows duplicate on every run | The model lost `incremental_strategy = 'merge'` or its `unique_key`; the default strategy appends. |
| `test_obt` warns about `NULL` keys | Working as intended. The OBT uses `LEFT JOIN`, so a `NULL` key is a real referential gap in the source, surfaced at `warn` rather than failing the build. |
| `sqlfluff` fails on dbt models in pre-commit | Layout rules, not SQL errors. `sqlfluff-fix` runs in the same hook and rewrites them; re-stage and commit again. |

---

## Roadmap

- [x] Synthetic UK retail dataset
- [x] Local Postgres via Docker, schema generated from the CSVs
- [x] Load to hosted Postgres (Supabase, or any public Postgres)
- [ ] **CDC into a Databricks catalog** ← current stage
- [ ] **Bronze → silver transformations in dbt, with data quality expectations** — models, the OBT, and the tests are written; they run once Stage 2 lands bronze
- [ ] Gold-layer dimensional model (star schema over orders)
- [ ] Orchestration and scheduling
- [ ] Tests and data quality gates in CI

---

## A note on the data

Everything in `data/` is synthetic and safe to publish. Phone numbers use the
Ofcom-reserved drama ranges (`07700 900xxx`, `020 7946 0xxx`, `01632 960xxx`) and
all emails use `example.*` domains, so no value corresponds to a real person or
line. Monetary values read as GBP.

Never commit `.env`. It is git-ignored, and `gitleaks` runs in pre-commit as a
second line of defence — but if a credential does reach a remote, rotate it
rather than trying to rewrite history.

This is an independent learning project. It is not affiliated with, endorsed by,
or connected to Tesco plc; the name and logo are used only to give the synthetic
dataset a recognisable retail theme.

## License

[MIT](LICENSE)
