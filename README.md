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
> design and the setup you need, and will be updated as the pipeline lands.

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
├── docker-compose.yaml            # Local Postgres 16
├── .env.example                   # Every variable, documented
├── .pre-commit-config.yaml        # ruff, sqlfluff, gitleaks, hygiene
├── .github/workflows/             # CI runs the same hooks
├── CHANGELOG.md
└── README.md
```

`ddl.sql` is generated output. To change the schema, change the inference rules
in `loader.py` and re-run — do not hand-edit the SQL.

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

---

## Roadmap

- [x] Synthetic UK retail dataset
- [x] Local Postgres via Docker, schema generated from the CSVs
- [x] Load to hosted Postgres (Supabase, or any public Postgres)
- [ ] **CDC into a Databricks catalog** ← current stage
- [ ] Bronze → silver transformations, with data quality expectations
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
