# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-08-24

Takes the pipeline off the laptop. Adds a hosted-Postgres target so the data
sits somewhere a cloud warehouse can reach, and documents the project as a
follow-along data engineering tutorial.

### Added

#### Hosted Postgres ingestion

- `scripts/python/load_supabase.py` — loads every CSV in `data/` into the
  `raw_data` schema of a hosted Postgres. Behaviour is identical to the local
  loader; only the connection differs.
  - **Connection string** — read from `SUPABASE_CONNECTION_STRING`. A literal
    `{SUPABASE_DB_PASSWORD}` placeholder in it is substituted at runtime and
    percent-encoded, so the password lives in one place and special characters
    cannot break URL parsing.
  - **TLS** — `sslmode=require` is appended when absent. psycopg2 would
    otherwise only *prefer* TLS and could silently fall back to plaintext.
  - **Redacted logging** — the target is printed without the password.
  - **Actionable connection errors** — a failure prints guidance covering the
    IPv6-only direct host, the pooler alternative, and paused free-tier
    projects, rather than a bare DNS error.
- `.env.example` — now documents the Supabase variables
  (`SUPABASE_CONNECTION_STRING`, `SUPABASE_DB_PASSWORD`), the optional
  `POSTGRES_HOST`, and is split into local and hosted sections. The pooler URI
  is the documented default, with the direct URI commented out beneath it.

#### Documentation

- `README.md` — written from placeholder to a full guide: what the project
  teaches, an architecture diagram, the dataset, which accounts to create and
  **when**, verified walkthroughs for the local and hosted loads, the Stage 2
  CDC design, repository layout, and a troubleshooting table.
  - Highlights, as a callout on the Databricks connector step, that a Supabase
    source must be registered with its **session pooler** credentials rather
    than the direct host, including the host/port/user comparison. The direct
    host does not connect over IPv4, so the managed connector cannot reach it.
  - Records the counterpart caveat: a connection that tests green is not proof
    replication will run, since Supabase documents that logical replication does
    not pass through the pooler.
- `assets/images/tesco-logo.png` — logo used in the README heading, stored in
  the repository rather than hotlinked so the page does not depend on a
  third-party CDN. Cropped to the wordmark and given a transparent background so
  it renders correctly in both light and dark themes.
- A note that the project is independent and not affiliated with or endorsed by
  Tesco plc.
- `scripts/sql/raw_data/ddl.sql` — the generated schema definition is now
  tracked, so schema changes appear in review.

### Changed

- **Shared loader module.** The profiling, type-inference, DDL-generation and
  `COPY` logic moved out of `load_data.py` into `scripts/python/loader.py`.
  `load_data.py` and `load_supabase.py` are now thin entry points that differ
  only in how they connect, so the two targets cannot drift apart.
- **Schema creation is conditional.** `CREATE SCHEMA` is no longer issued
  unconditionally: `information_schema.schemata` is checked first and the schema
  created only when genuinely missing. Issuing it unconditionally would require
  privileges the connecting role may not hold on a hosted database. The
  generated `ddl.sql` still carries `CREATE SCHEMA IF NOT EXISTS` so the file
  remains runnable on its own.
- Both loaders now set a 10-second connection timeout, so an unreachable host
  fails promptly instead of hanging.
- `pyproject.toml` — the package is renamed from `walmart` to `tesco` and its
  version tracks the release, having sat at `0.1.0` since the scaffold was
  created.

### Fixed

- `docker-compose.yaml` mounted `${DATASETS_PATH}`, a variable no `.env` defined
  or documented, leaving the container's `/data` mount pointing nowhere. It now
  uses `${DATA_PATH}`, matching `.env.example`.

### Notes

- **Both loads are now verified end-to-end**, superseding the note in 1.1.0:
  42,796 rows into local Postgres 16 and into hosted Postgres 17.6, with row
  counts, inferred types, primary keys and cross-table referential integrity
  checked on the target rather than trusted from the script's own output.
- **Hosted Supabase requires the connection pooler.** The direct host
  `db.<project-ref>.supabase.co` resolves to an IPv6 address only, so it is
  unreachable from an IPv4-only network. The session pooler
  (`aws-<n>-<region>.pooler.supabase.com`, port 5432, username
  `postgres.<project-ref>`) is IPv4-compatible. Both the `aws-<n>-` prefix and
  the region are per-project and cannot be guessed. Session mode is preferred
  over transaction mode because the loader copies inside one transaction.
- **Two blockers stand between this release and log-based CDC**, both documented
  vendor limits and both recorded in the README:
  - Lakeflow Connect's ingestion gateway runs on classic compute, while
    Databricks Free Edition is serverless-only — so the managed PostgreSQL
    connector needs a paid or trial workspace.
  - Logical replication cannot pass through Supabase's pooler and the direct
    host is IPv6-only, so CDC from a free Supabase project is not reachable;
    it needs the IPv4 add-on (Pro plan) or a different host such as Neon.

  The README therefore documents a second path — Lakehouse Federation plus
  `AUTO CDC` sequenced on `updated_timestamp` — that runs on free tiers.
- The Stage 2 SQL in the README is adapted from vendor documentation and is
  **not** yet executed by anything in this repository. The Lakeflow Connect
  PostgreSQL connector is in Public Preview and its details may change.
- Credentials belong only in `.env`, which stays git-ignored; `.env.example`
  carries placeholders. Rotate anything that reaches a remote rather than
  rewriting history.

## [1.1.0] - 2026-08-24

Makes the dataset loadable. Adds an ingestion script that derives the `raw_data`
table definitions from the CSVs themselves, plus the code-quality tooling that
guards the repository.

### Added

#### Raw layer ingestion

- `scripts/python/load_data.py` — loads every CSV in `data/` into the
  `raw_data` schema. The table definitions are generated rather than
  hand-written:
  - **Discovery** — every `*.csv` in `data/` becomes a table named after the
    file stem. Adding a new CSV is the only step needed to get a new table; the
    script itself does not change.
  - **Profiling** — each file is read once to record, per column, whether every
    value parses as an integer, decimal, timestamp or date, along with the
    longest value, the largest integer, the maximum numeric scale, whether any
    value is empty, and the distinct count.
  - **Type inference** — the narrowest type that still fits every observed
    value: `TIMESTAMP`, `DATE`, `BIGINT` for `*_id` columns (and for integers
    beyond int32), `INTEGER` otherwise, `NUMERIC(p, s)`, `CHAR(1)` for
    single-character columns, and a bucketed `VARCHAR` as the fallback.
  - **Key detection** — a leading `*_id` column that is unique and never empty
    becomes the `PRIMARY KEY`.
  - **Schema creation** — `CREATE SCHEMA IF NOT EXISTS raw_data` runs first, so
    a fresh database needs no manual preparation. Each table is then rebuilt
    with `DROP TABLE IF EXISTS ... CASCADE` followed by `CREATE TABLE`.
  - **Loading** — `COPY ... FROM STDIN`, after which each table is re-counted
    and the run fails if the row count does not match the source CSV.
  - **Connection** — read from `.env` (`POSTGRES_DB`, `POSTGRES_USER`,
    `POSTGRES_PASSWORD`, `POSTGRES_PORT`, and an optional `POSTGRES_HOST`
    defaulting to `localhost`) by a small built-in parser. Real environment
    variables take precedence, so the same script runs unchanged in CI.
- `scripts/sql/` — reserved for sql scripts.

#### Tooling

- `.pre-commit-config.yaml` — ruff (lint + format), nbQA and nbstripout for
  notebooks, sqlfluff (lint + fix), general hygiene hooks (trailing whitespace,
  end-of-file, YAML validity, merge conflicts, private keys, 5 MB file cap), and
  gitleaks secret scanning.
- `.sqlfluff` — sqlfluff configuration: 120-character lines, uppercase
  keywords, and `indented_using_on`.
- `.github/workflows/quality-checks.yml` — runs the pre-commit hooks on every
  pull request and on pushes to `main`, plus a guard that fails any diff
  introducing a file over 5 MB.

### Changed

- `pyproject.toml` — added `psycopg2-binary>=2.9.12` as a runtime dependency and
  `pre-commit>=4.6.2` as a dev-group dependency. `uv.lock` is now committed, so
  the environment is reproducible.

### Notes

- The whole load runs in a single transaction: either every table is rebuilt and
  loaded, or the database is left exactly as it was. There is no partially
  loaded state to clean up after a failure.
- `raw_data` is a landing layer, so only the primary key is `NOT NULL`; every
  other column stays nullable and accepts whatever the source sends. Constraints
  and conformed types belong in the bronze layer.
- Inferred `VARCHAR` lengths and `NUMERIC` precision carry deliberate headroom
  (widths round up to at least twice the longest observed value), so a slightly
  longer value in a later extract does not break the load.
- `ddl.sql` is generated output — edit `load_data.py` and re-run, rather than
  editing the SQL by hand. Its formatting is chosen to satisfy the sqlfluff
  rules enforced by pre-commit and CI.
- The load has been verified only up to the database boundary so far: CSV
  profiling, key detection, and the generated DDL are confirmed (the file parses
  as PostgreSQL and passes `sqlfluff lint`), and the Python passes `ruff check`
  and `ruff format`. The end-to-end run against a live Postgres instance is
  still to be exercised.

## [1.0.0] - 2026-08-24

Initial setup. Establishes the project skeleton for a Tesco (UK retail) data
engineering project: a containerised Postgres instance, environment-based
configuration, and a synthetic UK retail dataset to load into it.

### Added

#### Project scaffold

- `pyproject.toml` — Python project definition, targeting Python >= 3.12 (pinned
  via `.python-version`). No third-party dependencies yet.
- `main.py` — placeholder entry point.
- `scripts/` — empty directory reserved for ingestion / transformation scripts.
- `LICENSE` — MIT.
- `README.md` — placeholder, to be written.
- `.gitignore` — ignores Python build artefacts, `.venv`, `.DS_Store`, and `.env`.

#### Local database environment

- `docker-compose.yaml` — Postgres 16 service with:
  - container name, port, credentials and database name driven entirely by `.env`;
  - a named host path mounted at `/var/lib/postgresql/data` so data survives
    container restarts;
  - the local `data/` directory mounted read-write at `/data` inside the
    container, so the CSVs can be loaded with server-side `COPY`;
  - a `pg_isready` healthcheck (5s interval, 5 retries);
  - `restart: unless-stopped`.
- `.env.example` — template for the required variables:
  `POSTGRES_CONTAINER_NAME`, `POSTGRES_USER`, `POSTGRES_PASSWORD`,
  `POSTGRES_DB`, `POSTGRES_PORT`, `POSTGRES_DATA_PATH`, `DATA_PATH`.
  The real `.env` is git-ignored.

#### Sample dataset (`data/`)

Six CSVs modelling a retail chain, with headers in the first row and referential
integrity across all relationships:

| File | Rows | Grain |
| --- | ---: | --- |
| `stores.csv` | 25 | one row per store |
| `employees.csv` | 250 | one row per employee, `store_id` FK |
| `customers.csv` | 2,000 | one row per customer |
| `products.csv` | 500 | one row per product |
| `orders.csv` | 10,000 | one row per order, `customer_id` + `store_id` FKs |
| `order_items.csv` | 30,021 | one row per order line, `order_id` + `product_id` FKs |

Every table carries `created_timestamp`, `updated_timestamp` and an `is_active`
(`Y`/`N`) flag, so the data supports incremental-load and
slowly-changing-dimension exercises rather than full reloads only.

Order activity spans **2026-01-01 to 2027-01-01**. Orders resolve to one of
`Pending`, `Completed`, `Cancelled`, `Returned`, paid by `Cash`, `Credit Card`,
`Debit Card`, `Gift Card` or `Online`.

#### UK dataset

The dataset was generated on par to a UK/Tesco context:

- **Stores** — named in Tesco store formats (`Tesco Extra`, `Superstore`,
  `Metro`, `Express`), each in a distinct UK city with its matching county or
  council area.
- **Customers & employees** — UK given names and surnames, `.co.uk` /
  `.org.uk` email addresses derived from those names, and UK phone number
  formats (mobile, London, and regional landline).
- **Geography** — `city` and `province` hold real UK city/county pairs;
  `country` is `United Kingdom` throughout.
- **Employees** — `job_title` uses Tesco retail roles (Customer Assistant,
  Checkout Operator, Shift Leader, Personal Shopper, Bakery Assistant,
  Pharmacist, Store Manager and others), with `salary` in GBP within a
  realistic band for each role. Each store has exactly one Store Manager and
  one Deputy Store Manager; the remainder skew towards shop-floor roles.
- **Products** — UK product names with size, colour or age variants; categories
  are `Grocery`, `Clothing`, `Electronics`, `Home & Kitchen`,
  `Sports & Leisure` and `Toys & Games`; brands are UK/Tesco-appropriate and
  consistent with their category (Tesco Finest, F&F, Fox & Ivy, Go Cook,
  Technika, Carousel, Cadbury, Walkers, Slazenger and others).

### Notes

- All data is **synthetic**. Phone numbers use the Ofcom-reserved drama ranges
  (`07700 900xxx`, `020 7946 0xxx`, `01632 960xxx`) and email addresses use
  `example.*` domains, so no value maps to a real person or line.
- Monetary columns are read as **GBP**. `products.price` and
  `order_items.unit_price` were generated such that the two stay
  consistent with each other and with `line_amount` / `total_amount`; product
  prices therefore span roughly £5–£500 across all categories.
- Primary and foreign keys (`store_id`, `employee_id`, `customer_id`,
  `product_id`, `order_id`, `order_item_id`) are surrogate integers starting at
  1 and are stable — downstream scripts can rely on them.

[1.2.0]: https://github.com/aashishparuvada/tesco/
[1.1.0]: https://github.com/aashishparuvada/tesco/
[1.0.0]: https://github.com/aashishparuvada/tesco/
