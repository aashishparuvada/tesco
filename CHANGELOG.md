# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.1.0]: https://github.com/aashishparuvada/tesco/
[1.0.0]: https://github.com/aashishparuvada/tesco/
