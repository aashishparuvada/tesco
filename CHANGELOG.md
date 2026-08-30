# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2026-08-30

Adds `guide.html`, a self-contained interactive walkthrough of the whole
project. The README stays the reference document; the guide is the version you
build from, sequenced as a path with a verification step after every command.

### Added

- `guide.html` — a single-file, dependency-free HTML guide (no build step,
  nothing fetched at runtime beyond a Google Fonts stylesheet that degrades to
  a local fallback stack, so it works offline and from `file://`). Sixteen
  sections: an overview, thirteen numbered build steps, a filterable
  troubleshooting reference, and a repository map.
  - **Starts at the hosted database.** Stage 0's local Docker Postgres is
    omitted deliberately — CDC needs a source Databricks can reach, so the
    guide goes straight from the CSVs to a hosted Postgres. `load_data.py`
    remains in the repo and in this README for local development.
  - **Every step ends with a `Verify` block** giving the command to run and the
    exact output to expect, so a mistake surfaces at the step that caused it
    rather than eight steps later.
  - Colour encodes the medallion layer — source, bronze, silver, gold,
    orchestration — across the navigation rail, section eyebrows and controls,
    so the layer you are working in is always visible.
  - `⌘K` command palette searching every step, heading and troubleshooting
    symptom; selecting a symptom opens the table with that filter applied.
    Keyboard navigation (`←`/`→`, `/`, `T`), per-block copy buttons, tabbed
    Path A / Path B for Stage 2, and collapsible deep dives.
  - Progress and theme persist in `localStorage`; steps are deep-linkable by
    `#id`; full light and dark themes, a responsive drawer layout under
    1040px, `prefers-reduced-motion` support, and a print stylesheet that
    expands every step.
  - Syntax highlighting for SQL/Jinja, bash, YAML, JSON, Python and console
    output is hand-rolled in the file itself — no CDN dependency.
- `assets/images/guide.png` — the README preview of the guide, linked to
  `guide.html`.

### Changed

- `README.md` — new **Start here: the interactive guide** section at the top,
  with the preview image, how to open the file (GitHub renders it as source,
  not as a page), and a table setting out what the guide does that a reference
  document cannot. Added to the table of contents and to the repository layout.

## [1.4.0] - 2026-08-30

Adds orchestration. `airflow/` runs the whole pipeline — trigger the existing
Databricks bronze job, then silver, the OBT, and the gold layer — as one DAG
on a hand-written, single-container Airflow stack, and documents both the
`LocalExecutor`-vs-`CeleryExecutor` trade-off and the two setup mistakes made
building it.

### Added

- `airflow/` — a self-contained Airflow 3.3.1 stack, run as its own
  `docker compose` project separate from the repo-root Postgres one:
  - `docker-compose.yaml` — two services (`postgres` for Airflow's metadata
    DB, `airflow` for everything else), `command: standalone` with
    `AIRFLOW__CORE__EXECUTOR: LocalExecutor`. Deliberately not the official
    multi-service `CeleryExecutor` compose — one DAG has nothing for a worker
    fleet to distribute. Mounts `../dbt:/opt/dbt` read-write so a
    DAG-triggered `dbt run` writes `target/`, `logs/`, and `dbt_packages/`
    into the same place a local run does.
  - `Dockerfile` — `apache/airflow:3.3.1-python3.12` plus the same
    `dbt-core<1.12` / `dbt-databricks>=1.12.4` pins as `pyproject.toml`, and
    `databricks-sdk>=0.117.0`.
  - `dags/orchestrate.py` — the `dbt_tesco_pipeline` DAG:
    `databricks_ingest_cdc >> source_freshness >> silver >> silver_test >> obt
    >> obt_test >> gold_ephemeral >> gold_dimensions >> gold_fact`. Every dbt
    step is a `BashOperator` running `cwd='/opt/dbt/'`; `gold_dimensions` runs
    `dbt snapshot` rather than `dbt run`, per [the gold layer](README.md#4-materialization-is-centralized-in-dbt_projectyml)'s
    `dbt run` doesn't build snapshots note.
  - `dags/utils.py` — `establish_databricks_connection()` and
    `trigger_databricks_job(job_id)`, built directly on `databricks-sdk`'s
    `WorkspaceClient` rather than the `apache-airflow-providers-databricks`
    provider. `run_now(job_id=...)` starts the existing Databricks bronze job
    (created in the workspace, not by this repo), then polls `get_run()`
    every 5 seconds until a terminal `life_cycle_state`, raising on anything
    other than `RunResultState.SUCCESS` so the DAG never proceeds to
    `source_freshness` against a partially-written bronze table.
  - `.env.example` — `AIRFLOW_METADATA_CONTAINER_NAME`, `AIRFLOW_CONTAINER_NAME`,
    `AIRFLOW_PORT`, `AIRFLOW_UID`. `DATABRICKS_HOST` / `DATABRICKS_TOKEN` are
    intentionally absent from the template — added directly to the
    git-ignored `.env`, never example-shaped in git history.
- `pyproject.toml` — `apache-airflow>=3.3.1`, `databricks-sdk>=0.117.0`,
  `python-dotenv>=1.2.3` (the DAG's own package list, not shared with the
  `.venv` Stage 0/1 loaders run in — see `airflow/Dockerfile`'s reasoning for
  the separate image).
- `README.md` — new **Orchestrating with Airflow** section: the DAG's task
  chain, the `LocalExecutor`-vs-official-`CeleryExecutor` comparison and why
  `LocalExecutor` is the right call here rather than a shortcut, the SDK-based
  Databricks trigger and why it isn't a provider operator, the `.env`
  gotcha below, and how to swap in the official `docker-compose.yaml` if you
  outgrow one DAG. Plus a new **What you will learn** row, `Repository
  layout` entries for `airflow/`, four new Troubleshooting rows, and an
  updated Roadmap line.

### Fixed

- Editing `airflow/.env` to add `DATABRICKS_HOST` / `DATABRICKS_TOKEN` after
  the `airflow` container already existed, then running `docker compose up
  -d`, did not update the running container's environment —
  `establish_databricks_connection()` kept raising `ValueError` as if the
  file were empty. `env_file:` values are read at container **creation**;
  editing the file's contents alone did not make Compose recreate an
  already-running container the way editing `docker-compose.yaml` itself
  does. Fixed by recreating the container outright: `cd airflow && docker
  compose down && docker compose up -d --build` (`--build` also picked up
  the Dockerfile's new `databricks-sdk` install in the same pass — a plain
  `down`/`up` without it would have recreated the container from the old
  image and failed with `ModuleNotFoundError: No module named 'databricks'`
  instead).

### Removed

- `airflow-operators` from `pyproject.toml`. Added while exploring a
  dedicated Databricks operator package; nothing in `airflow/dags` ends up
  importing it once `utils.py` settled on calling `databricks-sdk` directly,
  so it — and the transitive dependencies it alone pulled in
  (`apache-airflow-providers-apache-kafka`, `apache-airflow-providers-mysql`,
  `confluent-kafka`, `mysql-connector-python`) — is dropped rather than kept
  as dead weight in `uv.lock`.

### Notes

- The Databricks job ID `databricks_ingest_cdc` triggers
  (`utils.trigger_databricks_job(job_id=1088290843715942)`) is a hardcoded
  integer literal in `orchestrate.py`, not read from `.env` or an Airflow
  Variable/Connection. Known rough edge, not a design choice — moving it to
  configuration is expected before this DAG is considered done.
- The Databricks job itself — the bronze ingestion / CDC job this DAG
  triggers — was created directly in the Databricks workspace and is not
  defined by anything in this repository.

## [1.3.2] - 2026-08-26

Builds the gold layer on top of the OBT: five ephemeral shaping models feeding
SCD Type 2 dimension snapshots, and an order-item-grain fact table
materialized directly instead of ephemerally. Documents both in the README
alongside the silver layer.

### Added

- `dbt/models/gold/ephemeral/*.sql` — one ephemeral model per dimension
  (`eph_customers`, `eph_employees`, `eph_orders`, `eph_products`,
  `eph_stores`), each a `SELECT DISTINCT` off `obt` plus a
  `*_gold_processed_at` audit column. `dbt_project.yml` marks the whole
  `ephemeral/` subtree `+materialized: ephemeral`, so none of the five ever
  builds a table or view — each inlines as a CTE inside whatever `ref()`s it.
  `eph_orders` deliberately omits `order_item_id`: it is order-item grain, and
  keeping it in a row-wide `DISTINCT` would produce more than one row per
  `order_id` for any multi-line order, violating `dim_orders`'s
  `unique_key: order_id`.
- `dbt/snapshots/dim_*.yml` — one YAML snapshot config per dimension
  (`dim_customers`, `dim_employees`, `dim_orders`, `dim_products`,
  `dim_stores`), `strategy: timestamp` keyed on the entity's `*_id` and
  `*_updated_timestamp`, `dbt_valid_to_current: "to_date('9999-12-31')"` so the
  current row's `dbt_valid_to` is a sentinel date rather than `NULL`.
  `relation: ref('eph_*')` targets the ephemeral models above; since those have
  no physical relation, dbt inlines their compiled SQL directly into the
  snapshot's own query.
- `dbt/models/gold/fact/fact_orders.sql` — the fact table, at order-item grain,
  carrying an FK to all five dimensions plus the order and line-item measures.
  Ships with no `config()` block: it inherits `+materialized: table` from
  `dbt_project.yml`'s `gold:` block, the same default the `ephemeral/` subtree
  overrides for itself.
- `README.md` — new **The gold layer in dbt** section: why the ephemeral
  models never materialize, the snapshot config and the SCD Type 2 mechanics
  behind it, why the fact table is a plain model instead of an ephemeral one,
  the path-based `dbt_project.yml` config that decides between the two, and
  the `dbt run` vs `dbt snapshot` vs `dbt build` gotcha. Plus new **What you
  will learn** rows, `Repository layout` entries for `gold/` and
  `snapshots/`, two new Troubleshooting rows, and an updated Roadmap line.

### Changed

- `dbt/dbt_project.yml` — added a `gold:` block (`+materialized: table`,
  `+schema: gold`) with a nested `ephemeral:` override
  (`+materialized: ephemeral`) scoped to `models/gold/ephemeral/`.

## [1.3.1] - 2026-08-26

Fills the empty dbt project with a model layer. Adds the six bronze source
declarations, six incremental silver models that merge on their primary key and
advance on `updated_timestamp`, a Jinja-generated one-big-table, generic and
singular tests, and the schema-name macro that stops dbt prefixing every schema
with the target name.

### Added

- `dbt/models/source/sources.yml` — declares the six Stage 2 bronze tables
  (`orders`, `customers`, `products`, `order_items`, `employees`, `stores`) as
  the `tesco_databricks` source, pointing at `tesco.bronze`. Every model reads
  through `source()` rather than hard-coding the catalog, so the bronze location
  moves in one file.
- `dbt/models/silver/*.sql` — one incremental model per source table. Each is
  `materialized = 'incremental'` with `incremental_strategy = 'merge'` and the
  table's own `*_id` as `unique_key`, selects `*` plus a `CURRENT_TIMESTAMP()
  AS processed_at` audit column, and on incremental runs filters to rows newer
  than `MAX(updated_timestamp)` already in `{{ this }}`. The `COALESCE(...,
  '1900-01-01')` fallback is what makes the first incremental run after a
  `--full-refresh` behave: an empty high-water mark would otherwise compare
  against `NULL` and select nothing. `is_active` is carried through unchanged —
  soft deletes stay a downstream decision.
- `dbt/models/silver/obt.sql` — a One Big Table over the six silver models,
  built from a Jinja `configs` list rather than hand-written SQL. Each entry
  carries its model name, its aliased column list, and its join condition; the
  model body loops over that list to emit the `SELECT` and a chain of `LEFT
  JOIN`s anchored on `orders`. Adding a dimension to the OBT means adding one
  dict, not editing two places. Tables are resolved with `ref()` inside the
  loop, so dbt records all six silver models as upstream edges and orders the
  build itself — and the relations resolve per target instead of pinning a dev
  run to production `silver`. The loop variable is `cfg` rather than `config`,
  which would shadow dbt's own context object. Overlapping column names are aliased by entity
  (`customer_email`, `employee_email`; `customer_city`, `store_city`), and every
  source table's `created_timestamp` / `updated_timestamp` / `is_active` /
  `processed_at` survives with an entity prefix, so lineage back to bronze is
  not lost in the flattening.
- `dbt/models/silver/properties.yml` — generic tests: `not_null` and `unique` on
  `orders.order_id`, and the same pair on `products.product_id` with the
  uniqueness check scoped by `config: where: "price > 0"`.
- `dbt/tests/test_obt.sql` — a singular test asserting no row of `obt` has a
  `NULL` key (`order_id`, `product_id`, `customer_id`, `order_item_id`,
  `employee_id`, `store_id`). Set to `severity='warn'`: the joins are `LEFT`, so
  an orphan is a fact about the data worth surfacing, not a reason to fail the
  run.
- `dbt/macros/custom_schema.sql` — overrides dbt's built-in
  `generate_schema_name` to return the custom schema verbatim instead of
  `<target_schema>_<custom_schema>`. Without it, `+schema: silver` produces
  `default_silver`, and the medallion layers do not line up with the catalog.
- `dbt/analyses/`, `dbt/macros/`, `dbt/seeds/`, `dbt/snapshots/`, `dbt/tests/` —
  `.gitkeep` files so the scaffolded layout survives a clone.
- `README.md` — new **The silver layer in dbt** section covering the source
  declaration, the incremental + merge pattern and why the watermark is written
  the way it is, the schema-name macro, the OBT generator and how to verify its
  `ref()` edges with `dbt ls --select +obt`, and the two kinds of test. Plus new
  **What you will learn** rows and Troubleshooting entries.

### Changed

- `dbt/dbt_project.yml` — replaced the `example: +materialized: view` stub left
  by `dbt init` with a real `silver` config: materialized as `table`, into
  schema `silver`.
- `.vscode/settings.json` — added `dbt.perspectiveTheme: "Pro Dark"` so dbt
  Power User's query results panel matches a dark editor theme.

## [1.3.0] - 2026-08-25

Begins the transformation layer. Adds dbt with the Databricks adapter and
documents the setup end to end — the version pin the adapter forces, the project
layout `dbt init` leaves behind, where the credentials live, the macOS
certificate problem that stands between a fresh machine and a passing
`dbt debug`, and the VS Code tooling on top.

### Added

- **dbt** — `dbt-core<1.12` and `dbt-databricks>=1.12.4` added to
  `pyproject.toml`; scaffolded dbt project in `dbt/`, targeting a Databricks SQL
  warehouse.
- `dbt/profiles.yml.example` — committed template. The real `dbt/profiles.yml`
  carries a personal access token and is now git-ignored, as is `dbt/.user.yml`
  (a machine-local anonymous id). Every host, HTTP path, and token in the docs is
  a placeholder — this repo is public.
- `README.md` — new **Setting up dbt for Databricks** section, written as the
  setup that was actually performed:
  - **Why `dbt-core` is pinned below 1.12.** `dbt-databricks` 1.12.4 requires
    `dbt-core>=1.11.2,<1.12.1`, so an unconstrained `uv add` picks a version the
    adapter rejects. Also records that the resulting *"dbt-core is out of date"*
    banner is expected and must not be acted on.
  - **Scaffolding.** `dbt init` cannot initialise into an existing directory, so
    it produced `dbt/tesco/`; the contents were moved up into `dbt/` and the
    empty `tesco/` removed, making the dbt project `dbt/` itself.
  - **Where `profiles.yml` lives** — moved out of `~/.dbt/` to sit beside the
    project, and why every dbt command runs from `dbt/`.
  - **The `CERTIFICATE_VERIFY_FAILED` error**, with the real root cause: a
    python.org framework Python ships no CA bundle until
    `Install Certificates.command` runs, so `.../etc/openssl/cert.pem` does not
    exist and OpenSSL reports the missing trust anchor as *"self-signed
    certificate in certificate chain"*. Documents the two commands that confirm
    it, the single command that fixes it machine-wide, and why patching
    `SSL_CERT_FILE` in a shell profile is the wrong answer — VS Code's extension
    host never sees it, and pointing it into a project `.venv` breaks every other
    project when that venv is rebuilt.
  - **VS Code**, where the dbt Power User extension runs dbt from the interpreter
    the Python extension selected — which in this layout is the venv at the repo
    root, one level above the dbt project — plus why `dbt.dbtIntegration` and the
    profiles directory need no configuration here.
  - **The 15-minute hang.** `databricks-sql-connector` retries the failed TLS
    handshake up to its 900-second policy before surfacing the error, so
    `dbt debug` looks stuck on *"Opening a new connection"*.
- `.vscode/settings.json` and `.vscode/extensions.json` — committed workspace
  config: pins the interpreter to `.venv/bin/python` so dbt Power User resolves
  dbt, scopes `jinja-sql` highlighting to `dbt/` so the generated `ddl.sql` stays
  plain SQL, and recommends the extension to anyone opening the repo.
- `README.md` — eight new Troubleshooting rows for the above.

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

[1.4.0]: https://github.com/aashishparuvada/tesco/
[1.3.2]: https://github.com/aashishparuvada/tesco/
[1.3.1]: https://github.com/aashishparuvada/tesco/
[1.3.0]: https://github.com/aashishparuvada/tesco/
[1.2.0]: https://github.com/aashishparuvada/tesco/
[1.1.0]: https://github.com/aashishparuvada/tesco/
[1.0.0]: https://github.com/aashishparuvada/tesco/
