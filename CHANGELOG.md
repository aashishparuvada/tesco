# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.0.0]: https://github.com/aashishparuvada/tesco/
