"""Shared CSV -> Postgres raw_data loading logic.

Both entry points use this module, so the two targets can never drift apart:

    scripts/python/load_data.py      -> local Postgres (docker-compose)
    scripts/python/load_supabase.py  -> hosted Supabase Postgres

Each CSV in data/ is profiled, a column type is inferred from the values it
actually holds, and the resulting DDL is both executed against the target and
written to scripts/sql/raw_data/ddl.sql.
"""

from __future__ import annotations

import csv
import os
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from psycopg2 import sql

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DDL_PATH = PROJECT_ROOT / "scripts" / "sql" / "raw_data" / "ddl.sql"
ENV_PATH = PROJECT_ROOT / ".env"

SCHEMA = "raw_data"

INT_RE = re.compile(r"^[+-]?\d+$")
DECIMAL_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")
IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")

TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
)
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")

INT32_MAX = 2_147_483_647
# VARCHAR sizes we round up to, so a slightly longer value in a later batch
# does not fail the load.
VARCHAR_BUCKETS = (10, 20, 50, 100, 255, 500, 1000, 4000)


# --- environment -------------------------------------------------------------


def load_env(path: Path = ENV_PATH) -> None:
    """Read KEY=VALUE pairs from .env without adding a dependency.

    Real environment variables always win, so the same script works in CI.
    """
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def require_env(*names: str) -> None:
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise SystemExit(
            f"Missing environment variable(s): {', '.join(missing)}. "
            "Copy .env.example to .env and fill it in."
        )


# --- profiling ---------------------------------------------------------------


@dataclass
class ColumnStats:
    """Everything observed about one CSV column, collected in a single pass."""

    name: str
    is_int: bool = True
    is_decimal: bool = True
    is_timestamp: bool = True
    is_date: bool = True
    has_empty: bool = False
    non_empty: int = 0
    max_len: int = 0
    max_abs_int: int = 0
    max_int_digits: int = 1
    max_scale: int = 0
    distinct: set[str] = field(default_factory=set)

    def observe(self, raw: str) -> None:
        value = raw.strip()
        if not value:
            self.has_empty = True
            return

        self.non_empty += 1
        self.max_len = max(self.max_len, len(value))
        self.distinct.add(value)

        if self.is_int:
            if INT_RE.match(value):
                self.max_abs_int = max(self.max_abs_int, abs(int(value)))
            else:
                self.is_int = False

        if self.is_decimal:
            if DECIMAL_RE.match(value):
                whole, _, frac = value.lstrip("+-").partition(".")
                self.max_int_digits = max(self.max_int_digits, len(whole) or 1)
                self.max_scale = max(self.max_scale, len(frac))
            else:
                self.is_decimal = False

        if self.is_timestamp and not _parses(value, TIMESTAMP_FORMATS):
            self.is_timestamp = False

        if self.is_date and not _parses(value, DATE_FORMATS):
            self.is_date = False

    @property
    def is_unique(self) -> bool:
        return self.non_empty > 0 and len(self.distinct) == self.non_empty


def _parses(value: str, formats: tuple[str, ...]) -> bool:
    for fmt in formats:
        try:
            # Probing the format only -- the parsed value is discarded and
            # the CSVs carry no timezone offset.
            datetime.strptime(value, fmt)  # noqa: DTZ007
        except ValueError:
            continue
        return True
    return False


def profile_csv(path: Path) -> tuple[list[ColumnStats], int]:
    """Read a CSV once and return per-column stats plus the row count."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise SystemExit(f"{path.name} is empty -- it has no header row.")

        stats = [ColumnStats(name=name.strip()) for name in header]
        rows = 0
        for row in reader:
            rows += 1
            for column, value in zip(stats, row):
                column.observe(value)

    for column in stats:
        if not IDENTIFIER_RE.match(column.name):
            raise SystemExit(
                f"{path.name}: column '{column.name}' is not a safe SQL "
                "identifier (expected lower_snake_case)."
            )
    return stats, rows


# --- type inference ----------------------------------------------------------


def sql_type(column: ColumnStats) -> str:
    """Pick the narrowest Postgres type that still fits every observed value."""
    if column.non_empty == 0:
        # Nothing to go on -- keep it permissive so the load never fails.
        return "VARCHAR(255)"

    if column.is_timestamp:
        return "TIMESTAMP"

    if column.is_date:
        return "DATE"

    if column.is_int:
        if column.name.endswith("_id") or column.max_abs_int > INT32_MAX:
            return "BIGINT"
        return "INTEGER"

    if column.is_decimal:
        # Leave headroom on both sides so bigger values load later on.
        scale = column.max_scale
        precision = min(38, max(10, column.max_int_digits + scale + 4))
        return f"NUMERIC({precision}, {scale})"

    if column.max_len == 1:
        return "CHAR(1)"

    for bucket in VARCHAR_BUCKETS:
        if bucket >= column.max_len * 2:
            return f"VARCHAR({bucket})"
    return "TEXT"


def primary_key(stats: list[ColumnStats]) -> str | None:
    """Treat a leading, unique, always-populated *_id column as the key."""
    if not stats:
        return None
    first = stats[0]
    if first.name.endswith("_id") and not first.has_empty and first.is_unique:
        return first.name
    return None


# --- DDL generation ----------------------------------------------------------


def table_ddl(table: str, stats: list[ColumnStats]) -> str:
    # Single spaces only, no column alignment padding -- sqlfluff's
    # layout.spacing rule (enforced in pre-commit and CI) rejects padding.
    pk = primary_key(stats)

    lines = []
    for column in stats:
        definition = f"{column.name} {sql_type(column)}"
        if column.name == pk:
            definition += " PRIMARY KEY"
        lines.append(f"    {definition}")

    body = ",\n".join(lines)
    return (
        f"-- CREATE table {table}\n\n"
        f"DROP TABLE IF EXISTS {SCHEMA}.{table} CASCADE;\n\n"
        f"CREATE TABLE {SCHEMA}.{table} (\n{body}\n);\n"
    )


def build_ddl(
    tables: dict[str, list[ColumnStats]], *, include_schema: bool = True
) -> str:
    """Render the full DDL script.

    include_schema adds the CREATE SCHEMA statement, which makes the .sql file
    runnable on its own. It is left out of what the loaders execute, because
    ensure_schema() handles that -- a hosted target may not grant the
    connecting role permission to create schemas.
    """
    header = (
        "-- AUTO-GENERATED by scripts/python/loader.py -- do not edit by hand.\n"
        "-- Column types are inferred from the CSV files in data/.\n"
        "-- Regenerate with: uv run scripts/python/load_data.py\n\n"
    )
    if include_schema:
        header += f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};\n\n\n"
    return header + "\n\n".join(
        table_ddl(table, stats) for table, stats in tables.items()
    )


# --- loading -----------------------------------------------------------------


def discover_csvs() -> dict[str, Path]:
    """Map table name -> CSV path for every file in data/."""
    if not DATA_DIR.is_dir():
        raise SystemExit(f"Data directory not found: {DATA_DIR}")

    found = {}
    for path in sorted(DATA_DIR.glob("*.csv")):
        table = path.stem.strip().lower().replace("-", "_").replace(" ", "_")
        if not IDENTIFIER_RE.match(table):
            raise SystemExit(
                f"{path.name}: cannot derive a safe table name from the filename."
            )
        found[table] = path

    if not found:
        raise SystemExit(f"No CSV files found in {DATA_DIR}")
    return found


def ensure_schema(cursor) -> None:
    """Create the schema only when it is genuinely missing.

    Issuing CREATE SCHEMA unconditionally would need privileges the connecting
    role may not have on a hosted database, so check first.
    """
    cursor.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
        (SCHEMA,),
    )
    if cursor.fetchone():
        print(f"Schema {SCHEMA} already exists.")
        return

    print(f"Schema {SCHEMA} not found -- creating it...")
    cursor.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA))
    )


def copy_csv(cursor, table: str, path: Path) -> int:
    copy_statement = sql.SQL(
        "COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    ).format(sql.Identifier(SCHEMA, table))

    with path.open(newline="", encoding="utf-8") as handle:
        cursor.copy_expert(copy_statement.as_string(cursor), handle)

    cursor.execute(
        sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(SCHEMA, table))
    )
    return cursor.fetchone()[0]


def run_load(connect: Callable[[], object], target: str) -> int:
    """Profile the CSVs, rebuild the raw_data tables, and COPY the data in.

    connect is a zero-argument callable returning a psycopg2 connection, which
    is what makes this reusable across the local and Supabase targets.
    """
    csv_files = discover_csvs()
    print(f"Found {len(csv_files)} CSV file(s) in {DATA_DIR}\n")

    print("Profiling CSV files to infer column types...")
    tables: dict[str, list[ColumnStats]] = {}
    expected_rows: dict[str, int] = {}
    for table, path in csv_files.items():
        stats, rows = profile_csv(path)
        tables[table] = stats
        expected_rows[table] = rows
        pk = primary_key(stats)
        key_note = f", pk={pk}" if pk else ""
        print(f"  {path.name:<20} {len(stats)} cols, {rows:,} rows{key_note}")

    DDL_PATH.parent.mkdir(parents=True, exist_ok=True)
    DDL_PATH.write_text(build_ddl(tables))
    print(f"\nWrote schema definition to {DDL_PATH.relative_to(PROJECT_ROOT)}")

    ddl = build_ddl(tables, include_schema=False)

    print(f"\nConnecting to {target}...")
    conn = connect()
    try:
        # One transaction for the whole run: either every table is rebuilt and
        # loaded, or the database is left exactly as it was.
        with conn, conn.cursor() as cursor:
            ensure_schema(cursor)
            cursor.execute(ddl)

            for table, path in csv_files.items():
                print(f"Loading {path.name} into {SCHEMA}.{table}...")
                loaded = copy_csv(cursor, table, path)
                if loaded != expected_rows[table]:
                    raise RuntimeError(
                        f"{table}: expected {expected_rows[table]:,} rows, "
                        f"found {loaded:,} after COPY"
                    )
                print(f"  loaded {loaded:,} rows")
    except Exception as exc:  # noqa: BLE001 -- report and fail the run
        print(f"\nError: {exc}", file=sys.stderr)
        print("No changes were committed.", file=sys.stderr)
        return 1
    finally:
        conn.close()

    total = sum(expected_rows.values())
    print(
        f"\nAll {len(csv_files)} table(s) loaded into {SCHEMA} "
        f"on {target} ({total:,} rows)."
    )
    return 0
