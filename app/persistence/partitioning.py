"""Daily UTC range partitioning for the three hot log tables.

Retention used to mean `DELETE` + `VACUUM`, both of which read the whole table — on a disk with bad
sectors, on a heap that reached 40 GB. Partitioned by day it becomes `DROP TABLE <partition>`: a file
unlink, no scan, no dead tuples, no vacuum. Reads of a single day touch one partition instead of the
whole heap.

This module is the single source of truth for WHICH tables are partitioned, on WHICH column, and what
a partition for a given day is called. The Alembic migration that builds them, the management worker
that keeps coverage ahead of ingestion, and the status endpoint that reports it all read from here, so
the three can never drift apart.

The DDL builders are pure string functions with no session and no I/O. That is deliberate: Alembic
runs on a synchronous connection while the worker and the API run on async ones, and a builder that
returns text serves both without a compatibility shim — and can be asserted on without a database.

Three things here are load-bearing:

*Bounds carry an explicit UTC offset.* A `timestamptz` bound written as `'2026-08-05'` is resolved in
the SESSION's TimeZone at the moment the partition is created. On a server set to Europe/London that
is 2026-08-04 23:00 UTC, so every partition would sit an hour off the day it is named after and rows
would quietly land in the neighbouring one. Every bound below is emitted as `'... 00:00:00+00'`.

*Every key is nullable, so every table needs a DEFAULT partition.* The parser legitimately produces
entries whose timestamp will not parse; without DEFAULT those inserts fail outright with "no partition
of relation found for row" and take the whole batch down with them.

*Identity is a UNIQUE, not a PRIMARY KEY.* PostgreSQL requires a unique constraint on a partitioned
table to contain every partition-key column, and silently forces PK columns to NOT NULL — which would
make the NULL-key rows above un-insertable. So identity is `UNIQUE NULLS NOT DISTINCT (id, key)`. The
key is included but never FIRST: leading with it measured 240x slower on lookups by id alone.
"""

from dataclasses import dataclass
from datetime import date as date_type, datetime, time, timedelta, timezone

# PostgreSQL truncates identifiers past this, which would silently collapse two days onto one name.
_MAX_IDENTIFIER = 63

# The suffix a DEFAULT partition gets. It holds only NULL-key rows, so it has no day of its own and is
# skipped everywhere a day is derived from a partition name.
_DEFAULT_SUFFIX = "default"


@dataclass(frozen=True)
class PartitionedTable:
    table: str
    key: str
    #: Why this table is cut on this column — kept with the config so the co-partitioning
    #: relationship between entries and their assignments is visible at the definition site.
    note: str


PARTITIONED: tuple[PartitionedTable, ...] = (
    PartitionedTable("log_entries", "timestamp",
                     "when the log line happened"),
    PartitionedTable("log_transactions", "started_at",
                     "its first entry's timestamp"),
    # Co-partitioned with log_entries ON PURPOSE: entry_ts is a copy of the entry's own timestamp, so
    # day D's assignments live in the same day as day D's entries and retention drops the pair
    # together. If these two keys ever disagreed, dropping a day of entries would strand that day's
    # assignments pointing at rows that no longer exist.
    PartitionedTable("log_entry_assignment", "entry_ts",
                     "a copy of its entry's timestamp, so it co-partitions with log_entries"),
)

BY_TABLE: dict[str, PartitionedTable] = {t.table: t for t in PARTITIONED}


# ============================================================== naming and bounds (pure)
def partition_name(table: str, day: date_type) -> str:
    """`log_entries_2026_08_05`. Underscores rather than dashes so it needs no quoting."""
    name = f"{table}_{day:%Y_%m_%d}"
    if len(name) > _MAX_IDENTIFIER:  # pragma: no cover - guarded by a test over every real table
        raise ValueError(f"partition name {name!r} exceeds PostgreSQL's {_MAX_IDENTIFIER}-char limit")
    return name


def default_partition_name(table: str) -> str:
    return f"{table}_{_DEFAULT_SUFFIX}"


def _bound(day: date_type) -> str:
    """A partition bound literal pinned to UTC — see the module docstring on why the offset is not
    optional."""
    return f"'{day:%Y-%m-%d} 00:00:00+00'"


def day_start(day: date_type) -> datetime:
    """Midnight UTC at the start of `day` — the inclusive lower edge of its partition."""
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def day_end(day: date_type) -> datetime:
    """Midnight UTC at the start of the NEXT day — the EXCLUSIVE upper edge of `day`'s partition.

    Exclusive so consecutive days tile without overlapping, matching the partition bounds exactly. A
    caller comparing against an inclusive 23:59:59.999999 would leave a sliver of the day unmatched.
    """
    return day_start(day + timedelta(days=1))


def create_partition_sql(table: str, day: date_type) -> str:
    """DDL for one day. Idempotent: the migration pre-creates a range and the worker re-runs on a
    schedule, so meeting an existing partition is normal, not an error."""
    return (f"CREATE TABLE IF NOT EXISTS {partition_name(table, day)} "
            f"PARTITION OF {table} "
            f"FOR VALUES FROM ({_bound(day)}) TO ({_bound(day + timedelta(days=1))})")


def create_default_sql(table: str) -> str:
    return (f"CREATE TABLE IF NOT EXISTS {default_partition_name(table)} "
            f"PARTITION OF {table} DEFAULT")


def drop_partition_sql(table: str, day: date_type) -> str:
    """Retention. A DROP unlinks the file — no row scan, no dead tuples, nothing left to vacuum."""
    return f"DROP TABLE IF EXISTS {partition_name(table, day)}"


def days_between(first: date_type, last: date_type) -> list[date_type]:
    """Every day in `[first, last]`, both ends included.

    An inverted range raises rather than returning empty: it means the caller derived its bounds
    wrongly, and quietly creating nothing would leave the table with no partition for the day being
    written and surface much later as a failing insert.
    """
    if last < first:
        raise ValueError(f"inverted partition range: {first} .. {last}")
    return [first + timedelta(days=i) for i in range((last - first).days + 1)]


def coverage_days(today: date_type, *, ahead: int) -> list[date_type]:
    """The days that must exist for ingestion to keep working: today plus `ahead` more.

    Today is included rather than assumed present so a cold start, or a gap left by a worker that was
    down, is repaired on the next pass instead of only being extended.
    """
    return days_between(today, today + timedelta(days=ahead))


#: Widest day span a one-off partition build will provision. Ten years is far beyond any real log
#: history and far below the point where the partition count becomes a problem in itself.
MAX_MIGRATION_SPAN_DAYS = 3650


def migration_days(lo: date_type | None, hi: date_type | None, today: date_type, *,
                   ahead: int, max_span_days: int = MAX_MIGRATION_SPAN_DAYS) -> list[date_type]:
    """Every day a one-off partition build must provision: the data's own span, widened to cover
    today and the pre-create runway.

    Both widenings are required. Days older than retention still need a partition or the copy fails
    with "no partition of relation found for row" — pruning them is retention's job, not the build's.
    Today and the runway are needed because an empty or historic-only table would otherwise have
    nowhere to put the first row written after the build.

    Raises on an absurd span rather than provisioning it. A single corrupt timestamp — a year-2999
    row from a malformed line — would otherwise silently turn into hundreds of thousands of
    partitions. Failing loudly lets the operator quarantine those rows first, which is a decision that
    belongs to them and not to a migration running unattended.
    """
    first = min(lo, today) if lo else today
    last = max(hi, today + timedelta(days=ahead)) if hi else today + timedelta(days=ahead)
    span = (last - first).days + 1
    if span > max_span_days:
        raise ValueError(
            f"refusing to create {span} daily partitions spanning {first} .. {last}. "
            f"That is almost certainly one or more corrupt timestamps rather than real history — "
            f"find them with: SELECT min(timestamp), max(timestamp) FROM <table>; "
            f"quarantine or fix those rows, then re-run.")
    return days_between(first, last)


def expired_days(covered: list[date_type], today: date_type, *, retention_days: int) -> list[date_type]:
    """Which of `covered` are past retention. Strictly older than the cutoff, so the boundary day is
    KEPT — off-by-one here deletes a day of production data that was still in policy."""
    cutoff = today - timedelta(days=retention_days)
    return sorted(d for d in covered if d < cutoff)


# ============================================================== async facade
async def partition_exists(db, table: str, day: date_type) -> bool:
    from sqlalchemy import text
    return bool(await db.scalar(
        text("SELECT to_regclass(:name) IS NOT NULL"), {"name": partition_name(table, day)}))


async def covered_days(db, table: str) -> list[date_type]:
    """The days this table has partitions for, ascending.

    Read from the partition BOUNDS rather than parsed out of the names: the bound is what PostgreSQL
    actually routes on, so a partition whose name and range disagreed would be reported honestly
    instead of plausibly. The DEFAULT partition has no bound and is skipped.
    """
    from sqlalchemy import text
    rows = (await db.execute(text("""
        SELECT (regexp_match(pg_get_expr(c.relpartbound, c.oid),
                             'FROM \\(''([0-9-]{10})'))[1]::date AS day
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = CAST(:tbl AS regclass)
          AND pg_get_expr(c.relpartbound, c.oid) <> 'DEFAULT'
        ORDER BY 1
    """), {"tbl": table})).scalars().all()
    return [d for d in rows if d is not None]


async def ensure_coverage(db, *, days: list[date_type]) -> int:
    """Create any missing partition for `days` across every partitioned table. Returns how many were
    created. Safe to re-run — the DDL is `IF NOT EXISTS`."""
    from sqlalchemy import text
    created = 0
    for t in PARTITIONED:
        for day in days:
            if await partition_exists(db, t.table, day):
                continue
            await db.execute(text(create_partition_sql(t.table, day)))
            created += 1
    return created
