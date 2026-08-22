"""UTC range partitioning, at a per-table grain, for every partitioned table.

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

*A partition is identified by its FIRST DAY, at whatever grain its table uses.* The three log tables
are daily, so a partition start and "a day" were the same thing and this module could talk only in
days. They are not the same thing for a monthly or yearly table, and conflating them fails in two
directions that both look like working code: a mid-month date names a partition that does not exist,
and an expiry check keyed on the start throws a month away up to 30 days early. So every function below
takes an arbitrary date and floors it to its table's period, and expiry compares against the period's
LAST day rather than its first. `Grain` is an enum and not a `timedelta` because months and years have
no fixed length; February and leap years are read off the calendar, never computed.
"""

import enum
from dataclasses import dataclass
from datetime import date as date_type, datetime, time, timedelta, timezone

# PostgreSQL truncates identifiers past this, which would silently collapse two days onto one name.
_MAX_IDENTIFIER = 63

# The suffix a DEFAULT partition gets. It holds only NULL-key rows, so it has no day of its own and is
# skipped everywhere a day is derived from a partition name.
_DEFAULT_SUFFIX = "default"


class Grain(str, enum.Enum):
    """How wide one partition is.

    An enum rather than a duration because months and years have no fixed length. Anything that needs
    "the next boundary" asks the calendar, so February and leap years are correct by construction
    instead of by a constant that is wrong for one day in four years.
    """

    daily = "daily"
    monthly = "monthly"
    yearly = "yearly"


#: Name suffix per grain. Narrower than the period it describes would collide (two months sharing
#: `_2026`); wider would name a partition that does not match its own bounds.
_SUFFIX_FORMAT: dict[Grain, str] = {
    Grain.daily: "%Y_%m_%d",
    Grain.monthly: "%Y_%m",
    Grain.yearly: "%Y",
}


@dataclass(frozen=True)
class PartitionedTable:
    table: str
    key: str
    #: Why this table is cut on this column — kept with the config so the co-partitioning
    #: relationship between entries and their assignments is visible at the definition site.
    note: str
    #: How wide one partition is. Stated explicitly on every entry rather than defaulted, because a
    #: table silently taking the wrong grain is the failure this field exists to prevent.
    grain: Grain = Grain.daily


PARTITIONED: tuple[PartitionedTable, ...] = (
    PartitionedTable("log_entries", "timestamp",
                     "when the log line happened", grain=Grain.daily),
    PartitionedTable("log_transactions", "started_at",
                     "its first entry's timestamp", grain=Grain.daily),
    # Co-partitioned with log_entries ON PURPOSE: entry_ts is a copy of the entry's own timestamp, so
    # day D's assignments live in the same day as day D's entries and retention drops the pair
    # together. If these two keys ever disagreed, dropping a day of entries would strand that day's
    # assignments pointing at rows that no longer exist. The shared GRAIN is part of that guarantee:
    # a monthly assignment table beside daily entries would drop 30 days of one at a time.
    PartitionedTable("log_entry_assignment", "entry_ts",
                     "a copy of its entry's timestamp, so it co-partitions with log_entries",
                     grain=Grain.daily),

    # --- analytics platform (Phase 1) ---
    # Every one states its grain explicitly, and every one has a matching retention policy in
    # log_partition_worker (KEEP_FOREVER or RETENTION_DAYS). A table registered here WITHOUT a policy
    # silently inherits the log tables' 60-day retention, which for the fact table and its ledger would
    # mean the worker dropping the two things nothing can rebuild.
    #
    # `analytics_monthly_rollups` is deliberately absent: roughly 300K rows over five years, so there is
    # nothing worth pruning and partitioning it would add planning cost for no gain.
    PartitionedTable("analytics_facts", "event_time",
                     "the transaction's own start instant; monthly because it is kept forever",
                     grain=Grain.monthly),
    PartitionedTable("analytics_fact_ledger", "recorded_at",
                     "when the version was written, not when it happened: the ledger is append-only",
                     grain=Grain.monthly),
    PartitionedTable("analytics_hourly_rollups", "bucket_start",
                     "start of the hour; cut DAILY because a day of hourly rows is what retention drops",
                     grain=Grain.daily),
    PartitionedTable("analytics_daily_rollups", "business_date",
                     "the tenant-LOCAL day; yearly because it is kept forever and stays small",
                     grain=Grain.yearly),
    PartitionedTable("analytics_quality_issues", "detected_at",
                     "when the row was quarantined; monthly, bounded at a year",
                     grain=Grain.monthly),
)

BY_TABLE: dict[str, PartitionedTable] = {t.table: t for t in PARTITIONED}


# ============================================================== grain arithmetic (pure)
def grain_of(table: str) -> Grain:
    """The grain `table` is partitioned at.

    Raises rather than defaulting to daily. A default would give a monthly table daily partitions,
    which is precisely the silent misconfiguration this module was generalised to prevent, and it
    would surface much later as a partition count nobody can explain.
    """
    try:
        return BY_TABLE[table].grain
    except KeyError:
        raise KeyError(
            f"{table!r} is not a registered partitioned table. Add it to partitioning.PARTITIONED "
            f"with an explicit grain, and give it a retention policy in "
            f"log_partition_worker (KEEP_FOREVER or RETENTION_DAYS)."
        ) from None


def period_start(grain: Grain, day: date_type) -> date_type:
    """The first day of the partition `day` falls in. Idempotent, so it is safe to apply twice."""
    match grain:
        case Grain.daily:
            return day
        case Grain.monthly:
            return day.replace(day=1)
        case Grain.yearly:
            return day.replace(month=1, day=1)


def next_period_start(grain: Grain, day: date_type) -> date_type:
    """The first day of the NEXT partition, which is this one's EXCLUSIVE upper bound.

    Floors its input first, so advancing from a mid-period date lands on the next boundary rather than
    the same day of the following period. Without that, two adjacent partitions would be emitted with
    overlapping ranges and the second CREATE would fail.
    """
    start = period_start(grain, day)
    match grain:
        case Grain.daily:
            return start + timedelta(days=1)
        case Grain.monthly:
            # Not `month + 1`: that raises on December. Integer division rolls the year instead.
            return date_type(start.year + start.month // 12, start.month % 12 + 1, 1)
        case Grain.yearly:
            return date_type(start.year + 1, 1, 1)


def period_end(grain: Grain, day: date_type) -> date_type:
    """The LAST day inside the partition, inclusive.

    This is what retention and runway must key on. A monthly partition starting 1 January is still in
    policy on 2 March under 60-day retention, because its newest row is from 31 January; comparing
    against the start would drop it 30 days early. For a daily grain start and end are the same day,
    which is why the old day-only code was correct until it was not.
    """
    return next_period_start(grain, day) - timedelta(days=1)


def periods_covering(grain: Grain, first: date_type, last: date_type) -> list[date_type]:
    """Every partition start whose partition intersects `[first, last]`, ascending.

    Callers state the calendar range they need covered and stay grain-agnostic: twenty days of one
    month is one monthly partition and twenty daily ones, and neither caller has to know which.

    An inverted range raises rather than returning empty, for the same reason `days_between` does:
    it means the caller derived its bounds wrongly, and quietly creating nothing would leave the table
    with no partition for the period being written.
    """
    if last < first:
        raise ValueError(f"inverted partition range: {first} .. {last}")
    out: list[date_type] = []
    cur, stop = period_start(grain, first), period_start(grain, last)
    while cur <= stop:
        out.append(cur)
        cur = next_period_start(grain, cur)
    return out


# ============================================================== naming and bounds (pure)
def partition_name(table: str, day: date_type) -> str:
    """`log_entries_2026_08_05` daily, `analytics_facts_2026_08` monthly, `..._2026` yearly.

    Underscores rather than dashes so it needs no quoting. `day` may be any date inside the partition;
    it is floored first, so two days of one month resolve to the same name instead of inventing two.
    """
    grain = grain_of(table)
    start = period_start(grain, day)
    name = f"{table}_{start:{_SUFFIX_FORMAT[grain]}}"
    if len(name) > _MAX_IDENTIFIER:  # pragma: no cover - guarded by a test over every real table
        raise ValueError(f"partition name {name!r} exceeds PostgreSQL's {_MAX_IDENTIFIER}-char limit")
    return name


def default_partition_name(table: str) -> str:
    return f"{table}_{_DEFAULT_SUFFIX}"


#: Regex fragment matching any suffix a partition of ours can carry, at ANY grain, plus DEFAULT.
#: Derived from `_SUFFIX_FORMAT` rather than written out, so adding a grain cannot leave a consumer
#: behind. Alembic's autogenerate filter is the consumer that matters: a partition it fails to
#: recognise is reflected as an unknown table, and it proposes DROPPING it and its indexes.
_SUFFIX_PATTERN = "|".join(sorted(
    {fmt.replace("%Y", r"\d{4}").replace("%m", r"\d{2}").replace("%d", r"\d{2}")
     for fmt in _SUFFIX_FORMAT.values()},
    key=len, reverse=True)) + f"|{_DEFAULT_SUFFIX}"


def partition_name_pattern() -> str:
    """A regex matching every partition name of every partitioned table, at every grain.

    Lives here because this module is the single source of truth for what a partition is called, and the
    one consumer that gets it wrong does so catastrophically: Alembic autogenerate reflects an
    unrecognised partition as an unknown table and proposes dropping it.

    Matches the PARENT NAME plus a grain-shaped suffix rather than a bare prefix, so a real table that
    merely starts with the same characters is never hidden.
    """
    parents = "|".join(t.table for t in PARTITIONED)
    return rf"(?:{parents})_(?:{_SUFFIX_PATTERN})"


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
    """DDL for the one partition `day` falls in, at the table's own grain.

    Idempotent: the migration pre-creates a range and the worker re-runs on a schedule, so meeting an
    existing partition is normal, not an error.

    The bounds come from `period_start`/`next_period_start` rather than `day` and `day + 1`, so a
    monthly table gets one partition spanning the month instead of a day-wide one misnamed after it.
    Half-open, so consecutive partitions tile exactly: this one's TO is the next one's FROM.
    """
    grain = grain_of(table)
    start = period_start(grain, day)
    return (f"CREATE TABLE IF NOT EXISTS {partition_name(table, start)} "
            f"PARTITION OF {table} "
            f"FOR VALUES FROM ({_bound(start)}) TO ({_bound(next_period_start(grain, start))})")


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


def expired_days(covered: list[date_type], today: date_type, *, retention_days: int,
                 grain: Grain = Grain.daily) -> list[date_type]:
    """Which of `covered` (partition starts) are past retention, oldest first.

    Strictly older than the cutoff, so the boundary is KEPT — off-by-one here deletes production data
    that was still in policy.

    Compares the partition's LAST day, not its first. At a daily grain those are identical, which is
    why `grain` defaults to daily and every existing caller is unaffected. At a monthly grain the
    difference is up to 30 days of data: January would otherwise be droppable on 2 March under 60-day
    retention, when its newest row is only 30 days old.
    """
    cutoff = today - timedelta(days=retention_days)
    return sorted(s for s in covered if period_end(grain, s) < cutoff)


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


async def ensure_coverage(db, *, days: list[date_type],
                         tables: tuple[str, ...] | None = None) -> int:
    """Create any missing partition covering `days`. Returns how many were created. Safe to re-run —
    the DDL is `IF NOT EXISTS`.

    `days` is a list of DAYS the caller needs covered, not a list of partitions. Each table maps them
    onto its own grain and de-duplicates, so a month of requested days is one CREATE for a monthly
    table and thirty for a daily one. That keeps every caller grain-agnostic: the worker asks for
    "today plus the precreate window" and the migration asks for the span of existing data, and neither
    has to know how any table is cut.

    `tables` restricts which tables are provisioned; None means all of them, which is what the runway
    worker and the log-table migration both want. The analytics worker passes only its own destinations:
    it is a strict reader of the ingestion pipeline, and creating log partitions for a historic range
    would hand retention new partitions to drop on tables it has no business touching.

    An unknown table name raises rather than being skipped. A typo would otherwise provision nothing and
    look exactly like a healthy no-op, which is precisely how a destination goes unprovisioned unnoticed.
    """
    from sqlalchemy import text
    if tables is None:
        selected = PARTITIONED
    else:
        selected = tuple(BY_TABLE[name] for name in tables)   # KeyError on a typo, deliberately
    created = 0
    for t in selected:
        for start in sorted({period_start(t.grain, d) for d in days}):
            if await partition_exists(db, t.table, start):
                continue
            await db.execute(text(create_partition_sql(t.table, start)))
            created += 1
    return created
