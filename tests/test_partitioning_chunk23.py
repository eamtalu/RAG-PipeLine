"""Chunk 23 (step 3 of docs/plan/2026-08-05_20-32_daily-partitioning.md): range-partition the three
hot log tables by UTC day.

`log_entries` (key `timestamp`), `log_transactions` (key `started_at`) and `log_entry_assignment`
(key `entry_ts`) become daily range partitions. `log_entry_assignment` is co-partitioned with
`log_entries` deliberately: day D's entries and day D's assignments then drop together, so retention
can never leave an assignment pointing at an entry that no longer exists.

Everything asserted here was first verified against real PostgreSQL rather than reasoned about, and
the traps it encodes are the ones that do not announce themselves:

- A `timestamptz` partition bound written as `'2026-08-05'` is resolved in the SESSION's TimeZone at
  CREATE time. Under Europe/London that is 2026-08-04 23:00 UTC, so every partition would sit an hour
  off the day it is named after and rows would land in the neighbouring file. The bounds must carry
  an explicit UTC offset.
- All three partition keys are NULLABLE, and a PRIMARY KEY silently forces NOT NULL. So identity is a
  `UNIQUE NULLS NOT DISTINCT` that CONTAINS the key rather than a PK, and a DEFAULT partition catches
  the NULL-key rows the parser genuinely produces.
- The partition key must be IN the unique constraint but must NOT be first: a measured 240x
  regression on lookups by id alone (see the plan, §2.2).
- `log_entries` dedup was `(customer_code, entry_hash)`; a unique on a partitioned table has to
  include the key, so it becomes `(customer_code, entry_hash, timestamp)`. That is only safe because
  `entry_hash` is a sha256 of the raw line INCLUDING its timestamp text, so a replay routes to the
  same partition. This file pins that behaviour, because if it ever stops holding, ingestion starts
  silently duplicating every replayed line.
"""

import uuid
from datetime import date as date_type, datetime, timezone

import pytest
from sqlalchemy import delete, select, text

from app.persistence import partitioning as pt
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction
from app.settings import settings

CC = "TEST_CHUNK23"


# ============================================================ naming and bounds (pure)
def test_a_partition_is_named_after_the_utc_day_it_holds():
    assert pt.partition_name("log_entries", date_type(2026, 8, 5)) == "log_entries_2026_08_05"
    assert pt.partition_name("log_entry_assignment", date_type(2026, 12, 31)) == \
        "log_entry_assignment_2026_12_31"


def test_partition_names_fit_postgres_identifier_limit():
    """A silently TRUNCATED identifier would make two different days collide on one name."""
    for t in pt.PARTITIONED:
        assert len(pt.partition_name(t.table, date_type(2026, 12, 31))) <= 63


def test_bounds_carry_an_explicit_utc_offset():
    """The trap this whole module exists to avoid. A bare '2026-08-05' bound on a timestamptz column
    is interpreted in the session's TimeZone when the partition is CREATED, so a server running
    Europe/London would cut every partition an hour early and rows would land in the wrong day."""
    sql = pt.create_partition_sql("log_entries", date_type(2026, 8, 5))
    assert "'2026-08-05 00:00:00+00'" in sql
    assert "'2026-08-06 00:00:00+00'" in sql


def test_bounds_are_half_open_so_consecutive_days_tile_exactly():
    a = pt.create_partition_sql("log_entries", date_type(2026, 8, 5))
    b = pt.create_partition_sql("log_entries", date_type(2026, 8, 6))
    assert "TO ('2026-08-06 00:00:00+00')" in a
    assert "FROM ('2026-08-06 00:00:00+00')" in b


def test_creating_a_partition_is_idempotent():
    """The management worker re-runs every cycle and the migration pre-creates a range; neither may
    fail because yesterday's partition already exists."""
    assert "IF NOT EXISTS" in pt.create_partition_sql("log_entries", date_type(2026, 8, 5))
    assert "IF NOT EXISTS" in pt.create_default_sql("log_entries")


def test_days_between_is_inclusive_of_both_ends():
    days = pt.days_between(date_type(2026, 8, 5), date_type(2026, 8, 8))
    assert days == [date_type(2026, 8, 5), date_type(2026, 8, 6),
                    date_type(2026, 8, 7), date_type(2026, 8, 8)]
    assert pt.days_between(date_type(2026, 8, 5), date_type(2026, 8, 5)) == [date_type(2026, 8, 5)]


def test_days_between_rejects_an_inverted_range_instead_of_silently_creating_nothing():
    """An inverted range means the caller derived its bounds wrongly. Returning [] would leave the
    table with no partitions and every insert failing at runtime instead of here."""
    with pytest.raises(ValueError):
        pt.days_between(date_type(2026, 8, 8), date_type(2026, 8, 5))


def test_migration_days_covers_the_data_and_the_runway():
    """Days older than retention still need a partition or the copy fails outright - pruning them is
    retention's job. Today and the runway are needed so the first row written AFTER the build has
    somewhere to land."""
    days = pt.migration_days(date_type(2026, 4, 14), date_type(2026, 6, 10),
                             date_type(2026, 8, 5), ahead=14)
    assert days[0] == date_type(2026, 4, 14)
    assert days[-1] == date_type(2026, 8, 19)


def test_migration_days_on_an_empty_table_still_provisions_today_and_ahead():
    days = pt.migration_days(None, None, date_type(2026, 8, 5), ahead=14)
    assert days[0] == date_type(2026, 8, 5)
    assert days[-1] == date_type(2026, 8, 19)


def test_migration_days_refuses_an_absurd_span():
    """One corrupt year-2999 timestamp would otherwise become hundreds of thousands of partitions.
    Failing loudly leaves the quarantine decision with the operator."""
    with pytest.raises(ValueError, match="refusing to create"):
        pt.migration_days(date_type(2026, 4, 14), date_type(2999, 1, 1),
                          date_type(2026, 8, 5), ahead=14)


def test_the_absurd_span_error_says_how_to_find_the_bad_rows():
    """An abort that does not tell the operator what to do next just blocks the migration."""
    with pytest.raises(ValueError) as e:
        pt.migration_days(date_type(1900, 1, 1), date_type(2026, 6, 10),
                          date_type(2026, 8, 5), ahead=14)
    assert "SELECT min(timestamp)" in str(e.value)


def test_expired_days_keeps_the_boundary_day():
    """Off-by-one here drops a day of production data that was still in policy."""
    covered = [date_type(2026, 6, 5), date_type(2026, 6, 6), date_type(2026, 6, 7)]
    got = pt.expired_days(covered, date_type(2026, 8, 5), retention_days=60)
    assert got == [date_type(2026, 6, 5)]        # cutoff is 2026-06-06, which is KEPT


def test_every_partitioned_table_declares_a_key_that_exists_on_its_model():
    """Guards the config against a rename: the key column is named as a string, so a model change
    would otherwise only surface as a runtime SQL error during a migration."""
    from app.persistence import models as all_models
    by_table = {getattr(all_models, n).__tablename__: getattr(all_models, n)
                for n in all_models.__all__
                if hasattr(getattr(all_models, n), "__tablename__")}
    for t in pt.PARTITIONED:
        model = by_table.get(t.table)
        assert model is not None, f"{t.table} is partitioned but has no registered model"
        assert t.key in model.__table__.columns, f"{t.table}.{t.key} is gone"


def test_exactly_the_expected_tables_are_configured():
    """Was "the three hot tables". Phase 1 added five analytics tables, so the set is eight.

    Kept as an EXACT set rather than a subset check on purpose: a table appearing here without being
    considered is how one silently inherits the log tables' 60-day retention, which for the fact table
    and its ledger would mean the worker dropping the two things nothing can rebuild.
    """
    assert {t.table for t in pt.PARTITIONED} == {
        # Stage 1 and 2, daily.
        "log_entries", "log_transactions", "log_entry_assignment",
        # Analytics (Phase 1). Grain and retention are asserted in test_analytics_schema_chunk41.
        "analytics_facts", "analytics_fact_ledger",
        "analytics_hourly_rollups", "analytics_daily_rollups", "analytics_quality_issues"}


def test_entries_and_their_assignments_are_co_partitioned_on_the_same_grain():
    """Retention drops day D from both. If the keys ever disagreed, dropping entries for a day would
    strand that day's assignments pointing at rows that no longer exist."""
    by_table = {t.table: t for t in pt.PARTITIONED}
    assert by_table["log_entries"].key == "timestamp"
    assert by_table["log_entry_assignment"].key == "entry_ts"


# ============================================================ the migrated schema
async def test_all_three_tables_are_actually_partitioned(db):
    rows = dict((await db.execute(text("""
        SELECT c.relname, c.relkind FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = ANY(:names)
    """), {"names": [t.table for t in pt.PARTITIONED]})).all())
    assert rows, "none of the log tables exist"
    for t in pt.PARTITIONED:
        kind = rows.get(t.table)
        kind = kind.decode() if isinstance(kind, bytes) else kind   # asyncpg gives "char" as bytes
        assert kind == "p", f"{t.table} is not partitioned (relkind={kind!r})"


async def test_each_table_is_partitioned_on_the_configured_key(db):
    for t in pt.PARTITIONED:
        key = await db.scalar(text(
            "SELECT pg_get_partkeydef(CAST(:tbl AS regclass))"), {"tbl": t.table})
        assert key == f'RANGE ("{t.key}")' or key == f"RANGE ({t.key})", f"{t.table}: {key}"


async def test_every_table_has_a_default_partition_for_null_keys(db):
    """The parser genuinely emits entries with no parsable timestamp. Without a DEFAULT partition
    those inserts fail outright with 'no partition of relation found for row'."""
    for t in pt.PARTITIONED:
        got = await db.scalar(text("""
            SELECT count(*) FROM pg_class c
            JOIN pg_inherits i ON i.inhrelid = c.oid
            WHERE i.inhparent = CAST(:tbl AS regclass)
              AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
        """), {"tbl": t.table})
        assert got == 1, f"{t.table} has no DEFAULT partition"


async def test_the_partition_key_stayed_nullable(db):
    """If a later change put the key in a PRIMARY KEY, PostgreSQL would silently make it NOT NULL and
    every NULL-timestamp entry would become un-insertable.

    That applies to the tables whose key comes from PARSED LOG DATA, where a NULL is legitimate: the
    parser genuinely produces entries whose timestamp will not parse, and `analytics_facts.event_time`
    inherits that nullability from `log_transactions.started_at`.

    It does NOT apply to keys the analytics worker COMPUTES. A rollup bucket, a ledger write instant and
    a quarantine timestamp are always known, so those are NOT NULL by design: allowing a NULL there
    would mean a row that no bucket owns, sitting in a DEFAULT partition that no reader prunes and no
    retention pass reclaims. They keep their DEFAULT partition anyway, as insurance against a key
    outside the provisioned runway rather than against a NULL.
    """
    # Key nullable because it is parsed from a log line and may legitimately be absent.
    FROM_LOG_DATA = {"log_entries", "log_transactions", "log_entry_assignment", "analytics_facts"}
    for t in pt.PARTITIONED:
        nullable = await db.scalar(text("""
            SELECT is_nullable FROM information_schema.columns
            WHERE table_name = :tbl AND column_name = :col
        """), {"tbl": t.table, "col": t.key})
        expected = "YES" if t.table in FROM_LOG_DATA else "NO"
        assert nullable == expected, (
            f"{t.table}.{t.key} is {nullable}, expected {expected}: a parsed key must stay nullable, "
            f"a computed one must not")


async def test_identity_is_a_unique_that_contains_the_key_but_does_not_lead_with_it(db):
    """Both halves matter. Containing the key is what PostgreSQL demands of a unique on a partitioned
    table; NOT leading with it is what keeps lookups by id alone an index scan — leading with the key
    measured 240x slower (plan §2.2)."""
    expected = {"log_entries": ("id", "timestamp"),
                "log_transactions": ("id", "started_at"),
                "log_entry_assignment": ("entry_id", "entry_ts")}
    for tbl, cols in expected.items():
        got = (await db.execute(text("""
            SELECT a.attname FROM pg_constraint con
            JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
            JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
            WHERE con.conrelid = CAST(:tbl AS regclass) AND con.contype = 'u'
              AND :key = ANY (SELECT a2.attname FROM pg_attribute a2
                              WHERE a2.attrelid = con.conrelid AND a2.attnum = ANY(con.conkey))
              AND :ident = ANY (SELECT a3.attname FROM pg_attribute a3
                                WHERE a3.attrelid = con.conrelid AND a3.attnum = ANY(con.conkey))
            ORDER BY k.ord
        """), {"tbl": tbl, "key": cols[1], "ident": cols[0]})).scalars().all()
        assert tuple(got) == cols, f"{tbl}: identity unique is {got}, expected {cols}"


async def test_identity_uniques_treat_nulls_as_equal(db):
    """With the default NULLS DISTINCT, two rows sharing an id but both with a NULL key would not
    conflict — the constraint would stop enforcing identity for exactly the rows in the DEFAULT
    partition."""
    for tbl in ("log_entries", "log_transactions", "log_entry_assignment"):
        defs = (await db.execute(text("""
            SELECT pg_get_constraintdef(oid) FROM pg_constraint
            WHERE conrelid = CAST(:tbl AS regclass) AND contype = 'u'
        """), {"tbl": tbl})).scalars().all()
        assert any("NULLS NOT DISTINCT" in d for d in defs), f"{tbl}: {defs}"


async def test_no_table_kept_a_primary_key_constraint(db):
    """A PK cannot coexist with a nullable partition key, so the migration must have replaced it."""
    for t in pt.PARTITIONED:
        n = await db.scalar(text(
            "SELECT count(*) FROM pg_constraint WHERE conrelid = CAST(:tbl AS regclass) AND contype = 'p'"
        ), {"tbl": t.table})
        assert n == 0, f"{t.table} still has a PRIMARY KEY"


async def test_nothing_references_these_tables_by_foreign_key(db):
    """An INCOMING foreign key makes a partition impossible to DETACH or DROP, which removes the
    entire point of partitioning. Outgoing FKs (job_id -> jobs) are fine and verified separately."""
    n = await db.scalar(text("""
        SELECT count(*) FROM pg_constraint con JOIN pg_class t ON t.oid = con.confrelid
        WHERE con.contype = 'f' AND t.relname = ANY(:names)
    """), {"names": [t.table for t in pt.PARTITIONED]})
    assert n == 0


async def test_the_indexes_the_hot_reads_depend_on_survived_the_rewrite(db):
    """A table rewrite that quietly loses an index turns a feed query into a seq scan over the day."""
    required = {
        "log_entries": ["ix_log_entries_customer_code", "ix_log_entries_timestamp"],
        "log_transactions": ["ix_log_transactions_customer_started",
                             "ix_log_transactions_customer_date_started",
                             "ix_log_transactions_customer_date", "ix_log_transactions_sealed"],
        "log_entry_assignment": ["ix_log_entry_assignment_txn",
                                 "ix_log_entry_assignment_customer",
                                 "ix_log_entry_assignment_entry_ts"],
    }
    have = set((await db.execute(text(
        "SELECT indexname FROM pg_indexes WHERE tablename = ANY(:t)"
    ), {"t": list(required)})).scalars().all())
    missing = [i for names in required.values() for i in names if i not in have]
    assert not missing, f"indexes lost in the rewrite: {missing}"


async def test_the_job_cascade_still_reaches_entries_and_transactions(db):
    """`logspace_cleanup` purges a tenant by deleting its jobs and relying on this cascade. LIKE does
    not copy foreign keys, so the migration has to re-add them or a purge silently leaves rows."""
    for tbl in ("log_entries", "log_transactions"):
        d = await db.scalar(text("""
            SELECT confdeltype FROM pg_constraint
            WHERE conrelid = CAST(:tbl AS regclass) AND contype = 'f'
        """), {"tbl": tbl})
        d = d.decode() if isinstance(d, bytes) else d              # asyncpg gives "char" as bytes
        assert d == "c", f"{tbl}.job_id is not ON DELETE CASCADE (confdeltype={d!r})"


# ============================================================ behaviour on the partitioned tables
async def _job(db):
    j = Job(customer_code=CC, filename="c23.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c23.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    return j


async def _cleanup(db):
    await db.execute(delete(LogEntryAssignment).where(LogEntryAssignment.customer_code == CC))
    await db.execute(delete(LogEntry).where(LogEntry.customer_code == CC))
    await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == CC))
    await db.execute(delete(Job).where(Job.customer_code == CC))
    await db.flush()


async def test_an_entry_with_no_timestamp_still_inserts(db):
    """Straight into the DEFAULT partition. Before it existed this raised 'no partition of relation
    found for row' and the whole ingest batch aborted."""
    await _cleanup(db)
    job = await _job(db)
    e = LogEntry(customer_code=CC, job_id=job.id, timestamp=None, source_file="c23.log",
                 line_number=1, level="INFO", raw_body="x", entry_hash=uuid.uuid4().hex)
    db.add(e)
    await db.flush()
    assert e.id is not None


async def test_rows_land_in_the_partition_named_after_their_utc_day(db):
    """The bound-timezone trap, observed end to end rather than by reading the DDL."""
    await _cleanup(db)
    job = await _job(db)
    day = date_type(2026, 8, 5)
    db.add(LogEntry(customer_code=CC, job_id=job.id,
                    timestamp=datetime(2026, 8, 5, 0, 30, tzinfo=timezone.utc),
                    source_file="c23.log", line_number=1, level="INFO", raw_body="x",
                    entry_hash=uuid.uuid4().hex))
    await db.flush()
    where = await db.scalar(text(
        f"SELECT count(*) FROM {pt.partition_name('log_entries', day)} WHERE customer_code = :c"
    ), {"c": CC})
    assert where == 1


async def test_replaying_an_identical_line_still_dedups(db):
    """The linchpin of the whole design. Dedup had to grow the partition key, so it now only works
    because `entry_hash` covers the raw text INCLUDING the timestamp — an identical replay therefore
    parses to the same instant and routes to the same partition. If this ever fails, every replayed
    line is being duplicated."""
    await _cleanup(db)
    job = await _job(db)
    from app.services.mnp_log_ingestion.pipeline.parse_insert import _entry_hash, _insert_dedup
    raw = "2026-08-05 09:00:00.123 INFO the same line"
    row = {"customer_code": CC, "job_id": job.id, "source_file": "c23.log", "line_number": 1,
           "level": "INFO", "raw_body": raw, "entry_hash": _entry_hash(raw),
           "timestamp": datetime(2026, 8, 5, 9, 0, 0, 123000, tzinfo=timezone.utc)}
    assert len(await _insert_dedup(db, [dict(row, id=uuid.uuid4())])) == 1
    assert len(await _insert_dedup(db, [dict(row, id=uuid.uuid4())])) == 0
    assert (await db.scalar(select(text("count(*)")).select_from(LogEntry)
                            .where(LogEntry.customer_code == CC))) == 1


async def test_a_day_filtered_read_prunes_to_a_single_partition(db):
    """The payoff. Without pruning the feed opens all 60 partitions for one day of data."""
    plan = "\n".join(r[0] for r in (await db.execute(text("""
        EXPLAIN SELECT * FROM log_entries
        WHERE timestamp >= '2026-08-05 00:00:00+00' AND timestamp < '2026-08-06 00:00:00+00'
    """))).all())
    named = [ln for ln in plan.splitlines() if "log_entries_" in ln]
    assert len(named) == 1, f"expected one partition scanned, got:\n{plan}"
    assert "log_entries_2026_08_05" in named[0]


async def test_the_default_partition_is_excluded_from_a_range_scan(db):
    """A DEFAULT partition holding NULL keys must not be dragged into every bounded query."""
    plan = "\n".join(r[0] for r in (await db.execute(text("""
        EXPLAIN SELECT * FROM log_entries
        WHERE timestamp >= '2026-08-05 00:00:00+00' AND timestamp < '2026-08-06 00:00:00+00'
    """))).all())
    assert "log_entries_default" not in plan, plan


# ============================================================ coverage and retention
async def test_ensure_coverage_creates_the_days_it_is_asked_for(db):
    far = date_type(2027, 3, 3)
    await pt.ensure_coverage(db, days=[far])
    for t in pt.PARTITIONED:
        assert await pt.partition_exists(db, t.table, far), f"{t.table} missing {far}"
    for t in pt.PARTITIONED:
        await db.execute(text(f"DROP TABLE IF EXISTS {pt.partition_name(t.table, far)}"))


async def test_ensure_coverage_is_safe_to_run_twice(db):
    """The management worker runs on a schedule; a second pass must be a no-op, not an error."""
    far = date_type(2027, 3, 4)
    await pt.ensure_coverage(db, days=[far])
    await pt.ensure_coverage(db, days=[far])
    for t in pt.PARTITIONED:
        await db.execute(text(f"DROP TABLE IF EXISTS {pt.partition_name(t.table, far)}"))


async def test_covered_days_reports_what_exists(db):
    """Feeds the status card: how many days ahead are actually provisioned."""
    far = date_type(2027, 3, 5)
    await pt.ensure_coverage(db, days=[far])
    assert far in await pt.covered_days(db, "log_entries")
    for t in pt.PARTITIONED:
        await db.execute(text(f"DROP TABLE IF EXISTS {pt.partition_name(t.table, far)}"))


async def test_covered_days_ignores_the_default_partition(db):
    """DEFAULT has no day, and counting it as one would overstate how far ahead coverage runs."""
    days = await pt.covered_days(db, "log_entries")
    assert all(isinstance(d, date_type) for d in days)


async def test_dropping_one_day_leaves_the_others_intact(db):
    a, b = date_type(2027, 3, 6), date_type(2027, 3, 7)
    await pt.ensure_coverage(db, days=[a, b])
    await db.execute(text(pt.drop_partition_sql("log_entries", a)))
    assert not await pt.partition_exists(db, "log_entries", a)
    assert await pt.partition_exists(db, "log_entries", b)
    for t in pt.PARTITIONED:
        for d in (a, b):
            await db.execute(text(f"DROP TABLE IF EXISTS {pt.partition_name(t.table, d)}"))


def test_nothing_builds_this_schema_with_create_all():
    """The models keep `primary_key=True` on their id column for ORM identity, but the DDL that
    implies - `PRIMARY KEY (id)` - is INVALID on a partitioned table (a PK must contain the partition
    key). That divergence is harmless only while Alembic is the sole schema builder, so this fails the
    moment a create_all appears and reintroduces the possibility of a table built the wrong way."""
    import ast
    import pathlib
    repo = pathlib.Path(__file__).resolve().parent.parent
    hits = []
    for d in ("app", "tests", "alembic"):
        for path in (repo / d).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                # An AST walk rather than a text search: the models MENTION create_all in the comments
                # explaining why it must never be used, and a substring scan would flag those.
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "create_all"):
                    hits.append(f"{path.relative_to(repo)}:{node.lineno}")
    assert not hits, f"create_all would emit invalid DDL for the partitioned tables: {hits}"


def test_the_orm_identity_stayed_a_single_column():
    """Widening the ORM key to (id, partition key) would silently break every `db.get(Model, id)`
    call site, which passes a bare id."""
    assert [c.name for c in LogEntry.__table__.primary_key] == ["id"]
    assert [c.name for c in LogTransaction.__table__.primary_key] == ["id"]
    assert [c.name for c in LogEntryAssignment.__table__.primary_key] == ["entry_id"]


def test_autogenerate_hides_partitions_but_not_their_parents():
    """Without this filter, `alembic revision --autogenerate` reflects all ~275 partitions as unknown
    tables and proposes DROPping the local index PostgreSQL created on each one from the parent's
    partitioned index. Running that revision would strip every index off the hot tables.

    The parent tables must stay visible, or real schema changes would silently stop being detected.
    """
    import importlib.util
    import pathlib as _p
    spec = importlib.util.spec_from_file_location(
        "alembic_env_probe", _p.Path(__file__).resolve().parent.parent / "alembic" / "env.py")
    # env.py runs migrations on import, so read the predicate out of its source instead.
    src = spec.origin and _p.Path(spec.origin).read_text()
    ns: dict = {}
    start = src.index("def _include_object")
    end = src.index("\ndef ", start + 1) if "\ndef " in src[start + 1:] else len(src)
    exec("import re\n" + "from app.persistence import partitioning as _pt\n" + src[start:end], ns)
    inc = ns["_include_object"]

    for t in pt.PARTITIONED:
        assert inc(None, t.table, "table", True, None) is True, f"{t.table} must stay visible"
        assert inc(None, pt.partition_name(t.table, date_type(2026, 8, 5)),
                   "table", True, None) is False
        assert inc(None, pt.default_partition_name(t.table), "table", True, None) is False
    # a table that merely shares a prefix is NOT a partition and must not be hidden
    assert inc(None, "log_entries_archive_manifest", "table", True, None) is True


def test_retention_settings_are_coherent():
    """Pre-creating fewer days than the worker's own cadence would let ingestion hit a day with no
    partition; retention shorter than the seal window would drop data still being stitched."""
    assert settings.log_partition_retention_days >= 1
    assert settings.log_partition_precreate_days >= 2
    assert settings.log_partition_retention_days * 86400 > settings.log_abandon_window_seconds
