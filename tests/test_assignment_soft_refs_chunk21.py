"""Chunk 21: make log_entry_assignment partition-ready - soft references, and a time column.

Step 1 of the daily-partitioning plan (docs/plan/2026-08-05_20-32_daily-partitioning.md). Three
changes, each forced by something measured against the real database rather than reasoned about:

1. DROP BOTH FOREIGN KEYS. A foreign key makes a partition impossible to remove:

       ALTER TABLE ... DETACH PARTITION  -> ERROR: violates foreign key constraint
       DROP TABLE <partition>            -> ERROR: other objects depend on it

   Retention IS dropping partitions, so the FKs and partitioning are mutually exclusive. They also
   cost real time on the hottest write path: inserting 200k assignments took 1,060 ms with the two
   FK triggers and 249 ms without - roughly 4x, on a table rewritten by every regroup of the live
   tail.

2. ADD `entry_ts`. The table has no time column at all, so there is nothing to partition it on. It
   is denormalised from the owning entry so assignment partitions line up with entry partitions and
   can be dropped together.

3. UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts) - in that order, and NOT a primary key.

   Not a PK because PostgreSQL silently forces every PK column to NOT NULL, and entry_ts must stay
   nullable. Verified: declaring a nullable column in a PK makes it NOT NULL and the NULL insert then
   fails. NULLS NOT DISTINCT (PG15+; production is 16.14) is what keeps the guarantee for
   timestamp-less entries - a plain UNIQUE treats two NULLs as different.

   The ORDER is worth 240x on 300k rows, looking up by entry_id alone:

       (entry_id)             0.045 ms  index scan
       (entry_ts, entry_id)  10.8   ms  SEQUENTIAL SCAN
       (entry_id, entry_ts)   0.046 ms  index scan

   Three hot paths look entries up by `entry_id` alone, so timestamp-first would be a severe
   regression. The published codex design puts the partition key first; following it here would have
   caused exactly that.

The cost of (1) is that deletes no longer cascade. Four paths relied on it, and a miss leaves orphan
assignment rows pointing at data that no longer exists - so most of this file is about those.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select, text as sa_text

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import assignments as A
from app.services.mnp_log_ingestion.pipeline import derive_transactions as d

CC = "TEST_CHUNK21"
T0 = datetime(2026, 8, 5, 10, 0, 0, tzinfo=timezone.utc)


# =============================================================== fixtures
async def _cleanup(cc: str = CC) -> None:
    async with async_session() as s:
        await s.execute(delete(LogEntryAssignment).where(LogEntryAssignment.customer_code == cc))
        await s.execute(delete(LogEntry).where(LogEntry.customer_code == cc))
        await s.execute(delete(LogTransaction).where(LogTransaction.customer_code == cc))
        await s.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == cc))
        await s.execute(delete(Job).where(Job.customer_code == cc))
        await s.execute(delete(Customer).where(Customer.customer_code == cc))
        await s.commit()


@pytest.fixture
async def clean():
    await _cleanup()
    yield
    await _cleanup()


async def _mk_job(cc: str = CC) -> uuid.UUID:
    async with async_session() as s:
        job = Job(customer_code=cc, filename="t.log", storage_key="k")
        s.add(job)
        await s.commit()
        return job.id


async def _mk_entries(job_id: uuid.UUID, n: int, *, cc: str = CC,
                      start: datetime = T0) -> list[LogEntry]:
    rows = []
    async with async_session() as s:
        for i in range(n):
            rows.append(LogEntry(id=uuid.uuid4(), customer_code=cc, job_id=job_id,
                                 entry_hash=uuid.uuid4().hex, source_file="t.log",
                                 line_number=i + 1, timestamp=start + timedelta(seconds=i),
                                 entry_type=LogEntryType.info, raw_body=f"line {i}"))
        s.add_all(rows)
        await s.commit()
    return rows


async def _assignments(cc: str = CC) -> int:
    async with async_session() as s:
        return await s.scalar(select(func.count()).select_from(LogEntryAssignment)
                              .where(LogEntryAssignment.customer_code == cc)) or 0


# =============================================================== schema shape
async def test_no_foreign_keys_remain_on_the_assignment_table():
    """The decisive constraint. While an FK points into a table, its partitions can be neither
    detached nor dropped - which is exactly what retention needs to do."""
    async with async_session() as s:
        fks = (await s.execute(sa_text(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = 'log_entry_assignment'::regclass AND contype = 'f'"
        ))).scalars().all()
    assert fks == [], f"foreign keys still block DROP PARTITION: {fks}"


async def test_uniqueness_is_entry_id_first_and_nulls_not_distinct():
    """Three things at once, all load-bearing:

    - the guarantee lives on a UNIQUE, not a PRIMARY KEY (a PK would force entry_ts NOT NULL);
    - entry_id comes FIRST (timestamp-first is a 240x regression on lookup-by-entry_id);
    - NULLS NOT DISTINCT, or one timestamp-less entry could collect several assignments.
    """
    async with async_session() as s:
        cols = (await s.execute(sa_text("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE ic.relname = 'uq_log_entry_assignment_entry'
            ORDER BY array_position(i.indkey, a.attnum)
        """))).scalars().all()
        nulls_not_distinct = await s.scalar(sa_text("""
            SELECT i.indnullsnotdistinct FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE ic.relname = 'uq_log_entry_assignment_entry'
        """))
        has_pk = await s.scalar(sa_text(
            "SELECT count(*) FROM pg_constraint "
            "WHERE conrelid = 'log_entry_assignment'::regclass AND contype = 'p'"))

    assert cols == ["entry_id", "entry_ts"], (
        f"constraint columns are {cols}; must be ['entry_id', 'entry_ts'] - entry_id FIRST, or "
        "lookups by entry_id degrade to a sequential scan")
    assert nulls_not_distinct is True, (
        "UNIQUE must be NULLS NOT DISTINCT, else a timestamp-less entry can hold several assignments")
    assert has_pk == 0, (
        "a PRIMARY KEY would force entry_ts NOT NULL, which timestamp-less entries cannot satisfy")


async def test_a_timestampless_entry_cannot_get_two_assignments(clean):
    """The behaviour NULLS NOT DISTINCT buys. With a plain UNIQUE this second write would succeed."""
    job = await _mk_job()
    async with async_session() as s:
        e = LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job, entry_hash=uuid.uuid4().hex,
                     source_file="t.log", line_number=1, timestamp=None,
                     entry_type=LogEntryType.info, raw_body="no ts")
        t1 = LogTransaction(id=uuid.uuid4(), job_id=job, customer_code=CC, started_at=T0,
                            date=T0.date(), entry_count=0)
        t2 = LogTransaction(id=uuid.uuid4(), job_id=job, customer_code=CC, started_at=T0,
                            date=T0.date(), entry_count=0)
        s.add_all([e, t1, t2])
        await s.commit()
        eid, tid1, tid2 = e.id, t1.id, t2.id

    async with async_session() as s:
        await A.write(s, transaction_id=tid1, entries=[e], customer_code=CC)
        await s.commit()
    async with async_session() as s:
        await A.write(s, transaction_id=tid2, entries=[e], customer_code=CC)
        await s.commit()

    async with async_session() as s:
        rows = (await s.execute(select(LogEntryAssignment)
                                .where(LogEntryAssignment.entry_id == eid))).scalars().all()
    assert len(rows) == 1, "a NULL-timestamp entry collected more than one assignment"
    assert rows[0].transaction_id == tid2, "the newest grouping should win"


async def test_entry_ts_exists_and_is_nullable():
    """Nullable because log_entries.timestamp is: an entry with no parsed timestamp still needs an
    assignment. Once partitioned those land in the DEFAULT partition."""
    async with async_session() as s:
        row = (await s.execute(sa_text(
            "SELECT data_type, is_nullable FROM information_schema.columns "
            "WHERE table_name = 'log_entry_assignment' AND column_name = 'entry_ts'"
        ))).first()
    assert row is not None, "entry_ts is missing - nothing to partition the table on"
    assert row[0] == "timestamp with time zone"
    assert row[1] == "YES"


async def test_lookup_by_entry_id_alone_uses_an_index(clean):
    """Guards the 240x regression directly. Three hot paths filter on entry_id with no time:
    load_transaction_by_entry, is_unassigned, belongs_to_transaction."""
    job = await _mk_job()
    entries = await _mk_entries(job, 5)
    async with async_session() as s:
        txn = LogTransaction(id=uuid.uuid4(), job_id=job, customer_code=CC,
                             started_at=T0, date=T0.date(), entry_count=0)
        s.add(txn)
        await s.commit()
        tid = txn.id
    async with async_session() as s:
        await A.write(s, transaction_id=tid, entries=entries, customer_code=CC)
        await s.commit()

    async with async_session() as s:
        # enable_seqscan=off, and assert on INDEX COND rather than on the plan node.
        #
        # A tiny test table is always cheapest to seq-scan, so asserting "no Seq Scan" would just be
        # measuring the planner's cost estimate. What actually matters is whether entry_id can be
        # SEEKED: that is true only while it is the LEADING column. With (entry_ts, entry_id) the
        # planner can still touch the index, but only as a full scan with the predicate demoted to a
        # Filter - which is exactly the 10.8 ms case. An "Index Cond" naming entry_id proves the seek.
        await s.execute(sa_text("SET LOCAL enable_seqscan = off"))
        plan = "\n".join((await s.execute(sa_text(
            f"EXPLAIN SELECT transaction_id FROM log_entry_assignment "
            f"WHERE entry_id = '{entries[0].id}'::uuid"
        ))).scalars().all())
    assert "Index Cond" in plan and "entry_id" in plan.split("Index Cond")[1].split("\n")[0], (
        f"entry_id is not the leading column - it cannot be seeked, only filtered:\n{plan}")


# =============================================================== entry_ts is populated
async def test_write_populates_entry_ts_from_the_owning_entry(clean):
    """entry_ts must match the entry's own timestamp, or the assignment lands in a different daily
    partition from the entry it describes and the two cannot be dropped together."""
    job = await _mk_job()
    entries = await _mk_entries(job, 3)
    async with async_session() as s:
        txn = LogTransaction(id=uuid.uuid4(), job_id=job, customer_code=CC,
                             started_at=T0, date=T0.date(), entry_count=0)
        s.add(txn)
        await s.commit()
        tid = txn.id

    async with async_session() as s:
        await A.write(s, transaction_id=tid, entries=entries, customer_code=CC)
        await s.commit()

    async with async_session() as s:
        mismatched = await s.scalar(sa_text("""
            SELECT count(*) FROM log_entry_assignment a
            JOIN log_entries e ON e.id = a.entry_id
            WHERE a.customer_code = :cc AND a.entry_ts IS DISTINCT FROM e.timestamp
        """), {"cc": CC})
    assert mismatched == 0, "entry_ts disagrees with the entry's timestamp"


async def test_stage2_populates_entry_ts_end_to_end(clean):
    """Through the real Stage 2 path, not just the repository."""
    job = await _mk_job()
    await _mk_entries(job, 6)
    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)

    async with async_session() as s:
        total = await s.scalar(select(func.count()).select_from(LogEntryAssignment)
                               .where(LogEntryAssignment.customer_code == CC))
        null_ts = await s.scalar(select(func.count()).select_from(LogEntryAssignment)
                                 .where(LogEntryAssignment.customer_code == CC,
                                        LogEntryAssignment.entry_ts.is_(None)))
    assert total == 6
    assert null_ts == 0, "Stage 2 wrote assignments with no entry_ts"


async def test_entry_with_no_timestamp_still_gets_an_assignment(clean):
    """A NULL-timestamp entry must not break the write. Its assignment carries a NULL entry_ts and,
    once partitioned, both land in the DEFAULT partition."""
    job = await _mk_job()
    async with async_session() as s:
        entry = LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job,
                         entry_hash=uuid.uuid4().hex, source_file="t.log", line_number=1,
                         timestamp=None, entry_type=LogEntryType.info, raw_body="no ts")
        txn = LogTransaction(id=uuid.uuid4(), job_id=job, customer_code=CC,
                             started_at=T0, date=T0.date(), entry_count=0)
        s.add_all([entry, txn])
        await s.commit()
        eid, tid = entry.id, txn.id

    async with async_session() as s:
        await A.write(s, transaction_id=tid, entries=[entry], customer_code=CC)
        await s.commit()

    async with async_session() as s:
        row = (await s.execute(select(LogEntryAssignment)
                               .where(LogEntryAssignment.entry_id == eid))).scalars().one()
    assert row.entry_ts is None


# =============================================================== the four delete paths
async def test_regroup_replaces_assignments_without_leaving_orphans(clean):
    """derive_transactions deletes transactions on every window rebuild. Without the cascade it must
    delete their assignments explicitly, or every regroup accumulates orphans."""
    job = await _mk_job()
    await _mk_entries(job, 6)
    for _ in range(3):
        async with async_session() as s:
            await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)

    async with async_session() as s:
        orphans = await s.scalar(sa_text("""
            SELECT count(*) FROM log_entry_assignment a
            LEFT JOIN log_transactions t ON t.id = a.transaction_id
            WHERE a.customer_code = :cc AND t.id IS NULL
        """), {"cc": CC})
    assert orphans == 0, "a regroup left assignments pointing at deleted transactions"


async def test_regroup_all_leaves_no_orphans(clean):
    """regroup_all deletes every transaction for the tenant - the widest delete in the codebase."""
    job = await _mk_job()
    await _mk_entries(job, 6)
    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)
    async with async_session() as s:
        await d.regroup_all(s, CC)

    async with async_session() as s:
        orphans = await s.scalar(sa_text("""
            SELECT count(*) FROM log_entry_assignment a
            LEFT JOIN log_transactions t ON t.id = a.transaction_id
            WHERE a.customer_code = :cc AND t.id IS NULL
        """), {"cc": CC})
    assert orphans == 0


async def test_regroup_incremental_leaves_no_orphans(clean):
    """regroup_incremental deletes every UNSEALED transaction, on the live path, every cycle."""
    job = await _mk_job()
    await _mk_entries(job, 6)
    async with async_session() as s:
        await d.regroup_incremental(s, CC)
    async with async_session() as s:
        await d.regroup_incremental(s, CC)

    async with async_session() as s:
        orphans = await s.scalar(sa_text("""
            SELECT count(*) FROM log_entry_assignment a
            LEFT JOIN log_transactions t ON t.id = a.transaction_id
            WHERE a.customer_code = :cc AND t.id IS NULL
        """), {"cc": CC})
    assert orphans == 0


async def test_tenant_purge_removes_assignments(clean):
    """logspace_cleanup relied on jobs -> entries -> assignments cascading. With the FK gone it must
    delete them itself, or a purged tenant leaves rows behind forever."""
    from app.services.logspace_cleanup import purge_logspace

    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="purge test"))
        await s.commit()
    job = await _mk_job()
    await _mk_entries(job, 4)
    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=4), commit=True)
    assert await _assignments() > 0, "nothing to purge - the test would pass vacuously"

    async with async_session() as s:
        assert await purge_logspace(s, CC) is True
    assert await _assignments() == 0, "tenant purge left orphan assignment rows"


async def test_full_wipe_removes_assignments(clean):
    """DELETE /logs/data?confirm=true - the tenant-scoped wipe, same cascade reliance."""
    from app.api.v1.logs import delete_log_data

    job = await _mk_job()
    await _mk_entries(job, 4)
    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=4), commit=True)
    assert await _assignments() > 0

    async with async_session() as s:
        await delete_log_data(customer=CC, date_from=None, date_to=None, confirm=True, db=s)
    assert await _assignments() == 0, "full wipe left orphan assignment rows"


async def test_date_range_delete_removes_assignments_for_that_range(clean):
    """The date-range delete removes entries directly. Their assignments must go too - an assignment
    whose entry no longer exists is unreachable and would never be cleaned up."""
    from app.api.v1.logs import delete_log_data
    from datetime import date as date_type

    job = await _mk_job()
    await _mk_entries(job, 4)
    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=4), commit=True)
    assert await _assignments() > 0

    async with async_session() as s:
        await delete_log_data(customer=CC, date_from=T0.date(), date_to=T0.date(),
                              confirm=False, db=s)

    async with async_session() as s:
        dangling = await s.scalar(sa_text("""
            SELECT count(*) FROM log_entry_assignment a
            LEFT JOIN log_entries e ON e.id = a.entry_id
            WHERE a.customer_code = :cc AND e.id IS NULL
        """), {"cc": CC})
    assert dangling == 0, "date-range delete left assignments whose entries are gone"
