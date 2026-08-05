"""Chunk 24 (step 2 part B of docs/plan/2026-08-05_20-32_daily-partitioning.md): bound Stage 2's
own queries.

Part A bounded the read paths. These are the three left on the WRITE path, and partitioning made them
worse rather than better - each now opens ~130 partitions instead of scanning one table:

- `_persist` loaded EVERY transaction id the tenant had ever had into a Python set, once per
  sub-window, purely to detect a deterministic-id clash. Measured on the local database that is an
  Append over all 129 partitions, sequential-scanning most of them. On production it was 109k ids per
  call, and it grows forever.
- `regroup_incremental` ran `SELECT DISTINCT customer_code ... WHERE unassigned` - a whole-table
  anti-join - even when the caller had already named the customer.
- its live-tail read then pulled every unassigned entry for that tenant with no time bound and no
  LIMIT, so a backlog loads unboundedly into one session.

The risk in all three is the same: a bound that is too tight silently drops work rather than failing.
So the tests below are mostly about the EDGES.

`_persist`'s clash check has an exactness argument worth stating, because the whole fix rests on it.
A transaction's id is `uuid5` of its anchor entry's hash, so a colliding transaction was built from
the SAME anchor entry and therefore shares its timestamp. Its `started_at` is the minimum over its
entries, so it lies in `[anchor_ts - pad, anchor_ts]` where pad is at least the seal window (the
system's guarantee that no transaction spans more). Asking only about the ids being written, within
that padded window, is therefore exact - not an approximation that happens to work.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select, text

from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import assignments
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.settings import settings

CC = "TEST_CHUNK24"
OTHER = "TEST_CHUNK24_OTHER"


def _utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


async def _cleanup(db):
    for c in (CC, OTHER):
        await db.execute(delete(LogEntryAssignment).where(LogEntryAssignment.customer_code == c))
        await db.execute(delete(LogEntry).where(LogEntry.customer_code == c))
        await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == c))
        await db.execute(delete(Job).where(Job.customer_code == c))
    await db.flush()


async def _job(db, cc=CC):
    j = Job(customer_code=cc, filename="c24.log", storage_key=f"{cc}/{uuid.uuid4().hex}/c24.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    return j


async def _entry(db, job, ts, line, *, cc=CC, body="INFO x"):
    e = LogEntry(customer_code=cc, job_id=job.id, timestamp=ts, source_file="c24.log",
                 line_number=line, level="INFO", raw_body=body, message=body,
                 entry_hash=uuid.uuid4().hex)
    db.add(e)
    await db.flush()
    return e


async def _txn(db, job, *, tid, started_at, cc=CC, sealed=False):
    db.add(LogTransaction(id=tid, customer_code=cc, job_id=job.id, sealed=sealed,
                          started_at=started_at, ended_at=started_at,
                          date=started_at.date() if started_at else None))
    await db.flush()
    return tid


# ==================================================== _persist: the clash check
async def test_existing_ids_asks_only_about_the_ids_being_written(db):
    """The whole point. It used to load every transaction the tenant had; it must now return only
    those of the candidate ids that actually exist."""
    await _cleanup(db)
    job = await _job(db)
    here, elsewhere = uuid.uuid4(), uuid.uuid4()
    await _txn(db, job, tid=here, started_at=_utc(2026, 6, 10, 9))
    await _txn(db, job, tid=elsewhere, started_at=_utc(2026, 6, 10, 9))

    got = await dt._existing_transaction_ids(db, CC, [here], window=None)
    assert got == {here}, "asked about one id, must not learn about the other"


async def test_existing_ids_returns_empty_when_none_of_the_candidates_exist(db):
    await _cleanup(db)
    assert await dt._existing_transaction_ids(db, CC, [uuid.uuid4()], window=None) == set()
    assert await dt._existing_transaction_ids(db, CC, [], window=None) == set()


async def test_existing_ids_never_crosses_tenants(db):
    """A deterministic id embeds the customer code, so a cross-tenant collision should be impossible -
    but the query must still scope by tenant rather than rely on that."""
    await _cleanup(db)
    job_other = await _job(db, OTHER)
    tid = uuid.uuid4()
    await _txn(db, job_other, tid=tid, started_at=_utc(2026, 6, 10, 9), cc=OTHER)
    assert await dt._existing_transaction_ids(db, CC, [tid], window=None) == set()


async def test_the_window_never_hides_a_real_clash_at_the_edge(db):
    """The exactness argument, tested at its boundary. A colliding transaction's `started_at` can sit
    up to a full pad EARLIER than the anchor entry it was built from, so the window must reach back at
    least that far or a genuine clash is missed and the builder overwrites it."""
    await _cleanup(db)
    job = await _job(db)
    anchor_ts = _utc(2026, 6, 10, 12, 0, 0)
    earliest = anchor_ts - dt._regroup_pad()
    tid = uuid.uuid4()
    await _txn(db, job, tid=tid, started_at=earliest)

    window = dt._clash_window([anchor_ts])
    assert await dt._existing_transaction_ids(db, CC, [tid], window=window) == {tid}


async def test_the_clash_window_covers_a_null_started_at(db):
    """A transaction all of whose entries lack a parsable timestamp has a NULL `started_at` and lives
    in the DEFAULT partition. A plain range predicate is FALSE for NULL, so it would go unseen and the
    clash would be missed."""
    await _cleanup(db)
    job = await _job(db)
    tid = uuid.uuid4()
    await _txn(db, job, tid=tid, started_at=None)
    window = dt._clash_window([_utc(2026, 6, 10, 12)])
    assert await dt._existing_transaction_ids(db, CC, [tid], window=window) == {tid}


def test_the_clash_window_is_padded_by_at_least_the_seal_window():
    """The pad IS the correctness margin - it is the system's guarantee for how far a transaction can
    span. A smaller pad would make the clash check lossy."""
    ts = _utc(2026, 6, 10, 12)
    w = dt._clash_window([ts])
    assert w.start <= ts - timedelta(seconds=settings.log_seal_window_seconds)
    assert w.end > ts


def test_the_clash_window_is_none_when_nothing_can_be_derived():
    """All-NULL timestamps yield no window, and the caller must then fall back to an unbounded (but
    still id-scoped, so bounded in rows) lookup rather than filtering everything out."""
    assert dt._clash_window([]) is None
    assert dt._clash_window([None, None]) is None


async def test_a_rebuild_still_skips_a_transaction_that_already_exists(db):
    """End-to-end no-regression on the behaviour the clash check exists for: regrouping a window whose
    transactions are already present must SKIP them, not duplicate them."""
    await _cleanup(db)
    job = await _job(db)
    base = _utc(2026, 6, 10, 9, 0, 0)
    for i in range(4):
        await _entry(db, job, base + timedelta(seconds=i), i)
    await db.commit()

    first = await dt.regroup_window(db, CC, base, base + timedelta(seconds=4))
    second = await dt.regroup_window(db, CC, base, base + timedelta(seconds=4))
    total = await db.scalar(select(func.count()).select_from(LogTransaction)
                            .where(LogTransaction.customer_code == CC))
    assert first["transactions_created"] >= 1
    # Deterministic ids mean the rebuild lands on the same rows; the count must not grow.
    assert total == first["transactions_created"], second


async def test_the_clash_lookup_prunes_partitions(db):
    """Without the window this is an id-only lookup, which is correct but probes all ~130 partitions."""
    ts = _utc(2026, 6, 10, 12)
    stmt = dt._existing_ids_stmt(CC, [uuid.uuid4()], window=dt._clash_window([ts]))
    plan = "\n".join(r[0] for r in (await db.execute(
        text("EXPLAIN " + str(stmt.compile(db.bind, compile_kwargs={"literal_binds": True}))))).all())
    scanned = [ln for ln in plan.splitlines() if "log_transactions_2026" in ln]
    assert len(scanned) <= 3, f"expected the padded window to prune to a few days:\n{plan}"


# ==================================================== regroup_incremental
async def test_naming_a_customer_skips_the_whole_table_anti_join(db):
    """The caller already knows the tenant in both real call sites. Running a DISTINCT anti-join over
    every entry in the database to rediscover it is pure waste."""
    await _cleanup(db)
    job = await _job(db)
    await _entry(db, job, _utc(2026, 6, 10, 9), 1)
    await db.commit()
    codes = await dt._codes_needing_regroup(db, CC)
    assert codes == [CC]


async def test_a_named_customer_with_no_unassigned_entries_returns_nothing(db):
    """It must still be an existence CHECK, not an unconditional yes - otherwise every cycle would
    pointlessly load and regroup an empty tail."""
    await _cleanup(db)
    await db.commit()
    assert await dt._codes_needing_regroup(db, CC) == []


async def test_without_a_customer_it_still_discovers_them(db):
    """The None path is the documented "process every tenant" behaviour and must not regress."""
    await _cleanup(db)
    job = await _job(db)
    await _entry(db, job, _utc(2026, 6, 10, 9), 1)
    await db.commit()
    assert CC in await dt._codes_needing_regroup(db, None)


async def test_the_live_tail_read_is_bounded_but_still_returns_the_live_tail(db):
    """No-regression: everything within the live window must still come back."""
    await _cleanup(db)
    job = await _job(db)
    newest = datetime.now(timezone.utc) - timedelta(minutes=1)
    for i in range(3):
        await _entry(db, job, newest - timedelta(seconds=i), i)
    await db.commit()
    rows = await dt._live_tail(db, CC)
    assert len(rows) == 3


async def test_the_live_tail_read_excludes_entries_past_the_abandon_window(db):
    """An entry older than the abandon window cannot join a live transaction - anything it belonged to
    is already sealed - so loading it every cycle is work that can never produce a result."""
    await _cleanup(db)
    job = await _job(db)
    newest = datetime.now(timezone.utc) - timedelta(minutes=1)
    await _entry(db, job, newest, 1)
    ancient = newest - timedelta(seconds=settings.log_abandon_window_seconds) - timedelta(days=2)
    await _entry(db, job, ancient, 2)
    await db.commit()
    rows = await dt._live_tail(db, CC)
    assert [r.line_number for r in rows] == [1]


async def test_a_timestampless_entry_is_never_dropped_from_the_live_tail(db):
    """It has no timestamp to compare, so a range predicate excludes it. Silently never grouping such
    an entry is exactly the kind of loss this whole chunk is written to prevent."""
    await _cleanup(db)
    job = await _job(db)
    newest = datetime.now(timezone.utc) - timedelta(minutes=1)
    await _entry(db, job, newest, 1)
    await _entry(db, job, None, 2)
    await db.commit()
    rows = await dt._live_tail(db, CC)
    assert {r.line_number for r in rows} == {1, 2}


async def test_a_tenant_whose_backlog_is_entirely_old_is_reported_not_silently_skipped(db, caplog):
    """The one real cost of bounding this read. If everything unassigned falls outside the live
    window, the tenant needs a full regroup - and an operator has to be TOLD, or the entries sit
    unassigned forever with nothing indicating it."""
    await _cleanup(db)
    job = await _job(db)
    ancient = datetime.now(timezone.utc) - timedelta(days=400)
    await _entry(db, job, ancient, 1)
    # a much newer entry makes the tenant's "now" recent, pushing the old one outside the window
    await _entry(db, job, datetime.now(timezone.utc) - timedelta(minutes=1), 2)
    await db.execute(text(
        "INSERT INTO log_entry_assignment (entry_id, entry_ts, transaction_id, seq, customer_code)"
        " SELECT id, timestamp, gen_random_uuid(), 0, customer_code FROM log_entries"
        " WHERE customer_code = :c AND line_number = 2"), {"c": CC})
    await db.commit()
    with caplog.at_level("WARNING"):
        rows = await dt._live_tail(db, CC)
    assert rows == []
    assert any("full regroup" in r.message for r in caplog.records), \
        "a tenant with only out-of-window work must be surfaced, not silently skipped"


async def test_regroup_incremental_still_groups_a_normal_live_tail(db):
    """The end-to-end no-regression guard over all three changes at once."""
    await _cleanup(db)
    job = await _job(db)
    base = datetime.now(timezone.utc) - timedelta(minutes=2)
    for i in range(4):
        await _entry(db, job, base + timedelta(seconds=i), i)
    await db.commit()
    stats = await dt.regroup_incremental(db, CC)
    assert stats["customers"] == 1
    assert stats["entries_scanned"] == 4
    n = await db.scalar(select(func.count()).select_from(LogEntryAssignment)
                        .where(LogEntryAssignment.customer_code == CC))
    assert n == 4, "every entry in the live tail should have been assigned"
