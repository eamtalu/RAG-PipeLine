"""Chunk 53 (S1 of docs/analytics-ml-architecture/final_architecture.md, section 18): make sealing an
EXPLICIT operation, and let the notification cursor see the result.

The bug S1 fixes
----------------
`sealed` was written in exactly one place - `_write_transaction` (`derive_transactions.py:611`) from
`_is_sealed` (`:542-553`). No UPDATE set it anywhere. So sealing was a SIDE EFFECT of re-insertion:
a row only ever became sealed because some later rebuild happened to reconstruct it and decide so.

`regroup_window` frees transactions anchored in `[lo - pad, hi]`. Once `lo_p` advances past a row's
`started_at` (about 960 s) nothing re-derives it, so it never seals. Measured on the deployed
database: 2,516 rows permanently unsealed, oldest 2026-08-06.

Why the sealer alone is NOT enough, which is what this chunk found
-----------------------------------------------------------------
The obvious fix is an UPDATE that sets `sealed = true`. That fixes the flag and nothing else, because
the notification cursor reads `created_at`:

    read_window: lo = the rule's cursor_at, hi = now - lag        (cursor.py:63-76)
    the cursor only ever moves FORWARD                            (cursor.py:106+)
    the 3600 s lookback applies ONLY when cursor_at IS NULL

`alertable_predicate`'s own docstring names the mechanism it relies on: *"every Stage 2 rebuild
refreshes created_at, so an in-flight transaction re-enters the cursor's feed on every rebuild until
it seals."* An UPDATE does not refresh `created_at`. So a row sealed by the sealer NEVER re-enters the
feed, `stability.py`'s `incomplete AND sealed` alert - which its own docstring calls "the genuinely
useful" one - still never fires, and the sealer would have introduced a SECOND silent miss on top of
the one it was written to fix.

So S1 is three things, not one:

  1. `updated_at`, backfilled to `created_at` so existing rows behave identically
  2. the notification cursor reads `updated_at` instead of `created_at`
  3. the sealer itself, bounded by a horizon

`created_at` keeps its current meaning here; S3 is what changes that. Moving the cursor NOW is
deliberate: today only the sealer writes an UPDATE, so the change is observable in isolation. After S3
every rebuild is an UPDATE and the same edit would land under churn.

Two clocks, on purpose
----------------------
The seal/abandon cutoffs are measured against the tenant's NEWEST ENTRY (`_cutoffs`, the log's notion
of "now") so back-dated ingestion seals correctly. The horizon is measured on the DATABASE clock,
because what it protects against is retention dropping the partition - and retention uses `db_today`.
A tenant whose logs are 90 days stale would otherwise get a horizon 150 days back and the sealer would
reach into partitions already gone.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select, text

from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.mnp_log_ingestion.pipeline import sealer
from app.services.mnp_log_ingestion.pipeline.derive_transactions import _cutoffs, _is_sealed
from app.services.notifications import cursor as cur
from app.settings import settings

CC = "test_chunk53"
OTHER = "test_chunk53_other"


# =============================================================== fixtures
async def _job(db, cc=CC):
    j = Job(customer_code=cc, filename="c53.log", storage_key=f"{cc}/{uuid.uuid4().hex}/c53.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    return j


async def _entry(db, job, ts, cc=CC):
    """An entry is what establishes the tenant's notion of 'now' for `_cutoffs`."""
    e = LogEntry(customer_code=cc, job_id=job.id, timestamp=ts, source_file="c53.log",
                 line_number=1, level="INFO", raw_body="x", message="x",
                 entry_hash=uuid.uuid4().hex)
    db.add(e)
    await db.flush()
    return e


async def _txn(db, job, *, status, ended_at, sealed=False, cc=CC, created_at=None):
    created_at = created_at or datetime.now(timezone.utc)
    t = LogTransaction(id=uuid.uuid4(), customer_code=cc, job_id=job.id, sealed=sealed,
                       status=status, started_at=ended_at, ended_at=ended_at,
                       date=ended_at.date())
    db.add(t)
    await db.flush()
    await db.execute(text("UPDATE log_transactions SET created_at = :c WHERE id = :i"),
                     {"c": created_at, "i": t.id})
    await db.flush()
    return t


async def _sealed_of(db, tid) -> bool:
    return await db.scalar(select(LogTransaction.sealed).where(LogTransaction.id == tid))


# =============================================================== 1. the column
async def test_log_transactions_has_updated_at(db):
    """The cursor cannot move to a column that does not exist. Nullable is not acceptable either: the
    cursor's ORDER BY and range filter would silently drop every NULL row from the feed."""
    col = await db.scalar(text(
        "SELECT is_nullable FROM information_schema.columns "
        "WHERE table_name = 'log_transactions' AND column_name = 'updated_at'"))
    assert col == "NO", "updated_at must exist and be NOT NULL, or the cursor loses rows"


async def test_a_newly_written_transaction_gets_updated_at(db):
    """`_write_transaction` is the ONE place a row is constructed (derive_transactions.py:611). If it
    does not stamp `updated_at`, every new row is invisible to the cursor."""
    job = await _job(db)
    t = await _txn(db, job, status=LogTransactionStatus.success,
                   ended_at=datetime.now(timezone.utc))
    row = (await db.execute(select(LogTransaction.created_at, LogTransaction.updated_at)
                            .where(LogTransaction.id == t.id))).one()
    assert row.updated_at is not None
    # equal at birth, so a row that is never updated behaves exactly as it does today
    assert abs((row.updated_at - row.created_at).total_seconds()) < 1.0


# =============================================================== 2. the cursor moved
async def test_the_cursor_reads_updated_at_not_created_at(db):
    """The whole point of S1's second half. Asserted on the compiled SQL rather than on behaviour so
    the failure names the column, and so a future edit that reverts it fails here loudly."""
    w = cur.Window(lo=datetime.now(timezone.utc) - timedelta(hours=1),
                   hi=datetime.now(timezone.utc))
    sql = str(cur.window_stmt(CC, w, limit=10).compile(
        db.bind, compile_kwargs={"literal_binds": True}))
    where = sql.split("WHERE", 1)[1]
    assert "updated_at" in where, "the cursor must filter on updated_at"
    assert "created_at" not in where, "created_at must no longer gate the feed"
    assert "ORDER BY log_transactions.updated_at ASC" in sql, \
        "advance() takes the newest row of the ordering; it must be the same column"


async def test_a_sealed_row_re_enters_the_feed(db):
    """The bug this chunk exists to close. `created_at` is old enough that any rule's cursor has long
    passed it; only a bumped `updated_at` can bring it back."""
    job = await _job(db)
    old = datetime.now(timezone.utc) - timedelta(days=5)
    t = await _txn(db, job, status=LogTransactionStatus.incomplete, ended_at=old, created_at=old)

    cursor_at = datetime.now(timezone.utc) - timedelta(hours=1)   # far ahead of created_at
    window = cur.read_window(cursor_at, now=datetime.now(timezone.utc),
                             lag=timedelta(seconds=settings.notification_cursor_lag_seconds),
                             lookback=timedelta(seconds=settings.notification_lookback_seconds))
    before = list((await db.execute(cur.window_stmt(CC, window, limit=50))).scalars().all())
    assert t.id not in [r.id for r in before], "precondition: the row is behind the cursor"

    await db.execute(text("UPDATE log_transactions SET sealed = true, "
                          "updated_at = clock_timestamp() - interval '120 seconds' WHERE id = :i"),
                     {"i": t.id})
    await db.flush()
    after = list((await db.execute(cur.window_stmt(CC, window, limit=50))).scalars().all())
    assert t.id in [r.id for r in after], \
        "a sealed row must re-enter the feed, or stability.py's alert can never fire"


def test_newest_seen_reads_the_same_column_the_window_orders_by():
    """`advance` moves the cursor to the newest row it saw. Reading a different column from the one the
    window ordered by would move the cursor past rows it never fetched - the one thing cursor.py's
    docstring says must never happen."""
    now = datetime.now(timezone.utc)

    class Row:
        def __init__(self, u): self.updated_at, self.created_at = u, now - timedelta(days=30)

    rows = [Row(now - timedelta(seconds=10)), Row(now - timedelta(seconds=5))]
    assert cur._newest(rows) == rows[-1].updated_at


# =============================================================== 3. the sealer
@pytest.mark.parametrize("status,age_seconds,expected", [
    # terminal statuses seal at the SHORT window
    (LogTransactionStatus.success, settings.log_seal_window_seconds + 60, True),
    (LogTransactionStatus.success, settings.log_seal_window_seconds - 60, False),
    (LogTransactionStatus.error, settings.log_seal_window_seconds + 60, True),
    (LogTransactionStatus.soft, settings.log_seal_window_seconds + 60, True),
    # incomplete waits for the LONG one: never split a slow request
    (LogTransactionStatus.incomplete, settings.log_seal_window_seconds + 60, False),
    (LogTransactionStatus.incomplete, settings.log_abandon_window_seconds + 60, True),
])
async def test_the_sealer_matches_is_sealed(db, status, age_seconds, expected):
    """The rule now has TWO implementations - `_is_sealed` in Python for the rebuild path, and the
    sealer's SQL. They must agree, or a row's sealed-ness would depend on which path touched it last.
    This is the most important test in the chunk."""
    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _entry(db, job, now)                      # the tenant's notion of "now"
    ended = now - timedelta(seconds=age_seconds)
    t = await _txn(db, job, status=status, ended_at=ended)

    seal_cut, abandon_cut = await _cutoffs(db, CC)
    python_says = _is_sealed({"ended_at": ended, "status": status}, seal_cut, abandon_cut)
    assert python_says is expected, "the fixture disagrees with _is_sealed; fix the fixture"

    await sealer.seal_customer(db, CC)
    assert await _sealed_of(db, t.id) is expected, \
        f"the SQL and _is_sealed disagree for {status} at {age_seconds}s"


async def test_a_transaction_with_no_end_is_never_sealed(db):
    """`ended_at IS NULL` means no entry has closed it. `_is_sealed` returns False for it and the SQL
    must too - a NULL comparison silently drops the row from the UPDATE, which is the right answer for
    the wrong reason, so it is pinned."""
    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _entry(db, job, now)
    t = LogTransaction(id=uuid.uuid4(), customer_code=CC, job_id=job.id, sealed=False,
                       status=LogTransactionStatus.incomplete,
                       started_at=now - timedelta(days=2), ended_at=None,
                       date=(now - timedelta(days=2)).date())
    db.add(t)
    await db.flush()
    await sealer.seal_customer(db, CC)
    assert await _sealed_of(db, t.id) is False


async def test_the_sealer_is_idempotent(db):
    """A tick runs constantly. The second run must seal nothing, or every tick is a full rewrite of the
    tail and S1 becomes a write amplifier instead of a fix."""
    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _entry(db, job, now)
    await _txn(db, job, status=LogTransactionStatus.success,
               ended_at=now - timedelta(seconds=settings.log_seal_window_seconds + 60))
    assert await sealer.seal_customer(db, CC) == 1
    assert await sealer.seal_customer(db, CC) == 0


async def test_sealing_bumps_updated_at_but_not_created_at(db):
    """`created_at` must keep meaning "first written" for the analytics frontier (F6). Refreshing it
    here would work for notifications and break the frontier - the trap option C in 18e names."""
    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _entry(db, job, now)
    born = now - timedelta(hours=3)
    t = await _txn(db, job, status=LogTransactionStatus.success,
                   ended_at=now - timedelta(seconds=settings.log_seal_window_seconds + 60),
                   created_at=born)
    await sealer.seal_customer(db, CC)
    row = (await db.execute(select(LogTransaction.created_at, LogTransaction.updated_at)
                            .where(LogTransaction.id == t.id))).one()
    assert abs((row.created_at - born).total_seconds()) < 1.0, "created_at must not move"
    assert row.updated_at > row.created_at, "updated_at must move"


# =============================================================== 4. the horizon
async def test_the_sealer_does_not_reach_past_the_horizon(db):
    """Unbounded, the sealer would seal a 59-day-old row, bump `updated_at`, and - now that the cursor
    reads it - alert on a transaction whose entries are dropped the next day, leaving a detail view
    with no entries. Horizon kept at 60 days by decision; the risk it leaves is the boundary day."""
    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _entry(db, job, now)
    beyond = now - timedelta(days=settings.log_seal_horizon_days + 1)
    t = await _txn(db, job, status=LogTransactionStatus.success, ended_at=beyond)
    await sealer.seal_customer(db, CC)
    assert await _sealed_of(db, t.id) is False, "a row past the horizon must be left alone"


async def test_the_horizon_uses_the_database_clock_not_the_log_clock(db):
    """A tenant whose logs stopped 90 days ago has a `max_entry_ts` 90 days back. A log-clock horizon
    would then be 150 days back and the sealer would reach into partitions retention already dropped.
    The cutoffs use the log clock; the horizon must not."""
    job = await _job(db)
    stale = datetime.now(timezone.utc) - timedelta(days=90)
    await _entry(db, job, stale)                       # the tenant's clock is 90 days behind
    t = await _txn(db, job, status=LogTransactionStatus.success,
                   ended_at=stale - timedelta(days=1))  # 91 days old: past ANY sane horizon
    await sealer.seal_customer(db, CC)
    assert await _sealed_of(db, t.id) is False


# =============================================================== 5. tenant enumeration
async def test_the_sealer_finds_a_tenant_with_no_open_ticket(db):
    """THE bug. The plan said to hang the sealer off the stitch worker, which iterates
    `customers_with_due_work()` - tenants with an OPEN log_regroup_pending row. But the 2,516 stuck
    rows are stuck precisely because nothing tickets them any more. Enumerating by ticket would leave
    the sealer unable to reach the rows it exists to fix."""
    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _entry(db, job, now)
    await _txn(db, job, status=LogTransactionStatus.success,
               ended_at=now - timedelta(seconds=settings.log_seal_window_seconds + 60))

    open_tickets = await db.scalar(
        select(func.count()).select_from(LogRegroupPending).where(
            LogRegroupPending.customer_code == CC,
            LogRegroupPending.consumed_at.is_(None)))
    assert open_tickets == 0, "precondition: this tenant has no queued stitch work"

    assert CC in await sealer.customers_needing_seal(db), \
        "the sealer must enumerate by unsealed rows, never by the stitch queue"


async def test_sealing_one_tenant_leaves_another_alone(db):
    """Cutoffs are per tenant by design (one customer's stale logs must not be dragged forward by
    another's active stream), so the UPDATE must be scoped too."""
    now = datetime.now(timezone.utc)
    ja, jb = await _job(db), await _job(db, OTHER)
    await _entry(db, ja, now)
    await _entry(db, jb, now, cc=OTHER)
    due = now - timedelta(seconds=settings.log_seal_window_seconds + 60)
    a = await _txn(db, ja, status=LogTransactionStatus.success, ended_at=due)
    b = await _txn(db, jb, status=LogTransactionStatus.success, ended_at=due, cc=OTHER)
    await sealer.seal_customer(db, CC)
    assert await _sealed_of(db, a.id) is True
    assert await _sealed_of(db, b.id) is False


async def test_a_tenant_with_nothing_to_seal_is_not_enumerated(db):
    """Otherwise every tick takes the per-tenant advisory lock for every tenant that has ever existed,
    and the sealer's cost grows with tenant count instead of with work."""
    job = await _job(db)
    now = datetime.now(timezone.utc)
    await _entry(db, job, now)
    await _txn(db, job, status=LogTransactionStatus.success, ended_at=now)   # far too recent to seal
    assert CC not in await sealer.customers_needing_seal(db)


# =============================================================== 6. the index that makes it cheap
async def test_the_partial_index_exists(db):
    """`customers_needing_seal` and the UPDATE both filter `NOT sealed`, which is 2.1% of rows. Without
    a partial index each tick is a full scan of a 60-day partition set."""
    idx = await db.scalar(text(
        "SELECT indexdef FROM pg_indexes "
        "WHERE tablename = 'log_transactions' AND indexname = 'ix_log_transactions_unsealed'"))
    assert idx is not None, "the sealer's partial index is missing"
    assert "NOT sealed" in idx.replace("(sealed = false)", "NOT sealed"), \
        f"index must be partial on NOT sealed, got: {idx}"


async def test_the_cursor_index_exists(db):
    """The cursor's window query is `customer_code = ? AND updated_at >= ? AND < ? ORDER BY
    updated_at` - CLAUDE.md rule 4: the index must match both the filter and the sort."""
    idx = await db.scalar(text(
        "SELECT indexdef FROM pg_indexes "
        "WHERE tablename = 'log_transactions' AND indexname = 'ix_log_transactions_customer_updated'"))
    assert idx is not None, "the cursor has no matching composite index"
    assert "customer_code" in idx and "updated_at" in idx


# =============================================================== 7. the worker entry point
async def _committed(status, ended_at, cc):
    """A COMMITTED row. `seal_due` opens its own sessions and commits, so it cannot see anything the
    `db` fixture is holding in an uncommitted transaction."""
    from app.config.database import async_session
    async with async_session() as s:
        j = Job(customer_code=cc, filename="c53.log",
                storage_key=f"{cc}/{uuid.uuid4().hex}/c53.log",
                document_type="transaction_log", status="completed")
        s.add(j)
        await s.flush()
        s.add(LogEntry(customer_code=cc, job_id=j.id, timestamp=datetime.now(timezone.utc),
                       source_file="c53.log", line_number=1, level="INFO", raw_body="x",
                       message="x", entry_hash=uuid.uuid4().hex))
        tid = uuid.uuid4()
        s.add(LogTransaction(id=tid, customer_code=cc, job_id=j.id, sealed=False, status=status,
                             started_at=ended_at, ended_at=ended_at, date=ended_at.date(),
                             created_at=ended_at, updated_at=ended_at))
        await s.commit()
    return tid


async def _purge(cc):
    from app.config.database import async_session
    async with async_session() as s:
        for t in ("log_transactions", "log_entries", "jobs"):
            await s.execute(text(f"DELETE FROM {t} WHERE customer_code = :c"), {"c": cc})
        await s.commit()


async def test_seal_due_is_the_entry_point_and_it_works_end_to_end():
    """`seal_due` is what the worker actually calls, and nothing else in this file exercises it: the
    per-tenant session, the advisory lock and the commit all live here rather than in `seal_customer`.

    Found while running this by hand: the sealed row is NOT immediately visible to the cursor, because
    the cursor refuses to read within `notification_cursor_lag_seconds` of the present. That is correct
    and deliberate - the lag is what guarantees every transaction that could write into a range has
    committed - so the assertion below simulates the NEXT tick rather than the current one.
    """
    cc = "test_chunk53_e2e"
    await _purge(cc)
    ended = datetime.now(timezone.utc) - timedelta(
        seconds=settings.log_abandon_window_seconds + 600)
    tid = await _committed(LogTransactionStatus.incomplete, ended, cc)
    try:
        stats = await sealer.seal_due()
        assert stats["sealed"] >= 1 and stats["failed"] == 0, stats

        from app.config.database import async_session
        async with async_session() as s:
            row = (await s.execute(
                select(LogTransaction.sealed, LogTransaction.created_at, LogTransaction.updated_at)
                .where(LogTransaction.id == tid))).one()
            assert row.sealed is True
            assert abs((row.created_at - ended).total_seconds()) < 1.0, "created_at must not move"
            assert row.updated_at > row.created_at

            later = datetime.now(timezone.utc) + timedelta(
                seconds=settings.notification_cursor_lag_seconds + 30)
            window = cur.read_window(
                ended + timedelta(seconds=1), now=later,
                lag=timedelta(seconds=settings.notification_cursor_lag_seconds),
                lookback=timedelta(seconds=settings.notification_lookback_seconds))
            ids = [r.id for r in (await s.execute(
                cur.window_stmt(cc, window, limit=50))).scalars().all()]
            assert tid in ids, "a cursor already past created_at must still see the sealed row"

        assert (await sealer.seal_due())["sealed"] == 0, "a second tick must seal nothing"
    finally:
        await _purge(cc)
