"""Chunk 20: make log_entries append-only by moving the assignment into its own table.

The problem, measured on production 2026-08-05:

    log_entries:  n_tup_upd = 105,838,123   n_tup_hot_upd = 162   -> 0.0% HOT
                  dead tuples 345,382 (15.3%)

~55 rewrites per row, essentially none of them HOT. Stage 2 writes transaction_id/seq back onto the
raw table (derive_transactions.py:522-524) and the delete side clears them through an ON DELETE SET
NULL cascade. transaction_id is indexed, so every one of those 105M updates rewrites index entries
too. That is the write amplification behind the outage.

Moving the assignment into log_entry_assignment makes log_entries insert-only: the churn moves to a
small table designed to be replaced.

Why this is provable rather than hopeful: transaction ids are uuid5(customer_code + anchor
entry_hash), so regrouping the same entries reproduces the same id. The correctness bar is therefore
exact - log_transactions must come out IDENTICAL - not "looks right".

Covered here:
- the assignment repository in isolation (write, replace, delete-by-transaction, anti-join, bulk load);
- dual-write parity: the new table agrees with the legacy columns on every entry;
- the three detection sites, and that the anti-join stays window-scoped;
- the explicit same-transaction delete replacing the ON DELETE SET NULL reliance;
- both read paths - the API feed and the agent tools - return the same ordered entries;
- purge cascades, and a source delete preserves evidence;
- the point of the exercise: a regroup performs no UPDATE on log_entries.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import assignments as A
from app.services.mnp_log_ingestion.pipeline import derive_transactions as d

CC = "TEST_CHUNK20"
T0 = datetime(2026, 8, 5, 10, 0, 0, tzinfo=timezone.utc)


# =============================================================== fixtures
async def _cleanup(cc: str = CC) -> None:
    async with async_session() as s:
        await s.execute(delete(LogEntryAssignment).where(LogEntryAssignment.customer_code == cc))
        await s.execute(delete(LogEntry).where(LogEntry.customer_code == cc))
        await s.execute(delete(LogTransaction).where(LogTransaction.customer_code == cc))
        await s.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == cc))
        await s.execute(delete(Job).where(Job.customer_code == cc))
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
                      start: datetime = T0) -> list[uuid.UUID]:
    """n timestamped entries, one second apart, committed."""
    ids = []
    async with async_session() as s:
        for i in range(n):
            # Set the id EXPLICITLY: LogEntry.id has a Python-side default that only runs at flush,
            # so reading e.id before then yields None.
            eid = uuid.uuid4()
            s.add(LogEntry(id=eid, customer_code=cc, job_id=job_id, entry_hash=uuid.uuid4().hex,
                           source_file="t.log", line_number=i + 1,
                           timestamp=start + timedelta(seconds=i),
                           entry_type=LogEntryType.info, raw_body=f"line {i}"))
            ids.append(eid)
        await s.commit()
    return ids


async def _mk_txn(job_id: uuid.UUID, cc: str = CC, *, started: datetime = T0) -> uuid.UUID:
    async with async_session() as s:
        t = LogTransaction(id=uuid.uuid4(), job_id=job_id, customer_code=cc, started_at=started,
                           date=started.date(), entry_count=0)
        s.add(t)
        await s.commit()
        return t.id


# =============================================================== the repository, in isolation
async def test_write_creates_one_row_per_entry_in_order(clean):
    """seq is the position in the transaction; the repository must preserve the order it is given."""
    job = await _mk_job()
    entries = await _mk_entries(job, 3)
    txn = await _mk_txn(job)

    async with async_session() as s:
        n = await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()
    assert n == 3

    async with async_session() as s:
        rows = (await s.execute(
            select(LogEntryAssignment).where(LogEntryAssignment.transaction_id == txn)
            .order_by(LogEntryAssignment.seq))).scalars().all()
    assert [r.entry_id for r in rows] == entries
    assert [r.seq for r in rows] == [0, 1, 2]
    assert all(r.customer_code == CC for r in rows)


async def test_write_is_idempotent_for_the_same_transaction(clean):
    """A regroup rebuilds the same window with the same deterministic id, so writing again must
    REPLACE rather than raise on the entry_id primary key."""
    job = await _mk_job()
    entries = await _mk_entries(job, 3)
    txn = await _mk_txn(job)

    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=list(reversed(entries)), customer_code=CC)
        await s.commit()

    async with async_session() as s:
        rows = (await s.execute(
            select(LogEntryAssignment).where(LogEntryAssignment.transaction_id == txn)
            .order_by(LogEntryAssignment.seq))).scalars().all()
    assert len(rows) == 3, "must not duplicate on re-write"
    assert [r.entry_id for r in rows] == list(reversed(entries)), "the newest grouping wins"


async def test_write_accepts_an_empty_list(clean):
    """A builder can legitimately produce no entries; that must not be a special case at call sites."""
    job = await _mk_job()
    txn = await _mk_txn(job)
    async with async_session() as s:
        assert await A.write(s, transaction_id=txn, entry_ids=[], customer_code=CC) == 0
        await s.commit()


async def test_delete_for_transactions_removes_exactly_those(clean):
    """This replaces the ON DELETE SET NULL cascade. It must be surgical - other transactions'
    assignments are untouched."""
    job = await _mk_job()
    e1 = await _mk_entries(job, 2)
    e2 = await _mk_entries(job, 2, start=T0 + timedelta(minutes=10))
    t1 = await _mk_txn(job)
    t2 = await _mk_txn(job, started=T0 + timedelta(minutes=10))

    async with async_session() as s:
        await A.write(s, transaction_id=t1, entry_ids=e1, customer_code=CC)
        await A.write(s, transaction_id=t2, entry_ids=e2, customer_code=CC)
        await s.commit()

    async with async_session() as s:
        n = await A.delete_for_transactions(s, [t1])
        await s.commit()
    assert n == 2

    async with async_session() as s:
        left = (await s.execute(select(LogEntryAssignment.transaction_id)
                                .where(LogEntryAssignment.customer_code == CC))).scalars().all()
    assert set(left) == {t2}


async def test_delete_for_transactions_accepts_an_empty_list(clean):
    async with async_session() as s:
        assert await A.delete_for_transactions(s, []) == 0
        await s.commit()


async def test_unassigned_predicate_selects_only_entries_without_an_assignment(clean):
    """'Unassigned' stops meaning transaction_id IS NULL and starts meaning 'no row in the
    assignment table'. This is the signal the whole stitch loop turns on."""
    job = await _mk_job()
    entries = await _mk_entries(job, 4)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries[:2], customer_code=CC)
        await s.commit()

    async with async_session() as s:
        rows = (await s.execute(
            select(LogEntry.id).where(LogEntry.customer_code == CC, A.is_unassigned())
        )).scalars().all()
    assert set(rows) == set(entries[2:])


async def test_load_seq_by_entry_returns_a_lookup_for_readers(clean):
    """Readers need seq per entry to order a transaction timeline. One bulk query, not N."""
    job = await _mk_job()
    entries = await _mk_entries(job, 3)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()

    async with async_session() as s:
        got = await A.load_seq_by_entry(s, [txn])
    assert got == {entries[0]: 0, entries[1]: 1, entries[2]: 2}


async def test_load_seq_by_entry_accepts_an_empty_list(clean):
    async with async_session() as s:
        assert await A.load_seq_by_entry(s, []) == {}


async def test_entry_ids_for_transactions_returns_them_in_seq_order(clean):
    job = await _mk_job()
    entries = await _mk_entries(job, 3)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()
    async with async_session() as s:
        assert await A.entry_ids_for_transactions(s, [txn]) == entries


# =============================================================== cascades
async def test_deleting_a_transaction_removes_its_assignments(clean):
    """Replaces today's ON DELETE SET NULL. The entries themselves must survive - that is the whole
    point: raw evidence is never destroyed by regrouping."""
    job = await _mk_job()
    entries = await _mk_entries(job, 3)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()

    async with async_session() as s:
        await s.execute(delete(LogTransaction).where(LogTransaction.id == txn))
        await s.commit()

    async with async_session() as s:
        assert await s.scalar(select(func.count()).select_from(LogEntryAssignment)
                              .where(LogEntryAssignment.customer_code == CC)) == 0
        assert await s.scalar(select(func.count()).select_from(LogEntry)
                              .where(LogEntry.customer_code == CC)) == 3


async def test_deleting_an_entry_removes_its_assignment(clean):
    """Keeps the existing purge path working: jobs -> entries -> assignments, all by cascade."""
    job = await _mk_job()
    entries = await _mk_entries(job, 2)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()

    async with async_session() as s:
        await s.execute(delete(Job).where(Job.id == job))   # cascades to entries
        await s.commit()

    async with async_session() as s:
        assert await s.scalar(select(func.count()).select_from(LogEntryAssignment)
                              .where(LogEntryAssignment.customer_code == CC)) == 0


# =============================================================== dual-write parity
async def test_stage2_writes_an_assignment_for_every_grouped_entry(clean):
    """Stage 2's only record of the grouping is now the assignment table, so every entry it groups
    must have exactly one row - no silent drops."""
    job = await _mk_job()
    await _mk_entries(job, 6)

    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)

    async with async_session() as s:
        assigned = await s.scalar(select(func.count()).select_from(LogEntryAssignment)
                                  .where(LogEntryAssignment.customer_code == CC))
        claimed = await s.scalar(select(func.sum(LogTransaction.entry_count))
                                 .where(LogTransaction.customer_code == CC))
    assert assigned and assigned > 0, "Stage 2 wrote no assignments at all"
    assert assigned == claimed, "entry_count disagrees with the assignments actually written"


async def test_regroup_is_idempotent_and_transactions_are_identical(clean):
    """The strong correctness bar. Deterministic uuid5 ids mean a rebuild of the same entries must
    reproduce the SAME transactions - identical ids, identical count - so this is exact rather than
    approximate."""
    job = await _mk_job()
    await _mk_entries(job, 6)

    async def _rebuild() -> list[tuple]:
        async with async_session() as s:
            await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)
        async with async_session() as s:
            return [(t.id, t.started_at, t.status, t.entry_count) for t in (await s.execute(
                select(LogTransaction).where(LogTransaction.customer_code == CC)
                .order_by(LogTransaction.started_at, LogTransaction.id))).scalars().all()]

    first = await _rebuild()
    second = await _rebuild()
    assert first == second, "a rebuild changed the transactions"
    assert first, "no transactions were produced at all"


async def test_regroup_replaces_assignments_without_orphaning_them(clean):
    """After a rebuild every assignment must still point at a transaction that exists. An orphan
    would make entries invisible to the feed while looking assigned."""
    job = await _mk_job()
    await _mk_entries(job, 6)
    for _ in range(2):
        async with async_session() as s:
            await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)

    async with async_session() as s:
        orphans = await s.scalar(select(func.count()).select_from(LogEntryAssignment).outerjoin(
            LogTransaction, LogTransaction.id == LogEntryAssignment.transaction_id
        ).where(LogEntryAssignment.customer_code == CC, LogTransaction.id.is_(None)))
    assert orphans == 0


# =============================================================== the point of the exercise
async def _log_entries_updates() -> int:
    """`n_tup_upd` for log_entries — the exact counter the production diagnosis is measured in.

    Sleeps first. Postgres accumulates these stats per backend and flushes them asynchronously
    (PGSTAT_MIN_INTERVAL, ~1s), so a read taken straight after a commit reports a stale number.

    A "read until two samples agree" loop looks smarter but is WORSE: before the flush the counter is
    simply not moving yet, so the first two samples agree and it returns early — which is exactly how
    an earlier version of this test passed while the ON DELETE SET NULL cascade was still rewriting
    every row. A plain wait is slower and honest.
    """
    import asyncio
    from sqlalchemy import text as sa_text
    await asyncio.sleep(1.5)
    async with async_session() as s:
        return await s.scalar(sa_text(
            "SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname = 'log_entries'")) or 0


async def test_a_second_regroup_performs_no_update_on_log_entries(clean):
    """THE test for this whole change.

    Stage 2 used to rewrite the raw table on every regroup — 105,838,123 updates at 0.0% HOT in
    production — from two sources:
      1. explicitly, writing transaction_id/seq back onto each entry, and
      2. implicitly, via the ON DELETE SET NULL cascade firing when the window's transactions were
         deleted.

    Both are now gone - the columns themselves were dropped in e93c47a15b08, so neither source can
    reappear without a migration.
    """
    job = await _mk_job()
    await _mk_entries(job, 6)

    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)
    before = await _log_entries_updates()

    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=6), commit=True)
    after = await _log_entries_updates()

    assert after == before, (
        f"a regroup still UPDATEd log_entries ({after - before} row updates) — the raw table is not "
        "append-only. Check both the explicit write in _persist AND the ON DELETE SET NULL cascade "
        "on log_entries.transaction_id.")


async def test_the_set_null_cascade_is_gone():
    """Guard for the implicit half. While `log_entries_transaction_id_fkey` is ON DELETE SET NULL,
    deleting a window's transactions UPDATEs every entry that pointed at them — so the table cannot
    be append-only however carefully the application code behaves."""
    from sqlalchemy import text as sa_text
    async with async_session() as s:
        # Compare in SQL, not in Python: asyncpg returns `confdeltype` (a "char") as BYTES, so a
        # Python `"n" not in cascades` silently never matches and the assertion passes no matter
        # what. That exact bug let this guard pass while the cascade was still in place.
        offenders = (await s.execute(sa_text(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = 'log_entries'::regclass AND contype = 'f' "
            "AND confdeltype = 'n'"
        ))).scalars().all()
    assert offenders == [], (
        f"ON DELETE SET NULL still rewrites log_entries via {offenders} — every window delete "
        "UPDATEs each entry that pointed at those transactions")


# =============================================================== detection stays window-scoped
def test_window_regroup_uses_a_bounded_anti_join():
    """regroup_incremental:596 is a WHOLE-TABLE `transaction_id IS NULL` scan today. A whole-table
    anti-join would be strictly worse. Every detection site must stay time-bounded."""
    import inspect
    src = inspect.getsource(d.regroup_window)
    assert "is_unassigned" in src, "regroup_window must use the assignment anti-join"
    assert "timestamp >=" in src or "LogEntry.timestamp >=" in src, \
        "the anti-join must remain bounded by the window"


def test_persist_does_not_write_any_column_on_log_entries():
    """_persist must record the grouping ONLY in the assignment table. Assigning to an entry
    attribute here is what made the raw table mutable in the first place."""
    import inspect
    src = inspect.getsource(d._persist)
    assert "e.transaction_id =" not in src
    assert "e.seq =" not in src
    assert "assignments.write" in src


# =============================================================== repository edge coverage
async def test_entry_ids_for_transactions_accepts_an_empty_list(clean):
    async with async_session() as s:
        assert await A.entry_ids_for_transactions(s, []) == []


async def test_belongs_to_transaction_filters_entries(clean):
    """The predicate the agent's entry search uses in place of `LogEntry.transaction_id == ?`."""
    job = await _mk_job()
    entries = await _mk_entries(job, 4)
    t1 = await _mk_txn(job)
    t2 = await _mk_txn(job, started=T0 + timedelta(minutes=5))
    async with async_session() as s:
        await A.write(s, transaction_id=t1, entry_ids=entries[:2], customer_code=CC)
        await A.write(s, transaction_id=t2, entry_ids=entries[2:], customer_code=CC)
        await s.commit()

    async with async_session() as s:
        got = (await s.execute(select(LogEntry.id).where(
            LogEntry.customer_code == CC, A.belongs_to_transaction(t1)))).scalars().all()
    assert set(got) == set(entries[:2])


# =============================================================== read paths
async def test_load_entries_returns_entry_with_its_transaction_and_seq(clean):
    """Readers need three things together: the entry, which transaction owns it, and its position.
    One bulk query returns all three so the API can group and order without touching LogEntry.seq."""
    job = await _mk_job()
    entries = await _mk_entries(job, 3)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()

    async with async_session() as s:
        rows = await A.load_entries(s, [txn], limit=100)
    assert [(e.id, t, q) for e, t, q in rows] == [(entries[0], txn, 0),
                                                  (entries[1], txn, 1),
                                                  (entries[2], txn, 2)]


async def test_load_entries_respects_the_limit(clean):
    """The feed caps rendered entries (MAX_RENDER_ENTRIES); the cap must be applied in SQL, not after
    materialising everything."""
    job = await _mk_job()
    entries = await _mk_entries(job, 5)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries, customer_code=CC)
        await s.commit()
    async with async_session() as s:
        assert len(await A.load_entries(s, [txn], limit=2)) == 2


async def test_load_entries_accepts_an_empty_list(clean):
    async with async_session() as s:
        assert await A.load_entries(s, [], limit=10) == []


async def test_transaction_detail_reads_through_the_assignment(clean):
    """GET /logs/transactions/{id} must return the entries in seq order, sourced from the assignment
    table rather than LogEntry.seq."""
    from app.api.v1.logs import _load_transaction_entries

    job = await _mk_job()
    await _mk_entries(job, 4)
    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=4), commit=True)
    async with async_session() as s:
        txn = (await s.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalars().first()
    assert txn is not None

    async with async_session() as s:
        # returns (entry, seq) pairs — the position travels with the row now
        t, pairs, truncated = await _load_transaction_entries(txn.id, CC, s)
    assert t.id == txn.id
    assert truncated is False
    assert len(pairs) == txn.entry_count
    assert [q for _e, q in pairs] == list(range(len(pairs))), "seq must be 0..n-1 in order"
    async with async_session() as s:
        expected = await A.entry_ids_for_transactions(s, [txn.id])
    assert [e.id for e, _q in pairs] == expected


async def test_agent_tool_reads_through_the_assignment(clean):
    """The agent tools are a SEPARATE read path from the API and are the easy one to miss."""
    from app.services.log_agent import tools

    job = await _mk_job()
    await _mk_entries(job, 4)
    async with async_session() as s:
        await d.regroup_window(s, CC, T0, T0 + timedelta(seconds=4), commit=True)
    async with async_session() as s:
        txn = (await s.execute(select(LogTransaction).where(
            LogTransaction.customer_code == CC))).scalars().first()

    async with async_session() as s:
        got = await tools._get_transaction(s, {"transaction_id": str(txn.id)}, CC)
    seqs = [step["seq"] for step in got["timeline"]]
    assert seqs == sorted(seqs), "agent output must be in seq order"
    assert all(q is not None for q in seqs), "seq must come from the assignment, not a NULL column"


# =============================================================== final state: the columns are gone
async def test_legacy_assignment_columns_no_longer_exist():
    """The end state. While `transaction_id` / `seq` remain on log_entries, something can still write
    them and quietly reintroduce the churn; and their index is dead weight on every insert.

    Dropping them is metadata-only in PostgreSQL — measured at ~0.1s on a 48 MB table, with the
    relfilenode unchanged — so there is no rewrite and no reason to keep them.
    """
    from sqlalchemy import text as sa_text
    async with async_session() as s:
        cols = (await s.execute(sa_text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'log_entries' AND column_name IN ('transaction_id', 'seq')"
        ))).scalars().all()
    assert cols == [], f"legacy assignment columns still on log_entries: {cols}"


async def test_the_transaction_id_index_is_gone():
    """That index was maintained on every one of the 105.8M updates. With the column gone it cannot
    exist, but assert it explicitly so a re-add is caught."""
    from sqlalchemy import text as sa_text
    async with async_session() as s:
        idx = (await s.execute(sa_text(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'log_entries' "
            "AND indexdef LIKE '%transaction_id%'"
        ))).scalars().all()
    assert idx == [], f"an index on the dropped column still exists: {idx}"


def test_no_dual_write_setting_remains():
    """Dual-write was scaffolding for a staged switch-over. Keeping a dead flag invites someone to
    turn it back on and start writing a column that no longer exists."""
    assert not hasattr(settings, "log_entry_assignment_dual_write")


def test_no_module_reads_or_writes_the_legacy_columns():
    """Static guard across every module that touched them, so a reintroduction fails loudly here
    rather than silently at runtime."""
    import inspect
    from app.api.v1 import logs as logs_api
    from app.services.log_agent import tools
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

    for mod in (logs_api, tools, dt):
        src = inspect.getsource(mod)
        for banned in ("LogEntry.transaction_id", "LogEntry.seq", "e.transaction_id", "e.seq"):
            assert banned not in src, f"{mod.__name__} still references {banned}"


async def test_incremental_regroup_uses_the_anti_join(clean):
    """regroup_incremental had TWO `transaction_id IS NULL` sites of its own — separate from
    regroup_window. They must use the assignment anti-join, or the live path silently regroups
    nothing once the column is gone."""
    job = await _mk_job()
    await _mk_entries(job, 6)

    async with async_session() as s:
        stats = await d.regroup_incremental(s, CC)
    assert stats.get("transactions_created", 0) > 0, \
        "regroup_incremental found no unassigned entries — its detection is still column-based"

    async with async_session() as s:
        n = await s.scalar(select(func.count()).select_from(LogEntryAssignment)
                           .where(LogEntryAssignment.customer_code == CC))
    assert n == 6


async def test_load_transaction_by_entry_maps_entries_to_their_owner(clean):
    """The inverse lookup, for list endpoints that show which transaction each entry belongs to.
    Entries with no assignment are absent from the map — the caller reports those as unassigned
    rather than guessing."""
    job = await _mk_job()
    entries = await _mk_entries(job, 4)
    txn = await _mk_txn(job)
    async with async_session() as s:
        await A.write(s, transaction_id=txn, entry_ids=entries[:3], customer_code=CC)
        await s.commit()

    async with async_session() as s:
        got = await A.load_transaction_by_entry(s, entries)
    assert got == {entries[0]: txn, entries[1]: txn, entries[2]: txn}
    assert entries[3] not in got, "an unassigned entry must simply be absent"


async def test_load_transaction_by_entry_accepts_an_empty_list(clean):
    async with async_session() as s:
        assert await A.load_transaction_by_entry(s, []) == {}
