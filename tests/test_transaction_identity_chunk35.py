"""Chunk 35: make a transaction's identity survive the rebuild that Stage 2 performs on it.

The defect, in one sentence: **a transaction's identity is derived from its content, and its content
can change.**

`_anchor` (derive_transactions.py:433) picks the REQUEST entry if the group has one, else `entries[0]`.
`regroup_window` deletes every transaction anchored in `[lo-pad, hi]` - sealed included - and rebuilds
from whatever entries are present at that moment. So if a backfilled file adds an EARLIER entry to a
group that has no REQUEST line, `entries[0]` becomes a different row, the anchor changes, and the
transaction comes back with a different id.

Measured on production over one full day (16,153 transactions): 15,937 (98.7%) carry a REQUEST entry
and are safe; **216 (1.3%) do not** and can change id on any rebuild. Those 216 hold 1,962 entries
including 161 responses, so they are real activity, not noise.

Everything that remembered the old id is then wrong - notification dedupe re-alerts, alert deep links
404, agent citations and saved frontend links rot.

The fix is to stop recomputing identity and start CARRYING it: `log_entry_assignment` already records
which entries belonged to which transaction, and `regroup_window` throws that away moments before it
would answer the question. Read it first, match each rebuilt group to the transaction that owned the
plurality of its entries, and reuse that id. `_txn_id` is untouched and remains the fallback for
genuinely new groups, which is what makes this safe to deploy: no existing id is rewritten.

Two safety properties get their own tests because **the database cannot enforce either of them**. The
unique constraint is `UNIQUE NULLS NOT DISTINCT (id, started_at)`, not unique on `id` alone - the
partition key has to be in it. So two rows sharing an id but differing in `started_at` are accepted
silently:

- only a FREED id may be reused (a freed row is deleted by `id IN (...)` with no `started_at` bound,
  so it is definitely gone; a non-freed row is not);
- two groups may never reuse the same id (the split case).
"""

import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select, text

from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import assignments as A
from app.services.mnp_log_ingestion.pipeline import continuity as C
from app.services.mnp_log_ingestion.pipeline import derive_transactions as d
from app.services.mnp_log_ingestion.pipeline import time_bounds

CC = "TEST_CHUNK35"
T0 = datetime(2026, 8, 8, 10, 0, 0, tzinfo=timezone.utc)


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


async def _add_entries(job_id: uuid.UUID, specs: list[tuple[int, str]], *,
                       cc: str = CC, thread: str = "T1", user: str = "alice",
                       entry_type: LogEntryType = LogEntryType.info) -> list[LogEntry]:
    """Commit one entry per (offset_seconds, tag). `tag` becomes the line body AND the entry_hash, so
    an entry's identity is readable in a failure message instead of being a random uuid."""
    rows = []
    async with async_session() as s:
        for offset, tag in specs:
            rows.append(LogEntry(
                id=uuid.uuid4(), customer_code=cc, job_id=job_id,
                entry_hash=f"hash-{tag}", source_file="t.log", line_number=offset + 1,
                timestamp=T0 + timedelta(seconds=offset), entry_type=entry_type,
                thread=thread, user_ctx=user, raw_body=f"line {tag}"))
        s.add_all(rows)
        await s.commit()
    return rows


async def _regroup() -> None:
    async with async_session() as s:
        await d.regroup_window(s, CC, T0 - timedelta(minutes=5), T0 + timedelta(minutes=5))


async def _transaction_ids() -> set[uuid.UUID]:
    async with async_session() as s:
        return set((await s.execute(
            select(LogTransaction.id).where(LogTransaction.customer_code == CC))).scalars().all())


# =============================================================== the bug, end to end
async def test_a_rebuild_that_gains_an_earlier_entry_keeps_the_id(clean):
    """THE BUG. A REQUEST-less group is anchored on `entries[0]`; backfilling an earlier line into it
    makes a different row `entries[0]`, so the anchor - and therefore the id - changes.

    This is the 1.3% of production transactions, and it is how a notification double-alerts and a deep
    link 404s after Stage 2 has merely re-stitched the same activity."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c"), (12, "d")])
    await _regroup()
    before = await _transaction_ids()
    assert len(before) == 1, "the three entries must stitch into ONE transaction"

    await _add_entries(job, [(9, "a")])       # the backfill: strictly EARLIER than the current anchor
    await _regroup()
    after = await _transaction_ids()

    assert len(after) == 1, "still one transaction - the backfill joined it, it did not split"
    assert after == before, "the transaction is the same transaction, so it must keep its id"


async def test_a_rebuild_that_gains_the_missing_request_line_keeps_the_id(clean):
    """The other half of the same defect. `_anchor` PREFERS a REQUEST entry, so a group that acquires
    one re-anchors from `entries[0]` onto the request - a change of id even though no entry left."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c")])
    await _regroup()
    before = await _transaction_ids()

    await _add_entries(job, [(9, "req")], entry_type=LogEntryType.request)
    await _regroup()

    assert await _transaction_ids() == before


async def test_a_rebuild_with_identical_entries_keeps_the_id(clean):
    """No regression on the 98.7% that were already stable. A rebuild that changes nothing must be a
    no-op for identity, which is also what makes `regroup_all` idempotent."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c")])
    await _regroup()
    before = await _transaction_ids()
    await _regroup()
    assert await _transaction_ids() == before


async def test_a_genuinely_new_group_still_mints_a_deterministic_id(clean):
    """Continuity applies only where there IS a predecessor. A group of entries nothing has ever owned
    must still get `_txn_id`, which is what guarantees deploying this rewrites no existing id."""
    job = await _mk_job()
    entries = await _add_entries(job, [(10, "b"), (11, "c")])
    await _regroup()
    (got,) = await _transaction_ids()

    entries.sort(key=d._entry_stream_order)
    assert got == d._txn_id(entries), "a first-time group is identified exactly as it is today"


async def test_no_log_entry_is_deleted_or_modified_by_a_rebuild(clean):
    """The floor under every other claim here: `log_entries` is append-only and the fix does not go
    near it, so a mis-grouping is always recoverable by regrouping again."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c"), (12, "d")])
    await _regroup()

    async def _snapshot() -> set:
        async with async_session() as s:
            return set((await s.execute(
                select(LogEntry.id, LogEntry.entry_hash, LogEntry.timestamp, LogEntry.raw_body)
                .where(LogEntry.customer_code == CC))).all())

    before = await _snapshot()
    await _add_entries(job, [(9, "a")])
    await _regroup()
    after = await _snapshot()

    assert before < after, "the backfill appended; it changed nothing that was already there"
    assert len(after) == len(before) + 1


async def test_no_transaction_id_is_ever_duplicated(clean):
    """The check the schema cannot make for us: `UNIQUE (id, started_at)` permits two rows with the
    same id in different partitions, so nothing but this assertion would catch a bad reuse."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c")])
    await _regroup()
    await _add_entries(job, [(9, "a"), (12, "d")])
    await _regroup()

    async with async_session() as s:
        dupes = (await s.execute(
            select(LogTransaction.id).where(LogTransaction.customer_code == CC)
            .group_by(LogTransaction.id).having(func.count() > 1))).scalars().all()
    assert dupes == []


# =============================================================== the bulk owner loader
async def test_the_owner_map_is_read_in_ONE_query_for_all_entries(clean):
    """Not one lookup per entry, and not one per transaction. An N+1 here would be the entire cost of
    the change; as a single bulk read it is 0.5 ms."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c"), (12, "d")])
    await _regroup()

    window = time_bounds.from_instants([T0, T0 + timedelta(minutes=1)], pad=timedelta(minutes=5))
    async with async_session() as s:
        owners = await A.load_owners_in_window(s, CC, window)

    assert len(owners) == 3, "every assigned entry in the window is present"
    assert len(set(owners.values())) == 1, "and all three point at the one transaction that owns them"


async def test_the_owner_map_finds_entries_whose_timestamp_did_not_parse(clean):
    """An entry with a NULL timestamp lives in the DEFAULT partition, and a range predicate is FALSE
    for NULL. Without the `include_null` branch those entries would silently lose their continuity -
    the same trap `_existing_ids_stmt` already documents."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b")])
    async with async_session() as s:
        s.add(LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=job, entry_hash="hash-null",
                       source_file="t.log", line_number=99, timestamp=None,
                       entry_type=LogEntryType.info, thread="T1", user_ctx="alice",
                       raw_body="no timestamp"))
        await s.commit()
    await _regroup()

    window = time_bounds.from_instants([T0, T0 + timedelta(minutes=1)], pad=timedelta(minutes=5))
    async with async_session() as s:
        owners = await A.load_owners_in_window(s, CC, window)
        rows = (await s.execute(select(LogEntryAssignment.entry_id)
                                .where(LogEntryAssignment.customer_code == CC))).scalars().all()

    assert set(owners) == set(rows), "the NULL-timestamp entry is not lost from the map"


async def test_the_owner_map_query_prunes_partitions(db):
    """Measured on production: the shape without a partition-key predicate plans in 109 ms and
    executes in 5 ms - planning DOMINATES, because the planner opens all ~20 partitions and plans an
    index scan into each. Bounding `entry_ts` prunes to two plus the default, and planning drops to
    23 ms with a 0.5 ms execution.

    Asserted with EXPLAIN rather than trusted, the same way `_existing_ids_stmt` and `entries_stmt`
    are, so the slow shape cannot come back unnoticed."""
    window = time_bounds.from_instants([T0, T0 + timedelta(minutes=1)], pad=timedelta(minutes=5))
    stmt = A.owners_in_window_stmt(CC, window)
    plan = "\n".join(r[0] for r in (await db.execute(
        text("EXPLAIN " + str(stmt.compile(db.bind, compile_kwargs={"literal_binds": True}))))).all())

    scanned = [ln for ln in plan.splitlines() if "log_entry_assignment_2" in ln]
    assert len(scanned) <= 4, f"expected pruning to a couple of daily partitions, got:\n{plan}"


# =============================================================== continuity: the pure decision
class _E:
    """A stand-in for LogEntry - continuity only ever reads `.id`, and saying so in the test keeps
    these cases free of a database."""

    def __init__(self, id): self.id = id


def _entries(*ids) -> list[_E]:
    return [_E(i) for i in ids]


def _continuity(owner_by_entry: dict, reusable) -> C.Continuity:
    return C.Continuity(owner_by_entry=owner_by_entry, reusable=frozenset(reusable))


A_ID, B_ID = uuid.UUID(int=1), uuid.UUID(int=2)
E1, E2, E3, E4 = (uuid.UUID(int=i) for i in (11, 12, 13, 14))


def _fallback(entries) -> uuid.UUID:
    """A stand-in for `_txn_id`: deterministic in the group's membership, like the real one."""
    return uuid.uuid5(uuid.NAMESPACE_OID, ",".join(sorted(str(e.id) for e in entries)))


def test_a_group_reuses_the_id_of_the_transaction_that_owned_it():
    got = C.assign([_entries(E1, E2)], _continuity({E1: A_ID, E2: A_ID}, {A_ID}), fallback=_fallback)
    assert got == [A_ID]


def test_a_group_with_no_predecessor_falls_back():
    entries = _entries(E1, E2)
    got = C.assign([entries], _continuity({}, set()), fallback=_fallback)
    assert got == [_fallback(entries)]


def test_a_merge_keeps_the_larger_contributor_s_id():
    """Two transactions became one. Keeping the majority's id preserves continuity for most of the
    entries and for whoever was already watching the bigger one."""
    got = C.assign([_entries(E1, E2, E3)],
                   _continuity({E1: A_ID, E2: A_ID, E3: B_ID}, {A_ID, B_ID}), fallback=_fallback)
    assert got == [A_ID]


def test_a_split_gives_the_id_to_the_larger_half_and_the_other_mints():
    """The DATABASE WILL NOT CATCH THIS. Both halves reusing A_ID would write `(A_ID, started_x)` and
    `(A_ID, started_y)`; different `started_at`, so `UNIQUE (id, started_at)` accepts both and two
    rows share an id silently. The tiebreak is correctness, not tidiness."""
    big, small = _entries(E1, E2, E3), _entries(E4)
    got = C.assign([big, small],
                   _continuity({E1: A_ID, E2: A_ID, E3: A_ID, E4: A_ID}, {A_ID}), fallback=_fallback)

    assert got == [A_ID, _fallback(small)]
    assert len(set(got)) == 2, "the same id must never be handed out twice"


def test_the_smaller_half_still_wins_when_it_is_listed_first():
    """Order of the rebuilt groups is an accident of the grouping state machine, so the award must
    depend on the match COUNT, not on who was seen first."""
    small, big = _entries(E4), _entries(E1, E2, E3)
    got = C.assign([small, big],
                   _continuity({E1: A_ID, E2: A_ID, E3: A_ID, E4: A_ID}, {A_ID}), fallback=_fallback)
    assert got == [_fallback(small), A_ID]


def test_an_id_outside_the_freed_set_is_NEVER_reused():
    """THE safety guard. A non-freed transaction still has its row; reusing its id inserts a SECOND
    row with that id into a different partition, and `UNIQUE (id, started_at)` permits it. Silent
    duplicate identity, no error, no way to tell afterwards which one is real."""
    entries = _entries(E1, E2)
    got = C.assign([entries], _continuity({E1: A_ID, E2: A_ID}, reusable=set()), fallback=_fallback)
    assert got == [_fallback(entries)], "an unfreed predecessor is not a predecessor"


def test_a_partly_reusable_group_counts_only_the_reusable_owners():
    """The guard is applied per entry, not per group: entries pointing at a live transaction must not
    contribute votes, or a group could be awarded an id that was never freed."""
    got = C.assign([_entries(E1, E2, E3)],
                   _continuity({E1: A_ID, E2: A_ID, E3: B_ID}, {B_ID}), fallback=_fallback)
    assert got == [B_ID], "A_ID had the plurality but was not freed, so it does not count"


def test_a_tie_between_two_predecessors_does_not_depend_on_entry_order():
    """A merge can be an exact 50/50 split between two former transactions, and then something has to
    break the tie. Leaving it to whichever owner is SEEN first ties identity to entry order - and entry
    order is not fixed: entries sharing a timestamp fall back to line number, which changes when the
    same activity is re-read from a differently-ordered file. The winner would flip on a rebuild that
    changed nothing real.

    Ranking on the id itself removes the dependency, so the same membership always elects the same
    predecessor."""
    cont = _continuity({E1: A_ID, E2: A_ID, E3: B_ID, E4: B_ID}, {A_ID, B_ID})

    forwards = C.assign([_entries(E1, E2, E3, E4)], cont, fallback=_fallback)
    backwards = C.assign([_entries(E4, E3, E2, E1)], cont, fallback=_fallback)

    assert forwards == backwards, "a tie must be settled by the ids, not by who arrived first"


def test_the_award_is_deterministic_when_two_groups_tie():
    """A rebuild must be reproducible - the same entries must always produce the same ids, or
    `regroup_all`'s idempotency claim is false. Ties are broken on the fallback id, which is itself
    derived from membership."""
    groups = [_entries(E1), _entries(E2)]
    cont = _continuity({E1: A_ID, E2: A_ID}, {A_ID})
    assert C.assign(groups, cont, fallback=_fallback) == C.assign(groups, cont, fallback=_fallback)


def test_an_empty_rebuild_is_not_a_special_case():
    assert C.assign([], _continuity({}, set()), fallback=_fallback) == []


# =============================================================== the ordering that makes it work
async def test_the_owner_map_must_be_read_before_the_delete(clean):
    """`regroup_window` deletes exactly the rows the map is read from. Reading it afterwards returns
    `{}`, which does not fail - it silently degrades to today's behaviour, and the bug comes back
    without a single test going red. So the ordering itself is asserted."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c")])
    await _regroup()

    window = time_bounds.from_instants([T0, T0 + timedelta(minutes=1)], pad=timedelta(minutes=5))
    async with async_session() as s:
        before = await A.load_owners_in_window(s, CC, window)
        freed = list((await s.execute(select(LogTransaction.id)
                                      .where(LogTransaction.customer_code == CC))).scalars().all())
        await A.delete_for_transactions(s, freed)
        after = await A.load_owners_in_window(s, CC, window)
        await s.rollback()

    assert before, "before the delete the map knows who owned what"
    assert after == {}, "after it, the evidence is gone - hence the required ordering"


async def test_continuity_survives_a_rebuild_that_both_gains_and_loses_entries(clean):
    """The realistic backfill: a file lands that adds earlier lines AND pushes later ones past the
    open gap into a transaction of their own. The majority stays put, so the id does."""
    job = await _mk_job()
    await _add_entries(job, [(10, "b"), (11, "c"), (12, "d")])
    await _regroup()
    before = await _transaction_ids()

    await _add_entries(job, [(9, "a")])
    await _add_entries(job, [(400, "far")])          # > log_open_gap_seconds -> its own transaction
    await _regroup()
    after = await _transaction_ids()

    assert before <= after, "the original transaction kept its id; the far entry added a new one"
    assert len(after) == 2
