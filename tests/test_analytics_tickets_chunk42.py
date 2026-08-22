"""Chunk 42, Phase 2: ticket publication (N1), and invariant 2 as an executable assertion.

Invariant 2: **no transaction is deleted by any path without a committed ticket whose range contains
its `started_at`.** Everything the analytics platform will ever compute rests on it, because the range
diff can only correct a window it is told to look at. A path that removes rows without a ticket leaves
their contribution in every total permanently, and once the raw entries are dropped at 60 days there is
nothing left to recount against.

The plan originally named THREE publish sites, all in `derive_transactions.py`. There are FIVE (F12):
both halves of `DELETE /logs/data` also remove `log_transactions` rows, and one of them does so through
a `jobs` cascade with no statement to hook. So the tests below are written per SITE rather than per
function, and the last one greps for any delete site that is not covered -- because the way the original
list came to be short was by reasoning about which paths rebuild instead of asking which paths delete.

Invariant 3: **the ticket and the change commit in the same transaction.** That is why `publish` never
commits: the caller owns the boundary, so a rolled-back rebuild takes its ticket with it. A ticket that
could outlive a rolled-back change would ask the worker to re-diff a window that never moved; a change
that could outlive its ticket is invariant 2 violated.

There is one path that deletes transactions and must NOT publish (F13): the tenant purge. Correcting a
departing tenant's totals is meaningless, and the worker would try to fold a tenant that no longer
exists. Asserted here too, so the two rules sit side by side.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select, text

from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import pending_windows as n1
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

CC = "ticket-probe"
T0 = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)


# ==================================================== fixtures
async def _cleanup(db):
    ids = (await db.execute(select(LogTransaction.id).where(
        LogTransaction.customer_code == CC))).scalars().all()
    if ids:
        await db.execute(delete(LogEntryAssignment).where(
            LogEntryAssignment.transaction_id.in_(list(ids))))
    await db.execute(delete(LogEntryAssignment).where(LogEntryAssignment.customer_code == CC))
    await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == CC))
    await db.execute(delete(LogEntry).where(LogEntry.customer_code == CC))
    await db.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == CC))
    await db.execute(delete(AnalyticsPendingWindow).where(
        AnalyticsPendingWindow.customer_code == CC))
    await db.execute(delete(Job).where(Job.customer_code == CC))
    await db.flush()


async def _job(db) -> Job:
    j = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
            storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
    db.add(j)
    await db.flush()
    return j


async def _txn(db, job, started_at, *, sealed=False) -> LogTransaction:
    # `date` is set as well as `started_at`, because the two are filtered on by different paths and a
    # fixture carrying only one of them silently matches nothing. Stage 2 derives `date` as the
    # tenant-LOCAL day of `started_at` (`to_display(started).date()`); the date-range delete filters on
    # `date`, while N1's bounds come from `started_at`.
    t = LogTransaction(customer_code=CC, job_id=job.id, started_at=started_at,
                       ended_at=started_at, sealed=sealed,
                       date=started_at.date() if started_at else None,
                       status=LogTransactionStatus.success, method="ConfirmPickLine")
    db.add(t)
    await db.flush()
    return t


async def _tickets(db) -> list[AnalyticsPendingWindow]:
    return list((await db.execute(select(AnalyticsPendingWindow).where(
        AnalyticsPendingWindow.customer_code == CC)
        .order_by(AnalyticsPendingWindow.range_start))).scalars().all())


async def _covered(db, instants) -> bool:
    """Whether some OPEN ticket's range contains every instant. Invariant 2, as a predicate."""
    tickets = await _tickets(db)
    return all(any(t.range_start <= i <= t.range_end for t in tickets) for i in instants)


# ==================================================== the publisher itself
async def test_publish_inserts_a_ticket_without_committing(db):
    """Invariant 3. `publish` must not commit, or a rolled-back rebuild would leave a ticket asking the
    worker to re-diff a window that never moved."""
    await _cleanup(db)
    await n1.publish(db, CC, lo=T0, hi=T0 + timedelta(minutes=5))
    assert len(await _tickets(db)) == 1
    assert db.in_transaction(), "publish must leave the caller's transaction open"
    await _cleanup(db)


async def test_the_published_range_is_padded_beyond_the_bounds_given(db):
    """A rebuild can produce a transaction whose `started_at` falls slightly outside the span of the
    rows that were freed -- it may have absorbed an earlier entry. The pad is the same one Stage 2 uses
    to decide a window is lossless, so a ticket cannot be narrower than the rebuild it describes."""
    await _cleanup(db)
    await n1.publish(db, CC, lo=T0, hi=T0)
    t = (await _tickets(db))[0]
    pad = dt._regroup_pad()
    assert t.range_start == T0 - pad
    assert t.range_end == T0 + pad
    await _cleanup(db)


async def test_publish_is_a_no_op_when_there_is_nothing_to_describe(db):
    """No rows freed means no window changed. Publishing anyway would be a ticket the worker has to
    claim, lock, diff and consume to discover nothing happened."""
    await _cleanup(db)
    await n1.publish_for_transactions(db, CC, started_ats=[])
    assert await _tickets(db) == []
    await _cleanup(db)


async def test_a_freed_transaction_with_no_start_instant_still_gets_a_ticket(db):
    """A transaction all of whose entries lack a parsable timestamp has `started_at = NULL`, so it
    cannot be placed in a range at all. It still has to be diffed, so a degenerate ticket is published:
    N3 reads its range with `include_null=True` (A7), which is what reaches the NULL bucket."""
    await _cleanup(db)
    await n1.publish_for_transactions(db, CC, started_ats=[None])
    tickets = await _tickets(db)
    assert len(tickets) == 1, "a NULL-only freed set must not be silently skipped"
    assert tickets[0].range_start <= tickets[0].range_end
    await _cleanup(db)


async def test_mixed_null_and_real_instants_bound_on_the_real_ones(db):
    await _cleanup(db)
    await n1.publish_for_transactions(db, CC, started_ats=[None, T0, T0 + timedelta(hours=1)])
    t = (await _tickets(db))[0]
    pad = dt._regroup_pad()
    assert t.range_start == T0 - pad
    assert t.range_end == T0 + timedelta(hours=1) + pad
    await _cleanup(db)


async def test_a_long_span_is_split_into_one_ticket_per_day(db):
    """`regroup_all` frees a tenant's whole history. One ticket spanning 60 days would have the worker
    read 60 days of transactions in a single transaction; the plan's stated alternative is one ticket
    per day of the span, which keeps each unit of work bounded."""
    await _cleanup(db)
    await n1.publish_for_transactions(db, CC, started_ats=[T0, T0 + timedelta(days=3)])
    tickets = await _tickets(db)
    assert len(tickets) >= 4, f"expected one per day across the span, got {len(tickets)}"
    assert await _covered(db, [T0, T0 + timedelta(days=3)])
    await _cleanup(db)


# ==================================================== invariant 2, per site
async def test_site_1_regroup_window_publishes_for_the_window_it_rebuilds(db):
    await _cleanup(db)
    job = await _job(db)
    t = await _txn(db, job, T0)
    await dt.regroup_window(db, CC, T0 - timedelta(minutes=1), T0 + timedelta(minutes=1),
                            commit=False)
    assert await _covered(db, [T0]), "the rebuilt window must be covered"
    await _cleanup(db)


async def test_site_1_publishes_even_when_the_rebuild_finds_nothing(db):
    """The subtle one. `regroup_window` DELETES the transactions in its window and only then reads the
    entries to rebuild them. If the read comes back empty it returns early -- but the delete already
    happened, so those facts must still be reversed. Publishing after that early return would miss
    exactly the case where data disappeared."""
    await _cleanup(db)
    job = await _job(db)
    await _txn(db, job, T0)          # a transaction with NO entries: freed, nothing to rebuild
    await dt.regroup_window(db, CC, T0 - timedelta(minutes=1), T0 + timedelta(minutes=1),
                            commit=False)
    left = (await db.execute(select(func.count()).select_from(LogTransaction)
                             .where(LogTransaction.customer_code == CC))).scalar()
    assert left == 0, "the premise: the transaction was freed and not rebuilt"
    assert await _covered(db, [T0]), "a freed-but-not-rebuilt window must still be ticketed"
    await _cleanup(db)


async def test_site_2_regroup_incremental_bounds_on_the_freed_set_not_the_new_entries(db):
    """F1. The delete has no time predicate, so bounds taken from incoming entries would miss an older
    unsealed row caught in the same sweep -- and that row's contribution would drift permanently."""
    await _cleanup(db)
    job = await _job(db)
    old = T0 - timedelta(days=20)
    await _txn(db, job, old, sealed=False)      # old and unsealed: freed, far from any new entry
    await _txn(db, job, T0, sealed=False)
    await dt.regroup_incremental(db, CC)
    assert await _covered(db, [old, T0]), "the OLD freed row must be covered, not just the recent one"
    await _cleanup(db)


async def test_site_2_leaves_sealed_transactions_alone_and_does_not_ticket_them(db):
    """It frees only unsealed rows, so a sealed one is untouched and needs no ticket. Publishing for it
    would be a window the worker re-diffs for nothing."""
    await _cleanup(db)
    job = await _job(db)
    await _txn(db, job, T0 - timedelta(days=30), sealed=True)
    await _txn(db, job, T0, sealed=False)
    await dt.regroup_incremental(db, CC)
    tickets = await _tickets(db)
    assert tickets, "the unsealed row was freed, so something must be ticketed"
    assert not any(t.range_start <= T0 - timedelta(days=30) <= t.range_end for t in tickets), \
        "a sealed row was never freed and must not be ticketed"
    await _cleanup(db)


async def test_site_3_regroup_all_publishes_before_it_deletes_everything(db):
    """It deletes the tenant's whole history and commits before rebuilding, so the span has to be read
    BEFORE the delete. Afterwards there is nothing left to derive bounds from."""
    await _cleanup(db)
    job = await _job(db)
    first, last = T0 - timedelta(days=2), T0
    await _txn(db, job, first)
    await _txn(db, job, last)
    await dt.regroup_all(db, CC)
    assert await _covered(db, [first, last])
    await _cleanup(db)


async def test_site_4_the_date_range_delete_publishes_for_what_it_removes(db):
    """F12, first half. An ordinary `delete(LogTransaction)`. The `regroup_incremental` call that
    follows it does NOT cover this: those bounds come from the freed unsealed set, and these rows are
    already gone by then."""
    await _cleanup(db)
    from app.api.v1.logs import delete_log_data
    job = await _job(db)
    await _txn(db, job, T0, sealed=True)     # sealed, so regroup_incremental would never free it
    await db.commit()

    await delete_log_data(customer=CC, confirm=False, db=db,
                          date_from=T0.date(), date_to=T0.date())
    assert await _covered(db, [T0]), "a purged range must be ticketed so its facts reverse"
    await _cleanup(db)


async def test_site_5_the_full_wipe_publishes_before_the_jobs_cascade(db):
    """F12, second half, and the awkward one. The wipe deletes `jobs` and the transactions go with them
    via ON DELETE CASCADE -- there is no `log_transactions` statement to hook, so the ticket must be
    published BEFORE the delete, while the rows are still readable."""
    await _cleanup(db)
    from app.api.v1.logs import delete_log_data
    job = await _job(db)
    await _txn(db, job, T0, sealed=True)
    await db.commit()

    await delete_log_data(customer=CC, confirm=True, db=db, date_from=None, date_to=None)
    left = (await db.execute(select(func.count()).select_from(LogTransaction)
                             .where(LogTransaction.customer_code == CC))).scalar()
    assert left == 0, "the premise: the cascade removed the transactions"
    assert await _covered(db, [T0]), "a wiped tenant's facts must still be reversed"
    await _cleanup(db)


# ==================================================== the deliberate non-site (F13)
async def test_the_tenant_purge_publishes_no_ticket(db):
    """F13. The tenant is going away, so correcting its totals is meaningless and the worker would try
    to fold a tenant that no longer exists. Its analytics rows are DELETED instead, which Phase 1's
    schema tests assert; here we assert the other half, that no ticket is left behind."""
    await _cleanup(db)
    from app.persistence.models.customer import Customer, LogSpaceKind
    from app.services.logspace_cleanup import purge_logspace
    cc = f"purge-noticket-{uuid.uuid4().hex[:8]}"
    db.add(Customer(customer_code=cc, kind=LogSpaceKind.disposable, notifications_enabled=False))
    j = Job(customer_code=cc, filename="t.log", document_type="transaction_log",
            storage_key=f"{cc}/x/t.log", status="completed")
    db.add(j)
    await db.flush()
    db.add(LogTransaction(customer_code=cc, job_id=j.id, started_at=T0, ended_at=T0,
                          status=LogTransactionStatus.success))
    await db.flush()

    await purge_logspace(db, cc)
    await db.flush()
    left = (await db.execute(select(func.count()).select_from(AnalyticsPendingWindow)
                             .where(AnalyticsPendingWindow.customer_code == cc))).scalar()
    assert left == 0, "a departing tenant must leave no ticket behind"


# ==================================================== coverage, proven by grep not by reasoning
def test_every_statement_that_removes_a_transaction_is_a_known_publish_site():
    """Phase 2's own instruction: prove coverage by grepping for every statement that removes a
    `log_transactions` row and checking each against the N1 table, rather than by reasoning about which
    paths rebuild. Reasoning is exactly how the original list came to be short by two.

    A new delete site added later fails this test until it is classified, which is the point.
    """
    import inspect
    import app.api.v1.logs as logs_api
    from app.services import logspace_cleanup

    #: STATEMENTS that remove a transaction, per module -- not sites. The two differ, and conflating
    #: them is what this test exists to stop being possible:
    #:
    #:   derive_transactions  4 statements, 3 sites. `regroup_window` writes its delete as a ternary
    #:                        (`... .in_(freed) if freed else ... .where(false())`), so one logical site
    #:                        is two `delete(LogTransaction)` occurrences. Both are inside the single
    #:                        publish at site 1.
    #:   logs                 2 statements, 2 sites: the date-range delete, and the `delete(Job)` whose
    #:                        cascade removes transactions with no statement of its own to hook.
    #:   logspace_cleanup     1 statement, 0 sites. The deliberate non-site: the tenant is leaving, so
    #:                        F13 deletes its analytics rows outright instead of publishing.
    KNOWN = {
        "derive_transactions": 4,
        "logs": 2,
        "logspace_cleanup": 1,
    }
    for module, expected in (
            (dt, KNOWN["derive_transactions"]),
            (logs_api, KNOWN["logs"]),
            (logspace_cleanup, KNOWN["logspace_cleanup"])):
        src = inspect.getsource(module)
        direct = len([1 for m in __import__("re").finditer(r"delete\(LogTransaction\)", src)])
        cascade = len([1 for m in __import__("re").finditer(r"delete\(Job\)", src)])
        assert direct + cascade == expected, (
            f"{module.__name__} has {direct + cascade} transaction-removing statement(s), expected "
            f"{expected}. A new one must be classified: either it publishes a ticket (F12) or it "
            f"deletes the analytics rows outright (F13).")
