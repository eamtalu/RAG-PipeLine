"""Chunk 45, Phase 3b: N3, the analytics worker. The diff against a real database.

Chunk 44 proved the diff as arithmetic. This proves the cycle: that the ranges read on both sides really
do match, that the ledger really does record every version, that a failure really does leave its tickets
open, and that the retention position really is safe for the tenant furthest behind.

Everything here COMMITS, and none of it uses the `db` fixture's transaction. It cannot: `consume_tenant`
opens its own sessions, so a fixture planted in an uncommitted transaction would be invisible to the
code under test and every assertion would pass against an empty range.

The three properties worth stating up front, because each one is a silent failure if broken:

**Invariant 4.** Tickets are stamped consumed in the SAME transaction as the changes they describe. A
crash between the two would either lose the work (stamped, not applied) or repeat it forever (applied,
not stamped).

**A failed run leaves its tickets open.** Not consumed, not lost -- open, with `attempts` bumped and a
backoff. A range that failed and got marked done is a total that is wrong with nothing to say so.

**The retention position is the MINIMUM across tenants.** `consumer_cursors` holds one row for the whole
consumer and retention is global, so publishing a leader's frontier would let `log_partition_worker`
drop source partitions a lagging tenant never read.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select, text

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.consumer_cursor import ConsumerCursor
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import diff as dd

CC = "n3-probe"
CC2 = "n3-probe-lagging"
T0 = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)


# ==================================================== committed fixtures
async def _wipe(*codes):
    async with async_session() as db:
        for cc in codes:
            for model in (AnalyticsFact, AnalyticsFactLedger, AnalyticsQualityIssue,
                          AnalyticsPendingWindow, AnalyticsTenantState, LogTransaction):
                await db.execute(delete(model).where(model.customer_code == cc))
            await db.execute(delete(Job).where(Job.customer_code == cc))
        await db.execute(delete(ConsumerCursor).where(ConsumerCursor.consumer == n3.CONSUMER))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe(CC, CC2)
    yield
    await _wipe(CC, CC2)


async def _plant(rows, *, cc=CC, ticket=True):
    """Commit transactions and (optionally) a ticket covering them, the way ingestion would."""
    async with async_session() as db:
        job = Job(customer_code=cc, filename="t.log", document_type="transaction_log",
                  storage_key=f"{cc}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        planted = []
        for spec in rows:
            t = LogTransaction(
                customer_code=cc, job_id=job.id, sealed=spec.get("sealed", True),
                started_at=spec["at"], ended_at=spec["at"],
                date=spec["at"].date() if spec["at"] else None,
                duration_ms=spec.get("duration_ms", 100),
                method=spec.get("method", "ConfirmPickLine"),
                transaction_name="Pick", transaction_type=spec.get("type", "002001"),
                status=spec.get("status", LogTransactionStatus.success),
                item_number=spec.get("item", "101978"), user_name="EDA", warehouse="BRI",
                attributes=spec.get("attrs", {"QuantityPicked": spec.get("qty", "10.0")}))
            if "id" in spec:
                t.id = spec["id"]
            db.add(t)
            planted.append(t)
        await db.flush()
        if ticket:
            db.add(AnalyticsPendingWindow(customer_code=cc, range_start=T0 - WIDE,
                                          range_end=T0 + WIDE))
        await db.commit()
        return [t.id for t in planted]


async def _facts(cc=CC) -> list[AnalyticsFact]:
    async with async_session() as db:
        return list((await db.execute(select(AnalyticsFact).where(
            AnalyticsFact.customer_code == cc).order_by(AnalyticsFact.event_time))).scalars().all())


async def _ledger(cc=CC) -> list[AnalyticsFactLedger]:
    async with async_session() as db:
        return list((await db.execute(select(AnalyticsFactLedger).where(
            AnalyticsFactLedger.customer_code == cc)
            .order_by(AnalyticsFactLedger.recorded_at, AnalyticsFactLedger.revision))).scalars().all())


async def _tickets(cc=CC) -> list[AnalyticsPendingWindow]:
    async with async_session() as db:
        return list((await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == cc))).scalars().all())


async def _state(cc=CC) -> AnalyticsTenantState | None:
    async with async_session() as db:
        return (await db.execute(select(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == cc))).scalar_one_or_none()


async def _qty_total(cc=CC) -> Decimal:
    async with async_session() as db:
        return await db.scalar(select(func.coalesce(func.sum(AnalyticsFact.quantity), 0)).where(
            AnalyticsFact.customer_code == cc))


async def _retire(txn_ids):
    """Delete source transactions, as a rebuild or a date-range delete would."""
    async with async_session() as db:
        await db.execute(delete(LogTransaction).where(LogTransaction.id.in_(list(txn_ids))))
        await db.commit()


async def _reticket(cc=CC):
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=cc, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()


# ==================================================== the happy path, end to end
async def test_a_ticket_becomes_facts_and_is_stamped_consumed():
    await _plant([{"at": T0, "qty": "10.0"}, {"at": T0 + timedelta(minutes=1), "qty": "5.0"}])
    stats = await n3.consume_tenant(CC)

    assert stats["runs"] == 1 and stats["inserted"] == 2 and stats["consumed"] == 1
    assert await _qty_total() == Decimal("15")
    assert all(t.consumed_at is not None for t in await _tickets()), "invariant 4"


async def test_nothing_pending_is_a_no_op():
    """Idempotence at the top: the worker ticks constantly, and most ticks have nothing to do."""
    assert (await n3.consume_tenant(CC))["runs"] == 0
    assert await _facts() == []


async def test_a_second_pass_over_the_same_range_writes_nothing():
    """Invariant 6 against a real database. 98.7% of transactions are rewritten, so the no-op path is
    the common path -- if it is not free the worker produces a constant stream of pointless writes."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    before = len(await _ledger())

    await _reticket()
    stats = await n3.consume_tenant(CC)
    assert stats["unchanged"] == 1 and stats["inserted"] == 0 and stats["updated"] == 0
    assert len(await _ledger()) == before, "an unchanged pass must not append a ledger version"


# ==================================================== the two cases that justify the range diff
async def test_a_merge_reverses_the_vanished_id():
    """Two transactions become one. A per-id upsert never asks about the departed id, so its 2 units
    would stay in the total alongside the merged 5 -- exactly double, silently."""
    ids = await _plant([{"at": T0, "qty": "2.0"}, {"at": T0 + timedelta(minutes=1), "qty": "3.0"}])
    await n3.consume_tenant(CC)
    assert await _qty_total() == Decimal("5")

    await _retire(ids)                                    # the rebuild frees both...
    await _plant([{"at": T0, "qty": "5.0"}])              # ...and writes one merged row
    stats = await n3.consume_tenant(CC)

    assert stats["reversed"] == 2 and stats["inserted"] == 1
    assert await _qty_total() == Decimal("5"), "not 10"
    assert len(await _facts()) == 1


async def test_deleting_every_transaction_in_a_range_reverses_every_fact():
    """Leak F12's shape: `DELETE /logs/data` removes transactions with no rebuild behind it. Without a
    reversal their contribution stays in every total permanently, and after 60 days there is nothing
    left to recount against."""
    ids = await _plant([{"at": T0, "qty": "10.0"}, {"at": T0 + timedelta(minutes=1), "qty": "5.0"}])
    await n3.consume_tenant(CC)

    await _retire(ids)
    await _reticket()
    stats = await n3.consume_tenant(CC)

    assert stats["reversed"] == 2
    assert await _facts() == []
    assert await _qty_total() == Decimal("0")


async def test_a_changed_quantity_updates_in_place_and_bumps_the_revision():
    txn_id = uuid.uuid4()
    await _plant([{"at": T0, "qty": "10.0", "id": txn_id}])
    await n3.consume_tenant(CC)
    assert (await _facts())[0].revision == 1

    await _retire([txn_id])
    await _plant([{"at": T0, "qty": "4.0", "id": txn_id}])
    stats = await n3.consume_tenant(CC)

    assert stats["updated"] == 1 and stats["inserted"] == 0, "same identity: an update, not a new row"
    facts = await _facts()
    assert len(facts) == 1 and facts[0].quantity == Decimal("4") and facts[0].revision == 2


async def test_an_update_does_not_touch_created_at():
    """`created_at` is what F6's frontier reads. Refreshing it on every rebuild would make the frontier
    track when analytics last ran rather than how far it has read."""
    txn_id = uuid.uuid4()
    await _plant([{"at": T0, "qty": "10.0", "id": txn_id}])
    await n3.consume_tenant(CC)
    first = (await _facts())[0].created_at

    await _retire([txn_id])
    await _plant([{"at": T0, "qty": "4.0", "id": txn_id}])
    await n3.consume_tenant(CC)
    assert (await _facts())[0].created_at == first


# ==================================================== the ledger
async def test_every_version_lands_in_the_ledger_including_the_reversal():
    """The ledger is the only thing that makes a training run reproducible once the raw entries are
    gone. A history that simply stops where a fact was removed cannot answer "what did the fact table
    hold at time T" for exactly the rows a merge or a delete took out."""
    txn_id = uuid.uuid4()
    await _plant([{"at": T0, "qty": "10.0", "id": txn_id}])
    await n3.consume_tenant(CC)

    await _retire([txn_id])
    await _plant([{"at": T0, "qty": "4.0", "id": txn_id}])
    await n3.consume_tenant(CC)

    await _retire([txn_id])
    await _reticket()
    await n3.consume_tenant(CC)

    versions = await _ledger()
    assert [v.reason for v in versions] == ["insert", "update", "reverse"]
    assert [v.revision for v in versions] == [1, 2, 3], "consecutive, so no version is ambiguous"
    assert [v.quantity for v in versions] == [Decimal("10"), Decimal("4"), Decimal("4")], \
        "the reversal records the value being retired, which is what cancels"


# ==================================================== quarantine never halts (A1)
async def test_one_unusable_row_does_not_stop_the_others():
    """The row that halts a tenant is by definition the one nobody understands yet, so halting freezes
    every metric until a human intervenes."""
    await _plant([
        {"at": T0, "qty": "10.0"},
        {"at": T0 + timedelta(minutes=1), "attrs": {"QuantityPicked": ""}},   # unusable
        {"at": T0 + timedelta(minutes=2), "qty": "5.0"},
    ])
    stats = await n3.consume_tenant(CC)

    assert stats["inserted"] == 2 and stats["quarantined"] == 1
    assert await _qty_total() == Decimal("15"), "the unusable row contributed nothing, not zero"
    async with async_session() as db:
        issues = list((await db.execute(select(AnalyticsQualityIssue).where(
            AnalyticsQualityIssue.customer_code == CC))).scalars().all())
    assert [i.reason for i in issues] == ["unusable_quantity"]
    assert issues[0].observed, "what was seen must survive the raw entry's 60-day retention"


async def test_a_row_that_becomes_unusable_has_its_earlier_fact_reversed():
    """Not a special case in the code, and that is the point: the quarantined row is simply absent from
    the source, so the same branch that handles a merge handles this."""
    txn_id = uuid.uuid4()
    await _plant([{"at": T0, "qty": "10.0", "id": txn_id}])
    await n3.consume_tenant(CC)
    assert await _qty_total() == Decimal("10")

    await _retire([txn_id])
    await _plant([{"at": T0, "attrs": {"QuantityPicked": ""}, "id": txn_id}])
    stats = await n3.consume_tenant(CC)

    assert stats["reversed"] == 1 and stats["quarantined"] == 1
    assert await _qty_total() == Decimal("0")


# ==================================================== failure policy
async def test_a_failed_run_leaves_its_tickets_open_and_bumps_attempts(monkeypatch):
    """A range that failed and got marked done is a total that is wrong with nothing to say so."""
    await _plant([{"at": T0, "qty": "10.0"}])

    async def boom(*a, **k):
        raise RuntimeError("planted failure")
    monkeypatch.setattr(n3, "_read_stored", boom)

    stats = await n3.consume_tenant(CC)
    assert stats["failed"] == 1 and stats["consumed"] == 0
    tickets = await _tickets()
    assert all(t.consumed_at is None for t in tickets), "must stay claimable"
    assert all(t.attempts == 1 and "planted failure" in (t.last_error or "") for t in tickets)
    assert await _facts() == [], "a failed run must not half-apply"


async def test_a_failed_run_backs_the_ticket_off_rather_than_spinning(monkeypatch):
    await _plant([{"at": T0, "qty": "10.0"}])

    async def boom(*a, **k):
        raise RuntimeError("planted failure")
    monkeypatch.setattr(n3, "_read_stored", boom)
    await n3.consume_tenant(CC)

    async with async_session() as db:
        due = await db.scalar(select(func.count()).select_from(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC, *n3._open_and_due()))
    assert due == 0, "the ticket is open but not yet due; without a backoff the retry is pointless"


async def test_a_ticket_is_dead_lettered_at_the_attempt_cap(monkeypatch):
    """A range that has failed five times is failing for a reason a sixth attempt will not change, and
    an abandoned ticket is visible on the status card rather than retried forever."""
    await _plant([{"at": T0, "qty": "10.0"}])
    monkeypatch.setattr("app.settings.settings.analytics_max_attempts", 1)

    async def boom(*a, **k):
        raise RuntimeError("planted failure")
    monkeypatch.setattr(n3, "_read_stored", boom)

    stats = await n3.consume_tenant(CC)
    assert stats["abandoned"] == 1
    assert all(t.abandoned_at is not None for t in await _tickets())


# ==================================================== tenant state (F4, F5)
async def test_the_state_row_carries_what_the_card_shows_without_a_second_table():
    await _plant([{"at": T0, "qty": "10.0", "sealed": False},
                  {"at": T0 + timedelta(minutes=1), "qty": "5.0", "sealed": True}])
    await n3.consume_tenant(CC)

    st = await _state()
    assert st is not None
    assert st.facts_total == 2 and st.quarantined_rows == 0
    assert st.open_tickets == 0 and st.abandoned_tickets == 0
    assert st.analytics_watermark == T0 + timedelta(minutes=1)
    assert st.revision >= 1
    assert st.last_cycle_at is not None and st.last_error is None


async def test_settledness_reports_the_unsealed_share_and_the_oldest_one():
    """F4. A window with unsealed contributors is PROVISIONAL, not stale -- different words for the
    user and different actions for an operator, which is why both numbers are stored."""
    await _plant([{"at": T0, "qty": "1.0", "sealed": False},
                  {"at": T0 + timedelta(minutes=1), "qty": "1.0", "sealed": True},
                  {"at": T0 + timedelta(minutes=2), "qty": "1.0", "sealed": True},
                  {"at": T0 + timedelta(minutes=3), "qty": "1.0", "sealed": True}])
    await n3.consume_tenant(CC)

    st = await _state()
    assert Decimal(st.unsealed_share) == Decimal("0.25000")
    assert st.oldest_unsealed_at == T0


async def test_facts_total_moves_incrementally_and_stays_exact():
    """Counted from the diff rather than recounted. N3 is the only writer, so the increment is exact --
    and a COUNT(*) per cycle over a table designed to reach 13M rows would get slower forever while
    answering a question the cycle already knows."""
    ids = await _plant([{"at": T0, "qty": "1.0"}, {"at": T0 + timedelta(minutes=1), "qty": "1.0"}])
    await n3.consume_tenant(CC)
    assert (await _state()).facts_total == 2

    await _retire(ids[:1])
    await _reticket()
    await n3.consume_tenant(CC)

    st = await _state()
    assert st.facts_total == 1
    assert st.facts_total == len(await _facts()), "the incremental count must match reality"


async def test_the_watermark_never_moves_backwards():
    """A run over an older range is completely normal -- a late backfill is exactly that -- and letting
    it drag the watermark back would report a regression that never happened."""
    await _plant([{"at": T0, "qty": "1.0"}])
    await n3.consume_tenant(CC)
    high = (await _state()).analytics_watermark

    await _plant([{"at": T0 - timedelta(hours=2), "qty": "1.0"}])
    await n3.consume_tenant(CC)
    assert (await _state()).analytics_watermark == high


# ==================================================== the retention position (F6)
async def test_the_position_is_the_minimum_across_tenants_not_the_maximum():
    """The whole reason the frontier is stored per tenant. Publishing a leader's frontier would let the
    partition worker drop source data a lagging tenant had never read, and that tenant's cursor would
    then move past the gap without noticing."""
    await _plant([{"at": T0, "qty": "1.0"}], cc=CC)
    await _plant([{"at": T0, "qty": "1.0"}], cc=CC2)
    await n3.consume_tenant(CC)
    await n3.consume_tenant(CC2)

    a, b = (await _state(CC)).source_write_frontier, (await _state(CC2)).source_write_frontier
    async with async_session() as db:
        published = await db.scalar(select(ConsumerCursor.position).where(
            ConsumerCursor.consumer == n3.CONSUMER))
    assert published == min(a, b)


async def test_nothing_is_published_while_any_tenant_has_processed_nothing():
    """A tenant with a NULL frontier cannot be spoken for at all. SQL's MIN would skip it and publish a
    claim that is too far ahead, which is the one direction that loses data."""
    async with async_session() as db:
        db.add(AnalyticsTenantState(customer_code=CC2))       # exists, never processed
        await db.commit()
    await _plant([{"at": T0, "qty": "1.0"}], cc=CC)
    await n3.consume_tenant(CC)

    async with async_session() as db:
        assert await db.scalar(select(func.count()).select_from(ConsumerCursor).where(
            ConsumerCursor.consumer == n3.CONSUMER)) == 0


async def test_the_consumer_name_is_the_one_retention_reads():
    """F6 names it, and `log_partition_worker` gates partition drops on the MIN across cursor rows. A
    typo here would silently opt analytics out of retention safety."""
    assert n3.CONSUMER == "analytics:warehouse-v1"


# ==================================================== ranges, timezones, NULLs
async def test_the_business_date_is_the_tenant_local_day():
    """23:30 UTC is the next day in London. An operator asking for "the 11th" means a different set of
    rows than a UTC day would give."""
    late = datetime(2026, 8, 10, 23, 30, tzinfo=timezone.utc)
    async with async_session() as db:
        from app.persistence.models.customer import Customer
        db.add(Customer(customer_code=CC, name="probe", timezone="Europe/London"))
        await db.commit()
    try:
        # Its own ticket: 23:30 sits outside the shared T0 +/- 6h window, and a ticket that does not
        # cover the row means the diff never sees it -- which is the correct behaviour, not a bug.
        await _plant([{"at": late, "qty": "1.0"}], ticket=False)
        async with async_session() as db:
            db.add(AnalyticsPendingWindow(customer_code=CC, range_start=late - timedelta(hours=1),
                                          range_end=late + timedelta(hours=1)))
            await db.commit()
        await n3.consume_tenant(CC)
        assert (await _facts())[0].business_date.isoformat() == "2026-08-11"
    finally:
        async with async_session() as db:
            from app.persistence.models.customer import Customer
            await db.execute(delete(Customer).where(Customer.customer_code == CC))
            await db.commit()


async def test_a_transaction_with_no_start_instant_is_folded_into_the_default_partition():
    """A7. It has no instant so it cannot be placed in a range, and `include_null=True` on BOTH sides is
    what reaches it. Dropping it would be the same class of bug as the range predicate that put 294,747
    rows in a DEFAULT partition nobody was looking at."""
    await _plant([{"at": None, "qty": "7.0"}])
    stats = await n3.consume_tenant(CC)
    assert stats["inserted"] == 1
    facts = await _facts()
    assert facts[0].event_time is None and facts[0].quantity == Decimal("7")
    assert facts[0].business_date is None


async def test_a_transaction_outside_the_ticket_range_is_left_completely_alone():
    """The diff is scoped on both sides, and this is what that buys: a fact outside the range is neither
    read nor reversed, so two adjacent day-chunks cannot undo each other."""
    await _plant([{"at": T0, "qty": "1.0"}])
    await n3.consume_tenant(CC)

    far = T0 + timedelta(days=30)
    await _plant([{"at": far, "qty": "99.0"}], ticket=False)
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0 + WIDE))
        await db.commit()

    stats = await n3.consume_tenant(CC)
    assert stats["inserted"] == 0 and stats["reversed"] == 0
    assert len(await _facts()) == 1, "the far transaction is outside the range, so it is not folded"


async def test_two_tickets_a_pad_apart_are_coalesced_into_one_run():
    """Not only efficiency. A transaction whose rebuild moved it across a ticket boundary would be
    reversed by one ticket and inserted by the next; merging them puts both sides in one diff."""
    async with async_session() as db:
        await db.execute(delete(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE, range_end=T0))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0, range_end=T0 + WIDE))
        await db.commit()
    await _plant([{"at": T0, "qty": "1.0"}], ticket=False)

    stats = await n3.consume_tenant(CC)
    assert stats["runs"] == 1, "two overlapping tickets are one unit of work"
    assert stats["consumed"] == 2, "and both are stamped"


# ==================================================== the shape of the cycle
def test_the_source_read_carries_no_limit():
    """The one place this codebase's "never fetch unbounded" rule must NOT be applied. The diff treats
    "stored, absent from the source" as *reverse it*, so a truncated source read would silently delete
    every fact past the cut. The read is bounded by the RANGE instead -- N1 splits tickets to one day."""
    import inspect
    src = inspect.getsource(n3._read_source)
    assert ".limit(" not in src, "a limit here would delete facts, not merely miss them"


def test_the_advisory_lock_is_not_the_stitchers():
    """Sharing `hashtext(customer_code)` would make analytics folding block Stage 2 stitching for the
    same tenant, coupling a read-only consumer to the write path for nothing."""
    import inspect
    src = inspect.getsource(n3)
    assert "'analytics:'" in src or '"analytics:"' in src
    assert n3._LOCK_NAMESPACE == "analytics:"
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
    stitcher = inspect.getsource(dt.finalize_pending)
    assert "hashtext(customer_code)" in stitcher.replace("func.", ""), \
        "if the stitcher's key changed, re-check that these two namespaces are still disjoint"


def test_the_claim_uses_the_database_clock_not_the_transaction_clock():
    """`now()` is `transaction_timestamp()`, so a session whose transaction began before a ticket was
    written would treat that ticket as permanently not-yet-due."""
    import inspect
    src = inspect.getsource(n3._open_and_due)
    assert "clock_timestamp" in src and "func.now()" not in src


def test_work_mem_is_set_locally_rather_than_for_the_session():
    """SET LOCAL, so it reverts at commit. A session-wide setting would follow the pooled connection
    into whatever ran next on it."""
    import inspect
    src = inspect.getsource(n3._consume_run)
    assert "SET LOCAL work_mem" in src


def test_the_worker_does_not_auto_start_on_a_backlog():
    """Unlike the stitch and parse workers, which do. For them an undrained queue means LOST DATA -- the
    parse worker's checkpoint has already advanced past those bytes. Here log_transactions still holds
    the truth and the ticket stays open, so the failure mode is a stale chart. Auto-starting a consumer
    that writes to nine tables on the strength of a queue depth would take that call from the operator
    for no safety gain."""
    import inspect
    import app.background as bg
    src = inspect.getsource(bg.start_background_tasks)
    marker = "if settings.analytics_worker_enabled:"
    assert marker in src
    after = src.split(marker, 1)[1][:400]
    assert "backlog" not in after.lower()


def test_the_cycle_stamps_tickets_after_it_applies_them():
    """Invariant 4 as an ordering claim: the stamp is the LAST thing in the transaction, so a crash
    anywhere above leaves the range claimable."""
    import inspect
    src = inspect.getsource(n3._consume_run)
    assert src.index("_apply(") < src.index("consumed_at=now")
    assert src.index("consumed_at=now") < src.index("await db.commit()")


# ==================================================== startup logging (found in production)
#
# A log line that LIES about the system's state cost real time: with the worker
# correctly enabled and the auditor correctly disabled, startup printed
# "Analytics worker disabled (analytics_worker_enabled=False)". It was believed over the
# running process, and the wrong thing was investigated for an hour.
#
# Cause: the reconcile-worker `if` was inserted between the analytics worker's `if` and
# its `else`, silently re-binding that `else` to the new condition. Nothing failed --
# both branches are syntactically fine, and no test looked at what startup SAYS.

def _startup_log(analytics: bool, reconcile: bool, caplog) -> list[str]:
    """The messages `start_background_tasks` emits for a given flag pair.

    Read from the source rather than by running it: starting the real loops would open
    database connections and SSH pollers. What is under test is the branch structure, and
    that is what the source shows.
    """
    import ast
    import inspect
    import app.background as bg

    tree = ast.parse(inspect.getsource(bg.start_background_tasks))
    out: list[str] = []
    flags = {"analytics_worker_enabled": analytics,
             "analytics_reconcile_worker_enabled": reconcile}

    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        # only the two single-attribute gates we care about
        if not (isinstance(test, ast.Attribute) and test.attr in flags):
            continue
        branch = node.body if flags[test.attr] else node.orelse
        for stmt in ast.walk(ast.Module(body=branch, type_ignores=[])):
            if isinstance(stmt, ast.Constant) and isinstance(stmt.value, str):
                out.append(stmt.value)
    return out


def test_startup_does_not_claim_the_worker_is_disabled_when_it_is_enabled():
    """The exact production symptom. This is the assertion whose absence let a false
    message ship, and it fails loudly if the two gates' branches ever cross again."""
    messages = " ".join(_startup_log(analytics=True, reconcile=False, caplog=None))
    assert "Analytics worker disabled" not in messages, (
        "startup announced the analytics worker as disabled while it was enabled: "
        + messages)


def test_startup_does_say_so_when_the_worker_really_is_disabled():
    """The other direction, so the fix is not simply deleting the message."""
    messages = " ".join(_startup_log(analytics=False, reconcile=False, caplog=None))
    assert "Analytics worker disabled" in messages


def test_each_gate_describes_only_its_own_worker():
    """The structural rule the bug broke: the reconcile gate must not talk about the
    folder, and the folder's gate must not talk about the auditor. Sharing a branch is
    how one flag came to report on the other."""
    reconcile_off = " ".join(_startup_log(analytics=True, reconcile=False, caplog=None))
    assert "reconcil" in reconcile_off.lower(), \
        "the disabled auditor must name ITSELF, not the folder"

    worker_off = " ".join(_startup_log(analytics=False, reconcile=True, caplog=None))
    assert "Analytics worker disabled" in worker_off
    assert "reconcil" not in worker_off.lower()


# ==================================================== F4's OTHER number (found in production)
#
# The first production fold reported `provisional: true, unsealed_share: 0.35903` -- and
# `lag_seconds: null`. Settledness worked; COPY FRESHNESS did not, because nothing ever
# wrote `source_watermark`. The column existed, the API read it, `freshness()` divided by
# it, and no component populated it.
#
# The consequence is exactly what F4 was written to prevent: with source_watermark NULL,
# `lag_seconds` is always None and `stale` is always False, so a pipeline that had fallen
# hours behind would report itself as Provisional or Settled. The one state that means
# "these numbers are missing recent activity" was unreachable.

async def test_the_fold_records_the_source_watermark_too():
    """F4 needs BOTH numbers. Without this the interface can never say "behind"."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    st = await _state()
    assert st.source_watermark is not None, \
        "source_watermark was never written, so lag_seconds is always null and stale never fires"


async def test_both_watermarks_come_from_the_same_snapshot():
    """The column's own docstring: "as observed at the same moment ... two reads would show a
    lag that is really just the gap between them". Reading the source watermark in a later
    transaction would make the reported lag include the worker's own scheduling delay."""
    import inspect
    src = inspect.getsource(n3._consume_run)
    assert src.index("source_watermark=") < src.index("await db.commit()"), \
        "the source watermark must be observed inside the fold's own transaction"


async def test_lag_is_computable_after_a_fold():
    """The end-to-end property: a real freshness reading, not two nulls."""
    from app.services.analytics import read as n6
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    st = await _state()
    f = n6.freshness(analytics_watermark=st.analytics_watermark,
                     source_watermark=st.source_watermark,
                     unsealed_share=st.unsealed_share,
                     oldest_unsealed_at=st.oldest_unsealed_at)
    assert f["lag_seconds"] is not None, "lag must be a number once a fold has happened"
    assert f["lag_seconds"] >= 0


async def test_the_source_watermark_is_the_projections_newest_row_not_the_folded_one():
    """They differ precisely when analytics is behind, which is the only time the number
    matters. Folding an OLD window while a newer transaction exists must report a lag."""
    from sqlalchemy import select as _select, func as _func
    from app.persistence.models.log_transaction import LogTransaction as _LT

    # A newer transaction that the fold's window does NOT cover.
    await _plant([{"at": T0 + timedelta(days=2), "qty": "1.0"}], ticket=False)
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)

    st = await _state()
    async with async_session() as db:
        newest = await db.scalar(_select(_func.max(_LT.started_at)).where(_LT.customer_code == CC))
    assert st.source_watermark == newest
    assert st.analytics_watermark < st.source_watermark, "so the lag is non-zero"
