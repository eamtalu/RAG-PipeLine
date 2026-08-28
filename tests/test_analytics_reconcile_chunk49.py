"""Chunk 49, Phase 4: reconciliation. The only component whose job is to catch the design being wrong.

    Phase 4. Windowed routine reconciliation plus explicit full runs, and the completeness check
    against `log_entries`.

Every invariant in this system fails SILENTLY, producing a plausible wrong number rather than an error.
So the value of this component is not that it repairs anything - it is that a wrong number stops being
undetectable.

Three checks, and the reason there are three is that each catches a class the others cannot see.

**facts vs transactions.** Does every transaction in the window have a fact, or a recorded reason it
does not? This is the only check that can see a MISSED TICKET, because it is the only one that reads the
projection rather than the warehouse. A recount from facts would agree with itself and report nothing.

**rollups vs a fresh fold of the facts.** Deliberately reuses N5's own fold. That looks like cheating and
is not: the bug class here is a bucket that was never recomputed - a dirty bucket the diff failed to
identify - and staleness is invisible to a second implementation of the arithmetic while being exactly
what re-folding catches. A hand-written second fold would only test whether two pieces of arithmetic
agree, which is not a failure anyone has.

**entries vs assignments.** F2, and the reason reconciliation cannot be recount-only:

    Recomputing from `log_transactions` cannot detect the roughly 1,000 orphaned entries, because both
    sides read the same incomplete projection and agree.

The cutoff is measured against the tenant's own data watermark, not wall clock, matching `_seal_cutoffs`.
A quiet tenant whose newest log line is a week old has not lost anything, and a wall-clock cutoff would
report all of its entries as orphaned.

**Report-only by default** (Phase 7: "then report-only reconciliation"), and when it does repair, the
repair is *publishing a ticket* - never a direct write. Nothing may bypass the range diff, or the repair
path becomes a second, untested way to change a total.
"""

import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select, text

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import reconcile as rc
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.mnp_log_ingestion.pipeline.parse_insert import _entry_hash
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

CC = "recon-probe"
T0 = datetime(2026, 8, 10, 14, 30, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)
WINDOW = UtcWindow(start=T0 - WIDE, end=T0 + WIDE)

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsMetric, LogEntryAssignment, LogTransaction, LogEntry)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _plant(rows, *, ticket=True):
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        made = []
        for spec in rows:
            method = spec.get("method", "ConfirmPickLine")
            t = LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=spec["at"],
                ended_at=spec["at"], date=spec["at"].date(), duration_ms=100, method=method,
                transaction_name="Pick", transaction_type="002001",
                status=spec.get("status", LogTransactionStatus.success), item_number="101978",
                user_name="EDA", warehouse="BRI",
                attributes=spec.get("attrs", {QF[method]: spec.get("qty", "10.0")}))
            db.add(t)
            made.append(t)
        await db.flush()
        if ticket:
            db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                          range_end=T0 + WIDE))
        await db.commit()
        return [t.id for t in made]


async def _entries(n, *, at, assigned, source="eSmartServerLog.txt", program="MMS100MI"):
    """Plant log entries, optionally WITHOUT an assignment row -- the orphan shape from leak 11."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename=source, document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/{source}", status="completed")
        db.add(job)
        await db.flush()
        for i in range(n):
            body = f"line {i} {program} {source}"
            # entry_hash is NOT NULL and is the dedup key; use the pipeline's own function rather than
            # a placeholder, so a fixture row is indistinguishable from an ingested one.
            e = LogEntry(customer_code=CC, job_id=job.id, timestamp=at + timedelta(seconds=i),
                         line_number=i + 1, raw_body=body, entry_hash=_entry_hash(body),
                         source_file=source, level="INFO")
            db.add(e)
            await db.flush()
            if assigned:
                db.add(LogEntryAssignment(customer_code=CC, entry_id=e.id, entry_ts=e.timestamp,
                                          transaction_id=uuid.uuid4(), seq=i))
        await db.commit()


def _by_check(findings):
    out = {}
    for f in findings:
        out.setdefault(f.check, []).append(f)
    return out


# ==================================================== check 1: facts vs transactions
async def test_a_clean_window_reports_nothing():
    """The baseline. A check that cannot be quiet is a check nobody reads."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert report["findings"] == [], report
    assert report["healthy"] is True


async def test_a_transaction_with_no_fact_is_reported():
    """The missed-ticket detector, and the only check that can see one. A transaction was never folded,
    so no ticket ever covered it -- and a recount from facts would agree with itself and say nothing."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    # A second transaction arrives with NO ticket: exactly what a missed publish site looks like.
    await _plant([{"at": T0 + timedelta(minutes=5), "qty": "7.0"}], ticket=False)

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    found = _by_check(report["findings"])
    assert "facts_vs_transactions" in found
    assert found["facts_vs_transactions"][0].detail["transactions_without_facts"] == 1
    assert report["healthy"] is False


async def test_a_quarantined_transaction_is_not_reported_as_missing():
    """It has no fact ON PURPOSE, and the reason is recorded. Reporting it would make the check cry
    wolf on every unusable row, which is how a check gets switched off."""
    await _plant([{"at": T0, "attrs": {"QuantityPicked": ""}}])
    await n3.consume_tenant(CC)

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert "facts_vs_transactions" not in _by_check(report["findings"])


async def test_the_check_is_windowed():
    """A4: routine reconciliation must be windowed, or a full recount grows with all retained history
    and becomes a job nobody runs. A transaction outside the window must not be reported."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    await _plant([{"at": T0 + timedelta(days=40), "qty": "5.0"}], ticket=False)

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert report["findings"] == [], "a transaction outside the window is not this run's business"


async def test_a_transaction_with_no_start_instant_is_still_checked():
    """A7. It lives in the DEFAULT partition and a range test is false for NULL, so a check that forgot
    `include_null` would never notice it was unfolded."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogTransaction(customer_code=CC, job_id=job.id, sealed=True, started_at=None,
                              ended_at=None, method="ConfirmPickLine", transaction_name="Pick",
                              status=LogTransactionStatus.success,
                              attributes={"QuantityPicked": "3.0"}))
        await db.commit()

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    found = _by_check(report["findings"])
    assert "facts_vs_transactions" in found, "the NULL bucket must be reachable by the check"


# ==================================================== check 2: rollups vs a fresh fold
async def test_a_stale_rollup_bucket_is_reported():
    """The bug this exists for: a dirty bucket the diff failed to identify, so the fact is right and the
    chart is wrong. Simulated by corrupting a stored rollup directly, which is what staleness looks
    like from the outside."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)

    async with async_session() as db:
        await db.execute(text("""
            UPDATE analytics_hourly_rollups SET sum_value = 999
            WHERE customer_code = :c AND measure_name = 'quantity'"""), {"c": CC})
        await db.commit()

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    found = _by_check(report["findings"])
    assert "rollups_vs_facts" in found
    d = found["rollups_vs_facts"][0].detail
    assert Decimal(str(d["stored"])) == Decimal(999)
    assert Decimal(str(d["recomputed"])) == Decimal(10)


async def test_a_rollup_row_that_should_not_exist_is_reported():
    """The other half of staleness: a bucket whose facts are all gone but whose rollup survived. That is
    the exact shape of the bug N5's "recompute to nothing means DELETE" rule prevents, so reconciliation
    has to be able to see it if the rule ever regresses."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(delete(AnalyticsFact).where(AnalyticsFact.customer_code == CC))
        await db.commit()

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert "rollups_vs_facts" in _by_check(report["findings"])


async def test_the_recount_reuses_the_folder_deliberately():
    """Not a shortcut. The bug class is a bucket that was never recomputed, and staleness is invisible
    to a second implementation of the arithmetic while being exactly what re-folding catches. A
    hand-written second fold would only test whether two pieces of arithmetic agree."""
    import inspect
    src = inspect.getsource(rc)
    assert "n5.group_fold" in src or "rollups.group_fold" in src


# ==================================================== check 3: entries vs assignments (F2)
async def test_orphaned_entries_are_reported_grouped_by_source_file():
    """F2. Recomputing from `log_transactions` cannot see these, because both sides read the same
    incomplete projection and agree. Measured on the live server: 847 to 1,079 such entries from two
    named files."""
    # The watermark FIRST: an orphan is old relative to the tenant's newest entry, not to wall clock.
    # Without a recent entry the cutoff sits an hour before the only entries there are, and nothing
    # qualifies -- which is what this fixture originally got wrong.
    await _entries(2, at=T0, assigned=True, source="eSmartServerLog.txt")
    await _entries(3, at=T0 - timedelta(days=2), assigned=False, source="eSmartServerLog.txt.40")

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    found = _by_check(report["findings"])
    assert "entries_vs_assignments" in found
    d = found["entries_vs_assignments"][0].detail
    assert d["orphaned"] == 3
    assert d["source_file"] == "eSmartServerLog.txt.40", "grouped by file, so a cause can be found"


async def test_an_entry_still_inside_the_abandon_window_is_not_an_orphan():
    """It may yet be stitched. Reporting it would make the check permanently noisy, since the newest
    entries are always unassigned for up to an hour."""
    await _entries(2, at=T0 - timedelta(days=2), assigned=True)   # establishes a real watermark below
    await _entries(3, at=T0, assigned=False)                      # ...and these are the newest
    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert "entries_vs_assignments" not in _by_check(report["findings"]), \
        "the newest entries are inside the abandon window and may still be stitched"


async def test_the_orphan_cutoff_follows_the_data_watermark_not_the_clock():
    """The same rule Stage 2 seals by. A quiet tenant whose newest log line is a week old has lost
    nothing, and a wall-clock cutoff would report every one of its entries as orphaned - which is the
    kind of false alarm that gets a check disabled."""
    week_old = datetime.now(timezone.utc) - timedelta(days=7)
    await _entries(3, at=week_old, assigned=False)

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=UtcWindow(start=None, end=None))
    found = _by_check(report["findings"])
    assert "entries_vs_assignments" not in found, \
        "these are the tenant's NEWEST entries, so they are inside the watermark-relative window"


async def test_the_orphan_check_ignores_the_reconciliation_window():
    """Deliberately not windowed, unlike the other two. An orphan is defined against the tenant's
    watermark, not against a range, and an orphan from three weeks ago is exactly the one nobody has
    noticed yet."""
    import inspect
    src = inspect.getsource(rc.orphaned_entries)
    assert "window" not in inspect.signature(rc.orphaned_entries).parameters


# ==================================================== repair is a ticket, never a write
async def test_report_only_is_the_default():
    """Phase 7: "then report-only reconciliation". A check that repairs on its first run cannot be
    trusted to have found what it says it found."""
    import inspect
    assert inspect.signature(rc.reconcile_tenant).parameters["repair"].default is False


async def test_report_only_changes_nothing():
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    await _plant([{"at": T0 + timedelta(minutes=5), "qty": "7.0"}], ticket=False)

    async with async_session() as db:
        before = await db.scalar(select(func.count()).select_from(AnalyticsPendingWindow)
                                 .where(AnalyticsPendingWindow.customer_code == CC))
        await rc.reconcile_tenant(db, CC, window=WINDOW)
        await db.commit()
        after = await db.scalar(select(func.count()).select_from(AnalyticsPendingWindow)
                                .where(AnalyticsPendingWindow.customer_code == CC))
    assert after == before, "report-only must not publish"


async def test_repair_publishes_a_ticket_rather_than_writing():
    """The repair must go through the normal path. A reconciler that corrected a total directly would be
    a second, untested way to change a number, and the range diff would no longer be the only writer."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    await _plant([{"at": T0 + timedelta(minutes=5), "qty": "7.0"}], ticket=False)

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW, repair=True)
        await db.commit()
    assert report["tickets_published"] >= 1

    # And the normal path then fixes it, with no special-casing anywhere.
    stats = await n3.consume_tenant(CC)
    assert stats["inserted"] == 1
    async with async_session() as db:
        assert await rc.reconcile_tenant(db, CC, window=WINDOW) == \
            {**await rc.reconcile_tenant(db, CC, window=WINDOW)}
        report2 = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert report2["findings"] == [], "the repair went through the diff and closed the gap"


async def test_repair_does_not_publish_for_an_orphan_finding():
    """A ticket cannot fix an entry that was never stitched into a transaction: re-diffing the window
    would find the same projection. That needs a Stage 2 regroup, which is an operator action, so the
    finding is reported and left alone rather than answered with a ticket that changes nothing."""
    await _entries(2, at=T0, assigned=True)
    await _entries(3, at=T0 - timedelta(days=2), assigned=False, source="eSmartServerLog.txt.40")
    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW, repair=True)
        await db.commit()
    assert report["tickets_published"] == 0
    assert "entries_vs_assignments" in _by_check(report["findings"])


# ==================================================== the shape of the component
def test_reconciliation_does_not_commit():
    """The caller owns the boundary, so a repair ticket lands with whatever else the caller is doing."""
    import inspect
    code = [l for l in inspect.getsource(rc).splitlines()
            if "commit(" in l and not l.strip().startswith("#")]
    assert code == [], code


def test_every_check_is_named_in_one_place():
    """So the worker can report per check and an operator can see which one is firing, rather than a
    single healthy/unhealthy bit."""
    # 18y added the record grain's two checks - the same one-name-per-check rule.
    assert set(rc.CHECKS) == {"facts_vs_transactions", "rollups_vs_facts", "entries_vs_assignments",
                              "record_rollups_vs_record_facts", "records_vs_facts"}


async def test_a_tenant_with_no_data_at_all_is_healthy_not_broken():
    """A new tenant, or one whose logs have not arrived. Reporting it as unhealthy would make the
    retention gate refuse to drop anything for a tenant that has never had a problem."""
    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert report["healthy"] is True and report["findings"] == []


# ==================================================== repair, per check (found by the Phase 4 E2E)
async def test_repairing_a_stale_rollup_does_not_go_through_a_ticket():
    """Found end to end, not by reasoning. A ticket cannot fix a stale rollup: the FACTS are unchanged,
    so the range diff correctly reports every one of them `unchanged`, no bucket is marked dirty, and N5
    never recomputes. The corruption survives its own repair.

    So the two repairable checks need DIFFERENT repairs. A missing fact is a change, and a ticket is
    exactly right for it. A drifted bucket is not a change to any fact, so the repair is to re-fold that
    bucket -- which is not a second way to change a total, because it calls the same folder N5 uses and
    recompute-and-replace is idempotent by construction."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(text("""
            UPDATE analytics_hourly_rollups SET sum_value = 999
            WHERE customer_code = :c AND measure_name = 'quantity'"""), {"c": CC})
        await db.commit()

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW, repair=True)
        await db.commit()
    assert report["tickets_published"] == 0, "a ticket would achieve nothing here"
    assert report["buckets_recomputed"] >= 1, "the bucket must be re-folded instead"

    async with async_session() as db:
        after = await rc.reconcile_tenant(db, CC, window=WINDOW)
    assert after["findings"] == [], "the drift must actually be gone after the repair"


async def test_the_rollup_repair_uses_the_same_folder_as_the_writer():
    """Recompute-and-replace, so repairing twice is indistinguishable from repairing once, and the
    repair cannot drift from the write path."""
    import inspect
    src = inspect.getsource(rc)
    assert "n5.recompute" in src


async def test_an_orphan_finding_produces_neither_a_ticket_nor_a_refold():
    """Neither can help: the entry was never stitched into a transaction, so re-diffing the range reads
    the same projection and re-folding aggregates the same facts. It needs a Stage 2 regroup, which is an
    operator action, so the finding is reported and left alone."""
    await _entries(2, at=T0, assigned=True)
    await _entries(3, at=T0 - timedelta(days=2), assigned=False, source="eSmartServerLog.txt.40")
    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW, repair=True)
        await db.commit()
    assert report["tickets_published"] == 0 and report["buckets_recomputed"] == 0
    assert "entries_vs_assignments" in _by_check(report["findings"])


async def test_a_missing_fact_is_repaired_by_a_ticket_not_a_refold():
    """The other half. Re-folding would aggregate facts that do not exist yet; the fact has to be
    derived first, and only the diff does that."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    await _plant([{"at": T0 + timedelta(minutes=5), "qty": "7.0"}], ticket=False)

    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=WINDOW, repair=True)
        await db.commit()
    assert report["tickets_published"] >= 1
    assert report["buckets_recomputed"] == 0, "there is no drifted bucket, only a missing fact"
