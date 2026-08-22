"""Chunk 48: the analytics run provisions its own destination partitions.

The gap this closes, measured on 2026-08-22:

    oldest source data that still EXISTS : 2026-06-23   (60-day retention, so nothing is older)
    analytics_facts partitions start at  : 2026-07-01
    -> an 8-day window whose facts have no partition to go to

`log_transactions` always has a partition for its own rows -- they are inside the retention window by
definition. It is the DESTINATION that can be missing, because the partition runway is built forward
only (`coverage_days(today, ahead=14)`) and has never had a reason to reach backwards.

Three paths write facts with an old `event_time`, and skipping the Phase 4 backfill removes only the
first: a bulk backfill, `regroup_all` re-deriving a tenant's whole history, and an ingested rotated log
file (`eSmartServerLog.txt.40`) carrying weeks-old lines.

Without a partition those rows land in `analytics_facts_default`, and that is a ONE-WAY DOOR. Verified
against PostgreSQL directly: once the default partition holds rows for a period, creating that period's
real partition fails with

    updated partition constraint for default partition would be violated by some row

So it cannot be repaired later by adding the partition -- the rows have to be moved out first. Which is
why the fix belongs before the write, not after.

Why not simply skip such rows
-----------------------------
Filtering the SOURCE read to "rows whose destination exists" makes the source set narrower than the
stored set, and the range diff reads exactly that as *reverse it*: any fact already in the default
partition would be DELETED and its contribution stripped from every rollup. The skip would have to be
whole-run to be safe, and then the transactions are silently absent from every chart -- a plausible
wrong number, which is the failure class this design exists to prevent. Creating the partition costs one
idempotent statement and leaves nothing to explain.
"""

import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select, text

from app.config.database import async_session
from app.persistence import partitioning as pt
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3

CC = "runway-probe"

#: Deliberately far outside any provisioned runway, and far outside the 60-day source window too, so
#: this test can never collide with a partition the real runway happens to have created.
OLD = datetime(2019, 3, 14, 10, 30, tzinfo=timezone.utc)

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow,
          AnalyticsTenantState, AnalyticsMetric, LogTransaction)

#: Partitions this test creates, dropped in teardown so it leaves the runway as it found it.
MADE = [("analytics_facts", date(2019, 3, 1)), ("analytics_fact_ledger", date(2019, 3, 1)),
        ("analytics_quality_issues", date(2019, 3, 1)),
        ("analytics_hourly_rollups", date(2019, 3, 13)),
        ("analytics_hourly_rollups", date(2019, 3, 14)),
        ("analytics_hourly_rollups", date(2019, 3, 15)),
        ("analytics_daily_rollups", date(2019, 1, 1))]


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()
    async with async_session() as db:
        for table, day in MADE:
            await db.execute(text(f"DROP TABLE IF EXISTS {pt.partition_name(table, day)}"))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _plant(at=OLD, qty="10.0"):
    """A transaction at `at`, plus a ticket covering it. `log_transactions` is daily-partitioned, so
    its own partition is created here too -- the point of the test is the DESTINATION, not the source."""
    async with async_session() as db:
        await pt.ensure_coverage(db, days=[at.date()], tables=("log_transactions",))
        await db.commit()
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        db.add(LogTransaction(
            customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
            date=at.date(), duration_ms=100, method="ConfirmPickLine", transaction_name="Pick",
            transaction_type="002001", status=LogTransactionStatus.success, item_number="101978",
            user_name="EDA", warehouse="BRI", attributes={"QuantityPicked": qty}))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=at - timedelta(hours=1),
                                      range_end=at + timedelta(hours=1)))
        await db.commit()


async def _where(model) -> list[str]:
    """Which physical partition each of this tenant's rows actually landed in."""
    async with async_session() as db:
        return list((await db.execute(
            select(text("tableoid::regclass::text")).select_from(model)
            .where(model.customer_code == CC))).scalars().all())


# ==================================================== the fix
async def test_a_run_over_an_unprovisioned_range_creates_its_own_partitions():
    """The whole point. Before this, folding a 2019 transaction put its fact in the default partition
    of a table that is never dropped, and permanently prevented the 2019-03 partition being created."""
    assert not await _exists("analytics_facts", date(2019, 3, 1)), "must start missing"
    await _plant()

    stats = await n3.consume_tenant(CC)
    assert stats["inserted"] == 1

    assert await _exists("analytics_facts", date(2019, 3, 1)), "the run must provision its own range"
    assert await _where(AnalyticsFact) == [
        pt.partition_name("analytics_facts", OLD.date())], "not the default partition"


async def _exists(table, day) -> bool:
    async with async_session() as db:
        return await pt.partition_exists(db, table, day)


async def test_no_fact_lands_in_the_default_partition():
    await _plant()
    await n3.consume_tenant(CC)
    async with async_session() as db:
        assert await db.scalar(text("SELECT count(*) FROM analytics_facts_default")) == 0


async def test_the_rollups_get_their_partitions_too():
    """Hourly is cut DAILY and daily is cut YEARLY, so one run needs partitions at three grains. A fix
    that only covered the fact table would move the problem into the rollups."""
    await _plant()
    await n3.consume_tenant(CC)
    # Three rows per bucket, not one: consumption has three measures and a rollup row is keyed per
    # measure (correction log C5). What matters here is that every one of them landed in the real
    # partition rather than the default.
    assert set(await _where(AnalyticsHourlyRollup)) == {
        pt.partition_name("analytics_hourly_rollups", OLD.date())}
    assert len(await _where(AnalyticsHourlyRollup)) == 3
    assert set(await _where(AnalyticsDailyRollup)) == {
        pt.partition_name("analytics_daily_rollups", OLD.date())}


async def test_the_ledger_and_the_quarantine_are_covered_as_well():
    """Both are keyed on a WRITE time, so their partition is normally today's and already exists. They
    are still provisioned, because "the runway happens to be healthy" is not a guarantee."""
    await _plant()
    await n3.consume_tenant(CC)
    rows = await _where(AnalyticsFactLedger)
    assert rows and all(r != "analytics_fact_ledger_default" for r in rows)


async def test_running_twice_creates_nothing_the_second_time():
    """Idempotent, and it has to be: this runs on EVERY run, and the common case is that every partition
    already exists. A statement that was not a no-op there would take an ACCESS EXCLUSIVE lock on the
    fact table once per ticket, forever."""
    await _plant()
    await n3.consume_tenant(CC)
    first = await _partition_count("analytics_facts")

    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=OLD - timedelta(hours=1),
                                      range_end=OLD + timedelta(hours=1)))
        await db.commit()
    await n3.consume_tenant(CC)
    assert await _partition_count("analytics_facts") == first


async def _partition_count(table) -> int:
    async with async_session() as db:
        return await db.scalar(text("""
            SELECT count(*) FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent
            WHERE p.relname = :t"""), {"t": table})


# ==================================================== the range asked for
async def test_the_range_is_widened_for_the_tenant_local_day():
    """`business_date` is the tenant-LOCAL day, so a ticket ending late in a UTC day can produce a
    business date one day later. Asking only for the UTC days in the range would leave that day
    unprovisioned at a year boundary."""
    days = n3._destination_days(datetime(2019, 3, 14, 12, tzinfo=timezone.utc),
                               datetime(2019, 3, 14, 13, tzinfo=timezone.utc))
    assert date(2019, 3, 13) in days and date(2019, 3, 15) in days


async def test_today_is_always_included():
    """The ledger and the quarantine are keyed on write time, so what they need is TODAY's partition --
    which is not in the run's range when the run is folding history."""
    days = n3._destination_days(OLD, OLD)
    assert datetime.now(timezone.utc).date() in days


# ==================================================== scope and locking
async def test_the_analytics_worker_does_not_provision_log_tables():
    """Invariant 1 in spirit: analytics is a strict reader of the ingestion pipeline. Creating log
    partitions for a historic range would also hand retention new partitions to drop, on tables this
    component has no business touching."""
    assert "log_entries" not in n3._DESTINATION_TABLES
    assert "log_transactions" not in n3._DESTINATION_TABLES
    assert "log_entry_assignment" not in n3._DESTINATION_TABLES
    assert set(n3._DESTINATION_TABLES) <= {t.table for t in pt.PARTITIONED}


async def test_every_partitioned_table_analytics_writes_is_provisioned():
    """The complement, and the one that catches a table added later. A destination missing from this
    tuple reintroduces the whole bug for that table alone."""
    written = {"analytics_facts", "analytics_fact_ledger", "analytics_quality_issues",
               "analytics_hourly_rollups", "analytics_daily_rollups"}
    assert written <= set(n3._DESTINATION_TABLES)


def test_the_ddl_runs_in_its_own_session_not_the_runs_transaction():
    """`CREATE TABLE ... PARTITION OF` takes ACCESS EXCLUSIVE on the parent. Held to the end of the
    run's transaction it would block every other tenant's fold and the partition worker; in its own
    short transaction it is held for milliseconds. An empty partition left behind by a run that then
    failed is harmless."""
    import inspect
    src = inspect.getsource(n3._ensure_destination_partitions)
    assert "async_session()" in src and "commit()" in src
    run = inspect.getsource(n3._consume_run)
    assert run.index("_ensure_destination_partitions") < run.index("async with async_session()"), \
        "provisioning must happen BEFORE the run's transaction opens"


def test_provisioning_happens_before_either_read():
    """It has to precede the reads, not just the writes: the whole run is one transaction, so a failure
    discovered at write time would roll back work already done."""
    import inspect
    src = inspect.getsource(n3._consume_run)
    assert src.index("_ensure_destination_partitions") < src.index("_read_source")


# ==================================================== the table filter on ensure_coverage
async def test_ensure_coverage_honours_a_table_filter():
    async with async_session() as db:
        made = await pt.ensure_coverage(db, days=[date(2019, 3, 14)],
                                        tables=("analytics_facts",))
        await db.commit()
    assert made == 1
    assert await _exists("analytics_facts", date(2019, 3, 1))
    assert not await _exists("analytics_hourly_rollups", date(2019, 3, 14))


async def test_ensure_coverage_with_no_filter_still_covers_everything():
    """Every existing caller passes no filter -- the partition worker's runway and the log-table
    migration both want all of them -- so the default must stay "all tables"."""
    import inspect
    sig = inspect.signature(pt.ensure_coverage)
    assert sig.parameters["tables"].default is None


async def test_an_unknown_table_in_the_filter_is_an_error_not_a_silent_skip():
    """A typo would otherwise provision nothing and look exactly like a healthy no-op, which is how a
    destination silently goes unprovisioned again."""
    with pytest.raises(KeyError):
        async with async_session() as db:
            await pt.ensure_coverage(db, days=[date(2019, 3, 14)], tables=("analytics_factz",))
