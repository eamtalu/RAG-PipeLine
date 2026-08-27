"""Chunk 64 (section 18q of docs/analytics-ml-architecture/final_architecture.md): the two S3
follow-ups the document itself mandated and the deploy left behind.

Section 18 ("What this changes for the analytics platform") names both, and both fired the moment S3
shipped on 2026-08-25:

**The frontier column.** F6's retention frontier was measured on `log_transactions.created_at`, which
was correct while Stage 2 delete-and-reinserted (every write refreshed it). S3 made rows UPDATE in
place, so `created_at` now means "first written" and a row's latest WRITE is `updated_at`. A frontier
still measured on `created_at` cannot see in-place rewrites: a tenant whose traffic is rebuild-heavy
reports a stalled frontier, `periods_blocked_by_consumers` holds source partitions forever, and the
document's own Flow F watch ("a deferred change to update-in-place would stop `created_at` moving and
break this silently") is exactly the failure. The constant `_FRONTIER_COLUMN` existed for this one
edit - these tests make it real and make sure it stays real.

**The dedup key.** `evaluators._txn_event` deduped on `(rule, transaction)`, version-blind, which was
the best available while every rebuild re-inserted rows. The accepted residual risk (stability.py):
a transaction whose status changes - incomplete that later ERRORS - is deduped away and never
re-alerts. Section 18 names the fix that S3 unlocks: put the status in the key, so a status CHANGE is
new information and alerts, while a re-poll of the same status still dedupes to exactly one event.

Frontier tests follow chunk 45's committed-fixture style because `consume_tenant` opens its own
sessions; evaluator tests are pure (no database)."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.consumer_cursor import ConsumerCursor
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.models.notification import NotificationRule
from app.services.analytics import consume as n3
from app.services.notifications.rules.evaluators import StatusMatchEvaluator

CC = "test_chunk64"
T0 = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)
WIDE = timedelta(hours=6)


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsFact, AnalyticsFactLedger, AnalyticsQualityIssue,
                      AnalyticsPendingWindow, AnalyticsTenantState, LogTransaction):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(ConsumerCursor).where(ConsumerCursor.consumer == n3.CONSUMER))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _plant_txn(*, created_at: datetime, updated_at: datetime, ticket: bool = True) -> uuid.UUID:
    """One committed transaction whose write instants are set EXPLICITLY, the way an S3 in-place
    update leaves them: `created_at` frozen at first write, `updated_at` at the latest rewrite."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        t = LogTransaction(
            customer_code=CC, job_id=job.id, sealed=True,
            started_at=T0, ended_at=T0, date=T0.date(), duration_ms=100,
            method="ConfirmPickLine", transaction_name="Pick", transaction_type="002001",
            status=LogTransactionStatus.success, item_number="101978", user_name="EDA",
            warehouse="BRI", attributes={"QuantityPicked": "10.0"},
            created_at=created_at, updated_at=updated_at)
        db.add(t)
        await db.flush()
        if ticket:
            db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                          range_end=T0 + WIDE))
        await db.commit()
        return t.id


async def _frontier() -> datetime | None:
    async with async_session() as db:
        return (await db.execute(select(AnalyticsTenantState.source_write_frontier).where(
            AnalyticsTenantState.customer_code == CC))).scalar_one_or_none()


# ==================================================== 1. the frontier column

async def test_the_frontier_advances_when_a_row_is_updated_in_place():
    """The defect itself. A row first written ten days ago and rewritten in place NOW carries an old
    `created_at` and a fresh `updated_at`. The retention frontier must claim the newest WRITE the fold
    has read - `updated_at` - because that is the promise `consumer_cursors` makes to the partition
    worker. Measured on `created_at`, the frontier reports ten days ago: the fold has consumed content
    the cursor never admits to, and a rebuild-heavy tenant stalls retention forever."""
    stale_birth = T0 - timedelta(days=10)
    fresh_write = T0 + timedelta(minutes=5)
    await _plant_txn(created_at=stale_birth, updated_at=fresh_write)

    await n3.consume_tenant(CC)

    frontier = await _frontier()
    assert frontier is not None
    assert frontier >= fresh_write, (
        f"frontier {frontier!r} still reads the stale created_at {stale_birth!r}; "
        f"it must reflect the latest write instant {fresh_write!r}")


async def test_the_frontier_constant_is_updated_at_and_is_actually_used():
    """Section 18: '`_FRONTIER_COLUMN` must move to `updated_at`. The constant existed for exactly
    this, and it is the whole of the edit.' Two assertions because the constant had quietly become
    DEAD: the fold read the literal string 'created_at', so repointing the constant alone would have
    changed nothing. The named constant must both say `updated_at` and be the thing the fold reads."""
    import inspect

    assert n3._FRONTIER_COLUMN is LogTransaction.updated_at

    src = inspect.getsource(n3._consume_run)
    assert "_FRONTIER_COLUMN.key" in src, (
        "the fold must derive the frontier through the named constant, not a string literal")


# ==================================================== 2. the dedup key

def _rule() -> NotificationRule:
    return NotificationRule(id=uuid.uuid4(), customer_code=CC, name="errors",
                            rule_type="status_match", match={"statuses": ["error", "soft"]},
                            severity="error")


def _txn(status: LogTransactionStatus) -> LogTransaction:
    return LogTransaction(id=uuid.uuid4(), customer_code=CC, job_id=uuid.uuid4(),
                          started_at=T0, ended_at=T0, date=T0.date(), duration_ms=100,
                          method="ConfirmPickLine", transaction_name="Pick",
                          transaction_type="002001", status=status,
                          item_number="101978", user_name="EDA", warehouse="BRI", attributes={})


def test_a_status_change_produces_a_new_dedup_key():
    """The accepted residual risk in stability.py, now closable: a transaction alerted as one status
    and later rewritten to another is NEW information. A version-blind key `(rule, txn)` dedupes the
    correction away forever; with the status in the key, the changed row alerts again."""
    rule = _rule()
    ev = StatusMatchEvaluator(rule)
    txn = _txn(LogTransactionStatus.error)

    first = ev.evaluate(txn)
    assert first is not None

    txn.status = LogTransactionStatus.soft
    second = ev.evaluate(txn)
    assert second is not None
    assert first.dedup_key != second.dedup_key, (
        "a status change must not be deduped away - it is the correction the alert exists for")


def test_the_same_status_still_dedupes_to_one_event():
    """The other half of the contract, so the change cannot flood: the worker polls, and an unchanged
    transaction is seen many times. Same (rule, transaction, status) must produce the identical key
    every time."""
    rule = _rule()
    ev = StatusMatchEvaluator(rule)
    txn = _txn(LogTransactionStatus.error)

    assert ev.evaluate(txn).dedup_key == ev.evaluate(txn).dedup_key
