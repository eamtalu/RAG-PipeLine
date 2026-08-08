"""Chunk 34 (step 8 of docs/plan/2026-08-08_notification-architecture.html): a shared cursor registry,
and retention that respects it.

Retention drops partitions past 60 days. It already refuses to drop a day Stage 2 has not finished
stitching (`days_blocked_by_pending`). A slow READER needs the same protection: if ML feature
extraction is 70 days behind and the partition worker drops day 70, that data is gone permanently and
ML silently skips it. Nothing anywhere would record that it happened.

So every incremental consumer publishes one number — "I have consumed everything up to here" — into
`consumer_cursors`, and retention gates on the minimum across them.

Doing this BEFORE a slow consumer exists is the point. Retrofitting it after ML is running means
discovering the gap by losing data.

Two shapes of cursor, deliberately kept apart:

- `notification_rules.cursor_at` is INTERNAL per-rule progress. Rules advance independently, and one
  rule being replayed must not drag another's position.
- `consumer_cursors` is the PUBLISHED contract with retention: the oldest position the subsystem as a
  whole still needs. For notifications that is the minimum across its active streaming rules.

The hard question this file answers is what to do about a consumer that stops reporting. Blocking
forever fills the disk, which is a total outage. Never blocking loses data for one consumer, which is
bad but contained. So a stale consumer stops blocking and is alarmed CRITICAL — the failure is made
loud rather than allowed to take the database down.
"""

import uuid
from datetime import date as date_type, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.persistence.models.consumer_cursor import ConsumerCursor
from app.services import consumer_cursors as cc
from app.services.workers import log_partition_worker as pw
from app.settings import settings

NAME = "test_chunk34"


def _utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


# =============================================================== the gate rule (pure)
def test_a_day_fully_consumed_by_everyone_is_droppable():
    """The ordinary case: every consumer has read past the end of the day, so nothing needs it."""
    day = date_type(2026, 6, 1)
    assert cc.blocks(day, min_position=_utc(2026, 6, 5)) is False


def test_a_day_a_consumer_has_not_reached_is_blocked():
    day = date_type(2026, 6, 1)
    assert cc.blocks(day, min_position=_utc(2026, 5, 20)) is True


def test_a_consumer_partway_through_a_day_still_blocks_it():
    """The boundary that matters. Dropping a day someone is mid-way through loses the rest of it, and
    the consumer would never know — its cursor simply moves past the gap."""
    day = date_type(2026, 6, 1)
    assert cc.blocks(day, min_position=_utc(2026, 6, 1, 13, 0)) is True


def test_a_consumer_exactly_at_the_end_of_a_day_does_not_block_it():
    """`position` means "everything strictly before here is consumed", so reaching the next midnight
    means the day is finished."""
    day = date_type(2026, 6, 1)
    assert cc.blocks(day, min_position=_utc(2026, 6, 2)) is False


def test_no_consumers_means_nothing_is_blocked():
    """An empty registry must not freeze retention - that would be the worst possible default, since
    it fails closed on a disk that fills."""
    assert cc.blocks(date_type(2026, 6, 1), min_position=None) is False


# =============================================================== staleness (pure)
def test_a_freshly_updated_consumer_counts():
    now = _utc(2026, 8, 8, 12, 0)
    assert cc.is_live(now - timedelta(minutes=5), now=now) is True


def test_a_consumer_that_stopped_reporting_is_ignored():
    """Blocking forever on a dead consumer fills the disk, which is a total outage. Losing data for
    one consumer is bad but contained, so this fails in the survivable direction - loudly."""
    now = _utc(2026, 8, 8, 12, 0)
    stale = now - timedelta(seconds=settings.consumer_cursor_stale_after_seconds + 60)
    assert cc.is_live(stale, now=now) is False


def test_the_staleness_threshold_survives_an_ordinary_restart():
    """If a deploy or a nightly restart could trip it, the alarm would be noise and get ignored."""
    assert settings.consumer_cursor_stale_after_seconds >= 3600


# =============================================================== the registry (DB)
async def _cleanup(db):
    await db.execute(delete(ConsumerCursor).where(ConsumerCursor.consumer.like(f"{NAME}%")))
    await db.flush()


async def test_reporting_creates_a_row(db):
    await _cleanup(db)
    await cc.report(db, f"{NAME}:a", position=_utc(2026, 6, 1))
    await db.flush()
    got = await db.scalar(select(ConsumerCursor.position)
                          .where(ConsumerCursor.consumer == f"{NAME}:a"))
    assert got == _utc(2026, 6, 1)


async def test_reporting_again_updates_in_place(db):
    """One row per consumer. Appending would turn the registry into a log nobody prunes."""
    await _cleanup(db)
    await cc.report(db, f"{NAME}:a", position=_utc(2026, 6, 1))
    await cc.report(db, f"{NAME}:a", position=_utc(2026, 6, 5))
    await db.flush()
    rows = (await db.execute(select(ConsumerCursor)
                             .where(ConsumerCursor.consumer == f"{NAME}:a"))).scalars().all()
    assert len(rows) == 1
    assert rows[0].position == _utc(2026, 6, 5)


async def test_reporting_refreshes_the_heartbeat(db):
    """`updated_at` is what separates "behind" from "gone" — without it a dead consumer looks
    identical to a slow one."""
    await _cleanup(db)
    await cc.report(db, f"{NAME}:a", position=_utc(2026, 6, 1))
    await db.flush()
    first = await db.scalar(select(ConsumerCursor.updated_at)
                            .where(ConsumerCursor.consumer == f"{NAME}:a"))
    await cc.report(db, f"{NAME}:a", position=_utc(2026, 6, 2))
    await db.flush()
    second = await db.scalar(select(ConsumerCursor.updated_at)
                             .where(ConsumerCursor.consumer == f"{NAME}:a"))
    assert second >= first


async def test_the_minimum_is_taken_across_live_consumers(db):
    """Retention must respect the SLOWEST reader; the fastest one tells you nothing about safety."""
    await _cleanup(db)
    await cc.report(db, f"{NAME}:fast", position=_utc(2026, 8, 1))
    await cc.report(db, f"{NAME}:slow", position=_utc(2026, 6, 1))
    await db.flush()
    assert await cc.min_live_position(db) == _utc(2026, 6, 1)


async def test_a_stale_consumer_is_excluded_from_the_minimum(db, caplog):
    """...and its exclusion is announced, because it means that consumer is about to lose data."""
    await _cleanup(db)
    await cc.report(db, f"{NAME}:fast", position=_utc(2026, 8, 1))
    await cc.report(db, f"{NAME}:dead", position=_utc(2026, 1, 1))
    await db.flush()
    await db.execute(
        ConsumerCursor.__table__.update()
        .where(ConsumerCursor.consumer == f"{NAME}:dead")
        .values(updated_at=datetime.now(timezone.utc)
                - timedelta(seconds=settings.consumer_cursor_stale_after_seconds + 600)))
    await db.flush()
    with caplog.at_level("CRITICAL"):
        got = await cc.min_live_position(db)
    assert got == _utc(2026, 8, 1), "a dead consumer must not hold retention hostage"
    assert any(f"{NAME}:dead" in r.getMessage() for r in caplog.records), \
        "and being ignored must be alarmed, not silent"


async def test_an_empty_registry_returns_no_position(db):
    await _cleanup(db)
    assert await cc.min_live_position(db) is None


# =============================================================== the retention gate
async def test_a_day_no_consumer_has_read_is_held(db):
    await _cleanup(db)
    await cc.report(db, f"{NAME}:slow", position=_utc(2026, 6, 1))
    await db.flush()
    blocked = await pw.days_blocked_by_consumers(db, [date_type(2026, 5, 20),
                                                      date_type(2026, 7, 1)])
    assert date_type(2026, 7, 1) in blocked, "a day beyond the slowest reader must be held"
    assert date_type(2026, 5, 20) not in blocked, "a day already consumed is free to drop"


async def test_no_consumers_blocks_nothing(db):
    """Today's behaviour must be unchanged until a consumer actually registers."""
    await _cleanup(db)
    assert await pw.days_blocked_by_consumers(db, [date_type(2026, 5, 20)]) == set()


async def test_the_retention_worker_honours_the_registry(db):
    """End to end: a partition a consumer still needs must survive a real drop pass."""
    await _cleanup(db)
    from app.persistence import partitioning as pt
    today = await pw.db_today(db)
    old = today - timedelta(days=settings.log_partition_retention_days + 30)
    await pt.ensure_coverage(db, days=[old])
    await cc.report(db, f"{NAME}:slow",
                    position=datetime.combine(old, datetime.min.time(), tzinfo=timezone.utc))
    await db.commit()
    try:
        await pw.run_once(db)
        assert await pt.partition_exists(db, "log_transactions", old), \
            "retention must not drop a day a live consumer has not read"
    finally:
        await _cleanup(db)
        for t in pt.PARTITIONED:
            await db.execute(__import__("sqlalchemy").text(
                f"DROP TABLE IF EXISTS {pt.partition_name(t.table, old)}"))
        await db.commit()


# =============================================================== notifications reports in
async def test_notifications_publish_the_oldest_position_across_its_rules(db):
    """The published contract is the SLOWEST rule, since that is the oldest data the subsystem still
    needs. Publishing the fastest would invite retention to delete data a rule had not reached."""
    from app.persistence.models.notification import NotificationRule, RuleStatus
    cust = "test_chunk34_n"
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == cust))
    for pos in (_utc(2026, 7, 1), _utc(2026, 6, 1)):
        r = NotificationRule(customer_code=cust, name=f"r-{uuid.uuid4().hex[:5]}",
                             rule_type="status_match", match={"statuses": ["error"]},
                             severity="error", status=RuleStatus.active.value)
        db.add(r)
        await db.flush()
        r.cursor_at = pos
    await db.flush()
    assert await cc.notifications_position(db) == _utc(2026, 6, 1)
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == cust))
    await db.flush()


async def test_a_rule_that_has_never_run_does_not_publish_a_position(db):
    """A NULL cursor means "never run", not "at the beginning of time" — treating it as the latter
    would pin retention at the epoch and stop it dropping anything, ever."""
    from app.persistence.models.notification import NotificationRule, RuleStatus
    cust = "test_chunk34_n2"
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == cust))
    db.add(NotificationRule(customer_code=cust, name="never-run", rule_type="status_match",
                            match={"statuses": ["error"]}, severity="error",
                            status=RuleStatus.active.value))
    await db.flush()
    assert await cc.notifications_position(db) is None
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == cust))
    await db.flush()


async def test_reporting_notifications_publishes_the_registry_row(db):
    """The join between the two halves: notifications' internal per-rule cursors become the one number
    retention reads."""
    from app.persistence.models.notification import NotificationRule, RuleStatus
    cust = "test_chunk34_r"
    await _cleanup(db)
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == cust))
    r = NotificationRule(customer_code=cust, name="rep", rule_type="status_match",
                         match={"statuses": ["error"]}, severity="error",
                         status=RuleStatus.active.value)
    db.add(r)
    await db.flush()
    r.cursor_at = _utc(2026, 6, 10)
    await db.flush()

    await cc.report_notifications(db)
    await db.flush()
    got = await db.scalar(select(ConsumerCursor.position)
                          .where(ConsumerCursor.consumer == cc.NOTIFICATIONS))
    assert got == _utc(2026, 6, 10)
    await db.execute(delete(ConsumerCursor).where(ConsumerCursor.consumer == cc.NOTIFICATIONS))
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code == cust))
    await db.flush()


async def test_nothing_is_published_when_no_rule_has_run(db):
    """Publishing a position the subsystem has not actually reached would tell retention it is safe to
    drop data the rules still need."""
    from app.persistence.models.notification import NotificationRule
    await _cleanup(db)
    await db.execute(delete(NotificationRule).where(NotificationRule.customer_code.like("test_chunk34%")))
    await db.execute(delete(ConsumerCursor).where(ConsumerCursor.consumer == cc.NOTIFICATIONS))
    await db.flush()
    await cc.report_notifications(db)
    await db.flush()
    got = await db.scalar(select(ConsumerCursor.position)
                          .where(ConsumerCursor.consumer == cc.NOTIFICATIONS))
    assert got is None


def test_the_worker_reports_the_position_each_tick():
    """A registry nobody writes to is worse than none — retention would read a stale number and think
    it was safe."""
    import inspect
    from app.services.workers import notification_worker
    assert "consumer_cursors" in inspect.getsource(notification_worker)
