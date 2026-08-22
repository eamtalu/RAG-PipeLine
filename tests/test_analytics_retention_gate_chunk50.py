"""Chunk 50, Phase 4: gate source retention on healthy analytics state.

    Phase 4. ... Gate source retention on healthy state.

The hazard being guarded against: `log_transactions` partitions drop at 60 days, and if analytics is
broken when that happens, the source needed to repair it is gone. Not merely undetected - unprovable.

The hazard being guarded against on the OTHER side is worse, and it is why this file is mostly about
limits. `consumer_cursors` already learned this lesson:

    A consumer that stops reporting is treated as gone: it stops blocking retention and is logged
    CRITICAL. That is the survivable failure - blocking forever fills the disk, which is a total
    outage, while losing data for one dead consumer is contained - but it is made loud rather than
    silent.

So this gate holds retention only for a BOUNDED time, and when the cap expires it releases and says so
at CRITICAL. A gate that could hold forever converts a stale chart into a full disk, which trades a
reporting problem for an outage.

Two more properties matter and are easy to get wrong:

**Unhealthy must mean unhealthy, not unused.** A tenant that has never folded anything, or has no
analytics rows at all, is not broken. Treating "no state" as unhealthy would hold retention for the whole
instance on the strength of a tenant nobody has switched on.

**The gate is instance-wide, because retention is.** Partitions are not per tenant, so one broken tenant
does hold everyone's partitions - which is exactly why the cap exists.
"""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.services.workers import log_partition_worker as pw
from app.settings import settings

CC = "gate-probe"
NOW = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)


async def _wipe():
    async with async_session() as db:
        await db.execute(delete(AnalyticsTenantState).where(
            AnalyticsTenantState.customer_code == CC))
        await db.execute(delete(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _state(**kw):
    async with async_session() as db:
        db.add(AnalyticsTenantState(customer_code=CC, **kw))
        await db.commit()


async def _abandoned_ticket(at=NOW):
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=at, range_end=at,
                                      abandoned_at=at))
        await db.commit()


async def _health():
    async with async_session() as db:
        return await pw.analytics_health(db, now=NOW)


# ==================================================== what counts as unhealthy
async def test_a_dead_lettered_ticket_makes_the_tenant_unhealthy():
    """The clearest signal there is. A ticket that exhausted its attempts means a range was never
    diffed, so some total is wrong and the source is what would prove it."""
    await _state(last_cycle_at=NOW, source_write_frontier=NOW)
    await _abandoned_ticket()
    h = await _health()
    assert h.holding is True
    assert CC in h.unhealthy_tenants
    assert "abandoned" in h.reason.lower()


async def test_a_recorded_cycle_error_makes_the_tenant_unhealthy():
    await _state(last_cycle_at=NOW, source_write_frontier=NOW, last_error="planted failure")
    h = await _health()
    assert h.holding is True and CC in h.unhealthy_tenants


async def test_a_healthy_tenant_holds_nothing():
    await _state(last_cycle_at=NOW, source_write_frontier=NOW)
    h = await _health()
    assert h.holding is False and h.unhealthy_tenants == []


# ==================================================== unused is not unhealthy
async def test_no_analytics_state_at_all_does_not_hold_retention():
    """The worker ships disabled, so this is the NORMAL state until someone switches it on. Holding
    retention here would freeze partition drops on every instance that has not adopted analytics."""
    h = await _health()
    assert h.holding is False, "an unused platform must not hold the whole instance's retention"


async def test_a_tenant_that_has_never_folded_is_not_unhealthy():
    """A NULL frontier means "processed nothing", which is what a new tenant looks like. It already
    suppresses the retention CURSOR (D5); it must not also be read as a fault."""
    await _state(last_cycle_at=None, source_write_frontier=None)
    h = await _health()
    assert h.holding is False and h.unhealthy_tenants == []


async def test_an_open_ticket_is_not_unhealthy_by_itself():
    """Open tickets are the normal steady state -- roughly one every 70 seconds. Only an ABANDONED one
    means the work will not happen."""
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=NOW, range_end=NOW))
        await db.commit()
    await _state(last_cycle_at=NOW, source_write_frontier=NOW)
    h = await _health()
    assert h.holding is False


# ==================================================== the cap, which is the whole point
async def test_the_hold_expires_and_releases_rather_than_blocking_forever():
    """The lesson consumer_cursors already learned. Blocking forever fills the disk, which is a total
    outage; losing the ability to prove one tenant's totals is contained."""
    stale = NOW - timedelta(days=settings.analytics_retention_hold_max_days + 1)
    await _state(last_cycle_at=stale, source_write_frontier=stale, last_error="broken for weeks")
    h = await _health()
    assert h.holding is False, "the cap must release"
    assert CC in h.expired_tenants, "and it must say which tenant it gave up on"


async def test_a_recently_broken_tenant_is_still_within_the_cap():
    recent = NOW - timedelta(days=1)
    await _state(last_cycle_at=recent, source_write_frontier=recent, last_error="broken yesterday")
    h = await _health()
    assert h.holding is True and h.expired_tenants == []


async def test_releasing_the_hold_is_logged_at_critical(caplog):
    """It must page someone. Silently releasing is how the guard becomes decorative."""
    import logging
    stale = NOW - timedelta(days=settings.analytics_retention_hold_max_days + 1)
    await _state(last_cycle_at=stale, source_write_frontier=stale, last_error="broken for weeks")
    with caplog.at_level(logging.CRITICAL):
        await _health()
    assert any(r.levelno >= logging.CRITICAL for r in caplog.records), \
        "giving up on a tenant's provability must be CRITICAL, not INFO"


async def test_a_tenant_with_no_cycle_yet_cannot_expire_the_cap():
    """`last_cycle_at IS NULL` means never run, not "broken since the epoch". Treating it as the latter
    would expire the cap instantly and make the gate a no-op."""
    await _abandoned_ticket()
    await _state(last_cycle_at=None, source_write_frontier=None)
    h = await _health()
    assert CC not in h.expired_tenants


# ==================================================== how it reaches the partition worker
async def test_the_gate_blocks_only_the_source_log_tables():
    """Analytics tables are not gated on analytics health -- that would be circular, and the fact table
    is KEEP_FOREVER anyway. What must survive is the SOURCE, which is what a repair reads."""
    assert set(pw.HEALTH_GATED) == {"log_entries", "log_transactions", "log_entry_assignment"}
    assert not any(t.startswith("analytics") for t in pw.HEALTH_GATED)


async def test_an_unhealthy_hold_blocks_every_source_period():
    from datetime import date
    await _state(last_cycle_at=NOW, source_write_frontier=NOW)
    await _abandoned_ticket()
    periods = [("log_entries", date(2026, 6, 1)), ("log_transactions", date(2026, 6, 1)),
               ("analytics_hourly_rollups", date(2026, 6, 1))]
    async with async_session() as db:
        blocked = await pw.periods_blocked_by_analytics(db, periods, now=NOW)
    assert ("log_entries", date(2026, 6, 1)) in blocked
    assert ("log_transactions", date(2026, 6, 1)) in blocked
    assert ("analytics_hourly_rollups", date(2026, 6, 1)) not in blocked, "not gated on itself"


async def test_a_healthy_instance_blocks_nothing():
    from datetime import date
    await _state(last_cycle_at=NOW, source_write_frontier=NOW)
    async with async_session() as db:
        blocked = await pw.periods_blocked_by_analytics(
            db, [("log_entries", date(2026, 6, 1))], now=NOW)
    assert blocked == set()


async def test_the_gate_is_wired_into_periods_blocked():
    """A guard that exists but is not consulted is worse than no guard: it reads as protection."""
    import inspect
    src = inspect.getsource(pw.periods_blocked)
    assert "periods_blocked_by_analytics" in src


async def test_the_gate_can_be_switched_off():
    """It must be possible to release retention deliberately when the disk is the emergency, without
    editing code. The three holds are independent, so turning this one off leaves Stage 2's and the
    consumer cursors' guards in place."""
    assert hasattr(settings, "analytics_retention_gate_enabled")
    from datetime import date
    await _state(last_cycle_at=NOW, source_write_frontier=NOW)
    await _abandoned_ticket()
    original = settings.analytics_retention_gate_enabled
    try:
        settings.analytics_retention_gate_enabled = False
        async with async_session() as db:
            assert await pw.periods_blocked_by_analytics(
                db, [("log_entries", date(2026, 6, 1))], now=NOW) == set()
    finally:
        settings.analytics_retention_gate_enabled = original


async def test_a_failure_reading_health_does_not_release_retention(monkeypatch):
    """Fail CLOSED here, unlike the consumer-cursor default. An error reading health is not evidence
    that analytics is fine, and the cost of holding one extra cycle is a day of disk against permanently
    unprovable totals. The cap still bounds it."""
    from datetime import date

    async def boom(*a, **k):
        raise RuntimeError("cannot read analytics health")
    monkeypatch.setattr(pw, "analytics_health", boom)
    async with async_session() as db:
        blocked = await pw.periods_blocked_by_analytics(
            db, [("log_entries", date(2026, 6, 1))], now=NOW)
    assert blocked == {("log_entries", date(2026, 6, 1))}
