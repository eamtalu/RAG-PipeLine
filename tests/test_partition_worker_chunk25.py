"""Chunk 25 (step 4 of docs/plan/2026-08-05_20-32_daily-partitioning.md): the partition management
worker.

Two jobs, both idempotent so a missed tick is harmless:

**Create** partitions for today through today + `log_partition_precreate_days`. This is the dangerous
half. Ingestion into a day with no partition fails outright with "no partition of relation found for
row", so exhausting the runway stops Stage 1 dead — which is why the worker alarms loudly when
coverage runs short rather than only logging what it managed to build.

**Drop** partitions past `log_partition_retention_days`, behind three gates. The third is the one that
is easy to get wrong and impossible to notice afterwards:

  1. the day is older than the retention cutoff;
  2. no OPEN `log_regroup_pending` window overlaps that day — dropping data Stage 2 is still waiting
     to stitch would silently lose transactions that were about to be built;
  3. **entries lag transactions by one day.** A transaction spans at most the seal window, so one
     starting at 23:58 owns entries until ~00:13 the NEXT day. Dropping day N's entries while a day
     N-1 transaction still references them leaves that transaction rendering half-empty, with nothing
     anywhere recording that its body used to exist. So `log_transactions` is dropped for day D and
     `log_entries` + `log_entry_assignment` only for day D-1. The cost is one extra day of entry
     storage; the bound is exact rather than a safety guess.

The drop is real and irreversible, so most of what follows tests what must NOT be dropped.
"""

import uuid
from datetime import date as date_type, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, text

from app.persistence import partitioning as pt
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.services.workers import log_partition_worker as pw
from app.settings import settings

CC = "TEST_CHUNK25"


def _utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


# ==================================================== which days get dropped (pure)
def test_transactions_are_dropped_at_the_retention_cutoff():
    today = date_type(2026, 8, 5)
    cutoff = today - timedelta(days=settings.log_partition_retention_days)
    covered = [cutoff - timedelta(days=1), cutoff, cutoff + timedelta(days=1)]
    assert pw.droppable_days("log_transactions", covered, today) == [cutoff - timedelta(days=1)]


def test_entries_are_kept_one_day_longer_than_transactions():
    """The midnight rule. A transaction starting at 23:58 owns entries into the next day, so entry
    days must survive until the transactions that can reference them are already gone."""
    today = date_type(2026, 8, 5)
    covered = pt.days_between(date_type(2026, 5, 1), date_type(2026, 7, 1))
    txn_days = pw.droppable_days("log_transactions", covered, today)
    entry_days = pw.droppable_days("log_entries", covered, today)
    assert entry_days, "entries should still be droppable, just later"
    assert max(entry_days) == max(txn_days) - timedelta(days=1)


def test_assignments_lag_exactly_with_their_entries():
    """They are co-partitioned; if the two schedules diverged, a day of assignments would be left
    pointing at entries that no longer exist, or vice versa."""
    today = date_type(2026, 8, 5)
    covered = pt.days_between(date_type(2026, 5, 1), date_type(2026, 7, 1))
    assert (pw.droppable_days("log_entry_assignment", covered, today)
            == pw.droppable_days("log_entries", covered, today))


def test_nothing_inside_the_retention_window_is_ever_droppable():
    today = date_type(2026, 8, 5)
    recent = [today, today - timedelta(days=1),
              today - timedelta(days=settings.log_partition_retention_days - 1)]
    for table in ("log_entries", "log_transactions", "log_entry_assignment"):
        assert pw.droppable_days(table, recent, today) == []


def test_future_partitions_are_never_droppable():
    """The worker creates a runway ahead of today; treating those as expired would delete the very
    partitions it just built and stop ingestion."""
    today = date_type(2026, 8, 5)
    ahead = [today + timedelta(days=i) for i in range(1, 15)]
    for table in ("log_entries", "log_transactions", "log_entry_assignment"):
        assert pw.droppable_days(table, ahead, today) == []


# ==================================================== gate 2: open stitch windows
async def _clear_pending(db):
    await db.execute(delete(LogRegroupPending).where(LogRegroupPending.customer_code == CC))
    await db.flush()


async def _pending(db, start, end, *, consumed=False, abandoned=False):
    db.add(LogRegroupPending(
        customer_code=CC, range_start=start, range_end=end,
        consumed_at=datetime.now(timezone.utc) if consumed else None,
        abandoned_at=datetime.now(timezone.utc) if abandoned else None))
    await db.flush()


async def test_a_day_with_an_open_stitch_window_is_protected(db):
    """Dropping data Stage 2 has not stitched yet destroys transactions that were about to exist."""
    await _clear_pending(db)
    day = date_type(2026, 6, 1)
    await _pending(db, _utc(2026, 6, 1, 10), _utc(2026, 6, 1, 11))
    assert await pw.days_blocked_by_pending(db, [day]) == {day}


async def test_a_consumed_window_does_not_protect_a_day(db):
    """Consumed means the stitch already happened; holding data forever for it would mean retention
    never runs on any day that was ever ingested."""
    await _clear_pending(db)
    day = date_type(2026, 6, 2)
    await _pending(db, _utc(2026, 6, 2, 10), _utc(2026, 6, 2, 11), consumed=True)
    assert await pw.days_blocked_by_pending(db, [day]) == set()


async def test_an_abandoned_window_does_not_protect_a_day(db):
    """An abandoned window is parked awaiting a human, not queued work. Letting it pin retention would
    mean one dead-lettered window stops disk being reclaimed forever."""
    await _clear_pending(db)
    day = date_type(2026, 6, 3)
    await _pending(db, _utc(2026, 6, 3, 10), _utc(2026, 6, 3, 11), abandoned=True)
    assert await pw.days_blocked_by_pending(db, [day]) == set()


async def test_a_window_that_merely_touches_a_day_still_protects_it(db):
    """Windows are padded and routinely straddle midnight; an overlap test that missed the boundary
    would drop the very day a straddling window is about to stitch."""
    await _clear_pending(db)
    day = date_type(2026, 6, 4)
    await _pending(db, _utc(2026, 6, 3, 23, 50), _utc(2026, 6, 4, 0, 10))
    assert day in await pw.days_blocked_by_pending(db, [day])


async def test_a_window_entirely_outside_the_day_does_not_protect_it(db):
    await _clear_pending(db)
    day = date_type(2026, 6, 5)
    await _pending(db, _utc(2026, 6, 7, 10), _utc(2026, 6, 7, 11))
    assert await pw.days_blocked_by_pending(db, [day]) == set()


async def test_no_days_means_no_query_and_no_blocks(db):
    assert await pw.days_blocked_by_pending(db, []) == set()


# ==================================================== creating the runway
async def test_a_tick_creates_the_configured_runway(db):
    stats = await pw.run_once(db)
    today = await pw.db_today(db)
    for t in pt.PARTITIONED:
        assert await pt.partition_exists(db, t.table, today), f"{t.table} missing today"
        assert await pt.partition_exists(
            db, t.table, today + timedelta(days=settings.log_partition_precreate_days))
    assert stats["created"] >= 0


async def test_a_second_tick_creates_nothing_new(db):
    """Both jobs are idempotent, which is what makes an hourly cadence and a missed tick harmless."""
    await pw.run_once(db)
    second = await pw.run_once(db)
    assert second["created"] == 0


async def test_coverage_is_reported_so_a_shortfall_is_visible(db):
    """The runway running out stops ingestion outright, so the worker has to report how much is left
    rather than only what it built."""
    await pw.run_once(db)
    stats = await pw.run_once(db)
    assert stats["days_ahead"] >= settings.log_partition_precreate_days


async def test_a_creation_failure_is_alarmed_not_swallowed(db, monkeypatch, caplog):
    """The dangerous failure. If creation silently stops, nothing breaks until the runway is exhausted
    days later and Stage 1 dies with 'no partition of relation found for row'."""
    async def boom(*a, **k):
        raise RuntimeError("disk full")
    monkeypatch.setattr(pt, "ensure_coverage", boom)
    with caplog.at_level("CRITICAL"):
        stats = await pw.run_once(db)
    assert stats["errors"], "a creation failure must be reported in the tick result"
    assert any("CRITICAL" == r.levelname for r in caplog.records)


async def test_a_drop_failure_does_not_stop_the_tick(db, monkeypatch, caplog):
    """A failed drop reclaims no disk and retries next tick — it must never prevent CREATION, which is
    the half that keeps ingestion alive."""
    async def boom(*a, **k):
        raise RuntimeError("lock timeout")
    monkeypatch.setattr(pw, "_drop_expired", boom)
    with caplog.at_level("WARNING"):
        stats = await pw.run_once(db)
    today = await pw.db_today(db)
    assert await pt.partition_exists(db, "log_entries", today), "creation must still have happened"
    assert stats["errors"]


# ==================================================== dropping, end to end
async def test_an_expired_day_is_actually_dropped(db):
    """The payoff: retention is a DROP, not a DELETE + VACUUM that reads the whole table."""
    today = await pw.db_today(db)
    old = today - timedelta(days=settings.log_partition_retention_days + 10)
    await pt.ensure_coverage(db, days=[old])
    assert await pt.partition_exists(db, "log_transactions", old)
    await pw.run_once(db)
    assert not await pt.partition_exists(db, "log_transactions", old)


async def test_the_entry_partition_for_the_boundary_day_survives_its_transactions(db):
    """The midnight rule, end to end. On the cycle that drops day D's transactions, day D's ENTRIES
    must still be there — a day D-1 transaction can still own them."""
    today = await pw.db_today(db)
    cutoff = today - timedelta(days=settings.log_partition_retention_days)
    boundary = cutoff - timedelta(days=1)          # the newest droppable transaction day
    await pt.ensure_coverage(db, days=[boundary])
    await pw.run_once(db)
    assert not await pt.partition_exists(db, "log_transactions", boundary)
    assert await pt.partition_exists(db, "log_entries", boundary), \
        "entries for the boundary day must outlive their transactions by one day"


async def test_a_day_held_open_by_a_pending_window_is_not_dropped(db):
    """Gate 2 end to end."""
    await _clear_pending(db)
    today = await pw.db_today(db)
    old = today - timedelta(days=settings.log_partition_retention_days + 20)
    await pt.ensure_coverage(db, days=[old])
    await _pending(db, datetime.combine(old, datetime.min.time(), tzinfo=timezone.utc),
                   datetime.combine(old, datetime.max.time(), tzinfo=timezone.utc))
    await db.commit()
    await pw.run_once(db)
    assert await pt.partition_exists(db, "log_transactions", old), \
        "a day Stage 2 has not finished stitching must never be dropped"
    await _clear_pending(db)
    await db.commit()


async def test_the_default_partition_is_never_dropped(db):
    """It holds the NULL-key rows and has no day of its own. Dropping it would make every
    timestamp-less entry un-insertable."""
    await pw.run_once(db)
    for t in pt.PARTITIONED:
        assert await db.scalar(text("SELECT to_regclass(:n) IS NOT NULL"),
                               {"n": pt.default_partition_name(t.table)}), \
            f"{t.table} lost its DEFAULT partition"


# ==================================================== reporting
def test_a_short_runway_is_alarmed_critical(caplog):
    """The failure that has no other symptom until ingestion dies. It must fire on every tick it
    applies to, not only when something changed — a tick that creates nothing looks identical whether
    the table is fully provisioned or creation has been broken for a week."""
    with caplog.at_level("CRITICAL"):
        pw.report({"created": 0, "dropped": [], "days_ahead": 0})
    assert any(r.levelname == "CRITICAL" and "runway" in r.message for r in caplog.records)


def test_a_healthy_runway_is_not_alarmed(caplog):
    with caplog.at_level("CRITICAL"):
        pw.report({"created": 0, "dropped": [],
                   "days_ahead": settings.log_partition_precreate_days})
    assert not [r for r in caplog.records if r.levelname == "CRITICAL"]


def test_work_done_is_reported(caplog):
    """A drop is irreversible, so which partitions went must appear in the log."""
    with caplog.at_level("INFO"):
        pw.report({"created": 2, "dropped": ["log_entries_2026_01_01"], "days_ahead": 14})
    rendered = [r.getMessage() for r in caplog.records]
    assert any("created=2" in m and "log_entries_2026_01_01" in m for m in rendered), rendered


def test_a_quiet_tick_says_nothing(caplog):
    """Hourly, mostly with nothing to do — logging every no-op tick would bury the ticks that matter."""
    with caplog.at_level("INFO"):
        pw.report({"created": 0, "dropped": [],
                   "days_ahead": settings.log_partition_precreate_days})
    assert not caplog.records


# ==================================================== wiring
def test_the_worker_is_registered_and_configurable():
    import inspect
    from app import background
    src = inspect.getsource(background)
    assert "run_log_partition_worker" in src, "the worker must be started by background.py"
    assert settings.log_partition_worker_enabled is True
    assert settings.log_partition_worker_interval_seconds >= 60


def test_the_runway_outlasts_many_missed_ticks():
    """The whole point of a long runway is that the worker can be down and ingestion continues."""
    ticks_per_day = 86400 / settings.log_partition_worker_interval_seconds
    assert ticks_per_day >= 1, "the worker must run at least daily"
    assert settings.log_partition_precreate_days >= 7, \
        "less than a week of runway leaves no room to notice the worker is down"
