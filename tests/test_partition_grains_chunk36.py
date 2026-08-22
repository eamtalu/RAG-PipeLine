"""Partition grains (daily / monthly / yearly) and keep-forever retention.

`partitioning.py` was daily everywhere: the name format, the FROM/TO bounds, the coverage arithmetic
and the expiry comparison all assumed one partition is one day. `log_partition_worker` matched it with
a single retention rule, `log_partition_retention_days`, applied to every registered table.

The analytics platform needs monthly and yearly partitions, and several of its tables must be retained
FOREVER because their raw source is dropped at 60 days. Registering such a table under the old code
does two silent, destructive things: it gets daily partitions instead of monthly ones, and the
retention worker drops it at 60 days with no way to say otherwise.

The properties pinned here, in order of how badly they fail:

1. A keep-forever table is NEVER dropped, whatever retention is configured.
2. A partition expires only once its LAST day is past the cutoff, not its first. Keying on the start
   drops a month up to 30 days early.
3. The pending-window and consumer gates must protect a WHOLE period. A monthly partition whose
   middle is covered by an open stitch window must be held, even though its first day is not.
4. Runway is measured to the END of the newest partition. One monthly partition is a month of runway,
   not zero.
5. The three existing log tables keep behaving exactly as before. Every assertion about them here is
   a regression test, not a new requirement.
"""

import uuid
from datetime import date as date_type, datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from app.persistence import partitioning as pt
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.consumer_cursor import ConsumerCursor
from app.services import consumer_cursors as cc
from app.services.workers import log_partition_worker as pw
from app.settings import settings

PROBE = "grain_probe"
CC = "grain-probe-tenant"


# ============================================================== helpers
def _register(monkeypatch, *tables: pt.PartitionedTable) -> None:
    """Replace the partitioned-table registry for one test. BY_TABLE is derived at import, so both
    have to move together or `grain_of` disagrees with what `ensure_coverage` iterates."""
    monkeypatch.setattr(pt, "PARTITIONED", tuple(tables))
    monkeypatch.setattr(pt, "BY_TABLE", {t.table: t for t in tables})


def _probe(grain: pt.Grain) -> pt.PartitionedTable:
    return pt.PartitionedTable(PROBE, "ts", "throwaway probe table", grain=grain)


# ============================================================== 1. grain arithmetic (pure)
def test_the_three_grains_exist_and_the_LOG_tables_are_still_all_daily():
    """Was "the real log tables", asserted over every registered table -- correct when only the three
    log tables existed, and wrong once Phase 1 registered monthly and yearly analytics tables.

    Narrowed to what it was actually guarding: the three production tables must not change grain.
    Whether the analytics tables have the right one is asserted in test_analytics_schema_chunk41, next
    to the retention policy it has to agree with.
    """
    assert {g.value for g in pt.Grain} == {"daily", "monthly", "yearly"}
    for table in ("log_entries", "log_transactions", "log_entry_assignment"):
        assert pt.grain_of(table) is pt.Grain.daily, f"{table} silently changed grain"


def test_period_start_floors_a_date_into_its_partition():
    d = date_type(2026, 8, 21)
    assert pt.period_start(pt.Grain.daily, d) == date_type(2026, 8, 21)
    assert pt.period_start(pt.Grain.monthly, d) == date_type(2026, 8, 1)
    assert pt.period_start(pt.Grain.yearly, d) == date_type(2026, 1, 1)


def test_next_period_start_is_the_exclusive_upper_bound():
    assert pt.next_period_start(pt.Grain.daily, date_type(2026, 8, 21)) == date_type(2026, 8, 22)
    assert pt.next_period_start(pt.Grain.monthly, date_type(2026, 8, 1)) == date_type(2026, 9, 1)
    assert pt.next_period_start(pt.Grain.yearly, date_type(2026, 1, 1)) == date_type(2027, 1, 1)


def test_next_period_start_rolls_the_year_over_at_december():
    """`month + 1` is the obvious implementation and it raises on December."""
    assert pt.next_period_start(pt.Grain.monthly, date_type(2026, 12, 1)) == date_type(2027, 1, 1)


def test_next_period_start_floors_a_mid_period_input():
    """Callers hold real dates, not period starts. Advancing from the 21st must land on the 1st of the
    next month, not the 21st, or the bounds of two adjacent partitions overlap."""
    assert pt.next_period_start(pt.Grain.monthly, date_type(2026, 8, 21)) == date_type(2026, 9, 1)


def test_february_length_is_taken_from_the_calendar_not_a_constant():
    assert pt.period_end(pt.Grain.monthly, date_type(2027, 2, 1)) == date_type(2027, 2, 28)
    assert pt.period_end(pt.Grain.monthly, date_type(2028, 2, 1)) == date_type(2028, 2, 29)


def test_period_end_is_the_last_day_inside_the_partition():
    assert pt.period_end(pt.Grain.daily, date_type(2026, 8, 21)) == date_type(2026, 8, 21)
    assert pt.period_end(pt.Grain.monthly, date_type(2026, 8, 1)) == date_type(2026, 8, 31)
    assert pt.period_end(pt.Grain.yearly, date_type(2026, 1, 1)) == date_type(2026, 12, 31)


# ============================================================== 2. names and DDL
def test_the_name_suffix_follows_the_grain(monkeypatch):
    _register(monkeypatch, _probe(pt.Grain.monthly))
    assert pt.partition_name(PROBE, date_type(2026, 8, 21)) == f"{PROBE}_2026_08"
    _register(monkeypatch, _probe(pt.Grain.yearly))
    assert pt.partition_name(PROBE, date_type(2026, 8, 21)) == f"{PROBE}_2026"


def test_daily_names_are_byte_for_byte_what_they_were():
    assert pt.partition_name("log_entries", date_type(2026, 8, 5)) == "log_entries_2026_08_05"
    assert pt.partition_name("log_entry_assignment", date_type(2026, 12, 31)) == \
        "log_entry_assignment_2026_12_31"


def test_a_mid_period_date_names_the_partition_it_falls_in(monkeypatch):
    """The worker and the migration both hand over real dates. Two different days of one month must
    resolve to ONE partition name, or `ensure_coverage` tries to create the same month repeatedly under
    different names and the second CREATE collides on the range."""
    _register(monkeypatch, _probe(pt.Grain.monthly))
    assert pt.partition_name(PROBE, date_type(2026, 8, 1)) == \
        pt.partition_name(PROBE, date_type(2026, 8, 31))


def test_ddl_bounds_span_the_whole_period(monkeypatch):
    _register(monkeypatch, _probe(pt.Grain.monthly))
    sql = pt.create_partition_sql(PROBE, date_type(2026, 8, 21))
    assert "FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00')" in sql
    assert f"{PROBE}_2026_08" in sql


def test_adjacent_periods_tile_with_no_gap_and_no_overlap(monkeypatch):
    """Half-open bounds. The upper bound of one partition must be the exact lower bound of the next, or
    a row on the boundary lands in the DEFAULT partition (gap) or the CREATE fails (overlap)."""
    _register(monkeypatch, _probe(pt.Grain.monthly))
    a = pt.create_partition_sql(PROBE, date_type(2026, 8, 1))
    b = pt.create_partition_sql(PROBE, date_type(2026, 9, 1))
    assert "TO ('2026-09-01 00:00:00+00')" in a
    assert "FROM ('2026-09-01 00:00:00+00')" in b


def test_every_real_table_name_fits_postgres_even_at_the_widest_grain():
    """The identifier limit is what forced the `analytics_` prefix over `warehouse_analytics_`. A daily
    suffix is the longest, so checking it covers the other two grains."""
    for t in pt.PARTITIONED:
        assert len(pt.partition_name(t.table, date_type(2026, 12, 31))) <= 63


def test_an_unregistered_table_raises_rather_than_defaulting_to_daily():
    """Silently defaulting would give a monthly analytics table daily partitions, which is the exact
    failure this whole change exists to prevent. It has to be loud."""
    with pytest.raises(KeyError):
        pt.grain_of("no_such_table")


# ============================================================== 3. coverage arithmetic
def test_periods_covering_returns_one_entry_per_partition_touched():
    got = pt.periods_covering(pt.Grain.monthly, date_type(2026, 8, 25), date_type(2026, 9, 8))
    assert got == [date_type(2026, 8, 1), date_type(2026, 9, 1)]


def test_a_fortnight_inside_one_month_needs_one_monthly_partition():
    got = pt.periods_covering(pt.Grain.monthly, date_type(2026, 8, 2), date_type(2026, 8, 16))
    assert got == [date_type(2026, 8, 1)]


def test_periods_covering_a_year_boundary_at_yearly_grain():
    got = pt.periods_covering(pt.Grain.yearly, date_type(2026, 12, 20), date_type(2027, 1, 5))
    assert got == [date_type(2026, 1, 1), date_type(2027, 1, 1)]


def test_periods_covering_rejects_an_inverted_range():
    with pytest.raises(ValueError):
        pt.periods_covering(pt.Grain.monthly, date_type(2026, 9, 1), date_type(2026, 8, 1))


# ============================================================== 4. expiry keys on the period END
def test_a_monthly_partition_is_not_expired_until_its_last_day_passes_retention():
    """The bug this prevents. January starts on the 1st; at 60 days' retention, keying on the START
    drops January once today is 2 March, throwing away 30 days of still-in-policy data."""
    jan = date_type(2026, 1, 1)
    # 2026-03-02 is 60 days after 2026-01-01 but only 30 after 2026-01-31.
    assert pt.expired_days([jan], date_type(2026, 3, 2),
                           retention_days=60, grain=pt.Grain.monthly) == []
    # 2026-04-02 is 61 days after 2026-01-31, so January is genuinely out of policy.
    assert pt.expired_days([jan], date_type(2026, 4, 2),
                           retention_days=60, grain=pt.Grain.monthly) == [jan]


def test_daily_expiry_is_unchanged_and_still_keeps_the_boundary_day():
    """Regression. Off-by-one here deletes a day of production data that was still in policy."""
    today = date_type(2026, 8, 5)
    cutoff = today - timedelta(days=60)
    assert pt.expired_days([cutoff], today, retention_days=60) == []
    assert pt.expired_days([cutoff - timedelta(days=1)], today, retention_days=60) == \
        [cutoff - timedelta(days=1)]


def test_expiry_defaults_to_daily_so_existing_callers_are_unaffected():
    covered = [date_type(2026, 1, 1), date_type(2026, 8, 1)]
    assert pt.expired_days(covered, date_type(2026, 8, 5), retention_days=60) == \
        pt.expired_days(covered, date_type(2026, 8, 5), retention_days=60, grain=pt.Grain.daily)


# ============================================================== 5. keep-forever retention
def test_a_keep_forever_table_is_never_droppable(monkeypatch):
    """The worst failure mode in the analytics plan: raw data is gone at 60 days, so a dropped fact
    partition is unrecoverable."""
    _register(monkeypatch, _probe(pt.Grain.monthly))
    monkeypatch.setattr(pw, "KEEP_FOREVER", frozenset({PROBE}))
    ancient = [date_type(2019, 1, 1), date_type(2020, 6, 1)]
    assert pw.droppable_days(PROBE, ancient, date_type(2026, 8, 21)) == []
    assert pw.retention_days_for(PROBE) is None


def test_a_per_table_retention_override_is_respected(monkeypatch):
    """The plan wants 90 days for hourly rollups and a year for quality issues, against 60 for logs."""
    _register(monkeypatch, _probe(pt.Grain.daily))
    monkeypatch.setattr(pw, "RETENTION_DAYS", {PROBE: 90})
    assert pw.retention_days_for(PROBE) == 90
    today = date_type(2026, 8, 21)
    just_inside = today - timedelta(days=90)
    just_outside = today - timedelta(days=91)
    assert pw.droppable_days(PROBE, [just_inside], today) == []
    assert pw.droppable_days(PROBE, [just_outside], today) == [just_outside]


def test_every_entry_in_the_retention_collections_is_a_registered_partitioned_table():
    """Was "both are empty today", which was true when the grains landed and is deliberately false now
    that Phase 1 has registered the analytics tables.

    What still needs guarding is that neither collection names something that does not exist: a typo
    there fails OPEN, silently leaving the real table on the log tables' 60-day retention.
    """
    for table in pw.KEEP_FOREVER | set(pw.RETENTION_DAYS):
        assert table in pt.BY_TABLE, (
            f"{table} has a retention policy but is not a registered partitioned table; a typo here "
            f"fails open and leaves the real table on the log default")


def test_the_log_tables_are_untouched_by_those_collections():
    """The regression that matters: the three production tables must still follow
    `log_partition_retention_days`, with only the documented one-day entries lag."""
    from app.settings import settings
    base = settings.log_partition_retention_days
    assert pw.retention_days_for("log_transactions") == base
    assert pw.retention_days_for("log_entries") == base + 1
    assert pw.retention_days_for("log_entry_assignment") == base + 1
    for table in ("log_entries", "log_transactions", "log_entry_assignment"):
        assert table not in pw.KEEP_FOREVER and table not in pw.RETENTION_DAYS


def test_log_table_retention_is_unchanged_including_the_one_day_lag():
    """Entries and assignments outlive transactions by a day, because a transaction starting at 23:58
    owns entries into the next day."""
    base = settings.log_partition_retention_days
    assert pw.retention_days_for("log_transactions") == base
    assert pw.retention_days_for("log_entries") == base + 1
    assert pw.retention_days_for("log_entry_assignment") == base + 1


# ============================================================== 6. the gates protect a whole period
async def test_an_open_stitch_window_mid_month_holds_the_whole_monthly_partition(db, monkeypatch):
    """The gate used to be keyed on a DAY. A monthly partition's start is the 1st, so a window covering
    only the 15th would not block it and a month of unstitched data would be dropped."""
    _register(monkeypatch, _probe(pt.Grain.monthly))
    await db.execute(text("DELETE FROM log_regroup_pending WHERE customer_code = :c"), {"c": CC})
    mid = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)
    db.add(LogRegroupPending(customer_code=CC, job_id=uuid.uuid4(),
                             range_start=mid, range_end=mid + timedelta(minutes=5)))
    await db.flush()

    blocked = await pw.periods_blocked(db, [(PROBE, date_type(2026, 8, 1))])
    assert (PROBE, date_type(2026, 8, 1)) in blocked, \
        "a window inside the month must hold the whole monthly partition"

    # ... and a month the window does not touch is free to go.
    free = await pw.periods_blocked(db, [(PROBE, date_type(2026, 6, 1))])
    assert free == set()


async def test_a_consumer_sitting_mid_month_holds_the_whole_monthly_partition(db, monkeypatch):
    _register(monkeypatch, _probe(pt.Grain.monthly))
    await db.execute(text("DELETE FROM log_regroup_pending WHERE customer_code = :c"), {"c": CC})
    await db.execute(text("DELETE FROM consumer_cursors WHERE consumer = :n"), {"n": "probe-reader"})
    db.add(ConsumerCursor(consumer="probe-reader",
                          position=datetime(2026, 8, 15, tzinfo=timezone.utc),
                          updated_at=datetime.now(timezone.utc)))
    await db.flush()

    blocked = await pw.periods_blocked(db, [(PROBE, date_type(2026, 8, 1))])
    assert (PROBE, date_type(2026, 8, 1)) in blocked, \
        "a reader partway through the month must hold the whole monthly partition"


async def test_the_day_keyed_gates_still_work_for_the_daily_log_tables(db):
    """Regression: `days_blocked_by_pending` and `days_blocked_by_consumers` keep their signatures,
    because every existing caller and test speaks in days."""
    await db.execute(text("DELETE FROM log_regroup_pending WHERE customer_code = :c"), {"c": CC})
    day = date_type(2026, 7, 9)
    inside = datetime(2026, 7, 9, 3, 0, tzinfo=timezone.utc)
    db.add(LogRegroupPending(customer_code=CC, job_id=uuid.uuid4(),
                             range_start=inside, range_end=inside + timedelta(minutes=1)))
    await db.flush()
    assert day in await pw.days_blocked_by_pending(db, [day])
    assert date_type(2026, 7, 11) not in await pw.days_blocked_by_pending(db, [date_type(2026, 7, 11)])


def test_consumer_blocks_until_takes_an_exclusive_end_instant():
    """The range form the period gate needs. `blocks` is now the daily special case of it."""
    floor = datetime(2026, 8, 15, tzinfo=timezone.utc)
    assert cc.blocks_until(datetime(2026, 9, 1, tzinfo=timezone.utc), min_position=floor) is True
    assert cc.blocks_until(datetime(2026, 8, 1, tzinfo=timezone.utc), min_position=floor) is False
    assert cc.blocks_until(datetime(2026, 9, 1, tzinfo=timezone.utc), min_position=None) is False


# ============================================================== 7. runway measures to the period end
async def test_one_monthly_partition_reports_a_month_of_runway_not_zero(db, monkeypatch):
    """Keyed on the partition START, a monthly table provisioned for the current month reports 0 days
    of runway on the 1st, which trips the CRITICAL runway alarm on every tick forever."""
    _register(monkeypatch, _probe(pt.Grain.monthly))
    await db.execute(text(f"DROP TABLE IF EXISTS {PROBE} CASCADE"))
    await db.execute(text(f"CREATE TABLE {PROBE} (id uuid NOT NULL, ts timestamptz) "
                          f"PARTITION BY RANGE (ts)"))
    try:
        created = await pt.ensure_coverage(db, days=pt.days_between(date_type(2026, 8, 1),
                                                                    date_type(2026, 8, 20)))
        assert created == 1, "twenty days of one month is ONE monthly partition"
        assert await pt.covered_days(db, PROBE) == [date_type(2026, 8, 1)]

        runway = await pw._runway_for(db, PROBE, date_type(2026, 8, 1))
        assert runway == 30, "1 Aug to 31 Aug inclusive is 30 days ahead"
    finally:
        await db.execute(text(f"DROP TABLE IF EXISTS {PROBE} CASCADE"))


async def test_ensure_coverage_creates_one_partition_per_period_not_per_day(db, monkeypatch):
    """The whole point of the grain. A month of requested days must not become 31 monthly partitions."""
    _register(monkeypatch, _probe(pt.Grain.monthly))
    await db.execute(text(f"DROP TABLE IF EXISTS {PROBE} CASCADE"))
    await db.execute(text(f"CREATE TABLE {PROBE} (id uuid NOT NULL, ts timestamptz) "
                          f"PARTITION BY RANGE (ts)"))
    try:
        days = pt.days_between(date_type(2026, 8, 25), date_type(2026, 9, 5))
        created = await pt.ensure_coverage(db, days=days)
        assert created == 2, "a span crossing one month boundary is two monthly partitions"
        assert await pt.covered_days(db, PROBE) == [date_type(2026, 8, 1), date_type(2026, 9, 1)]
        # Idempotent: the DDL is IF NOT EXISTS and the worker re-runs hourly.
        assert await pt.ensure_coverage(db, days=days) == 0
    finally:
        await db.execute(text(f"DROP TABLE IF EXISTS {PROBE} CASCADE"))
