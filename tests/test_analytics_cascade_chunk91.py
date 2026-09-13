"""Chunk 91: the grain cascade becomes facts -> hourly -> daily -> monthly, guarded by the tenant zone.

Before this chunk the daily bucket was folded from the whole business day of facts, once per active
metric, whenever one fact in that day changed. Now the hourly bucket is folded from the dirty hours'
facts, read ONCE per run, and the daily bucket is merged from its hourly rows - exact whenever the
tenant's local midnights fall on whole UTC hours, which is every whole-hour zone including
Europe/London on both sides of a clock change. A zone with a half-hour offset, a day older than the
hourly retention horizon, or a metric without an hourly grain falls back to the fact read.

The same chunk fixes two defects the probe on 13 Sep 2026 exposed: a ticket over UNCHANGED facts
never refolded anything, so a metric activated with a past `rollups_from` got no rollups and a `show`
flip left hidden rows in place. Tickets now carry `refold_rollups`, and a run holding one treats every
bucket in its range as dirty.

Pinned here
-----------
    guard          local_day_span: whole-hour zones give a span (23/24/25 hours across DST), half-hour
                   zones give None, no zone means UTC, a bogus zone falls back to UTC exactly as
                   business_date does
    derivation     daily rows merged from hourly equal daily rows folded from facts, for sum, count,
                   min, max and the distinct sketch, across a London midnight
    read once      one fact read per run however many metrics are active; no date predicate when
                   every dirty day is derivable
    skip           a metric whose filter no changed fact matches is not folded
    fallback       Asia/Kolkata and a 100-day-old day read facts by date and are still correct
    refold         activation backfill builds rollups; show off removes rows; show on restores them
    moved fact     a fact crossing local midnight leaves one day and lands in the next
    stats          rollup_rows_written and definitions_skipped are reported
"""
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select, update

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import diff as dd
from app.services.analytics import hll
from app.services.analytics import normalizer as n2
from app.services.analytics import pending_windows
from app.services.analytics import registry
from app.services.analytics import rollups as n5
from app.services.analytics.contract import QUANTITY_FIELD as QF
from app.services.workers.log_partition_worker import RETENTION_DAYS

CC = "test_chunk91"
#: 10 Sep 2026 09:00 UTC = 10:00 BST. The London day runs 09 Sep 23:00Z .. 10 Sep 23:00Z.
T0 = datetime(2026, 9, 10, 9, 0, tzinfo=timezone.utc)
DAY = date(2026, 9, 10)
WIDE = timedelta(hours=6)

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsFactLedger, AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState,
          AnalyticsMetric, AnalyticsTransactionRegistry, LogTransaction)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


async def _tenant(tz: str | None = "Europe/London"):
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="cascade probe", timezone=tz))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _plant(rows, *, lo=None, hi=None, ticket=True):
    """`rows` are (instant, item, quantity[, method]) tuples. One ticket over them unless told not to."""
    async with async_session() as db:
        job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                  storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
        db.add(job)
        await db.flush()
        ids = []
        for spec in rows:
            at, item, qty = spec[0], spec[1], spec[2]
            method = spec[3] if len(spec) > 3 else "ConfirmPickLine"
            t = LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True, started_at=at, ended_at=at,
                date=at.date(), duration_ms=100, method=method, transaction_name="Pick",
                transaction_type="002001", status=LogTransactionStatus.success, item_number=item,
                user_name="EDA", warehouse="BRI", attributes={QF[method]: qty} if method in QF else {})
            db.add(t)
            ids.append(t)
        await db.flush()
        if ticket:
            instants = [r[0] for r in rows]
            db.add(AnalyticsPendingWindow(customer_code=CC,
                                          range_start=lo or (min(instants) - WIDE),
                                          range_end=hi or (max(instants) + WIDE)))
        await db.commit()
        return [t.id for t in ids]


def _definition(**over) -> d.MetricDefinition:
    base = dict(name="picks", dimensions=("method", "warehouse"),
                measures=(d.Measure(name="quantity", aggregation=d.Aggregation.sum, field="quantity"),
                          d.Measure(name="items", aggregation=d.Aggregation.distinct, field="item_number"),
                          d.Measure(name="span", aggregation=d.Aggregation.extent, field="quantity")),
                grains=("hourly", "daily", "monthly"), method_filter=("ConfirmPickLine",),
                status=d.Status.active)
    base.update(over)
    return d.MetricDefinition(**base)


async def _register(definition: d.MetricDefinition) -> uuid.UUID:
    async with async_session() as db:
        row = AnalyticsMetric(**registry.to_row(definition, customer_code=CC, created_by="test"))
        db.add(row)
        await db.commit()
        return row.id


async def _rows(model, definition_id, measure="quantity"):
    async with async_session() as db:
        return list((await db.execute(select(model).where(
            model.customer_code == CC, model.definition_id == definition_id,
            model.measure_name == measure))).scalars().all())


async def _facts():
    async with async_session() as db:
        rows = (await db.execute(select(AnalyticsFact).where(
            AnalyticsFact.customer_code == CC))).scalars().all()
        return [{c.name: getattr(r, c.name) for c in AnalyticsFact.__table__.columns} for r in rows]


def _daily_from_facts(facts, definition):
    """The reference: what a fact fold says each local day holds. Same fold the writer uses."""
    return n5.group_fold(facts, definition, lambda r: r.get("business_date"))


# =============================================================== 1. the guard, pure

def test_london_in_summer_is_a_whole_hour_zone_so_the_day_is_a_span_of_hourly_buckets():
    lo, hi = n5.local_day_span(DAY, "Europe/London")
    assert lo == datetime(2026, 9, 9, 23, tzinfo=timezone.utc)
    assert hi == datetime(2026, 9, 10, 23, tzinfo=timezone.utc)
    assert (hi - lo) == timedelta(hours=24)


def test_london_in_winter_is_midnight_utc():
    lo, hi = n5.local_day_span(date(2026, 1, 15), "Europe/London")
    assert lo == datetime(2026, 1, 15, 0, tzinfo=timezone.utc)
    assert hi == datetime(2026, 1, 16, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("day, hours", [(date(2026, 3, 29), 23), (date(2026, 10, 25), 25)])
def test_the_clock_change_days_are_23_and_25_hourly_buckets_and_still_whole_hours(day, hours):
    lo, hi = n5.local_day_span(day, "Europe/London")
    assert (hi - lo) == timedelta(hours=hours)
    assert lo == n5.hour_of(lo) and hi == n5.hour_of(hi)


def test_a_half_hour_zone_fails_the_guard():
    assert n5.local_day_span(DAY, "Asia/Kolkata") is None
    assert n5.local_day_span(DAY, "America/St_Johns") is None


def test_no_zone_means_utc_and_a_bogus_zone_falls_back_to_utc_like_business_date_does():
    assert n5.local_day_span(DAY, None) == (datetime(2026, 9, 10, tzinfo=timezone.utc),
                                            datetime(2026, 9, 11, tzinfo=timezone.utc))
    bogus = "Mars/Olympus_Mons"
    span = n5.local_day_span(DAY, bogus)
    assert span == n5.local_day_span(DAY, None)
    # The guard and business_date must resolve a zone the same way, or a derived day would not be
    # the day the facts were filed under.
    assert n2._local_date(datetime(2026, 9, 10, 23, 30, tzinfo=timezone.utc), bogus) == DAY


def test_hours_in_covers_every_bucket_the_range_touches_inclusive():
    hours = n5.hours_in(datetime(2026, 9, 10, 8, 45, tzinfo=timezone.utc),
                        datetime(2026, 9, 10, 10, 15, tzinfo=timezone.utc))
    assert hours == {datetime(2026, 9, 10, h, tzinfo=timezone.utc) for h in (8, 9, 10)}


def test_local_dates_in_are_the_tenant_days_the_range_touches():
    lo = datetime(2026, 9, 10, 22, 30, tzinfo=timezone.utc)      # 23:30 BST on the 10th
    hi = datetime(2026, 9, 10, 23, 30, tzinfo=timezone.utc)      # 00:30 BST on the 11th
    assert n5.local_dates_in(lo, hi, "Europe/London") == {DAY, DAY + timedelta(days=1)}
    assert n5.local_dates_in(lo, hi, None) == {DAY}


def test_daily_is_derivable_only_inside_the_hourly_retention_horizon():
    today = DAY
    assert n5.daily_derivable(DAY, "Europe/London", today=today)
    old = today - timedelta(days=n5.HOURLY_RETENTION_DAYS - n5.DERIVE_MARGIN_DAYS + 1)
    assert not n5.daily_derivable(old, "Europe/London", today=today), \
        "hourly rows for that day may already be dropped; deriving would delete the day's history"
    assert not n5.daily_derivable(DAY, "Asia/Kolkata", today=today)


def test_the_retention_horizon_matches_the_partition_worker():
    assert n5.HOURLY_RETENTION_DAYS == RETENTION_DAYS["analytics_hourly_rollups"]


# =============================================================== 2. the skip rule, pure

def _fact(method="ConfirmPickLine", name="Pick", at=T0):
    return {"source_transaction_id": "t1", "event_time": at, "business_date": DAY,
            "method": method, "transaction_name": name, "source_version_hash": "v"}


def test_changed_of_collects_both_sides_of_every_writing_outcome():
    outcomes = [
        dd.Outcome(dd.Action.update, ("t1", T0), _fact("ConfirmPickLine"), _fact("Other", "Count")),
        dd.Outcome(dd.Action.reverse, ("t2", T0), None, _fact("StockMove", "Move")),
        dd.Outcome(dd.Action.unchanged, ("t3", T0), _fact("Ignored"), _fact("Ignored")),
    ]
    changed = n5.changed_of(outcomes)
    assert changed.methods == frozenset({"ConfirmPickLine", "Other", "StockMove"})
    assert changed.names == frozenset({"Pick", "Count", "Move"})


def test_concerns_is_false_only_when_a_filter_excludes_every_changed_fact():
    changed = n5.Changed(methods=frozenset({"ConfirmPickLine"}), names=frozenset({"Pick"}))
    assert n5.concerns(_definition(), changed)
    assert n5.concerns(_definition(method_filter=()), changed), "no filter: everything concerns it"
    assert not n5.concerns(_definition(method_filter=("Other",)), changed)
    assert not n5.concerns(_definition(method_filter=(), transaction_filter=("Count",)), changed)
    assert n5.concerns(_definition(method_filter=("Other", "ConfirmPickLine")), changed)


# =============================================================== 3. derivation is exact

async def test_daily_merged_from_hourly_equals_daily_folded_from_facts_across_london_midnight():
    """The property the whole change rests on. Facts at 09:00Z, 10:00Z and 22:30Z are the 10th in
    London; 23:30Z is the 11th. Sum, count, min, max and the distinct sketch must all agree with a
    fold of the facts themselves."""
    await _tenant("Europe/London")
    did = await _register(_definition())
    await _plant([(T0, "A", "10.0"), (T0 + timedelta(hours=1), "A", "5.0"),
                  (datetime(2026, 9, 10, 22, 30, tzinfo=timezone.utc), "B", "7.0"),
                  (datetime(2026, 9, 10, 23, 30, tzinfo=timezone.utc), "C", "1.0")])
    await n3.consume_tenant(CC)

    facts = await _facts()
    assert len(facts) == 4
    reference = _daily_from_facts(facts, _definition())
    for measure in ("quantity", "items", "span"):
        daily = {r.business_date: r for r in await _rows(AnalyticsDailyRollup, did, measure)}
        assert set(daily) == {DAY, DAY + timedelta(days=1)}
        for (day, dims), measures in reference.items():
            roles = measures[measure]
            row = daily[day]
            assert (row.dim1, row.dim2) == dims[:2]
            if d.Role.sum_value in roles:
                assert Decimal(row.sum_value) == roles[d.Role.sum_value]
            if d.Role.count_value in roles:
                assert row.count_value == roles[d.Role.count_value]
            if d.Role.min_value in roles:
                assert Decimal(row.min_value) == roles[d.Role.min_value]
                assert Decimal(row.max_value) == roles[d.Role.max_value]
            if d.Role.distinct_sketch in roles:
                assert row.distinct_sketch == roles[d.Role.distinct_sketch]
    items = {r.business_date: hll.estimate(r.distinct_sketch)
             for r in await _rows(AnalyticsDailyRollup, did, "items")}
    assert items == {DAY: 2, DAY + timedelta(days=1): 1}, "A twice in two hours is one item"
    hourly = sorted(r.bucket_start for r in await _rows(AnalyticsHourlyRollup, did))
    assert hourly == [datetime(2026, 9, 10, h, tzinfo=timezone.utc) for h in (9, 10, 22, 23)]
    monthly = await _rows(AnalyticsMonthlyRollup, did)
    assert len(monthly) == 1 and Decimal(monthly[0].sum_value) == Decimal("23.0")


async def test_facts_are_read_once_per_run_and_by_hour_only_when_every_day_derives(monkeypatch):
    await _tenant("Europe/London")
    for name in ("a", "b", "c"):
        await _register(_definition(name=name))
    calls: list[dict] = []
    real = n5._read_dirty_facts

    async def spy(db, customer_code, hours, dates, hidden=frozenset(), since=None):
        calls.append({"hours": set(hours), "dates": set(dates), "since": since})
        return await real(db, customer_code, hours, dates, hidden, since=since)

    monkeypatch.setattr(n5, "_read_dirty_facts", spy)
    await _plant([(T0, "A", "10.0"), (T0 + timedelta(hours=1), "B", "5.0")])
    await n3.consume_tenant(CC)
    assert len(calls) == 1, f"one read for three metrics (plus the seed), got {len(calls)}"
    assert calls[0]["dates"] == set(), "every dirty day derives from hourly, so no date predicate"
    assert calls[0]["hours"] == {T0, T0 + timedelta(hours=1)}
    assert calls[0]["since"] is None, "rollups_from is applied per metric in Python, not at the read"
    for name in ("a", "b", "c"):
        async with async_session() as db:
            did = await db.scalar(select(AnalyticsMetric.id).where(
                AnalyticsMetric.customer_code == CC, AnalyticsMetric.name == name))
        assert sum(Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, did)) == Decimal("15.0")


async def test_rollups_from_is_still_honoured_when_facts_are_read_once():
    await _tenant("Europe/London")
    bounded = await _register(_definition(name="bounded", rollups_from=T0 + timedelta(minutes=30)))
    unbounded = await _register(_definition(name="unbounded"))
    await _plant([(T0, "A", "10.0"), (T0 + timedelta(hours=1), "B", "5.0")])
    await n3.consume_tenant(CC)
    assert sum(Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, bounded)) == Decimal("5.0")
    assert sum(Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, unbounded)) == Decimal("15.0")


async def test_a_metric_no_changed_fact_can_match_is_skipped():
    await _tenant("Europe/London")
    other = await _register(_definition(name="other-only", method_filter=("Other",),
                                        measures=(d.Measure(name="n", aggregation=d.Aggregation.count),)))
    picks = await _register(_definition(name="picks"))
    await _plant([(T0, "A", "10.0")])
    first = await n3.consume_tenant(CC)
    assert first["definitions_skipped"] >= 1
    assert await _rows(AnalyticsHourlyRollup, other, "n") == []
    assert len(await _rows(AnalyticsHourlyRollup, picks)) == 1


# =============================================================== 4. the fallbacks

async def test_a_half_hour_zone_reads_facts_by_date_and_is_still_correct(monkeypatch):
    await _tenant("Asia/Kolkata")
    did = await _register(_definition())
    calls: list[set] = []
    real = n5._read_dirty_facts

    async def spy(db, customer_code, hours, dates, hidden=frozenset(), since=None):
        calls.append(set(dates))
        return await real(db, customer_code, hours, dates, hidden, since=since)

    monkeypatch.setattr(n5, "_read_dirty_facts", spy)
    # 18:45Z is 00:15 IST on the 11th; 17:45Z is 23:15 IST on the 10th.
    await _plant([(datetime(2026, 9, 10, 17, 45, tzinfo=timezone.utc), "A", "10.0"),
                  (datetime(2026, 9, 10, 18, 45, tzinfo=timezone.utc), "B", "5.0")])
    await n3.consume_tenant(CC)
    assert calls and calls[0] == {DAY, DAY + timedelta(days=1)}, "the guard failed, so dates are read"
    daily = {r.business_date: Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, did)}
    assert daily == {DAY: Decimal("10.0"), DAY + timedelta(days=1): Decimal("5.0")}


async def test_a_day_past_the_hourly_horizon_reads_facts_by_date(monkeypatch):
    await _tenant("Europe/London")
    did = await _register(_definition())
    calls: list[set] = []
    real = n5._read_dirty_facts

    async def spy(db, customer_code, hours, dates, hidden=frozenset(), since=None):
        calls.append(set(dates))
        return await real(db, customer_code, hours, dates, hidden, since=since)

    monkeypatch.setattr(n5, "_read_dirty_facts", spy)
    old = datetime.now(timezone.utc) - timedelta(days=100)
    old = old.replace(minute=0, second=0, microsecond=0)
    await _plant([(old, "A", "10.0")])
    await n3.consume_tenant(CC)
    old_day = n2._local_date(old, "Europe/London")
    assert calls and old_day in calls[0]
    daily = await _rows(AnalyticsDailyRollup, did)
    assert [(r.business_date, Decimal(r.sum_value)) for r in daily] == [(old_day, Decimal("10.0"))]


async def test_a_metric_without_an_hourly_grain_still_gets_a_correct_daily_bucket():
    await _tenant("Europe/London")
    did = await _register(_definition(grains=("daily", "monthly")))
    await _plant([(T0, "A", "10.0"), (T0 + timedelta(hours=1), "B", "5.0")])
    await n3.consume_tenant(CC)
    assert await _rows(AnalyticsHourlyRollup, did) == []
    assert sum(Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, did)) == Decimal("15.0")


# =============================================================== 5. refold tickets

async def test_activating_a_metric_with_a_past_start_builds_its_rollups_from_unchanged_facts():
    """The chunk 87 defect. The facts were folded before the metric existed; nothing about them
    changes when it is activated, so the diff says unchanged - and the refold ticket is what makes the
    run fold the range anyway."""
    await _tenant("Europe/London")
    await _plant([(T0, "A", "10.0"), (T0 + timedelta(hours=1), "B", "5.0")])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        out = await api.create_metric(payload={
            "name": "late", "description": "registered after the facts", "dimensions": ["method"],
            "source": "transaction",
            "measures": [{"name": "q", "aggregation": "sum", "field": "quantity", "unit": "units"}],
            "filter": {"methods": ["ConfirmPickLine"], "transactions": []},
            "grains": ["hourly", "daily", "monthly"]}, customer=CC, db=db)
        upd = await api.update_metric(out["id"], payload={
            "status": "active", "rollups_from": (T0 - timedelta(days=1)).isoformat()},
            customer=CC, db=db)
    assert upd["tickets_published"] >= 1
    async with async_session() as db:
        tickets = (await db.execute(select(AnalyticsPendingWindow).where(
            AnalyticsPendingWindow.customer_code == CC,
            AnalyticsPendingWindow.consumed_at.is_(None)))).scalars().all()
    assert tickets and all(t.refold_rollups for t in tickets)
    stats = await n3.consume_tenant(CC)
    assert stats["unchanged"] == 2 and stats["definitions_rolled"] >= 1
    hourly = await _rows(AnalyticsHourlyRollup, uuid.UUID(out["id"]), "q")
    assert sorted(Decimal(r.sum_value) for r in hourly) == [Decimal("5.0"), Decimal("10.0")]
    daily = await _rows(AnalyticsDailyRollup, uuid.UUID(out["id"]), "q")
    assert [Decimal(r.sum_value) for r in daily] == [Decimal("15.0")]


async def test_show_off_removes_the_hidden_transaction_and_show_on_restores_it():
    """The pre-existing defect. `show` gates the rollups only, so a flip changes no fact; without a
    refold the switch was a no-op for every bucket already folded."""
    await _tenant("Europe/London")
    did = await _register(_definition())
    await _plant([(T0, "A", "10.0")])
    await n3.consume_tenant(CC)
    assert len(await _rows(AnalyticsHourlyRollup, did)) == 1

    async with async_session() as db:
        out = await api.set_transaction_switches("Pick", payload={"show": False}, customer=CC, db=db)
    assert out["tickets_published"] >= 1
    await n3.consume_tenant(CC)
    assert await _rows(AnalyticsHourlyRollup, did) == []
    assert await _rows(AnalyticsDailyRollup, did) == []
    assert await _rows(AnalyticsMonthlyRollup, did) == []

    async with async_session() as db:
        await api.set_transaction_switches("Pick", payload={"show": True}, customer=CC, db=db)
    await n3.consume_tenant(CC)
    assert [Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, did)] == [Decimal("10.0")]


async def test_publish_records_the_refold_flag_and_defaults_to_false():
    await _tenant()
    async with async_session() as db:
        await pending_windows.publish(db, CC, lo=T0, hi=T0 + timedelta(hours=1))
        await pending_windows.publish(db, CC, lo=T0, hi=T0 + timedelta(hours=1), refold=True)
        await db.commit()
        flags = sorted((await db.execute(select(AnalyticsPendingWindow.refold_rollups).where(
            AnalyticsPendingWindow.customer_code == CC))).scalars().all())
    assert flags == [False, True]


async def test_a_refold_run_skips_no_metric():
    await _tenant("Europe/London")
    other = await _register(_definition(name="other-only", method_filter=("Other",),
                                        measures=(d.Measure(name="n", aggregation=d.Aggregation.count),)))
    await _plant([(T0, "A", "10.0", "Other")])
    await n3.consume_tenant(CC)
    assert len(await _rows(AnalyticsHourlyRollup, other, "n")) == 1
    # A refold ticket over a range where nothing changed: the metric must be folded, not skipped.
    async with async_session() as db:
        await pending_windows.publish(db, CC, lo=T0 - WIDE, hi=T0 + WIDE, refold=True)
        await db.commit()
    stats = await n3.consume_tenant(CC)
    assert stats["definitions_skipped"] == 0 and stats["definitions_rolled"] >= 1


# =============================================================== 6. a fact that moves across midnight

async def test_a_fact_moved_across_local_midnight_leaves_one_day_and_lands_in_the_next():
    await _tenant("Europe/London")
    did = await _register(_definition())
    late = datetime(2026, 9, 10, 22, 30, tzinfo=timezone.utc)          # 23:30 BST, the 10th
    (tid,) = await _plant([(late, "A", "10.0")])
    await n3.consume_tenant(CC)
    assert [r.business_date for r in await _rows(AnalyticsDailyRollup, did)] == [DAY]

    moved = late + timedelta(hours=1)                                    # 00:30 BST, the 11th
    async with async_session() as db:
        await db.execute(update(LogTransaction).where(LogTransaction.id == tid)
                         .values(started_at=moved, ended_at=moved, date=moved.date(),
                                 row_fingerprint=None))
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=late - WIDE, range_end=moved + WIDE))
        await db.commit()
    stats = await n3.consume_tenant(CC)
    assert stats["inserted"] == 1 and stats["reversed"] == 1, "a new key: reverse plus insert"
    daily = {r.business_date: Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, did)}
    assert daily == {DAY + timedelta(days=1): Decimal("10.0")}, "the 10th is gone, the 11th holds it"
    hourly = [r.bucket_start for r in await _rows(AnalyticsHourlyRollup, did)]
    assert hourly == [datetime(2026, 9, 10, 23, tzinfo=timezone.utc)]


# =============================================================== 7. the single-definition entry still works

async def test_recompute_for_one_definition_derives_daily_from_hourly_when_it_can():
    """Reconcile repairs a drifted daily bucket through `recompute(dates={day})`. With hourly rows
    present the day is derived from them and the drift is gone."""
    await _tenant("Europe/London")
    did = await _register(_definition())
    await _plant([(T0, "A", "10.0")])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(update(AnalyticsDailyRollup).where(
            AnalyticsDailyRollup.definition_id == did, AnalyticsDailyRollup.measure_name == "quantity")
            .values(sum_value=Decimal("999")))
        await db.commit()
    async with async_session() as db:
        await n5.recompute(db, CC, did, _definition(), hours=set(), dates={DAY}, tz="Europe/London")
        await db.commit()
    assert [Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, did)] == [Decimal("10.0")]


async def test_reconcile_repairs_a_drifted_london_day_on_the_local_boundary():
    """The repair path calls the single-definition entry. Had it not passed the tenant zone, the
    22:30Z fact (23:30 BST, the 10th) would have been merged into a UTC day and the London daily
    bucket for the 10th would have come back wrong or empty."""
    from sqlalchemy import text
    from app.services.analytics import reconcile as rc
    from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow
    await _tenant("Europe/London")
    did = await _register(_definition())
    late = datetime(2026, 9, 10, 22, 30, tzinfo=timezone.utc)
    await _plant([(late, "A", "10.0"), (T0, "B", "5.0")])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        await db.execute(text("""UPDATE analytics_daily_rollups SET sum_value = 999
                                 WHERE customer_code = :c AND measure_name = 'quantity'"""), {"c": CC})
        await db.commit()
    window = UtcWindow(start=datetime(2026, 9, 8, 23, tzinfo=timezone.utc),
                       end=datetime(2026, 9, 11, 23, tzinfo=timezone.utc))
    async with async_session() as db:
        report = await rc.reconcile_tenant(db, CC, window=window, repair=True)
        await db.commit()
    assert report["buckets_recomputed"] >= 1
    daily = {r.business_date: Decimal(r.sum_value) for r in await _rows(AnalyticsDailyRollup, did)}
    assert daily == {DAY: Decimal("15.0")}, "both facts are the 10th in London"
    async with async_session() as db:
        after = await rc.reconcile_tenant(db, CC, window=window)
    assert [f for f in after["findings"] if f.check == "rollups_vs_facts"] == []


async def test_run_stats_report_rows_written():
    await _tenant("Europe/London")
    await _register(_definition())
    await _plant([(T0, "A", "10.0")])
    stats = await n3.consume_tenant(CC)
    assert stats["rollup_rows_written"] > 0
    assert "definitions_skipped" in stats
