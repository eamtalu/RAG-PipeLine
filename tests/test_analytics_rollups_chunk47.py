"""Chunk 47, Phase 3c: N5, the rollup folder and the grain cascade.

    Maintains the grain cascade per active definition: `facts -> hourly -> daily -> monthly`.
    **Every write is recompute-and-replace, never increment.** An additive upsert double-counts on the
    first retry.

Three properties carry the file, and each is a silent failure if broken.

**Replace, never increment.** Folding the same range twice must produce the same numbers. A cycle that
fails after writing rollups but before committing its tickets is retried, and an additive write would
add the same delta again -- so this is not a corner case, it is the consequence of any failure.

**A bucket that recomputes to nothing must be DELETED.** When the last fact in an hour is reversed,
"replace this bucket's rows" has to mean removing them. Leaving them strands the old total in every chart
with nothing to say it is stale.

**A sum of zero is not nothing.** A bucket holding only zero-unit picks sums to 0 and must still be
stored, or the zero-pick rate loses exactly the rows it is about -- and 1,333 of 16,075 live pick
confirmations are zero-unit.

Everything DB-backed here commits, because `consume_tenant` opens its own sessions.
"""

import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue
from app.persistence.models.analytics_rollup import (AnalyticsDailyRollup, AnalyticsHourlyRollup,
                                                     AnalyticsMonthlyRollup)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.consumer_cursor import ConsumerCursor
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.analytics import consume as n3
from app.services.analytics import definition as d
from app.services.analytics import diff as dd
from app.services.analytics import registry
from app.services.analytics import rollups as n5

CC = "n5-probe"
#: 14:30 UTC on a fixed day, so the hour bucket and the London business date are both unambiguous.
T0 = datetime(2026, 8, 10, 14, 30, tzinfo=timezone.utc)
HOUR = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
DAY = date(2026, 8, 10)
WIDE = timedelta(hours=6)

#: Which attribute holds each method's quantity. Taken from the contract rather than hardcoded: a
#: fixture that put QuantityPicked on a ReportCount would be quarantined as unusable and never become a
#: fact, so the test would be asserting against rows that do not exist.
from app.services.analytics.contract import QUANTITY_FIELD as _QTY_FIELD

MODELS = (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup, AnalyticsFact,
          AnalyticsQualityIssue, AnalyticsPendingWindow, AnalyticsTenantState, AnalyticsMetric,
          LogTransaction)


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.execute(delete(ConsumerCursor).where(ConsumerCursor.consumer == n3.CONSUMER))
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
        ids = []
        for spec in rows:
            t = LogTransaction(
                customer_code=CC, job_id=job.id, sealed=True,
                started_at=spec["at"], ended_at=spec["at"],
                date=spec["at"].date() if spec["at"] else None, duration_ms=spec.get("ms", 100),
                method=spec.get("method", "ConfirmPickLine"), transaction_name="Pick",
                transaction_type="002001", status=spec.get("status", LogTransactionStatus.success),
                item_number="101978", user_name="EDA", warehouse="BRI",
                attributes={_QTY_FIELD[spec.get("method", "ConfirmPickLine")]:
                            spec.get("qty", "10.0")})
            if "id" in spec:
                t.id = spec["id"]
            db.add(t)
            ids.append(t)
        await db.flush()
        if ticket:
            db.add(AnalyticsPendingWindow(customer_code=CC, range_start=T0 - WIDE,
                                          range_end=T0 + WIDE))
        await db.commit()
        return [t.id for t in ids]


async def _rollups(model, measure="quantity"):
    async with async_session() as db:
        return list((await db.execute(select(model).where(
            model.customer_code == CC, model.measure_name == measure))).scalars().all())


async def _sum(model, measure="quantity") -> Decimal:
    rows = await _rollups(model, measure)
    return sum((Decimal(r.sum_value) for r in rows if r.sum_value is not None), Decimal(0))


async def _count(model, measure) -> int:
    rows = await _rollups(model, measure)
    return sum(r.count_value or 0 for r in rows)


async def _reticket(lo=None, hi=None):
    async with async_session() as db:
        db.add(AnalyticsPendingWindow(customer_code=CC, range_start=lo or T0 - WIDE,
                                      range_end=hi or T0 + WIDE))
        await db.commit()


async def _retire(ids):
    async with async_session() as db:
        await db.execute(delete(LogTransaction).where(LogTransaction.id.in_(list(ids))))
        await db.commit()


# ==================================================== pure bucketing
def test_the_hour_bucket_floors_to_the_utc_hour():
    assert n5.hour_of(T0) == HOUR
    assert n5.hour_of(datetime(2026, 8, 10, 14, 59, 59, 999999, tzinfo=timezone.utc)) == HOUR


def test_the_hour_bucket_converts_to_utc_first():
    """The hourly grain is a machine-time axis. Bucketing a +02:00 instant by its local hour would put
    the same moment in two different buckets depending on who wrote it."""
    from zoneinfo import ZoneInfo
    berlin = T0.astimezone(ZoneInfo("Europe/Berlin"))
    assert berlin.hour == 16, "the fixture must actually be in a different local hour"
    assert n5.hour_of(berlin) == HOUR


@pytest.mark.parametrize("day,start", [
    (date(2026, 8, 10), date(2026, 8, 1)), (date(2026, 1, 1), date(2026, 1, 1)),
    (date(2026, 12, 31), date(2026, 12, 1))])
def test_the_month_bucket_is_a_pure_function_of_the_business_date(day, start):
    """Which is what makes monthly foldable from daily EXACTLY -- no daily bucket can straddle two
    months, unlike an hour straddling two local dates."""
    assert n5.month_of(day) == start


@pytest.mark.parametrize("start,end", [
    (date(2026, 1, 1), date(2026, 1, 31)), (date(2026, 2, 1), date(2026, 2, 28)),
    (date(2024, 2, 1), date(2024, 2, 29)), (date(2026, 12, 1), date(2026, 12, 31))])
def test_the_month_end_handles_february_and_december(start, end):
    """December is the case a naive `month + 1` gets wrong, and February the one a length table does."""
    assert n5._month_end(start) == end


# ==================================================== which buckets are dirty
def _fact(at=T0, day=DAY, qty="10.0", version="v1"):
    return {"source_transaction_id": "t1", "event_time": at, "business_date": day,
            "source_version_hash": version, "method": "ConfirmPickLine", "status": "success",
            "quantity": Decimal(qty), "quantity_classification": "pick", "revision": 1}


def test_both_sides_of_a_moved_fact_are_dirty():
    """A rebuild can move a transaction's event_time. Recomputing only the bucket it arrived in would
    leave the bucket it left holding a contribution that exists nowhere."""
    old = _fact(at=T0, day=DAY)
    new = _fact(at=T0 + timedelta(hours=3), day=DAY + timedelta(days=1), version="v2")
    hours, dates = n5.dirty_buckets([dd.Outcome(dd.Action.update, ("t1", T0), new, old)])
    assert hours == {HOUR, HOUR + timedelta(hours=3)}
    assert dates == {DAY, DAY + timedelta(days=1)}


def test_an_unchanged_outcome_dirties_nothing():
    """What keeps the 98.7% rebuild case free all the way through to the rollups, rather than only as
    far as the fact table."""
    f = _fact()
    hours, dates = n5.dirty_buckets([dd.Outcome(dd.Action.unchanged, ("t1", T0), f, f)])
    assert hours == set() and dates == set()


def test_a_reversal_dirties_the_bucket_it_leaves():
    f = _fact()
    hours, dates = n5.dirty_buckets([dd.Outcome(dd.Action.reverse, ("t1", T0), None, f)])
    assert hours == {HOUR} and dates == {DAY}


def test_a_fact_with_no_event_time_dirties_no_bucket():
    """Both bucket columns are NOT NULL. Such a row is excluded from every grain rather than bucketed
    into a DEFAULT partition retention could never reclaim."""
    f = {**_fact(), "event_time": None, "business_date": None}
    hours, dates = n5.dirty_buckets([dd.Outcome(dd.Action.insert, ("t1", None), f, None)])
    assert hours == set() and dates == set()


# ==================================================== the cascade, end to end
async def test_folding_writes_all_three_grains():
    await _plant([{"at": T0, "qty": "10.0"}, {"at": T0 + timedelta(minutes=5), "qty": "5.0"}])
    stats = await n3.consume_tenant(CC)

    assert stats["definitions_rolled"] == 1
    assert await _sum(AnalyticsHourlyRollup) == Decimal(15)
    assert await _sum(AnalyticsDailyRollup) == Decimal(15)
    assert await _sum(AnalyticsMonthlyRollup) == Decimal(15)


async def test_the_three_grains_agree_with_each_other_and_with_the_facts():
    """The cascade's only real contract. A monthly total that disagrees with its own days is the failure
    nobody notices until someone compares two screens."""
    await _plant([{"at": T0, "qty": "3.5"}, {"at": T0 + timedelta(minutes=5), "qty": "0.0"},
                  {"at": T0 + timedelta(hours=2), "qty": "1.25"}])
    await n3.consume_tenant(CC)

    async with async_session() as db:
        facts = await db.scalar(select(func.coalesce(func.sum(AnalyticsFact.quantity), 0)).where(
            AnalyticsFact.customer_code == CC))
    assert Decimal(facts) == Decimal("4.75")
    for model in (AnalyticsHourlyRollup, AnalyticsDailyRollup, AnalyticsMonthlyRollup):
        assert await _sum(model) == Decimal("4.75"), model.__tablename__


async def test_a_measure_gets_its_own_row_per_bucket():
    """Correction log C5: a definition needing one sum and two counts cannot share one set of role
    columns, so consumption emits three rows per bucket rather than one."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        names = (await db.execute(select(AnalyticsHourlyRollup.measure_name).where(
            AnalyticsHourlyRollup.customer_code == CC))).scalars().all()
    assert sorted(names) == ["attempt_count", "pick_count", "quantity"]


async def test_the_hour_and_the_local_day_are_different_axes():
    """23:30 UTC is the next day in London, so the same fact sits in one UTC hour and the FOLLOWING
    business date. This is why daily is folded from the facts and not from hourly."""
    async with async_session() as db:
        from app.persistence.models.customer import Customer
        db.add(Customer(customer_code=CC, name="probe", timezone="Europe/London"))
        await db.commit()
    try:
        late = datetime(2026, 8, 10, 23, 30, tzinfo=timezone.utc)
        await _plant([{"at": late, "qty": "10.0"}], ticket=False)
        await _reticket(late - timedelta(hours=1), late + timedelta(hours=1))
        await n3.consume_tenant(CC)

        hourly = await _rollups(AnalyticsHourlyRollup)
        daily = await _rollups(AnalyticsDailyRollup)
        assert hourly[0].bucket_start == datetime(2026, 8, 10, 23, tzinfo=timezone.utc)
        assert daily[0].business_date == date(2026, 8, 11), "the LOCAL day, one later"
        assert await _sum(AnalyticsMonthlyRollup) == Decimal(10)
    finally:
        async with async_session() as db:
            from app.persistence.models.customer import Customer
            await db.execute(delete(Customer).where(Customer.customer_code == CC))
            await db.commit()


# ==================================================== replace, never increment
async def test_folding_the_same_range_twice_does_not_double_the_total():
    """THE property. A cycle that fails after writing rollups but before committing its tickets is
    retried, so an additive write would double on the first failure of any kind."""
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    assert await _sum(AnalyticsHourlyRollup) == Decimal(10)

    await _reticket()
    await n3.consume_tenant(CC)
    assert await _sum(AnalyticsHourlyRollup) == Decimal(10), "incremented instead of replaced"
    assert await _sum(AnalyticsMonthlyRollup) == Decimal(10)


async def test_a_changed_quantity_replaces_rather_than_accumulates():
    txn = uuid.uuid4()
    await _plant([{"at": T0, "qty": "10.0", "id": txn}])
    await n3.consume_tenant(CC)

    await _retire([txn])
    await _plant([{"at": T0, "qty": "4.0", "id": txn}])
    await n3.consume_tenant(CC)

    assert await _sum(AnalyticsHourlyRollup) == Decimal(4), "not 14, and not 10"
    assert await _sum(AnalyticsDailyRollup) == Decimal(4)
    assert await _sum(AnalyticsMonthlyRollup) == Decimal(4)


async def test_a_bucket_that_recomputes_to_nothing_is_deleted():
    """The easy one to miss. "Replace this bucket's rows" has to mean removing them when the last fact
    goes, or the old total stays in every chart with nothing to say it is stale."""
    ids = await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    assert await _rollups(AnalyticsHourlyRollup)

    await _retire(ids)
    await _reticket()
    await n3.consume_tenant(CC)

    assert await _rollups(AnalyticsHourlyRollup) == [], "an emptied hour must be removed"
    assert await _rollups(AnalyticsDailyRollup) == []
    assert await _rollups(AnalyticsMonthlyRollup) == []


async def test_a_dimension_combination_that_disappears_is_removed():
    """A merge can collapse two methods' rows into one. Keying the delete to the RECOMPUTED rows instead
    of to the bucket would leave the departed combination behind forever."""
    ids = await _plant([{"at": T0, "qty": "10.0", "method": "ConfirmPickLine"},
                        {"at": T0 + timedelta(minutes=1), "qty": "3.0", "method": "ReportCount"}])
    await n3.consume_tenant(CC)
    assert len(await _rollups(AnalyticsHourlyRollup)) == 2

    await _retire([ids[1]])
    await _reticket()
    await n3.consume_tenant(CC)

    rows = await _rollups(AnalyticsHourlyRollup)
    assert len(rows) == 1 and rows[0].dim1 == "ConfirmPickLine"


async def test_only_the_dirty_buckets_are_touched():
    """A neighbouring hour must not be recomputed, let alone deleted, by a ticket that does not mention
    it. Otherwise one narrow ticket could wipe a day."""
    await _plant([{"at": T0, "qty": "10.0"}], ticket=False)
    await _reticket(T0 - timedelta(minutes=30), T0 + timedelta(minutes=30))
    await n3.consume_tenant(CC)
    untouched = (await _rollups(AnalyticsHourlyRollup))[0].computed_at

    far = T0 + timedelta(hours=5)
    await _plant([{"at": far, "qty": "1.0"}], ticket=False)
    await _reticket(far - timedelta(minutes=30), far + timedelta(minutes=30))
    await n3.consume_tenant(CC)

    rows = {r.bucket_start: r for r in await _rollups(AnalyticsHourlyRollup)}
    assert len(rows) == 2
    assert rows[HOUR].computed_at == untouched, "the earlier hour was recomputed for no reason"
    assert await _sum(AnalyticsHourlyRollup) == Decimal(11)


# ==================================================== zero is not nothing
async def test_a_bucket_of_only_zero_picks_is_still_stored():
    """1,333 of 16,075 live pick confirmations record zero units. Dropping a bucket because its SUM is
    zero would delete exactly the rows the zero-pick rate is about."""
    await _plant([{"at": T0, "qty": "0.0"}])
    await n3.consume_tenant(CC)

    rows = await _rollups(AnalyticsHourlyRollup)
    assert len(rows) == 1
    assert Decimal(rows[0].sum_value) == Decimal(0)
    assert rows[0].count_value == 1, "one observation, summing to zero"
    assert await _count(AnalyticsHourlyRollup, "attempt_count") == 1
    assert await _count(AnalyticsHourlyRollup, "pick_count") == 0


async def test_the_zero_pick_rate_is_reconstructible_from_the_stored_counters():
    """Invariant 8: a rollup stores additive components, never finished answers. The rate is computed at
    read time from two counts, which is what lets twelve months compose into a year."""
    await _plant([{"at": T0, "qty": "0.0"}, {"at": T0 + timedelta(minutes=1), "qty": "5.0"}])
    await n3.consume_tenant(CC)

    picks = await _count(AnalyticsDailyRollup, "pick_count")
    attempts = await _count(AnalyticsDailyRollup, "attempt_count")
    assert (attempts - picks) / attempts == 0.5


# ==================================================== the status filter reaches the rollups
async def test_an_errored_pick_never_reaches_a_rollup():
    """The 3b finding, verified at the level the charts actually read. The FACT still records its
    quantity -- that is what happened -- but consumption must not count it."""
    await _plant([{"at": T0, "qty": "10.0"},
                  {"at": T0 + timedelta(minutes=1), "qty": "10.0",
                   "status": LogTransactionStatus.error},
                  {"at": T0 + timedelta(minutes=2), "qty": "10.0",
                   "status": LogTransactionStatus.incomplete}])
    await n3.consume_tenant(CC)

    async with async_session() as db:
        facts = await db.scalar(select(func.coalesce(func.sum(AnalyticsFact.quantity), 0)).where(
            AnalyticsFact.customer_code == CC))
    assert Decimal(facts) == Decimal(30), "the facts record what happened"
    assert await _sum(AnalyticsHourlyRollup) == Decimal(10), "the rollup counts what completed"
    assert await _count(AnalyticsHourlyRollup, "attempt_count") == 1


# ==================================================== registry-driven, not hardcoded
async def test_the_seed_definition_is_created_as_a_registry_row():
    await _plant([{"at": T0, "qty": "10.0"}])
    await n3.consume_tenant(CC)
    async with async_session() as db:
        rows = list((await db.execute(select(AnalyticsMetric).where(
            AnalyticsMetric.customer_code == CC))).scalars().all())
    assert [r.name for r in rows] == ["consumption"]
    assert rows[0].status == "active"


async def test_a_second_definition_folds_with_no_code_change():
    """The property the whole user-configurable design rests on. This metric does not exist anywhere in
    the codebase -- it is inserted as DATA, and the same worker folds it."""
    await _plant([{"at": T0, "qty": "10.0", "ms": 250},
                  {"at": T0 + timedelta(minutes=1), "qty": "1.0", "ms": 750}], ticket=False)
    invented = d.MetricDefinition(
        name="how-slow-are-picks", dimensions=("method", "user_name"),
        measures=(d.Measure("duration", d.Aggregation.average, field="duration_ms"),),
        grains=("hourly", "daily"), status=d.Status.active)
    assert d.validate(invented) == []
    async with async_session() as db:
        db.add(AnalyticsMetric(**registry.to_row(invented, customer_code=CC)))
        await db.commit()
    await _reticket()

    stats = await n3.consume_tenant(CC)
    assert stats["definitions_rolled"] == 2, "the seed plus the invented one"

    rows = await _rollups(AnalyticsHourlyRollup, "duration")
    assert len(rows) == 1
    # An average is stored as sum + count, never as a finished answer.
    assert Decimal(rows[0].sum_value) == Decimal(1000) and rows[0].count_value == 2
    assert Decimal(rows[0].sum_value) / rows[0].count_value == Decimal(500)


async def test_an_average_has_no_column_to_be_written_into():
    """Invariant 8 made structural. There is no `average` column, so "never store a finished answer"
    stops being a rule someone has to catch in review."""
    assert not hasattr(AnalyticsHourlyRollup, "average")
    assert {r.value for r in d.Role} <= {c.name for c in AnalyticsHourlyRollup.__table__.columns}


async def test_an_inactive_definition_is_not_folded():
    await _plant([{"at": T0, "qty": "10.0"}], ticket=False)
    async with async_session() as db:
        db.add(AnalyticsMetric(**registry.to_row(
            d.MetricDefinition(name="draft-metric", dimensions=("method",),
                               measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",),
                               status=d.Status.draft), customer_code=CC)))
        await db.commit()
    await _reticket()
    stats = await n3.consume_tenant(CC)
    assert stats["definitions_rolled"] == 1, "only the active seed"


async def test_an_invalid_registry_row_is_skipped_rather_than_stopping_the_others():
    """A1's reasoning applied to definitions: one malformed row must not stop every other metric for
    that tenant from folding."""
    await _plant([{"at": T0, "qty": "10.0"}], ticket=False)
    async with async_session() as db:
        broken = registry.to_row(
            d.MetricDefinition(name="broken", dimensions=("no_such_field",),
                               measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",),
                               status=d.Status.active), customer_code=CC)
        db.add(AnalyticsMetric(**broken))
        await db.commit()
    await _reticket()

    stats = await n3.consume_tenant(CC)
    assert stats["definitions_rolled"] == 1, "the seed still folded"
    assert await _sum(AnalyticsDailyRollup) == Decimal(10)


# ==================================================== registry round-trip
def test_a_definition_survives_a_round_trip_through_its_stored_form():
    """A lossy serialisation is worse than a failing one: the fold would quietly use a different filter
    from the one the user saved, and the chart would be confidently wrong."""
    row = AnalyticsMetric(**registry.to_row(d.CONSUMPTION, customer_code=CC))
    back = registry.from_row(row)
    assert back.name == d.CONSUMPTION.name
    assert back.dimensions == d.CONSUMPTION.dimensions
    assert back.grains == d.CONSUMPTION.grains
    assert back.method_filter == d.CONSUMPTION.method_filter
    assert back.measures == d.CONSUMPTION.measures, "measures must survive exactly"


def test_the_status_filter_survives_the_round_trip():
    """The 3b fix is only real if it is still there after being stored. A dropped `statuses` set would
    put errored picks back into every total, silently."""
    row = AnalyticsMetric(**registry.to_row(d.CONSUMPTION, customer_code=CC))
    assert registry.from_row(row).measures[0].statuses == frozenset({"success"})


def test_the_stored_form_is_ordering_stable():
    """Sets are dumped sorted, so two identical definitions compare equal across processes."""
    a = registry.to_row(d.CONSUMPTION, customer_code=CC)
    b = registry.to_row(d.CONSUMPTION, customer_code=CC)
    assert a == b


# ==================================================== the transaction boundary
def test_the_folder_does_not_commit():
    """The rollups must land in the same transaction as the facts they summarise. A chart that disagrees
    with the fact table for the length of a gap between commits is bad; one that disagrees forever
    because the second commit failed is what nobody finds until a number is questioned."""
    import inspect
    code = [ln for ln in inspect.getsource(n5).splitlines()
            if "commit(" in ln and not ln.strip().startswith("#")]
    assert code == [], f"N5 must leave the transaction boundary to its caller: {code}"


def test_monthly_is_folded_after_daily_has_been_replaced():
    """Monthly reads the level below, so folding it from a stale daily level would produce a month that
    disagrees with its own days. 18y moved the grain cascade into `_fold_grains`, SHARED by
    `recompute` (transaction grain) and `recompute_records` (record grain), so this ordering pin now
    protects both grains at once."""
    import inspect
    src = inspect.getsource(n5._fold_grains)
    assert src.index('"daily" in definition.grains') < src.index('"monthly" in definition.grains')


def test_a_rollup_failure_fails_the_run_rather_than_passing_quietly():
    """Deliberately unlike quarantine. A row nobody understands must not halt a tenant (A1), but a
    rollup that silently did not update is a chart that is wrong with nothing to say so -- worse than a
    ticket that stays open and retries."""
    import inspect
    src = inspect.getsource(n3._roll_up)
    assert "raise" in src
