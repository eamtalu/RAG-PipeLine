"""Chunk 22 (step 2 of docs/plan/2026-08-05_20-32_daily-partitioning.md): bound the queries that
carry no partition key.

Once `log_entries` / `log_transactions` / `log_entry_assignment` are range-partitioned by UTC day, a
query with no predicate on the partition-key COLUMN touches all 60 partitions. PostgreSQL will not
infer the key from anything else - not from `log_transactions.date` (which is a customer-LOCAL day,
not the UTC instant the partition is cut on), and not from a join on `entry_id`. So every hot read
has to state its time bound explicitly, and every bound has to be provably wide enough not to drop a
row that the unbounded query would have returned.

That "provably wide enough" is the whole risk here, and it is what most of this file tests:

- an entry with a NULL timestamp lives in the DEFAULT partition, and `BETWEEN` excludes NULLs, so a
  naive bound silently drops it from the feed;
- `log_transactions.date` is derived from `started_at` through the customer's display timezone, so if
  that timezone is ever CHANGED the local day and the UTC instant stop lining up - the window has to
  absorb that or old rows vanish from the day view;
- a tenant whose newest log is older than the lookback must still seal, so the bounded probe in
  `_cutoffs` needs a fallback rather than reporting "no entries".

Covered here: the shared window arithmetic (`time_bounds`), the bounded feed read
(`assignments.load_entries`), the bounded seal cutoff (`derive_transactions._cutoffs`), the day-view
and date-range-delete windows, and a source guard that no caller of `load_entries` is left unbounded.
"""

import ast
import pathlib
import uuid
from datetime import date as date_type, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, text

from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import assignments, time_bounds
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.settings import settings

CC = "TEST_CHUNK22"
REPO = pathlib.Path(__file__).resolve().parent.parent


def _utc(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)


# ======================================================= time_bounds: from_instants
def test_from_instants_spans_min_to_max_with_an_exclusive_end():
    """The end is EXCLUSIVE, so it must sit strictly after the newest instant - an inclusive-looking
    `< max` bound would drop the very last entry of a transaction."""
    w = time_bounds.from_instants([_utc(2026, 6, 10, 9), _utc(2026, 6, 10, 11), _utc(2026, 6, 10, 10)])
    assert w.start == _utc(2026, 6, 10, 9)
    assert w.end > _utc(2026, 6, 10, 11)


def test_from_instants_pads_both_sides():
    w = time_bounds.from_instants([_utc(2026, 6, 10, 9)], pad=timedelta(hours=2))
    assert w.start == _utc(2026, 6, 10, 7)
    assert w.end >= _utc(2026, 6, 10, 11)


def test_from_instants_ignores_nones_but_returns_none_when_all_are_none():
    """A transaction with no started_at/ended_at yields NO window rather than a bogus one - the
    caller must fall back to unbounded rather than silently filter everything out."""
    assert time_bounds.from_instants([None, _utc(2026, 6, 10, 9), None]).start == _utc(2026, 6, 10, 9)
    assert time_bounds.from_instants([None, None]) is None
    assert time_bounds.from_instants([]) is None


def test_from_instants_normalises_naive_values_as_utc():
    """Values read back from asyncpg can arrive naive; treating one as local time would shift the
    window by the host offset and cut real rows out of the feed."""
    w = time_bounds.from_instants([datetime(2026, 6, 10, 9)])
    assert w.start == _utc(2026, 6, 10, 9)


# ======================================================= time_bounds: from_local_dates
def test_local_day_maps_to_the_utc_instants_that_day_actually_covers():
    """A London summer day starts at 23:00 UTC the previous day. Cutting partitions on UTC while
    filtering on a LOCAL date is exactly the mismatch that prunes nothing."""
    w = time_bounds.from_local_dates(date_type(2026, 7, 15), date_type(2026, 7, 15),
                                     "Europe/London", pad=timedelta(0))
    assert w.start == _utc(2026, 7, 14, 23)
    assert w.end == _utc(2026, 7, 15, 23)


def test_a_dst_shortened_local_day_is_23_hours_not_24():
    """Spring-forward days are 23 hours long. Hard-coding 24 would run the window an hour past the
    day on one side and be wrong for every following day of the year."""
    spring_forward = next(
        d for d in (date_type(2026, 3, 1) + timedelta(days=i) for i in range(31))
        if (time_bounds.from_local_dates(d, d, "Europe/London", pad=timedelta(0)).end
            - time_bounds.from_local_dates(d, d, "Europe/London", pad=timedelta(0)).start)
        == timedelta(hours=23)
    )
    w = time_bounds.from_local_dates(spring_forward, spring_forward, "Europe/London", pad=timedelta(0))
    assert w.end - w.start == timedelta(hours=23)


def test_local_dates_pad_absorbs_a_customer_timezone_change():
    """`log_transactions.date` was computed with whatever display zone the customer had AT THE TIME.
    If that zone is later changed, a zero-pad window derived from the NEW zone can sit beside the
    stored instants and the day view would go blank. The pad must exceed the widest real offset
    difference (UTC-12 to UTC+14 = 26 hours)."""
    d = date_type(2026, 7, 15)
    w = time_bounds.from_local_dates(d, d, "Europe/London")
    assert w.start <= _utc(2026, 7, 14, 23) - timedelta(hours=26)
    assert w.end >= _utc(2026, 7, 15, 23) + timedelta(hours=26)


def test_open_ended_local_range_bounds_only_the_side_it_can():
    """The date-range delete allows an open end. Inventing a bound for the missing side would delete
    the wrong rows; leaving it None is honest and simply prunes less."""
    lo = time_bounds.from_local_dates(None, date_type(2026, 7, 15), "Europe/London")
    assert lo.start is None and lo.end is not None
    hi = time_bounds.from_local_dates(date_type(2026, 7, 15), None, "Europe/London")
    assert hi.start is not None and hi.end is None
    assert time_bounds.from_local_dates(None, None, "Europe/London") is None


def test_local_range_spans_from_the_first_day_to_the_end_of_the_last():
    w = time_bounds.from_local_dates(date_type(2026, 7, 15), date_type(2026, 7, 17),
                                     "Europe/London", pad=timedelta(0))
    assert w.start == _utc(2026, 7, 14, 23)
    assert w.end == _utc(2026, 7, 17, 23)


def test_an_unknown_timezone_name_falls_back_instead_of_raising():
    """A bad tz string on a customer row must not 500 the feed."""
    d = date_type(2026, 7, 15)
    assert time_bounds.from_local_dates(d, d, "Not/AZone") is not None


# ======================================================= time_bounds: the predicate
def test_covers_emits_a_null_branch_only_when_asked():
    """`timestamp BETWEEN a AND b` is FALSE for NULL. Entries with no parsable timestamp must still
    render, so the feed asks for the NULL branch; a query that provably cannot match NULLs skips it
    and keeps the DEFAULT partition out of the plan."""
    w = time_bounds.from_instants([_utc(2026, 6, 10, 9)])
    assert "IS NULL" in str(w.covers(LogEntry.timestamp, include_null=True))
    assert "IS NULL" not in str(w.covers(LogEntry.timestamp, include_null=False))


def test_covers_is_half_open_so_adjacent_windows_cannot_double_count():
    w = time_bounds.from_instants([_utc(2026, 6, 10, 9)])
    sql = str(w.covers(LogEntry.timestamp, include_null=False))
    assert ">=" in sql and "<" in sql and "<=" not in sql


# ======================================================= DB fixtures
async def _cleanup(db):
    await db.execute(delete(LogEntryAssignment).where(LogEntryAssignment.customer_code == CC))
    await db.execute(delete(LogEntry).where(LogEntry.customer_code == CC))
    await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == CC))
    await db.execute(delete(Job).where(Job.customer_code == CC))
    await db.flush()


async def _job(db):
    j = Job(customer_code=CC, filename="c22.log", storage_key=f"{CC}/{uuid.uuid4().hex}/c22.log",
            document_type="transaction_log", status="completed")
    db.add(j)
    await db.flush()
    return j


async def _entry(db, job, ts, line):
    e = LogEntry(customer_code=CC, job_id=job.id, timestamp=ts, source_file="c22.log",
                 line_number=line, level="INFO", raw_body=f"line {line}",
                 entry_hash=uuid.uuid4().hex)
    db.add(e)
    await db.flush()
    return e


async def _txn_with(db, job, entries, *, tid=None):
    tid = tid or uuid.uuid4()
    stamps = [e.timestamp for e in entries if e.timestamp is not None]
    db.add(LogTransaction(id=tid, customer_code=CC, job_id=job.id, sealed=False,
                          started_at=min(stamps) if stamps else None,
                          ended_at=max(stamps) if stamps else None,
                          date=min(stamps).date() if stamps else None))
    await db.flush()
    await assignments.write(db, transaction_id=tid, entries=entries, customer_code=CC)
    await db.flush()
    return tid


# ======================================================= load_entries: the feed hot path
async def test_bounded_load_returns_exactly_what_the_unbounded_load_returns(db):
    """The no-regression guard. A window derived from the transaction's own started_at/ended_at is
    derived FROM the entries, so it must never exclude one of them."""
    await _cleanup(db)
    job = await _job(db)
    entries = [await _entry(db, job, _utc(2026, 6, 10, 9, 0, i), i) for i in range(5)]
    tid = await _txn_with(db, job, entries)

    unbounded = await assignments.load_entries(db, [tid], limit=100)
    win = time_bounds.from_instants([_utc(2026, 6, 10, 9, 0, 0), _utc(2026, 6, 10, 9, 0, 4)])
    bounded = await assignments.load_entries(db, [tid], limit=100, window=win)
    assert [e.id for e, _t, _s in bounded] == [e.id for e, _t, _s in unbounded]
    assert len(bounded) == 5


async def test_the_window_is_really_applied_to_the_entry_timestamp(db):
    """Proves the bound is not decorative: an entry outside the window disappears. If this passes
    while the previous test also passes, the window is both effective and correctly derived."""
    await _cleanup(db)
    job = await _job(db)
    inside = await _entry(db, job, _utc(2026, 6, 10, 9, 0, 0), 1)
    outside = await _entry(db, job, _utc(2026, 6, 20, 9, 0, 0), 2)
    tid = await _txn_with(db, job, [inside, outside])

    narrow = time_bounds.from_instants([_utc(2026, 6, 10, 9, 0, 0)])
    got = await assignments.load_entries(db, [tid], limit=100, window=narrow)
    assert [e.id for e, _t, _s in got] == [inside.id]


async def test_a_null_timestamp_entry_survives_the_bound(db):
    """The regression this file exists to prevent. `entry_ts` is NULL for an entry whose timestamp
    could not be parsed; a plain range predicate is FALSE for NULL, so without an explicit NULL
    branch the entry vanishes from the rendered transaction with no error anywhere."""
    await _cleanup(db)
    job = await _job(db)
    timed = await _entry(db, job, _utc(2026, 6, 10, 9, 0, 0), 1)
    untimed = await _entry(db, job, None, 2)
    tid = await _txn_with(db, job, [untimed, timed])

    win = time_bounds.from_instants([_utc(2026, 6, 10, 9, 0, 0)])
    got = await assignments.load_entries(db, [tid], limit=100, window=win)
    assert {e.id for e, _t, _s in got} == {timed.id, untimed.id}


async def test_the_bound_also_lands_on_the_assignment_table(db):
    """`log_entry_assignment` is partitioned by `entry_ts` too, so bounding only `log_entries` would
    still scan all 60 assignment partitions to resolve the join."""
    await _cleanup(db)
    job = await _job(db)
    e = await _entry(db, job, _utc(2026, 6, 10, 9), 1)
    tid = await _txn_with(db, job, [e])
    win = time_bounds.from_instants([_utc(2026, 6, 10, 9)])

    stmt = assignments.entries_stmt([tid], limit=10, window=win)
    plan = "\n".join(r[0] for r in (await db.execute(
        text("EXPLAIN " + str(stmt.compile(db.bind, compile_kwargs={"literal_binds": True}))))).all())
    assert "entry_ts" in plan, plan


async def test_load_entries_without_a_window_is_unchanged(db):
    """A transaction with no timestamps at all yields no window, and the caller must still get its
    entries rather than an empty timeline."""
    await _cleanup(db)
    job = await _job(db)
    e = await _entry(db, job, None, 1)
    tid = await _txn_with(db, job, [e])
    assert time_bounds.from_instants([None]) is None
    got = await assignments.load_entries(db, [tid], limit=100, window=None)
    assert [x.id for x, _t, _s in got] == [e.id]


def test_every_load_entries_caller_passes_a_window():
    """Source guard. The window defaults to None so an un-migrated caller keeps working - which also
    means a forgotten call site would stay silently unbounded forever. This fails if one appears."""
    unbounded = []
    for path in (REPO / "app").rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "load_entries"
                    and not any(k.arg == "window" for k in node.keywords)):
                unbounded.append(f"{path.relative_to(REPO)}:{node.lineno}")
    assert not unbounded, f"load_entries called without an explicit window: {unbounded}"


# ======================================================= _cutoffs
async def test_cutoffs_are_unchanged_for_a_live_tenant(db):
    """No-regression: the seal/abandon cutoffs must still be measured from the newest entry."""
    await _cleanup(db)
    job = await _job(db)
    newest = datetime.now(timezone.utc) - timedelta(minutes=5)
    await _entry(db, job, newest - timedelta(hours=1), 1)
    await _entry(db, job, newest, 2)

    seal, abandon = await dt._cutoffs(db, CC)
    assert seal == newest - timedelta(seconds=settings.log_seal_window_seconds)
    assert abandon == newest - timedelta(seconds=settings.log_abandon_window_seconds)


async def test_cutoffs_fall_back_when_every_entry_predates_the_lookback(db):
    """A bounded probe alone would report "no entries" for a tenant whose ingestion stopped, or one
    importing back-dated logs - and nothing would ever seal. The probe must fall back to the full
    scan rather than return None."""
    await _cleanup(db)
    job = await _job(db)
    ancient = datetime.now(timezone.utc) - timedelta(days=settings.log_cutoff_lookback_days + 30)
    await _entry(db, job, ancient, 1)

    seal, abandon = await dt._cutoffs(db, CC)
    assert seal == ancient - timedelta(seconds=settings.log_seal_window_seconds)
    assert abandon is not None


async def test_cutoffs_return_none_for_a_tenant_with_no_entries(db):
    await _cleanup(db)
    assert await dt._cutoffs(db, "TEST_CHUNK22_EMPTY") == (None, None)


async def test_the_cutoff_probe_is_bounded_by_timestamp(db):
    """The fast path must carry a predicate on the partition key, or it prunes nothing."""
    stmt = dt._recent_max_ts_stmt(CC)
    plan = "\n".join(r[0] for r in (await db.execute(
        text("EXPLAIN " + str(stmt.compile(db.bind, compile_kwargs={"literal_binds": True}))))).all())
    assert "timestamp" in plan.lower(), plan


def test_the_cutoff_lookback_is_generous_enough_to_be_the_normal_path():
    """If the lookback were shorter than the abandon window, a tenant that is merely quiet would take
    the slow fallback on every single regroup cycle."""
    assert settings.log_cutoff_lookback_days >= 1
    assert (settings.log_cutoff_lookback_days * 86400) > settings.log_abandon_window_seconds


# ======================================================= the day view and the date-range delete
def test_the_day_view_window_contains_the_whole_local_day():
    """`view_transactions` filters on the LOCAL `date`; the added window must cover every UTC instant
    that could carry that local date, or transactions disappear from the feed."""
    d = date_type(2026, 7, 15)
    w = time_bounds.from_local_dates(d, d, "Europe/London")
    assert w.start <= _utc(2026, 7, 14, 23)
    assert w.end >= _utc(2026, 7, 15, 23)


def test_the_day_view_states_the_partition_key_and_not_only_the_local_date():
    """The point of the change. Filtering on `date` alone is what prunes nothing, so the conditions
    must name `started_at` as well - the previous test only proves nothing BROKE, not that the bound
    was ever added."""
    sql = " ".join(str(c) for c in dt_day_conds(CC, date_type(2026, 7, 15), "Europe/London"))
    assert "log_transactions.date" in sql
    assert "log_transactions.started_at" in sql


async def test_the_day_view_still_returns_a_transaction_at_the_local_midnight_edge(db):
    """The end of a London summer day is 22:59:59 UTC. A window built on UTC midnight instead of the
    local day would drop the last hour of every day."""
    await _cleanup(db)
    job = await _job(db)
    e = await _entry(db, job, _utc(2026, 7, 15, 22, 59, 59), 1)
    tid = await _txn_with(db, job, [e])
    await db.execute(text(
        "UPDATE log_transactions SET date = :d WHERE id = :i"
    ), {"d": date_type(2026, 7, 15), "i": tid})
    await db.flush()

    conds = dt_day_conds(CC, date_type(2026, 7, 15), "Europe/London")
    got = (await db.execute(select(LogTransaction.id).where(*conds))).scalars().all()
    assert tid in got


def test_the_date_range_delete_bounds_both_sides_it_is_given():
    lo, hi = date_type(2026, 7, 15), date_type(2026, 7, 17)
    w = time_bounds.from_local_dates(lo, hi, "Europe/London")
    assert w.start is not None and w.end is not None
    assert w.start < _utc(2026, 7, 15) and w.end > _utc(2026, 7, 17)


# imported late so the module import above stays about the pipeline, not the API layer
from app.api.v1.logs import _day_conds as dt_day_conds  # noqa: E402
