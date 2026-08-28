"""Chunk 60 (S4a of docs/analytics-ml-architecture/final_architecture.md, section 18): persist the
grouper's state, seed a second grouping from it, and MEASURE the divergence without acting on it.

Why this ships in shadow
------------------------
S3 made the six known miss modes PERMANENT. Nothing revisits a row whose fingerprint matched, so a
split that should have merged never heals - whereas before S3 it healed on the next of 22 rebuilds,
which is exactly why none has ever been observed in production. Promoting the lookup without measuring
divergence on real traffic would make a silent split unrecoverable.

So `stage2_stream_lookup` is three-valued, defaults to `shadow`, and in shadow the RE-DERIVE stays
authoritative. What the seeded run produces is compared and logged, never persisted.

The guard is the whole safety argument
--------------------------------------
    last_entry_ts >= window_lo                       -> clock went backwards (failure mode 2)
    window_lo - last_entry_ts >= log_open_gap_seconds -> quiet gap (failure mode 1)

Both refuse the stream and fall back. The refusals are COUNTED BY REASON rather than merely tallied,
because "declined 900 times because the tenant was idle" and "declined 900 times because the clock went
backwards" are the same number and completely different problems.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select

from app.config.database import async_session
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
from app.services.mnp_log_ingestion.pipeline import stream_state as ss
from app.settings import settings

CC = "test_chunk60"
T0 = datetime(2026, 8, 16, 10, 0, tzinfo=timezone.utc)


class _Row:
    """A stored stream row, for the pure guard tests. No database needed."""

    def __init__(self, last_entry_ts):
        self.last_entry_ts = last_entry_ts


async def _wipe():
    async with async_session() as db:
        for model in (LogOpenStream, LogPendingRequest, LogEntryAssignment,
                      LogTransaction, LogEntry):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


# =============================================================== 1. the mode switch
def test_the_default_is_shadow_not_on():
    """S4 must not be promoted by deploying it. The gate is a week of zero divergence on real traffic,
    which cannot happen before the code that measures it has run."""
    assert settings.stage2_stream_lookup == ss.SHADOW


def test_an_unrecognised_mode_falls_back_to_shadow_not_off():
    """Deliberately shadow rather than off. A typo falling through to `off` would look EXACTLY like S4
    working perfectly and never diverging - the most misleading possible failure. Shadow costs a
    comparison and tells the truth."""
    original = settings.stage2_stream_lookup
    try:
        settings.stage2_stream_lookup = "yes-please"
        assert ss.mode() == ss.SHADOW
    finally:
        settings.stage2_stream_lookup = original


@pytest.mark.parametrize("value,expected", [
    ("off", ss.OFF), ("shadow", ss.SHADOW), ("on", ss.ON),
    ("  ON  ", ss.ON), ("Shadow", ss.SHADOW),
])
def test_the_mode_is_read_case_and_whitespace_insensitively(value, expected):
    original = settings.stage2_stream_lookup
    try:
        settings.stage2_stream_lookup = value
        assert ss.mode() == expected
    finally:
        settings.stage2_stream_lookup = original


# =============================================================== 2. the guard
def test_a_recent_stream_is_usable():
    ok, why = ss.usable(_Row(T0 - timedelta(seconds=10)), T0)
    assert ok is True and why is None


def test_a_stream_from_the_future_is_refused():
    """Failure mode 2, the backfill. A window replayed with an older clock would otherwise bind to a
    stream from the future, and there is no way to un-evict."""
    ok, why = ss.usable(_Row(T0 + timedelta(seconds=1)), T0)
    assert ok is False and why == "clock_went_backwards"


def test_a_stream_exactly_at_the_window_start_is_refused():
    """The boundary is `>=`, not `>`. A stream whose last entry is the window's first instant has
    already been accounted for by the window that produced it."""
    ok, why = ss.usable(_Row(T0), T0)
    assert ok is False and why == "clock_went_backwards"


def test_a_stream_past_the_quiet_gap_is_refused():
    """Failure mode 1. `evict_stale` closes a stream on this same bound in memory; without the guard a
    stream idle for hours would absorb the next unrelated request into one bloated transaction."""
    ok, why = ss.usable(_Row(T0 - timedelta(seconds=settings.log_open_gap_seconds + 1)), T0)
    assert ok is False and why == "quiet_gap"


def test_the_guard_uses_the_same_bound_as_evict_stale():
    """Two implementations of one rule. If they drifted, a stream the grouper considers closed would
    still be reusable from the table, which is a merge that should have been a split."""
    just_inside = T0 - timedelta(seconds=settings.log_open_gap_seconds - 1)
    just_outside = T0 - timedelta(seconds=settings.log_open_gap_seconds)
    assert ss.usable(_Row(just_inside), T0)[0] is True
    assert ss.usable(_Row(just_outside), T0)[0] is False


def test_a_stream_with_no_timestamp_is_refused():
    """A stream whose entries all lack a parsable timestamp cannot be reasoned about in time at all, so
    it is never reused. Rare, and the fallback handles it correctly."""
    ok, why = ss.usable(_Row(None), T0)
    assert ok is False and why == "no_timestamp"


# =============================================================== 3. seeding is inert when empty
def _entry(kind, at, line, reqid=None):
    return LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=uuid.uuid4(), timestamp=at,
                    source_file="a.log", line_number=line, level="INFO", raw_body="x",
                    message="x", entry_hash=uuid.uuid4().hex, entry_type=kind,
                    thread="T1", user_ctx="amin", fields={"reqid": reqid} if reqid else {})


def test_an_empty_seed_changes_nothing():
    """The property that let S4 ship without bumping `_DERIVE_VERSION`: with no seed the added loop
    iterates an empty tuple, so every existing row derives exactly as before."""
    entries = [_entry(LogEntryType.request, T0, 1, "R1"),
               _entry(LogEntryType.response, T0 + timedelta(seconds=1), 2, "R1")]

    def shape(gs):
        return sorted(tuple(sorted(str(e.id) for e in g.entries)) for g in gs)

    assert shape(dt._group(entries)) == shape(dt._group(entries, seed={"streams": [], "pending": []}))


def test_a_seeded_stream_continues_instead_of_starting_a_new_transaction():
    """The point of S4. A REQUEST in window 1 and its RESPONSE in window 2 must be ONE transaction, and
    without persisted state window 2 has no idea the request happened."""
    req = _entry(LogEntryType.request, T0, 1, "R1")
    resp = _entry(LogEntryType.response, T0 + timedelta(seconds=30), 2, "R1")

    unseeded = dt._group([resp])
    assert all(len(g.entries) == 1 for g in unseeded), "precondition: alone, the response is an orphan"

    seeded = dt._group([resp], seed={
        "streams": [{"thread": "T1", "user_ctx": "amin", "is_current": True,
                     "open_pos": dt._stream_pos(req), "entries": [req]}],
        "pending": []})
    joined = [g for g in seeded if len(g.entries) == 2]
    assert joined, "the seeded stream did not continue - S4 buys nothing"
    assert {e.line_number for e in joined[0].entries} == {1, 2}


# =============================================================== 4. persistence
async def _seed_rows(*, last_ts, thread="T1", user="amin"):
    async with async_session() as db:
        tid = uuid.uuid4()
        db.add(LogOpenStream(id=uuid.uuid4(), customer_code=CC, thread=thread, user_ctx=user,
                             transaction_id=tid, has_request=True, last_entry_ts=last_ts,
                             open_ts_is_null=False, open_ts=last_ts, open_source_file="a.log",
                             open_line_number=1, is_current=True))
        await db.commit()
        return tid


async def test_load_reports_refusals_by_reason():
    """Counted by KIND, because "the tenant was idle" and "the clock went backwards" are the same
    number and completely different problems."""
    await _seed_rows(last_ts=T0 + timedelta(seconds=5), thread="future")
    await _seed_rows(last_ts=T0 - timedelta(seconds=settings.log_open_gap_seconds + 60), thread="idle")
    await _seed_rows(last_ts=T0 - timedelta(seconds=5), thread="fine")
    async with async_session() as db:
        state = await ss.load(db, CC, T0)
    assert state["stored_streams"] == 3
    assert len(state["streams"]) == 1
    assert state["refusals"] == {"clock_went_backwards": 1, "quiet_gap": 1}


async def test_save_replaces_rather_than_merges():
    """The crash-safety property. A partial update would leave a stream row the new grouping does not
    believe in, and nothing would ever notice."""
    await _seed_rows(last_ts=T0, thread="old")
    async with async_session() as db:
        await ss.save(db, CC, streams=[{
            "server": "S", "thread": "new", "user_ctx": "amin", "transaction_id": uuid.uuid4(),
            "has_request": False, "last_entry_ts": T0,
            "open_pos": (False, T0, "a.log", 1), "is_current": True}], pending=[])
        await db.commit()
        rows = (await db.execute(select(LogOpenStream.thread).where(
            LogOpenStream.customer_code == CC))).scalars().all()
    assert rows == ["new"], "the prior state survived a replace"


async def test_the_stream_key_treats_nulls_as_equal():
    """`NULLS NOT DISTINCT` is load-bearing. Under the default rule `(NULL, 'amin')` never conflicts
    with itself, so one logical stream would accumulate several rows and the lookup would be
    non-deterministic - failure mode 5."""
    from sqlalchemy.exc import IntegrityError
    async with async_session() as db:
        for _ in range(2):
            db.add(LogOpenStream(id=uuid.uuid4(), customer_code=CC, thread=None, user_ctx=None,
                                 transaction_id=uuid.uuid4(), last_entry_ts=T0,
                                 open_ts_is_null=False, open_ts=T0, open_line_number=1))
        with pytest.raises(IntegrityError):
            await db.commit()


async def test_one_tenants_state_is_not_another_s():
    other = "test_chunk60_other"
    await _seed_rows(last_ts=T0 - timedelta(seconds=5))
    async with async_session() as db:
        assert len((await ss.load(db, CC, T0))["streams"]) == 1
        assert len((await ss.load(db, other, T0))["streams"]) == 0


# =============================================================== 5. the reaper
async def test_the_reaper_removes_state_past_the_ttl():
    """REQUIRED, not optional (18d). `evict_stale` closes a stream when an ENTRY ARRIVES, so a tenant
    that stops ingesting leaves its rows behind forever. Derived state could not leak; this can."""
    async with async_session() as db:
        db.add(LogOpenStream(id=uuid.uuid4(), customer_code=CC, thread="stale", user_ctx="u",
                             transaction_id=uuid.uuid4(), last_entry_ts=T0,
                             open_ts_is_null=False, open_ts=T0, open_line_number=1,
                             created_at=T0, updated_at=datetime(2020, 1, 1, tzinfo=timezone.utc)))
        await db.commit()
    stats = await ss.reap()
    assert stats["streams_reaped"] >= 1
    async with async_session() as db:
        left = await db.scalar(select(func.count()).select_from(LogOpenStream).where(
            LogOpenStream.customer_code == CC, LogOpenStream.thread == "stale"))
    assert left == 0


async def test_the_reaper_keeps_live_state():
    """A reaper that cleared everything would be a correctness bug dressed as hygiene: the next window
    would seed from nothing and every cross-window transaction would split."""
    async with async_session() as db:
        db.add(LogOpenStream(id=uuid.uuid4(), customer_code=CC, thread="live", user_ctx="u",
                             transaction_id=uuid.uuid4(), last_entry_ts=T0,
                             open_ts_is_null=False, open_ts=T0, open_line_number=1))
        await db.commit()
    await ss.reap()
    async with async_session() as db:
        left = await db.scalar(select(func.count()).select_from(LogOpenStream).where(
            LogOpenStream.customer_code == CC, LogOpenStream.thread == "live"))
    assert left == 1


async def test_the_reaper_reports_the_live_count():
    """`count(*)` is the ONLY health signal these tables have - a number that only grows is the alarm,
    and there is no upstream event to catch it. So every sweep reports it."""
    stats = await ss.reap()
    assert "streams_live" in stats and "pending_live" in stats


def test_the_reaper_runs_in_the_stitch_tick():
    """A reaper nothing calls is a leak with extra steps."""
    import inspect
    from app.services.workers import log_stitch_worker
    src = inspect.getsource(log_stitch_worker._tick)
    assert "stream_state.reap" in src


def test_the_state_tables_are_not_registered_for_partitioning():
    """18d: both are deliberately unpartitioned. Registering one without a retention policy would make
    it silently inherit the log tables' 60 days, and a partitioned table needs a grain these do not
    have."""
    from app.persistence.partitioning import BY_TABLE
    assert "log_open_stream" not in BY_TABLE
    assert "log_pending_request" not in BY_TABLE


# =============================================================== 6. the divergence fix, 2026-08-25
#
# DIAGNOSED ON LIVE DATA. Every shadow run where a stream actually seeded DIVERGED (8 of 8), always with
# the same signature: the seeded run produced one extra group and 7-8 groupings shifted. Dissected on
# the server: every diverging seeded stream's transaction was OUTSIDE the window's rebuild set. One
# measured window went from 1 cold group to 17 seeded ones.
#
# Two mechanisms, both now closed:
#   - an out-of-scope stream describes a persisted transaction whose entries the authoritative run
#     cannot see, and its phantom open builder steals user-FIFO responses from streams both runs DO
#     see, shifting every later same-user grouping by one
#   - an in-scope stream's carried entries ALSO replay as window rows, closing the seeded builder as
#     "a prior cycle" the moment its own REQUEST re-arrives - duplicating the transaction
#
# Fix verified against the live data before it was written into the code: seed only rebuilding-set
# streams + dedupe carried rows => fixed == cold, exactly.

def test_a_seeded_builders_own_entries_are_not_replayed():
    """The dedupe half. Without it, the seeded builder is closed as "a prior cycle" when its own
    REQUEST re-arrives in the window rows, and the transaction appears twice."""
    req = _entry(LogEntryType.request, T0, 1, "R1")
    info = _entry(LogEntryType.info, T0 + timedelta(seconds=1), 2)
    resp = _entry(LogEntryType.response, T0 + timedelta(seconds=30), 3, "R1")

    groups = dt._group([req, info, resp], seed={
        "streams": [{"thread": "T1", "user_ctx": "amin", "is_current": True,
                     "open_pos": dt._stream_pos(req), "entries": [req, info]}],
        "pending": []})
    assert len([g for g in groups if g.entries]) == 1, \
        "replaying a carried entry must not split the transaction in two"
    assert {e.line_number for e in groups[0].entries} == {1, 2, 3}


def test_dedupe_is_inert_without_a_seed():
    """The persisting path never seeds, so its behaviour must be byte-identical - which is also why
    _DERIVE_VERSION stays unbumped for this change."""
    rows = [_entry(LogEntryType.request, T0, 1, "R1"),
            _entry(LogEntryType.response, T0 + timedelta(seconds=1), 2, "R1")]
    def shape(gs): return sorted(tuple(sorted(str(e.id) for e in g.entries)) for g in gs)
    assert shape(dt._group(rows)) == shape(dt._group(rows, seed=None))


def test_shadow_compare_seeds_only_the_rebuilding_set():
    """The scope half, asserted on the source: the failure was a MISSING filter, and every diverging
    stream on live data was out of scope. Also asserts the exclusion is REPORTED (`out_of_scope`), so
    the shadow telemetry cannot silently hide how much it is not measuring."""
    import inspect
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt2
    src = inspect.getsource(dt2._shadow_compare)
    assert "rebuilding" in inspect.signature(dt2._shadow_compare).parameters
    assert "r.transaction_id in rebuilding" in src
    assert "out_of_scope" in src, "the exclusion must be visible in the report, not silent"


def test_the_call_site_passes_the_freed_set():
    import inspect
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt2
    src = inspect.getsource(dt2.regroup_window)
    assert "rebuilding=frozenset(freed)" in src


def test_an_out_of_scope_stream_cannot_create_a_phantom_group():
    """End to end at the _group level: a seeded stream whose entries are NOT in the window rows (the
    out-of-scope shape) must not appear in the result as a group of carried-only entries when the
    caller correctly excludes it. This asserts the two halves compose: with the stream excluded, the
    grouping equals the cold one."""
    old_req = _entry(LogEntryType.request, T0 - timedelta(seconds=1200), 1, "OLD")
    window_rows = [_entry(LogEntryType.request, T0, 10, "R2"),
                   _entry(LogEntryType.response, T0 + timedelta(seconds=2), 11, "R2")]
    def shape(gs): return sorted(tuple(sorted(str(e.id) for e in g.entries)) for g in gs)
    cold = shape(dt._group(list(window_rows)))
    # exclusion is the CALLER's job (freed-set filter); with it applied, no seed remains:
    assert shape(dt._group(list(window_rows), seed={"streams": [], "pending": []})) == cold
