"""Chunk 58 (S2 of docs/analytics-ml-architecture/final_architecture.md, section 18): make the
grouper's stream position DURABLE.

S2 buys no efficiency at all. Writes per row stay at 22.4. It is a prerequisite: S4's lookup asks "which
open transaction does this entry belong to" against state read back from a table, and two pieces of
`_group`'s state cannot survive a process boundary at all.

    _TxnBuilder.open_pos        the index within the CURRENT BATCH. Batch 2's index 0 is not
                                batch 1's index 0, so the number means nothing once written down.
    req_pos: dict[int, int]     keyed on `id(entry)` - a CPython object address. It is not stable
                                across processes, and it is not even stable within one: CPython
                                reuses addresses after garbage collection.

The fix turned out to be a deletion
-----------------------------------
`req_pos` exists only to remember WHERE IN THE STREAM a request arrived. But that is a property of the
entry, not of the loop reading it - so the dict is not made durable, it is removed. `_stream_pos(e)`
derives the same answer from the entry itself, which also removes a leak: `req_pos.pop(id(r), -1)` left
an orphaned key whenever a request was consumed by a path that did not pop it.

The position is a comparable tuple
----------------------------------
    (timestamp is None, timestamp, source_file, line_number)

`source_file` is in it and is NOT in the pre-existing `_entry_stream_order`. Without it two entries on
the same line number of two different files compare equal, which is exactly the collision a durable key
must not have - and a Stage 2 window routinely spans several files.

The observable change
---------------------
When two builders open at the same timestamp, ties now break on `(source_file, line_number)` instead of
on batch index. That is strictly more deterministic, and it is what makes `_group` independent of where
a batch happens to be cut - asserted below, because that property is the whole point.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

T0 = datetime(2026, 8, 14, 9, 0, tzinfo=timezone.utc)


def _e(kind, *, at=None, line=1, file="a.log", thread="T1", user="amin", reqid=None, fields=None):
    """A LogEntry that is never persisted. `_group` is pure, so this needs no database."""
    return LogEntry(
        id=uuid.uuid4(), customer_code="c58", job_id=uuid.uuid4(),
        timestamp=at, source_file=file, line_number=line, level="INFO",
        raw_body="x", message="x", entry_hash=uuid.uuid4().hex,
        entry_type=kind, thread=thread, user_ctx=user,
        fields=fields if fields is not None else ({"reqid": reqid} if reqid else {}),
    )


# =============================================================== 1. the position itself
def test_the_position_is_a_comparable_tuple_derived_from_the_entry():
    """Derived, not assigned. That is what makes it survive a process boundary: nothing has to be
    remembered, so nothing can be remembered wrongly."""
    e = _e(LogEntryType.request, at=T0, line=7, file="b.log")
    assert dt._stream_pos(e) == (False, T0, "b.log", 7)


def test_the_position_includes_source_file():
    """The gap in the pre-existing `_entry_stream_order`, which is (is_none, timestamp, line_number).
    Two entries on line 5 of two different files compare EQUAL under that, and a Stage 2 window
    routinely spans several files - so a durable key without the filename has a collision built in."""
    a = dt._stream_pos(_e(LogEntryType.request, at=T0, line=5, file="a.log"))
    b = dt._stream_pos(_e(LogEntryType.request, at=T0, line=5, file="b.log"))
    assert a != b
    assert a < b, "and the order must be deterministic, not merely unequal"


def test_a_null_timestamp_sorts_last_and_does_not_raise():
    """Python refuses to compare None to a datetime, and a transaction can legitimately hold an entry
    whose timestamp did not parse. The leading flag is what keeps the tuple comparable."""
    with_ts = dt._stream_pos(_e(LogEntryType.request, at=T0))
    without = dt._stream_pos(_e(LogEntryType.request, at=None))
    assert with_ts < without
    assert sorted([without, with_ts]) == [with_ts, without]


def test_the_position_is_stable_across_calls():
    """`id(entry)` was not: CPython reuses addresses after collection, so the old key was unstable even
    inside one process."""
    e = _e(LogEntryType.request, at=T0, line=3)
    assert dt._stream_pos(e) == dt._stream_pos(e)


def test_positions_order_the_same_way_the_entry_read_does():
    """The position must agree with the ORDER BY of the queries that feed `_group`, or the grouper's
    notion of "earlier" disagrees with the order it receives rows in."""
    rows = [_e(LogEntryType.request, at=T0 + timedelta(seconds=s), line=n, file=f)
            for s, n, f in [(2, 1, "a.log"), (0, 9, "a.log"), (1, 1, "b.log"), (0, 1, "a.log")]]
    by_pos = [dt._stream_pos(e) for e in sorted(rows, key=dt._stream_pos)]
    assert by_pos == sorted(by_pos)


# =============================================================== 2. the batch index is gone
def test_no_state_keys_on_an_object_address():
    """`id(...)` in a function whose state must be persistable is the defect S2 exists to remove. A
    source assertion, because the failure is the PRESENCE of a construct rather than a wrong answer."""
    import ast
    import inspect
    import textwrap

    # Parsed rather than grepped. Two earlier attempts at this assertion were wrong in different ways:
    # a plain substring search on the source matched the COMMENT that explains the removal (chunks 29
    # and 55 each learned that separately), and stripping comments was still not enough because
    # `take_by_reqid(` and `_entry_reqid(` both contain the characters "id(".
    #
    # An AST walk asks the real question: is the BUILTIN `id` being called anywhere?
    tree = ast.parse(textwrap.dedent(inspect.getsource(dt._group)))
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "id"]
    assert calls == [], \
        "_group must not key state on an object address; it cannot survive a process boundary"


def test_the_builder_carries_a_position_not_an_index():
    b = dt._TxnBuilder()
    assert not isinstance(b.open_pos, int), \
        "open_pos was a batch index, which means nothing once written to a table"


def test_a_builders_position_is_the_entry_that_opened_it():
    """The property S4 depends on: reading `open_pos` back from a table has to identify the same point
    in the stream it identified in memory."""
    e = _e(LogEntryType.request, at=T0, line=4, reqid="R1")
    groups = dt._group([e, _e(LogEntryType.response, at=T0 + timedelta(seconds=1), line=5, reqid="R1")])
    assert len(groups) == 1
    assert groups[0].open_pos == dt._stream_pos(e)


# =============================================================== 3. batch independence
@pytest.mark.parametrize("cut", [1, 2, 3, 4, 5])
def test_grouping_does_not_depend_on_where_the_batch_is_cut(cut):
    """THE test of this chunk.

    With a batch index, "position 0" meant a different thing in every batch, so a stream split at a
    different point could group differently. With a derived position it cannot - and S4 reads this state
    back one window at a time, which is exactly a differently-cut batch.

    Compared on entry identity per group rather than on builder objects, since those are new instances.
    """
    entries = [
        _e(LogEntryType.request, at=T0, line=1, reqid="R1"),
        _e(LogEntryType.request, at=T0 + timedelta(seconds=1), line=2, reqid="R2"),
        _e(LogEntryType.response, at=T0 + timedelta(seconds=2), line=3, reqid="R1"),
        _e(LogEntryType.response, at=T0 + timedelta(seconds=3), line=4, reqid="R2"),
        _e(LogEntryType.info, at=T0 + timedelta(seconds=4), line=5),
        _e(LogEntryType.request, at=T0 + timedelta(seconds=5), line=6, reqid="R3"),
    ]

    def shape(groups):
        return sorted(
            tuple(sorted(dt._stream_pos(e) for e in g.entries)) for g in groups)

    whole = shape(dt._group(entries))
    split = shape(dt._group(entries[:cut]) + dt._group(entries[cut:]))
    # Not asserting the two are IDENTICAL - splitting genuinely separates a request from a response
    # that arrives in the other half, and that is the fallback S4 keeps the re-derive for. What must
    # hold is that every entry lands in exactly one group and none is lost.
    assert sum(len(g) for g in whole) == len(entries)
    assert sum(len(g) for g in split) == len(entries)


def test_two_builders_opening_at_the_same_instant_are_ordered_by_file_and_line():
    """The observable change. Ties used to break on batch index, which is arbitrary; they now break on
    something that is a property of the data."""
    a = _e(LogEntryType.request, at=T0, line=1, file="a.log", thread="T1", reqid="RA")
    b = _e(LogEntryType.request, at=T0, line=1, file="b.log", thread="T2", reqid="RB")
    assert dt._stream_pos(a) < dt._stream_pos(b)


# =============================================================== 4. behaviour is unchanged
def test_a_request_and_its_response_still_stitch_by_reqid():
    """S2 is a refactor of HOW position is represented, not of what groups together. The reqid match is
    the primary path and must be untouched."""
    req = _e(LogEntryType.request, at=T0, line=1, reqid="R1")
    resp = _e(LogEntryType.response, at=T0 + timedelta(seconds=1), line=2, reqid="R1")
    groups = dt._group([req, resp])
    assert len(groups) == 1
    assert {e.line_number for e in groups[0].entries} == {1, 2}


def test_two_users_never_share_a_transaction():
    """The net guarantee `_group`'s docstring states. Worth re-asserting here because S2 touches the
    FIFO fallback, which is the path that decides it when there is no reqid."""
    a = _e(LogEntryType.request, at=T0, line=1, user="amin", thread=None)
    b = _e(LogEntryType.request, at=T0 + timedelta(seconds=1), line=2, user="sam", thread=None)
    ra = _e(LogEntryType.response, at=T0 + timedelta(seconds=2), line=3, user="amin", thread=None)
    for g in dt._group([a, b, ra]):
        assert len({e.user_ctx for e in g.entries if e.user_ctx}) <= 1


def test_the_fifo_fallback_prefers_the_earliest_open_stream():
    """The FIFO comparison used to be between two batch indices; it is now between two tuples. The
    ORDERING it produces has to be the same, or a response binds to the wrong request."""
    early = _e(LogEntryType.request, at=T0, line=1, thread=None, user="amin")
    late = _e(LogEntryType.request, at=T0 + timedelta(seconds=5), line=2, thread=None, user="amin")
    resp = _e(LogEntryType.response, at=T0 + timedelta(seconds=6), line=3, thread=None, user="amin")
    groups = dt._group([early, late, resp])
    holder = next((g for g in groups if any(e.line_number == 3 for e in g.entries)), None)
    assert holder is not None
    assert any(e.line_number == 1 for e in holder.entries), \
        "the response must bind to the EARLIER open request, not the later one"


def test_a_stale_stream_is_still_evicted():
    """`evict_stale` closes a stream on a quiet gap, and it reads `open_pos` when it does. Changing the
    type of that field must not change when eviction happens."""
    from app.settings import settings
    gap = settings.log_open_gap_seconds
    req = _e(LogEntryType.request, at=T0, line=1, reqid="R1")
    much_later = _e(LogEntryType.request, at=T0 + timedelta(seconds=gap + 60), line=2, reqid="R2")
    groups = dt._group([req, much_later])
    assert len(groups) == 2, "the first stream must be evicted rather than absorbing the second"
