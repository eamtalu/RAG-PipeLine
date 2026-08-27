"""Chunk 67 (section 18r): grouping is scoped to the SERVER a line came from.

Diagnosed 2026-08-27 on live data, and the mechanism is worth stating in full because it produced
three different symptoms that looked unrelated:

The warehouse runs two app servers (TMP-AZ-BEC01, TMP-AZ-BEC02), each writing its own log file. A
thread id is only meaningful INSIDE one server process - thread 33 on BEC01 and thread 33 on BEC02
are different worlds. But the grouper's cross-entry matching pools ignored the file: a POST body
binds "the most recent id-less pending request" from EITHER server, a GET binds "the oldest pending
request of this user" from EITHER server, and a response FIFO-matches open work of its user on
EITHER server. One picker whose two operations hit both servers within milliseconds cross-binds.

Measured example (2026-08-27 12:09:35, user OPRACHASUK): the persisted, sealed, "success"
transaction was seq0 = BEC01's request, seq1-8 = BEC02's body and work, seq9 = BEC01's response - a
chimera of two real operations. The two leftover halves (BEC02's request, BEC01's body) then minted
the SAME deterministic id as the chimera (the anchor is the request's content hash), hit `_persist`'s
clash skip, and were stranded unassigned FOREVER - S3's fingerprint skip means nothing revisits them.

The three symptoms: ~300 orphaned entries/day (5,353 since 2026-08-10), the hourly "skipped
builder(s) with an already-sealed id" warnings, and a silent analytics undercount (two operations
counted as one, with mixed-server attributes).

The fix: every grouping key and every matching pool carries the server (the first path segment of
`source_file`). Within one server nothing changes - the single-server tests below pin that.
Grouping behaviour changes for cross-server interleavings, so `_DERIVE_VERSION` bumps to 2."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

CC = "test_chunk67"
T0 = datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc)

S1 = "TMP-AZ-BEC01/eSmartServerLog.txt"
S2 = "TMP-AZ-BEC02/eSmartServerLog.txt"


def _e(kind, at, line, *, source, thread, user="OPRACHASUK", fields=None):
    return LogEntry(id=uuid.uuid4(), customer_code=CC, job_id=uuid.uuid4(), timestamp=at,
                    source_file=source, line_number=line, level="INFO", raw_body="x",
                    message="x", entry_hash=uuid.uuid4().hex, entry_type=LogEntryType(kind),
                    thread=thread, user_ctx=user, fields=fields or {})


def _shape(groups):
    """Membership by (server, line) pairs, order-free - builder identity is irrelevant."""
    return sorted(tuple(sorted((e.source_file.split("/")[0], e.line_number) for e in g.entries))
                  for g in groups)


def _ms(n):
    return T0 + timedelta(milliseconds=n)


#: The live chimera, reconstructed line for line from the 12:09:35 forensics: two complete
#: single-server conversations by the same user, interleaved within ~30 ms.
_TWO_SERVER_INTERLEAVE = [
    _e("request", _ms(113), 1, source=S1, thread="45"),
    _e("request", _ms(115), 1, source=S2, thread="33"),
    _e("request_body", _ms(134), 2, source=S1, thread="45"),
    _e("request_body", _ms(144), 2, source=S2, thread="33"),
    _e("info", _ms(145), 3, source=S2, thread="33"),
    _e("mi_call", _ms(146), 4, source=S2, thread="33"),
    _e("info", _ms(150), 3, source=S1, thread="45"),
    _e("mi_result", _ms(325), 5, source=S2, thread="33"),
    _e("response", _ms(330), 6, source=S1, thread="40"),
    _e("response", _ms(338), 7, source=S2, thread="34"),
]


# ==================================================== 1. the chimera

def test_two_servers_same_user_do_not_cross_bind():
    """The flagship, reconstructed from the live forensics. Correct output: exactly TWO
    transactions, each containing lines from ONE server only. Today's output is a chimera plus
    stranded leftovers."""
    groups = [g for g in dt._group(list(_TWO_SERVER_INTERLEAVE)) if g.entries]

    servers_per_group = [{e.source_file.split("/")[0] for e in g.entries} for g in groups]
    assert all(len(s) == 1 for s in servers_per_group), (
        f"a transaction mixed lines from two servers: {_shape(groups)}")
    assert len(groups) == 2, f"expected the two real operations, got {len(groups)}: {_shape(groups)}"


def test_same_thread_number_on_two_servers_are_distinct_streams():
    """Thread ids are small integers reused by every server process, so the builder key itself
    collides across servers even without the user-scoped pools. Two servers, same thread number,
    same user: two streams."""
    entries = [
        _e("request", _ms(0), 1, source=S1, thread="33"),
        _e("request_body", _ms(5), 2, source=S1, thread="33"),
        _e("request", _ms(10), 1, source=S2, thread="33"),
        _e("request_body", _ms(15), 2, source=S2, thread="33"),
        _e("info", _ms(20), 3, source=S1, thread="33"),
        _e("info", _ms(25), 3, source=S2, thread="33"),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert _shape(groups) == [
        ((("TMP-AZ-BEC01", 1)), ("TMP-AZ-BEC01", 2), ("TMP-AZ-BEC01", 3)),
        ((("TMP-AZ-BEC02", 1)), ("TMP-AZ-BEC02", 2), ("TMP-AZ-BEC02", 3)),
    ]


def test_a_response_never_crosses_servers():
    """A response is written by the server process that handled the request; matching it to the
    other server's open work fabricates a conversation that never happened. With no candidate on its
    own server it becomes an orphan-response transaction, which is the honest answer."""
    entries = [
        _e("request", _ms(0), 1, source=S1, thread="10"),
        _e("request_body", _ms(2), 2, source=S1, thread="10"),
        _e("info", _ms(4), 3, source=S1, thread="10"),
        _e("response", _ms(6), 1, source=S2, thread="20"),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    shapes = _shape(groups)
    assert (("TMP-AZ-BEC02", 1),) in shapes, (
        f"the other server's response must not join BEC01's work: {shapes}")


def test_get_request_user_binding_is_server_scoped():
    """The GET path: work lines bind 'the oldest pending request of this user' - which must mean
    of this user ON THIS SERVER."""
    entries = [
        _e("request", _ms(0), 1, source=S1, thread="10"),
        _e("request", _ms(1), 1, source=S2, thread="20"),
        _e("info", _ms(5), 2, source=S2, thread="20"),
        _e("info", _ms(6), 2, source=S1, thread="10"),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    servers_per_group = [{e.source_file.split("/")[0] for e in g.entries} for g in groups]
    assert all(len(s) == 1 for s in servers_per_group), _shape(groups)


# ==================================================== 2. within one server nothing changes

def test_single_server_grouping_is_unchanged():
    """The guard for the other direction: a normal one-server conversation groups exactly as
    before - request, body, work and response in one transaction."""
    entries = [
        _e("request", _ms(0), 1, source=S1, thread="10"),
        _e("request_body", _ms(2), 2, source=S1, thread="10"),
        _e("info", _ms(4), 3, source=S1, thread="10"),
        _e("mi_call", _ms(5), 4, source=S1, thread="10"),
        _e("response", _ms(8), 5, source=S1, thread="11"),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert len(groups) == 1
    assert {e.line_number for e in groups[0].entries} == {1, 2, 3, 4, 5}


def test_reqid_binding_still_works_within_a_server():
    """GET requests carrying a ReqID bind their body by identity; that path stays intact."""
    entries = [
        _e("request", _ms(0), 1, source=S1, thread="10", fields={"params": {"ReqID": "r-77"}}),
        _e("request_body", _ms(2), 2, source=S1, thread="10", fields={"ReqID": "r-77"}),
        _e("info", _ms(4), 3, source=S1, thread="10"),
    ]
    groups = [g for g in dt._group(entries) if g.entries]
    assert len(groups) == 1
    assert {e.line_number for e in groups[0].entries} == {1, 2, 3}


# ==================================================== 3. the leak, end to end

async def _wipe():
    async with async_session() as db:
        for model in (LogOpenStream, LogPendingRequest, AnalyticsPendingWindow,
                      LogRegroupPending, LogEntryAssignment, LogTransaction, LogEntry):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Job).where(Job.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def test_the_orphan_leak_case_end_to_end():
    """The full production shape, which needs TWO ingestion phases: the SSH fetch delivers the
    conversation's lines across two ticks, so the first stitch groups a PARTIAL view (this is where
    the cross-server mash forms), and the second stitch regroups the complete set differently - the
    builder holding most of the old transaction INHERITS its id while the leftover request MINTS the
    same id, and the collision skip strands the leftovers forever. Correct behaviour: every entry
    assigned, two single-server transactions, nothing clash-skipped, on both passes."""
    async def _ingest(entries):
        async with async_session() as db:
            job = Job(customer_code=CC, filename="t.log", document_type="transaction_log",
                      storage_key=f"{CC}/{uuid.uuid4().hex}/t.log", status="completed")
            db.add(job)
            await db.flush()
            for e in entries:
                row = _e(e.entry_type.value, e.timestamp, e.line_number,
                         source=e.source_file, thread=e.thread)
                row.job_id = job.id
                db.add(row)
            await db.commit()

    # One FILE arrives first, not one time-slice: the fetcher pulls BEC02's file a tick before
    # BEC01's. The partial pass builds a transaction anchored at BEC02's request; when BEC01's lines
    # arrive, the re-deal hands that request to a different builder, which INHERITS the old id by
    # plurality - while the loser builder MINTS the very same id from the same request and is
    # skipped. That inherit-vs-mint collision is the exact live strand (12:09:35 forensics).
    first_arrival = [e for e in _TWO_SERVER_INTERLEAVE if e.source_file == S2]
    late_arrival = [e for e in _TWO_SERVER_INTERLEAVE if e.source_file == S1]

    await _ingest(first_arrival)
    async with async_session() as db:
        s1 = await dt.regroup_window(db, CC, T0, T0 + timedelta(seconds=1))

    await _ingest(late_arrival)
    async with async_session() as db:
        stats = await dt.regroup_window(db, CC, T0 + timedelta(milliseconds=200),
                                        T0 + timedelta(seconds=2))
    stats["transactions_skipped"] = stats.get("transactions_skipped", 0) + s1.get(
        "transactions_skipped", 0)

    assert stats.get("transactions_skipped", 0) == 0, "the clash skip is the leak"
    async with async_session() as db:
        unassigned = (await db.execute(select(LogEntry.id).where(
            LogEntry.customer_code == CC,
            ~select(LogEntryAssignment.entry_id).where(
                LogEntryAssignment.entry_id == LogEntry.id).exists()))).scalars().all()
        txns = (await db.execute(select(LogTransaction.id).where(
            LogTransaction.customer_code == CC))).scalars().all()
    assert unassigned == [], f"{len(unassigned)} entries stranded - the leak lives"
    assert len(txns) == 2, f"expected the two real operations, got {len(txns)}"


# ==================================================== 4. the repair path must be able to run

def test_the_full_regroup_relaxes_the_statement_guard():
    """Found running the 18r backlog repair in production: `regroup_all` reads a tenant's WHOLE
    history in one SELECT, which legitimately exceeds the web tier's 30 s statement guard - the same
    deliberate exception Stage 1's bulk insert and the analytics fold already make. Without the
    relax, the one repair the orphan-leak fix depends on cannot run on production volume."""
    import inspect
    src = inspect.getsource(dt.regroup_all)
    assert src.count('SET LOCAL statement_timeout = 0') >= 2, (
        "the relax must be issued per transaction phase - regroup_all commits between phases")
