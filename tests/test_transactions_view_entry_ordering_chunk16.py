"""Chunk 16: transactions/view orders each transaction's entries in Python, not SQL.

The entry-fetch in `view_transactions` dropped its global `ORDER BY seq, line_number` (which sorted
across ALL of a page's transactions and spilled to disk); ordering is now restored per-transaction
in Python via `_assigned_sort_key`. See docs/transactions-view-load-spike-and-db-concepts-primer.md.

Since chunk 20 the position comes from `log_entry_assignment` rather than `LogEntry.seq`, so the key
takes `(seq, entry)` pairs. The NULL-safety it guards is unchanged and still correctness-critical.

Two layers of coverage:
  1. Pure unit tests of `_assigned_sort_key` — the correctness-critical bit — including the NULL
     crash-traps that a naive key would hit (no DB).
  2. An end-to-end test that seeds a transaction whose entries are INSERTED out of order (incl. a
     NULL-seq entry) and asserts the rendered feed still shows them in seq/line order — proving the
     removal of the SQL sort caused no regression.
"""

import hashlib
import uuid
from datetime import datetime, date as date_type, timezone
from types import SimpleNamespace

from app.api.v1.logs import _assigned_sort_key, view_transactions
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus

D = date_type(2026, 6, 26)


def _e(seq, line):
    """A `(seq, entry)` pair, matching what `_assigned_sort_key` now receives: the position comes
    from the assignment row, and only `line_number` is read off the entry itself."""
    return (seq, SimpleNamespace(line_number=line))


# --------------------------------------------------------------------------- pure sort-key tests

def test_sorts_by_seq_then_line_number():
    rows = [_e(2, 10), _e(0, 99), _e(1, 5), _e(0, 3)]
    ordered = sorted(rows, key=_assigned_sort_key)
    assert [(q, e.line_number) for q, e in ordered] == [(0, 3), (0, 99), (1, 5), (2, 10)]


def test_null_seq_sorts_last():
    rows = [_e(None, 1), _e(5, 100), _e(0, 100)]
    ordered = sorted(rows, key=_assigned_sort_key)
    assert [q for q, _e in ordered] == [0, 5, None]  # NULLS LAST, regardless of line_number


def test_two_null_seqs_do_not_crash_and_order_by_line_number():
    """The critical trap: a `(e.seq is None, e.seq, ...)` key raises TypeError when two rows both
    have seq=None (None < None). Our key must not, and must fall back to line_number."""
    rows = [_e(None, 42), _e(None, 40), _e(0, 5)]
    ordered = sorted(rows, key=_assigned_sort_key)  # must not raise
    assert [(q, e.line_number) for q, e in ordered] == [(0, 5), (None, 40), (None, 42)]


def test_null_line_number_sorts_last_within_same_seq():
    rows = [_e(1, None), _e(1, 7), _e(1, 3)]
    ordered = sorted(rows, key=_assigned_sort_key)
    assert [e.line_number for _q, e in ordered] == [3, 7, None]  # line_number NULLS LAST too


def test_all_null_rows_do_not_crash():
    rows = [_e(None, None), _e(None, None)]
    ordered = sorted(rows, key=_assigned_sort_key)  # must not raise
    assert len(ordered) == 2


def test_scrambled_input_yields_deterministic_order():
    """Order-independence: any input permutation of the same rows yields the same result — this is
    what makes fetching unordered from the DB safe."""
    canonical = [_e(0, 10), _e(1, 12), _e(2, 15), _e(None, 33), _e(None, 40)]
    expected = [(q, e.line_number) for q, e in sorted(canonical, key=_assigned_sort_key)]
    for perm in ([4, 0, 3, 1, 2], [2, 4, 1, 0, 3], [3, 2, 1, 0, 4]):
        shuffled = [canonical[i] for i in perm]
        got = [(q, e.line_number) for q, e in sorted(shuffled, key=_assigned_sort_key)]
        assert got == expected


# --------------------------------------------------------------------------- end-to-end regression

async def _seed_txn_with_scrambled_entries(db, cc: str) -> LogTransaction:
    """One transaction whose entries are ADDED in a deliberately wrong order (seq 2 before seq 1,
    a NULL-seq entry in the middle), so a correct render must reorder them."""
    job = Job(customer_code=cc, filename="f.log", storage_key="k")
    db.add(job)
    await db.flush()
    tx = LogTransaction(
        customer_code=cc, job_id=job.id, date=D,
        started_at=datetime(2026, 6, 26, 1, 0, 0, tzinfo=timezone.utc),
        status=LogTransactionStatus.success,
    )
    db.add(tx)
    await db.flush()

    def entry(seq, line, etype, msg):
        """`seq` is carried alongside for the assignment rows below — log_entries no longer has a
        seq or transaction_id column; the assignment table owns both."""
        raw = f"{cc}-{msg}-{line}"
        e = LogEntry(
            id=uuid.uuid4(),
            customer_code=cc, job_id=job.id,
            source_file="src.txt", line_number=line, entry_type=etype,
            entry_hash=hashlib.sha256(raw.encode()).hexdigest(), message=msg,
        )
        return e, seq

    # insertion order is intentionally NOT the render order:
    pairs = [
        entry(3, 40, LogEntryType.response, "RESPONSE: done"),
        entry(2, 30, LogEntryType.info, "STEP_SECOND"),
        entry(None, 99, LogEntryType.info, "STEP_UNASSIGNED"),  # no assignment -> not in this txn
        entry(0, 10, LogEntryType.request, "REQUEST: start"),
        entry(1, 20, LogEntryType.info, "STEP_FIRST"),
    ]
    db.add_all([e for e, _q in pairs])
    await db.flush()

    # Since chunk 20 the render reads the position from log_entry_assignment, not LogEntry.seq, so a
    # seeded transaction must carry assignment rows exactly as a real Stage 2 run would. The NULL-seq
    # entry is deliberately given NO assignment row - that is what "unstitched" now looks like.
    db.add_all([
        LogEntryAssignment(entry_id=e.id, transaction_id=tx.id, seq=q, customer_code=cc)
        for e, q in pairs if q is not None
    ])
    await db.flush()
    return tx


async def test_rendered_entries_are_in_seq_order_despite_scrambled_insert(db):
    cc = f"TESTCH16_{uuid.uuid4().hex[:6]}"
    await _seed_txn_with_scrambled_entries(db, cc)

    r = await view_transactions(customer=cc, db=db, pending={}, date=D, limit=50, offset=0,
                                user=None, hour=None, status=None,
                                order_number=None, item_number=None, verbose=True)
    body = r.body.decode()

    # steps must appear in seq order (1 then 2) regardless of the scrambled insertion order above.
    # (the REQUEST/RESPONSE bracket lines are rendered as formatted headers, not raw messages, so
    # the ordering is asserted on the two intermediate steps.)
    assert body.index("STEP_FIRST") < body.index("STEP_SECOND")

    # The NULL-seq entry is NOT rendered under this transaction, and that is the correct new
    # behaviour rather than a regression.
    #
    # Before chunk 20, `transaction_id` and `seq` were two independently nullable columns on
    # log_entries, so "assigned to a transaction but with no position" was a reachable state and the
    # render had to place such an entry last. log_entry_assignment carries both together and `seq` is
    # NOT NULL, so that state is now structurally impossible: an entry either has an assignment (and
    # therefore a position) or it is unassigned and belongs to no transaction yet.
    #
    # An unassigned entry is still visible - it is simply awaiting stitching, and shows through the
    # entry-level endpoints and the pending_regroup flag rather than inside a transaction it does not
    # belong to.
    assert "STEP_UNASSIGNED" not in body
