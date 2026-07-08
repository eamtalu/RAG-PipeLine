"""Chunk 9: Stage 2 stitching survives an over-length promoted dimension value.

Root cause fixed (see docs/stage2-stitching-stall-postmortem-and-fix.md): the WMS puts a
composite/doubled ItemNumber in the request URL (observed up to 75 chars). Promoting it into
log_transactions.item_number (was varchar(64)) raised asyncpg StringDataRightTruncationError, which
aborted the whole Stage 2 batch; because finalize retries the oldest pending window first, that one
row stalled ALL transaction stitching for the tenant.

Covered:
- a 70-char ItemNumber now groups into a transaction with the value preserved (column widened to 128);
- a value beyond the column limit is defensively capped by _persist instead of raising;
- finalize_pending isolates a failing run: other runs still complete and their pending is consumed.
"""

import hashlib
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.services.mnp_log_ingestion.pipeline import derive_transactions as d

T = datetime(2026, 6, 26, 9, 0, 0, tzinfo=timezone.utc)


async def _make_request_entry(db, customer_code: str, item_number: str) -> None:
    """Insert one Job + one REQUEST LogEntry (as the parser would produce) carrying `item_number` in
    its request params. A lone request groups into its own (incomplete) transaction, which is enough
    to exercise _persist's promotion of item_number."""
    job = Job(customer_code=customer_code, filename="t.log", storage_key="k")
    db.add(job)
    await db.flush()
    raw = f"REQUEST for {customer_code} {item_number}"
    db.add(LogEntry(
        customer_code=customer_code, job_id=job.id, source_file="TMP-AZ-BEC02/eSmartServerLog.txt",
        line_number=1, timestamp=T, entry_type=LogEntryType.request,
        entry_hash=hashlib.sha256(raw.encode()).hexdigest(), level="DEBUG",
        message="REQUEST: http://h/api/balance/ItemEnquiry",
        fields={"url": "http://h/api/balance/ItemEnquiry",
                "params": {"MethodName": "ItemEnquiry", "User": "HWORREL", "Company": "915",
                           "WarehouseID": "1", "DeviceID": "9", "ItemNumber": item_number}},
    ))
    await db.flush()


async def test_overlength_item_number_groups_without_error(db):
    cc = "TEST_CHUNK9_A"
    item = "BEC|V1|105943|2607041120|6758001|534BEC|V1|105943|2607041120|6758001|534JIT"  # 75 chars
    assert len(item) > 64
    await _make_request_entry(db, cc, item)

    # must NOT raise (previously StringDataRightTruncationError) and must create the transaction
    stats = await d.regroup_window(db, cc, T - timedelta(seconds=1), T + timedelta(seconds=1), commit=False)
    assert stats["transactions_created"] == 1

    txn = (await db.execute(
        select(LogTransaction).where(LogTransaction.customer_code == cc))).scalars().one()
    assert txn.item_number == item          # full 75-char value preserved (column is now 128)
    assert txn.method == "ItemEnquiry"


async def test_value_beyond_column_limit_is_capped_not_raised(db):
    cc = "TEST_CHUNK9_B"
    item = "X" * 200                        # beyond even the widened 128
    await _make_request_entry(db, cc, item)

    stats = await d.regroup_window(db, cc, T - timedelta(seconds=1), T + timedelta(seconds=1), commit=False)
    assert stats["transactions_created"] == 1

    txn = (await db.execute(
        select(LogTransaction).where(LogTransaction.customer_code == cc))).scalars().one()
    assert txn.item_number == "X" * 128     # capped to the column width, no exception


async def test_finalize_isolates_failing_run(db, monkeypatch):
    """One poison run must not block the others: finalize records it under `failures`, still processes
    the good run, and consumes only the good run's pending. Reproduces the isolation guarantee that
    stops a single bad record from stalling all stitching."""
    cc = "TEST_CHUNK9_ISO"
    run_a_lo = T                                  # this run's window will be made to fail
    run_b_lo = T + timedelta(hours=2)             # > 2*pad away, so it stays a separate run
    for lo in (run_a_lo, run_b_lo):
        db.add(LogRegroupPending(customer_code=cc, range_start=lo, range_end=lo + timedelta(seconds=1)))
    await db.flush()

    calls = []

    async def fake_regroup_window(wdb, customer_code, lo, hi, commit=True):
        calls.append(lo)
        if lo == run_a_lo:
            raise RuntimeError("boom in run A")
        return {"mode": "window", "transactions_created": 1}

    monkeypatch.setattr(d, "regroup_window", fake_regroup_window)

    res = await d.finalize_pending(db, cc)        # must NOT raise
    assert run_a_lo in calls and run_b_lo in calls  # both runs were attempted (isolation, not abort)
    assert len(res.get("failures", [])) == 1
    assert res["failures"][0]["window_start"] == run_a_lo.isoformat()
    assert len(res["by_window"]) == 1             # only run B produced a window result
    assert res["pending_consumed"] == 1           # only run B's pending marked consumed
