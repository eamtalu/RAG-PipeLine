"""Chunk 13: GET /logs/transactions/view is date-scoped, ascending, and paginated.

- `date` is REQUIRED — the endpoint 422s without it;
- one page of a day's transactions is returned oldest -> newest (from 00:00), sliced by limit/offset;
- pagination metadata is returned in X-Total-Count / X-Offset / X-Limit / X-Page / X-Page-Count headers.

The ordering/offset test calls the handler directly against the rolled-back `db` fixture (repo style).
The date-required test uses a TestClient with the auth/session deps overridden so it exercises only
FastAPI's required-query-param validation (no DB, no app lifespan / background workers).
"""

import uuid
from datetime import datetime, date as date_type, timezone

from fastapi.testclient import TestClient

from app.api.deps import get_current_customer
from app.api.v1.logs import view_transactions, read_pending_state
from app.config.database import get_session
from app.main import app
from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus

D = date_type(2026, 6, 26)


async def _seed(db, cc: str, n: int) -> list[LogTransaction]:
    """A job + n transactions on date D at ascending times 01:00, 02:00, ... (returned in that order)."""
    job = Job(customer_code=cc, filename="f.log", storage_key="k")
    db.add(job)
    await db.flush()
    txns: list[LogTransaction] = []
    for i in range(n):
        tx = LogTransaction(
            customer_code=cc, job_id=job.id, date=D,
            started_at=datetime(2026, 6, 26, 1 + i, 0, 0, tzinfo=timezone.utc),
            status=LogTransactionStatus.success,
        )
        db.add(tx)
        txns.append(tx)
    await db.flush()
    return txns


async def test_view_is_ascending_and_paginated(db):
    cc = f"TESTCH13_{uuid.uuid4().hex[:6]}"
    txns = await _seed(db, cc, 3)

    # page 1: limit 2, offset 0 -> oldest two (01:00, 02:00), 03:00 excluded
    # NOTE: calling the handler directly bypasses FastAPI's resolution of Query(default=...) defaults,
    # so the optional params must be passed explicitly as None/False (otherwise they are Query sentinels).
    r1 = await view_transactions(customer=cc, db=db, pending={}, date=D, limit=2, offset=0,
                                 user=None, hour=None, status=None, verbose=False)
    b1 = r1.body.decode()
    assert r1.headers["X-Total-Count"] == "3"
    assert r1.headers["X-Limit"] == "2"
    assert r1.headers["X-Offset"] == "0"
    assert r1.headers["X-Page"] == "1"
    assert r1.headers["X-Page-Count"] == "2"
    assert b1.index(str(txns[0].id)) < b1.index(str(txns[1].id))   # oldest -> newest
    assert str(txns[2].id) not in b1                                # not on page 1

    # page 2: the remaining one (03:00)
    r2 = await view_transactions(customer=cc, db=db, pending={}, date=D, limit=2, offset=2,
                                 user=None, hour=None, status=None, verbose=False)
    b2 = r2.body.decode()
    assert r2.headers["X-Page"] == "2"
    assert str(txns[2].id) in b2
    assert str(txns[0].id) not in b2 and str(txns[1].id) not in b2


async def test_view_empty_page_still_reports_total(db):
    cc = f"TESTCH13_{uuid.uuid4().hex[:6]}"
    await _seed(db, cc, 2)
    # offset past the end -> empty page, but the total (and header) still reflect the day
    r = await view_transactions(customer=cc, db=db, pending={}, date=D, limit=50, offset=50,
                                user=None, hour=None, status=None, verbose=False)
    assert r.headers["X-Total-Count"] == "2"
    assert "(no transactions on this page)" in r.body.decode()


def test_view_requires_date():
    """Missing `date` -> 422 (FastAPI required-query-param validation), naming `date`."""
    app.dependency_overrides[get_current_customer] = lambda: "TESTCC"
    app.dependency_overrides[read_pending_state] = lambda: {}

    async def _no_session():
        yield None

    app.dependency_overrides[get_session] = _no_session
    try:
        client = TestClient(app)  # no `with` -> app lifespan / background workers do NOT start
        missing = client.get("/api/v1/logs/transactions/view")
        assert missing.status_code == 422
        locs = [".".join(str(p) for p in e.get("loc", [])) for e in missing.json()["detail"]]
        assert any(loc.endswith("date") for loc in locs), locs
    finally:
        app.dependency_overrides.clear()
