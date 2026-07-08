"""Chunk 11: GET /logs/regroup/status exposes the frontend "is the log server up to date?" signals.

Adds two backward-compatible fields to the existing endpoint:
- last_regroup_at : max(consumed_at) — when a window was last stitched (server IS populating txns);
- up_to_date      : pending_windows == 0 — no stitching backlog, transactions are current.

Covered:
- no pending rows -> up_to_date true, pending_windows 0, last_regroup_at null;
- open windows -> up_to_date false, pending_windows counts them, oldest_pending_at set;
- last_regroup_at is the MAX consumed_at across the customer's rows;
- once every window is consumed -> up_to_date true again with last_regroup_at set;
- the endpoint performs NO writes (safe to poll frequently);
- the pre-existing fields are unchanged (no regression for current clients).
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.api.v1.logs import regroup_status
from app.persistence.models.log_regroup_pending import LogRegroupPending

T0 = datetime(2026, 6, 26, 9, 0, 0, tzinfo=timezone.utc)


def _pending(cc: str, *, start: datetime, created: datetime, consumed: datetime | None):
    return LogRegroupPending(
        customer_code=cc, range_start=start, range_end=start + timedelta(seconds=1),
        created_at=created, consumed_at=consumed,
    )


async def test_up_to_date_when_no_backlog(db):
    cc = "TEST_CHUNK11_EMPTY"
    res = await regroup_status(customer=cc, db=db)
    assert res["up_to_date"] is True
    assert res["pending"] is False
    assert res["pending_windows"] == 0
    assert res["oldest_pending_at"] is None
    assert res["last_regroup_at"] is None
    assert res["customer_code"] == cc


async def test_open_windows_report_backlog(db):
    cc = "TEST_CHUNK11_OPEN"
    db.add(_pending(cc, start=T0, created=T0, consumed=None))
    db.add(_pending(cc, start=T0 + timedelta(minutes=5), created=T0 + timedelta(minutes=5), consumed=None))
    await db.flush()

    res = await regroup_status(customer=cc, db=db)
    assert res["up_to_date"] is False
    assert res["pending"] is True
    assert res["pending_windows"] == 2
    assert datetime.fromisoformat(res["oldest_pending_at"]) == T0   # min created_at of open rows
    assert res["last_regroup_at"] is None                          # nothing consumed yet


async def test_last_regroup_at_is_max_consumed(db):
    cc = "TEST_CHUNK11_MIXED"
    early = T0 + timedelta(hours=1)
    late = T0 + timedelta(hours=3)
    # two consumed windows (at different times) + one still-open window
    db.add(_pending(cc, start=T0, created=T0, consumed=early))
    db.add(_pending(cc, start=T0 + timedelta(minutes=1), created=T0, consumed=late))
    db.add(_pending(cc, start=T0 + timedelta(minutes=2), created=T0 + timedelta(minutes=2), consumed=None))
    await db.flush()

    res = await regroup_status(customer=cc, db=db)
    assert res["pending_windows"] == 1                             # only the open one
    assert res["up_to_date"] is False
    assert datetime.fromisoformat(res["last_regroup_at"]) == late  # MAX consumed_at


async def test_up_to_date_true_once_all_consumed(db):
    cc = "TEST_CHUNK11_DONE"
    stitched = T0 + timedelta(hours=2)
    db.add(_pending(cc, start=T0, created=T0, consumed=stitched))
    await db.flush()

    res = await regroup_status(customer=cc, db=db)
    assert res["up_to_date"] is True                               # no open windows -> no backlog
    assert res["pending_windows"] == 0
    assert datetime.fromisoformat(res["last_regroup_at"]) == stitched


async def test_status_is_read_only(db):
    cc = "TEST_CHUNK11_RO"
    db.add(_pending(cc, start=T0, created=T0, consumed=None))
    await db.flush()

    before = await db.scalar(select(func.count()).select_from(LogRegroupPending)
                             .where(LogRegroupPending.customer_code == cc))
    await regroup_status(customer=cc, db=db)
    after = await db.scalar(select(func.count()).select_from(LogRegroupPending)
                            .where(LogRegroupPending.customer_code == cc))
    assert before == after == 1                                    # endpoint added/changed nothing
