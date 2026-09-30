"""Chunk 132: the feed's filter, as one shared rule.

The ask box answers from the records the feed's filter fetched. So the filter the feed applies
(`GET /logs/transactions/view`) and the one the logspace agent reads through must be the same code:
`scope_from` turns the frontend's filters into a FeedScope (today when nothing was fetched) and
`scope_conditions` turns a FeedScope into the WHERE clauses both use.
"""

import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import func, select

from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.log_feed.scope import FeedScope, describe, scope_conditions, scope_from

TODAY = date_type(2026, 9, 30)


def test_nothing_fetched_means_today():
    s = scope_from(None, TODAY)
    assert s == FeedScope(day=TODAY, explicit=False)
    assert scope_from({}, TODAY) == s


def test_the_feed_filters_are_read_blanks_dropped_and_values_trimmed():
    s = scope_from({"date": "2026-09-29", "user": " PEVANS ", "hour": "6", "status": "error",
                    "orderNumber": "", "order_number": "0001234", "item_number": "  ", "reqid": "13-x",
                    "limit": 300, "verbose": True}, TODAY)
    assert s == FeedScope(day=date_type(2026, 9, 29), user="PEVANS", hour=6, status=LogTransactionStatus.error,
                          order_number="0001234", reqid="13-x", explicit=True)


def test_nonsense_values_are_ignored_not_trusted():
    s = scope_from({"date": "yesterday-ish", "hour": "25", "status": "broken", "user": 5}, TODAY)
    assert s.day == TODAY and s.hour is None and s.status is None and s.user == "5"


def test_the_frontend_camel_case_keys_are_understood():
    s = scope_from({"date": "2026-09-30", "orderNumber": "A1", "itemNumber": "I9"}, TODAY)
    assert s.order_number == "A1" and s.item_number == "I9"


def test_describe_says_which_records_in_words():
    assert describe(FeedScope(day=TODAY, explicit=False), today=TODAY) == "today (2026-09-30)"
    assert describe(FeedScope(day=date_type(2026, 9, 29), user="PEVANS", hour=6, status=LogTransactionStatus.error,
                              reqid="13-x", explicit=True), today=TODAY) == \
        "2026-09-29 · user PEVANS · hour 06:00 · status error · request 13-x"


async def _seed(db, cc):
    job = Job(customer_code=cc, filename="f.log", storage_key="k")
    db.add(job)
    await db.flush()
    rows = [
        dict(user_name="PEVANS", status=LogTransactionStatus.error, reqid="A", order_number="O1", item_number="I1", h=6),
        dict(user_name="PEVANS", status=LogTransactionStatus.success, reqid="B", order_number="O2", item_number="I1", h=7),
        dict(user_name="BCHAM", status=LogTransactionStatus.success, reqid="C", order_number="O1", item_number="I2", h=6),
    ]
    for r in rows:
        h = r.pop("h")
        db.add(LogTransaction(customer_code=cc, job_id=job.id, date=TODAY,
                              started_at=datetime(2026, 9, 30, h - 1, 30, tzinfo=timezone.utc), **r))
    await db.flush()


async def _count(db, cc, scope):
    return await db.scalar(select(func.count()).select_from(LogTransaction)
                           .where(*scope_conditions(cc, scope, "Europe/London")))


async def test_every_filter_narrows_the_set_like_the_feed(db):
    cc = f"TESTCH132_{uuid.uuid4().hex[:6]}"
    await _seed(db, cc)
    base = dict(day=TODAY, explicit=True)
    assert await _count(db, cc, FeedScope(**base)) == 3
    assert await _count(db, cc, FeedScope(**base, user="PEVANS")) == 2
    assert await _count(db, cc, FeedScope(**base, hour=6)) == 2          # 05:30 UTC = 06:30 London
    assert await _count(db, cc, FeedScope(**base, status=LogTransactionStatus.error)) == 1
    assert await _count(db, cc, FeedScope(**base, order_number="O1")) == 2
    assert await _count(db, cc, FeedScope(**base, item_number="I1")) == 2
    assert await _count(db, cc, FeedScope(**base, reqid="C")) == 1
    assert await _count(db, cc, FeedScope(**base, user="PEVANS", order_number="O1")) == 1
    assert await _count(db, cc, FeedScope(day=date_type(2026, 9, 29), explicit=True)) == 0


async def test_another_tenant_never_leaks_in(db):
    cc = f"TESTCH132_{uuid.uuid4().hex[:6]}"
    await _seed(db, cc)
    assert await _count(db, "SOMEONE_ELSE", FeedScope(day=TODAY, explicit=True)) == 0
