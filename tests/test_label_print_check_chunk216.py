"""Chunk 216 (D40): show label copies; flag a case label printed before its item was picked.

The incident (17 Nov 2026, Bicester, tmp-test): during Bulk Pick the picker pressed print on the
item screen BEFORE confirming the pick, then confirming the pick printed the same item again
automatically. The spare labels stayed on the mobile printer roll and were torn off for the next
item, so every later case got the previous item's label ("labels offset to what they were picking").

The log explorer showed nothing wrong: every print row carries the same SOFT status (a harmless
"Date code wasn't supplied" WARN), and the REQUEST line did not show how many copies were printed.

Rule under test: a `PrintPickedCaseLabel` is an early print when the same user's most recent
`ConfirmPickLine` in the 30 minutes before it is for a different item, or there is none.
Reprint transactions ("Reprint PICK Label") are deliberate and never flagged.
Measured on live data 10-17 Nov: the rule finds exactly the 6 cases from the raw logs plus 3 earlier
ones, and nothing else.
"""

import uuid
from datetime import date as date_type, datetime, timedelta, timezone

from app.persistence.models.job import Job
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.mnp_log_ingestion.label_print_check import (
    EarlyLabelPrint,
    is_case_label_print,
    load_early_label_prints,
)
from app.services.mnp_log_ingestion.render import render_transaction

D = date_type(2026, 11, 17)
T0 = datetime(2026, 11, 17, 13, 18, 41, tzinfo=timezone.utc)  # GMT in November, so UTC = UK time


def _txn(cc="cc", *, method, item=None, user="BPLAKUT", at=T0, name="Bulk Pick (Main)", copies=None,
         job_id=None):
    attrs = {"MethodName": method}
    if copies is not None:
        attrs["NumberOfCopies"] = copies
    return LogTransaction(
        id=uuid.uuid4(), customer_code=cc, job_id=job_id, date=D, started_at=at, ended_at=at,
        user_name=user, warehouse="BIC", method=method, item_number=item, transaction_name=name,
        endpoint_url=f"http://host/api/x/{method}", status=LogTransactionStatus.soft,
        attributes=attrs,
    )


# --------------------------------------------------------------------------- render: copies

def test_request_line_shows_copies_for_a_label_print():
    t = _txn(method="PrintPickedCaseLabel", item="101897", copies="5")
    out = render_transaction(t, [])
    assert "▶ REQUEST   WHLO BIC · Item 101897 · Copies 5 · user BPLAKUT" in out


def test_request_line_shows_copies_for_a_package_label():
    t = _txn(method="PrintPackageLabel", copies="1")
    t.delivery_number = "16924"
    assert "Delivery 16924 · Copies 1 · user BPLAKUT" in render_transaction(t, [])


def test_request_line_without_copies_is_unchanged():
    t = _txn(method="ConfirmPickLine", item="101897")
    assert "▶ REQUEST   WHLO BIC · Item 101897 · user BPLAKUT" in render_transaction(t, [])


# --------------------------------------------------------------------------- render: warning line

def test_early_print_line_is_appended_last_so_no_existing_line_moves():
    """Saved line comments are keyed `<txn id>#<line index>` and step deep links by step number;
    appending keeps every existing line at the same position."""
    t = _txn(method="PrintPickedCaseLabel", item="101897", copies="5")
    early = EarlyLabelPrint(item_number="101897", last_pick_item="100697",
                            last_pick_at=T0 + timedelta(seconds=1, milliseconds=711))
    plain = render_transaction(t, []).splitlines()
    flagged = render_transaction(t, [], early_print=early).splitlines()
    assert flagged[:-1] == plain
    assert flagged[-1] == ("  ⚠ PRINTED BEFORE PICK  item 101897 not picked yet"
                           " · last pick 100697 at 13:18:42.711")


def test_early_print_line_when_user_had_no_pick_before():
    t = _txn(method="PrintPickedCaseLabel", item="101897")
    early = EarlyLabelPrint(item_number="101897", last_pick_item=None, last_pick_at=None)
    out = render_transaction(t, [], early_print=early)
    assert out.splitlines()[-1] == ("  ⚠ PRINTED BEFORE PICK  item 101897 not picked yet"
                                    " · no pick in the 30 min before")


def test_no_warning_line_by_default():
    t = _txn(method="PrintPickedCaseLabel", item="101897")
    assert "PRINTED BEFORE PICK" not in render_transaction(t, [])


# --------------------------------------------------------------------------- candidate filter

def test_only_bulk_case_label_prints_are_candidates():
    assert is_case_label_print(_txn(method="PrintPickedCaseLabel", item="1"))
    reprint = _txn(method="PrintPickedCaseLabel", item="1", name="Reprint PICK Label")
    assert not is_case_label_print(reprint)
    assert not is_case_label_print(_txn(method="PrintPackageLabel"))
    assert not is_case_label_print(_txn(method="ConfirmPickLine", item="1"))
    assert not is_case_label_print(_txn(method="PrintPickedCaseLabel", item=None))
    assert not is_case_label_print(_txn(method="PrintPickedCaseLabel", item="1", user=None))


# ------------------------------------------------------------------ database: the 17 Nov sequence

async def _seed(db, cc, txns):
    job = Job(customer_code=cc, filename="f.log", storage_key="k")
    db.add(job)
    await db.flush()
    for t in txns:
        t.customer_code = cc
        t.job_id = job.id
    db.add_all(txns)
    await db.flush()
    return txns


def _s(seconds):
    return T0 + timedelta(seconds=seconds)


async def test_bicester_sequence_flags_exactly_the_two_manual_prints(db):
    cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    beer_pick = _txn(method="ConfirmPickLine", item="100697", at=_s(0))
    beer_print = _txn(method="PrintPickedCaseLabel", item="100697", at=_s(2), copies="3")
    apple_early = _txn(method="PrintPickedCaseLabel", item="101897", at=_s(33), copies="5")
    apple_pick = _txn(method="ConfirmPickLine", item="101897", at=_s(178))
    apple_print = _txn(method="PrintPickedCaseLabel", item="101897", at=_s(180), copies="3")
    lemon_early = _txn(method="PrintPickedCaseLabel", item="101977", at=_s(334), copies="3")
    lemon_pick = _txn(method="ConfirmPickLine", item="101977", at=_s(425))
    lemon_print = _txn(method="PrintPickedCaseLabel", item="101977", at=_s(429), copies="5")
    page = await _seed(db, cc, [beer_pick, beer_print, apple_early, apple_pick, apple_print,
                                lemon_early, lemon_pick, lemon_print])

    found = await load_early_label_prints(db, cc, page)

    assert set(found) == {apple_early.id, lemon_early.id}
    assert found[apple_early.id].last_pick_item == "100697"
    assert found[lemon_early.id].last_pick_item == "101897"
    assert found[apple_early.id].last_pick_at == beer_pick.started_at


async def test_previous_pick_off_the_page_still_counts(db):
    """The feed is paged and filtered; the pick that clears a print may not be on this page."""
    cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    pick = _txn(method="ConfirmPickLine", item="104528", at=_s(0))
    auto = _txn(method="PrintPickedCaseLabel", item="104528", at=_s(2))
    await _seed(db, cc, [pick, auto])

    assert await load_early_label_prints(db, cc, [auto]) == {}


async def test_another_users_pick_does_not_count(db):
    cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    other = _txn(method="ConfirmPickLine", item="104528", user="DJACKSON", at=_s(0))
    mine = _txn(method="PrintPickedCaseLabel", item="104528", user="BPLAKUT", at=_s(2))
    await _seed(db, cc, [other, mine])

    found = await load_early_label_prints(db, cc, [mine])
    assert found[mine.id].last_pick_item is None


async def test_another_tenants_pick_does_not_count(db):
    cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    other_cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    await _seed(db, other_cc, [_txn(method="ConfirmPickLine", item="104528", at=_s(0))])
    mine = _txn(method="PrintPickedCaseLabel", item="104528", at=_s(2))
    await _seed(db, cc, [mine])

    assert mine.id in await load_early_label_prints(db, cc, [mine])


async def test_pick_older_than_30_minutes_does_not_count(db):
    cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    old = _txn(method="ConfirmPickLine", item="104528", at=_s(0))
    late = _txn(method="PrintPickedCaseLabel", item="104528", at=_s(31 * 60))
    await _seed(db, cc, [old, late])

    assert late.id in await load_early_label_prints(db, cc, [late])


async def test_reprints_and_non_print_rows_are_never_flagged_or_queried(db):
    cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    reprint = _txn(method="PrintPickedCaseLabel", item="100274", at=_s(0),
                   name="Reprint PICK Label")
    pack = _txn(method="PrintPackageLabel", at=_s(1))
    await _seed(db, cc, [reprint, pack])

    assert await load_early_label_prints(db, cc, [reprint, pack]) == {}


async def test_empty_page_needs_no_query():
    assert await load_early_label_prints(None, "cc", []) == {}


# ------------------------------------------------------------------ endpoints: feed + detail agree

async def _seed_apple_early(db):
    cc = f"TESTCH216_{uuid.uuid4().hex[:6]}"
    beer_pick = _txn(method="ConfirmPickLine", item="100697", at=_s(0))
    apple_early = _txn(method="PrintPickedCaseLabel", item="101897", at=_s(33), copies="5")
    apple_pick = _txn(method="ConfirmPickLine", item="101897", at=_s(178))
    apple_print = _txn(method="PrintPickedCaseLabel", item="101897", at=_s(180), copies="3")
    await _seed(db, cc, [beer_pick, apple_early, apple_pick, apple_print])
    return cc, apple_early, apple_print


EARLY_LINE = ("  ⚠ PRINTED BEFORE PICK  item 101897 not picked yet"
              " · last pick 100697 at 13:18:41.000")


async def test_feed_flags_only_the_early_print(db):
    from app.api.v1.logs import view_transactions

    cc, _early, _auto = await _seed_apple_early(db)
    r = await view_transactions(customer=cc, db=db, pending={}, date=D, limit=50, offset=0,
                                user=None, hour=None, status=None, order_number=None,
                                item_number=None, verbose=False, reqid=None)
    body = r.body.decode()

    assert body.count("PRINTED BEFORE PICK") == 1
    assert EARLY_LINE in body
    assert "Item 101897 · Copies 5 · user BPLAKUT" in body
    assert "Item 101897 · Copies 3 · user BPLAKUT" in body


async def test_detail_view_and_json_render_the_same_text_as_the_feed(db):
    from app.api.v1.logs import get_transaction, get_transaction_view, view_transactions

    cc, early, auto = await _seed_apple_early(db)
    view = await get_transaction_view(transaction_id=early.id, customer=cc, verbose=False, db=db,
                                      pending={})
    detail = await get_transaction(transaction_id=early.id, customer=cc, db=db, pending={})
    auto_view = await get_transaction_view(transaction_id=auto.id, customer=cc, verbose=False,
                                           db=db, pending={})
    feed = (await view_transactions(customer=cc, db=db, pending={}, date=D, limit=50, offset=0,
                                    user=None, hour=None, status=None, order_number=None,
                                    item_number=None, verbose=False, reqid=None)).body.decode()

    assert EARLY_LINE in view
    assert detail["rendered"] in feed  # byte-for-byte, so step deep links keep their numbers
    assert view in feed
    assert "PRINTED BEFORE PICK" not in auto_view
