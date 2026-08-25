"""Chunk 57 (R2 of docs/analytics-ml-architecture/final_architecture.md, 18b/18d): the registry API,
and the `show` switch finally wired to something.

Three switches, three very different consequences
-------------------------------------------------
    capture  gates the FACT. Irreversible, because entries expire at 60 days.
    show     gates the ROLLUP. Reversible at the cost of one recompute.
    expand   gates R4's per-record rows. Not built, so it changes nothing yet.

The asymmetry the ticket logic encodes
--------------------------------------
Turning `capture` ON needs the retention range re-examined, so the newly captured transaction gets
facts. Turning it OFF needs nothing re-examined: the existing facts are deliberately left alone (the
diff never sees them), so there is nothing for a fold to change. `show` needs a ticket in BOTH
directions, because rollups genuinely have to be recomputed either way.

Publishing unconditionally would re-fold the whole retention window every time somebody ticked
`expand`, which changes nothing until R4 exists.

The default that had to be corrected
------------------------------------
`show` originally defaulted FALSE, reasoned as "an unreviewed transaction never surprises a reader".
That was backwards. Discovery registers every transaction it sees, so the first fold after deploying R1
would have marked every EXISTING transaction hidden, and this chunk's rollup gate would then have
blanked every existing chart. Twenty-three tests caught it by folding to zero. Both defaults are now ON
and the review is SURFACED (`needs_review`) rather than enforced by hiding data.
"""

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, func, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry
from app.persistence.models.customer import Customer
from app.services.analytics import capture

from datetime import datetime, timedelta, timezone

CC = "test_chunk57"
T0 = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)

#: Handlers are called DIRECTLY rather than through TestClient, which is the convention chunk 52
#: documents and whose reason applies here too: TestClient runs the app on its own event loop, and
#: mixing that with pytest-asyncio's makes asyncpg raise "attached to a different loop". That is a
#: harness fault rather than a bug in the app, and it takes a while to misdiagnose as one.


# =============================================================== fixtures
async def _wipe():
    async with async_session() as db:
        # AnalyticsFact is in this list because two tests below plant facts, and the fixture that
        # forgot to clear them let one test's unnamed row leak into another's assertion. Same class of
        # isolation bug as chunk 45's, found the same way: a failure that looked like a code defect.
        for model in (AnalyticsTransactionRegistry, AnalyticsFieldRegistry,
                      AnalyticsPendingWindow, AnalyticsTenantState, AnalyticsFact):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    yield
    await _wipe()


async def _seed(*, with_state=True, capture_on=True, show=True, expand=False):
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="r2 probe", timezone="Europe/London"))
        db.add(AnalyticsTransactionRegistry(customer_code=CC,
                                            transaction_name="Brighton Stock Pick"))
        db.add(AnalyticsTransactionRegistry(customer_code=CC, transaction_name="Full Stock Count",
                                            capture=capture_on, show=show, expand=expand))
        for field, cap in (("resp.BaseUoM", True), ("resp.AccessToken", False),
                           ("resp.NewThing", False)):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="MMS060MI", source="response",
                                          field=field, captured=cap))
        if with_state:
            db.add(AnalyticsTenantState(customer_code=CC, source_watermark=T0,
                                        history_starts_at=T0 - timedelta(days=3)))
        await db.commit()


async def _tickets() -> int:
    async with async_session() as db:
        return await db.scalar(
            select(func.count()).select_from(AnalyticsPendingWindow).where(
                AnalyticsPendingWindow.customer_code == CC)) or 0


async def _txns():
    async with async_session() as db:
        return (await api.list_transaction_registry(customer=CC, db=db))["transactions"]


async def _fields(*, only_unreviewed=False, limit=500):
    # Query() defaults are FastAPI marker objects, so a direct handler call has to supply them
    # explicitly - the framework only resolves them when it is the one doing the calling.
    async with async_session() as db:
        return (await api.list_field_registry(only_unreviewed=only_unreviewed, limit=limit,
                                              customer=CC, db=db))["fields"]


async def _patch_txn(name, body):
    async with async_session() as db:
        return await api.set_transaction_switches(name, payload=body, customer=CC, db=db)


async def _patch_field(field_id, body):
    async with async_session() as db:
        return await api.set_field_capture(field_id, payload=body, customer=CC, db=db)


# =============================================================== 1. reading
async def test_the_transaction_list_reports_the_switches_and_review_state():
    await _seed()
    rows = await _txns()
    assert [x["transaction_name"] for x in rows] == ["Brighton Stock Pick", "Full Stock Count"]
    for x in rows:
        # Both defaults ON - see the module docstring for why `show` is not false here.
        assert x["capture"] is True and x["show"] is True and x["expand"] is False
        assert x["needs_review"] is True, "nobody has touched the switches yet"


async def test_an_empty_list_means_analytics_has_not_folded_yet():
    """Rows are created by the FOLD, never by this API. So empty means "nothing observed", which is a
    different thing from "nothing configured" - and the two look identical on a screen."""
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="r2 probe", timezone="Europe/London"))
        await db.commit()
    assert await _txns() == []


async def test_the_field_list_flags_credential_shaped_names_without_blocking_them():
    """Advice, not a block. The veto reserves the DECISION for a person, so the interface has to warn
    them - and the response can carry that warning safely, because there is no column in the table a
    value could live in."""
    await _seed()
    fields = await _fields()
    by_name = {f["field"]: f for f in fields}
    assert by_name["resp.AccessToken"]["credential_shaped"] is True
    assert by_name["resp.AccessToken"]["captured"] is False
    assert by_name["resp.BaseUoM"]["credential_shaped"] is False
    assert by_name["resp.BaseUoM"]["seeded"] is True
    assert fields[0]["captured"] is False, "unreviewed first: the tail is the point of the list"


async def test_the_field_list_never_carries_a_value():
    """Structural. No key in the response can hold one, so the review screen cannot leak a credential
    even for a field nobody has looked at yet."""
    await _seed()
    for f in await _fields():
        for forbidden in ("value", "sample", "example", "observed_value", "last_value"):
            assert forbidden not in f, f"must not expose {forbidden}"


async def test_only_unreviewed_filters_the_list():
    await _seed()
    assert len(await _fields()) == 3
    target = next(f for f in await _fields() if f["field"] == "resp.NewThing")
    await _patch_field(target["id"], {"captured": True})
    assert all(f["field"] != "resp.NewThing" for f in await _fields(only_unreviewed=True))


# =============================================================== 2. writing, and the ticket asymmetry
async def test_turning_capture_off_publishes_no_ticket():
    """Nothing needs re-examining. The existing facts are deliberately left alone - the diff never sees
    them - so a fold would find nothing to change and the ticket would be pure cost."""
    await _seed()
    before = await _tickets()
    r = await _patch_txn("Full Stock Count", {"capture": False})
    assert r["capture"] is False
    assert r["tickets_published"] == 0
    assert await _tickets() == before


async def test_turning_capture_on_publishes_tickets():
    """The direction that DOES need work: the transaction has no facts for the range it was off, so the
    retention window has to be re-examined for them to appear."""
    await _seed(capture_on=False)
    r = await _patch_txn("Full Stock Count", {"capture": True})
    assert r["capture"] is True
    assert r["tickets_published"] > 0


async def test_toggling_show_publishes_tickets_in_both_directions():
    """`show` gates the ROLLUPS, which have to be recomputed whichever way it moves. That recompute IS
    the cost the switch advertises, and it is why `show` is the reversible one."""
    await _seed()
    off = await _patch_txn("Full Stock Count", {"show": False})
    assert off["show"] is False and off["tickets_published"] > 0
    on = await _patch_txn("Full Stock Count", {"show": True})
    assert on["show"] is True and on["tickets_published"] > 0


async def test_toggling_expand_alone_publishes_nothing():
    """R4 is not built, so `expand` changes no stored data. Publishing here would re-fold the whole
    retention window for a switch that currently does nothing."""
    await _seed()
    before = await _tickets()
    r = await _patch_txn("Full Stock Count", {"expand": True})
    assert r["expand"] is True
    assert r["tickets_published"] == 0
    assert await _tickets() == before


async def test_a_patch_changes_only_the_keys_it_names():
    """A PUT would make "I toggled show" silently reassert capture and expand from whatever the client
    last read, which is how one person's stale tab undoes another's decision."""
    await _seed(capture_on=True, show=True, expand=True)
    got = await _patch_txn("Full Stock Count", {"show": False})
    assert got["show"] is False
    assert got["capture"] is True, "capture must be untouched"
    assert got["expand"] is True, "expand must be untouched"


async def test_patching_an_unknown_transaction_is_a_404_not_an_upsert():
    """A name analytics has never seen is almost always a typo. Creating a row for it would accept the
    typo silently and then measurably do nothing."""
    await _seed()
    with pytest.raises(HTTPException) as e:
        await _patch_txn("No Such Transaction", {"show": False})
    assert e.value.status_code == 404


async def test_a_switch_change_is_recorded_as_reviewed():
    await _seed()
    await _patch_txn("Full Stock Count", {"show": False, "reviewed_by": "amin"})
    row = next(x for x in await _txns() if x["transaction_name"] == "Full Stock Count")
    assert row["needs_review"] is False
    assert row["reviewed_by"] == "amin"


async def test_a_tenant_with_no_state_does_not_fail():
    """A tenant analytics has never folded has no watermark, so there is no range to ticket. The switch
    must still be settable; the fold picks it up the first time it runs."""
    await _seed(with_state=False)
    r = await _patch_txn("Full Stock Count", {"show": False})
    assert r["show"] is False
    assert r["tickets_published"] == 0


# =============================================================== 3. the show gate actually gates
async def test_hidden_names_reflects_the_switch():
    await _seed()
    assert await capture.hidden_names_for(CC) == frozenset()
    await _patch_txn("Full Stock Count", {"show": False})
    assert await capture.hidden_names_for(CC) == frozenset({"Full Stock Count"})


async def test_capture_and_show_are_independent_gates():
    """The one thing that must not be conflated: `show` is free and reversible, `capture` is not. A
    change to one must never move the other."""
    await _seed()
    await _patch_txn("Full Stock Count", {"show": False})
    assert await capture.suppressed_names_for(CC) == frozenset(), \
        "hiding a transaction must NOT stop capturing it, or a free switch became irreversible"
    assert await capture.hidden_names_for(CC) == frozenset({"Full Stock Count"})


async def test_a_hidden_transaction_is_excluded_from_a_rollup_read():
    """The gate at its actual application point. Facts stay exactly where they are; only the rollup
    read skips them, which is what makes the switch instant to reverse."""
    from app.services.analytics.rollups import _read_dirty_facts
    from app.persistence.models.analytics_fact import AnalyticsFact
    import uuid as _u
    await _seed()
    async with async_session() as db:
        for name in ("Full Stock Count", "Brighton Stock Pick"):
            db.add(AnalyticsFact(id=_u.uuid4(), customer_code=CC,
                                 source_transaction_id=_u.uuid4(), source_started_at=T0,
                                 source_version_hash="h" * 8, revision=1, event_time=T0,
                                 business_date=T0.date(), transaction_name=name,
                                 method="ConfirmPickLine", status="success",
                                 quantity_classification="pick", attributes={}, created_at=T0))
        await db.commit()
    async with async_session() as db:
        both = await _read_dirty_facts(db, CC, {T0}, {T0.date()}, frozenset())
        gated = await _read_dirty_facts(db, CC, {T0}, {T0.date()},
                                        frozenset({"Full Stock Count"}))
    assert {r["transaction_name"] for r in both} == {"Full Stock Count", "Brighton Stock Pick"}
    assert {r["transaction_name"] for r in gated} == {"Brighton Stock Pick"}


async def test_an_unnamed_transaction_survives_the_show_gate():
    """The unnamed rows are the connectivity probes. `x NOT IN (...)` is NULL for a NULL x and a row is
    kept only when the predicate is TRUE, so without an explicit IS NULL they would be silently dropped
    the moment any transaction was hidden."""
    from app.services.analytics.rollups import _read_dirty_facts
    from app.persistence.models.analytics_fact import AnalyticsFact
    import uuid as _u
    await _seed()
    async with async_session() as db:
        db.add(AnalyticsFact(id=_u.uuid4(), customer_code=CC, source_transaction_id=_u.uuid4(),
                             source_started_at=T0, source_version_hash="n" * 8, revision=1,
                             event_time=T0, business_date=T0.date(), transaction_name=None,
                             method="CheckServer", status="success",
                             quantity_classification="non_quantity", attributes={}, created_at=T0))
        await db.commit()
    async with async_session() as db:
        rows = await _read_dirty_facts(db, CC, {T0}, {T0.date()},
                                       frozenset({"Full Stock Count"}))
    assert any(r["transaction_name"] is None for r in rows)


# =============================================================== 4. field approval
async def test_approving_a_field_publishes_tickets():
    """Its values are in no existing fact, so the retention range has to be re-folded for them to
    appear. Approval is a row rather than a deploy - but it is not free either."""
    await _seed()
    target = next(f for f in await _fields() if f["field"] == "resp.NewThing")
    r = await _patch_field(target["id"], {"captured": True})
    assert r["captured"] is True
    assert r["tickets_published"] > 0


async def test_re_approving_an_already_approved_field_publishes_nothing():
    await _seed()
    target = next(f for f in await _fields() if f["field"] == "resp.BaseUoM")
    r = await _patch_field(target["id"], {"captured": True})
    assert r["tickets_published"] == 0


async def test_a_field_patch_without_captured_is_a_400():
    await _seed()
    fields = await _fields()
    with pytest.raises(HTTPException) as e:
        await _patch_field(fields[0]["id"], {"reviewed_by": "amin"})
    assert e.value.status_code == 400


async def test_a_credential_can_still_be_approved_deliberately():
    """The veto blocks AUTO-approval, not a person's decision. Making it un-overridable would be the
    wrong trade: somebody may have a legitimate reason, and a system that cannot express it invites a
    worse workaround."""
    await _seed()
    target = next(f for f in await _fields() if f["field"] == "resp.AccessToken")
    r = await _patch_field(target["id"], {"captured": True, "reviewed_by": "amin"})
    assert r["captured"] is True
