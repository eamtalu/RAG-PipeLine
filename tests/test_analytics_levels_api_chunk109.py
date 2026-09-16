"""Chunk 109: marking a field a level, and what that then refuses.

The arithmetic is pinned in `test_analytics_levels_chunk109.py`, with no database. This pins the
seam: that a person can record the marking, that the marking reaches the gate where metrics are
chosen, and - the part that matters most - that it does NOT reach the fold.

Measured on the live tenant: 73 on-hand readings of item 104353 add to 41,206 where 427 are on the
shelf, and every on-hand reading on the tenant adds to 340,206 where the stock is 18,248. Six fields
are levels and all six are ticked and available today.
"""

import pytest
from sqlalchemy import delete, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_field_meaning import AnalyticsFieldMeaning
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.customer import Customer
from app.services.analytics import capture, catalog, registry
from app.services.log_agent import analytics_tools
from app.services.analytics import definition as d
from fastapi import HTTPException

CC = "test_chunk109"
MODELS = (AnalyticsFieldMeaning, AnalyticsFieldRegistry, AnalyticsMetric)

#: The field this whole chunk is about. Ticked for capture, so approval is never what refuses it.
ON_HAND = "resp.QuantityOnHand"


async def _wipe():
    async with async_session() as db:
        for model in MODELS:
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="level probe", timezone="Europe/London"))
        for field in (ON_HAND, "CountedQuantity", "BalanceQuantity", "QuantityPicked"):
            db.add(AnalyticsFieldRegistry(customer_code=CC, method="GetOldestItemBalanceAPI",
                                          source="response", field=field, captured=True,
                                          seen_count=50))
        await db.commit()
    yield
    await _wipe()


def _body(**kw):
    """A metric that adds up on-hand stock, which is the thing this chunk exists to refuse."""
    measure = {"name": "on_hand", "aggregation": kw.pop("aggregation", "sum"),
               "field": kw.pop("field", f"attr:{ON_HAND}"), "unit": "units"}
    if kw.get("minus"):
        measure["minus"] = kw.pop("minus")
    return {"name": kw.pop("name", "stock"), "description": "Stock on hand by warehouse",
            "dimensions": ["warehouse"], "measures": [measure],
            "filter": {"methods": [], "transactions": []},
            "grains": ["hourly", "daily"], "source": "transaction",
            "status": kw.pop("status", "active"), **kw}


async def _mark(field: str, kind: str | None, **kw):
    async with async_session() as db:
        return await api.set_field_meaning(
            body={"field": field, "kind": kind, **kw}, customer=CC, db=db)


# ==================================================== 1. recording the marking

async def test_level_is_an_accepted_kind_and_is_offered():
    """The option has to exist before anybody can choose it, and has to be listed before anybody
    knows it exists."""
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        listing = await api.list_field_meanings(customer=CC, db=db)
    assert "level" in listing["kinds"]
    assert next(m for m in listing["meanings"] if m["field"] == ON_HAND)["kind"] == "level"


async def test_marking_a_level_keeps_its_unit():
    """427 UNITS on hand. A level is a quantity and has a unit exactly as a measure does.

    The screen used to send the unit only for `kind == "measure"`, so marking one of the six live
    fields a level would have silently wiped a unit somebody had typed.
    """
    await _mark(ON_HAND, "measure", description="Stock on the shelf", unit="units")
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        row = await db.scalar(select(AnalyticsFieldMeaning).where(
            AnalyticsFieldMeaning.customer_code == CC, AnalyticsFieldMeaning.field == ON_HAND))
    assert row.unit == "units"


async def test_the_loader_returns_the_full_namespaced_key():
    """`resp.QuantityOnHand` being a level says nothing about a request field spelled
    `QuantityOnHand`. A bare-name match would let a field inherit a marking nobody gave it."""
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        assert await capture.field_kinds(db, CC) == {ON_HAND: "level"}


async def test_a_field_marked_something_else_is_not_a_level():
    await _mark(ON_HAND, "measure")
    async with async_session() as db:
        assert await capture.field_kinds(db, CC) == {ON_HAND: "measure"}


# ==================================================== 2. the gate on the way in

async def test_creating_a_metric_that_adds_a_level_up_is_refused():
    """The 340,206 case, stopped at the only moment anybody knows enough to stop it."""
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        with pytest.raises(HTTPException) as caught:
            await api.create_metric(payload=_body(), customer=CC, db=db)
    assert caught.value.status_code == 400
    assert any(ON_HAND in p and "LEVEL" in p for p in caught.value.detail)


async def test_the_same_metric_is_accepted_before_anybody_marks_the_field():
    """Nothing in the values says the field is a level. Until a person says so, it is an ordinary
    number, and that is exactly why this chunk needs a human decision rather than a rule."""
    async with async_session() as db:
        out = await api.create_metric(payload=_body(), customer=CC, db=db)
    assert out["name"] == "stock"


async def test_averaging_a_level_is_accepted():
    """Mean stock over the window is a real answer, so the refusal must not read as "levels are
    unusable"."""
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        out = await api.create_metric(payload=_body(aggregation="average"), customer=CC, db=db)
    assert out["name"] == "stock"


async def test_previewing_one_is_not_ok_before_anything_is_saved():
    """The wizard's dry run has to say no too, or the person types a whole metric and only learns at
    the last press."""
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        out = await api.preview_metric(payload=_body(), window_hours=24, customer=CC, db=db)
    assert out["ok"] is False
    assert any(ON_HAND in p for p in out["problems"])


async def test_a_draft_that_adds_a_level_up_cannot_be_activated():
    """A draft has never folded a row, so going live for the first time is the metric's birth and the
    gate applies."""
    async with async_session() as db:
        out = await api.create_metric(payload=_body(status="draft"), customer=CC, db=db)
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        with pytest.raises(HTTPException) as caught:
            await api.update_metric(metric_id=out["id"], payload={"status": "active"},
                                    customer=CC, db=db)
    assert caught.value.status_code == 400


async def test_reshaping_a_draft_onto_a_level_is_refused():
    """Choosing the measure again is choosing it.

    Only a draft can be reshaped at all - an active metric's shape is frozen and a shape edit there
    is a 409 long before this rule is reached - so a reshape is always somebody still deciding.
    """
    async with async_session() as db:
        out = await api.create_metric(payload=_body(field="attr:QuantityPicked", status="draft"),
                                      customer=CC, db=db)
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        with pytest.raises(HTTPException) as caught:
            await api.update_metric(
                metric_id=out["id"],
                payload={"measures": [{"name": "on_hand", "aggregation": "sum",
                                       "field": f"attr:{ON_HAND}", "unit": "units"}]},
                customer=CC, db=db)
    assert caught.value.status_code == 400


# ==================================================== 3. what must NOT be refused

async def test_an_active_metric_that_adds_a_level_up_still_folds():
    """The decision-3 test at the seam.

    Somebody ticks a box on a describe form this morning. A metric that has been charted for months
    must not stop folding because of it. `active_definitions` skips a definition that fails
    validation, so it is deliberately never handed the level set.
    """
    async with async_session() as db:
        await api.create_metric(payload=_body(), customer=CC, db=db)
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        live = await registry.active_definitions(db, CC)
    assert [name for _, definition in live for name in [definition.name]] == ["stock"]


async def test_a_paused_metric_that_adds_a_level_up_can_be_restarted():
    """It already ran. Refusing it now would strand it paused for ever over a decision taken long
    after it was built, which is a worse outcome than the wrong word on a chart."""
    async with async_session() as db:
        out = await api.create_metric(payload=_body(), customer=CC, db=db)
        await api.update_metric(metric_id=out["id"], payload={"status": "inactive"},
                                customer=CC, db=db)
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        back = await api.update_metric(metric_id=out["id"], payload={"status": "active"},
                                       customer=CC, db=db)
    assert back["status"] == "active"


async def test_adding_up_the_difference_of_two_levels_is_accepted():
    """Count variance, chunk 101's flagship. A stock minus a stock is a change, and changes add."""
    await _mark("CountedQuantity", "level")
    await _mark("BalanceQuantity", "level")
    async with async_session() as db:
        out = await api.create_metric(
            payload=_body(name="variance", field="attr:CountedQuantity",
                          minus="attr:BalanceQuantity"), customer=CC, db=db)
    assert out["name"] == "variance"


async def test_marking_a_level_writes_only_the_meaning_row():
    """The marking is documentation, not a migration. No rollup is rebuilt and no metric is edited."""
    async with async_session() as db:
        out = await api.create_metric(payload=_body(), customer=CC, db=db)
        before = await db.scalar(select(AnalyticsMetric.updated_at).where(
            AnalyticsMetric.id == out["id"]))
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        after = await db.scalar(select(AnalyticsMetric.updated_at).where(
            AnalyticsMetric.id == out["id"]))
    assert before == after


# ==================================================== 4. what the catalogue then says

async def test_the_catalogue_says_which_fields_are_levels():
    """An agent reading this is the difference between "18,248 in stock" and "340,206 in stock"."""
    await _mark(ON_HAND, "level", description="Stock on the shelf right now", unit="units")
    async with async_session() as db:
        body = await catalog.build(db, CC)
    entry = next(f for f in body["fields"] if f["field"] == ON_HAND)
    assert entry["kind"] == "level"
    assert next(f for f in body["fields"] if f["field"] == "QuantityPicked")["kind"] is None


async def test_the_catalogue_says_a_measure_reads_a_level():
    """Published per measure, because the screen that must not print a summed level under the word
    "total" has the measure in hand and not the field list."""
    async with async_session() as db:
        await api.create_metric(payload=_body(aggregation="average"), customer=CC, db=db)
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        body = await catalog.build(db, CC)
    metric = next(m for m in body["metrics"] if m["name"] == "stock")
    assert metric["measures"][0]["level"] is True


async def test_a_difference_of_two_levels_is_not_itself_a_level():
    """A stock minus a stock is a change, and a change is an ordinary amount that adds up."""
    await _mark("CountedQuantity", "level")
    await _mark("BalanceQuantity", "level")
    async with async_session() as db:
        await api.create_metric(payload=_body(name="variance", field="attr:CountedQuantity",
                                              minus="attr:BalanceQuantity"), customer=CC, db=db)
        body = await catalog.build(db, CC)
    metric = next(m for m in body["metrics"] if m["name"] == "variance")
    assert metric["measures"][0]["level"] is False


# ==================================================== 5. what a reader is then told

def _measure(aggregation=d.Aggregation.average, field=f"attr:{ON_HAND}", minus=None):
    return d.Measure(name="on_hand", aggregation=aggregation, field=field, minus=minus)


def test_a_sum_is_never_reported_as_a_level():
    """Decision 3, at the reporting end. The level rule refuses a NEW sum and leaves an existing one
    running, so for that metric the total genuinely is the answer it was built to give. Relabelling
    it now would make an old chart unreadable without making any number more true."""
    kinds = {ON_HAND: "level"}
    assert api._measure_reads_a_level(_measure(d.Aggregation.sum), kinds) is False
    assert api._measure_reads_a_level(_measure(d.Aggregation.average), kinds) is True


def test_a_difference_of_two_levels_is_not_reported_as_a_level():
    """A stock minus a stock is a change, and a change adds like any other amount."""
    kinds = {"CountedQuantity": "level", "BalanceQuantity": "level"}
    both = _measure(field="attr:CountedQuantity", minus="attr:BalanceQuantity")
    assert api._measure_reads_a_level(both, kinds) is False


def test_an_unmarked_field_is_not_reported_as_a_level():
    assert api._measure_reads_a_level(_measure(), {}) is False
    assert api._measure_reads_a_level(_measure(field="quantity"), {ON_HAND: "level"}) is False
    assert api._measure_reads_a_level(
        d.Measure(name="n", aggregation=d.Aggregation.count), {ON_HAND: "level"}) is False


async def test_the_breakdown_says_its_measure_reads_a_level():
    """The screen showing the top-N has only this response to go on, and it must not head the value
    column with the measure's own name when that value is a summed level."""
    async with async_session() as db:
        await api.create_metric(payload=_body(aggregation="average"), customer=CC, db=db)
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        out = await api.analytics_breakdown(customer=CC, db=db, metric="stock", measure="on_hand",
                                            dimension="method", start=None, end=None, top=10)
    assert out["level"] is True


async def test_the_breakdown_of_an_ordinary_measure_is_not_a_level():
    async with async_session() as db:
        await api.create_metric(payload=_body(field="attr:QuantityPicked"), customer=CC, db=db)
        out = await api.analytics_breakdown(customer=CC, db=db, metric="stock", measure="on_hand",
                                            dimension="method", start=None, end=None, top=10)
    assert out["level"] is False


async def test_the_chat_agent_is_told_not_to_quote_the_component_sum():
    """The one consumer most likely to paste the number into a confident sentence.

    An average measure ships `sum_value` in its roles, because that is what the mean is divided from.
    An agent reading the tool output has no way to know that is machinery unless the output says so.
    """
    async with async_session() as db:
        await api.create_metric(payload=_body(aggregation="average"), customer=CC, db=db)
    await _mark(ON_HAND, "level")
    async with async_session() as db:
        out = await analytics_tools.query_metric(db, {"metric": "stock"}, CC)
    assert any("LEVEL" in note and "41,206" in note for note in out["notes"])


async def test_the_chat_agent_is_told_nothing_about_an_ordinary_measure():
    """A note on every metric is a note nobody reads."""
    async with async_session() as db:
        await api.create_metric(payload=_body(field="attr:QuantityPicked"), customer=CC, db=db)
        out = await analytics_tools.query_metric(db, {"metric": "stock"}, CC)
    assert not any("LEVEL" in note for note in out["notes"])


async def test_the_catalogue_says_which_aggregation_refuses_a_level():
    """Derived from the definition module, so the builder greys out exactly what the server refuses
    and no screen keeps a second copy of the rule."""
    async with async_session() as db:
        body = await catalog.build(db, CC)
    refusing = {a["name"] for a in body["aggregations"] if "level" in a["refuses_kinds"]}
    assert refusing == {"sum"}
    assert len(body["aggregations"]) == len(d.Aggregation)
