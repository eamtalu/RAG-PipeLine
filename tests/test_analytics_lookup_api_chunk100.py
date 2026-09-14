"""Chunk 100, the routes: declaring a lookup, editing one, and what each refuses.

Driven over ASGI on the TEST's own event loop rather than through `TestClient`. `TestClient` is
synchronous and opens a fresh loop per REQUEST, so a second call within one test finds the app's
pooled connection bound to the first loop, which asyncpg reports as "another operation is in
progress". The rest of this suite keeps its `TestClient` tests free of the database for that reason;
these tests are about database-backed routes, so they share the test's loop instead.
"""

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete

from app.config.database import async_session
from app.main import app
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue
from app.persistence.models.customer import Customer

CC = "test_chunk100_api"
BASE = "/api/v1/analytics"

VALID = {"name": "delivery", "key_field": "delivery_number",
         "description": "who a delivery is for",
         "attributes": [{"name": "customer_name", "stable": True, "sources": [
             {"method": "NewDeliveryPackage", "key_field": "delivery_number",
              "value_field": "CustomerName"},
             {"method": "GetNextDeliveryByRoute", "key_field": "resp.DeliveryNumber",
              "value_field": "resp.CustomerName"}]}]}


def _client() -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver",
                       headers={"X-Customer-Code": CC})


async def call(method: str, url: str, **kw):
    async with _client() as c:
        return await c.request(method, BASE + url, **kw)


async def _reset():
    async with async_session() as db:
        for model in (AnalyticsLookupValue, AnalyticsLookup):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        db.add(Customer(customer_code=CC, name="lookup route probe", timezone="Europe/London"))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _reset()
    yield
    await _reset()


# ==================================================================== what is refused

async def test_a_key_that_is_not_on_the_fact_row_is_refused():
    """Fails closed. A key nothing carries would key the whole lookup on nothing, and every answer
    would read as "not known" rather than as the mistake it is."""
    r = await call("POST", "/lookups", json={**VALID, "key_field": "nonesuch"})
    assert r.status_code == 400
    assert "not a field on the fact row" in r.json()["detail"]


async def test_a_source_missing_a_field_name_is_refused_and_says_which():
    """Both field names are explicit because the spelling genuinely differs per method: a delivery
    number is `DeliveryNumber` on a packing record and `resp.DeliveryNumber` on a routing one."""
    r = await call("POST", "/lookups", json={
        **VALID, "attributes": [{"name": "customer_name",
                                 "sources": [{"method": "NewDeliveryPackage"}]}]})
    assert r.status_code == 400
    assert "key_field" in r.json()["detail"] and "value_field" in r.json()["detail"]


async def test_a_lookup_with_no_attributes_is_refused():
    r = await call("POST", "/lookups", json={**VALID, "attributes": []})
    assert r.status_code == 400
    assert "no attributes" in r.json()["detail"]


async def test_a_name_with_a_dot_is_refused_because_the_dot_separates_the_attribute():
    r = await call("POST", "/lookups", json={**VALID, "name": "delivery.customer"})
    assert r.status_code == 400
    assert "dot" in r.json()["detail"]


async def test_a_duplicate_name_is_refused():
    assert (await call("POST", "/lookups", json=VALID)).status_code == 201
    assert (await call("POST", "/lookups", json=VALID)).status_code == 409


async def test_editing_a_lookup_that_does_not_exist_is_a_404():
    assert (await call("PATCH", "/lookups/nothing", json={"enabled": False})).status_code == 404


async def test_backfilling_a_lookup_that_does_not_exist_is_a_404():
    assert (await call("POST", "/lookups/nothing/backfill")).status_code == 404


async def test_the_suggester_refuses_a_key_that_is_not_on_the_fact():
    """Validated at the edge: the field name reaches SQL, and a typo must be a 400 rather than an
    empty answer somebody reads as "nothing to find"."""
    assert (await call("GET", "/lookups/suggest?key_field=nonesuch")).status_code == 400
    assert (await call("GET", "/lookups/suggest?key_field=delivery_number")).status_code == 200


# ==================================================================== what works

async def test_a_declaration_round_trips_through_the_list():
    created = await call("POST", "/lookups", json=VALID)
    assert created.status_code == 201, created.text
    assert created.json()["key_field"] == "delivery_number"

    listed = (await call("GET", "/lookups")).json()["lookups"]
    assert [row["name"] for row in listed] == ["delivery"]
    sources = listed[0]["attributes"][0]["sources"]
    assert {s["method"]: s["value_field"] for s in sources} == {
        "NewDeliveryPackage": "CustomerName",
        "GetNextDeliveryByRoute": "resp.CustomerName"}
    assert listed[0]["values"] == 0, "declared, nothing harvested yet"


async def test_a_lookup_can_be_switched_off_without_losing_what_it_learned():
    """Harvested values are history. Switching off stops the harvest and takes the lookup out of the
    groupings; it does not delete what was already true."""
    assert (await call("POST", "/lookups", json=VALID)).status_code == 201
    off = await call("PATCH", "/lookups/delivery", json={"enabled": False})
    assert off.status_code == 200 and off.json()["enabled"] is False
    assert (await call("GET", "/lookups")).json()["lookups"][0]["enabled"] is False


async def test_an_edit_that_breaks_the_declaration_is_refused_and_changes_nothing():
    assert (await call("POST", "/lookups", json=VALID)).status_code == 201
    assert (await call("PATCH", "/lookups/delivery",
                       json={"key_field": "nonesuch"})).status_code == 400
    listed = (await call("GET", "/lookups")).json()["lookups"]
    assert listed[0]["key_field"] == "delivery_number"


async def test_a_source_can_be_added_to_an_existing_lookup():
    """The normal way a lookup improves: a method nobody had noticed turns out to name the same key."""
    assert (await call("POST", "/lookups", json=VALID)).status_code == 201
    added = await call("PATCH", "/lookups/delivery", json={"attributes": [
        {"name": "customer_name", "stable": True, "sources": [
            {"method": "NewDeliveryPackage", "key_field": "delivery_number",
             "value_field": "CustomerName"},
            {"method": "PrintPackageLabel", "key_field": "delivery_number",
             "value_field": "CustomerName"}]}]})
    assert added.status_code == 200
    methods = {s["method"] for s in added.json()["attributes"][0]["sources"]}
    assert methods == {"NewDeliveryPackage", "PrintPackageLabel"}


async def test_the_values_endpoint_answers_for_a_lookup_with_nothing_yet():
    assert (await call("POST", "/lookups", json=VALID)).status_code == 201
    assert (await call("GET", "/lookups/delivery/values")).json()["values"] == []
