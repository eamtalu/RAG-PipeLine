"""Chunk 89: the wizard's two semantic fields reach the row (metric builder part 5, backend half).

The wizard refuses an empty description and requires a unit on every numeric measure, because a chat
agent reading the catalog months later has nothing else to go on. Both were already READ by the catalog
(chunk 84); this pins that create WRITES them, and that a measure's unit survives the JSON round trip
through the registry.
"""
from datetime import timedelta

import pytest
from sqlalchemy import delete, select

from app.api.v1 import analytics as api
from app.config.database import async_session
from app.persistence.models.analytics_metric import AnalyticsMetric
from app.persistence.models.customer import Customer
from app.services.analytics import catalog
from app.services.analytics import definition as d
from app.services.analytics import registry

CC = "test_chunk89"


async def _wipe():
    async with async_session() as db:
        await db.execute(delete(AnalyticsMetric).where(AnalyticsMetric.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture(autouse=True)
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="semantics probe", timezone="UTC"))
        await db.commit()
    yield
    await _wipe()


BODY = {"name": "picks", "description": "  Units confirmed as picked, per method  ",
        "dimensions": ["method", "transaction_name"],
        "measures": [{"name": "quantity", "aggregation": "sum", "field": "quantity", "unit": "units"}],
        "filter": {"methods": ["ConfirmPickLine"], "transactions": []},
        "grains": ["hourly", "daily", "monthly"], "source": "transaction", "status": "active"}


async def test_create_stores_the_description_trimmed_and_returns_it():
    async with async_session() as db:
        out = await api.create_metric(payload=BODY, customer=CC, db=db)
    assert out["description"] == "Units confirmed as picked, per method"
    async with async_session() as db:
        row = await db.scalar(select(AnalyticsMetric).where(AnalyticsMetric.id == out["id"]))
        listed = await api.list_metrics(customer=CC, db=db, limit=50)
    assert row.description == "Units confirmed as picked, per method"
    assert next(m for m in listed["metrics"] if m["name"] == "picks")["description"] == row.description


async def test_a_blank_description_is_stored_as_null_not_empty_string():
    async with async_session() as db:
        out = await api.create_metric(payload={**BODY, "description": "   "}, customer=CC, db=db)
    assert out["description"] is None


async def test_a_measure_unit_survives_the_registry_round_trip():
    measure = d.Measure(name="quantity", aggregation=d.Aggregation.sum, field="quantity", unit="units")
    assert registry.measure_from_json(registry.measure_to_json(measure)) == measure
    bare = d.Measure(name="n", aggregation=d.Aggregation.count)
    assert "unit" not in registry.measure_to_json(bare), \
        "an absent unit is absent, so every pre-builder stored form is byte-identical to before"
    assert registry.measure_from_json(registry.measure_to_json(bare)).unit is None


async def test_the_catalog_shows_the_measure_unit_the_wizard_saved():
    async with async_session() as db:
        out = await api.create_metric(payload=BODY, customer=CC, db=db)
        body = await catalog.build(db, CC)
    metric = next(m for m in body["metrics"] if m["id"] == out["id"])
    assert metric["measures"][0]["unit"] == "units"
    assert metric["description"] == "Units confirmed as picked, per method"
