"""Chunk 182: "Ask why" on an advisory answers from the facts (fix of 13 November 2026).

The edge sends the advisory id as the question's context; the consumer frames the question with
it; the `explain_advisory` tool returns the finding's own words and the rows the agent read, so
the model has something true to say instead of searching the logs until its iteration cap; and a
draft that ran out of tool rounds is replaced by the plain sentence, whatever the model wrote.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_fact import AnalyticsFact
from app.persistence.models.customer import Customer
from app.persistence.models.tenant_site import TenantSite
from app.services.advisories import explain, store
from app.services.advisories.agents import location as location_agent
from app.services.advisories.agents import stock as stock_agent
from app.services.analytics_agent import advisory_tools, namespaces
from app.services.teams.contracts import QuestionJob
from app.teams_consumer import framed_question

CC = "test_c182"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 13, 13, 0, tzinfo=timezone.utc)


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(AnalyticsFact).where(AnalyticsFact.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C182", timezone="Europe/London"))
        s.add(TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1))
        await s.commit()
    yield
    await _wipe()


def _fact(method: str, when: datetime, *, item: str, attributes: dict, classification: str | None = None,
          user: str = "PEVANS", delivery: str | None = None) -> AnalyticsFact:
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.astimezone(TZ).date(),
        transaction_name="Quick Stock Count" if method == "ReportCount" else "JIT and Shorts Pick", method=method,
        status="success", quantity_classification=classification, warehouse="BRI", item_number=item,
        user_name=user, delivery_number=delivery, attributes=attributes, created_at=when)


async def test_a_count_advisory_is_explained_from_the_counts_and_the_picks_at_the_location():
    async with async_session() as db:
        db.add_all([
            _fact("ReportCount", NOW - timedelta(hours=2), item="104341",
                  attributes={"Location": "L03B", "CountedQuantity": "0", "BalanceQuantity": "0.465"}, user="JBURCH"),
            _fact("ConfirmPickLine", NOW - timedelta(hours=3), item="104341", classification="attempt", delivery="D-9",
                  attributes={"FromLocation": "L03B", "ExpectedQuantity": "2", "QuantityPicked": "0"}),
            _fact("ConfirmPickLine", NOW - timedelta(hours=4), item="999", classification="pick",
                  attributes={"FromLocation": "L03B", "ExpectedQuantity": "5", "QuantityPicked": "5"}),
        ])
        await db.commit()
        findings = await stock_agent.run(db, CC, NOW, TZ)
        await store.reconcile(db, CC, "stock", findings, now=NOW)
        await db.commit()
        row = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one()
        out = await explain.explain(db, CC, str(row.id), now=NOW)
    assert out["advisory"]["title"] == "Item 104341 at L03B counted empty, system has 0.465"
    assert out["facts"]["counts_today"] == [{"at": "11:00", "by": "JBURCH", "counted": "0", "system": "0.465"}]
    assert out["facts"]["picks_from_location_today"] == [
        {"at": "10:00", "by": "PEVANS", "item": "104341", "expected": "2", "picked": "0", "kind": "attempt", "delivery": "D-9"}]
    assert "nothing more in the logs" in out["note"]


async def test_a_location_advisory_lists_the_zero_picks_per_item_and_an_unknown_id_is_named():
    async with async_session() as db:
        for i in range(4):
            db.add(_fact("ConfirmPickLine", NOW - timedelta(minutes=10 * i), item=f"10{i % 2}", classification="attempt",
                         attributes={"FromLocation": "H02B", "ExpectedQuantity": "5", "QuantityPicked": "0"}))
        await db.commit()
        findings = await location_agent.run(db, CC, NOW, TZ)
        await store.reconcile(db, CC, "location", findings, now=NOW)
        await db.commit()
        row = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one()
        out = await explain.explain(db, CC, str(row.id), now=NOW)
        assert out["facts"]["zero_picks_by_item_today"] == [{"item": "100", "zero_picks": 2}, {"item": "101", "zero_picks": 2}]
        assert len(out["facts"]["picks_from_location_today"]) == 4
        # the tool answers with the same, and names a bad id
        raw = json.loads(await advisory_tools.run_advisory_tool(db, CC, "explain_advisory", {"advisory_id": str(row.id)}))
        assert raw["advisory"]["id"] == str(row.id) and raw["facts"]["zero_picks_by_item_today"][0]["zero_picks"] == 2
        assert "no such advisory" in json.loads(await advisory_tools.run_advisory_tool(db, CC, "explain_advisory", {"advisory_id": str(uuid.uuid4())}))["error"]
        assert "required" in json.loads(await advisory_tools.run_advisory_tool(db, CC, "explain_advisory", {}))["error"]


def test_the_tool_is_in_the_analytics_namespace_and_the_question_is_framed_with_the_id():
    names = [s.name for s in namespaces.analytics_namespace().specs]
    assert "explain_advisory" in names
    job = QuestionJob(job_id="j", tenant_id="t", customer_code=CC, conversation_id="c", question="Why is it empty?",
                      sender_object_id="u", context={"advisory_id": "a-1"})
    assert framed_question(job) == "Why is it empty?\n\n(About advisory a-1: call explain_advisory with this id first.)"
    assert framed_question(QuestionJob(job_id="j", tenant_id="t", customer_code=CC, conversation_id="c",
                                       question="How many?", sender_object_id="u")) == "How many?"
