"""Chunk 235: a fact outlives its source, so a missing source row past retention withdraws nothing.

Source transactions are kept 60 days and facts much longer. The fact writer withdraws a stored
fact that has no source row, which is right while the source is still kept and wrong after
retention has dropped it: on 27 Nov 2026 a rollup rebuild queued tickets over September and the
writer withdrew 111,560 facts whose source had simply aged out.

Pinned here
-----------
    the rule       which stored facts outlive their source
    past           a ticket over a day past retention keeps the facts, the ledger and the rollups
    refold         the same when the ticket is a refold ticket
    inside         inside retention a missing source row still withdraws the fact
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select

from app.config.database import async_session
from app.persistence.models.analytics_fact import AnalyticsFact, AnalyticsFactLedger
from app.persistence.models.analytics_rollup import AnalyticsDailyRollup, AnalyticsHourlyRollup
from app.persistence.models.log_transaction import LogTransaction
from app.services.analytics import consume as n3
from app.settings import settings
from tests import test_rollup_place_chunk232 as base

CC = base.CC
clean = base.clean


async def _count(model, *where) -> int:
    async with async_session() as db:
        return int(await db.scalar(select(func.count()).select_from(model).where(
            model.customer_code == CC, *where)) or 0)


async def _source_ages_out() -> None:
    """What retention does: the source rows go, and nothing tells the fact writer."""
    async with async_session() as db:
        await db.execute(delete(LogTransaction).where(LogTransaction.customer_code == CC))
        await db.commit()


async def _folded_day() -> None:
    await base._register()
    await base._plant((0, "10.0", "915", "BRI"), (10, "7.0", "915", "BRI"))
    await n3.consume_tenant(CC)
    assert await _count(AnalyticsFact) == 2


def test_the_rule_names_the_stored_facts_that_outlive_their_source():
    kept_from = datetime(2026, 9, 28, tzinfo=timezone.utc)
    old, new = kept_from - timedelta(days=3), kept_from + timedelta(days=3)
    stored = [{"source_transaction_id": "a", "event_time": old},      # gone and past: outlives
              {"source_transaction_id": "b", "event_time": old},      # still in the source
              {"source_transaction_id": "c", "event_time": new},      # gone but inside retention
              {"source_transaction_id": "d", "event_time": None}]     # no time: never past
    kept = n3.facts_outliving_source(stored, {"b"}, kept_from)
    assert [f["source_transaction_id"] for f in kept] == ["a"]


@pytest.mark.parametrize("refold", [False, True])
async def test_a_ticket_past_retention_keeps_facts_ledger_and_rollups(monkeypatch, refold):
    monkeypatch.setattr(settings, "log_partition_retention_days", 1)
    await _folded_day()
    await _source_ages_out()
    await base._ticket(refold=refold)
    stats = await n3.consume_tenant(CC)
    assert stats["reversed"] == 0 and stats["outlived_source"] == 2
    assert await _count(AnalyticsFact) == 2
    assert await _count(AnalyticsFactLedger, AnalyticsFactLedger.reason == "reverse") == 0
    for model in (AnalyticsHourlyRollup, AnalyticsDailyRollup):
        assert await base._places(model) == {("915", "BRI"): Decimal("17")}


async def test_inside_retention_a_missing_source_row_still_withdraws_the_fact(monkeypatch):
    monkeypatch.setattr(settings, "log_partition_retention_days", 100_000)
    await _folded_day()
    await _source_ages_out()
    await base._ticket()
    stats = await n3.consume_tenant(CC)
    assert stats["reversed"] == 2 and stats["outlived_source"] == 0
    assert await _count(AnalyticsFact) == 0
    assert await base._places(AnalyticsHourlyRollup) == {}
