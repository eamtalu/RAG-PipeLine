"""Step 39 (chunk 215): /brief and /review in Ask, answered from the routine library's reports.

The owner asked (17 November 2026) for the commands in Ask. They are answered without the model:
the command is read before it (as "What can I ask" is) and the answer is the report the Routines
Library shows, drawn with Ask's own tiles and findings blocks. No tool and no tool description
changes, so no other question can be answered differently."""

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.repositories.site_repository import SiteRepository
from app.services.access.authorization.records import AccessRecord
from app.services.ask import briefs, slash
from app.services.teams.contracts import FindingsBlock, KpisBlock
from tests.golden import plant

CC = "test_chunk215"
NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
TODAY = date(2026, 9, 18)
ALL = {"view_own_site": "allowed", "view_all_sites": "allowed", "ask": "allowed"}
ADMIN = AccessRecord(CC, "u-1", "Sam Lee", "admin", "Admin", "active", True, (), ALL)
SUPERVISOR = AccessRecord(CC, "u-2", "Alex Morgan", "supervisor", "Supervisor", "active", False, ("BRI",),
                          {**ALL, "view_all_sites": "no"})


# ==================================================== 1. reading the commands


def test_brief_and_review_are_report_commands_and_the_others_are_unchanged():
    assert slash.resolve("/brief", ADMIN).report_command == "/brief"
    assert slash.resolve("/review", ADMIN).report_command == "/review"
    assert slash.resolve("/graph fill rate this week", ADMIN).text == "Show fill rate this week as a chart"
    assert slash.resolve("/graph fill rate this week", ADMIN).report_command is None
    assert "/brief" in slash.commands_for(SUPERVISOR) and "/review" in slash.commands_for(SUPERVISOR)
    assert slash.resolve("/nonsense", ADMIN).answer.startswith("I do not know /nonsense")
    assert set(briefs.REPORTS_BY_COMMAND) == set(slash.REPORT_COMMANDS)


def test_a_report_becomes_tiles_and_findings_within_the_blocks_limits():
    report = {"title": "Daily health check", "headline": "3 things need attention", "scope": "Brighton",
              "tiles": [{"label": f"Tile {i}", "value": str(i), "sub": "x", "tone": "attention"} for i in range(8)],
              "findings": [{"title": f"Finding {i}", "detail": "why", "tone": "attention"} for i in range(9)],
              "actions": [f"Action {i}" for i in range(7)],
              "placeholders": [{"label": "Orders received", "note": "Not in the WMS logs yet."}]}
    kpis, findings = briefs.blocks_of(report)
    KpisBlock(**kpis)
    FindingsBlock(**findings)
    assert len(kpis["items"]) == 6 and kpis["items"][0]["tone"] == "medium"
    assert len(findings["findings"]) == 6 and len(findings["recommendations"]) == 4
    assert findings["limitations"] == ["Orders received: not available yet. Not in the WMS logs yet."]


def test_a_report_with_nothing_to_flag_still_has_one_finding():
    report = {"title": "Daily health check", "headline": "On track", "scope": "Brighton", "tiles": [], "findings": [],
              "actions": ["Nothing needs action: keep an eye on the dispatch board."], "placeholders": []}
    kpis, findings = briefs.blocks_of(report)
    assert kpis is None and findings["findings"] == [{"text": "On track", "reasoning": None, "source": "routine library"}]


# ==================================================== 2. the answers on a planted tenant


@pytest.fixture
async def planted():
    await plant.plant_analytics(CC, week=True)
    async with async_session() as db:
        await SiteRepository(db).upsert(CC, "BRI", "Brighton", sort_order=1)
        db.add(AnalyticsAtRiskDelivery(customer_code=CC, delivery_number="D2", departure_date=TODAY,
                                       departure_at=NOW + timedelta(hours=3), route="R1", warehouse="BRI", status="open",
                                       tier="at_risk", lines_confirmed=2, lines_expected=5, rule_version="v3",
                                       last_evaluated_at=NOW))
        await db.commit()
    yield
    async with async_session() as db:
        await db.execute(delete(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))
        await SiteRepository(db).remove(CC, "BRI")
        await db.commit()
    await plant.wipe_analytics(CC)


async def test_brief_answers_the_health_check_and_dispatch_readiness_for_the_site(planted):
    async with async_session() as db:
        text, blocks = await briefs.answer(db, "/brief", CC, "BRI", SUPERVISOR, now=NOW)
    assert text.startswith("Brighton brief:")
    assert [b["type"] for b in blocks] == ["kpis", "findings", "kpis", "findings"]
    assert blocks[0]["title"] == "Daily health check" and blocks[2]["title"] == "Dispatch readiness check"
    for b in blocks:
        (KpisBlock if b["type"] == "kpis" else FindingsBlock)(**b)


async def test_review_is_for_all_sites_and_says_so_to_a_one_site_record(planted):
    async with async_session() as db:
        text, blocks = await briefs.answer(db, "/review", CC, "BRI", SUPERVISOR, now=NOW)
        assert blocks == [] and "every site" in text
        text, blocks = await briefs.answer(db, "/review", CC, "all", ADMIN, now=NOW)
    assert text.startswith("Site performance:") and blocks[0]["title"] == "Site performance"


async def test_brief_for_a_site_outside_the_record_is_refused(planted):
    async with async_session() as db:
        text, blocks = await briefs.answer(db, "/brief", CC, "all", SUPERVISOR, now=NOW)
    assert blocks == [] and "not one of your sites" in text
