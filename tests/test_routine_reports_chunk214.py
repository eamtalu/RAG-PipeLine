"""Step 38 (chunk 214): the routine library's reports, each answering what happened, why, and what to do.

The owner asked (16-17 November 2026) for a library of routines for supervisors and managers, ready
for the demo, every figure from the data, a placeholder where no transaction exists, and the
existing pattern: the backend builds one row per site, the edge lays it out. Four routines are ready
(S1 health check, S4 dispatch readiness, S5 outstanding exceptions, M1 site performance); the other
eight are listed as planned."""

import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.advisory import Advisory
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.repositories.site_repository import SiteRepository
from app.services.teams import routine_reports as rr
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.teams.sites import Site
from tests.golden import plant

CC = "test_chunk214"
NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)  # 13:00 in London, the planted day
TODAY = date(2026, 9, 18)


def _delivery(number, *, route="R1", warehouse="BRI", status="open", tier="none", confirmed=0, expected=None,
              created=0, loaded=0, customer="Sample Customer"):
    return AnalyticsAtRiskDelivery(
        customer_code=CC, delivery_number=number, departure_date=TODAY, departure_at=NOW + timedelta(hours=3),
        route=route, warehouse=warehouse, status=status, tier=tier, lines_confirmed=confirmed, lines_expected=expected,
        lines_picked=confirmed, packages_created=created, packages_loaded=loaded, customer_name=customer,
        rule_version="v3", last_evaluated_at=NOW)


def _advisory(title, *, severity="high", agent="stock", site="BRI", recommended="Recount the location"):
    return Advisory(id=uuid.uuid4(), customer_code=CC, site_id=site, agent=agent, severity=severity, title=title,
                    recommended=recommended, status="open", dedup_key=uuid.uuid4().hex, source={}, about={}, actions=[])


# ==================================================== 1. the library


def test_the_library_lists_twelve_routines_in_six_categories_with_four_ready():
    lib = rr.library()
    assert len(lib) == 12
    assert {r["category"] for r in lib} == {"Daily operations", "Picking", "Inventory", "Dispatch", "Exceptions", "Workforce"}
    assert sorted(r["id"] for r in lib if r["status"] == "ready") == ["dispatch_readiness", "health_check",
                                                                       "outstanding_exceptions", "site_performance"]
    assert all({"id", "category", "role", "title", "answers", "status", "scope"} <= set(r) for r in lib)
    assert {r["role"] for r in lib} == {"Supervisor", "Manager"}


# ==================================================== 2. the rules


def test_the_health_check_says_what_needs_doing_in_order_of_urgency():
    facts = {"fill_rate": 0.92, "target": 0.98, "short_lines": 52, "to_fix": 2, "routes_out": 6, "routes_total": 8,
             "advisories_high": 3, "backlog": 5}
    findings, actions = rr.health_rules(facts)
    assert [f["title"] for f in findings] == ["2 deliveries need action now", "Fill rate 92.0% is under the 98% target",
                                              "2 of 8 routes still to leave", "3 high advisories open"]
    assert actions[0] == "Check the 2 flagged deliveries on the dispatch board before their vans leave."
    quiet = rr.health_rules({**facts, "fill_rate": 0.99, "to_fix": 0, "routes_out": 8, "advisories_high": 0})
    assert quiet[0] == [] and quiet[1] == ["Nothing needs action: keep an eye on the dispatch board."]


# ==================================================== 3. the reports on a planted tenant


@pytest.fixture
async def planted():
    await plant.plant_analytics(CC, week=True)
    async with async_session() as db:
        await SiteRepository(db).upsert(CC, "BRI", "Brighton", sort_order=1)
        await SiteRepository(db).upsert(CC, "LON", "London", sort_order=2)
        db.add_all([_delivery("D1", status="closed", route="R2"), _delivery("D2", confirmed=2, expected=5, tier="at_risk"),
                    _delivery("D3", confirmed=5, expected=5), _delivery("D4", confirmed=5, expected=5, created=2),
                    _delivery("D9", warehouse="LON")])
        db.add_all([_advisory("KALE at K03A: counted 6, system 9", recommended="Recount K03A"),
                    _advisory("Location M05A empty 7 times", agent="location", severity="medium", recommended="Check M05A")])
        await db.commit()
    yield
    async with async_session() as db:
        await db.execute(delete(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))
        await db.execute(delete(Advisory).where(Advisory.customer_code == CC))
        for site in ("BRI", "LON"):
            await SiteRepository(db).remove(CC, site)
        await db.commit()
    await plant.wipe_analytics(CC)


def _report(row, report_id):
    return row["reports"][report_id]


async def test_a_sites_row_carries_its_three_supervisor_reports_and_no_other_sites_figures(planted):
    async with async_session() as db:
        row = await rr.compute(db, CC, Site("BRI", "Brighton"), now=NOW)
    assert row["available"] is True and row["site_id"] == "BRI" and row["scope"] == "Brighton"
    assert set(row["reports"]) == {"health_check", "dispatch_readiness", "outstanding_exceptions"}
    for report in row["reports"].values():
        assert {"id", "title", "headline", "tiles", "findings", "actions", "placeholders"} <= set(report)
    health = _report(row, "health_check")
    tiles = {t["label"]: t for t in health["tiles"]}
    assert tiles["Routes departed"]["value"] == "1 / 2" and tiles["Open advisories"]["value"] == "2"
    assert [p["label"] for p in health["placeholders"]] == ["Orders received"]
    dispatch = {t["label"]: t["value"] for t in _report(row, "dispatch_readiness")["tiles"]}
    # D2 picking, D3 picked with no package, D4 packed and staged, D1 gone; London's D9 is not here
    assert dispatch["Not fully picked"] == "1" and dispatch["Awaiting packing"] == "1" and dispatch["Staged, not loaded"] == "1"
    exceptions = _report(row, "outstanding_exceptions")
    assert exceptions["actions"][:1] == ["Recount K03A"]  # the high advisory's own recommended action
    assert [p["label"] for p in exceptions["placeholders"]] == ["Unallocated orders"]
    # a site row never carries the manager's comparison of every site
    assert next(r for r in row["library"] if r["id"] == "site_performance")["status"] == "all_sites_only"


async def test_the_whole_customers_row_adds_site_performance(planted):
    async with async_session() as db:
        row = await rr.compute(db, CC, None, now=NOW)
    assert row["site_id"] == "all" and "site_performance" in row["reports"]
    perf = _report(row, "site_performance")
    assert [f["title"].split(" ·")[0] for f in perf["findings"][:2]] == ["Brighton", "London"]


async def test_the_sweep_mirrors_the_whole_customer_and_each_site(planted):
    mirror = InMemoryMirrorWriter()
    assert await rr.sweep_once(mirror, only=[CC]) == [CC]
    assert sorted(sk for _, sk in mirror.items) == ["SITE#BRI#REPORTS", "SITE#LON#REPORTS", "SITE#all#REPORTS"]


async def test_a_failing_mirror_writes_nothing_and_does_not_raise(planted):
    assert await rr.sweep_once(InMemoryMirrorWriter(fail=True), only=[CC]) == []


async def test_a_site_with_nothing_today_says_so_rather_than_on_track(planted):
    async with async_session() as db:
        await SiteRepository(db).upsert(CC, "BIC", "Bicester", sort_order=3)
        await db.commit()
        row = await rr.compute(db, CC, Site("BIC", "Bicester"), now=NOW)
        await SiteRepository(db).remove(CC, "BIC")
        await db.commit()
    health = row["reports"]["health_check"]
    assert health["headline"] == "No activity today" and health["actions"] == ["Nothing to report for this site today."]
