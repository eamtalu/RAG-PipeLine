"""Step 34 (chunk 212): the all-sites view of the Teams Overview, for the C-level reader.

The owner asked (16 November 2026) for one view of every site when "All sites" is chosen: the same
flow side by side per site, fill rate by site by day, the fill rate of the last 8 weeks against the
target and what changed since last week in plain facts. Every figure is read from what the app
already holds (the settled releases, the at-risk deliveries, the advisories); nothing without a
source is shown (no shifts, no rosters)."""

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import delete

from app.config.database import async_session
from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.persistence.repositories.site_repository import SiteRepository
from app.services.teams import all_sites as av
from app.services.teams.mirror import InMemoryMirrorWriter
from tests.golden import plant

CC = "test_chunk212"
NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)  # 13:00 in London, the planted day
TODAY = date(2026, 9, 18)


def _delivery(number: str, *, route="R1", warehouse="BRI", status="open", tier="none", confirmed=0, expected=None,
              created=0, loaded=0, loading_expected=True, checked_by=None) -> AnalyticsAtRiskDelivery:
    return AnalyticsAtRiskDelivery(
        customer_code=CC, delivery_number=number, departure_date=TODAY, departure_at=NOW + timedelta(hours=3),
        route=route, warehouse=warehouse, status=status, tier=tier, lines_confirmed=confirmed, lines_expected=expected,
        lines_picked=confirmed, packages_created=created, packages_loaded=loaded, loading_expected=loading_expected,
        checked_by=checked_by, rule_version="v3", last_evaluated_at=NOW)


# ==================================================== 1. the flow of one site's departures today


def test_each_delivery_sits_in_one_stage_of_the_flow():
    assert av.stage_of(_delivery("1")) == "to_pick"
    assert av.stage_of(_delivery("2", confirmed=3, expected=10)) == "picking"
    # with the expected count unknown, any confirmed line means picked, as the at-risk board judges it
    assert av.stage_of(_delivery("3", confirmed=3)) == "staged"
    assert av.stage_of(_delivery("4", confirmed=10, expected=10, created=4)) == "staged"
    assert av.stage_of(_delivery("5", confirmed=10, expected=10, created=4, loaded=1)) == "loading"
    assert av.stage_of(_delivery("6", confirmed=10, loading_expected=False)) == "staged"
    assert av.stage_of(_delivery("7", status="closed", confirmed=10, loaded=4)) == "departed"


def test_the_flow_counts_the_stages_and_the_routes_whose_van_has_gone():
    rows = [_delivery("1"), _delivery("2", confirmed=3, expected=10), _delivery("3", status="closed", route="R2"),
            _delivery("4", status="closed", route="R2"), _delivery("5", status="closed", route="R1")]
    out = av.flow(rows)
    assert out["stages"] == {"to_pick": 1, "picking": 1, "staged": 0, "loading": 0, "departed": 3}
    assert out["total"] == 5
    # R2's deliveries have all gone; R1 still has two open
    assert (out["routes_out"], out["routes_total"]) == (1, 2)


def test_the_status_counts_open_deliveries_flagged_and_who_checked_them():
    rows = [_delivery("1", tier="left_behind", checked_by="Sam Lee"), _delivery("2", tier="at_risk"),
            _delivery("3", tier="watch"), _delivery("4", tier="left_behind", status="closed")]
    assert av.status_of(rows) == {"to_fix": 2, "checked_by": ["Sam Lee"], "state": "attention"}
    assert av.status_of([_delivery("1", tier="watch")])["state"] == "on_track"


# ==================================================== 2. what changed, in plain facts


def _m(fill, fill_prev, on_time=0.95, on_time_prev=0.95, lines=1000, lines_prev=1000, pickers=10, pickers_prev=10,
       departures=40, left_behind=2):
    return {"fill_rate": fill, "fill_rate_prev": fill_prev, "on_time": on_time, "on_time_prev": on_time_prev,
            "lines": lines, "lines_prev": lines_prev, "pickers": pickers, "pickers_prev": pickers_prev,
            "departures": departures, "left_behind": left_behind}


def test_a_fill_rate_fall_is_said_with_the_days_under_the_band():
    out = av.what_changed([("BRI", "Brighton", _m(0.962, 0.976), [0.95, 0.97, 0.955, 0.98, 0.958, 0.99, 0.94])],
                          _m(0.962, 0.976))
    assert out[0] == {"direction": "down", "title": "Brighton fill rate fell to 96.2%.",
                      "detail": "4 of 7 days under 96%.", "delta_text": "−1.4 pts"}


def test_on_time_volume_steady_and_quiet_sites_each_get_their_line():
    sites = [("BRI", "Brighton", _m(0.981, 0.98, on_time=0.96, on_time_prev=0.939), [0.98] * 7),
             ("LON", "London", _m(0.979, 0.98), [0.98] * 7),
             ("BIC", "Bicester", None, [None] * 7)]
    out = av.what_changed(sites, _m(0.98, 0.98, lines=31440, lines_prev=30230, pickers=20, pickers_prev=21))
    titles = [x["title"] for x in out]
    assert titles == ["Brighton on-time dispatch rose to 96.0%.", "Volume grew 4%.",
                      "London held steady on every measure.", "Bicester: no activity in either week."]
    assert out[0]["delta_text"] == "+2.1 pts" and out[1]["detail"] == "31,440 lines across the sites (was 30,230)."  # no headcount: the reader judges staffing (owner, 16 Nov)
    assert out[2]["direction"] == "steady" and out[3]["direction"] == "none"


# ==================================================== 3. the row on a planted tenant


@pytest.fixture
async def planted():
    await plant.plant_analytics(CC, week=True)
    async with async_session() as db:
        await SiteRepository(db).upsert(CC, "BRI", "Brighton", sort_order=1)
        await SiteRepository(db).upsert(CC, "LON", "London", sort_order=2)
        db.add_all([_delivery("D1", status="closed"), _delivery("D2", confirmed=2, expected=5, tier="at_risk"),
                    _delivery("D9", warehouse="LON")])
        await db.commit()
    yield
    async with async_session() as db:
        await db.execute(delete(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == CC))
        for site in ("BRI", "LON"):
            await SiteRepository(db).remove(CC, site)
        await db.commit()
    await plant.wipe_analytics(CC)


async def test_the_row_holds_every_site_with_its_flow_by_day_weeks_and_changes(planted):
    async with async_session() as db:
        row = await av.compute(db, CC, now=NOW)
    assert row["available"] is True and row["target_fill_rate"] == 0.98
    assert [s["site_id"] for s in row["sites"]] == ["BRI", "LON"]
    bri, lon = row["sites"]
    assert bri["flow"]["stages"]["departed"] == 1 and bri["flow"]["stages"]["picking"] == 1
    assert bri["status"]["to_fix"] == 1 and lon["flow"]["total"] == 1
    # the golden settlement carries no warehouse, so the sites have no releases of their own
    assert bri["has_activity"] is True and bri["today"]["lines"] == 0
    assert len(row["by_day"]["labels"]) == 7 and row["by_day"]["labels"][-1] == "2026-09-18"
    assert row["by_day"]["all"][-1] is not None and set(row["by_day"]["sites"]) == {"BRI", "LON"}
    assert len(row["weeks"]["labels"]) == 8 and row["weeks"]["labels"][-1] == "2026-09-14"
    assert isinstance(row["changes"], list)


async def test_the_sweep_mirrors_one_row_per_customer(planted):
    mirror = InMemoryMirrorWriter()
    assert await av.sweep_once(mirror, only=[CC]) == [CC]
    assert list(mirror.items) == [(f"EYE#{CC}", "SITE#all#SITES")]


async def test_a_failing_mirror_writes_nothing_and_does_not_raise(planted):
    assert await av.sweep_once(InMemoryMirrorWriter(fail=True), only=[CC]) == []
