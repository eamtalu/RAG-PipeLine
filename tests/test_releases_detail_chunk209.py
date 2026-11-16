"""Step 31 (chunk 209): the Teams Pick releases page's row, the web Pick releases page's panels.

The owner asked (16 November 2026) for the web app's Pick releases page in Teams, opened from
Overview, with fixed windows and every panel. The web page computes its panels in the browser from
the settled rows; `releases_stats` is that arithmetic in Python and `releases_detail` reads the same
rows through the same functions as the web page's API, so the two pages agree (a parity script runs
the web page's own code on the live rows; see the runbook)."""

import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from app.config.database import async_session
from app.persistence.repositories.site_repository import SiteRepository
from app.services.teams import releases_detail as rd
from app.services.teams import releases_stats as rs
from app.services.teams.mirror import InMemoryMirrorWriter
from app.services.teams.sites import Site
from tests.golden import plant

CC = "test_chunk209"
LONDON = ZoneInfo("Europe/London")
NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)  # 13:00 in London, the planted day


@pytest.fixture(autouse=True)
async def clean():
    await plant.plant_analytics(CC, week=True)
    yield
    await plant.wipe_analytics(CC)
    async with async_session() as db:
        await SiteRepository(db).remove(CC, "BRI")
        await db.commit()


def _release(at: str, *, picker="P1", tx="Brighton Stock Pick", expected=4, picked=4, duration="60", delivery="D1",
             customer="C1", item="I1", refused="0") -> rs.Release:
    return rs.to_release({"key": [at], "event_time": at, "transaction_name": tx, "user_name": picker, "delivery_number": delivery,
                          "item_number": item, "calls": 1, "looked_up": {"delivery.customer_name": customer},
                          "attributes": {"expected": str(expected), "picked": str(picked), "duration_s": duration, "refused": refused}})


# ==================================================== 1. the arithmetic, as the web page's


def test_pickers_are_ranked_by_lines_with_their_dominant_pick_type():
    rows = [_release("2026-09-18T06:00:00+00:00", picker="A"), _release("2026-09-18T06:01:00+00:00", picker="B", tx="JIT Pick"),
            _release("2026-09-18T06:02:00+00:00", picker="B", tx="JIT Pick", picked=0)]
    pickers = rs.per_picker(rows)
    assert [p["picker"] for p in pickers] == ["B", "A"]
    assert pickers[0]["dominant"] == {"tx": "jit", "share": 1.0} and pickers[0]["zeroPicks"] == 1
    assert pickers[0]["exactPct"] == 0.5 and pickers[0]["share"] == 2 / 3


def test_customers_short_and_by_volume_follow_the_web_pages_order():
    rows = [_release("2026-09-18T06:00:00+00:00", customer="Zed", picked=0),
            _release("2026-09-18T06:01:00+00:00", customer="alpha", expected=10, picked=2),
            _release("2026-09-18T06:02:00+00:00", customer="Beta")]
    assert [c["customer"] for c in rs.customer_shortfall(rows)] == ["alpha", "Zed"]  # 8 units short, then 4
    # one line each: by name, case-blind, as localeCompare orders them
    assert [c["customer"] for c in rs.customer_volume(rows)] == ["alpha", "Beta", "Zed"]


def test_the_duration_histogram_has_the_web_pages_bands_and_longest_line():
    rows = [_release(f"2026-09-18T06:0{i}:00+00:00", duration=d) for i, d in enumerate(["10", "45", "90", "400", "1000"])]
    stats = rs.duration_stats(rows)
    assert [b["count"] for b in stats["bands"]] == [1, 1, 1, 0, 1, 1]
    assert stats["medianS"] == 90 and stats["p95S"] == 1000 and stats["over5min"] == 2 and stats["longestS"] == 1000


def test_day_buckets_step_by_fixed_days_across_the_clock_change_as_the_browser_does():
    # 25 October 2026 the clocks go back: the browser steps 86,400,000 ms from the last midnight,
    # so the buckets before the change start at 01:00 summer time
    end = datetime(2026, 10, 28, 12, 0, tzinfo=LONDON)
    out = rs.buckets([], end, 7, "day", LONDON)
    assert out[-1]["start"] == datetime(2026, 10, 28, 0, 0, tzinfo=LONDON)
    assert out[0]["start"].isoformat() == "2026-10-22T01:00:00+01:00"


# ==================================================== 2. the row on the planted tenant


async def test_the_whole_customers_row_holds_three_windows_with_every_panel():
    async with async_session() as db:
        row = await rd.compute(db, CC, now=NOW)
    assert row["available"] is True and row["site_id"] == "all" and row["scope"] == "all sites"
    assert list(row["windows"]) == ["today", "yesterday", "week"]
    today = row["windows"]["today"]
    assert today["rolling"] is True and today["word"] == "today" and today["caption"] == "today 00:00 → 13:00"
    # 540551 (10 expected, 9 picked), A1 exact, B1 partial, B2 zero-pick, all on the 18th
    assert today["tiles"]["lines"] == 4 and today["split"] == {"exact": 1, "over": 0, "zeroPick": 1, "partial": 2, "total": 4}
    assert today["tiles"]["deliveries"] == 2 and today["tiles"]["deliveries_incomplete"] == 2
    assert len(today["chart"]) == 24 and sum(b["total"] for b in today["chart"]) == 4
    assert today["feed_total"] == 1 and today["feed"][0]["item"] == "100230"
    assert today["durations"]["all"]["bands"][0]["label"] == "< 30 s"
    assert {"pickers", "incomplete", "customers", "items", "volume", "split_base", "present"} <= set(today)
    week = row["windows"]["week"]
    assert week["bucket"] == "day" and len(week["chart"]) == 7 and week["tiles"]["lines"] == 6
    yesterday = row["windows"]["yesterday"]
    assert yesterday["fixed"] is True and yesterday["tiles"]["lines"] == 1 and len(yesterday["chart"]) == 24


async def _plant_two_warehouses() -> None:
    """Three releases at BRI and one at LON on the planted day, under a settlement that carries the
    warehouse (the golden one does not, so it has no per-site figures)."""
    await plant.wipe_analytics(CC)
    await plant.register_analytics_tenant(CC)
    facts = [plant.fact(CC, 30, 4, 4, rep="A1", item="100606"), plant.fact(CC, 40, 7, 4, rep="B1"),
             plant.fact(CC, 50, 7, 0, rep="B2", item="100230"), plant.fact(CC, 60, 5, 5, rep="L1", delivery="D3")]
    facts[-1].warehouse = "LON"
    async with async_session() as db:
        for f in facts:
            db.add(f)
        await db.commit()
        await plant.api.create_settlement(body={**plant.BODY, "carry": [*plant.BODY["carry"], "warehouse"]}, backfill=True,
                                          customer=CC, db=db)


async def test_a_sites_row_reads_that_warehouse_only():
    await _plant_two_warehouses()
    async with async_session() as db:
        brighton = await rd.compute(db, CC, Site("BRI", "Brighton"), now=NOW)
        london = await rd.compute(db, CC, Site("LON", "London"), now=NOW)
        everyone = await rd.compute(db, CC, now=NOW)
    assert brighton["scope"] == "Brighton" and brighton["windows"]["today"]["tiles"]["lines"] == 3
    assert brighton["windows"]["today"]["feed_total"] == 1
    assert london["windows"]["today"]["tiles"]["lines"] == 1 and london["windows"]["today"]["feed"] == []
    assert everyone["windows"]["today"]["tiles"]["lines"] == 4


async def test_the_row_fits_a_dynamodb_item_and_a_long_row_is_cut_with_a_note():
    async with async_session() as db:
        row = await rd.compute(db, CC, now=NOW)
    assert len(json.dumps(row).encode()) < rd.MAX_ROW_BYTES
    big = {"windows": {"today": {k: [{"x": "y" * 100}] * 100 for k in ("pickers", "incomplete", "customers", "items", "volume")}}}
    cut = rd.fit(big, max_bytes=10_000)
    assert len(cut["windows"]["today"]["pickers"]) == 25 and cut["trimmed"] == ["long tables cut to 25 rows"]


async def test_a_tenant_without_the_settlement_is_told_so():
    await plant.wipe_analytics(CC)
    await plant.register_analytics_tenant(CC)
    async with async_session() as db:
        row = await rd.compute(db, CC, now=NOW)
    assert row["available"] is False and row["note"] == rd.NO_SETTLEMENT_NOTE


# ==================================================== 3. the sweep


async def test_the_sweep_mirrors_the_whole_customer_and_each_site():
    async with async_session() as db:
        await SiteRepository(db).upsert(CC, "BRI", "Brighton", sort_order=1)
        await db.commit()
    mirror = InMemoryMirrorWriter()
    assert await rd.sweep_once(mirror, only=[CC]) == [CC]
    assert sorted(mirror.items) == [(f"EYE#{CC}", "SITE#BRI#RELEASES"), (f"EYE#{CC}", "SITE#all#RELEASES")]


async def test_a_failing_mirror_writes_nothing_and_does_not_raise():
    assert await rd.sweep_once(InMemoryMirrorWriter(fail=True), only=[CC]) == []
