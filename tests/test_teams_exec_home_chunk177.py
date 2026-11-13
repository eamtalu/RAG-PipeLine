"""Chunk 177: the executive Home (stage D, step 7d): the operation across every site per period,
from the same settled rows as the supervisor's Home, the 30-day series, the decisions waiting,
mirrored as EXEC#HOME.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from app.config.database import async_session
from app.services.access import approvals
from app.services.teams import exec_home
from app.services.teams.mirror import InMemoryMirrorWriter
from tests.test_teams_sites_chunk174 import (  # noqa: F401 - the fixture is used by name
    CC,
    _configure_sites,
    _plant_two_warehouses,
    clean,
)


async def test_the_executive_home_counts_every_site_per_period_and_mirrors_once():
    now = datetime.now(timezone.utc)
    await _plant_two_warehouses(now)
    await _configure_sites()
    async with async_session() as db:
        await approvals.request(db, CC, kind="create_user", requested_by="u-1", requested_by_name="Sam",
                                requested_via="admin", title="New user: Dan Roberts", now=now)
        await db.commit()
        payload = await exec_home.compute(db, CC, now=now, target=0.98)
    assert payload is not None
    assert [s["site_id"] for s in payload["sites"]] == ["BRI", "LON"]
    week = payload["periods"]["week"]
    assert week["label"] == "This week" and "figures to" in week["range_text"]
    # three releases at BRI, one at LON, four in all; the fill rate from expected and picked
    assert week["all"]["lines"] == 4 and week["sites"]["BRI"]["lines"] == 3 and week["sites"]["LON"]["lines"] == 1
    assert week["all"]["pickers"] >= 1 and week["sites"]["LON"]["fill_rate"] == 1.0
    assert 0 < week["sites"]["BRI"]["fill_rate"] < 1
    assert week["all"]["on_time"] is None and week["all"]["departures"] == 0  # no closed departures planted
    assert set(payload["periods"]) == {"today", "week", "month", "quarter"}
    series = payload["series"]
    assert len(series["labels"]) == 30 and len(series["all"]) == 30 and set(series["sites"]) == {"BRI", "LON"}
    assert series["all"][-1] == week["all"]["fill_rate"]  # today's point is the week's figure here
    assert [d["reference"] for d in payload["decisions"]] == ["USR-1"]
    assert payload["advisories"]["open"] == 0 and payload["brief"]["text"] is None
    assert payload["target_fill_rate"] == 0.98

    mirror = InMemoryMirrorWriter()
    written = await exec_home.sweep_once(mirror, only=[CC], target=0.98)
    assert CC in written
    env = mirror.items[(f"EYE#{CC}", "EXEC#HOME")]
    assert env.row_type == "exec_home" and env.ttl is not None
    assert json.loads(env.payload_json)["periods"]["week"]["all"]["lines"] == 4


def test_the_windows_and_the_shaping():
    friday = datetime(2026, 11, 13, 9, 30, tzinfo=timezone.utc)
    start, end, prev_start, prev_end = exec_home.period_windows(friday, "month")
    assert end == friday and start == friday.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=30)
    assert prev_start == start - timedelta(days=30) and prev_end == end - timedelta(days=30)
    assert exec_home.on_time({"fine": 3, "held": 1, "missed": 0}) == (0.75, 4, 0)
    assert exec_home.fill_rate("100", "-150") == 0.0
