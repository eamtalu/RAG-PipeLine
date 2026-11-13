"""Chunk 184: the per-group cap on advisory cards (stage D, step 13).

Routing (step 12) decides who gets a card; the cap decides how many. A group sends at most
`max_per_hour` advisory cards per clock hour. The first finding a group holds in an hour sends one
notice, the rest are recorded as held, and the first tick after the hour sends one summary of what
was held. A finding that comes back within the quiet period keeps its stamp and is not announced
again. The Admin tab sees the cap and the hour's counters on the GROUPS row.
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
from app.persistence.models.customer import Customer
from app.persistence.models.notification import CustomerNotificationChannel
from app.persistence.models.notification_group import AdvisoryAnnouncement
from app.persistence.models.tenant_site import TenantSite
from app.services.access import commands as access_commands, users
from app.services.advisories import runner, store
from app.services.advisories.registry import Finding
from app.services.groups import caps, commands, routing
from app.services.groups import mirror as groups_mirror
from app.services.groups import store as groups_store
from app.services.teams.contracts import Command, QuestionJob
from app.services.teams.mirror import InMemoryMirrorWriter

CC = "test_c184"
TZ = ZoneInfo("Europe/London")
NOW = datetime(2026, 11, 13, 15, 10, tzinfo=timezone.utc)
SUPER = users.Actor.system("superadmin")
CH1, CH2 = uuid.UUID(int=1841), uuid.UUID(int=1842)


async def _wipe() -> None:
    async with async_session() as s:
        await s.execute(delete(CustomerNotificationChannel).where(CustomerNotificationChannel.customer_code == CC))
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


@pytest.fixture(autouse=True)
async def _customer():
    await _wipe()
    async with async_session() as s:
        s.add(Customer(customer_code=CC, display_name="C184", timezone="Europe/London"))
        s.add(TenantSite(customer_code=CC, site_id="BRI", name="Brighton", sort_order=1))
        s.add_all([CustomerNotificationChannel(id=CH1, customer_code=CC, channel_type="teams", name="stock", config={}),
                   CustomerNotificationChannel(id=CH2, customer_code=CC, channel_type="teams", name="ops", config={})])
        await s.commit()
        await users.create_user(s, CC, "u-ops", actor=SUPER, now=NOW,
                                change=users.UserChange(display_name="Olu Ops", role_code="ops_manager", all_sites=True))
        await s.commit()
    yield
    await _wipe()


def _finding(n: int, severity: str = "high", agent_prefix: str = "count") -> Finding:
    return Finding(dedup_key=f"{agent_prefix}:ITEM{n}:A-{n:02d}", severity=severity,
                   title=f"Count at A-{n:02d} is {n} units off", evidence=f"counted {10 - n}, system 10", site_id="BRI")


async def _group(db, name: str, *, cap: int, channels: list[uuid.UUID], severity: str = "medium"):
    group = await groups_store.set_group(db, CC, group_id=None, name=name, topics=["advisory:stock", "advisory:location"],
                                         min_severity=severity, channel_ids=[str(c) for c in channels], member_object_ids=[],
                                         actor_object_id="u-ops", now=NOW, max_per_hour=cap)
    await db.commit()
    return group


async def _tick(db, findings: list[Finding], now: datetime, announced: list, *, agent: str = "stock") -> int:
    """Open the findings as the agent would, then announce; the cards enqueued by this tick."""
    await store.reconcile(db, CC, agent, findings, now=now, reannounce_after=timedelta(hours=2))

    async def enqueue(event):
        announced.append(event)
        return [uuid.uuid4()]

    sent = await runner.notify(db, CC, now=now, enqueue=enqueue, tz=TZ)
    await db.commit()
    return sent


async def _ledger(db, group_id) -> dict[str, int]:
    rows = (await db.execute(select(AdvisoryAnnouncement).where(AdvisoryAnnouncement.group_id == group_id))).scalars()
    out: dict[str, int] = {}
    for row in rows:
        out[row.status] = out.get(row.status, 0) + 1
    return out


def _command(kind: str, payload: dict) -> QuestionJob:
    return QuestionJob(job_id=f"j-{uuid.uuid4()}", tenant_id="t", customer_code=CC, conversation_id="c",
                       sender_object_id="u-ops", sender_name="Olu", question=f"groups: {kind}", source="tab",
                       site_id="BRI", command=Command(kind=kind, by="Olu", payload=payload))


def test_the_window_and_cap_rules():
    assert routing.window_start(NOW) == datetime(2026, 11, 13, 15, 0, tzinfo=timezone.utc)
    assert routing.clean_cap(None) == 5 and routing.clean_cap("3") == 3 and routing.clean_cap(60) == 60
    for bad in (0, 61, "many"):
        with pytest.raises(ValueError, match="between 1 and 60"):
            routing.clean_cap(bad)


async def test_a_group_sends_its_cap_then_one_notice_then_one_summary_after_the_hour():
    announced: list = []
    async with async_session() as db:
        stock = await _group(db, "Stock", cap=2, channels=[CH1])
        # Step 1: four HIGH findings in one tick: two cards, one notice, two held
        assert await _tick(db, [_finding(n) for n in (1, 2, 3, 4)], NOW, announced) == 2
        cards = [e for e in announced if e.event_type == "advisory"]
        notices = [e for e in announced if e.event_type == "advisory_cap"]
        assert len(cards) == 2 and all(e.target_channel_ids == [str(CH1)] and e.payload["groups"] == ["Stock"] for e in cards)
        assert len(notices) == 1
        notice = notices[0]
        assert notice.title == "Stock: quiet until 16:00" and notice.target_channel_ids == [str(CH1)]
        assert notice.dedup_key == f"advisory-cap:{stock.id}:2026-11-13T15:00:00+00:00"
        assert "2 cards sent this hour" in notice.summary and "Advisories tab" in notice.summary
        assert await _ledger(db, stock.id) == {"sent": 2, "held": 2, "notice": 1}
        rows = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalars().all()
        assert len(rows) == 4 and all(r.notified_at == NOW for r in rows)  # held ones are seen too: no backlog later
        # Step 2: one more finding ten minutes later is held without a second notice
        assert await _tick(db, [_finding(n) for n in (1, 2, 3, 4, 5)], NOW + timedelta(minutes=10), announced) == 0
        assert len(announced) == 3 and (await _ledger(db, stock.id)) == {"sent": 2, "held": 3, "notice": 1}
        # Step 3: the Admin tab sees the cap and the hour's counters
        payload = await groups_mirror.groups_payload(db, CC, now=NOW + timedelta(minutes=10))
        group = payload["groups"][0]
        assert group["max_per_hour"] == 2 and group["sent_this_hour"] == 2 and group["held_this_hour"] == 3
        # Step 4: the first tick of the next hour sends one summary of what was held, once
        assert await _tick(db, [_finding(n) for n in (1, 2, 3, 4, 5)], NOW + timedelta(minutes=55), announced) == 0
        summaries = [e for e in announced if e.event_type == "advisory_summary"]
        assert len(summaries) == 1
        summary = summaries[0]
        assert summary.title == "Stock: 3 more findings between 15:00 and 16:00"
        assert summary.summary == "3 HIGH · stock 3 · open the Advisories tab in eSmart Eye"
        assert summary.dedup_key == f"advisory-summary:{stock.id}:2026-11-13T15:00:00+00:00"
        assert summary.target_channel_ids == [str(CH1)] and summary.payload["held"] == 3
        assert await _tick(db, [_finding(n) for n in (1, 2, 3, 4, 5)], NOW + timedelta(minutes=56), announced) == 0
        assert len([e for e in announced if e.event_type == "advisory_summary"]) == 1
        # Step 5: a new finding in the new hour is a card again
        assert await _tick(db, [_finding(n) for n in (1, 2, 3, 4, 5, 6)], NOW + timedelta(minutes=57), announced) == 1
        assert (await _ledger(db, stock.id)) == {"sent": 3, "held": 3, "notice": 1, "summary": 1}
        counters = (await groups_mirror.groups_payload(db, CC, now=NOW + timedelta(minutes=57)))["groups"][0]
        assert counters["sent_this_hour"] == 1 and counters["held_this_hour"] == 0


async def test_only_the_groups_with_room_get_the_card():
    announced: list = []
    async with async_session() as db:
        narrow = await _group(db, "Narrow", cap=1, channels=[CH1])
        wide = await _group(db, "Wide", cap=5, channels=[CH2])
        assert await _tick(db, [_finding(1), _finding(2)], NOW, announced) == 2
        cards = [e for e in announced if e.event_type == "advisory"]
        assert sorted(cards[0].target_channel_ids) == sorted([str(CH1), str(CH2)]) and cards[0].payload["groups"] == ["Narrow", "Wide"]
        assert cards[1].target_channel_ids == [str(CH2)] and cards[1].payload["groups"] == ["Wide"]
        notices = [e for e in announced if e.event_type == "advisory_cap"]
        assert [n.title for n in notices] == ["Narrow: quiet until 16:00"]
        assert await _ledger(db, narrow.id) == {"sent": 1, "held": 1, "notice": 1}
        assert await _ledger(db, wide.id) == {"sent": 2}


async def test_a_finding_that_comes_back_within_the_quiet_period_is_not_announced_again():
    announced: list = []
    async with async_session() as db:
        await _group(db, "Stock", cap=5, channels=[CH1])
        assert await _tick(db, [_finding(1)], NOW, announced) == 1
        # cleared five minutes later, back after thirty: the stamp is kept, no second card
        assert await _tick(db, [], NOW + timedelta(minutes=5), announced) == 0
        assert await _tick(db, [_finding(1)], NOW + timedelta(minutes=30), announced) == 0
        row = (await db.execute(select(Advisory).where(Advisory.customer_code == CC))).scalar_one()
        assert row.status == "open" and row.notified_at == NOW
        # cleared again, back three hours after the card: announced once more
        assert await _tick(db, [], NOW + timedelta(minutes=40), announced) == 0
        assert await _tick(db, [_finding(1)], NOW + timedelta(hours=3), announced) == 1
        assert len([e for e in announced if e.event_type == "advisory"]) == 2


async def test_the_cap_is_set_through_the_command_and_checked():
    mirror = InMemoryMirrorWriter()
    with pytest.raises(access_commands.Refused, match="between 1 and 60"):
        await commands.run(_command("group_set", {"name": "Stock", "topics": ["advisory:stock"], "max_per_hour": 0}),
                           mirror=mirror, now=NOW)
    outcome = await commands.run(_command("group_set", {"name": "Stock", "topics": ["advisory:stock"], "max_per_hour": 3,
                                                        "channel_ids": [str(CH1)]}), mirror=mirror, now=NOW)
    assert outcome.text == "group Stock saved · 1 channel · 0 members · 3 cards an hour"
    group_id = outcome.blocks[0]["reference"]
    row = json.loads(mirror.items[(f"EYE#{CC}", "GROUPS")].payload_json)["groups"][0]
    assert row["max_per_hour"] == 3 and row["sent_this_hour"] == 0 and row["held_this_hour"] == 0
    # an edit that does not mention the cap keeps it; the default for a new group is five
    await commands.run(_command("group_set", {"group_id": group_id, "name": "Stock", "topics": ["advisory:stock"]}),
                       mirror=mirror, now=NOW)
    await commands.run(_command("group_set", {"name": "Floor", "topics": ["schedule"]}), mirror=mirror, now=NOW)
    rows = {g["name"]: g for g in json.loads(mirror.items[(f"EYE#{CC}", "GROUPS")].payload_json)["groups"]}
    assert rows["Stock"]["max_per_hour"] == 3 and rows["Floor"]["max_per_hour"] == 5
    async with async_session() as db:
        assert await caps.window_counts(db, CC, routing.window_start(NOW)) == {}
