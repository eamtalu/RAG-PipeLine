"""The per-group cap on advisory cards (stage D, step 13): which of the groups that take a finding
still have room this clock hour, the ledger of what each group sent and held, the one notice a
group sends when it goes quiet, and the one summary of what it held once the hour is over. Used by
the advisories runner (announcing) and by mirror.py (the counters the Admin tab shows).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.advisory import Advisory
from app.persistence.models.notification_group import AdvisoryAnnouncement, NotificationGroup
from app.services.groups import routing
from app.services.groups.store import GroupView, list_groups
from app.services.notifications.events import NotificationEvent

NOTICE_EVENT = "advisory_cap"
SUMMARY_EVENT = "advisory_summary"


@dataclass(frozen=True)
class Admission:
    """What the cap decided for one finding: the groups whose channels get the card, the groups
    that held it, and the groups that went quiet with it (their first hold this hour)."""

    sent: tuple[GroupView, ...] = ()
    held: tuple[GroupView, ...] = ()
    notice_due: tuple[GroupView, ...] = ()


@dataclass(frozen=True)
class Summary:
    """What one group held in one finished clock hour."""

    view: GroupView
    window_start: datetime
    held: int
    by_severity: dict[str, int]
    by_agent: dict[str, int]


async def window_counts(db: AsyncSession, customer_code: str, window_start: datetime) -> dict[uuid.UUID, dict[str, int]]:
    """Per group, how many ledger lines of each status fall in the clock hour starting then.

    (Stock sent 2, held 3, notice 1 this hour) -> {stock_id: {"sent": 2, "held": 3, "notice": 1}} ; (quiet) -> {}
    """
    rows = await db.execute(
        select(AdvisoryAnnouncement.group_id, AdvisoryAnnouncement.status, func.count())
        .where(AdvisoryAnnouncement.customer_code == customer_code, AdvisoryAnnouncement.window_start == window_start)
        .group_by(AdvisoryAnnouncement.group_id, AdvisoryAnnouncement.status))
    counts: dict[uuid.UUID, dict[str, int]] = {}
    for group_id, status, n in rows:
        counts.setdefault(group_id, {})[status] = int(n)
    return counts


async def admit(db: AsyncSession, customer_code: str, takers: list[GroupView], advisory_id: uuid.UUID, *,
                now: datetime) -> Admission:
    """Decide, for the groups that take a finding, which still have room this hour, and write each
    group's ledger line (sent or held, plus the notice line the first time a group holds).

    (Stock cap 2 with 1 sent) -> sent ; (Stock cap 2 with 2 sent, no notice yet) -> held + notice_due
    """
    # Step 1: Read this hour's counts once, so every group is judged against the same ledger and a
    # tick with ten findings does not re-count the ledger ten times per group
    start = routing.window_start(now)
    counts = await window_counts(db, customer_code, start)
    sent: list[GroupView] = []
    held: list[GroupView] = []
    notice_due: list[GroupView] = []
    for view in takers:
        mine = counts.get(view.group.id, {})
        # Step 2: A group with room sends; one at its cap holds, and the first hold of the hour owes
        # the notice, otherwise the reader sees the cards stop and does not know why
        if mine.get("sent", 0) < view.group.max_per_hour:
            sent.append(view)
            status = "sent"
        else:
            held.append(view)
            status = "held"
            if not mine.get("notice"):
                notice_due.append(view)
                db.add(AdvisoryAnnouncement(id=uuid.uuid4(), customer_code=customer_code, group_id=view.group.id,
                                            advisory_id=None, status="notice", window_start=start, at=now))
        db.add(AdvisoryAnnouncement(id=uuid.uuid4(), customer_code=customer_code, group_id=view.group.id,
                                    advisory_id=advisory_id, status=status, window_start=start, at=now))
    await db.flush()
    return Admission(tuple(sent), tuple(held), tuple(notice_due))


async def summaries_due(db: AsyncSession, customer_code: str, *, now: datetime) -> list[Summary]:
    """The finished clock hours in which a group held findings and no summary went out yet, each
    with what was held by severity and agent; the summary line is written as the ledger's own,
    so a tick that dies after this never sends the same summary twice.

    (Stock held 3 between 15:00 and 16:00, now 16:05, no summary yet) -> [Summary(Stock, 15:00, 3, ...)] ; (summary sent) -> []
    """
    # Step 1: Find (group, hour) pairs with held lines before this hour and no summary line, so an
    # hour missed while the consumer was down is still summarised when it comes back
    current = routing.window_start(now)
    summarised = select(AdvisoryAnnouncement.group_id, AdvisoryAnnouncement.window_start).where(
        AdvisoryAnnouncement.customer_code == customer_code, AdvisoryAnnouncement.status == "summary")
    held_rows = (await db.execute(
        select(AdvisoryAnnouncement.group_id, AdvisoryAnnouncement.window_start, Advisory.severity, Advisory.agent)
        .join(Advisory, Advisory.id == AdvisoryAnnouncement.advisory_id)
        .where(AdvisoryAnnouncement.customer_code == customer_code, AdvisoryAnnouncement.status == "held",
               AdvisoryAnnouncement.window_start < current,
               ~func.row(AdvisoryAnnouncement.group_id, AdvisoryAnnouncement.window_start).in_(summarised))
        .order_by(AdvisoryAnnouncement.window_start))).all()
    if not held_rows:
        return []
    views = {v.group.id: v for v in await list_groups(db, customer_code, include_disabled=False)}
    # Step 2: Fold the held lines per (group, hour), then write the summary line before anything
    # is sent, so the ledger is the guard against a double send
    folded: dict[tuple[uuid.UUID, datetime], Summary] = {}
    for group_id, start, severity, agent in held_rows:
        view = views.get(group_id)
        if view is None:
            continue
        key = (group_id, start)
        summary = folded.get(key) or Summary(view=view, window_start=start, held=0, by_severity={}, by_agent={})
        summary.by_severity[severity] = summary.by_severity.get(severity, 0) + 1
        summary.by_agent[agent] = summary.by_agent.get(agent, 0) + 1
        folded[key] = Summary(view=view, window_start=start, held=summary.held + 1,
                              by_severity=summary.by_severity, by_agent=summary.by_agent)
    for (group_id, start) in folded:
        db.add(AdvisoryAnnouncement(id=uuid.uuid4(), customer_code=customer_code, group_id=group_id,
                                    advisory_id=None, status="summary", window_start=start, at=now))
    await db.flush()
    return list(folded.values())


def _clock(moment: datetime, tz: ZoneInfo) -> str:
    return moment.astimezone(tz).strftime("%H:%M")


def notice_for(view: GroupView, customer_code: str, *, now: datetime, tz: ZoneInfo) -> NotificationEvent:
    """The one card a group sends when it reaches its cap: the cards stop, the tab goes on, a
    summary follows at the hour.

    (Stock cap 5 at 15:10) -> "Stock: quiet until 16:00" / "5 cards sent this hour, the cap. ..."
    """
    start = routing.window_start(now)
    until = _clock(start + routing.WINDOW, tz)
    cap = view.group.max_per_hour
    return NotificationEvent(
        event_type=NOTICE_EVENT, customer_code=customer_code, severity="info",
        title=f"{view.group.name}: quiet until {until}",
        summary=(f"{cap} card{'s' if cap != 1 else ''} sent this hour, the cap. New findings keep going to the "
                 f"Advisories tab in eSmart Eye; one summary follows at {until}."),
        dedup_key=f"advisory-cap:{view.group.id}:{start.isoformat()}",
        target_channel_ids=[str(c) for c in view.channel_ids],
        payload={"group_id": str(view.group.id), "group": view.group.name, "max_per_hour": cap,
                 "window_start": start.isoformat()},
    )


def summary_for(summary: Summary, customer_code: str, *, tz: ZoneInfo) -> NotificationEvent:
    """The one card for a finished hour: how many more findings the group held, by severity and agent.

    (Stock held 3 HIGH stock 15:00-16:00) -> "Stock: 3 more findings between 15:00 and 16:00" / "3 HIGH · stock 3 · ..."
    """
    view, start = summary.view, summary.window_start
    severities = " · ".join(f"{summary.by_severity[s]} {s.upper()}" for s in ("high", "medium", "low")
                            if summary.by_severity.get(s))
    agents = ", ".join(f"{agent} {n}" for agent, n in sorted(summary.by_agent.items()))
    return NotificationEvent(
        event_type=SUMMARY_EVENT, customer_code=customer_code, severity="info",
        title=(f"{view.group.name}: {summary.held} more finding{'s' if summary.held != 1 else ''} between "
               f"{_clock(start, tz)} and {_clock(start + routing.WINDOW, tz)}"),
        summary=f"{severities} · {agents} · open the Advisories tab in eSmart Eye",
        dedup_key=f"advisory-summary:{view.group.id}:{start.isoformat()}",
        target_channel_ids=[str(c) for c in view.channel_ids],
        payload={"group_id": str(view.group.id), "group": view.group.name, "window_start": start.isoformat(),
                 "held": summary.held, "by_severity": dict(summary.by_severity), "by_agent": dict(summary.by_agent)},
    )


def group_cap(group: NotificationGroup) -> int:
    return int(group.max_per_hour or routing.CAP_DEFAULT)
