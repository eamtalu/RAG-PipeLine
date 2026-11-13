"""Runs the agents on their cadences (stage D, step 9): seeds each customer's switches from the
registry, runs whichever agent is due, reconciles its findings into the advisories table, writes
the run ledger, announces new advisories to the groups that take them within each group's hourly
cap (steps 12 and 13), and mirrors the row the edge reads. Called by the consumer's advisories
loop every few seconds; nothing runs before its time.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.advisory import AdvisoryAgent, AdvisoryAgentRun
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.advisories import registry, store
from app.services.advisories import mirror as advisories_mirror
from app.services.groups import caps, routing
from app.services.groups import store as groups_store
from app.services.groups.store import Route
from app.services.notifications.events import NotificationEvent
from app.services.teams.mirror import ChangeTracker, MirrorWriter
from app.settings import settings

logger = logging.getLogger(__name__)


async def ensure_agents(db: AsyncSession, customer_code: str) -> int:
    """Seed the customer's switches from the registry for agents not yet listed; how many added."""
    have = {row.agent for row in (await db.execute(
        select(AdvisoryAgent).where(AdvisoryAgent.customer_code == customer_code))).scalars()}
    added = 0
    for spec in registry.AGENTS:
        if spec.name not in have:
            db.add(AdvisoryAgent(customer_code=customer_code, agent=spec.name, enabled=spec.enabled_by_default,
                                 cadence_seconds=spec.cadence_seconds, updated_by="system"))
            added += 1
    await db.flush()
    return added


async def due_agents(db: AsyncSession, customer_code: str, *, now: datetime) -> list[AdvisoryAgent]:
    """The enabled, runnable agents whose next run is now or earlier (never run counts as due)."""
    rows = (await db.execute(select(AdvisoryAgent).where(
        AdvisoryAgent.customer_code == customer_code, AdvisoryAgent.enabled.is_(True)))).scalars()
    return [r for r in rows if registry.spec(r.agent).run is not None and (r.next_run_at is None or r.next_run_at <= now)]


async def run_agent(db: AsyncSession, customer_code: str, switch: AdvisoryAgent, *, now: datetime,
                    tz: ZoneInfo) -> AdvisoryAgentRun:
    """Run one agent once, reconcile its findings, write its ledger row; the ledger row."""
    spec = registry.spec(switch.agent)
    run = AdvisoryAgentRun(id=uuid.uuid4(), customer_code=customer_code, agent=spec.name, started_at=now)
    db.add(run)
    try:
        # Step 1: The agent reads the customer's data and returns findings; an agent that raises
        # must not take the others down, so the error lands on its ledger row instead
        findings = await spec.run(db, customer_code, now, tz)
        run.opened, run.resolved = await store.reconcile(
            db, customer_code, spec.name, findings, now=now,
            reannounce_after=timedelta(hours=settings.teams_advisories_reannounce_hours))
        switch.last_result = {"opened": run.opened, "resolved": run.resolved, "found": len(findings), "error": None}
    except Exception as exc:  # noqa: BLE001 - the ledger keeps the reason
        logger.exception("advisory agent %s failed for %s", spec.name, customer_code)
        run.error = f"{type(exc).__name__}: {exc}"[:2000]
        switch.last_result = {"opened": 0, "resolved": 0, "found": 0, "error": run.error[:200]}
    # Step 2: Book the next run from now, not from the planned time, so a slow run does not
    # make the next one fire at once
    run.finished_at = datetime.now(timezone.utc)
    switch.last_run_at = now
    switch.next_run_at = now + timedelta(seconds=switch.cadence_seconds)
    await db.flush()
    return run


def notification_for(row, customer_code: str, *, route: Route | None = None) -> NotificationEvent:
    """The channel message for an advisory: the title and the words the card carries, aimed at
    the channels of the groups that took it (step 12).

    (delivery row "Route R14 ... 24 lines behind", route to ch1) -> event advisory:<id>, severity warning, target ch1
    """
    summary = " · ".join(p for p in (row.evidence, row.impact) if p)
    return NotificationEvent(
        event_type="advisory", customer_code=customer_code,
        severity="warning" if row.severity == "high" else "info",
        title=f"{registry.spec(row.agent).label}: {row.title}", summary=summary or None,
        dedup_key=f"advisory:{row.id}",
        target_channel_ids=list(route.channel_ids) if route is not None else None,
        payload={"advisory_id": str(row.id), "agent": row.agent, "severity": row.severity,
                 "site_id": row.site_id, "recommended": row.recommended, "source": dict(row.source or {}),
                 "groups": list(route.group_names) if route is not None else []},
    )


async def _announce(enqueue, event: NotificationEvent, what: str) -> bool:
    """Hand one card to the outbox; False (and a log line) when the outbox refused it, so one bad
    card never stops the tick."""
    try:
        await enqueue(event)
    except Exception:
        logger.exception("%s not announced (%s)", what, event.dedup_key)
        return False
    return True


async def notify(db: AsyncSession, customer_code: str, *, now: datetime, enqueue, tz: ZoneInfo | None = None) -> int:
    """Announce each new advisory to the channels of the groups that take its topic and severity
    and still have room this hour (steps 12 and 13); an advisory no group takes is marked seen and
    goes nowhere, one a group holds gets that group's notice once an hour, and a finished hour's
    held findings go out as one summary per group. How many advisory cards were sent."""
    tz = tz or timezone.utc
    sent = 0
    # Step 1: Every new advisory is stamped on this tick whether sent or held, so a group made
    # later never receives today's backlog and a held finding is never retried as new
    for row in await store.unnotified(db, customer_code):
        takers = await groups_store.takers(db, customer_code, routing.topic_of(row.agent), row.severity)
        row.notified_at = now
        if not takers:
            continue
        admission = await caps.admit(db, customer_code, takers, row.id, now=now)
        route = groups_store.route_of(admission.sent)
        if route.channel_ids and await _announce(enqueue, notification_for(row, customer_code, route=route), f"advisory {row.id}"):
            sent += 1
        # Step 2: A group that just reached its cap says so once, otherwise its readers see the
        # cards stop and do not know the findings continue on the tab
        for view in admission.notice_due:
            if view.channel_ids:
                await _announce(enqueue, caps.notice_for(view, customer_code, now=now, tz=tz), f"cap notice {view.group.name}")
    # Step 3: Once an hour is over, what each group held in it goes out as one card; the ledger
    # line is written before the send, so a tick that dies here never repeats the summary
    for summary in await caps.summaries_due(db, customer_code, now=now):
        if summary.view.channel_ids:
            await _announce(enqueue, caps.summary_for(summary, customer_code, tz=tz), f"cap summary {summary.view.group.name}")
    await db.flush()
    return sent


async def run_due(db: AsyncSession, customer_code: str, *, now: datetime, enqueue=None) -> list[AdvisoryAgentRun]:
    """Everything one tick owes one customer: seed, wake snoozes, run due agents, announce HIGH."""
    await ensure_agents(db, customer_code)
    await store.wake_snoozed(db, customer_code, now=now)
    tz = ZoneInfo(await get_customer_timezone(db, customer_code))
    runs = [await run_agent(db, customer_code, switch, now=now, tz=tz)
            for switch in await due_agents(db, customer_code, now=now)]
    if enqueue is not None:
        await notify(db, customer_code, now=now, enqueue=enqueue, tz=tz)
    return runs


async def sweep_once(mirror: MirrorWriter | None, *, only: list[str] | None = None,
                     tracker: ChangeTracker | None = None, enqueue=None, session_factory=None,
                     now: datetime | None = None) -> dict[str, int]:
    """One tick over every bound customer (or `only`): agent runs per customer. One customer's
    failure does not stop the rest."""
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    ran: dict[str, int] = {}
    async with factory() as db:
        if only is None:
            codes = sorted({b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        else:
            codes = list(only)
        for code in codes:
            try:
                runs = await run_due(db, code, now=now, enqueue=enqueue)
                await db.commit()
                ran[code] = len(runs)
                if mirror is not None:
                    await advisories_mirror.mirror_customer(db, mirror, code, tracker=tracker, now=now)
            except Exception:
                logger.exception("advisories tick failed for %s", code)
                await db.rollback()
    return ran
