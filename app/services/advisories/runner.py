"""Runs the agents on their cadences (stage D, step 9): seeds each customer's switches from the
registry, runs whichever agent is due, reconciles its findings into the advisories table, writes
the run ledger, tells the channel about new HIGH advisories, and mirrors the row the edge reads.
Called by the consumer's advisories loop every few seconds; nothing runs before its time.
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
from app.services.notifications.events import NotificationEvent
from app.services.teams.mirror import ChangeTracker, MirrorWriter

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
        run.opened, run.resolved = await store.reconcile(db, customer_code, spec.name, findings, now=now)
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


def notification_for(row, customer_code: str) -> NotificationEvent:
    """The channel message for a HIGH advisory: the title and the words the card carries.

    (delivery row "Route R14 ... 24 lines behind") -> event advisory:<id>, severity warning
    """
    summary = " · ".join(p for p in (row.evidence, row.impact) if p)
    return NotificationEvent(
        event_type="advisory", customer_code=customer_code, severity="warning",
        title=f"{registry.spec(row.agent).label}: {row.title}", summary=summary or None,
        dedup_key=f"advisory:{row.id}",
        payload={"advisory_id": str(row.id), "agent": row.agent, "severity": row.severity,
                 "site_id": row.site_id, "recommended": row.recommended, "source": dict(row.source or {})},
    )


async def notify_high(db: AsyncSession, customer_code: str, *, now: datetime, enqueue) -> int:
    """Tell the customer's channels about each HIGH advisory not yet announced; how many."""
    sent = 0
    for row in await store.unnotified_high(db, customer_code):
        try:
            await enqueue(notification_for(row, customer_code))
        except Exception:
            logger.exception("advisory %s not announced", row.id)
            continue
        row.notified_at = now
        sent += 1
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
        await notify_high(db, customer_code, now=now, enqueue=enqueue)
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
