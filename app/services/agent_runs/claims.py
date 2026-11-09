"""Claiming a run by request id, so the same question is never paid for twice.

Used by the Teams consumer before every run (`app/teams_consumer.py`). The rules are the ones in
docs/esmart-agent-architecture.md: a second delivery that finds the row running leaves the
message alone; one that finds it done re-posts the stored result; a running row older than the
queue's visibility window belongs to a dead consumer and is taken over.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.agent_run import STATUS_DONE, STATUS_RUNNING, AgentRun

Outcome = Literal["claimed", "running", "done"]


@dataclass(frozen=True)
class Claim:
    """What the caller may do: run (`claimed`), wait (`running`) or replay `stored` (`done`)."""

    outcome: Outcome
    stored: dict | None = None


async def try_claim(db: AsyncSession, *, request_id: str, customer_code: str, channel: str,
                    now: datetime, stale_after: timedelta) -> Claim:
    """Claim the run for `request_id`, or learn who has it; exactly one caller is ever `claimed`.

    (new id) -> Claim("claimed") ; (finished) -> Claim("done", stored=payload) ;
    (running, fresh) -> Claim("running") ; (running, older than stale_after) -> Claim("claimed")
    """
    # Step 1: Insert the row and let the unique request id decide the race, so that two consumers
    # holding the same redelivered message can never both run the model.
    inserted = await db.execute(
        pg_insert(AgentRun).values(id=uuid.uuid4(), request_id=request_id,
                                   customer_code=customer_code, channel=channel,
                                   status=STATUS_RUNNING, claimed_at=now, attempts=1)
        .on_conflict_do_nothing(index_elements=[AgentRun.request_id])
        .returning(AgentRun.id))
    if inserted.scalar_one_or_none() is not None:
        return Claim("claimed")

    # Step 2: The row exists; a finished run hands back its answer so the caller replays it instead
    # of asking the model again.
    row = (await db.execute(select(AgentRun).where(AgentRun.request_id == request_id))).scalar_one()
    if row.status == STATUS_DONE:
        return Claim("done", stored=row.result)

    # Step 3: A running row older than the visibility window is a dead consumer's, otherwise the
    # message would not be visible again; take it over atomically, or report that it is in flight.
    cutoff = now - stale_after
    taken = await db.execute(
        update(AgentRun)
        .where(AgentRun.request_id == request_id, AgentRun.status == STATUS_RUNNING,
               AgentRun.claimed_at < cutoff)
        .values(claimed_at=now, attempts=AgentRun.attempts + 1))
    return Claim("claimed") if taken.rowcount == 1 else Claim("running")


async def finish(db: AsyncSession, *, request_id: str, result: dict, now: datetime) -> None:
    """Store the answer as posted and mark the run done, before it is posted.

    (running, result) -> row done with result ; the next try_claim -> Claim("done", stored=result)
    """
    await db.execute(update(AgentRun).where(AgentRun.request_id == request_id)
                     .values(status=STATUS_DONE, finished_at=now, result=result))
