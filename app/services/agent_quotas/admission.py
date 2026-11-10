"""Admission for agent runs: shared counters in Postgres, one atomic decision per request.

The gateway asks `admit` before any model call and `release` when the run ends. The decision is
one transaction under a per-tenant advisory lock: sweep the tenant's expired in-flight rows and
its day-old hourly rows, count what is live, read the hour's two counters, decide, and only when
admitted increment both counters and insert the in-flight row. Every worker and web process does
the same, so five of them enforce one number. A refused attempt writes nothing, so a person
hammering refusals cannot lock the tenant out. This module imports nothing from the agent package:
the gateway's port takes plain values (docs/agent-quotas.md).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.agent_in_flight import AgentInFlight
from app.persistence.models.quota_window import QuotaWindow
from app.settings import settings

#: the tenant-wide counter row
TENANT_ROW = ""
#: hourly rows older than this are swept on admission
WINDOW_RETENTION = timedelta(days=1)


@dataclass(frozen=True, slots=True)
class QuotaLimits:
    """The three limits a tenant runs under; global today, per tenant later."""

    tenant_per_hour: int
    user_per_hour: int
    tenant_in_flight: int

    @classmethod
    def from_settings(cls) -> QuotaLimits:
        """The limits the settings name (`AGENT_QUOTA_*`), read once by a composition root."""
        return cls(
            tenant_per_hour=settings.agent_quota_tenant_per_hour,
            user_per_hour=settings.agent_quota_user_per_hour,
            tenant_in_flight=settings.agent_quota_tenant_in_flight,
        )


def decide(
    limits: QuotaLimits, *, in_flight: int, tenant_used: int, user_used: int
) -> str | None:
    """The first limit the next run would exceed, or None when it may run.

    >>> limits = QuotaLimits(tenant_per_hour=60, user_per_hour=20, tenant_in_flight=8)
    >>> decide(limits, in_flight=8, tenant_used=0, user_used=0)
    'tenant_in_flight'
    >>> decide(limits, in_flight=0, tenant_used=60, user_used=3)
    'tenant_per_hour'
    >>> decide(limits, in_flight=0, tenant_used=30, user_used=20)
    'user_per_hour'
    >>> decide(limits, in_flight=7, tenant_used=59, user_used=19) is None
    True
    """
    if in_flight >= limits.tenant_in_flight:
        return "tenant_in_flight"
    if tenant_used >= limits.tenant_per_hour:
        return "tenant_per_hour"
    if user_used >= limits.user_per_hour:
        return "user_per_hour"
    return None


def window_start(now: datetime) -> datetime:
    """The hour the moment belongs to.

    >>> from datetime import timezone
    >>> window_start(datetime(2026, 11, 10, 14, 30, 5, tzinfo=timezone.utc)).isoformat()
    '2026-11-10T14:00:00+00:00'
    """
    return now.replace(minute=0, second=0, microsecond=0)


class PostgresAdmission:
    """The gateway's admission over Postgres; one short transaction per call."""

    def __init__(self, session_factory: Callable[[], AsyncSession], limits: QuotaLimits):
        self._session_factory = session_factory
        self.limits = limits

    async def admit(
        self, *, tenant: str, user_id: str, trace_id: str, deadline_at: datetime, now: datetime
    ) -> str | None:
        """Admit the run, or name the limit it would exceed; admitted runs are counted at once.

        (tenant under every limit) -> None, counters +1, one in-flight row ;
        (eight runs live, limit 8) -> "tenant_in_flight", nothing written
        """
        window = window_start(now)
        async with self._session_factory() as db:
            # Step 1: Serialise this tenant's admissions, so that two workers deciding at the
            # same instant cannot both see the slot free; other tenants are not held.
            await db.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
                {"key": f"agent_admission:{tenant}"},
            )
            # Step 2: Sweep what time has already settled, because a crashed worker never
            # releases and a day-old window will never be read again.
            await db.execute(
                delete(AgentInFlight).where(
                    AgentInFlight.customer_code == tenant, AgentInFlight.deadline_at < now
                )
            )
            await db.execute(
                delete(QuotaWindow).where(
                    QuotaWindow.customer_code == tenant,
                    QuotaWindow.window_start < now - WINDOW_RETENTION,
                )
            )
            # Step 3: Read the three numbers the decision needs, inside the same lock.
            in_flight = await db.scalar(
                select(func.count())
                .select_from(AgentInFlight)
                .where(AgentInFlight.customer_code == tenant)
            )
            rows = await db.execute(
                select(QuotaWindow.user_id, QuotaWindow.used).where(
                    QuotaWindow.customer_code == tenant,
                    QuotaWindow.window_start == window,
                    QuotaWindow.user_id.in_((TENANT_ROW, user_id)),
                )
            )
            used = {row.user_id: row.used for row in rows}
            reason = decide(
                self.limits,
                in_flight=int(in_flight or 0),
                tenant_used=used.get(TENANT_ROW, 0),
                user_used=used.get(user_id, 0),
            )
            # Step 4: A refusal writes nothing, otherwise a person being refused all hour would
            # push the tenant's own counter over its limit and lock everyone else out.
            if reason is not None:
                await db.rollback()
                return reason
            # Step 5: Count the run on both rows and record it as live, in the one transaction
            # the lock covers, so the numbers the next admission reads include this run.
            for uid in (TENANT_ROW, user_id):
                await db.execute(
                    pg_insert(QuotaWindow)
                    .values(customer_code=tenant, user_id=uid, window_start=window, used=1)
                    .on_conflict_do_update(
                        index_elements=[
                            QuotaWindow.customer_code,
                            QuotaWindow.user_id,
                            QuotaWindow.window_start,
                        ],
                        set_={"used": QuotaWindow.used + 1},
                    )
                )
            db.add(AgentInFlight(trace_id=trace_id, customer_code=tenant, deadline_at=deadline_at))
            await db.commit()
            return None

    async def release(self, *, tenant: str, trace_id: str) -> None:
        """Free the run's slot; a run that was never admitted, or already swept, is a no-op."""
        async with self._session_factory() as db:
            await db.execute(
                delete(AgentInFlight).where(
                    AgentInFlight.customer_code == tenant, AgentInFlight.trace_id == trace_id
                )
            )
            await db.commit()
