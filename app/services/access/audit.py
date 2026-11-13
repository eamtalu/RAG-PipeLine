"""The audit log (stage D, step 7): every permission decision that changed data, as one sentence
with its facts. Append-only; the consumer purges rows older than the retention.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import AuditEvent

logger = logging.getLogger(__name__)

PURGE_BATCH = 1000


async def record_event(db: AsyncSession, customer_code: str, *, actor_object_id: str, action: str,
                       decision: str, text: str, actor_name: str | None = None,
                       actor_kind: str = "user", capability_code: str | None = None,
                       subject_kind: str | None = None, subject_id: str | None = None,
                       site_id: str | None = None, request_id: str | None = None,
                       trace_id: str | None = None, detail: dict | None = None,
                       at: datetime | None = None) -> AuditEvent:
    """Append one event; the caller commits with whatever it audited, so the two never disagree.

    (u-7 "access.user_created" allowed "Amin created Dan Roberts") -> the row, not yet committed
    """
    row = AuditEvent(customer_code=customer_code, at=at or datetime.now(timezone.utc),
                     actor_object_id=actor_object_id, actor_name=actor_name, actor_kind=actor_kind,
                     action=action, capability_code=capability_code, decision=decision,
                     subject_kind=subject_kind, subject_id=subject_id, site_id=site_id,
                     request_id=request_id, trace_id=trace_id, text=text, detail=detail)
    db.add(row)
    await db.flush()
    logger.info("audit %s actor=%s decision=%s subject=%s/%s", action, actor_object_id, decision,
                subject_kind, subject_id)
    return row


async def recent_events(db: AsyncSession, customer_code: str, *, limit: int = 100) -> list[AuditEvent]:
    """The latest events of a customer, newest first."""
    rows = (await db.execute(
        select(AuditEvent).where(AuditEvent.customer_code == customer_code)
        .order_by(AuditEvent.at.desc()).limit(limit)
    )).scalars()
    return list(rows)


async def purge_once(db: AsyncSession, *, now: datetime, retention_days: int,
                     batch_size: int = PURGE_BATCH) -> int:
    """Delete events older than the retention, one batch per call; how many went."""
    cutoff = now - timedelta(days=retention_days)
    oldest = (select(AuditEvent.id).where(AuditEvent.at < cutoff)
              .order_by(AuditEvent.at).limit(batch_size))
    result = await db.execute(delete(AuditEvent).where(AuditEvent.id.in_(oldest)))
    await db.commit()
    gone = result.rowcount or 0
    if gone:
        logger.info("audit.purged rows=%d cutoff=%s", gone, cutoff.isoformat())
    return gone


def event_payload(row: AuditEvent) -> dict:
    """One event as the Admin tab's list shows it."""
    return {
        "id": str(row.id), "at": row.at.isoformat(), "actor": row.actor_name or row.actor_object_id,
        "actor_kind": row.actor_kind, "action": row.action, "capability": row.capability_code,
        "decision": row.decision, "subject_kind": row.subject_kind, "subject_id": row.subject_id,
        "site_id": row.site_id, "text": row.text,
    }
