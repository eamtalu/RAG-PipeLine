"""Dispatcher — the bus subscriber that makes delivery durable, multi-channel, and tracked.

On publish it (1) persists the event to the outbox (idempotent by dedup_key), (2) creates one
`pending` delivery row per targeted channel = the fan-out, then (3) attempts each delivery
concurrently. Failures are not fatal: the row stays `failed` with a backoff schedule and the
redelivery loop (`retry_pending`) re-attempts it until it succeeds or hits the attempt cap (then
`dead`). So a channel/internet outage delays alerts but never drops them, and every (event ×
channel) pair carries its own sent/unsent state.

Each concurrent attempt uses its OWN AsyncSession (an AsyncSession is not safe to share across
tasks); attempts therefore commit independently, which is exactly what we want for per-channel
isolation.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.notification import (
    CustomerNotificationChannel,
    NotificationEvent as NotificationEventRow,
    NotificationDelivery,
    DeliveryStatus,
)
from app.services.notifications.bus import bus
from app.services.notifications.events import NotificationEvent
from app.services.notifications.channels import get_channel

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _backoff_seconds(attempts: int) -> int:
    schedule = settings.notification_retry_backoff_seconds or [60]
    return int(schedule[min(max(attempts, 1) - 1, len(schedule) - 1)])


def _event_from_row(row: NotificationEventRow) -> NotificationEvent:
    return NotificationEvent(
        event_type=row.event_type,
        customer_code=row.customer_code,
        severity=row.severity,
        title=row.title,
        summary=row.summary,
        dedup_key=row.dedup_key,
        payload=row.payload or {},
        target_channel_ids=row.target_channel_ids,
        rule_id=str(row.rule_id) if row.rule_id else None,
    )


# ---- publish path (bus subscriber) ----------------------------------------------------------------
async def handle(event: NotificationEvent) -> None:
    """Persist the event + per-channel deliveries, then attempt delivery. Idempotent by dedup_key."""
    delivery_ids: list[uuid.UUID] = []
    async with async_session() as db:
        existing = await db.scalar(
            select(NotificationEventRow).where(NotificationEventRow.dedup_key == event.dedup_key)
        )
        if existing is not None:
            return  # already published (and deliveries already created) — nothing to do

        channels = await _resolve_channels(db, event)
        if not channels:
            logger.info("notification event %s has no enabled target channel for customer %s — "
                        "persisting outbox only", event.dedup_key, event.customer_code)

        event_row = NotificationEventRow(
            dedup_key=event.dedup_key,
            customer_code=event.customer_code,
            rule_id=uuid.UUID(event.rule_id) if event.rule_id else None,
            event_type=event.event_type,
            severity=event.severity,
            title=event.title,
            summary=event.summary,
            payload=event.payload or {},
            target_channel_ids=event.target_channel_ids,
        )
        db.add(event_row)
        await db.flush()  # assign event_row.id

        for ch in channels:
            d = NotificationDelivery(
                event_id=event_row.id, channel_id=ch.id, channel_type=ch.channel_type,
                status=DeliveryStatus.pending.value, attempts=0,
            )
            db.add(d)
            await db.flush()
            delivery_ids.append(d.id)
        await db.commit()

    # Attempt all channels concurrently — one failing never blocks the others.
    if delivery_ids:
        await asyncio.gather(*(_attempt_delivery(did) for did in delivery_ids))


async def _resolve_channels(db: AsyncSession,
                            event: NotificationEvent) -> list[CustomerNotificationChannel]:
    stmt = select(CustomerNotificationChannel).where(
        CustomerNotificationChannel.customer_code == event.customer_code,
        CustomerNotificationChannel.enabled.is_(True),
    )
    rows = list((await db.execute(stmt)).scalars().all())
    if event.target_channel_ids:
        wanted = {str(c) for c in event.target_channel_ids}
        rows = [r for r in rows if str(r.id) in wanted]
    return rows


# ---- delivery attempt (shared by publish + retry) -------------------------------------------------
async def _attempt_delivery(delivery_id: uuid.UUID) -> None:
    async with async_session() as db:
        delivery = await db.get(NotificationDelivery, delivery_id)
        if delivery is None or delivery.status == DeliveryStatus.delivered.value:
            return
        event_row = await db.get(NotificationEventRow, delivery.event_id)
        channel_row = (await db.get(CustomerNotificationChannel, delivery.channel_id)
                       if delivery.channel_id else None)
        adapter = get_channel(delivery.channel_type)

        try:
            if channel_row is None:
                raise RuntimeError("target channel was removed")
            if not channel_row.enabled:
                raise RuntimeError("target channel is disabled")
            if adapter is None:
                raise RuntimeError(f"no adapter registered for channel type {delivery.channel_type!r}")
            await adapter.send(_event_from_row(event_row), channel_row.config or {})
        except Exception as exc:  # noqa: BLE001 — any failure → record + schedule retry
            _record_failure(delivery, exc)
            logger.warning("notification delivery %s failed (attempt %d, status=%s): %s",
                           delivery.id, delivery.attempts, delivery.status, exc)
        else:
            delivery.status = DeliveryStatus.delivered.value
            delivery.delivered_at = _now()
            delivery.last_error = None
            delivery.next_attempt_at = None
        await db.commit()


def _record_failure(delivery: NotificationDelivery, exc: Exception) -> None:
    delivery.attempts += 1
    delivery.last_error = str(exc)[:2000]
    if delivery.attempts >= settings.notification_max_attempts:
        delivery.status = DeliveryStatus.dead.value
        delivery.next_attempt_at = None
    else:
        delivery.status = DeliveryStatus.failed.value
        delivery.next_attempt_at = _now() + timedelta(seconds=_backoff_seconds(delivery.attempts))


# ---- redelivery loop (store-and-forward) ----------------------------------------------------------
async def retry_pending(batch: int = 100) -> int:
    """Claim due pending/failed deliveries and re-attempt them. Returns how many were attempted.

    Claiming uses FOR UPDATE SKIP LOCKED and bumps next_attempt_at to a short lease so a second
    worker process won't grab the same rows, and a crash mid-send only delays (never duplicates much).
    """
    now = _now()
    lease = timedelta(seconds=max(settings.notification_poll_seconds * 3, 60))
    claimed: list[uuid.UUID] = []
    async with async_session() as db:
        async with db.begin():
            rows = (await db.execute(
                select(NotificationDelivery).where(
                    NotificationDelivery.status.in_(
                        [DeliveryStatus.pending.value, DeliveryStatus.failed.value]),
                    (NotificationDelivery.next_attempt_at.is_(None)) |
                    (NotificationDelivery.next_attempt_at <= now),
                ).order_by(NotificationDelivery.next_attempt_at.asc().nullsfirst())
                .limit(batch).with_for_update(skip_locked=True)
            )).scalars().all()
            for r in rows:
                r.next_attempt_at = now + lease  # lease so a concurrent loop skips it
                claimed.append(r.id)
        # transaction committed here → locks released

    for did in claimed:
        await _attempt_delivery(did)
    return len(claimed)


def register() -> None:
    """Subscribe the dispatcher to the bus (called once on startup)."""
    bus.subscribe(handle)
