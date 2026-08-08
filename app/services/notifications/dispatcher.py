"""Dispatcher — the bus subscriber that makes delivery durable, multi-channel, and tracked.

On publish it (1) persists the event to the outbox (idempotent by dedup_key), (2) creates one
`pending` delivery row per targeted channel = the fan-out, then (3) attempts each delivery
concurrently. Failures are not fatal: the row stays `failed` with a backoff schedule and the
outbox drain (`deliver_due`) attempts it until it succeeds or hits the attempt cap (then
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
def _event_row(event: NotificationEvent) -> NotificationEventRow:
    """The outbox row for a published event."""
    return NotificationEventRow(
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


async def enqueue(event: NotificationEvent) -> list[uuid.UUID]:
    """Persist the event + one delivery row per target channel. Returns those delivery ids.

    **This never sends.** Delivery belongs to `deliver_due`, the outbox drain, which is the single
    place HTTP happens — and therefore the only place pacing, cross-tenant fairness and Retry-After
    can be applied. Sending here (as this did originally) left nowhere to attach any of them, and let
    one tenant's flood block every other tenant inside the evaluation loop.

    Costs no latency: the worker drains immediately after publishing, and a new delivery has
    `next_attempt_at IS NULL`, which the drain already treats as due.

    Idempotent by dedup_key — a re-published event returns [] rather than duplicating deliveries.
    """
    delivery_ids: list[uuid.UUID] = []
    async with async_session() as db:
        existing = await db.scalar(
            select(NotificationEventRow).where(NotificationEventRow.dedup_key == event.dedup_key)
        )
        if existing is not None:
            return []  # already published (and deliveries already created) — nothing to do

        channels = await _resolve_channels(db, event)
        if not channels:
            logger.info("notification event %s has no enabled target channel for customer %s — "
                        "persisting outbox only", event.dedup_key, event.customer_code)

        event_row = _event_row(event)
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
        await db.commit()   # durable BEFORE anything is sent — an outage cannot lose the alert
    return delivery_ids


async def handle(event: NotificationEvent) -> None:
    """Bus subscriber. Enqueue only; the worker's drain does the sending."""
    await enqueue(event)


async def deliver_now(delivery_ids: list[uuid.UUID]) -> None:
    """Send these deliveries immediately, bypassing the drain.

    ONLY for interactive paths — `POST /{customer}/publish`, where a person clicked "notify the team"
    and the endpoint's contract is to return the per-channel outcome. One human action to a handful of
    channels cannot flood, so skipping the paced drain is acceptable there and nowhere else.

    Concurrent across channels: one failing channel must never hold up the others.
    """
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
async def _send(adapter, delivery: NotificationDelivery,
                channel_row: CustomerNotificationChannel | None, event_row) -> None:
    """Hand the event to its transport, or raise.

    Every reason a send cannot proceed is raised rather than returned, so all of them land on the same
    failure path and get recorded, backed off and eventually dead-lettered. A channel deleted or
    disabled mid-flight is a delivery failure like any other — silently returning would leave the row
    pending forever with nothing explaining why.
    """
    if channel_row is None:
        raise RuntimeError("target channel was removed")
    if not channel_row.enabled:
        raise RuntimeError("target channel is disabled")
    if adapter is None:
        raise RuntimeError(f"no adapter registered for channel type {delivery.channel_type!r}")
    await adapter.send(_event_from_row(event_row), channel_row.config or {})


def _record_success(delivery: NotificationDelivery) -> None:
    """Clear the retry state as well as marking it delivered, so a row that succeeded after failures
    does not keep a stale error or a scheduled next attempt."""
    delivery.status = DeliveryStatus.delivered.value
    delivery.delivered_at = _now()
    delivery.last_error = None
    delivery.next_attempt_at = None


async def _load_for_send(db, delivery_id: uuid.UUID):
    """The delivery plus everything needed to send it, or None if there is nothing to do.

    None means already delivered or already gone — both are normal races, not errors: two workers can
    claim overlapping batches, and an event can be purged mid-flight.
    """
    delivery = await db.get(NotificationDelivery, delivery_id)
    if delivery is None or delivery.status == DeliveryStatus.delivered.value:
        return None
    event_row = await db.get(NotificationEventRow, delivery.event_id)
    channel_row = (await db.get(CustomerNotificationChannel, delivery.channel_id)
                   if delivery.channel_id else None)
    return delivery, event_row, channel_row


async def _attempt_delivery(delivery_id: uuid.UUID) -> None:
    async with async_session() as db:
        loaded = await _load_for_send(db, delivery_id)
        if loaded is None:
            return
        delivery, event_row, channel_row = loaded

        try:
            await _send(get_channel(delivery.channel_type), delivery, channel_row, event_row)
        except Exception as exc:  # noqa: BLE001 — any failure → record + schedule retry
            _record_failure(delivery, exc)
            logger.warning("notification delivery %s failed (attempt %d, status=%s): %s",
                           delivery.id, delivery.attempts, delivery.status, exc)
        else:
            _record_success(delivery)
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


# ---- the outbox drain: the ONE place delivery happens ---------------------------------------------
async def deliver_due(batch: int | None = None) -> int:
    """Claim every delivery that is due and attempt it. Returns how many were attempted.

    Named for what it now does. It began as a retry loop for failures, but since enqueuing stopped
    sending this is the PRIMARY delivery path: a freshly published delivery has
    `next_attempt_at IS NULL`, which the predicate below treats as due, so first attempts and retries
    drain through exactly the same code.

    That single seam is the point of the change. Per-channel pacing, cross-tenant fairness and
    Retry-After all attach here, and cannot be bypassed by a new publisher.

    `batch` bounds one pass, so a burst is spread across ticks rather than sent as one wall of HTTP.

    Claiming uses FOR UPDATE SKIP LOCKED and bumps next_attempt_at to a short lease so a second
    worker process won't grab the same rows, and a crash mid-send only delays (never duplicates much).
    """
    batch = batch if batch is not None else settings.notification_delivery_batch
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
