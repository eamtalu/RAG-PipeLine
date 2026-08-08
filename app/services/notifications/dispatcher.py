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

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.notification import (
    NotificationRule,
    CustomerNotificationChannel,
    NotificationEvent as NotificationEventRow,
    NotificationDelivery,
    DeliveryStatus,
)
from app.services.notifications.bus import bus
from app.services.notifications.events import NotificationEvent
from app.services.notifications import pacing, rollup
from app.services.notifications.channels import get_channel
from app.services.notifications.channels.base import ChannelRateLimited

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
async def _delivery_status_for(db, event: NotificationEvent) -> str:
    """`pending` normally; `suppressed` once this rule has spilled past its burst cap for the window.

    Two exemptions, both load-bearing:

    *Events with no rule* — manual publishes and channel tests. Human-initiated, cannot flood, and
    silently collapsing them would be baffling.

    *Rollup summaries* — they carry their rule's id for provenance, so without this the cap would
    suppress the very card that exists to report the suppression, and the flood would vanish in
    silence. Exempted by event TYPE rather than by a missing rule, so the provenance is kept.
    """
    if not event.rule_id or event.event_type == rollup.EVENT_TYPE:
        return DeliveryStatus.pending.value
    rule_id = uuid.UUID(event.rule_id)
    window = settings.notification_rollup_window_seconds
    since = _now() - timedelta(seconds=window)
    rule = await db.get(NotificationRule, rule_id)
    cap = rollup.burst_cap(rule.match if rule else None)
    already = await rollup.count_delivered_this_window(db, rule_id, since)
    if rollup.should_suppress(already_sent=already, cap=cap):
        return DeliveryStatus.suppressed.value
    return DeliveryStatus.pending.value


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

        status = await _delivery_status_for(db, event)
        for ch in channels:
            d = NotificationDelivery(
                event_id=event_row.id, channel_id=ch.id, channel_type=ch.channel_type,
                status=status, attempts=0,
            )
            db.add(d)
            await db.flush()
            if status == DeliveryStatus.pending.value:
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


def _record_throttled(delivery: NotificationDelivery, exc: ChannelRateLimited) -> None:
    """The channel asked us to slow down. That is not a delivery defect.

    So `attempts` is untouched and the row stays `pending`: counting a 429 toward
    notification_max_attempts would dead-letter a perfectly good alert after 50 rate-limit responses,
    which is the opposite of what being throttled means. We wait the server's own Retry-After when it
    gave one, and fall back to our jittered window when it did not — some throttling responses omit
    the header, and retrying immediately would only make it worse.
    """
    delay = exc.retry_after if exc.retry_after is not None else None
    delivery.next_attempt_at = (_now() + timedelta(seconds=delay) if delay is not None
                                else pacing.retry_at(_now()))
    logger.info("notification delivery %s throttled by the channel; retrying at %s",
                delivery.id, delivery.next_attempt_at.isoformat())


def _record_failure(delivery: NotificationDelivery, exc: Exception) -> None:
    if isinstance(exc, ChannelRateLimited):
        return _record_throttled(delivery, exc)
    delivery.attempts += 1
    delivery.last_error = str(exc)[:2000]
    if delivery.attempts >= settings.notification_max_attempts:
        delivery.status = DeliveryStatus.dead.value
        delivery.next_attempt_at = None
    else:
        delivery.status = DeliveryStatus.failed.value
        delivery.next_attempt_at = _now() + timedelta(seconds=_backoff_seconds(delivery.attempts))


def _spend_budgets(selected, budgets: dict, *, now: datetime,
                   lease: timedelta) -> list[uuid.UUID]:
    """Claim what fits each channel's remaining budget; reschedule the rest. Returns ids to send.

    A row held back is NOT a failure: `attempts` is untouched and no error is recorded, because it was
    never attempted. It is rescheduled inside the rate window, jittered so a burst deferred together
    does not come back in lockstep.
    """
    claimed: list[uuid.UUID] = []
    for row, _cc in selected:
        if budgets.get(row.channel_id, 0) <= 0:
            row.next_attempt_at = pacing.retry_at(now)
            continue
        budgets[row.channel_id] -= 1
        row.next_attempt_at = now + lease  # lease so a concurrent loop skips it
        claimed.append(row.id)
    return claimed


async def _claim_due(db, now: datetime) -> list[tuple[NotificationDelivery, str]]:
    """Every delivery that is due, locked, paired with its owning tenant.

    Deliberately UNBOUNDED: fairness has to be decided across the whole due set, and a `LIMIT` here
    would cut the batch before round-robin ever saw the quiet tenants. The set is small in practice —
    it is what is due right now, not the whole outbox — and the row cap still applies afterwards.

    The tenant comes from the event, since a delivery does not carry `customer_code` itself.
    """
    rows = (await db.execute(
        select(NotificationDelivery, NotificationEventRow.customer_code)
        .join(NotificationEventRow, NotificationEventRow.id == NotificationDelivery.event_id)
        .where(
            NotificationDelivery.status.in_(
                [DeliveryStatus.pending.value, DeliveryStatus.failed.value]),
            (NotificationDelivery.next_attempt_at.is_(None)) |
            (NotificationDelivery.next_attempt_at <= now),
        )
        .order_by(NotificationDelivery.created_at.asc())
        .with_for_update(skip_locked=True, of=NotificationDelivery)
    )).all()
    return [(r, cc) for r, cc in rows]


async def _channel_budgets(db, channel_ids: set, now: datetime) -> dict:
    """Remaining sends per channel in the current rate window.

    Counted from the deliveries already made rather than from a separate counter table, so it stays
    correct across restarts and across worker processes for free.
    """
    ids = [c for c in channel_ids if c is not None]
    if not ids:
        return {}
    since = now - timedelta(seconds=settings.notification_rate_window_seconds)
    sent = dict((await db.execute(
        select(NotificationDelivery.channel_id, func.count())
        .where(NotificationDelivery.channel_id.in_(ids),
               NotificationDelivery.delivered_at.isnot(None),
               NotificationDelivery.delivered_at >= since)
        .group_by(NotificationDelivery.channel_id)
    )).all())
    configs = dict((await db.execute(
        select(CustomerNotificationChannel.id, CustomerNotificationChannel.config)
        .where(CustomerNotificationChannel.id.in_(ids))
    )).all())
    return {cid: pacing.allowance(sent_in_window=sent.get(cid, 0),
                                  limit=pacing.channel_limit(configs.get(cid)))
            for cid in ids}


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
    async with async_session() as db:
        async with db.begin():
            due = await _claim_due(db, now)
            # Fair across tenants BEFORE the batch is cut. Ordering by next_attempt_at and taking the
            # first N let one tenant's flood fill every batch, because freshly published deliveries
            # all have NULL — the quiet tenant's single alert waited however many ticks the flood took.
            selected = pacing.round_robin(due, key=lambda r: r[1], limit=batch)

            budgets = await _channel_budgets(db, {r.channel_id for r, _ in selected}, now)
            claimed = _spend_budgets(selected, budgets, now=now, lease=lease)
        # transaction committed here → locks released

    for did in claimed:
        await _attempt_delivery(did)
    return len(claimed)


def register() -> None:
    """Subscribe the dispatcher to the bus (called once on startup)."""
    bus.subscribe(handle)
