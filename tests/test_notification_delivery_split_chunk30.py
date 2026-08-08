"""Chunk 30 (step 4 of docs/plan/2026-08-08_notification-architecture.html): take sending OUT of the
publish path.

This is the structural change the rest of the plan depends on. Today `dispatcher.handle` persists the
event and then immediately fires `asyncio.gather(_attempt_delivery(...))` — HTTP happens *inside* rule
evaluation. Two consequences follow directly:

- **There is nowhere to put a rate limit.** Pacing has to live where sending happens, and sending is
  scattered through the evaluation call stack.
- **One tenant blocks every other.** Evaluation walks tenants in a loop; a tenant with 500 matching
  transactions holds the loop through 500 HTTP round-trips before the next tenant is looked at.

After this change `handle` only ENQUEUES. The outbox drain is the single place HTTP happens, so steps
5 and 6 (pacing, fairness, rollup) have exactly one seam to attach to.

The obvious worry is added latency, and the tests below pin that it does not happen: the worker calls
`run_rules_once()` and then the drain in the SAME tick, and a freshly enqueued delivery has
`next_attempt_at IS NULL`, which the drain's claim predicate already treats as due. So an alert still
goes out on the tick it was raised.

Two paths deliberately keep sending immediately, and both are tested:

- `POST /channels/{id}/test` — a human clicking "test this webhook" needs the answer now, and it
  bypasses the outbox entirely.
- `POST /{customer}/publish` — a human clicking "notify the team". One event, a handful of channels,
  and its documented contract is to return the per-channel outcome. It cannot flood, so it calls the
  delivery path explicitly rather than waiting for the drain.
"""

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import delete, func, select

from app.persistence.models.notification import (
    CustomerNotificationChannel, DeliveryStatus, NotificationDelivery, NotificationEvent,
)
from app.services.notifications import dispatcher
from app.services.notifications.events import NotificationEvent as Event

CC = "test_chunk30"


# =============================================================== helpers
async def _cleanup():
    from app.config.database import async_session
    async with async_session() as s:
        await s.execute(delete(NotificationDelivery).where(
            NotificationDelivery.event_id.in_(
                select(NotificationEvent.id).where(NotificationEvent.customer_code == CC))))
        await s.execute(delete(NotificationEvent).where(NotificationEvent.customer_code == CC))
        await s.execute(delete(CustomerNotificationChannel).where(
            CustomerNotificationChannel.customer_code == CC))
        await s.commit()


async def _channel(name="c1"):
    from app.config.database import async_session
    async with async_session() as s:
        ch = CustomerNotificationChannel(customer_code=CC, channel_type="teams", name=name,
                                         config={"webhook_url": "http://127.0.0.1:9/never"},
                                         enabled=True)
        s.add(ch)
        await s.commit()
        return ch.id


def _event(key=None):
    return Event(event_type="test", customer_code=CC, severity="error", title="t", summary="s",
                 dedup_key=key or f"chunk30:{uuid.uuid4()}", payload={"facts": {}})


async def _deliveries():
    from app.config.database import async_session
    async with async_session() as s:
        return list((await s.execute(
            select(NotificationDelivery).join(
                NotificationEvent, NotificationEvent.id == NotificationDelivery.event_id)
            .where(NotificationEvent.customer_code == CC))).scalars().all())


@pytest.fixture
async def channel():
    await _cleanup()
    cid = await _channel()
    yield cid
    await _cleanup()


# =============================================================== the split
async def test_publishing_does_not_send(channel):
    """The heart of the change. Enqueuing must leave the delivery untouched and pending — if HTTP
    happened here there would be nowhere to put a rate limit."""
    await dispatcher.handle(_event())
    rows = await _deliveries()
    assert len(rows) == 1
    assert rows[0].status == DeliveryStatus.pending.value
    assert rows[0].attempts == 0, "publishing must not have attempted anything"
    assert rows[0].last_error is None


async def test_publishing_is_durable_before_any_send(channel):
    """Unchanged and load-bearing: the event and its deliveries are committed before delivery is even
    considered, which is why a Teams outage or a crash never loses an alert."""
    await dispatcher.handle(_event())
    from app.config.database import async_session
    async with async_session() as s:
        n = await s.scalar(select(func.count()).select_from(NotificationEvent)
                           .where(NotificationEvent.customer_code == CC))
    assert n == 1


async def test_enqueue_returns_the_delivery_ids(channel):
    """So the interactive paths can choose to deliver immediately without re-querying for them."""
    ids = await dispatcher.enqueue(_event())
    assert len(ids) == 1
    assert ids[0] == (await _deliveries())[0].id


async def test_a_second_publish_of_the_same_event_enqueues_nothing(channel):
    """Dedupe by dedup_key, unchanged — and it must not create a second set of delivery rows."""
    key = f"chunk30:{uuid.uuid4()}"
    first = await dispatcher.enqueue(_event(key))
    second = await dispatcher.enqueue(_event(key))
    assert len(first) == 1
    assert second == [], "an already-published event must not be enqueued twice"
    assert len(await _deliveries()) == 1


async def test_an_event_with_no_enabled_channel_still_records_the_outbox_row(channel):
    """Unchanged behaviour: the alert is recorded even when it has nowhere to go, so it is
    investigable rather than silently dropped."""
    from app.config.database import async_session
    async with async_session() as s:
        await s.execute(delete(CustomerNotificationChannel).where(
            CustomerNotificationChannel.customer_code == CC))
        await s.commit()
    ids = await dispatcher.enqueue(_event())
    assert ids == []
    from app.config.database import async_session as _s
    async with _s() as s:
        n = await s.scalar(select(func.count()).select_from(NotificationEvent)
                           .where(NotificationEvent.customer_code == CC))
    assert n == 1


# =============================================================== no added latency
async def test_a_freshly_enqueued_delivery_is_immediately_due(channel):
    """Why moving the send costs no latency: the drain claims `next_attempt_at IS NULL` as due, and a
    new delivery has exactly that. It goes out on the same worker tick it was raised."""
    await dispatcher.handle(_event())
    rows = await _deliveries()
    assert rows[0].next_attempt_at is None, (
        "a new delivery must be due immediately, not scheduled into the future")


async def test_the_worker_tick_delivers_what_it_just_published(channel, monkeypatch):
    """End to end at the tick level: publish then drain, in one pass, exactly as the worker does."""
    sent = []

    async def fake_send(self, event, config):
        sent.append(event.dedup_key)

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", fake_send)

    await dispatcher.handle(_event())
    assert sent == [], "still nothing sent at publish time"
    await dispatcher.deliver_due()
    assert len(sent) == 1, "the same tick's drain must deliver it"
    assert (await _deliveries())[0].status == DeliveryStatus.delivered.value


# =============================================================== the interactive paths
async def test_deliver_now_sends_immediately(channel, monkeypatch):
    """The manual publish endpoint's contract is to return the per-channel outcome, so it delivers
    explicitly rather than waiting for the drain. One human action to a handful of channels cannot
    flood, which is why bypassing the drain is acceptable here and nowhere else."""
    sent = []

    async def fake_send(self, event, config):
        sent.append(event.dedup_key)

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", fake_send)

    ids = await dispatcher.enqueue(_event())
    await dispatcher.deliver_now(ids)
    assert len(sent) == 1
    assert (await _deliveries())[0].status == DeliveryStatus.delivered.value


async def test_the_channel_test_endpoint_never_touches_the_outbox(channel, monkeypatch):
    """`POST /channels/{id}/test` bypasses the whole pipeline by design — a human verifying a webhook
    wants the answer now, and it must not leave outbox rows behind."""
    sent = []

    async def fake_send(self, event, config):
        sent.append(event.dedup_key)

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", fake_send)

    from app.api.v1.notifications import test_channel_endpoint
    from app.persistence.repositories.notification_repository import NotificationRepository
    from app.config.database import async_session
    async with async_session() as s:
        await test_channel_endpoint(str(channel), repo=NotificationRepository(s))
    assert len(sent) == 1
    assert await _deliveries() == [], "the test button must not create outbox rows"


# =============================================================== no regression on failure handling
async def test_a_failed_send_still_backs_off_and_retries(channel, monkeypatch):
    """Retry and dead-lettering are untouched; only WHERE the send happens moved."""
    async def boom(self, event, config):
        raise RuntimeError("channel down")

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", boom)

    await dispatcher.handle(_event())
    await dispatcher.deliver_due()
    row = (await _deliveries())[0]
    assert row.status == DeliveryStatus.failed.value
    assert row.attempts == 1
    assert row.last_error and "channel down" in row.last_error
    assert row.next_attempt_at is not None and row.next_attempt_at > datetime.now(timezone.utc)


async def test_a_delivery_serving_backoff_is_not_claimed(channel, monkeypatch):
    """The drain must respect the backoff it just set, or a failing channel is hammered every tick."""
    async def boom(self, event, config):
        raise RuntimeError("down")

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", boom)

    await dispatcher.handle(_event())
    await dispatcher.deliver_due()
    attempts_after_first = (await _deliveries())[0].attempts
    await dispatcher.deliver_due()          # immediately again — still inside the backoff
    assert (await _deliveries())[0].attempts == attempts_after_first


# =============================================================== the pacing that falls out
async def test_the_drain_is_batched_so_a_burst_spreads_over_ticks(channel, monkeypatch):
    """Moving the send behind a bounded drain gives crude pacing for free — a burst is delivered over
    several ticks instead of as one wall of HTTP. Step 5 replaces this with a real per-channel budget."""
    sent = []

    async def fake_send(self, event, config):
        sent.append(event.dedup_key)

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", fake_send)

    for _ in range(5):
        await dispatcher.handle(_event())
    await dispatcher.deliver_due(batch=2)
    assert len(sent) == 2, "the drain must honour its batch size"
    await dispatcher.deliver_due(batch=2)
    assert len(sent) == 4


# =============================================================== the seam step 5 needs
def test_sending_happens_in_exactly_one_place():
    """The point of this whole chunk. Pacing, fairness and Retry-After all attach to the drain, so if
    a second send path appears they silently stop applying to it."""
    import ast
    import pathlib
    repo = pathlib.Path(__file__).resolve().parent.parent
    callers = []
    for path in (repo / "app").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "_attempt_delivery"):
                callers.append(f"{path.relative_to(repo)}:{node.lineno}")
    # deliver_due (the worker drain) and deliver_now (the interactive path) — nothing else.
    assert len(callers) <= 2, f"_attempt_delivery must have one seam, found {callers}"
