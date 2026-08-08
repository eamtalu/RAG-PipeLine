"""Chunk 33 (step 7 of docs/plan/2026-08-08_notification-architecture.html): stop scaffolded channels
from silently burning the retry budget.

`SlackChannel.send` and `WhatsAppChannel.send` raise `NotImplementedError` — they are registered
adapters with no implementation behind them. But the create endpoint validates against
`KNOWN_CHANNEL_TYPES`, which includes both, so a Slack channel can be configured today. Every alert
routed to it then fails 50 times on the retry ladder before dead-lettering, logging a warning each
time, for a reason no amount of retrying could ever fix.

Two fixes, and the order matters:

**Prevent it.** The create endpoint validates against `IMPLEMENTED_CHANNEL_TYPES`, with a message that
distinguishes "we know this transport but it is not built yet" from "we have never heard of it" —
those call for completely different reactions from whoever hit the error.

**Fail fast for anything that already exists.** A `NotImplementedError` dead-letters on the FIRST
attempt rather than the 50th. This is the same permanent-versus-transient distinction the Stage 1
ingest queue already makes (`app/services/queueing/retry_policy.py`): a missing implementation is not
a flaky network, and retrying it just triples the log noise.

Production currently has only Teams channels, so the second fix is defence in depth rather than a
repair — but it is what stops the same trap reappearing the moment a fourth adapter is scaffolded.
"""

import uuid

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select

from app.persistence.models.notification import (
    CustomerNotificationChannel, DeliveryStatus, NotificationDelivery, NotificationEvent,
)
from app.services.notifications import dispatcher
from app.services.notifications.channels import (
    CHANNELS, IMPLEMENTED_CHANNEL_TYPES, KNOWN_CHANNEL_TYPES,
)
from app.settings import settings

CC = "test_chunk33"


# =============================================================== the registry
def test_the_two_lists_are_not_the_same_thing():
    """`KNOWN` is what the code can name; `IMPLEMENTED` is what can actually deliver. Conflating them
    is exactly the bug this chunk fixes."""
    assert IMPLEMENTED_CHANNEL_TYPES < KNOWN_CHANNEL_TYPES
    assert "teams" in IMPLEMENTED_CHANNEL_TYPES


def test_every_scaffolded_adapter_refuses_loudly():
    """A stub that silently returned would look like a successful delivery — the alert would be marked
    delivered and never seen by anyone."""
    import asyncio
    for name in KNOWN_CHANNEL_TYPES - IMPLEMENTED_CHANNEL_TYPES:
        with pytest.raises(NotImplementedError):
            asyncio.get_event_loop().run_until_complete(CHANNELS[name].send(None, {}))


def test_every_implemented_adapter_is_actually_registered():
    """Guards the reverse mistake: claiming something is implemented when nothing serves it."""
    assert IMPLEMENTED_CHANNEL_TYPES <= set(CHANNELS)


# =============================================================== creating a channel
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


async def _create(channel_type):
    """Drive the real endpoint, not the repository — the guard belongs at the API boundary."""
    from app.api.v1.notifications import create_channel_endpoint, CreateChannelRequest
    from app.persistence.repositories.notification_repository import NotificationRepository
    from app.persistence.repositories.customer_repository import CustomerRepository
    from app.config.database import async_session
    async with async_session() as s:
        return await create_channel_endpoint(
            CC, CreateChannelRequest(channel_type=channel_type, name="c",
                                     config={"webhook_url": "http://x"}),
            repo=NotificationRepository(s), customers=CustomerRepository(s))


@pytest.fixture
async def customer(db):
    """A registered tenant — the endpoint refuses unknown customers before it looks at the type."""
    from app.persistence.models.customer import Customer
    from app.config.database import async_session
    async with async_session() as s:
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        s.add(Customer(customer_code=CC, name="chunk33", active=True))
        await s.commit()
    await _cleanup()
    yield
    await _cleanup()
    async with async_session() as s:
        await s.execute(delete(Customer).where(Customer.customer_code == CC))
        await s.commit()


async def test_an_implemented_channel_can_be_created(customer):
    """No regression — Teams is the one that works and must keep working."""
    out = await _create("teams")
    assert out["channel_type"] == "teams"


async def test_a_scaffolded_channel_is_refused(customer):
    """The primary fix. Creating one is what leads to 50 pointless retries per alert."""
    with pytest.raises(HTTPException) as e:
        await _create("slack")
    assert e.value.status_code == 400


async def test_the_refusal_distinguishes_not_built_from_never_heard_of(customer):
    """Two very different situations for whoever hit the error: one means 'wait for it to be built',
    the other means 'you typed it wrong'. A single generic message would leave them guessing."""
    with pytest.raises(HTTPException) as scaffolded:
        await _create("slack")
    with pytest.raises(HTTPException) as unknown:
        await _create("carrier-pigeon")
    assert "slack" in scaffolded.value.detail and "not implemented" in scaffolded.value.detail.lower()
    assert "teams" in scaffolded.value.detail, "it must say what IS available"
    assert "unknown" in unknown.value.detail.lower()
    assert scaffolded.value.detail != unknown.value.detail


async def test_no_channel_row_is_left_behind_by_a_refusal(customer):
    """A half-created channel would keep producing failing deliveries — the exact outcome being
    prevented."""
    with pytest.raises(HTTPException):
        await _create("whatsapp")
    from app.config.database import async_session
    from sqlalchemy import func
    async with async_session() as s:
        n = await s.scalar(select(func.count()).select_from(CustomerNotificationChannel)
                           .where(CustomerNotificationChannel.customer_code == CC))
    assert n == 0


# =============================================================== fail fast for what already exists
def test_a_missing_implementation_dead_letters_immediately():
    """A NotImplementedError cannot be fixed by trying again. Spending 50 attempts on it is pure log
    noise, and delays the dead-letter that tells an operator something is actually misconfigured."""
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="slack",
                             status=DeliveryStatus.pending.value, attempts=0)
    dispatcher._record_failure(d, NotImplementedError("Slack channel is scaffolded"))
    assert d.status == DeliveryStatus.dead.value, "must dead-letter on the FIRST attempt"
    assert d.next_attempt_at is None, "and must not be scheduled for another try"


def test_the_dead_letter_reason_is_preserved():
    """`last_error` is the only thing telling an operator why it stopped."""
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="slack",
                             status=DeliveryStatus.pending.value, attempts=0)
    dispatcher._record_failure(d, NotImplementedError("Slack channel is scaffolded but not implemented"))
    assert d.last_error and "scaffolded" in d.last_error


def test_an_ordinary_failure_still_gets_its_full_budget():
    """No regression: a network blip must still be retried, not discarded on the first stumble."""
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="teams",
                             status=DeliveryStatus.pending.value, attempts=0)
    dispatcher._record_failure(d, RuntimeError("connection reset"))
    assert d.status == DeliveryStatus.failed.value
    assert d.attempts == 1
    assert d.next_attempt_at is not None


def test_a_disabled_channel_is_retried_not_dead_lettered():
    """Someone can re-enable it, so this genuinely may succeed later — unlike a missing
    implementation. Getting these two confused in either direction loses alerts or wastes attempts."""
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="teams",
                             status=DeliveryStatus.pending.value, attempts=0)
    dispatcher._record_failure(d, RuntimeError("target channel is disabled"))
    assert d.status == DeliveryStatus.failed.value


def test_a_throttled_send_is_still_neither_of_those():
    """The three-way distinction stays intact: throttled != failed != permanently broken."""
    from app.services.notifications.channels.base import ChannelRateLimited
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="teams",
                             status=DeliveryStatus.pending.value, attempts=4)
    dispatcher._record_failure(d, ChannelRateLimited(retry_after=10.0))
    assert d.attempts == 4
    assert d.status == DeliveryStatus.pending.value


# =============================================================== the UI contract
async def test_the_api_still_reports_both_lists(db):
    """The picker needs to show scaffolded types greyed out rather than hiding them, so the roadmap is
    visible instead of looking like the transport does not exist."""
    from app.api.v1.notifications import list_channel_types
    out = await list_channel_types()
    assert set(out["channel_types"]) == KNOWN_CHANNEL_TYPES
    assert set(out["implemented"]) == IMPLEMENTED_CHANNEL_TYPES
