"""Chunk 31 (step 5 of docs/plan/2026-08-08_notification-architecture.html): pace the drain, be fair
across tenants, and obey a 429.

Step 4 made this possible by leaving exactly one place where HTTP happens. Three things attach there,
and they solve three different problems:

**Pacing** — a per-channel budget. Without it, 500 matching transactions become 500 back-to-back POSTs
at whatever rate the network allows. Configured per channel (Teams and Slack do not throttle alike,
and one noisy tenant should not force everyone down) with a global default.

**Fairness** — the claim used to order by `next_attempt_at` and take the first N. Freshly published
deliveries all have NULL, so a tenant with 500 of them fills the batch and every other tenant waits
however many ticks it takes to drain. Round-robin takes the oldest from each tenant, then the next
from each, so a flood delays itself rather than everybody.

**Retry-After** — a 429 is currently indistinguishable from a network error: it burns an attempt off
the 50-attempt budget and backs off on a ladder Microsoft never asked for. Being throttled is not a
delivery defect, so it must neither consume the budget nor guess the delay.

The distinction that runs through all of this: a delivery held back for budget is NOT a failure. It
has not been attempted, so it must not increment `attempts`, must not record an error, and must never
edge toward dead-lettering. Confusing "not yet" with "went wrong" would silently discard alerts after
50 quiet deferrals.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.persistence.models.notification import (
    CustomerNotificationChannel, DeliveryStatus, NotificationDelivery, NotificationEvent,
)
from app.services.notifications import dispatcher, pacing
from app.services.notifications.channels.base import ChannelRateLimited
from app.services.notifications.events import NotificationEvent as Event
from app.settings import settings

CC_A = "test_chunk31_a"
CC_B = "test_chunk31_b"


# =============================================================== the budget (pure)
def test_a_channel_uses_the_global_default_when_it_says_nothing():
    assert pacing.channel_limit({}) == settings.notification_channel_max_per_minute
    assert pacing.channel_limit(None) == settings.notification_channel_max_per_minute


def test_a_channel_can_override_the_default():
    """Teams and Slack do not throttle alike, and one tenant's tight webhook should not force every
    other channel down to its rate."""
    assert pacing.channel_limit({"max_per_minute": 3}) == 3


def test_a_nonsense_override_falls_back_rather_than_crashing_the_drain():
    """`config` is operator-edited JSONB. A bad value must not take delivery down for everyone."""
    for bad in ({"max_per_minute": "many"}, {"max_per_minute": None}, {"max_per_minute": -5},
                {"max_per_minute": 0}):
        assert pacing.channel_limit(bad) == settings.notification_channel_max_per_minute


def test_the_allowance_is_what_is_left_of_the_budget():
    assert pacing.allowance(sent_in_window=0, limit=10) == 10
    assert pacing.allowance(sent_in_window=7, limit=10) == 3


def test_an_exhausted_budget_allows_nothing_and_never_goes_negative():
    """A negative allowance would be read as 'send this many' by any caller doing arithmetic on it."""
    assert pacing.allowance(sent_in_window=10, limit=10) == 0
    assert pacing.allowance(sent_in_window=99, limit=10) == 0


def test_a_deferred_delivery_is_rescheduled_inside_the_window():
    """Far enough out that a slot has genuinely freed, near enough that the alert is not parked for
    minutes. Jittered so a burst deferred together does not return as a synchronised thundering herd."""
    now = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    whens = {pacing.retry_at(now) for _ in range(40)}
    assert all(now < w <= now + timedelta(seconds=settings.notification_rate_window_seconds)
               for w in whens)
    assert len(whens) > 1, "deferrals must be jittered, not all scheduled to the same instant"


def test_the_pacing_settings_are_sane():
    assert settings.notification_channel_max_per_minute > 0
    assert settings.notification_rate_window_seconds > 0


# =============================================================== fairness (pure)
def test_round_robin_interleaves_tenants():
    """One from each tenant, then the next from each. A tenant with a backlog delays itself rather
    than everyone."""
    queued = [("a", 1), ("a", 2), ("a", 3), ("b", 10), ("c", 20), ("c", 21)]
    got = pacing.round_robin(queued, key=lambda x: x[0], limit=4)
    assert [t for t, _ in got] == ["a", "b", "c", "a"]


def test_round_robin_preserves_order_within_a_tenant():
    """Oldest first inside a tenant, or an alert could sit behind newer ones indefinitely."""
    queued = [("a", 1), ("a", 2), ("a", 3)]
    assert [n for _, n in pacing.round_robin(queued, key=lambda x: x[0], limit=3)] == [1, 2, 3]


def test_round_robin_returns_everything_when_under_the_limit():
    queued = [("a", 1), ("b", 2)]
    assert len(pacing.round_robin(queued, key=lambda x: x[0], limit=99)) == 2


def test_round_robin_on_a_single_tenant_is_just_the_first_n():
    queued = [("a", i) for i in range(10)]
    assert [n for _, n in pacing.round_robin(queued, key=lambda x: x[0], limit=3)] == [0, 1, 2]


# =============================================================== Retry-After
def test_a_rate_limited_channel_raises_its_own_exception():
    """It must be distinguishable from a network error, or it lands on the generic backoff and burns
    an attempt off a budget that has nothing to do with the problem."""
    exc = ChannelRateLimited(retry_after=45.0)
    assert exc.retry_after == 45.0


def test_being_throttled_does_not_consume_the_retry_budget():
    """The core distinction. A 429 is the platform saying "slower", not "this delivery is broken".
    Counting it toward `notification_max_attempts` would dead-letter a perfectly good alert."""
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="teams",
                             status=DeliveryStatus.pending.value, attempts=3)
    dispatcher._record_failure(d, ChannelRateLimited(retry_after=30.0))
    assert d.attempts == 3, "a throttled send is not a failed attempt"
    assert d.status == DeliveryStatus.pending.value, "and it is not a failure state"


def test_retry_after_is_honoured_instead_of_the_generic_ladder():
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="teams",
                             status=DeliveryStatus.pending.value, attempts=0)
    dispatcher._record_failure(d, ChannelRateLimited(retry_after=120.0))
    delay = (d.next_attempt_at - datetime.now(timezone.utc)).total_seconds()
    assert 110 < delay < 130, f"expected ~120s from Retry-After, got {delay}"


def test_a_429_without_a_retry_after_header_still_backs_off():
    """Not every throttling response carries the header; retrying immediately would make it worse."""
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="teams",
                             status=DeliveryStatus.pending.value, attempts=0)
    dispatcher._record_failure(d, ChannelRateLimited(retry_after=None))
    assert d.next_attempt_at is not None and d.next_attempt_at > datetime.now(timezone.utc)


def test_a_real_failure_still_counts_and_still_dead_letters():
    """No regression: an actual delivery defect must still consume the budget and eventually stop."""
    d = NotificationDelivery(id=uuid.uuid4(), event_id=uuid.uuid4(), channel_type="teams",
                             status=DeliveryStatus.pending.value,
                             attempts=settings.notification_max_attempts - 1)
    dispatcher._record_failure(d, RuntimeError("channel exploded"))
    assert d.attempts == settings.notification_max_attempts
    assert d.status == DeliveryStatus.dead.value


def test_the_teams_adapter_turns_a_429_into_the_rate_limited_exception():
    """Parsed from the real response so the platform's own number is used rather than a guess."""
    import httpx
    from app.services.notifications.channels.teams import TeamsChannel
    resp = httpx.Response(429, headers={"Retry-After": "17"},
                          request=httpx.Request("POST", "http://x"))
    exc = TeamsChannel()._as_error(resp)
    assert isinstance(exc, ChannelRateLimited)
    assert exc.retry_after == 17.0


def test_a_non_429_error_is_not_treated_as_throttling():
    import httpx
    from app.services.notifications.channels.teams import TeamsChannel
    resp = httpx.Response(500, request=httpx.Request("POST", "http://x"))
    assert not isinstance(TeamsChannel()._as_error(resp), ChannelRateLimited)


# =============================================================== the drain, end to end
async def _cleanup():
    from app.config.database import async_session
    async with async_session() as s:
        for cc in (CC_A, CC_B):
            await s.execute(delete(NotificationDelivery).where(
                NotificationDelivery.event_id.in_(
                    select(NotificationEvent.id).where(NotificationEvent.customer_code == cc))))
            await s.execute(delete(NotificationEvent).where(NotificationEvent.customer_code == cc))
            await s.execute(delete(CustomerNotificationChannel).where(
                CustomerNotificationChannel.customer_code == cc))
        await s.commit()


async def _channel(cc, *, config=None):
    from app.config.database import async_session
    async with async_session() as s:
        ch = CustomerNotificationChannel(
            customer_code=cc, channel_type="teams", name=f"c-{uuid.uuid4().hex[:5]}",
            config={"webhook_url": "http://x", **(config or {})}, enabled=True)
        s.add(ch)
        await s.commit()
        return ch.id


async def _publish(cc, n=1):
    for _ in range(n):
        await dispatcher.enqueue(Event(event_type="t", customer_code=cc, severity="error",
                                       title="t", summary="s",
                                       dedup_key=f"c31:{uuid.uuid4()}", payload={"facts": {}}))


async def _rows(cc):
    from app.config.database import async_session
    async with async_session() as s:
        return list((await s.execute(
            select(NotificationDelivery).join(
                NotificationEvent, NotificationEvent.id == NotificationDelivery.event_id)
            .where(NotificationEvent.customer_code == cc))).scalars().all())


@pytest.fixture
def captured(monkeypatch):
    sent = []

    async def fake_send(self, event, config):
        sent.append(event.customer_code)

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", fake_send)
    return sent


async def test_the_drain_stops_at_the_channel_budget(captured):
    """The flood fix, measured. Everything beyond the budget stays queued rather than being sent."""
    await _cleanup()
    try:
        await _channel(CC_A, config={"max_per_minute": 3})
        await _publish(CC_A, 10)
        await dispatcher.deliver_due()
        assert len(captured) == 3, f"budget is 3/min, sent {len(captured)}"
    finally:
        await _cleanup()


async def test_a_deferred_delivery_is_not_a_failure(captured):
    """The distinction the whole design rests on: held back for budget must not look like broken."""
    await _cleanup()
    try:
        await _channel(CC_A, config={"max_per_minute": 1})
        await _publish(CC_A, 4)
        await dispatcher.deliver_due()
        held = [r for r in await _rows(CC_A) if r.status == DeliveryStatus.pending.value]
        assert len(held) == 3
        assert all(r.attempts == 0 for r in held), "a deferral must not consume an attempt"
        assert all(r.last_error is None for r in held), "a deferral is not an error"
        assert all(r.next_attempt_at is not None for r in held), "and it must be rescheduled"
    finally:
        await _cleanup()


async def test_nothing_is_lost_when_the_budget_frees_up(captured):
    """Deferral delays, it must never drop."""
    await _cleanup()
    try:
        await _channel(CC_A, config={"max_per_minute": 2})
        await _publish(CC_A, 5)
        await dispatcher.deliver_due()
        assert len(captured) == 2
        # simulate the window elapsing: clear the sends and make the held rows due again
        from app.config.database import async_session
        async with async_session() as s:
            await s.execute(
                NotificationDelivery.__table__.update()
                .where(NotificationDelivery.status == DeliveryStatus.pending.value)
                .values(next_attempt_at=None))
            await s.execute(
                NotificationDelivery.__table__.update()
                .where(NotificationDelivery.status == DeliveryStatus.delivered.value)
                .values(delivered_at=datetime.now(timezone.utc) - timedelta(hours=1)))
            await s.commit()
        await dispatcher.deliver_due()
        assert len(captured) == 4, "the held deliveries must go out once the window rolls"
    finally:
        await _cleanup()


async def test_one_tenants_flood_does_not_starve_another(captured):
    """Before round-robin, tenant A's 20 pending rows filled the batch and B waited. B's single alert
    is the one a person is probably watching for."""
    await _cleanup()
    try:
        await _channel(CC_A)
        await _channel(CC_B)
        await _publish(CC_A, 20)
        await _publish(CC_B, 1)
        await dispatcher.deliver_due(batch=5)
        assert CC_B in captured, "the quiet tenant must not wait behind the flood"
    finally:
        await _cleanup()


async def test_budgets_are_independent_per_channel(captured):
    """One tenant hitting its ceiling must not hold up another's channel."""
    await _cleanup()
    try:
        await _channel(CC_A, config={"max_per_minute": 1})
        await _channel(CC_B, config={"max_per_minute": 5})
        await _publish(CC_A, 5)
        await _publish(CC_B, 5)
        await dispatcher.deliver_due()
        assert captured.count(CC_A) == 1
        assert captured.count(CC_B) == 5
    finally:
        await _cleanup()
