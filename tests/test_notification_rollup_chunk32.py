"""Chunk 32 (step 6 of docs/plan/2026-08-08_notification-architecture.html): collapse a flood into a
summary.

Step 5 protects the *webhook*: a burst is paced so Teams is never hammered. It does nothing for the
*person reading the channel*. 500 alerts delivered politely over twenty minutes is still 500 cards,
and the channel becomes unusable exactly when someone needs it.

So past a per-rule burst cap, further matches stop being delivered individually and are represented by
ONE summary card per window:

    ⚠ 473 more errors in the last 5 min
      Top: PurchaseOrderLine (310), StockTransaction (98)

Three things this must not do, and each has tests below:

**It must not lose the audit trail.** Every match still gets its `notification_events` row and its
`notification_deliveries` row — the row is marked `suppressed`, not skipped. "Which transactions were
in that rollup?" has to stay answerable, and the cursor has already moved past them so nothing else
records it.

**It must not summarise twice.** The rollup is idempotent by dedup key and only ever covers a
COMPLETED window, mirroring the existing digest rules so the two behave alike.

**It must not silently swallow a small burst.** Under the cap, behaviour is exactly as before —
individual cards, no summary.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.persistence.models.notification import (
    CustomerNotificationChannel, DeliveryStatus, NotificationDelivery, NotificationEvent,
    NotificationRule, RuleStatus,
)
from app.services.notifications import dispatcher, rollup
from app.services.notifications.events import NotificationEvent as Event
from app.settings import settings

CC = "test_chunk32"


# =============================================================== the cap (pure)
def test_a_rule_uses_the_global_burst_cap_by_default():
    assert rollup.burst_cap({}) == settings.notification_rule_burst_cap
    assert rollup.burst_cap(None) == settings.notification_rule_burst_cap


def test_a_rule_can_set_its_own_cap():
    """A tenant watching a rare condition may want every card; one watching a noisy one may want two."""
    assert rollup.burst_cap({"burst_cap": 2}) == 2


def test_a_nonsense_cap_falls_back_rather_than_breaking_delivery():
    """`match` is operator-edited JSONB — a typo must not stop that rule alerting entirely."""
    for bad in ({"burst_cap": "lots"}, {"burst_cap": None}, {"burst_cap": -1}):
        assert rollup.burst_cap(bad) == settings.notification_rule_burst_cap


def test_a_cap_of_zero_is_honoured_not_treated_as_unset():
    """Zero is a legitimate choice: summarise everything, never send an individual card."""
    assert rollup.burst_cap({"burst_cap": 0}) == 0


def test_window_index_buckets_time_the_same_way_digest_rules_do():
    """Same arithmetic as ErrorDigestEvaluator, so the two features behave alike and an operator only
    has to learn one notion of 'window'."""
    at = datetime(2026, 8, 8, 12, 3, 20, tzinfo=timezone.utc)
    idx = rollup.window_index(at, window_seconds=300)
    assert idx == int(at.timestamp()) // 300
    assert rollup.window_index(at + timedelta(seconds=30), window_seconds=300) == idx
    assert rollup.window_index(at + timedelta(seconds=300), window_seconds=300) == idx + 1


def test_the_rollup_settings_are_sane():
    assert settings.notification_rule_burst_cap >= 0
    assert settings.notification_rollup_window_seconds >= 60


# =============================================================== the summary card (pure)
def test_the_summary_reports_the_true_count():
    """The number is the whole point — it is what tells someone the scale of what was collapsed."""
    ev = rollup.build_summary(customer_code=CC, rule_id=uuid.uuid4(), rule_name="WMS errors",
                              titles=["[mnp] error: A"] * 310 + ["[mnp] error: B"] * 98,
                              window_index=1, severity="error", target_channel_ids=None)
    assert "408" in ev.title or "408" in (ev.summary or "")


def test_the_summary_names_the_top_offenders():
    """A bare count says something is wrong but not what. The top few make it actionable without
    reading the 473 cards it replaced."""
    ev = rollup.build_summary(customer_code=CC, rule_id=uuid.uuid4(), rule_name="WMS errors",
                              titles=["[mnp] error: PurchaseOrderLine"] * 310
                                     + ["[mnp] error: StockTransaction"] * 98,
                              window_index=1, severity="error", target_channel_ids=None)
    text = f"{ev.title} {ev.summary} {ev.payload}"
    assert "PurchaseOrderLine" in text and "310" in text


def test_the_summary_is_idempotent_by_window():
    """Same rule, same window -> same dedup key, so a second pass publishes nothing."""
    rid = uuid.uuid4()
    a = rollup.build_summary(customer_code=CC, rule_id=rid, rule_name="r", titles=["x"],
                             window_index=7, severity="error", target_channel_ids=None)
    b = rollup.build_summary(customer_code=CC, rule_id=rid, rule_name="r", titles=["x", "y"],
                             window_index=7, severity="error", target_channel_ids=None)
    assert a.dedup_key == b.dedup_key
    assert "7" in a.dedup_key


def test_the_summary_inherits_the_rules_channel_targeting():
    """It must land where the individual cards would have; sending it somewhere else would be worse
    than not sending it."""
    targets = [str(uuid.uuid4())]
    ev = rollup.build_summary(customer_code=CC, rule_id=uuid.uuid4(), rule_name="r", titles=["x"],
                              window_index=1, severity="error", target_channel_ids=targets)
    assert ev.target_channel_ids == targets


# =============================================================== suppression (DB)
async def _cleanup():
    from app.config.database import async_session
    async with async_session() as s:
        await s.execute(delete(NotificationDelivery).where(
            NotificationDelivery.event_id.in_(
                select(NotificationEvent.id).where(NotificationEvent.customer_code == CC))))
        await s.execute(delete(NotificationEvent).where(NotificationEvent.customer_code == CC))
        await s.execute(delete(NotificationRule).where(NotificationRule.customer_code == CC))
        await s.execute(delete(CustomerNotificationChannel).where(
            CustomerNotificationChannel.customer_code == CC))
        await s.commit()


async def _channel():
    from app.config.database import async_session
    async with async_session() as s:
        ch = CustomerNotificationChannel(customer_code=CC, channel_type="teams", name="c",
                                         config={"webhook_url": "http://x"}, enabled=True)
        s.add(ch)
        await s.commit()
        return ch.id


async def _rule(*, cap=None, name="r"):
    from app.config.database import async_session
    async with async_session() as s:
        match = {"statuses": ["error"]}
        if cap is not None:
            match["burst_cap"] = cap
        r = NotificationRule(customer_code=CC, name=name, rule_type="status_match", match=match,
                             severity="error", status=RuleStatus.active.value)
        s.add(r)
        await s.commit()
        return r.id


async def _publish(rule_id, n, *, title="[x] error: Thing"):
    for i in range(n):
        await dispatcher.enqueue(Event(
            event_type="transaction_match", customer_code=CC, severity="error",
            title=title, summary="s", dedup_key=f"c32:{uuid.uuid4()}",
            payload={"facts": {}}, rule_id=str(rule_id) if rule_id else None))


async def _statuses():
    from collections import Counter
    from app.config.database import async_session
    async with async_session() as s:
        rows = (await s.execute(
            select(NotificationDelivery.status).join(
                NotificationEvent, NotificationEvent.id == NotificationDelivery.event_id)
            .where(NotificationEvent.customer_code == CC))).scalars().all()
    return Counter(rows)


@pytest.fixture
async def wired():
    """A clean tenant with one channel, and the dispatcher SUBSCRIBED.

    `bus.publish` fans out to subscribers, and the only thing that registers one is
    `background.py:151` at startup. Without it the rollup publishes into a void and reports success —
    which is exactly how the first run of these tests failed.
    """
    from app.services.notifications import dispatcher as nd
    from app.services.notifications.bus import bus
    await _cleanup()
    await _channel()
    bus.clear()
    nd.register()
    yield
    await _cleanup()


async def test_under_the_cap_everything_is_delivered_individually(wired):
    """No regression for the ordinary case — a handful of alerts must still arrive as cards."""
    rid = await _rule(cap=5)
    await _publish(rid, 3)
    assert _counts_of(await _statuses()) == (3, 0)


def _counts_of(counter):
    return (counter.get(DeliveryStatus.pending.value, 0),
            counter.get(DeliveryStatus.suppressed.value, 0))


async def test_past_the_cap_the_overflow_is_suppressed(wired):
    """The flood fix for the reader: the first few land as cards, the rest do not."""
    rid = await _rule(cap=3)
    await _publish(rid, 10)
    pending, suppressed = _counts_of(await _statuses())
    assert pending == 3
    assert suppressed == 7


async def test_a_suppressed_delivery_still_has_its_event_row(wired):
    """The audit trail. The cursor has already moved past these transactions, so if the event row
    were skipped there would be no record anywhere of what the rollup covered."""
    rid = await _rule(cap=2)
    await _publish(rid, 6)
    from app.config.database import async_session
    from sqlalchemy import func
    async with async_session() as s:
        n = await s.scalar(select(func.count()).select_from(NotificationEvent)
                           .where(NotificationEvent.customer_code == CC))
    assert n == 6, "every match must still be recorded, delivered or not"


async def test_a_suppressed_delivery_is_never_claimed_by_the_drain(wired, monkeypatch):
    """Suppressed means "represented by the summary instead", not "queued". If the drain picked them
    up the cap would achieve nothing."""
    sent = []

    async def fake_send(self, event, config):
        sent.append(event.dedup_key)

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", fake_send)

    rid = await _rule(cap=2)
    await _publish(rid, 8)
    await dispatcher.deliver_due()
    assert len(sent) == 2, f"only the un-suppressed cards may send, sent {len(sent)}"


async def test_the_cap_is_per_rule_not_shared(wired):
    """Two rules on one tenant must not eat each other's allowance — a noisy rule would silence a
    quiet one that happens to share a customer."""
    a = await _rule(cap=2, name="a")
    b = await _rule(cap=2, name="b")
    await _publish(a, 5)
    await _publish(b, 5)
    pending, suppressed = _counts_of(await _statuses())
    assert pending == 4, "each rule keeps its own cap of 2"
    assert suppressed == 6


async def test_an_event_with_no_rule_is_never_suppressed(wired):
    """Manual publishes and channel tests carry no rule_id. They are human-initiated, cannot flood,
    and silently collapsing them would be baffling."""
    await _publish(None, 10)
    pending, suppressed = _counts_of(await _statuses())
    assert suppressed == 0
    assert pending == 10


# =============================================================== the summary, end to end
async def test_a_completed_window_with_suppressions_emits_one_summary(wired):
    rid = await _rule(cap=2)
    await _publish(rid, 9)
    made = await rollup.run_once(now=datetime.now(timezone.utc)
                                 + timedelta(seconds=settings.notification_rollup_window_seconds))
    assert made == 1, "exactly one summary per (rule, window)"
    from app.config.database import async_session
    async with async_session() as s:
        ev = (await s.execute(select(NotificationEvent).where(
            NotificationEvent.customer_code == CC,
            NotificationEvent.event_type == "rollup"))).scalars().all()
    assert len(ev) == 1
    assert "7" in ev[0].title or "7" in (ev[0].summary or ""), "must report the 7 suppressed"


async def test_running_the_rollup_twice_emits_nothing_new(wired):
    """Idempotent by dedup key, the same guarantee the digest rules give."""
    rid = await _rule(cap=2)
    await _publish(rid, 9)
    later = datetime.now(timezone.utc) + timedelta(
        seconds=settings.notification_rollup_window_seconds)
    assert await rollup.run_once(now=later) == 1
    assert await rollup.run_once(now=later) == 0


async def test_a_window_with_no_suppressions_emits_nothing(wired):
    """Silence when there is nothing to say — a summary card saying "0 more" is noise."""
    rid = await _rule(cap=10)
    await _publish(rid, 3)
    later = datetime.now(timezone.utc) + timedelta(
        seconds=settings.notification_rollup_window_seconds)
    assert await rollup.run_once(now=later) == 0


async def test_the_current_window_is_not_summarised_yet(wired):
    """Only COMPLETED windows, or the summary would fire early and undercount — and its dedup key
    would then block the real one."""
    rid = await _rule(cap=2)
    await _publish(rid, 9)
    assert await rollup.run_once(now=datetime.now(timezone.utc)) == 0


async def test_the_summary_is_delivered_like_any_other_alert(wired, monkeypatch):
    """It goes through the same outbox, pacing and retry path — it is an alert, not a special case."""
    sent = []

    async def fake_send(self, event, config):
        sent.append(event.title)

    from app.services.notifications.channels.teams import TeamsChannel
    monkeypatch.setattr(TeamsChannel, "send", fake_send)

    rid = await _rule(cap=2)
    await _publish(rid, 9)
    await rollup.run_once(now=datetime.now(timezone.utc)
                          + timedelta(seconds=settings.notification_rollup_window_seconds))
    await dispatcher.deliver_due()
    assert any("more" in t for t in sent), f"the summary card must actually be sent: {sent}"


async def test_the_summary_is_never_suppressed_by_the_cap_it_reports(wired):
    """The self-referential trap, found the hard way.

    A summary carries its rule's id for provenance — so the burst cap counted it as one more alert
    from that rule and suppressed it. The flood would then have vanished in total silence, which is
    strictly worse than sending all 500 cards. Exempted by event TYPE, so provenance survives.
    """
    rid = await _rule(cap=1)
    await _publish(rid, 8)
    await rollup.run_once(now=datetime.now(timezone.utc)
                          + timedelta(seconds=settings.notification_rollup_window_seconds))
    from app.config.database import async_session
    async with async_session() as s:
        status = await s.scalar(
            select(NotificationDelivery.status)
            .join(NotificationEvent, NotificationEvent.id == NotificationDelivery.event_id)
            .where(NotificationEvent.customer_code == CC,
                   NotificationEvent.event_type == rollup.EVENT_TYPE))
    assert status == DeliveryStatus.pending.value, "the summary must be deliverable, not suppressed"


def test_the_worker_runs_the_rollup():
    """It has to be on the tick, or suppressed alerts are collapsed and then never summarised — which
    would be strictly worse than not suppressing at all."""
    import inspect
    from app.services.workers import notification_worker
    assert "rollup" in inspect.getsource(notification_worker)
