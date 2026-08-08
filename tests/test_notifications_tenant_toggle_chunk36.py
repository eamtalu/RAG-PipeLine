"""Chunk 36: move the notifications on/off switch out of `.env` and into the product, per customer.

Amin activated a rule in the GUI and nothing happened. Activating a RULE and enabling the SUBSYSTEM
were two different switches, and the second one was invisible from the frontend:
`notifications_enabled` was a Pydantic setting defaulting to False, read ONCE at process boot
(`app/background.py:151`) to decide whether the worker task was ever created. A GUI toggle writing to
Postgres could not have changed anything, because no task existed to notice.

So the flag moves to `customers.notifications_enabled` and the check moves from boot into the loop.
The toggle then takes effect within one poll interval instead of needing a worker restart.

**One predicate, three places.** `tenant_gate.enabled()` is used by rule loading, by the delivery
drain, and by the retention position. Three call sites, one definition, so "is this tenant switched
on?" cannot drift between them.

Why the drain is gated and not just evaluation: "turn notifications off" has to mean messages STOP.
Gating only rule evaluation would let an already-queued burst keep draining into Teams for as long as
pacing took - exactly the situation someone hits the switch to escape. Queued rows stay pending and
resume on re-enable; nothing is discarded.

**The trap this chunk exists to avoid.** `consumer_cursors.notifications_position` takes MIN(cursor_at)
over rules whose RULE status is active. A switched-off customer still has active rules - their cursors
simply freeze. Publishing that frozen minimum to `consumer_cursors` would pin partition retention for
the WHOLE INSTANCE, because retention gates drops on it. Switching off one tenant's notifications
would quietly stop the disk being reclaimed. A disabled tenant is not a slow reader; it is not a
reader, and the position query has to say so.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select

from app.config.database import async_session
from app.persistence.models.customer import Customer
from app.persistence.models.notification import (
    DeliveryStatus, NotificationDelivery, NotificationEvent as NotificationEventRow,
    NotificationRule, RuleStatus,
)
from app.persistence.repositories.notification_repository import NotificationRepository
from app.services import consumer_cursors
from app.services.notifications import dispatcher, tenant_gate

ON, OFF = "test_c36_on", "test_c36_off"
T0 = datetime(2026, 8, 8, 10, 0, 0, tzinfo=timezone.utc)


# =============================================================== fixtures
async def _cleanup() -> None:
    async with async_session() as s:
        await s.execute(delete(NotificationDelivery).where(
            NotificationDelivery.event_id.in_(
                select(NotificationEventRow.id).where(
                    NotificationEventRow.customer_code.in_([ON, OFF])))))
        await s.execute(delete(NotificationEventRow).where(
            NotificationEventRow.customer_code.in_([ON, OFF])))
        await s.execute(delete(NotificationRule).where(
            NotificationRule.customer_code.in_([ON, OFF])))
        await s.execute(delete(Customer).where(Customer.customer_code.in_([ON, OFF])))
        await s.execute(delete(consumer_cursors.ConsumerCursor).where(
            consumer_cursors.ConsumerCursor.consumer == consumer_cursors.NOTIFICATIONS))
        await s.commit()


@pytest.fixture
async def tenants():
    """Two tenants that differ ONLY in the new flag, so any behaviour difference is attributable."""
    await _cleanup()
    async with async_session() as s:
        s.add(Customer(customer_code=ON, name="on", active=True, notifications_enabled=True))
        s.add(Customer(customer_code=OFF, name="off", active=True, notifications_enabled=False))
        await s.commit()
    yield
    await _cleanup()


async def _add_rule(customer_code: str, *, cursor_at: datetime | None = None) -> uuid.UUID:
    async with async_session() as s:
        rule = NotificationRule(
            id=uuid.uuid4(), customer_code=customer_code, name=f"r-{customer_code}",
            rule_type="status_match", match={"statuses": ["error"]}, severity="error",
            status=RuleStatus.active.value, cursor_at=cursor_at)
        s.add(rule)
        await s.commit()
        return rule.id


async def _add_due_delivery(customer_code: str) -> uuid.UUID:
    """An event plus a delivery that is due right now, so the drain would claim it if allowed."""
    async with async_session() as s:
        event = NotificationEventRow(
            id=uuid.uuid4(), customer_code=customer_code, event_type="test",
            severity="error", title=f"t-{customer_code}", dedup_key=f"dk-{uuid.uuid4()}")
        s.add(event)
        await s.flush()
        delivery = NotificationDelivery(
            id=uuid.uuid4(), event_id=event.id, channel_type="teams",
            status=DeliveryStatus.pending.value, attempts=0, next_attempt_at=None)
        s.add(delivery)
        await s.commit()
        return delivery.id


# =============================================================== the shared predicate
async def test_the_gate_admits_an_enabled_tenant_and_refuses_a_disabled_one(tenants):
    """One definition of "switched on", so the three call sites below cannot drift apart."""
    async with async_session() as s:
        codes = set((await s.execute(
            select(Customer.customer_code)
            .where(Customer.customer_code.in_([ON, OFF]),
                   tenant_gate.enabled(Customer.customer_code)))).scalars().all())
    assert codes == {ON}


async def test_an_unknown_customer_code_is_not_admitted(tenants):
    """Rows can outlive their tenant row (a purge, a typo). Defaulting those to ON would resume
    sending for a tenant that no longer exists."""
    async with async_session() as s:
        rows = (await s.execute(
            select(NotificationEventRow.id).where(
                NotificationEventRow.customer_code == "test_c36_ghost",
                tenant_gate.enabled(NotificationEventRow.customer_code)))).all()
    assert rows == []


# =============================================================== 1. rule loading
async def test_a_disabled_tenants_rules_are_not_loaded_for_evaluation(tenants):
    """The primary switch. `list_active_rules` is the single choke point every rule path goes
    through, streaming and windowed alike."""
    await _add_rule(OFF)
    async with async_session() as s:
        rules = await NotificationRepository(s).list_active_rules()
    assert [r for r in rules if r.customer_code == OFF] == []


async def test_an_enabled_tenants_rules_still_load(tenants):
    """No regression: switching the mechanism on must not switch everyone off."""
    await _add_rule(ON)
    async with async_session() as s:
        rules = await NotificationRepository(s).list_active_rules()
    assert [r.customer_code for r in rules if r.customer_code == ON] == [ON]


async def test_disabling_one_tenant_leaves_another_alone(tenants):
    """The entire point of making this per-customer rather than global. A global flag could not
    express this, and the earlier design would have had one tenant silence another."""
    await _add_rule(ON)
    await _add_rule(OFF)
    async with async_session() as s:
        loaded = {r.customer_code for r in await NotificationRepository(s).list_active_rules()}
    assert ON in loaded and OFF not in loaded


async def test_an_inactive_rule_stays_excluded_even_for_an_enabled_tenant(tenants):
    """The two switches are independent and both must hold. Enabling the tenant must not resurrect
    rules its owner deliberately deactivated."""
    async with async_session() as s:
        s.add(NotificationRule(id=uuid.uuid4(), customer_code=ON, name="inactive",
                               rule_type="status_match", match={"statuses": ["error"]},
                               severity="error", status=RuleStatus.inactive.value))
        await s.commit()
        loaded = await NotificationRepository(s).list_active_rules()
    assert [r for r in loaded if r.name == "inactive"] == []


# =============================================================== 2. the delivery drain
async def test_a_disabled_tenants_queued_delivery_is_not_claimed(tenants):
    """Gating evaluation alone would let a queued burst keep arriving in Teams long after someone
    hit the switch to stop it - which is when they would be hitting it."""
    await _add_due_delivery(OFF)
    async with async_session() as s:
        claimed = await dispatcher._claim_due(s, datetime.now(timezone.utc))
    assert [cc for _d, cc in claimed if cc == OFF] == []


async def test_an_enabled_tenants_queued_delivery_is_still_claimed(tenants):
    await _add_due_delivery(ON)
    async with async_session() as s:
        claimed = await dispatcher._claim_due(s, datetime.now(timezone.utc))
    assert [cc for _d, cc in claimed if cc == ON] == [ON]


async def test_a_suppressed_delivery_is_not_discarded_and_resumes_on_re_enable(tenants):
    """Switching off must PAUSE, not destroy. The row stays pending, so nothing is lost and the
    decision is reversible."""
    delivery_id = await _add_due_delivery(OFF)
    async with async_session() as s:
        row = await s.get(NotificationDelivery, delivery_id)
        assert row.status == DeliveryStatus.pending.value, "still queued, not dead-lettered"

    async with async_session() as s:  # the operator changes their mind
        cust = (await s.execute(select(Customer)
                                .where(Customer.customer_code == OFF))).scalars().one()
        cust.notifications_enabled = True
        await s.commit()
    async with async_session() as s:
        claimed = await dispatcher._claim_due(s, datetime.now(timezone.utc))
    assert [cc for _d, cc in claimed if cc == OFF] == [OFF]


# =============================================================== 3. retention (the dangerous one)
async def test_a_disabled_tenants_frozen_cursor_does_not_pin_retention(tenants):
    """THE TRAP. A switched-off tenant keeps `status='active'` rules whose cursors simply stop
    advancing. Feeding that frozen minimum to `consumer_cursors` pins partition retention for the
    WHOLE INSTANCE - one tenant's notification preference silently stops the disk being reclaimed.

    A disabled tenant is not a slow reader. It is not a reader."""
    old, new = T0 - timedelta(days=90), T0
    await _add_rule(OFF, cursor_at=old)
    await _add_rule(ON, cursor_at=new)

    async with async_session() as s:
        position = await consumer_cursors.notifications_position(s)

    assert position == new, f"retention must not be held at the disabled tenant's {old}"


async def test_the_published_position_is_the_gated_one(tenants):
    """`report_notifications` is what retention actually reads, so the filter has to survive the
    trip through it - asserting only on the inner query would miss a regression here."""
    await _add_rule(OFF, cursor_at=T0 - timedelta(days=90))
    await _add_rule(ON, cursor_at=T0)
    async with async_session() as s:
        await consumer_cursors.report_notifications(s)
        await s.commit()
        row = await s.get(consumer_cursors.ConsumerCursor, consumer_cursors.NOTIFICATIONS)
    assert row is not None and row.position == T0


async def test_no_enabled_tenant_means_nothing_is_pinned(tenants):
    """With every tenant switched off there is no reader at all, so retention must be free rather
    than frozen at whatever the last position happened to be."""
    await _add_rule(OFF, cursor_at=T0 - timedelta(days=90))
    async with async_session() as s:
        assert await consumer_cursors.notifications_position(s) is None


# =============================================================== the column and the API
async def test_a_new_tenant_does_not_start_sending(db):
    """Default false. Deploying this must not begin alerting for anyone who never asked for it -
    especially since every existing tenant gets the column at once."""
    code = "test_c36_fresh"
    async with async_session() as s:
        await s.execute(delete(Customer).where(Customer.customer_code == code))
        s.add(Customer(customer_code=code, name="fresh", active=True))
        await s.commit()
        cust = (await s.execute(select(Customer)
                                .where(Customer.customer_code == code))).scalars().one()
        assert cust.notifications_enabled is False
        await s.execute(delete(Customer).where(Customer.customer_code == code))
        await s.commit()


async def test_the_patch_endpoint_round_trips_the_flag(tenants):
    """The whole point: reachable from the frontend, through the PATCH that already exists rather
    than a new surface."""
    from app.api.v1.customers import UpdateCustomerRequest, update_customer
    from app.persistence.repositories.customer_repository import CustomerRepository
    from app.persistence.repositories.logspace_presence_repository import (
        LogspacePresenceRepository,
    )
    async with async_session() as s:
        out = await update_customer(
            OFF, UpdateCustomerRequest(notifications_enabled=True),
            db=s, repo=CustomerRepository(s),
            presence_repo=LogspacePresenceRepository(s))
    assert out["notifications_enabled"] is True

    async with async_session() as s:
        cust = (await s.execute(select(Customer)
                                .where(Customer.customer_code == OFF))).scalars().one()
    assert cust.notifications_enabled is True, "persisted, not just echoed back"


async def test_the_flag_is_reported_so_the_ui_can_render_its_state(tenants):
    """The banner has to know. A rule listing as "active" while nothing runs is the exact confusion
    that produced this work."""
    from app.api.v1.customers import get_customer
    from app.persistence.repositories.customer_repository import CustomerRepository
    from app.persistence.repositories.logspace_presence_repository import (
        LogspacePresenceRepository,
    )
    async with async_session() as s:
        out = await get_customer(ON, repo=CustomerRepository(s),
                                 presence_repo=LogspacePresenceRepository(s))
    assert out["notifications_enabled"] is True


# =============================================================== the boot gate is gone
def test_the_worker_no_longer_depends_on_a_boot_time_setting():
    """The structural fix. While the flag gated `create_task`, no runtime toggle could ever work -
    there was no task running to observe it."""
    from app.settings import settings
    assert not hasattr(settings, "notifications_enabled"), \
        "the env flag is retired; the switch is per-customer and read every tick"


def test_the_background_starter_never_reads_a_global_notifications_flag():
    """Guards the other half: if starting the task were still conditional on anything global, a
    per-customer toggle would silently do nothing for everybody.

    Checked against the AST rather than the raw source, so a COMMENT explaining the retired flag does
    not fail the test. (It did, on the first run - which is the argument for parsing over grepping.)
    """
    import ast
    import inspect
    from app import background
    reads = [n for n in ast.walk(ast.parse(inspect.getsource(background)))
             if isinstance(n, ast.Attribute) and n.attr == "notifications_enabled"]
    assert reads == [], "the worker must start unconditionally; the gate is per tenant, per tick"
