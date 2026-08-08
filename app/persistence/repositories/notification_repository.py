"""Repository for the notifications subsystem (channels, rules, outbox events, deliveries).

CRUD + queries used by the management API, the rule engine, and the dispatcher. Methods that mutate
commit themselves (matching the codebase's repo style). The dispatcher's claim-and-retry flow needs
finer transaction control, so its row-locking query (`claim_due_deliveries`) is the one method that
operates inside the caller's open transaction and does NOT commit.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi import Depends
from sqlalchemy import select, update, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.services.notifications import tenant_gate
from app.persistence.models.notification import (
    CustomerNotificationChannel,
    NotificationRule,
    NotificationEvent,
    NotificationDelivery,
    RuleStatus,
    DeliveryStatus,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class NotificationRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    # ----- channels -----------------------------------------------------------------------------
    async def create_channel(self, *, customer_code: str, channel_type: str, name: str,
                             config: dict, enabled: bool = True) -> CustomerNotificationChannel:
        row = CustomerNotificationChannel(
            customer_code=customer_code, channel_type=channel_type, name=name,
            config=config or {}, enabled=enabled,
        )
        self.db.add(row)
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def get_channel(self, channel_id: uuid.UUID) -> CustomerNotificationChannel | None:
        return await self.db.get(CustomerNotificationChannel, channel_id)

    async def list_channels(self, *, customer_code: str | None = None,
                            enabled_only: bool = False) -> list[CustomerNotificationChannel]:
        stmt = select(CustomerNotificationChannel)
        if customer_code:
            stmt = stmt.where(CustomerNotificationChannel.customer_code == customer_code)
        if enabled_only:
            stmt = stmt.where(CustomerNotificationChannel.enabled.is_(True))
        stmt = stmt.order_by(CustomerNotificationChannel.customer_code.asc(),
                             CustomerNotificationChannel.channel_type.asc(),
                             CustomerNotificationChannel.name.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def update_channel(self, channel_id: uuid.UUID, *,
                             name: str | None = None, config: dict | None = None,
                             enabled: bool | None = None) -> CustomerNotificationChannel | None:
        row = await self.get_channel(channel_id)
        if row is None:
            return None
        if name is not None:
            row.name = name
        if config is not None:
            row.config = config
        if enabled is not None:
            row.enabled = enabled
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def delete_channel(self, channel_id: uuid.UUID) -> bool:
        row = await self.get_channel(channel_id)
        if row is None:
            return False
        await self.db.delete(row)
        await self.db.commit()
        return True

    # ----- rules --------------------------------------------------------------------------------
    async def create_rule(self, *, customer_code: str, name: str, rule_type: str, match: dict,
                          severity: str = "error", description: str | None = None,
                          target_channel_ids: list[str] | None = None,
                          status: str = RuleStatus.draft.value) -> NotificationRule:
        row = NotificationRule(
            customer_code=customer_code, name=name, rule_type=rule_type, match=match or {},
            severity=severity, description=description, target_channel_ids=target_channel_ids,
            status=status,
        )
        self.db.add(row)
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def get_rule(self, rule_id: uuid.UUID) -> NotificationRule | None:
        return await self.db.get(NotificationRule, rule_id)

    async def list_rules(self, *, customer_code: str | None = None,
                         status: str | None = None) -> list[NotificationRule]:
        stmt = select(NotificationRule)
        if customer_code:
            stmt = stmt.where(NotificationRule.customer_code == customer_code)
        if status:
            stmt = stmt.where(NotificationRule.status == status)
        stmt = stmt.order_by(NotificationRule.customer_code.asc(), NotificationRule.created_at.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def list_active_rules(self) -> list[NotificationRule]:
        """Every active rule belonging to a tenant that has notifications switched on.

        TWO switches, and both must be on: a rule's own `status`, and its tenant's
        `notifications_enabled`. They mean different things — one is "should this rule fire", the
        other is "is this tenant using notifications at all" — and conflating them would mean turning
        the subsystem off for a customer silently rewrote all their rules.

        This is the single choke point every rule path goes through, streaming and windowed alike, so
        the tenant gate is applied once here rather than repeated in each evaluator.
        """
        stmt = (select(NotificationRule)
                .where(NotificationRule.status == RuleStatus.active.value,
                       tenant_gate.enabled(NotificationRule.customer_code))
                .order_by(NotificationRule.customer_code.asc(), NotificationRule.created_at.asc()))
        return list((await self.db.execute(stmt)).scalars().all())

    async def update_rule(self, rule_id: uuid.UUID, *, name: str | None = None,
                          description: str | None = None, rule_type: str | None = None,
                          match: dict | None = None, severity: str | None = None,
                          target_channel_ids: list[str] | None = None,
                          unset_target_channels: bool = False) -> NotificationRule | None:
        row = await self.get_rule(rule_id)
        if row is None:
            return None
        if name is not None:
            row.name = name
        if description is not None:
            row.description = description
        if rule_type is not None:
            row.rule_type = rule_type
        if match is not None:
            row.match = match
        if severity is not None:
            row.severity = severity
        if unset_target_channels:
            row.target_channel_ids = None
        elif target_channel_ids is not None:
            row.target_channel_ids = target_channel_ids
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def set_rule_status(self, rule_id: uuid.UUID, status: str) -> NotificationRule | None:
        row = await self.get_rule(rule_id)
        if row is None:
            return None
        row.status = status
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def delete_rule(self, rule_id: uuid.UUID) -> bool:
        row = await self.get_rule(rule_id)
        if row is None:
            return False
        await self.db.delete(row)
        await self.db.commit()
        return True

    # ----- events (outbox) ----------------------------------------------------------------------
    async def get_event_by_dedup_key(self, dedup_key: str) -> NotificationEvent | None:
        return await self.db.scalar(
            select(NotificationEvent).where(NotificationEvent.dedup_key == dedup_key)
        )

    async def existing_dedup_keys(self, dedup_keys: list[str]) -> set[str]:
        if not dedup_keys:
            return set()
        rows = (await self.db.execute(
            select(NotificationEvent.dedup_key).where(NotificationEvent.dedup_key.in_(dedup_keys))
        )).scalars().all()
        return set(rows)

    async def get_event(self, event_id: uuid.UUID) -> NotificationEvent | None:
        return await self.db.get(NotificationEvent, event_id)

    # ----- deliveries ---------------------------------------------------------------------------
    async def list_deliveries(self, *, customer_code: str | None = None, status: str | None = None,
                              event_id: uuid.UUID | None = None,
                              limit: int = 200) -> list[NotificationDelivery]:
        stmt = select(NotificationDelivery)
        if event_id:
            stmt = stmt.where(NotificationDelivery.event_id == event_id)
        if status:
            stmt = stmt.where(NotificationDelivery.status == status)
        if customer_code:
            stmt = stmt.join(NotificationEvent,
                             NotificationEvent.id == NotificationDelivery.event_id).where(
                NotificationEvent.customer_code == customer_code)
        stmt = stmt.order_by(NotificationDelivery.created_at.desc()).limit(limit)
        return list((await self.db.execute(stmt)).scalars().all())

    async def get_deliveries_for_event(self, event_id: uuid.UUID) -> list[NotificationDelivery]:
        return await self.list_deliveries(event_id=event_id, limit=1000)

    async def delivery_counts(self, *, customer_code: str | None = None) -> dict[str, int]:
        stmt = select(NotificationDelivery.status, func.count()).group_by(NotificationDelivery.status)
        if customer_code:
            stmt = stmt.join(NotificationEvent,
                             NotificationEvent.id == NotificationDelivery.event_id).where(
                NotificationEvent.customer_code == customer_code)
        rows = (await self.db.execute(stmt)).all()
        return {status: count for status, count in rows}


def get_notification_repository(db: AsyncSession = Depends(get_session)) -> NotificationRepository:
    """FastAPI dependency — provides NotificationRepository with the request session injected."""
    return NotificationRepository(db)
