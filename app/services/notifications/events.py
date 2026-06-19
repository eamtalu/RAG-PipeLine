"""NotificationEvent — the normalized, in-memory event a rule publishes onto the bus.

This is the transport-agnostic "something happened" message. It is distinct from the DB outbox row
(`app.persistence.models.notification.NotificationEvent`): the dispatcher persists this into that
row at publish time. Keeping it a plain pydantic model means rules and channels never depend on the
ORM or on each other.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class NotificationEvent(BaseModel):
    # what kind of thing happened, e.g. "transaction_error", "error_digest" (used as event_type).
    event_type: str
    customer_code: str
    severity: str = "error"  # "info" | "warning" | "error"

    title: str
    summary: str | None = None

    # stable idempotency key — the same condition yields the same key so it publishes exactly once
    # (e.g. "txn-error:{txn_id}" for streaming, "digest:{customer}:{rule}:{window}" for digests).
    dedup_key: str

    # structured context the channel adapter renders (e.g. {"facts": {...}, "url": "..."}).
    payload: dict = Field(default_factory=dict)

    # optional fan-out narrowing: channel ids (as strings) this event should go to. None/empty ⇒ all
    # of the customer's enabled channels.
    target_channel_ids: list[str] | None = None

    # provenance — the rule that produced this event (for audit on the outbox row).
    rule_id: str | None = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
