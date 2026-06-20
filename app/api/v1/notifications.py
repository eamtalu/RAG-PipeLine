"""Notifications management + observability API.

The frontend uses these to configure the whole alerting workflow per customer:
  - Channels  : WHERE alerts go (Teams/Slack/WhatsApp destinations). A customer may have many.
  - Rules     : WHEN to alert (data-driven). Lifecycle: draft → (publish) → active → (deactivate)
                → inactive. Only ACTIVE rules fire. A rule may target specific channels.
  - Deliveries: read-only visibility into what was sent / pending / failed, per channel.

Tenant scoping is explicit in the path (this is an admin/config surface that manages many customers),
validated against the customer registry — unlike the log read endpoints which use X-Customer-Code.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.deps import normalize_customer_code
from app.persistence.models.notification import (
    CustomerNotificationChannel, NotificationRule, NotificationEvent, NotificationDelivery,
    RuleStatus, RuleType,
)
from app.persistence.repositories.customer_repository import (
    CustomerRepository, get_customer_repository,
)
from app.persistence.repositories.notification_repository import (
    NotificationRepository, get_notification_repository,
)
from app.services.notifications.channels import (
    KNOWN_CHANNEL_TYPES, IMPLEMENTED_CHANNEL_TYPES, get_channel,
)
from app.services.notifications.events import NotificationEvent as Event
from app.services.notifications import dispatcher

router = APIRouter(prefix="/notifications", tags=["notifications"])

_VALID_RULE_TYPES = {t.value for t in RuleType}
_VALID_SEVERITIES = {"success", "info", "warning", "error"}


# ===================================================================================================
# request/response models
# ===================================================================================================
class CreateChannelRequest(BaseModel):
    channel_type: str = Field(..., description="teams | slack | whatsapp")
    name: str = Field(default="default", max_length=128, description="Label distinguishing this "
                      "destination from the customer's others, e.g. 'ops' vs 'oncall'.")
    config: dict = Field(default_factory=dict, description="Transport config, e.g. {'webhook_url': ...}")
    enabled: bool = Field(default=True)


class UpdateChannelRequest(BaseModel):
    name: str | None = None
    config: dict | None = None
    enabled: bool | None = None


class ManualPublishRequest(BaseModel):
    """An ad-hoc, user-initiated alert (no rule) — e.g. an analyst flagging something to the team."""
    title: str = Field(..., max_length=512, description="Headline shown on the alert card.")
    message: str | None = Field(default=None, description="Longer description / context.")
    severity: str = Field(default="info", description="success | info | warning | error "
                          "(sets the card color and the shown Status).")
    target_channel_ids: list[str] | None = Field(
        default=None, description="Specific channel ids to post to; null/empty ⇒ all enabled channels.")
    posted_by: str | None = Field(default=None, description="Who is sending this (shown on the card).")
    transaction_id: str | None = Field(default=None, description="Optional log transaction this refers "
                                       "to — adds a deep link + fact to the card.")
    facts: dict | None = Field(default=None, description="Extra key/value rows to show on the card.")


class CreateRuleRequest(BaseModel):
    name: str = Field(..., max_length=128)
    rule_type: str = Field(..., description="status_match | text_match | digest")
    match: dict = Field(default_factory=dict, description="Evaluator params (see rule_type).")
    severity: str = Field(default="error", description="info | warning | error (card styling).")
    description: str | None = None
    target_channel_ids: list[str] | None = Field(
        default=None, description="Restrict this rule to specific channel ids; null/empty ⇒ all "
                                  "enabled channels of the customer.")


class UpdateRuleRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    rule_type: str | None = None
    match: dict | None = None
    severity: str | None = None
    target_channel_ids: list[str] | None = None
    clear_target_channels: bool = Field(default=False, description="Set true to reset targeting to "
                                        "ALL enabled channels (ignores target_channel_ids).")


# ===================================================================================================
# serializers
# ===================================================================================================
def _ser_channel(c: CustomerNotificationChannel) -> dict:
    return {
        "id": str(c.id), "customer_code": c.customer_code, "channel_type": c.channel_type,
        "name": c.name, "config": c.config, "enabled": c.enabled,
        "implemented": c.channel_type in IMPLEMENTED_CHANNEL_TYPES,
        "created_at": c.created_at.isoformat() if c.created_at else None,
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    }


def _ser_rule(r: NotificationRule) -> dict:
    return {
        "id": str(r.id), "customer_code": r.customer_code, "name": r.name,
        "description": r.description, "rule_type": r.rule_type, "match": r.match,
        "severity": r.severity, "target_channel_ids": r.target_channel_ids, "status": r.status,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
    }


def _ser_delivery(d: NotificationDelivery) -> dict:
    return {
        "id": str(d.id), "event_id": str(d.event_id),
        "channel_id": str(d.channel_id) if d.channel_id else None, "channel_type": d.channel_type,
        "status": d.status, "attempts": d.attempts,
        "next_attempt_at": d.next_attempt_at.isoformat() if d.next_attempt_at else None,
        "last_error": d.last_error,
        "delivered_at": d.delivered_at.isoformat() if d.delivered_at else None,
        "created_at": d.created_at.isoformat() if d.created_at else None,
    }


def _ser_event(e: NotificationEvent) -> dict:
    return {
        "id": str(e.id), "dedup_key": e.dedup_key, "customer_code": e.customer_code,
        "rule_id": str(e.rule_id) if e.rule_id else None, "event_type": e.event_type,
        "severity": e.severity, "title": e.title, "summary": e.summary, "payload": e.payload,
        "target_channel_ids": e.target_channel_ids,
        "created_at": e.created_at.isoformat() if e.created_at else None,
    }


# ===================================================================================================
# helpers
# ===================================================================================================
async def _require_customer(customer_code: str, customers: CustomerRepository) -> str:
    code = normalize_customer_code(customer_code)
    if code is None:
        raise HTTPException(400, detail="Invalid customer_code (expected a slug like 'acme').")
    if not await customers.exists(code):
        raise HTTPException(404, detail=f"Unknown customer: {code!r}. Create its log space first "
                                        f"(POST /api/v1/customers).")
    return code


def _parse_uuid(value: str, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError):
        raise HTTPException(400, detail=f"Invalid {what} id: {value!r}")


def _validate_rule(rule_type: str, match: dict) -> None:
    if rule_type not in _VALID_RULE_TYPES:
        raise HTTPException(400, detail=f"Invalid rule_type {rule_type!r}. "
                                        f"Expected one of {sorted(_VALID_RULE_TYPES)}.")
    if rule_type == RuleType.text_match.value:
        if not (match or {}).get("pattern"):
            raise HTTPException(400, detail="text_match rule requires match.pattern")
        if match.get("is_regex"):
            try:
                re.compile(match["pattern"])
            except re.error as exc:
                raise HTTPException(400, detail=f"Invalid regex in match.pattern: {exc}")


# ===================================================================================================
# meta
# ===================================================================================================
@router.get("/channel-types")
async def list_channel_types():
    """Channel types the backend knows about, and which are deliverable today (for the UI picker)."""
    return {
        "channel_types": sorted(KNOWN_CHANNEL_TYPES),
        "implemented": sorted(IMPLEMENTED_CHANNEL_TYPES),
        "rule_types": sorted(_VALID_RULE_TYPES),
        "rule_statuses": [s.value for s in RuleStatus],
    }


# ===================================================================================================
# channels — single-resource ops (literal prefix; declared before the customer-scoped routes)
# ===================================================================================================
@router.get("/channels/{channel_id}")
async def get_channel_endpoint(
    channel_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    row = await repo.get_channel(_parse_uuid(channel_id, "channel"))
    if row is None:
        raise HTTPException(404, detail=f"Unknown channel: {channel_id!r}")
    return _ser_channel(row)


@router.patch("/channels/{channel_id}")
async def update_channel_endpoint(
    channel_id: str, body: UpdateChannelRequest,
    repo: NotificationRepository = Depends(get_notification_repository),
):
    row = await repo.update_channel(_parse_uuid(channel_id, "channel"),
                                    name=body.name, config=body.config, enabled=body.enabled)
    if row is None:
        raise HTTPException(404, detail=f"Unknown channel: {channel_id!r}")
    return _ser_channel(row)


@router.delete("/channels/{channel_id}", status_code=204)
async def delete_channel_endpoint(
    channel_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    if not await repo.delete_channel(_parse_uuid(channel_id, "channel")):
        raise HTTPException(404, detail=f"Unknown channel: {channel_id!r}")
    return None


@router.post("/channels/{channel_id}/test")
async def test_channel_endpoint(
    channel_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    """Send a synthetic alert through this channel right now (bypasses rules/outbox). Lets the UI
    show a 'Test' button that verifies the webhook is reachable and well-configured."""
    row = await repo.get_channel(_parse_uuid(channel_id, "channel"))
    if row is None:
        raise HTTPException(404, detail=f"Unknown channel: {channel_id!r}")
    adapter = get_channel(row.channel_type)
    if adapter is None:
        raise HTTPException(400, detail=f"No adapter for channel type {row.channel_type!r}")
    event = Event(
        event_type="test", customer_code=row.customer_code, severity="info",
        title=f"[{row.customer_code}] Test notification",
        summary="If you can see this, the channel is configured correctly.",
        dedup_key=f"test:{row.id}:{uuid.uuid4()}",
        payload={"facts": {"Channel": row.name, "Type": row.channel_type}},
    )
    try:
        await adapter.send(event, row.config or {})
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, detail=f"Channel test failed: {exc}")
    return {"ok": True, "channel_id": str(row.id)}


# ===================================================================================================
# channels — customer-scoped collection
# ===================================================================================================
@router.post("/{customer_code}/channels", status_code=201)
async def create_channel_endpoint(
    customer_code: str, body: CreateChannelRequest,
    repo: NotificationRepository = Depends(get_notification_repository),
    customers: CustomerRepository = Depends(get_customer_repository),
):
    code = await _require_customer(customer_code, customers)
    if body.channel_type not in KNOWN_CHANNEL_TYPES:
        raise HTTPException(400, detail=f"Unknown channel_type {body.channel_type!r}. "
                                        f"Expected one of {sorted(KNOWN_CHANNEL_TYPES)}.")
    row = await repo.create_channel(customer_code=code, channel_type=body.channel_type,
                                    name=body.name, config=body.config, enabled=body.enabled)
    return _ser_channel(row)


@router.post("/{customer_code}/publish", status_code=201)
async def publish_manual_endpoint(
    customer_code: str, body: ManualPublishRequest,
    repo: NotificationRepository = Depends(get_notification_repository),
    customers: CustomerRepository = Depends(get_customer_repository),
):
    """Manually publish an ad-hoc alert to the customer's channel(s) — no rule involved.

    Goes through the SAME durable pipeline as rule-driven alerts (outbox → per-channel delivery →
    retry → activity), so it's tracked and resilient. Returns the created event plus the immediate
    per-channel delivery outcome. Use this for an analyst-initiated 'notify the team' action.
    """
    code = await _require_customer(customer_code, customers)
    if body.severity not in _VALID_SEVERITIES:
        raise HTTPException(400, detail=f"Invalid severity {body.severity!r}. "
                                        f"Expected one of {sorted(_VALID_SEVERITIES)}.")

    # Resolve the destination channels up-front so the user gets immediate feedback (vs. silently
    # persisting an event with nowhere to go).
    enabled = await repo.list_channels(customer_code=code, enabled_only=True)
    if body.target_channel_ids:
        wanted = {str(c) for c in body.target_channel_ids}
        resolved = [c for c in enabled if str(c.id) in wanted]
        if not resolved:
            raise HTTPException(400, detail="None of the requested channels are enabled for this "
                                            "customer.")
        targets = [str(c.id) for c in resolved]
    else:
        if not enabled:
            raise HTTPException(400, detail="This customer has no enabled channels to publish to. "
                                            "Add or enable a channel first.")
        targets = None  # all enabled

    facts: dict = {}
    if body.facts:
        facts.update({str(k): v for k, v in body.facts.items()})
    facts["Status"] = body.severity
    if body.posted_by:
        facts["Posted by"] = body.posted_by
    facts["Posted at"] = datetime.now(timezone.utc).isoformat()
    payload: dict = {"facts": facts}
    if body.transaction_id:
        payload["transaction_id"] = body.transaction_id
        facts["Transaction"] = body.transaction_id

    event = Event(
        event_type="manual",
        customer_code=code,
        severity=body.severity,
        title=body.title,
        summary=body.message,
        # unique per click — manual alerts always send (never deduped against a prior one).
        dedup_key=f"manual:{uuid.uuid4()}",
        payload=payload,
        target_channel_ids=targets,
    )
    # Persist outbox + create per-channel deliveries + attempt immediate send (independent of the
    # background worker; failures are picked up by the worker's retry loop when it's running).
    await dispatcher.handle(event)

    row = await repo.get_event_by_dedup_key(event.dedup_key)
    deliveries = await repo.get_deliveries_for_event(row.id) if row else []
    return {"event": _ser_event(row) if row else None,
            "deliveries": [_ser_delivery(d) for d in deliveries]}


@router.get("/{customer_code}/channels")
async def list_channels_endpoint(
    customer_code: str, enabled_only: bool = Query(default=False),
    repo: NotificationRepository = Depends(get_notification_repository),
    customers: CustomerRepository = Depends(get_customer_repository),
):
    code = await _require_customer(customer_code, customers)
    rows = await repo.list_channels(customer_code=code, enabled_only=enabled_only)
    return {"customer_code": code, "count": len(rows), "channels": [_ser_channel(r) for r in rows]}


# ===================================================================================================
# rules — single-resource ops
# ===================================================================================================
@router.get("/rules/{rule_id}")
async def get_rule_endpoint(
    rule_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    row = await repo.get_rule(_parse_uuid(rule_id, "rule"))
    if row is None:
        raise HTTPException(404, detail=f"Unknown rule: {rule_id!r}")
    return _ser_rule(row)


@router.patch("/rules/{rule_id}")
async def update_rule_endpoint(
    rule_id: str, body: UpdateRuleRequest,
    repo: NotificationRepository = Depends(get_notification_repository),
):
    rid = _parse_uuid(rule_id, "rule")
    current = await repo.get_rule(rid)
    if current is None:
        raise HTTPException(404, detail=f"Unknown rule: {rule_id!r}")
    # validate the resulting (type, match) pair
    rule_type = body.rule_type or current.rule_type
    match = body.match if body.match is not None else current.match
    _validate_rule(rule_type, match)
    row = await repo.update_rule(
        rid, name=body.name, description=body.description, rule_type=body.rule_type,
        match=body.match, severity=body.severity,
        target_channel_ids=body.target_channel_ids,
        unset_target_channels=body.clear_target_channels,
    )
    return _ser_rule(row)


@router.post("/rules/{rule_id}/publish")
async def publish_rule_endpoint(
    rule_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    """Make a rule live. Only ACTIVE rules are evaluated by the engine."""
    row = await repo.set_rule_status(_parse_uuid(rule_id, "rule"), RuleStatus.active.value)
    if row is None:
        raise HTTPException(404, detail=f"Unknown rule: {rule_id!r}")
    return _ser_rule(row)


@router.post("/rules/{rule_id}/deactivate")
async def deactivate_rule_endpoint(
    rule_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    """Stop a rule from firing without deleting it (can be published again later)."""
    row = await repo.set_rule_status(_parse_uuid(rule_id, "rule"), RuleStatus.inactive.value)
    if row is None:
        raise HTTPException(404, detail=f"Unknown rule: {rule_id!r}")
    return _ser_rule(row)


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule_endpoint(
    rule_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    if not await repo.delete_rule(_parse_uuid(rule_id, "rule")):
        raise HTTPException(404, detail=f"Unknown rule: {rule_id!r}")
    return None


# ===================================================================================================
# rules — customer-scoped collection
# ===================================================================================================
@router.post("/{customer_code}/rules", status_code=201)
async def create_rule_endpoint(
    customer_code: str, body: CreateRuleRequest,
    repo: NotificationRepository = Depends(get_notification_repository),
    customers: CustomerRepository = Depends(get_customer_repository),
):
    code = await _require_customer(customer_code, customers)
    _validate_rule(body.rule_type, body.match)
    row = await repo.create_rule(
        customer_code=code, name=body.name, rule_type=body.rule_type, match=body.match,
        severity=body.severity, description=body.description,
        target_channel_ids=body.target_channel_ids,
    )  # created as draft — publish to activate
    return _ser_rule(row)


@router.get("/{customer_code}/rules")
async def list_rules_endpoint(
    customer_code: str,
    status: str | None = Query(default=None, description="Filter by draft | active | inactive."),
    repo: NotificationRepository = Depends(get_notification_repository),
    customers: CustomerRepository = Depends(get_customer_repository),
):
    code = await _require_customer(customer_code, customers)
    rows = await repo.list_rules(customer_code=code, status=status)
    return {"customer_code": code, "count": len(rows), "rules": [_ser_rule(r) for r in rows]}


# ===================================================================================================
# observability — deliveries + events
# ===================================================================================================
@router.get("/events/{event_id}")
async def get_event_endpoint(
    event_id: str, repo: NotificationRepository = Depends(get_notification_repository),
):
    """An event plus its per-channel delivery rows (what was sent / pending / failed)."""
    eid = _parse_uuid(event_id, "event")
    event = await repo.get_event(eid)
    if event is None:
        raise HTTPException(404, detail=f"Unknown event: {event_id!r}")
    deliveries = await repo.get_deliveries_for_event(eid)
    return {"event": _ser_event(event), "deliveries": [_ser_delivery(d) for d in deliveries]}


@router.get("/{customer_code}/deliveries")
async def list_deliveries_endpoint(
    customer_code: str,
    status: str | None = Query(default=None, description="pending | delivered | failed | dead"),
    limit: int = Query(default=200, ge=1, le=1000),
    repo: NotificationRepository = Depends(get_notification_repository),
    customers: CustomerRepository = Depends(get_customer_repository),
):
    code = await _require_customer(customer_code, customers)
    rows = await repo.list_deliveries(customer_code=code, status=status, limit=limit)
    counts = await repo.delivery_counts(customer_code=code)
    return {"customer_code": code, "counts": counts, "count": len(rows),
            "deliveries": [_ser_delivery(d) for d in rows]}
