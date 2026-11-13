"""The approvals queue (stage D, step 7): a "with approval" request waits here for an Ops manager
(or anyone with the approve capability), expires after a day, and when approved applies what it
asked for (a capability unlocked for good, a user created, an action run). Audited throughout.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import AccessApproval, TeamsUserCapability
from app.services.access import audit, users
from app.services.access.matrix import ALLOWED

logger = logging.getLogger(__name__)

#: The reference prefix per kind, as screen 07 shows them (USR-2041, ACC-311).
PREFIXES = {"create_user": "USR", "change_role": "USR", "grant_capability": "ACC",
            "add_sites": "ACC", "run_action": "ACT", "export": "EXP"}
KINDS = tuple(PREFIXES)


async def next_reference(db: AsyncSession, customer_code: str, kind: str) -> str:
    """The next reference of a kind for the customer: the prefix and a number that only grows.

    ("acme", "create_user" with 2040 so far) -> "USR-2041"
    """
    prefix = PREFIXES[kind]
    count = (await db.execute(
        select(func.count()).select_from(AccessApproval)
        .where(AccessApproval.customer_code == customer_code,
               AccessApproval.reference.like(f"{prefix}-%"))
    )).scalar_one()
    return f"{prefix}-{count + 1}"


async def request(db: AsyncSession, customer_code: str, *, kind: str, requested_by: str,
                  requested_by_name: str | None, requested_via: str, title: str,
                  detail: str | None = None, site_id: str | None = None,
                  payload: dict | None = None, hours: int = 24,
                  now: datetime | None = None) -> AccessApproval:
    """Queue one request; audited as needs_approval. Raises on an unknown kind.

    ("acme", grant_capability view_all_sites by u-7) -> AccessApproval(reference "ACC-1", pending)
    """
    if kind not in KINDS:
        raise ValueError(f"unknown approval kind {kind!r}")
    now = now or datetime.now(timezone.utc)
    row = AccessApproval(customer_code=customer_code,
                         reference=await next_reference(db, customer_code, kind), kind=kind,
                         requested_by=requested_by, requested_by_name=requested_by_name,
                         requested_via=requested_via, site_id=site_id, title=title, detail=detail,
                         payload=payload or {}, status="pending",
                         expires_at=now + timedelta(hours=hours), created_at=now)
    db.add(row)
    await db.flush()
    await audit.record_event(
        db, customer_code, actor_object_id=requested_by, actor_name=requested_by_name,
        action=f"approval.requested.{kind}", capability_code=(payload or {}).get("capability"),
        decision="needs_approval", subject_kind="approval", subject_id=row.reference,
        site_id=site_id, text=f"{requested_by_name or requested_by} requested {title} "
                              f"via {requested_via} · pending", at=now)
    return row


async def pending(db: AsyncSession, customer_code: str) -> list[AccessApproval]:
    rows = (await db.execute(
        select(AccessApproval).where(AccessApproval.customer_code == customer_code,
                                     AccessApproval.status == "pending")
        .order_by(AccessApproval.created_at.desc())
    )).scalars()
    return list(rows)


async def recent(db: AsyncSession, customer_code: str, *, limit: int = 50) -> list[AccessApproval]:
    rows = (await db.execute(
        select(AccessApproval).where(AccessApproval.customer_code == customer_code)
        .order_by(AccessApproval.created_at.desc()).limit(limit)
    )).scalars()
    return list(rows)


async def decide(db: AsyncSession, customer_code: str, reference: str, *, approve: bool,
                 decided_by: str, decided_by_name: str | None, changes: dict | None = None,
                 note: str | None = None, now: datetime | None = None) -> AccessApproval:
    """Approve or decline one pending request and, on approval, apply it; audited.

    ("ACC-1" approve) -> status approved and the capability unlocked ; (not pending) -> ValueError
    """
    now = now or datetime.now(timezone.utc)
    row = (await db.execute(
        select(AccessApproval).where(AccessApproval.customer_code == customer_code,
                                     AccessApproval.reference == reference)
    )).scalar_one_or_none()
    if row is None or row.status != "pending":
        raise ValueError(f"approval {reference!r} is not pending")
    # Step 1: Record the decision first, so a failure in applying it leaves a declined-looking
    # row rather than a silently re-runnable one
    row.status = "approved" if approve else "declined"
    row.decided_by, row.decided_at, row.changes, row.note = decided_by, now, changes, note
    await db.flush()
    actor = users.Actor(decided_by, decided_by_name)
    if approve:
        await _apply(db, row, actor, now)
    await audit.record_event(
        db, customer_code, actor_object_id=decided_by, actor_name=decided_by_name,
        action=f"approval.{row.status}.{row.kind}", capability_code=row.payload.get("capability"),
        decision="approved" if approve else "declined", subject_kind="approval",
        subject_id=row.reference, site_id=row.site_id, detail=changes,
        text=f"{decided_by_name or decided_by} {row.status} {row.reference} · {row.title}"
             + (f" · {note}" if note else ""), at=now)
    return row


async def _apply(db: AsyncSession, row: AccessApproval, actor: users.Actor, now: datetime) -> None:
    """What an approved request does: the grant, the user, or nothing yet for an action."""
    payload = {**row.payload, **(row.changes or {})}
    if row.kind == "grant_capability":
        # the person keeps the capability for good (owner's decision of 13 November)
        user = await users.get_user(db, row.customer_code, row.requested_by)
        if user is None:
            raise ValueError(f"requester {row.requested_by!r} has no user row")
        code = payload["capability"]
        existing = (await db.execute(
            select(TeamsUserCapability).where(TeamsUserCapability.user_id == user.id,
                                              TeamsUserCapability.capability_code == code)
        )).scalar_one_or_none()
        if existing is None:
            db.add(TeamsUserCapability(user_id=user.id, capability_code=code, state=ALLOWED,
                                       granted_by=actor.object_id, granted_at=now, approval_id=row.id))
        else:
            existing.state, existing.granted_by, existing.granted_at = ALLOWED, actor.object_id, now
        if code == "view_all_sites":
            user.all_sites = True
    elif row.kind == "add_sites":
        user = await users.get_user(db, row.customer_code, row.requested_by)
        if user is None:
            raise ValueError(f"requester {row.requested_by!r} has no user row")
        current = {s.site_id for s in await _sites_of(db, user.id)}
        await users.update_user(db, row.customer_code, row.requested_by,
                                change=users.UserChange(site_ids=tuple(sorted(current | set(payload.get("site_ids", []))))),
                                actor=actor, now=now)
    elif row.kind in ("create_user", "change_role"):
        change = users.UserChange(
            display_name=payload.get("display_name"), email=payload.get("email"),
            role_code=payload.get("role"), all_sites=payload.get("all_sites"),
            site_ids=tuple(payload["site_ids"]) if payload.get("site_ids") is not None else None,
            starts_on=_date(payload.get("starts_on")), note=payload.get("note"), status="active")
        if row.kind == "create_user":
            await users.create_user(db, row.customer_code, payload["object_id"], change=change,
                                    actor=actor, now=now)
        else:
            await users.update_user(db, row.customer_code, payload["object_id"], change=change,
                                    actor=actor, now=now)
    # run_action and export: the step that runs actions (step 8) applies them; approved here only
    await db.flush()


async def _sites_of(db: AsyncSession, user_id):
    from app.persistence.models.access import TeamsUserSite

    return list((await db.execute(
        select(TeamsUserSite).where(TeamsUserSite.user_id == user_id))).scalars())


def _date(value):
    from datetime import date

    return date.fromisoformat(value) if value else None


async def withdraw(db: AsyncSession, customer_code: str, reference: str, *, by: str, by_name: str | None,
                   now: datetime | None = None) -> AccessApproval:
    """The requester takes a pending request back (a receipt's Undo, step 8); audited.

    ("USR-1" by its requester) -> status withdrawn ; (someone else's, or decided) -> ValueError
    """
    now = now or datetime.now(timezone.utc)
    row = (await db.execute(
        select(AccessApproval).where(AccessApproval.customer_code == customer_code,
                                     AccessApproval.reference == reference)
    )).scalar_one_or_none()
    if row is None or row.status != "pending":
        raise ValueError(f"approval {reference!r} is not pending")
    if row.requested_by != by:
        raise ValueError(f"approval {reference!r} was not requested by you")
    row.status, row.decided_by, row.decided_at = "withdrawn", by, now
    await db.flush()
    await audit.record_event(
        db, customer_code, actor_object_id=by, actor_name=by_name, action=f"approval.withdrawn.{row.kind}",
        decision="declined", subject_kind="approval", subject_id=row.reference,
        text=f"{by_name or by} withdrew {row.reference} · {row.title}", at=now)
    return row


async def expire_once(db: AsyncSession, *, now: datetime | None = None) -> int:
    """Mark pending requests past their expiry as expired; how many. Audited as system."""
    now = now or datetime.now(timezone.utc)
    rows = list((await db.execute(
        select(AccessApproval).where(AccessApproval.status == "pending",
                                     AccessApproval.expires_at < now)
    )).scalars())
    for row in rows:
        row.status, row.decided_at = "expired", now
        await audit.record_event(
            db, row.customer_code, actor_object_id="system", actor_name="expiry", actor_kind="system",
            action=f"approval.expired.{row.kind}", decision="expired", subject_kind="approval",
            subject_id=row.reference, text=f"{row.reference} · {row.title} expired unanswered", at=now)
    if rows:
        await db.commit()
    return len(rows)


def approval_payload(row: AccessApproval) -> dict:
    """One request as the Admin tab's queue shows it."""
    return {
        "reference": row.reference, "kind": row.kind, "title": row.title, "detail": row.detail,
        "requested_by": row.requested_by, "requested_by_name": row.requested_by_name,
        "requested_via": row.requested_via, "site_id": row.site_id, "status": row.status,
        "payload": row.payload, "decided_by": row.decided_by, "note": row.note,
        "created_at": row.created_at.isoformat(), "expires_at": row.expires_at.isoformat(),
        "decided_at": row.decided_at.isoformat() if row.decided_at else None,
    }
