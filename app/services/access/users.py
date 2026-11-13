"""The people of a customer (stage D, step 7): create, change, deactivate, list, and the first
Admin at initialisation. Every change writes an audit event in the same transaction.
Used by the ops API (the superadmin), the Admin tab's commands and scripts/teams_users.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import (
    USER_STATUSES,
    TeamsUser,
    TeamsUserCapability,
    TeamsUserSite,
)
from app.persistence.models.tenant_site import TenantSite
from app.services.access import audit
from app.services.access import matrix as matrix_file
from app.services.access.matrix import STATES


@dataclass(frozen=True)
class Actor:
    """Who is making the change, for the audit line: the superadmin or a Teams user."""

    object_id: str
    name: str | None = None
    kind: str = "user"  # user | system

    @classmethod
    def system(cls, name: str = "superadmin") -> "Actor":
        return cls(object_id="system", name=name, kind="system")


@dataclass(frozen=True)
class UserChange:
    """What may be set on a user; None leaves a field as it is."""

    display_name: str | None = None
    email: str | None = None
    role_code: str | None = None
    all_sites: bool | None = None
    site_ids: tuple[str, ...] | None = None
    capabilities: dict[str, str] | None = None  # per-capability overrides; {} clears them
    status: str | None = None
    starts_on: date | None = None
    note: str | None = None


def _check(change: UserChange, customer_sites: set[str]) -> None:
    m = matrix_file.load()
    if change.role_code is not None and change.role_code not in m.role_codes:
        raise ValueError(f"unknown role {change.role_code!r}")
    if change.status is not None and change.status not in USER_STATUSES:
        raise ValueError(f"unknown status {change.status!r}")
    for site_id in change.site_ids or ():
        if site_id not in customer_sites:
            raise ValueError(f"unknown site {site_id!r}")
    for code, state in (change.capabilities or {}).items():
        if code not in m.capability_codes:
            raise ValueError(f"unknown capability {code!r}")
        if state not in STATES:
            raise ValueError(f"unknown state {state!r} for {code}")


async def _customer_sites(db: AsyncSession, customer_code: str) -> set[str]:
    rows = (await db.execute(
        select(TenantSite.site_id).where(TenantSite.customer_code == customer_code))).scalars()
    return set(rows)


async def get_user(db: AsyncSession, customer_code: str, object_id: str) -> TeamsUser | None:
    return (await db.execute(
        select(TeamsUser).where(TeamsUser.customer_code == customer_code,
                                TeamsUser.object_id == object_id)
    )).scalar_one_or_none()


async def list_users(db: AsyncSession, customer_code: str) -> list[TeamsUser]:
    rows = (await db.execute(
        select(TeamsUser).where(TeamsUser.customer_code == customer_code)
        .order_by(TeamsUser.display_name)
    )).scalars()
    return list(rows)


async def create_user(db: AsyncSession, customer_code: str, object_id: str, *, change: UserChange,
                      actor: Actor, now: datetime | None = None) -> TeamsUser:
    """Create one user with a role and sites; audited. Raises on an unknown role, site or state.

    ("acme", "u-9", role supervisor, sites (BRI,)) -> the row ; (object id exists) -> ValueError
    """
    now = now or datetime.now(timezone.utc)
    # Step 1: Refuse a second row for the same person, so one object id has exactly one record
    if await get_user(db, customer_code, object_id) is not None:
        raise ValueError(f"user {object_id!r} already exists for {customer_code}")
    if not change.display_name or not change.role_code:
        raise ValueError("a new user needs a display_name and a role_code")
    _check(change, await _customer_sites(db, customer_code))
    user = TeamsUser(customer_code=customer_code, object_id=object_id,
                     display_name=change.display_name, email=change.email,
                     role_code=change.role_code, all_sites=bool(change.all_sites),
                     status=change.status or "active", starts_on=change.starts_on,
                     note=change.note, created_by=actor.object_id, created_at=now, updated_at=now)
    db.add(user)
    await db.flush()
    # Step 2: Sites and overrides are rows of their own, so a change later is a plain rewrite
    await _apply_sites_and_overrides(db, user, change, actor, now)
    await audit.record_event(
        db, customer_code, actor_object_id=actor.object_id, actor_name=actor.name,
        actor_kind=actor.kind, action="access.user_created", capability_code="manage_users",
        decision="allowed", subject_kind="user", subject_id=object_id,
        text=f"{actor.name or actor.object_id} created {user.display_name} as "
             f"{_role_name(user.role_code)}" + _sites_text(change), at=now)
    return user


async def update_user(db: AsyncSession, customer_code: str, object_id: str, *, change: UserChange,
                      actor: Actor, now: datetime | None = None) -> TeamsUser:
    """Change a user's fields, sites or overrides; audited with what changed.

    ("u-9", role ops_manager) -> the row, role changed ; (unknown user) -> ValueError
    """
    now = now or datetime.now(timezone.utc)
    user = await get_user(db, customer_code, object_id)
    if user is None:
        raise ValueError(f"no user {object_id!r} for {customer_code}")
    _check(change, await _customer_sites(db, customer_code))
    changed: dict[str, object] = {}
    for field_name in ("display_name", "email", "role_code", "all_sites", "status", "starts_on", "note"):
        value = getattr(change, field_name)
        if value is not None and getattr(user, field_name) != value:
            changed[field_name] = value
            setattr(user, field_name, value)
    if change.status == "deactivated" and user.deactivated_at is None:
        user.deactivated_at = now
    if change.status == "active":
        user.deactivated_at = None
    user.updated_at = now
    if change.site_ids is not None:
        changed["site_ids"] = list(change.site_ids)
    if change.capabilities is not None:
        changed["capabilities"] = dict(change.capabilities)
    await _apply_sites_and_overrides(db, user, change, actor, now)
    action = "access.user_deactivated" if change.status == "deactivated" else "access.user_updated"
    await audit.record_event(
        db, customer_code, actor_object_id=actor.object_id, actor_name=actor.name,
        actor_kind=actor.kind, action=action, capability_code="manage_users", decision="allowed",
        subject_kind="user", subject_id=object_id, detail=changed,
        text=f"{actor.name or actor.object_id} changed {user.display_name}: "
             + (", ".join(f"{k} {v}" for k, v in changed.items()) or "nothing"), at=now)
    return user


async def touch_last_active(db: AsyncSession, customer_code: str, object_id: str, *,
                            now: datetime) -> None:
    """Stamp when the person last asked; no row, nothing happens."""
    user = await get_user(db, customer_code, object_id)
    if user is not None:
        user.last_active_at = now


async def _apply_sites_and_overrides(db: AsyncSession, user: TeamsUser, change: UserChange,
                                     actor: Actor, now: datetime) -> None:
    if change.site_ids is not None or change.all_sites:
        for row in (await db.execute(
                select(TeamsUserSite).where(TeamsUserSite.user_id == user.id))).scalars():
            await db.delete(row)
        if not user.all_sites:
            for site_id in change.site_ids or ():
                db.add(TeamsUserSite(user_id=user.id, customer_code=user.customer_code, site_id=site_id))
    if change.capabilities is not None:
        for row in (await db.execute(
                select(TeamsUserCapability).where(TeamsUserCapability.user_id == user.id))).scalars():
            await db.delete(row)
        for code, state in change.capabilities.items():
            db.add(TeamsUserCapability(user_id=user.id, capability_code=code, state=state,
                                       granted_by=actor.object_id, granted_at=now))
    await db.flush()


def _role_name(code: str) -> str:
    return next((r.name for r in matrix_file.load().roles if r.code == code), code)


def _sites_text(change: UserChange) -> str:
    if change.all_sites:
        return " for all sites"
    return f" for {', '.join(change.site_ids)}" if change.site_ids else ""


def user_payload(user: TeamsUser, site_ids: list[str]) -> dict:
    """One user as the Admin tab lists them (no secrets)."""
    return {
        "object_id": user.object_id, "display_name": user.display_name, "email": user.email,
        "role": user.role_code, "role_name": _role_name(user.role_code),
        "all_sites": user.all_sites, "site_ids": site_ids, "status": user.status,
        "starts_on": user.starts_on.isoformat() if user.starts_on else None, "note": user.note,
        "last_active_at": user.last_active_at.isoformat() if user.last_active_at else None,
        "created_at": user.created_at.isoformat() if user.created_at else None,
    }
