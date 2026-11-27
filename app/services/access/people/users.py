"""The people of a customer (stage D, step 7): create, change, deactivate, list, and the first
Admin at initialisation. Every change writes an audit event in the same transaction.
Step 43: invite a person by email (a pending row with no object id), claimed at their first
sign-in, expired when unclaimed for `teams_invite_days`.
Used by the ops API (the superadmin), the Admin tab's commands and scripts/teams_users.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import (
    USER_STATUSES,
    TeamsUser,
    TeamsUserCapability,
    TeamsUserSite,
)
from app.persistence.models.tenant_site import TenantSite
from app.services.access import audit
from app.services.access.authorization import matrix as matrix_file
from app.services.access.authorization import roles
from app.services.access.authorization.matrix import STATES
from app.services.access.tenancy.bindings import customer_email_domains
from app.services.teams.mirror_rows import email_domain, normalize_email
from app.settings import settings


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


def _check(change: UserChange, customer_sites: set[str],
           customer_roles: list[roles.RoleInfo], *, current_role: str | None = None) -> None:
    m = matrix_file.load()
    role = next((r for r in customer_roles if r.code == change.role_code), None)
    if change.role_code is not None and role is None:
        raise ValueError(f"unknown role {change.role_code!r}")
    # a person who already carries a retired role keeps it until someone moves them; only
    # giving it to someone is refused
    if role is not None and not role.is_active and role.code != current_role:
        raise ValueError(f"the role {role.name} is retired; bring it back or pick another")
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
    if change.email and await get_user_by_email(db, customer_code, change.email) is not None:
        raise ValueError(f"{normalize_email(change.email)} is already a user of {customer_code}")
    _check(change, await _customer_sites(db, customer_code),
           await roles.customer_roles(db, customer_code))
    user = TeamsUser(customer_code=customer_code, object_id=object_id,
                     display_name=change.display_name, email=normalize_email(change.email) or None,
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
             f"{await _name_of(db, customer_code, user.role_code)}" + _sites_text(change), at=now)
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
    return await update_user_row(db, user, change=change, actor=actor, now=now)


async def update_user_row(db: AsyncSession, user: TeamsUser, *, change: UserChange, actor: Actor,
                          now: datetime | None = None) -> TeamsUser:
    """Change one user row, found by object id or, for a pending invite, by email; audited.

    (row, role ops_manager) -> the row, role changed ; (row, status deactivated) -> deactivated_at set
    """
    now = now or datetime.now(timezone.utc)
    customer_code = user.customer_code
    if change.email is not None:
        change = _with_email(change, normalize_email(change.email) or None)
    _check(change, await _customer_sites(db, customer_code),
           await roles.customer_roles(db, customer_code), current_role=user.role_code)
    if user.object_id is None and change.status == "active":
        raise ValueError(f"{user.display_name} has not signed in yet; the invite turns active on "
                         "their first sign-in")
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
        subject_kind="user", subject_id=user.object_id or user.email, detail=changed,
        text=f"{actor.name or actor.object_id} changed {user.display_name}: "
             + (", ".join(f"{k} {v}" for k, v in changed.items()) or "nothing"), at=now)
    return user


async def get_user_by_email(db: AsyncSession, customer_code: str, email: str) -> TeamsUser | None:
    """The user of one customer with this email, in any case; None when there is none."""
    return (await db.execute(
        select(TeamsUser).where(TeamsUser.customer_code == customer_code,
                                func.lower(TeamsUser.email) == normalize_email(email))
    )).scalar_one_or_none()


async def _check_domain(db: AsyncSession, customer_code: str, email: str) -> None:
    domains = await customer_email_domains(db, customer_code)
    if not domains:
        raise ValueError(f"{customer_code} has no email domains on its tenant binding; set them "
                         "first (PUT /api/v1/teams/bindings/<tenant> with email_domains)")
    if email_domain(email) not in domains:
        raise ValueError(f"{email} is outside {customer_code}'s email domains ({', '.join(sorted(domains))})")


def is_invite_expired(user: TeamsUser, now: datetime) -> bool:
    """Whether a pending invite is older than the invite window.

    (invited 8 days ago, window 7) -> True ; (invited today) -> False
    """
    return now - user.created_at > timedelta(days=settings.teams_invite_days)


async def invite_user(db: AsyncSession, customer_code: str, email: str, *, change: UserChange,
                      actor: Actor, now: datetime | None = None) -> TeamsUser:
    """Invite one person by email: a pending user with a role and sites, no object id yet; audited.

    ("acme", "Jo@Cust.com", supervisor BRI) -> pending row, email "jo@cust.com" ;
    (email taken, or outside the customer's domains) -> ValueError
    """
    now = now or datetime.now(timezone.utc)
    email = normalize_email(email)
    # Step 1: Accept only a real address inside the customer's own domains, so an invite can never
    # be claimed by a guest or by someone at another company
    if not email_domain(email):
        raise ValueError(f"{email!r} is not an email address")
    await _check_domain(db, customer_code, email)
    if await get_user_by_email(db, customer_code, email) is not None:
        raise ValueError(f"{email} is already a user of {customer_code}")
    if not change.display_name or not change.role_code:
        raise ValueError("an invite needs a display_name and a role_code")
    _check(change, await _customer_sites(db, customer_code),
           await roles.customer_roles(db, customer_code))
    # Step 2: The row is the pending user itself, so approving, editing or withdrawing it uses the
    # same code as for any user, and the claim only fills in the object id
    user = TeamsUser(customer_code=customer_code, object_id=None, display_name=change.display_name,
                     email=email, role_code=change.role_code, all_sites=bool(change.all_sites),
                     status="pending", starts_on=change.starts_on, note=change.note,
                     created_by=actor.object_id, created_at=now, updated_at=now)
    db.add(user)
    await db.flush()
    await _apply_sites_and_overrides(db, user, change, actor, now)
    await audit.record_event(
        db, customer_code, actor_object_id=actor.object_id, actor_name=actor.name,
        actor_kind=actor.kind, action="access.user_invited", capability_code="manage_users",
        decision="allowed", subject_kind="user", subject_id=email,
        text=f"{actor.name or actor.object_id} invited {user.display_name} ({email}) as "
             f"{await _name_of(db, customer_code, user.role_code)}" + _sites_text(change), at=now)
    return user


async def claim_invite(db: AsyncSession, customer_code: str, email: str, object_id: str, *,
                       name: str | None, now: datetime | None = None) -> TeamsUser:
    """Link a person's first sign-in to their pending invite; the user row, now active; audited.

    ("acme", "jo@cust.com", "u-jo") -> row with object_id u-jo, active ;
    (oid already a user, no pending invite, expired, outside the domains) -> ValueError
    """
    now = now or datetime.now(timezone.utc)
    email = normalize_email(email)
    object_id = (object_id or "").strip()
    if not object_id:
        raise ValueError("a claim needs the signed-in person's object id")
    # Step 1: Check the domain again here, because the backend never trusts the edge's own check
    await _check_domain(db, customer_code, email)
    # Step 2: An object id that already has a row keeps it, otherwise one person could end up
    # with two records and the edge would read whichever was mirrored last
    if await get_user(db, customer_code, object_id) is not None:
        raise ValueError("this person already has access")
    user = await get_user_by_email(db, customer_code, email)
    if user is None or user.object_id is not None or user.status != "pending":
        raise ValueError(f"no pending invite for {email}")
    if is_invite_expired(user, now):
        raise ValueError(f"the invite for {email} has expired; ask your admin to invite you again")
    # Step 3: From here the object id is the key for good, so the email is never matched again
    user.object_id, user.status = object_id, "active"
    user.last_active_at, user.updated_at, user.claimed_at = now, now, now
    await db.flush()
    who = name or object_id
    await audit.record_event(
        db, customer_code, actor_object_id=object_id, actor_name=name, actor_kind="user",
        action="access.user_claimed", capability_code=None, decision="allowed",
        subject_kind="user", subject_id=object_id, detail={"email": email},
        text=(f"{who} joined as {user.display_name} ({email}), "
              f"{await _name_of(db, customer_code, user.role_code)}"), at=now)
    return user


async def expire_invites_once(db: AsyncSession, *, now: datetime | None = None) -> int:
    """Mark pending invites past the invite window as expired, every customer; how many. Audited."""
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=settings.teams_invite_days)
    rows = list((await db.execute(
        select(TeamsUser).where(TeamsUser.status == "pending", TeamsUser.object_id.is_(None),
                                TeamsUser.created_at < cutoff).limit(500))).scalars())
    for user in rows:
        user.status, user.updated_at = "expired", now
        await audit.record_event(
            db, user.customer_code, actor_object_id="system", actor_name="system", actor_kind="system",
            action="access.invite_expired", capability_code="manage_users", decision="allowed",
            subject_kind="user", subject_id=user.email,
            text=f"invite for {user.display_name} ({user.email}) expired unclaimed", at=now)
    if rows:
        await db.flush()
    return len(rows)


def _with_email(change: UserChange, email: str | None) -> UserChange:
    from dataclasses import replace

    return replace(change, email=email)


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


def _role_name(code: str, names: dict[str, str] | None = None) -> str:
    """A role's name: the customer's own when `names` is given, else the built-in one."""
    if names is not None and code in names:
        return names[code]
    return next((r.name for r in matrix_file.load().roles if r.code == code), code)


async def _name_of(db: AsyncSession, customer_code: str, code: str) -> str:
    """The customer's own name for a role, for a sentence in the audit log."""
    return (await roles.role_names(db, customer_code)).get(code, code)


def _sites_text(change: UserChange) -> str:
    if change.all_sites:
        return " for all sites"
    return f" for {', '.join(change.site_ids)}" if change.site_ids else ""


def user_payload(user: TeamsUser, site_ids: list[str],
                 role_names: dict[str, str] | None = None) -> dict:
    """One user as the Admin tab lists them (no secrets); `role_names` are the customer's own."""
    return {
        "object_id": user.object_id, "display_name": user.display_name, "email": user.email,
        "role": user.role_code, "role_name": _role_name(user.role_code, role_names),
        "all_sites": user.all_sites, "site_ids": site_ids, "status": user.status,
        "starts_on": user.starts_on.isoformat() if user.starts_on else None, "note": user.note,
        "last_active_at": user.last_active_at.isoformat() if user.last_active_at else None,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        # step 43: an invite's state for the Admin tab ("Invited · until", "Joined · when")
        "claimed_at": user.claimed_at.isoformat() if user.claimed_at else None,
        "invite_expires_at": (
            (user.created_at + timedelta(days=settings.teams_invite_days)).isoformat()
            if user.status == "pending" and user.object_id is None and user.created_at else None),
    }
