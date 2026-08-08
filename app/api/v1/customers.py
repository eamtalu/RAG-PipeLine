"""Tenant registry API — create + manage customer "log spaces".

A customer must be created here before any log can be ingested under its code. The frontend lists
these to let a user pick which tenant's log space to view or ingest into.
"""

import logging
import uuid as _uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError, available_timezones

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import get_session
from app.api.deps import normalize_customer_code, require_admin
from app.persistence.models.customer import Customer, LogSpaceKind, LogSpaceEnvironment
from app.persistence.models.customer_display_name import CustomerDisplayName
from app.persistence.models.logspace_presence import LogspacePresence
from app.persistence.repositories.customer_repository import CustomerRepository, get_customer_repository
from app.persistence.repositories.logspace_presence_repository import (
    LogspacePresenceRepository, get_logspace_presence_repository)
from app.services.logspace_cleanup import purge_logspace
from app.services import timezone_change_guard

router = APIRouter(prefix="/customers", tags=["customers"])

logger = logging.getLogger(__name__)

_TZ_HELP = ("IANA timezone of this customer's log server, e.g. 'Europe/London' or 'Europe/Berlin'. "
            "Omit to leave it unconfigured (flagged as timezone_set=false until set).")


def _validate_tz(tz: str) -> str:
    """Reject anything that isn't a real IANA zone (a bad value would corrupt every timestamp)."""
    tz = (tz or "").strip()
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        raise HTTPException(400, detail=f"Invalid timezone {tz!r}. Use an IANA name like 'Europe/London'.")
    return tz


class CreateCustomerRequest(BaseModel):
    customer_code: str = Field(..., description="Stable slug (lowercase letters/digits/-/_), e.g. 'acme'.")
    display_name: str | None = Field(default=None, description="Human-readable name shown in the UI.")
    timezone: str | None = Field(default=None, description=_TZ_HELP)
    # Which kind of log space to create. Omit for the legacy create-or-attach path (kept unchanged).
    kind: LogSpaceKind | None = Field(default=None, description="'permanent' | 'disposable'. Omit for "
                                      "the legacy create-or-attach behavior.")
    # disposable-only
    owner_name: str | None = Field(default=None, description="Who created the disposable (disposable kind).")
    expires_at: datetime | None = Field(default=None, description="When the disposable auto-expires; "
                                        "defaults to now + configured TTL (disposable kind).")
    # permanent-only
    name: str | None = Field(default=None, description="Human name of a permanent space (permanent kind).")
    description: str | None = Field(default=None, description="Free-text description (permanent kind).")
    environment: LogSpaceEnvironment | None = Field(default=None, description="'live' | 'test' (permanent kind).")


class UpdateCustomerRequest(BaseModel):
    active: bool | None = Field(default=None, description="Set false to retire the tenant from ingestion + selection.")
    notifications_enabled: bool | None = Field(
        default=None,
        description="Whether this tenant's notification rules run and its queued alerts are sent. "
                    "Separate from a rule's own status: BOTH must be on. Takes effect within one "
                    "worker poll (~10s), no restart. Turning it off pauses delivery rather than "
                    "discarding it, so queued alerts resume if it is turned back on.")
    timezone: str | None = Field(default=None, description=_TZ_HELP)
    # permanent-space fields (admin-only edit)
    name: str | None = Field(default=None, description="Human name of a permanent space.")
    description: str | None = Field(default=None, description="Free-text description of a permanent space.")
    environment: LogSpaceEnvironment | None = Field(default=None, description="'live' | 'test'.")


class AddDisplayNameRequest(BaseModel):
    display_name: str = Field(..., min_length=1, max_length=128,
                              description="An additional human label / username to attach to this tenant.")


class PresenceRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128, description="Who is present (self-declared).")
    note: str | None = Field(default=None, max_length=256, description="Optional, e.g. 'debugging 15656'.")


def _presence_fresh_after() -> datetime:
    """Cutoff for 'active' presence: rows refreshed before this are stale and excluded from reads."""
    return datetime.now(timezone.utc) - timedelta(seconds=settings.logspace_presence_ttl_seconds)


def _presence_brief(p: LogspacePresence) -> dict:
    """The lightweight shape embedded in a log space's `active_presence`."""
    return {"name": p.name, "note": p.note, "since": p.since.isoformat() if p.since else None}


def _serialize_presence(p: LogspacePresence) -> dict:
    """The full presence object returned by the presence endpoints (spec D.2)."""
    return {
        "id": str(p.id),
        "customer_code": p.customer_code,
        "name": p.name,
        "note": p.note,
        "since": p.since.isoformat() if p.since else None,
    }


def _serialize(c: Customer, presence: list[LogspacePresence] | None = None) -> dict:
    return {
        "id": str(c.id),
        "customer_code": c.customer_code,
        "display_name": c.display_name,
        "timezone": c.timezone,                                      # exactly as stored (null = unset)
        "timezone_set": c.timezone is not None,                      # false → needs attention
        "effective_timezone": c.timezone or settings.display_timezone,  # what ingestion/display use
        "active": c.active,
        "notifications_enabled": c.notifications_enabled,   # the UI needs this to render its banner
        "created_at": c.created_at.isoformat() if c.created_at else None,
        # --- log-space kind + per-kind fields (permanent-only or disposable-only are null otherwise) ---
        "kind": c.kind.value if c.kind else None,
        "owner_name": c.owner_name,                                  # disposable
        "expires_at": c.expires_at.isoformat() if c.expires_at else None,  # disposable
        "name": c.name,                                              # permanent
        "description": c.description,                                # permanent
        "environment": c.environment.value if c.environment else None,  # permanent
        # derived from ingestion metrics; not computed yet → always null (never stored).
        "ingest_rate": None,
        "active_presence": [_presence_brief(p) for p in (presence or [])],
    }


def _serialize_display_name(d: CustomerDisplayName) -> dict:
    return {
        "id": str(d.id),
        "customer_code": d.customer_code,
        "display_name": d.display_name,
        "active": d.active,
        "created_at": d.created_at.isoformat() if d.created_at else None,
    }


def _group_presence(rows: list[LogspacePresence]) -> dict[str, list[LogspacePresence]]:
    by_code: dict[str, list[LogspacePresence]] = {}
    for p in rows:
        by_code.setdefault(p.customer_code, []).append(p)
    return by_code


async def _serialize_with_names(
    c: Customer, repo: CustomerRepository, presence_repo: LogspacePresenceRepository,
) -> dict:
    """Customer object plus its full list of attached display names (usernames) and active presence."""
    presence = await presence_repo.list_for_code(c.customer_code, fresh_after=_presence_fresh_after())
    out = _serialize(c, presence=presence)
    names = await repo.list_display_names(c.customer_code)
    out["display_names"] = [_serialize_display_name(n) for n in names]
    return out


@router.post("", status_code=201)
async def create_customer(
    body: CreateCustomerRequest,
    response: Response,
    repo: CustomerRepository = Depends(get_customer_repository),
    presence_repo: LogspacePresenceRepository = Depends(get_logspace_presence_repository),
):
    """Create a log space, or attach another display name to an existing one.

    Three paths, selected by `kind`:
    - `kind` omitted → LEGACY create-or-attach (unchanged): unknown code creates the tenant (201);
      existing code attaches the display_name (200), never 409.
    - `kind="disposable"` → a throwaway space owning a brand-new code: 409 if the code already exists;
      stamps `owner_name` and `expires_at` (now + configured TTL unless supplied). Returns 201.
    - `kind="permanent"` → an admin-curated space (admin-only): requires `name` + `environment`; 409 if
      the code already exists. Returns 201.
    """
    code = normalize_customer_code(body.customer_code)
    if code is None:
        raise HTTPException(400, detail="Invalid customer_code (expected a slug like 'acme').")
    tz = _validate_tz(body.timezone) if (body.timezone or "").strip() else None
    name = (body.display_name or "").strip() or None

    # --- PERMANENT: admin-curated, must not collide with an existing code ---
    if body.kind == LogSpaceKind.permanent:
        await require_admin()
        perm_name = (body.name or "").strip() or None
        if perm_name is None or body.environment is None:
            raise HTTPException(400, detail="A permanent log space requires 'name' and "
                                            "'environment' ('live' | 'test').")
        if await repo.get_by_code(code) is not None:
            raise HTTPException(409, detail=f"Customer code {code!r} already exists.")
        cust = await repo.create(code, name, timezone=tz, kind=LogSpaceKind.permanent,
                                 name=perm_name, description=(body.description or None),
                                 environment=body.environment)
        if name:
            await repo.add_display_name(code, name)
        return await _serialize_with_names(cust, repo, presence_repo)

    # --- DISPOSABLE: owns a brand-new code (1:1), so an existing code is a conflict ---
    if body.kind == LogSpaceKind.disposable:
        if await repo.get_by_code(code) is not None:
            raise HTTPException(409, detail=f"Customer code {code!r} already exists (a disposable owns "
                                            f"a brand-new code — pick a fresh one).")
        expires_at = body.expires_at or (
            datetime.now(timezone.utc) + timedelta(seconds=settings.logspace_disposable_ttl_seconds))
        owner = (body.owner_name or "").strip() or None
        cust = await repo.create(code, name, timezone=tz, kind=LogSpaceKind.disposable,
                                 owner_name=owner, expires_at=expires_at)
        if name:
            await repo.add_display_name(code, name)
        return await _serialize_with_names(cust, repo, presence_repo)

    # --- LEGACY (kind omitted): create-or-attach, unchanged behavior ---
    existing = await repo.get_by_code(code)
    if existing is not None:
        # tenant already exists → just attach the display name (idempotent, no 409)
        if name and await repo.get_display_name(code, name) is None:
            await repo.add_display_name(code, name)
        response.status_code = 200
        return await _serialize_with_names(existing, repo, presence_repo)

    # new tenant → create the row, and record the display name in the names list too (when given)
    cust = await repo.create(code, name, timezone=tz)
    if name:
        await repo.add_display_name(code, name)
    return await _serialize_with_names(cust, repo, presence_repo)


@router.get("")
async def list_customers(
    include_inactive: bool = Query(default=True, description="Include retired (inactive) tenants."),
    unset_timezone_only: bool = Query(default=False,
                                      description="Only tenants whose timezone is not yet configured "
                                                  "(timezone_set=false) — for a 'needs attention' view."),
    repo: CustomerRepository = Depends(get_customer_repository),
    presence_repo: LogspacePresenceRepository = Depends(get_logspace_presence_repository),
):
    """List all tenants (for the frontend's log-space selector). Each row carries `timezone`,
    `timezone_set`, and `effective_timezone` so the UI can flag any tenant still missing a timezone."""
    rows = await repo.list_all(include_inactive=include_inactive)
    if unset_timezone_only:
        rows = [c for c in rows if c.timezone is None]
    presence_by_code = _group_presence(await presence_repo.list_all(fresh_after=_presence_fresh_after()))
    return {"count": len(rows),
            "customers": [_serialize(c, presence_by_code.get(c.customer_code, [])) for c in rows]}


# NOTE: declared BEFORE GET "/{customer_code}" so "timezones" isn't parsed as a customer code.
@router.get("/timezones")
async def list_timezones():
    """Every valid IANA timezone name (sorted) — for a searchable picker in the customer create/edit
    UI. The `current_default` is what an unconfigured customer falls back to."""
    return {"current_default": settings.display_timezone,
            "timezones": sorted(available_timezones())}


# NOTE: must be declared BEFORE GET "/{customer_code}" — otherwise "log-spaces" is parsed as a code.
def _log_space_row(c: Customer, label: str, presence: list[LogspacePresence]) -> dict:
    """One selector row: the existing {label, customer_code, active} plus the enriched kind/per-kind
    fields (spec D.1) so the palette can render without a second join to GET /customers."""
    return {
        "label": label,
        "customer_code": c.customer_code,
        "active": c.active,
        "kind": c.kind.value if c.kind else None,
        "created_at": c.created_at.isoformat() if c.created_at else None,
        # disposable-only
        "owner_name": c.owner_name,
        "expires_at": c.expires_at.isoformat() if c.expires_at else None,
        # permanent-only
        "name": c.name,
        "description": c.description,
        "environment": c.environment.value if c.environment else None,
        "ingest_rate": None,
        "active_presence": [_presence_brief(p) for p in presence],
    }


@router.get("/log-spaces")
async def list_log_spaces(
    include_inactive: bool = Query(default=False, description="Include retired tenants / names."),
    repo: CustomerRepository = Depends(get_customer_repository),
    presence_repo: LogspacePresenceRepository = Depends(get_logspace_presence_repository),
):
    """Flat selector list for switching log spaces: one entry per display name (username), each
    resolving to a customer_code. A tenant with no attached display name still appears once
    (label falls back to its display_name, then its code). Each row carries the enriched log-space
    fields (kind, per-kind fields, active_presence) so the palette needs no second call."""
    customers = await repo.list_all(include_inactive=include_inactive)
    names = await repo.list_all_display_names(include_inactive=include_inactive)
    presence_by_code = _group_presence(await presence_repo.list_all(fresh_after=_presence_fresh_after()))

    by_code: dict[str, list[CustomerDisplayName]] = {}
    for n in names:
        by_code.setdefault(n.customer_code, []).append(n)

    out: list[dict] = []
    for c in customers:
        presence = presence_by_code.get(c.customer_code, [])
        rows = by_code.get(c.customer_code, [])
        if rows:
            for n in rows:
                out.append(_log_space_row(c, n.display_name, presence))
        else:
            out.append(_log_space_row(c, c.display_name or c.customer_code, presence))
    return {"count": len(out), "log_spaces": out}


@router.get("/{customer_code}")
async def get_customer(
    customer_code: str,
    repo: CustomerRepository = Depends(get_customer_repository),
    presence_repo: LogspacePresenceRepository = Depends(get_logspace_presence_repository),
):
    code = normalize_customer_code(customer_code)
    cust = await repo.get_by_code(code) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    # additive: existing keys unchanged; expose all attached display names (usernames) for this tenant.
    return await _serialize_with_names(cust, repo, presence_repo)


@router.get("/{customer_code}/display-names")
async def list_customer_display_names(
    customer_code: str,
    include_inactive: bool = Query(default=True, description="Include retired display names."),
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """List every display name (username) attached to this tenant."""
    code = normalize_customer_code(customer_code)
    cust = await repo.get_by_code(code) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    rows = await repo.list_display_names(code, include_inactive=include_inactive)
    return {"customer_code": code, "count": len(rows),
            "display_names": [_serialize_display_name(r) for r in rows]}


@router.post("/{customer_code}/display-names", status_code=201)
async def add_customer_display_name(
    customer_code: str,
    body: AddDisplayNameRequest,
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """Attach an additional display name (username) to an existing customer_code.

    A single tenant (e.g. 'mnp') may carry many display names. 404 if the code isn't registered,
    409 if the same display name is already attached to this tenant.
    """
    code = normalize_customer_code(customer_code)
    cust = await repo.get_by_code(code) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}. Create its log space "
                                        f"first (POST /api/v1/customers).")
    name = body.display_name.strip()
    if not name:
        raise HTTPException(400, detail="display_name must not be blank.")
    if await repo.get_display_name(code, name) is not None:
        raise HTTPException(409, detail=f"Display name {name!r} is already attached to {code!r}.")
    row = await repo.add_display_name(code, name)
    return _serialize_display_name(row)


@router.delete("/{customer_code}/display-names/{name_id}", status_code=204)
async def remove_customer_display_name(
    customer_code: str,
    name_id: str,
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """Detach a display name from a tenant. 404 if the tenant or the display name id is unknown."""
    code = normalize_customer_code(customer_code)
    cust = await repo.get_by_code(code) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    if not await repo.remove_display_name(code, name_id):
        raise HTTPException(404, detail=f"Unknown display name id {name_id!r} for {code!r}.")
    return None


def _permanent_fields(body: UpdateCustomerRequest) -> dict:
    """The admin-only permanent-space fields actually present on the request."""
    return {k: v for k in ("name", "description", "environment")
            if (v := getattr(body, k)) is not None}


def _require_something_to_update(body: UpdateCustomerRequest, perm_kwargs: dict) -> None:
    """Reject an empty PATCH rather than silently returning the unchanged tenant, which reads to the
    caller exactly like a successful update."""
    if (body.active is None and body.timezone is None
            and body.notifications_enabled is None and not perm_kwargs):
        raise HTTPException(400, detail="Provide 'active', 'timezone', 'notifications_enabled', "
                                        "and/or a permanent field "
                                        "('name'/'description'/'environment') to update.")


async def _set_timezone_guarded(db: AsyncSession, repo: CustomerRepository, cust: Customer,
                                requested_tz: str, *, allow_mixed: bool) -> Customer:
    """Change a tenant's timezone, refusing when doing so would corrupt already-ingested data.

    Judged against the STORED value rather than the serialized effective one: `null` means "the global
    default", and the guard needs that distinction to tell a real change from an operator merely
    writing down the default the tenant was already using.
    """
    new_tz = _validate_tz(requested_tz)
    reason = await timezone_change_guard.blocking_reason(
        db, customer_code=cust.customer_code, stored_tz=cust.timezone, new_tz=new_tz)
    if reason and not allow_mixed:
        # 409, not 400: the request is well-formed and would be valid against a tenant with no
        # entries. It conflicts with the current state of THIS one.
        raise HTTPException(409, detail=reason)
    if reason:
        # CRITICAL because from here on nothing in the DATA marks where the derivation changed —
        # this log line is the only record that the seam exists.
        logger.critical(
            "Timezone of %r changed from %r to %r WITH entries already ingested (%s=true). "
            "Timestamps stored before now keep the old derivation; re-ingesting an already-loaded "
            "file can now create duplicate entries.",
            cust.customer_code, cust.timezone, new_tz, timezone_change_guard.OVERRIDE_PARAM)
    return await repo.set_timezone(cust.customer_code, new_tz)


@router.patch("/{customer_code}")
async def update_customer(
    customer_code: str,
    body: UpdateCustomerRequest,
    allow_mixed_timezones: bool = Query(
        default=False,
        description="Proceed with a timezone change even though this tenant already has ingested "
                    "entries. Their stored timestamps keep the OLD derivation while new ones use the "
                    "new zone, and re-ingesting a file can then insert duplicate rows. Only set this "
                    "if you accept a split timeline; the safe route is to purge the log data first.",
    ),
    db: AsyncSession = Depends(get_session),
    repo: CustomerRepository = Depends(get_customer_repository),
    presence_repo: LogspacePresenceRepository = Depends(get_logspace_presence_repository),
):
    """Activate/deactivate a tenant, update its timezone, and/or edit permanent-space fields
    (name/description/environment — admin-only). Deactivating retires it from ingestion + selection
    without deleting its historical data (use DELETE /customers/{code} to purge the space itself).

    Changing `timezone` affects how NEW log lines are converted to UTC at ingestion and how all of this
    tenant's timestamps are displayed; it does NOT rewrite already-stored instants. Because of that,
    changing it once entries exist is REFUSED with 409 unless `allow_mixed_timezones=true` — see
    app/services/timezone_change_guard.py for why."""
    code = normalize_customer_code(customer_code)
    if code is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    perm_kwargs = _permanent_fields(body)
    _require_something_to_update(body, perm_kwargs)

    cust = await repo.get_by_code(code)
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    if perm_kwargs:
        await require_admin()
        cust = await repo.update_fields(code, **perm_kwargs)
    if body.timezone is not None:
        cust = await _set_timezone_guarded(db, repo, cust, body.timezone,
                                           allow_mixed=allow_mixed_timezones)
    if body.active is not None:
        cust = await repo.set_active(code, body.active)
    if body.notifications_enabled is not None:
        cust = await repo.set_notifications_enabled(code, body.notifications_enabled)
    presence = await presence_repo.list_for_code(code, fresh_after=_presence_fresh_after())
    return _serialize(cust, presence=presence)


@router.delete("/{customer_code}", status_code=204)
async def delete_customer(
    customer_code: str,
    _admin: None = Depends(require_admin),
    db: AsyncSession = Depends(get_session),
):
    """Hard-delete a log space (admin-only) — the real purge, not a soft deactivate.

    Removes the customer_code and ALL data keyed by it: display-name aliases, presence rows, jobs and
    their log entries/transactions/chunks/embeddings, saved views, SSH sources, and notification config.
    204 on success, 404 if the code doesn't exist. Applies to both disposable and permanent spaces.
    """
    code = normalize_customer_code(customer_code)
    purged = await purge_logspace(db, code) if code else False
    if not purged:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    return None


@router.post("/{customer_code}/presence")
async def enter_presence(
    customer_code: str,
    body: PresenceRequest,
    repo: CustomerRepository = Depends(get_customer_repository),
    presence_repo: LogspacePresenceRepository = Depends(get_logspace_presence_repository),
):
    """Declare presence in a log space (a user opened it). Upserts by (customer_code, name): a repeat
    call refreshes `since` + `note` rather than duplicating. 404 if the code isn't registered."""
    code = normalize_customer_code(customer_code)
    cust = await repo.get_by_code(code) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    name = body.name.strip()
    if not name:
        raise HTTPException(400, detail="name must not be blank.")
    note = (body.note or "").strip() or None
    row = await presence_repo.upsert(code, name, note)
    return _serialize_presence(row)


@router.delete("/{customer_code}/presence/{presence_id}", status_code=204)
async def leave_presence(
    customer_code: str,
    presence_id: str,
    presence_repo: LogspacePresenceRepository = Depends(get_logspace_presence_repository),
):
    """Remove a presence row (a user left the space). 404 if the code or presence id is unknown."""
    code = normalize_customer_code(customer_code)
    try:
        # a malformed (non-UUID) id is a by-id 404, never a 422/500 from the DB layer.
        _uuid.UUID(presence_id)
        removed = bool(code) and await presence_repo.remove(code, presence_id)
    except ValueError:
        removed = False
    if not removed:
        raise HTTPException(404, detail=f"Unknown presence {presence_id!r} for {customer_code!r}.")
    return None
