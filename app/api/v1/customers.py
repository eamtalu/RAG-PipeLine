"""Tenant registry API — create + manage customer "log spaces".

A customer must be created here before any log can be ingested under its code. The frontend lists
these to let a user pick which tenant's log space to view or ingest into.
"""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from app.api.deps import normalize_customer_code
from app.persistence.models.customer import Customer
from app.persistence.models.customer_display_name import CustomerDisplayName
from app.persistence.repositories.customer_repository import CustomerRepository, get_customer_repository

router = APIRouter(prefix="/customers", tags=["customers"])


class CreateCustomerRequest(BaseModel):
    customer_code: str = Field(..., description="Stable slug (lowercase letters/digits/-/_), e.g. 'acme'.")
    display_name: str | None = Field(default=None, description="Human-readable name shown in the UI.")


class UpdateCustomerRequest(BaseModel):
    active: bool = Field(..., description="Set false to retire the tenant from ingestion + selection.")


class AddDisplayNameRequest(BaseModel):
    display_name: str = Field(..., min_length=1, max_length=128,
                              description="An additional human label / username to attach to this tenant.")


def _serialize(c: Customer) -> dict:
    return {
        "id": str(c.id),
        "customer_code": c.customer_code,
        "display_name": c.display_name,
        "active": c.active,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


def _serialize_display_name(d: CustomerDisplayName) -> dict:
    return {
        "id": str(d.id),
        "customer_code": d.customer_code,
        "display_name": d.display_name,
        "active": d.active,
        "created_at": d.created_at.isoformat() if d.created_at else None,
    }


async def _serialize_with_names(c: Customer, repo: CustomerRepository) -> dict:
    """Customer object plus its full list of attached display names (usernames)."""
    out = _serialize(c)
    names = await repo.list_display_names(c.customer_code)
    out["display_names"] = [_serialize_display_name(n) for n in names]
    return out


@router.post("", status_code=201)
async def create_customer(
    body: CreateCustomerRequest,
    response: Response,
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """Create a log space, or attach another display name to an existing one.

    - Unknown customer_code → create the tenant row (HTTP 201).
    - Existing customer_code → attach display_name as an extra row under that tenant (HTTP 200).
      If that display_name is already attached (or none was given) it's a no-op and the existing
      tenant is returned. The customer_code itself is never duplicated.
    """
    code = normalize_customer_code(body.customer_code)
    if code is None:
        raise HTTPException(400, detail="Invalid customer_code (expected a slug like 'acme').")

    name = (body.display_name or "").strip() or None
    existing = await repo.get_by_code(code)

    if existing is not None:
        # tenant already exists → just attach the display name (idempotent, no 409)
        if name and await repo.get_display_name(code, name) is None:
            await repo.add_display_name(code, name)
        response.status_code = 200
        return await _serialize_with_names(existing, repo)

    # new tenant → create the row, and record the display name in the names list too (when given)
    cust = await repo.create(code, name)
    if name:
        await repo.add_display_name(code, name)
    return await _serialize_with_names(cust, repo)


@router.get("")
async def list_customers(
    include_inactive: bool = Query(default=True, description="Include retired (inactive) tenants."),
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """List all tenants (for the frontend's log-space selector)."""
    rows = await repo.list_all(include_inactive=include_inactive)
    return {"count": len(rows), "customers": [_serialize(c) for c in rows]}


# NOTE: must be declared BEFORE GET "/{customer_code}" — otherwise "log-spaces" is parsed as a code.
@router.get("/log-spaces")
async def list_log_spaces(
    include_inactive: bool = Query(default=False, description="Include retired tenants / names."),
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """Flat selector list for switching log spaces: one entry per display name (username), each
    resolving to a customer_code. A tenant with no attached display name still appears once
    (label falls back to its display_name, then its code)."""
    customers = await repo.list_all(include_inactive=include_inactive)
    names = await repo.list_all_display_names(include_inactive=include_inactive)

    by_code: dict[str, list[CustomerDisplayName]] = {}
    for n in names:
        by_code.setdefault(n.customer_code, []).append(n)

    out: list[dict] = []
    for c in customers:
        rows = by_code.get(c.customer_code, [])
        if rows:
            for n in rows:
                out.append({"label": n.display_name, "customer_code": c.customer_code, "active": c.active})
        else:
            out.append({"label": c.display_name or c.customer_code, "customer_code": c.customer_code,
                        "active": c.active})
    return {"count": len(out), "log_spaces": out}


@router.get("/{customer_code}")
async def get_customer(
    customer_code: str,
    repo: CustomerRepository = Depends(get_customer_repository),
):
    code = normalize_customer_code(customer_code)
    cust = await repo.get_by_code(code) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    # additive: existing keys unchanged; expose all attached display names (usernames) for this tenant.
    return await _serialize_with_names(cust, repo)


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


@router.patch("/{customer_code}")
async def update_customer(
    customer_code: str,
    body: UpdateCustomerRequest,
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """Activate / deactivate a tenant. Deactivating retires it from ingestion + selection without
    deleting its historical data (use DELETE /logs/data to remove the data itself)."""
    code = normalize_customer_code(customer_code)
    cust = await repo.set_active(code, body.active) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    return _serialize(cust)
