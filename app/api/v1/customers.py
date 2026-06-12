"""Tenant registry API — create + manage customer "log spaces".

A customer must be created here before any log can be ingested under its code. The frontend lists
these to let a user pick which tenant's log space to view or ingest into.
"""

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.deps import normalize_customer_code
from app.persistence.models.customer import Customer
from app.persistence.repositories.customer_repository import CustomerRepository, get_customer_repository

router = APIRouter(prefix="/customers", tags=["customers"])


class CreateCustomerRequest(BaseModel):
    customer_code: str = Field(..., description="Stable slug (lowercase letters/digits/-/_), e.g. 'acme'.")
    display_name: str | None = Field(default=None, description="Human-readable name shown in the UI.")


class UpdateCustomerRequest(BaseModel):
    active: bool = Field(..., description="Set false to retire the tenant from ingestion + selection.")


def _serialize(c: Customer) -> dict:
    return {
        "id": str(c.id),
        "customer_code": c.customer_code,
        "display_name": c.display_name,
        "active": c.active,
        "created_at": c.created_at.isoformat() if c.created_at else None,
    }


@router.post("", status_code=201)
async def create_customer(
    body: CreateCustomerRequest,
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """Create a new customer log space. 409 if the code already exists."""
    code = normalize_customer_code(body.customer_code)
    if code is None:
        raise HTTPException(400, detail="Invalid customer_code (expected a slug like 'acme').")
    if await repo.get_by_code(code) is not None:
        raise HTTPException(409, detail=f"Customer already exists: {code!r}")
    cust = await repo.create(code, body.display_name)
    return _serialize(cust)


@router.get("")
async def list_customers(
    include_inactive: bool = Query(default=True, description="Include retired (inactive) tenants."),
    repo: CustomerRepository = Depends(get_customer_repository),
):
    """List all tenants (for the frontend's log-space selector)."""
    rows = await repo.list_all(include_inactive=include_inactive)
    return {"count": len(rows), "customers": [_serialize(c) for c in rows]}


@router.get("/{customer_code}")
async def get_customer(
    customer_code: str,
    repo: CustomerRepository = Depends(get_customer_repository),
):
    code = normalize_customer_code(customer_code)
    cust = await repo.get_by_code(code) if code else None
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {customer_code!r}")
    return _serialize(cust)


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
