"""Shared FastAPI dependencies for the API layer.

Tenant identity is resolved here from the `X-Customer-Code` header and validated against the
customers registry (the day we add authentication, only these functions change — every endpoint,
query, the Stage 2 grouper, and the debugging agent keep working because they all receive the
customer through one of these dependencies).

Two levels:
- `get_current_customer`  — the code must be well-formed AND exist in the registry (active or not).
  Used by reads, regroup, delete, and the agent so a typo'd tenant gets a clear 404, never a silent
  cross/empty query.
- `get_active_customer`   — additionally requires the tenant to be ACTIVE. Used by ingest/scan, so a
  log can only be written into an existing, non-retired customer space.
"""

import re

from fastapi import Depends, Header, HTTPException

from app.persistence.repositories.customer_repository import CustomerRepository, get_customer_repository
from app.services.mnp_log_ingestion.timefmt import set_display_timezone

# customer codes are slugs: lowercase letters, digits, hyphen/underscore, 1–64 chars.
_CUSTOMER_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def normalize_customer_code(value: str | None) -> str | None:
    """Normalize + format-validate a customer code. Returns the canonical slug, or None if malformed.
    Pure helper (no DB) reused by the API dependencies, the create-tenant endpoint, and the watcher."""
    code = (value or "").strip().lower()
    return code if _CUSTOMER_RE.match(code) else None


async def require_admin() -> None:
    """Guard for admin-only log-space operations (permanent create/edit and any hard delete).

    There is no authentication system yet, so this is a single, centralized permit-all placeholder: the
    admin-only routes already depend on it, so when real auth lands only this function changes and the
    whole Manage surface is gated at once. Raise HTTPException(403) here to enforce.
    """
    # TODO(auth): resolve the caller and reject non-admins once authentication exists.
    return


async def get_current_customer(
    x_customer_code: str = Header(..., alias="X-Customer-Code",
                                 description="Customer/tenant code the request operates on."),
    repo: CustomerRepository = Depends(get_customer_repository),
) -> str:
    """Resolve + validate the tenant for this request: well-formed AND registered.

    400 if malformed, 404 if the code isn't a registered customer. The header is required, so an
    omitted header gets a 422 from FastAPI — reads never fall back to a cross-tenant query.
    """
    code = normalize_customer_code(x_customer_code)
    if code is None:
        raise HTTPException(400, detail="Invalid X-Customer-Code (expected a slug like 'acme').")
    cust = await repo.get_by_code(code)
    if cust is None:
        raise HTTPException(404, detail=f"Unknown customer: {code!r}. Create its log space first "
                                        f"(POST /api/v1/customers).")
    # pin this request's display zone to the customer's timezone so every timestamp this request
    # serializes/renders comes back in that customer's local time.
    set_display_timezone(cust.timezone)
    return code


async def get_active_customer(
    x_customer_code: str = Header(..., alias="X-Customer-Code",
                                 description="Customer/tenant code to ingest into (must be active)."),
    repo: CustomerRepository = Depends(get_customer_repository),
) -> str:
    """Like get_current_customer but also requires the tenant to be ACTIVE — used for ingestion."""
    code = normalize_customer_code(x_customer_code)
    if code is None:
        raise HTTPException(400, detail="Invalid X-Customer-Code (expected a slug like 'acme').")
    cust = await repo.get_by_code(code)
    if cust is None or not cust.active:
        raise HTTPException(404, detail=f"Unknown or inactive customer: {code!r}. Create/activate its "
                                        f"log space first (POST /api/v1/customers).")
    set_display_timezone(cust.timezone)
    return code
