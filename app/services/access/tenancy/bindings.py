"""Tenancy: which customer a Microsoft 365 tenant belongs to, and which email domains are that
customer's own (step 43). The binding rows are the superadmin's (PUT /api/v1/teams/bindings);
`binding_mirror` and `binding_sweep` copy them to the edge, which turns a sign-in's tenant into a
customer before any record is read.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.teams_binding import TeamsTenantBinding


async def customer_email_domains(db: AsyncSession, customer_code: str) -> set[str]:
    """The email domains named on the customer's enabled tenant bindings, lower case.

    ("tmp-live" with a binding for becsi.co.uk) -> {"becsi.co.uk"} ; (no domains set) -> set()
    """
    rows = (await db.execute(
        select(TeamsTenantBinding.email_domains).where(TeamsTenantBinding.customer_code == customer_code,
                                                       TeamsTenantBinding.enabled.is_(True)))).scalars()
    return {d.strip().lower() for domains in rows for d in (domains or ()) if d and d.strip()}
