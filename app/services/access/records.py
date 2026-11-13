"""One person's access record and the decision it gives (stage D, step 7).

The record is what both sides enforce from: the role, the sites, the status and, per capability,
the effective state (the customer's matrix cell, overridden by anything an Admin set or an
approval granted for that person). The backend computes it here from Postgres; the consumer
mirrors it to the edge, which checks it on every /api/v2 route. Decisions never raise: a route
turns "refused" into 403 and a job into a sentence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.access import (
    AccessRoleCapability,
    TeamsUser,
    TeamsUserCapability,
    TeamsUserSite,
)
from app.services.access import matrix as matrix_file
from app.services.access.matrix import ALLOWED, NO, WITH_APPROVAL

ALL_SITES = "all"


@dataclass(frozen=True)
class AccessRecord:
    """What a person may do: the shape the edge mirrors and checks."""

    customer_code: str
    object_id: str
    display_name: str
    role_code: str
    role_name: str
    status: str
    all_sites: bool
    site_ids: tuple[str, ...]
    capabilities: dict[str, str] = field(default_factory=dict)  # code -> allowed|with_approval|no
    last_active_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    def state(self, capability: str) -> str:
        """The effective state of one capability; "no" for one the record does not name.

        >>> AccessRecord("c", "u", "U", "picker", "Picker", "active", False, ("BRI",),
        ...              {"ask": "with_approval"}).state("ask")
        'with_approval'
        >>> AccessRecord("c", "u", "U", "picker", "Picker", "active", False, ()).state("export")
        'no'
        """
        return self.capabilities.get(capability, NO)

    def may_see(self, site_id: str | None) -> bool:
        """Whether the record's sites cover one site, or every site for "all" / None.

        >>> r = AccessRecord("c", "u", "U", "supervisor", "Supervisor", "active", False, ("BRI",))
        >>> r.may_see("BRI"), r.may_see("LON"), r.may_see("all"), r.may_see(None)
        (True, False, False, False)
        """
        if site_id is None or site_id == ALL_SITES:
            return self.all_sites
        return self.all_sites or site_id in self.site_ids

    def as_payload(self) -> dict:
        """The record as the mirror row carries it."""
        return {
            "customer_code": self.customer_code,
            "object_id": self.object_id,
            "display_name": self.display_name,
            "role": self.role_code,
            "role_name": self.role_name,
            "status": self.status,
            "all_sites": self.all_sites,
            "site_ids": list(self.site_ids),
            "capabilities": dict(self.capabilities),
            "last_active_at": self.last_active_at.isoformat() if self.last_active_at else None,
        }


@dataclass(frozen=True)
class Decision:
    """The outcome of asking "may this person do X at site Y": allowed, needs approval, refused."""

    outcome: str  # allowed | needs_approval | refused
    reason: str = ""

    @property
    def is_allowed(self) -> bool:
        return self.outcome == "allowed"


def decide(record: AccessRecord | None, capability: str, site_id: str | None = None) -> Decision:
    """Whether a record may use a capability at a site, from the record alone.

    >>> r = AccessRecord("c", "u", "U", "supervisor", "Supervisor", "active", False, ("BRI",),
    ...                  {"view_own_site": "allowed", "view_all_sites": "with_approval",
    ...                   "approve": "no", "ask": "allowed"})
    >>> decide(r, "ask", "BRI").outcome, decide(r, "ask", "LON").outcome
    ('allowed', 'refused')
    >>> decide(r, "view_all_sites", "all").outcome, decide(r, "approve").outcome
    ('needs_approval', 'refused')
    >>> decide(None, "ask").outcome
    'refused'
    """
    # Step 1: No record, or a person switched off, may do nothing, so a deactivated leaver is
    # locked out the moment the record is rewritten
    if record is None:
        return Decision("refused", "no access record")
    if not record.is_active:
        return Decision("refused", f"user is {record.status}")
    # Step 2: The capability's own state decides, except that a site outside the person's scope
    # is a refusal unless the capability is the one that widens the scope (view_all_sites)
    state = record.state(capability)
    if state == NO:
        return Decision("refused", f"{capability} is not allowed for {record.role_name}")
    if site_id is not None and capability != "view_all_sites" and not record.may_see(site_id):
        if record.state("view_all_sites") == ALLOWED:
            return Decision(state if state != ALLOWED else "allowed", "")
        return Decision("refused", f"site {site_id} is outside this user's sites")
    if state == WITH_APPROVAL:
        return Decision("needs_approval", f"{capability} needs approval for {record.role_name}")
    return Decision("allowed")


async def ensure_matrix(db: AsyncSession, customer_code: str) -> int:
    """Seed the customer's matrix cells from the file where they are missing; how many were added.

    (new customer) -> 48 ; (seeded already) -> 0 ; (one new capability in the file) -> 6
    """
    m = matrix_file.load()
    existing = {
        (row.role_code, row.capability_code)
        for row in (await db.execute(
            select(AccessRoleCapability).where(AccessRoleCapability.customer_code == customer_code)
        )).scalars()
    }
    added = 0
    for (role_code, capability_code), state in m.defaults.items():
        if (role_code, capability_code) not in existing:
            db.add(AccessRoleCapability(customer_code=customer_code, role_code=role_code,
                                        capability_code=capability_code, state=state))
            added += 1
    if added:
        await db.flush()
    return added


async def customer_matrix(db: AsyncSession, customer_code: str) -> dict[tuple[str, str], str]:
    """The customer's cells, seeded first if need be: (role, capability) -> state."""
    await ensure_matrix(db, customer_code)
    rows = (await db.execute(
        select(AccessRoleCapability).where(AccessRoleCapability.customer_code == customer_code)
    )).scalars()
    return {(r.role_code, r.capability_code): r.state for r in rows}


async def record_for_user(db: AsyncSession, user: TeamsUser) -> AccessRecord:
    """The access record of one user row: the matrix for their role with their overrides on top."""
    m = matrix_file.load()
    cells = await customer_matrix(db, user.customer_code)
    overrides = {
        row.capability_code: row.state
        for row in (await db.execute(
            select(TeamsUserCapability).where(TeamsUserCapability.user_id == user.id)
        )).scalars()
    }
    capabilities = {
        code: overrides.get(code, cells.get((user.role_code, code), NO)) for code in m.capability_codes
    }
    sites = tuple(sorted(
        row.site_id
        for row in (await db.execute(
            select(TeamsUserSite).where(TeamsUserSite.user_id == user.id)
        )).scalars()
    ))
    role_name = next((r.name for r in m.roles if r.code == user.role_code), user.role_code)
    return AccessRecord(
        customer_code=user.customer_code, object_id=user.object_id,
        display_name=user.display_name, role_code=user.role_code, role_name=role_name,
        status=user.status, all_sites=user.all_sites, site_ids=sites,
        capabilities=capabilities, last_active_at=user.last_active_at,
    )


async def access_record_for(db: AsyncSession, customer_code: str, object_id: str) -> AccessRecord | None:
    """One person's record by their Entra object id, or None when they have no user row.

    ("acme", "u-7" with a row) -> AccessRecord ; ("acme", "nobody") -> None
    """
    user = (await db.execute(
        select(TeamsUser).where(TeamsUser.customer_code == customer_code,
                                TeamsUser.object_id == object_id)
    )).scalar_one_or_none()
    if user is None:
        return None
    return await record_for_user(db, user)


async def set_matrix_cell(db: AsyncSession, customer_code: str, role_code: str, capability_code: str,
                          state: str, *, actor) -> None:
    """Change one cell of a customer's matrix (an Admin's click in the app); audited.

    ("acme", "supervisor", "export", "allowed") -> the cell, an audit line ; (unknown role) -> ValueError
    """
    from app.services.access import audit  # local: audit imports nothing from here
    from app.services.access.matrix import STATES

    m = matrix_file.load()
    if role_code not in m.role_codes:
        raise ValueError(f"unknown role {role_code!r}")
    if capability_code not in m.capability_codes:
        raise ValueError(f"unknown capability {capability_code!r}")
    if state not in STATES:
        raise ValueError(f"unknown state {state!r}")
    await ensure_matrix(db, customer_code)
    row = (await db.execute(
        select(AccessRoleCapability).where(AccessRoleCapability.customer_code == customer_code,
                                           AccessRoleCapability.role_code == role_code,
                                           AccessRoleCapability.capability_code == capability_code)
    )).scalar_one()
    before = row.state
    row.state, row.updated_by = state, actor.object_id
    await db.flush()
    await audit.record_event(
        db, customer_code, actor_object_id=actor.object_id, actor_name=actor.name,
        actor_kind=actor.kind, action="access.matrix_changed", capability_code=capability_code,
        decision="allowed", subject_kind="role", subject_id=role_code,
        detail={"before": before, "after": state},
        text=f"{actor.name or actor.object_id} set {capability_code} for {role_code}: {before} → {state}")
