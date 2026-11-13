"""Access control admin API (stage D, step 7): the superadmin's door to a customer's users and
matrix. Admin-only (the ops service token), as the tenant bindings are.

Postgres is the record; every write is audited in the same transaction and the customer's access
rows are mirrored to the edge at once (a failed mirror does not fail the write: the consumer's
sweep rewrites the rows within a minute). This is also how the first Admin of a customer is made
at initialisation; after that the Admin tab does the rest through commands.
"""

from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import normalize_customer_code, require_admin
from app.config.database import get_session
from app.persistence.repositories.customer_repository import CustomerRepository, get_customer_repository
from app.services.access import mirror as access_mirror
from app.services.access import records, users
from app.services.access.matrix import STATES
from app.services.teams.mirror import MirrorWriter, build_mirror_writer_from_settings

router = APIRouter(prefix="/teams/access", tags=["teams"])
logger = logging.getLogger(__name__)


def get_access_mirror() -> MirrorWriter | None:
    return build_mirror_writer_from_settings()


class UserRequest(BaseModel):
    display_name: str | None = Field(default=None, max_length=128)
    email: str | None = Field(default=None, max_length=256)
    role: str | None = Field(default=None, max_length=32)
    all_sites: bool | None = None
    site_ids: list[str] | None = None
    capabilities: dict[str, str] | None = Field(
        default=None, description="Per-user overrides, capability code -> allowed|with_approval|no")
    status: str | None = Field(default=None, max_length=16)
    starts_on: date | None = None
    note: str | None = Field(default=None, max_length=300)
    actor: str | None = Field(default=None, max_length=128, description="Who is making the change, for the audit line.")

    def as_change(self) -> users.UserChange:
        return users.UserChange(
            display_name=self.display_name, email=self.email, role_code=self.role,
            all_sites=self.all_sites,
            site_ids=tuple(self.site_ids) if self.site_ids is not None else None,
            capabilities=self.capabilities, status=self.status, starts_on=self.starts_on,
            note=self.note)


class MatrixCellRequest(BaseModel):
    state: str = Field(..., pattern="^(allowed|with_approval|no)$")
    actor: str | None = Field(default=None, max_length=128)


async def _customer(code: str, customers: CustomerRepository) -> str:
    code = normalize_customer_code(code)
    if not await customers.exists(code):
        raise HTTPException(404, detail=f"customer {code!r} does not exist")
    return code


async def _mirror(db: AsyncSession, mirror: MirrorWriter | None, customer_code: str) -> bool:
    if mirror is None:
        return False
    try:
        await access_mirror.mirror_customer(db, mirror, customer_code)
        return True
    except Exception:
        logger.warning("access rows for %s not mirrored; the sweep will retry", customer_code,
                       exc_info=True)
        return False


@router.get("/{customer_code}/users")
async def list_users(customer_code: str, _admin: None = Depends(require_admin),
                     db: AsyncSession = Depends(get_session),
                     customers: CustomerRepository = Depends(get_customer_repository)):
    code = await _customer(customer_code, customers)
    out = []
    for user in await users.list_users(db, code):
        record = await records.record_for_user(db, user)
        out.append({**users.user_payload(user, list(record.site_ids)), "capabilities": record.capabilities})
    return {"customer_code": code, "users": out}


@router.put("/{customer_code}/users/{object_id}")
async def put_user(customer_code: str, object_id: str, body: UserRequest,
                   _admin: None = Depends(require_admin), db: AsyncSession = Depends(get_session),
                   customers: CustomerRepository = Depends(get_customer_repository),
                   mirror: MirrorWriter | None = Depends(get_access_mirror)):
    """Create the user when new, else change them; the first Admin of a customer is made here.

    (new object id, role admin, all_sites) -> 200 the record, mirrored ; (unknown role) -> 400
    """
    code = await _customer(customer_code, customers)
    actor = users.Actor.system(body.actor or "superadmin")
    try:
        if await users.get_user(db, code, object_id) is None:
            user = await users.create_user(db, code, object_id, change=body.as_change(), actor=actor)
        else:
            user = await users.update_user(db, code, object_id, change=body.as_change(), actor=actor)
        await db.commit()
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(400, detail=str(exc))
    record = await records.record_for_user(db, user)
    mirrored = await _mirror(db, mirror, code)
    await db.commit()
    return {**record.as_payload(), "mirrored": mirrored}


@router.get("/{customer_code}/matrix")
async def get_matrix(customer_code: str, _admin: None = Depends(require_admin),
                     db: AsyncSession = Depends(get_session),
                     customers: CustomerRepository = Depends(get_customer_repository)):
    code = await _customer(customer_code, customers)
    payload = await access_mirror.roles_payload(db, code)
    await db.commit()  # the seed, when this is the first read
    return payload


@router.put("/{customer_code}/matrix/{role}/{capability}")
async def put_matrix_cell(customer_code: str, role: str, capability: str, body: MatrixCellRequest,
                          _admin: None = Depends(require_admin),
                          db: AsyncSession = Depends(get_session),
                          customers: CustomerRepository = Depends(get_customer_repository),
                          mirror: MirrorWriter | None = Depends(get_access_mirror)):
    """Change one cell of the customer's matrix; audited and mirrored."""
    code = await _customer(customer_code, customers)
    try:
        await records.set_matrix_cell(db, code, role, capability, body.state,
                                      actor=users.Actor.system(body.actor or "superadmin"))
        await db.commit()
    except ValueError as exc:
        await db.rollback()
        raise HTTPException(400, detail=str(exc))
    mirrored = await _mirror(db, mirror, code)
    await db.commit()
    return {"role": role, "capability": capability, "state": body.state, "states": list(STATES),
            "mirrored": mirrored}
