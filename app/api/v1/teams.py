"""Teams bot admin API: which Microsoft 365 tenant maps to which log space.

Admin-only. Postgres is the source of truth; every write is mirrored to the edge's DynamoDB table so
the bot can answer "is this tenant onboarded?" without calling this server. A failed mirror does not
fail the write: the row is saved, reported as `mirrored=false`, and the consumer process re-pushes
it on its next sweep.
"""

import logging
import re

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.deps import normalize_customer_code, require_admin
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.repositories.customer_repository import CustomerRepository, get_customer_repository
from app.persistence.repositories.teams_repository import TeamsBindingRepository, get_teams_binding_repository
from app.services.teams.binding_mirror import BindingMirror, build_mirror_from_settings

router = APIRouter(prefix="/teams", tags=["teams"])
logger = logging.getLogger(__name__)

_TENANT_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def get_binding_mirror() -> BindingMirror:
    return build_mirror_from_settings()


class BindingRequest(BaseModel):
    customer_code: str = Field(..., description="Log space this tenant's questions are answered from.")
    enabled: bool = Field(default=True, description="False switches the bot off for this tenant "
                                                     "without losing the mapping.")
    display_name: str | None = Field(default=None, max_length=128, description="Customer name, for logs.")
    created_by: str | None = Field(default=None, max_length=128)


def _serialize(b: TeamsTenantBinding, *, mirrored: bool | None = None) -> dict:
    return {
        "tenant_id": b.tenant_id,
        "customer_code": b.customer_code,
        "enabled": b.enabled,
        "display_name": b.display_name,
        "created_by": b.created_by,
        "mirrored": (not b.needs_mirror) if mirrored is None else mirrored,
        "mirrored_at": b.mirrored_at.isoformat() if b.mirrored_at else None,
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "updated_at": b.updated_at.isoformat() if b.updated_at else None,
    }


def _validate_tenant(tenant_id: str) -> str:
    tenant_id = (tenant_id or "").strip().lower()
    if not _TENANT_RE.match(tenant_id):
        raise HTTPException(400, detail="tenant_id must be an Entra tenant GUID.")
    return tenant_id


@router.get("/bindings")
async def list_bindings(_admin: None = Depends(require_admin),
                        repo: TeamsBindingRepository = Depends(get_teams_binding_repository)):
    return {"bindings": [_serialize(b) for b in await repo.list_all()]}


@router.put("/bindings/{tenant_id}")
async def upsert_binding(tenant_id: str, body: BindingRequest,
                         _admin: None = Depends(require_admin),
                         repo: TeamsBindingRepository = Depends(get_teams_binding_repository),
                         customers: CustomerRepository = Depends(get_customer_repository),
                         mirror: BindingMirror = Depends(get_binding_mirror)):
    tenant_id = _validate_tenant(tenant_id)
    code = normalize_customer_code(body.customer_code)
    if code is None:
        raise HTTPException(400, detail="Invalid customer_code (expected a slug like 'acme').")
    if not await customers.exists(code):
        raise HTTPException(404, detail=f"Unknown customer: {code!r}. Create its log space first.")

    row = await repo.upsert(tenant_id, code, enabled=body.enabled,
                            display_name=(body.display_name or "").strip() or None,
                            created_by=(body.created_by or "").strip() or None)
    mirrored = await _try_mirror(mirror, repo, row)
    return _serialize(row, mirrored=mirrored)


@router.delete("/bindings/{tenant_id}", status_code=200)
async def delete_binding(tenant_id: str, _admin: None = Depends(require_admin),
                         repo: TeamsBindingRepository = Depends(get_teams_binding_repository),
                         mirror: BindingMirror = Depends(get_binding_mirror)):
    tenant_id = _validate_tenant(tenant_id)
    if not await repo.delete(tenant_id):
        raise HTTPException(404, detail="No binding for that tenant.")
    try:
        await mirror.remove(tenant_id)
        removed_from_edge = True
    except Exception:
        # The row is gone here. The edge would keep answering until the sweep notices, so say so.
        logger.warning("binding %s deleted locally but not removed from the edge", tenant_id, exc_info=True)
        removed_from_edge = False
    return {"tenant_id": tenant_id, "deleted": True, "removed_from_edge": removed_from_edge}


async def _try_mirror(mirror: BindingMirror, repo: TeamsBindingRepository, row: TeamsTenantBinding) -> bool:
    try:
        await mirror.put(row)
    except Exception:
        logger.warning("binding %s saved but not mirrored to the edge; the consumer sweep will retry",
                       row.tenant_id, exc_info=True)
        return False
    await repo.mark_mirrored(row.tenant_id)
    return True
