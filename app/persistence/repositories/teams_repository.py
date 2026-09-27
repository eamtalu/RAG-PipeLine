"""Repositories for the Teams bot: tenant bindings and conversation memory."""

from datetime import datetime, timezone

from fastapi import Depends
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.models.teams_conversation_turn import ROLE_ASSISTANT, ROLE_USER, TeamsConversationTurn


def _now() -> datetime:
    return datetime.now(timezone.utc)


class TeamsBindingRepository:
    """Source of truth for tenant -> customer_code. The edge reads a mirrored copy."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_tenant(self, tenant_id: str) -> TeamsTenantBinding | None:
        return await self.db.scalar(
            select(TeamsTenantBinding).where(TeamsTenantBinding.tenant_id == tenant_id))

    async def list_all(self) -> list[TeamsTenantBinding]:
        stmt = select(TeamsTenantBinding).order_by(TeamsTenantBinding.created_at.asc())
        return list((await self.db.execute(stmt)).scalars().all())

    async def list_needing_mirror(self) -> list[TeamsTenantBinding]:
        """Rows the edge has never seen, or has seen an older version of."""
        stmt = select(TeamsTenantBinding).where(
            (TeamsTenantBinding.mirrored_at.is_(None))
            | (TeamsTenantBinding.mirrored_at < TeamsTenantBinding.updated_at))
        return list((await self.db.execute(stmt)).scalars().all())

    async def upsert(self, tenant_id: str, customer_code: str, *, enabled: bool = True,
                     display_name: str | None = None, created_by: str | None = None) -> TeamsTenantBinding:
        """Create or replace the binding for a tenant. Any change clears mirrored_at implicitly
        because updated_at moves forward and `needs_mirror` compares the two."""
        row = await self.get_by_tenant(tenant_id)
        if row is None:
            row = TeamsTenantBinding(tenant_id=tenant_id, customer_code=customer_code, enabled=enabled,
                                     display_name=display_name, created_by=created_by)
            self.db.add(row)
        else:
            row.customer_code = customer_code
            row.enabled = enabled
            row.display_name = display_name
            row.updated_at = _now()
        await self.db.commit()
        await self.db.refresh(row)
        return row

    async def mark_mirrored(self, tenant_id: str) -> None:
        """Record that the edge now holds this version of the row.

        Stamps mirrored_at with the row's OWN updated_at, as one SQL statement, and names updated_at
        explicitly so the column's onupdate hook does not move it forward: a mirror confirmation is
        not an edit, and if it bumped updated_at every row would read as stale forever.
        """
        await self.db.execute(
            update(TeamsTenantBinding)
            .where(TeamsTenantBinding.tenant_id == tenant_id)
            .values(mirrored_at=TeamsTenantBinding.updated_at, updated_at=TeamsTenantBinding.updated_at))
        await self.db.commit()

    async def delete(self, tenant_id: str) -> bool:
        result = await self.db.execute(
            delete(TeamsTenantBinding).where(TeamsTenantBinding.tenant_id == tenant_id))
        await self.db.commit()
        return (result.rowcount or 0) > 0


class TeamsConversationRepository:
    """Last N turns of one Teams conversation, oldest first, ready for the agent."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def recent_turns(self, conversation_id: str, customer_code: str, *, limit: int) -> list[dict]:
        if limit <= 0:
            return []
        stmt = (select(TeamsConversationTurn.role, TeamsConversationTurn.content)
                .where(TeamsConversationTurn.conversation_id == conversation_id,
                       TeamsConversationTurn.customer_code == customer_code)
                .order_by(TeamsConversationTurn.created_at.desc(), TeamsConversationTurn.position.desc())
                .limit(limit))
        rows = (await self.db.execute(stmt)).all()
        return [{"role": role, "content": content} for role, content in reversed(rows)]

    async def append_exchange(self, conversation_id: str, customer_code: str, *,
                              question: str, answer: str, job_id: str | None) -> None:
        now = _now()
        self.db.add_all([
            TeamsConversationTurn(conversation_id=conversation_id, customer_code=customer_code,
                                  role=ROLE_USER, position=0, content=question, job_id=job_id, created_at=now),
            TeamsConversationTurn(conversation_id=conversation_id, customer_code=customer_code,
                                  role=ROLE_ASSISTANT, position=1, content=answer, job_id=job_id, created_at=now),
        ])
        await self.db.commit()

    async def purge_customer(self, customer_code: str) -> int:
        result = await self.db.execute(
            delete(TeamsConversationTurn).where(TeamsConversationTurn.customer_code == customer_code))
        await self.db.commit()
        return result.rowcount or 0


def get_teams_binding_repository(db: AsyncSession = Depends(get_session)) -> TeamsBindingRepository:
    return TeamsBindingRepository(db)
