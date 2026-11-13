"""Repositories for the Teams bot: tenant bindings and conversation memory (and, since stage D
step 6, the Ask history read from the same turns)."""

from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Depends
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.models.teams_conversation import TeamsConversation
from app.persistence.models.teams_conversation_turn import ROLE_ASSISTANT, ROLE_USER, TeamsConversationTurn


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ExchangeDetails:
    """What the Ask history keeps about an exchange beyond its text (step 6)."""

    sender_object_id: str
    site_id: str | None
    blocks: list[dict]
    asked_at: datetime | None
    answered_at: datetime | None
    data_as_of: datetime | None
    provisional: bool


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
                              question: str, answer: str, job_id: str | None,
                              details: "ExchangeDetails | None" = None) -> None:
        """Store one question and its answer as two rows written in the same instant.

        (conv, "acme", question="q", answer="a", job_id="j1") -> rows user q (0), assistant a (1) ;
        with details -> the answer row also carries who asked, the site, blocks and the times
        """
        now = _now()
        who = dict(sender_object_id=details.sender_object_id, site_id=details.site_id) if details else {}
        what = (dict(blocks=details.blocks, asked_at=details.asked_at, answered_at=details.answered_at,
                     data_as_of=details.data_as_of, provisional=details.provisional) if details else {})
        self.db.add_all([
            TeamsConversationTurn(conversation_id=conversation_id, customer_code=customer_code,
                                  role=ROLE_USER, position=0, content=question, job_id=job_id, created_at=now,
                                  **who),
            TeamsConversationTurn(conversation_id=conversation_id, customer_code=customer_code,
                                  role=ROLE_ASSISTANT, position=1, content=answer, job_id=job_id, created_at=now,
                                  **who, **what),
        ])
        await self.db.commit()

    async def user_exchanges(self, customer_code: str, sender_object_id: str, *, since: datetime,
                             limit: int) -> list[tuple[TeamsConversationTurn, TeamsConversationTurn]]:
        """One person's exchanges since a moment, newest first: (question row, answer row) pairs.

        (acme, u-7, since 30 days ago, limit 100) -> [(q row, a row), ...] ; (nothing asked) -> []
        """
        if limit <= 0:
            return []
        # Step 1: Read the answer rows first, because they are one per exchange and carry the
        # history fields; the limit bounds the exchanges, not the rows
        answers = (select(TeamsConversationTurn)
                   .where(TeamsConversationTurn.customer_code == customer_code,
                          TeamsConversationTurn.sender_object_id == sender_object_id,
                          TeamsConversationTurn.role == ROLE_ASSISTANT,
                          TeamsConversationTurn.created_at >= since)
                   .order_by(TeamsConversationTurn.created_at.desc())
                   .limit(limit))
        answer_rows = list((await self.db.execute(answers)).scalars())
        if not answer_rows:
            return []
        # Step 2: Fetch the matching questions by job id in one query, so an exchange whose
        # question row is missing (never happens, but) is skipped rather than guessed
        job_ids = [a.job_id for a in answer_rows if a.job_id]
        questions = (select(TeamsConversationTurn)
                     .where(TeamsConversationTurn.customer_code == customer_code,
                            TeamsConversationTurn.role == ROLE_USER,
                            TeamsConversationTurn.job_id.in_(job_ids)))
        by_job = {q.job_id: q for q in (await self.db.execute(questions)).scalars()}
        return [(by_job[a.job_id], a) for a in answer_rows if a.job_id in by_job]

    async def touch_conversation(self, customer_code: str, conversation_id: str, *,
                                 sender_object_id: str, title: str, site_id: str | None,
                                 now: datetime) -> TeamsConversation:
        """Create the conversation on its first turn, else bump its last turn and count.

        (new id, "How many?") -> row titled "How many?", 1 turn ; (again) -> 2 turns, later last_turn_at
        """
        row = (await self.db.execute(
            select(TeamsConversation).where(TeamsConversation.customer_code == customer_code,
                                            TeamsConversation.conversation_id == conversation_id)
        )).scalar_one_or_none()
        if row is None:
            row = TeamsConversation(customer_code=customer_code, conversation_id=conversation_id,
                                    sender_object_id=sender_object_id, title=title[:200],
                                    site_id=site_id, started_at=now, last_turn_at=now, turn_count=1)
            self.db.add(row)
        else:
            row.last_turn_at = now
            row.turn_count = (row.turn_count or 0) + 1
        await self.db.commit()
        return row

    async def user_conversations(self, customer_code: str, sender_object_id: str, *, since: datetime,
                                 limit: int) -> list[TeamsConversation]:
        """One person's conversations with a turn since the moment, most recent first."""
        if limit <= 0:
            return []
        stmt = (select(TeamsConversation)
                .where(TeamsConversation.customer_code == customer_code,
                       TeamsConversation.sender_object_id == sender_object_id,
                       TeamsConversation.last_turn_at >= since)
                .order_by(TeamsConversation.last_turn_at.desc())
                .limit(limit))
        return list((await self.db.execute(stmt)).scalars())

    async def purge_conversations_before(self, cutoff: datetime) -> int:
        """Delete the headers of conversations whose last turn is older than the cutoff."""
        result = await self.db.execute(
            delete(TeamsConversation).where(TeamsConversation.last_turn_at < cutoff))
        await self.db.commit()
        return result.rowcount or 0

    async def purge_older_than(self, cutoff: datetime, *, batch_size: int) -> int:
        """Delete turns created before the cutoff, at most one batch, and say how many went.

        (cutoff 30 days ago, 1,000 old rows, batch 500) -> 500 ; (nothing older) -> 0
        """
        oldest = (select(TeamsConversationTurn.id)
                  .where(TeamsConversationTurn.created_at < cutoff)
                  .order_by(TeamsConversationTurn.created_at)
                  .limit(batch_size))
        result = await self.db.execute(
            delete(TeamsConversationTurn).where(TeamsConversationTurn.id.in_(oldest)))
        await self.db.commit()
        return result.rowcount or 0

    async def purge_customer(self, customer_code: str) -> int:
        result = await self.db.execute(
            delete(TeamsConversationTurn).where(TeamsConversationTurn.customer_code == customer_code))
        await self.db.commit()
        return result.rowcount or 0


def get_teams_binding_repository(db: AsyncSession = Depends(get_session)) -> TeamsBindingRepository:
    return TeamsBindingRepository(db)
