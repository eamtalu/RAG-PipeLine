"""Conversation memory for the Teams bot: bounded, per conversation, per tenant."""

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.repositories.teams_repository import TeamsConversationRepository


class ConversationMemory:
    def __init__(self, db: AsyncSession, *, max_turns: int):
        self._repo = TeamsConversationRepository(db)
        self._max_turns = max_turns

    async def history(self, conversation_id: str, customer_code: str) -> list[dict]:
        """The last turns as Anthropic-shaped messages, oldest first, at most max_turns entries."""
        return await self._repo.recent_turns(conversation_id, customer_code, limit=self._max_turns)

    async def record(self, conversation_id: str, customer_code: str, *, question: str, answer: str,
                     job_id: str | None) -> None:
        await self._repo.append_exchange(conversation_id, customer_code, question=question,
                                         answer=answer, job_id=job_id)
