# teams_conversation_turn.py — conversation memory for the Teams bot
#
#   The debugging agent answers one question at a time. A Teams chat is a thread of follow-ups
#   ("and for yesterday?"), so the consumer stores each question and answer here and replays the last
#   few turns to the agent. Keyed by the Teams conversation id, which Microsoft assigns and the edge
#   passes through unchanged in every job.
#
#   customer_code is stamped on every row (soft tenant key, like the log tables) so a purge of a log
#   space can remove its conversations, and so a turn can never be replayed into another tenant's
#   question even if two tenants ever shared a conversation id.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, SmallInteger, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base

ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"


class TeamsConversationTurn(Base):
    __tablename__ = "teams_conversation_turns"
    __table_args__ = (
        # the read is always "last N turns of one conversation", newest first
        Index("ix_teams_conversation_turns_conv_created", "conversation_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id: Mapped[str] = mapped_column(String(256), nullable=False)
    customer_code: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    role: Mapped[str] = mapped_column(String(16), nullable=False)  # "user" | "assistant"
    # Order within one exchange: the question and its answer are written in the same instant, so the
    # timestamp alone cannot order them. 0 = the question, 1 = the answer.
    position: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0, server_default="0")
    content: Mapped[str] = mapped_column(Text, nullable=False)
    job_id: Mapped[str | None] = mapped_column(String(64), nullable=True)  # edge job that produced it
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
