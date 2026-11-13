# teams_conversation.py — one thread of the eSmart Eye Teams app's Ask history (stage D, step 6b)
#
#   The page starts a conversation whenever the person presses "New conversation"; every question
#   in it carries the same conversation id, the agent replays the last turns of that id only, and
#   the history rail lists conversations (titled by their first question) rather than questions.
#   The turns themselves stay in teams_conversation_turns; this row is the thread's header.
#
#   customer_code is the soft tenant key, as on the turns; sender_object_id is who owns it.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class TeamsConversation(Base):
    __tablename__ = "teams_conversations"
    __table_args__ = (
        UniqueConstraint("customer_code", "conversation_id", name="uq_teams_conversations_conv"),
        # the history read: one person's conversations within the tenant, most recent first
        Index("ix_teams_conversations_user_last", "customer_code", "sender_object_id", "last_turn_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    conversation_id: Mapped[str] = mapped_column(String(256), nullable=False)
    sender_object_id: Mapped[str] = mapped_column(String(128), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)  # the first question, cut
    site_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_turn_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    turn_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
