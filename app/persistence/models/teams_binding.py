# teams_binding.py — which customer log space a Microsoft Teams tenant maps to
#
#   The Teams bot is one registration serving many customer organisations. Every message it receives
#   carries the sender's Entra tenant id, set by Microsoft and therefore trustworthy. This table turns
#   that id into a customer_code, which is the only key the agent and its tools understand.
#
#   Postgres is the source of truth. The bot edge on AWS reads a mirrored copy from DynamoDB so it never
#   has to reach this server; `mirrored_at` records when the row last reached the edge, and NULL (or an
#   updated_at newer than mirrored_at) means the consumer process must push it again.
#
#   customer_code is a real foreign key: a binding to a log space that does not exist is a config error
#   and must fail loudly at write time, not silently at question time.

import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class TeamsTenantBinding(Base):
    __tablename__ = "teams_tenant_bindings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # Entra tenant id (a GUID). One binding per tenant: one default log space per customer.
    tenant_id: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    customer_code: Mapped[str] = mapped_column(
        String(64), ForeignKey("customers.customer_code", ondelete="CASCADE"), index=True, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true", nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # When this row last reached the edge's DynamoDB copy. NULL = never; older than updated_at = stale.
    mirrored_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    @property
    def needs_mirror(self) -> bool:
        return self.mirrored_at is None or self.mirrored_at < self.updated_at
