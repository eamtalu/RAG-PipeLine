# tenant_secret.py - a tenant's secret values, sealed at rest under envelope encryption
#
#   One row per secret: a webhook URL today, a connector credential later. The value is encrypted
#   with a random data key, and the data key is wrapped with a master key from settings whose id is
#   stored beside it, so the master key can be rotated by re-wrapping the data keys without
#   touching a single ciphertext (app/services/secrets/envelope.py).
#
#   `owner` says what the secret belongs to ("notification_channel:<id>"), `kind` what it is
#   ("webhook_url"); the pair is what a caller replaces when a new value is set. customer_code is
#   the soft tenant key and every read requires it: a secret is never revealed to another tenant.

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, LargeBinary, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class TenantSecret(Base):
    __tablename__ = "tenant_secrets"
    __table_args__ = (
        # replace-by-owner and delete-by-owner read "this tenant's secrets for this thing"
        Index("ix_tenant_secrets_customer_owner", "customer_code", "owner"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)  # "webhook_url"
    owner: Mapped[str] = mapped_column(String(128), nullable=False)  # "notification_channel:<id>"
    key_id: Mapped[str] = mapped_column(String(32), nullable=False)  # which master key wraps it
    wrapped_key: Mapped[bytes] = mapped_column(
        LargeBinary, nullable=False
    )  # nonce + wrapped data key
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)  # nonce + sealed value
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    rotated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
