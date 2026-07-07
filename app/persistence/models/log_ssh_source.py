# log_ssh_source.py — A Windows Server SSH/SFTP source for remote log pull-ingestion
#
#   A customer can have ONE OR MORE Windows Servers (each running OpenSSH); each is one row,
#   identified within the tenant by a `name` label. A row holds host/port/user, the private key to
#   authenticate with, and the remote log directory + glob to pull. The poller
#   (services/workers/ssh_log_fetcher) and the on-demand POST /logs/fetch-remote both read these.
#
#   SECRETS: prefer `private_key_path` — a path to a key file ON THE BACKEND HOST, so no private
#   material ever lands in the DB. If inline material is unavoidable, it is stored Fernet-encrypted
#   in `private_key_enc` / `key_passphrase_enc` (see services/.../remote/secrets.py); the API never
#   serializes any of these fields back out.

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Integer, Boolean, DateTime, Text, Float, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class LogSshSource(Base):
    __tablename__ = "log_ssh_sources"

    # a tenant may register several servers, so customer_code is NOT unique — it's unique together
    # with the human-given `name` (e.g. "prod-wms-1"), which is how a source is addressed in the API.
    __table_args__ = (
        UniqueConstraint("customer_code", "name", name="uq_log_ssh_sources_customer_name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # the X-Customer-Code this source belongs to (one customer → many sources).
    customer_code: Mapped[str] = mapped_column(String(64), index=True)
    # tenant-local label distinguishing this server from the customer's others.
    name: Mapped[str] = mapped_column(String(128))

    # --- connection ---
    host: Mapped[str] = mapped_column(String(255))
    port: Mapped[int] = mapped_column(Integer, default=22)
    username: Mapped[str] = mapped_column(String(255))
    # key-file path on the backend host (preferred); OR inline encrypted material.
    private_key_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    private_key_enc: Mapped[str | None] = mapped_column(Text, nullable=True)
    key_passphrase_enc: Mapped[str | None] = mapped_column(Text, nullable=True)
    # pinned on first successful connect; a later mismatch is rejected (MITM guard).
    host_key_fingerprint: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # --- what to pull ---
    # remote path is POSIX-style over SFTP even on Windows OpenSSH, e.g. "C:/logs/m3".
    remote_log_dir: Mapped[str] = mapped_column(String(1024))
    file_glob: Mapped[str] = mapped_column(String(255), default="*.log")

    # --- poller ---
    enabled: Mapped[bool] = mapped_column(Boolean, default=False)  # drives the background poller
    poll_interval_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)  # else global

    # --- bookkeeping ---
    last_ok_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # --- outage / circuit breaker + computed status (see design doc §4.5, §9.6) ---
    # last time a fetch was ATTEMPTED (success or failure); last_ok_at remains last SUCCESS.
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # consecutive failed poller fetches; reset to 0 on any success. Drives the auto-disable breaker.
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    # set when the breaker flips enabled=False after a sustained outage (null = operator disable / never).
    auto_disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
