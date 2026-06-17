"""add SSH remote-source tables (pull-ingestion from the Windows Server)

Revision ID: d7f1a2c3b4e5
Revises: f8b3c1d24a90
Create Date: 2026-06-17

Adds the per-customer (many-per-tenant) SSH source config, the per-source/per-remote-file
incremental byte cursor, and the async fetch-run status record. Mirrors the existing
log_regroup_runs pattern for the async run tracking.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "d7f1a2c3b4e5"
down_revision = "f8b3c1d24a90"
branch_labels = None
depends_on = None

ssh_fetch_run_status = sa.Enum("running", "completed", "failed", name="logsshfetchrunstatus")
ssh_fetch_mode = sa.Enum("incremental", "timestamp", "full", name="logsshfetchmode")


def upgrade() -> None:
    op.create_table(
        "log_ssh_sources",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False, index=True),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("host", sa.String(255), nullable=False),
        sa.Column("port", sa.Integer, nullable=False, server_default="22"),
        sa.Column("username", sa.String(255), nullable=False),
        sa.Column("private_key_path", sa.String(1024), nullable=True),
        sa.Column("private_key_enc", sa.Text, nullable=True),
        sa.Column("key_passphrase_enc", sa.Text, nullable=True),
        sa.Column("host_key_fingerprint", sa.String(255), nullable=True),
        sa.Column("remote_log_dir", sa.String(1024), nullable=False),
        sa.Column("file_glob", sa.String(255), nullable=False, server_default="*.log"),
        sa.Column("enabled", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("poll_interval_seconds", sa.Float, nullable=True),
        sa.Column("last_ok_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "name", name="uq_log_ssh_sources_customer_name"),
    )

    op.create_table(
        "log_ssh_file_checkpoints",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("source_id", UUID(as_uuid=True),
                  sa.ForeignKey("log_ssh_sources.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("customer_code", sa.String(64), nullable=False, index=True),
        sa.Column("remote_path", sa.String(1024), nullable=False),
        sa.Column("last_size", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("last_mtime", sa.Float, nullable=False, server_default="0"),
        sa.Column("last_offset", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("last_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("source_id", "remote_path", name="uq_log_ssh_file_ckpt_source_path"),
    )

    op.create_table(
        "log_ssh_fetch_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False, index=True),
        sa.Column("source_id", UUID(as_uuid=True), nullable=True, index=True),
        sa.Column("mode", ssh_fetch_mode, nullable=False),
        sa.Column("requested_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", ssh_fetch_run_status, nullable=False, index=True),
        sa.Column("files_considered", sa.Integer, nullable=True),
        sa.Column("files_fetched", sa.Integer, nullable=True),
        sa.Column("bytes_fetched", sa.BigInteger, nullable=True),
        sa.Column("entries_ingested", sa.Integer, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("result", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("log_ssh_fetch_runs")
    op.drop_table("log_ssh_file_checkpoints")
    op.drop_table("log_ssh_sources")
    ssh_fetch_run_status.drop(op.get_bind(), checkfirst=True)
    ssh_fetch_mode.drop(op.get_bind(), checkfirst=True)
