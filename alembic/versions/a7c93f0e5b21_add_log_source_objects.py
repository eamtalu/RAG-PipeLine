"""log_source_objects: durable handoff between SSH fetching and Stage 1 parsing

Revision ID: a7c93f0e5b21
Revises: f3b8d1e07c92
Create Date: 2026-08-05

Creates the queue/provenance table that lets the fetcher stop parsing inline. The fetcher inserts a
row and advances log_ssh_file_checkpoints in ONE transaction, then releases the SSH connection and
the per-host advisory lock; a separate worker leases the row and runs Stage 1.

This migration is purely additive - one new table plus three indexes. It touches no existing table,
so there is no rewrite of the ~40 GB log_entries heap and nothing reads or writes the new table
until settings.log_parse_worker_enabled is turned on. Safe to ship on its own.

The claim and lease indexes are PARTIAL so they only cover live work: a queue that has processed
millions of rows still has a tiny index for the handful that are pending or leased.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a7c93f0e5b21"
down_revision = "f3b8d1e07c92"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "log_source_objects",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("customer_code", sa.String(length=64), nullable=False),

        # provenance — source_id is deliberately NOT a foreign key: deleting an SSH source must
        # never delete ingestion evidence (same precedent as log_ssh_fetch_runs.source_id).
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("source_name", sa.String(length=255), nullable=False),
        sa.Column("remote_path", sa.String(length=1024), nullable=False),
        sa.Column("start_offset", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("end_offset", sa.BigInteger(), nullable=False),
        sa.Column("observed_size", sa.BigInteger(), nullable=True),
        sa.Column("observed_mtime", sa.Float(), nullable=True),
        sa.Column("head_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("content_sha256", sa.String(length=64), nullable=True),

        sa.Column("storage_key", sa.String(length=1024), nullable=False),

        # queue state
        sa.Column("status", sa.String(length=24), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False,
                  # clock_timestamp(), not now(): this column is written AND compared by the
                  # database clock, so no app-host clock skew can make a row look not-yet-due.
                  server_default=sa.text("clock_timestamp()")),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),

        # outcome — job_id is a soft reference and transitional; it goes away when `jobs` is retired
        # from the log path during the partitioning work.
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("entries_inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("file_deleted_at", sa.DateTime(timezone=True), nullable=True),

        sa.CheckConstraint("status IN ('pending','leased','ingested','abandoned')",
                           name="ck_log_source_objects_status"),
        sa.CheckConstraint("end_offset >= start_offset", name="ck_log_source_objects_offsets"),
        sa.CheckConstraint("attempts >= 0", name="ck_log_source_objects_attempts"),
        sa.CheckConstraint("max_attempts > 0", name="ck_log_source_objects_max_attempts"),
    )

    op.create_index("ix_log_source_objects_customer_code", "log_source_objects", ["customer_code"])
    op.create_index("ix_log_source_objects_status", "log_source_objects", ["status"])

    # partial: only live work is indexed, so the claim stays cheap however large the history grows.
    op.create_index("ix_log_source_objects_claim", "log_source_objects",
                    ["available_at", "created_at"],
                    postgresql_where=sa.text("status = 'pending'"))
    op.create_index("ix_log_source_objects_customer", "log_source_objects",
                    ["customer_code", "created_at"])
    op.create_index("ix_log_source_objects_lease", "log_source_objects", ["lease_expires_at"],
                    postgresql_where=sa.text("status = 'leased'"))


def downgrade() -> None:
    op.drop_index("ix_log_source_objects_lease", table_name="log_source_objects")
    op.drop_index("ix_log_source_objects_customer", table_name="log_source_objects")
    op.drop_index("ix_log_source_objects_claim", table_name="log_source_objects")
    op.drop_index("ix_log_source_objects_status", table_name="log_source_objects")
    op.drop_index("ix_log_source_objects_customer_code", table_name="log_source_objects")
    op.drop_table("log_source_objects")
