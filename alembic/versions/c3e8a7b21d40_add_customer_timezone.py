"""add timezone to customers (per-ingestor log-server timezone)

Revision ID: c3e8a7b21d40
Revises: b2d1f4a6c809
Create Date: 2026-06-22

Each customer's log server runs in its own local timezone, and the log lines carry that local
wall-clock with no offset. Storing this timezone per customer lets ingestion convert the naive
wall-clock to a TRUE UTC instant regardless of where the ingest process runs (no longer depending on
the host's timezone), and lets reads render those instants back in the customer's local time.

Non-null with server_default 'Europe/London' so the existing tenant(s) — currently the UK 'mnp'
customer — get the correct zone automatically; new customers set their own at creation.
"""

from alembic import op
import sqlalchemy as sa

revision = "c3e8a7b21d40"
down_revision = "b2d1f4a6c809"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "customers",
        sa.Column("timezone", sa.String(length=64), nullable=False, server_default="Europe/London"),
    )


def downgrade() -> None:
    op.drop_column("customers", "timezone")
