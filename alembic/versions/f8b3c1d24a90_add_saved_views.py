"""add saved_views ("Saved Analyses" / saved views)

Revision ID: f8b3c1d24a90
Revises: e7c9a2b4d610
Create Date: 2026-06-15

A saved view is a persisted, shareable snapshot of an analysis session: opaque feed/filter
view-state (`state`) plus a lightweight collaboration layer — status workflow, an append-only
comment thread (embedded as JSONB), and a closure record (embedded as JSONB). Tenant-scoped by
`customer_code` (set from X-Customer-Code on create). `state`, `comments`, and `closure` are JSONB
and round-tripped verbatim; `name` is client-generated and stored as-is (not unique-constrained).
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "f8b3c1d24a90"
down_revision = "e7c9a2b4d610"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "saved_views",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False, index=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("title", sa.String(512), nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("saved_by", sa.String(256), nullable=True),
        sa.Column("assignee", sa.String(256), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, index=True),
        sa.Column("due_date", sa.String(64), nullable=True),
        sa.Column("comments", JSONB, nullable=False, server_default="[]"),
        sa.Column("closure", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", JSONB, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("saved_views")
