"""add document_type to jobs

Revision ID: d4e8f2a91b03
Revises: c3a1f7b80e42
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa

revision = "d4e8f2a91b03"
down_revision = "c3a1f7b80e42"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("document_type", sa.String(64), nullable=False, server_default="general"),
    )


def downgrade() -> None:
    op.drop_column("jobs", "document_type")
