"""log_stream_frontier - the head lane's bookmark (P4, chunk 72)

Revision ID: f3d94b8a5c17
Revises: e8b26a7c4d91
Create Date: 2026-08-28
"""
import sqlalchemy as sa
from alembic import op

revision = "f3d94b8a5c17"
down_revision = "e8b26a7c4d91"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "log_stream_frontier",
        sa.Column("customer_code", sa.String(64), primary_key=True),
        sa.Column("frontier_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("log_stream_frontier")
