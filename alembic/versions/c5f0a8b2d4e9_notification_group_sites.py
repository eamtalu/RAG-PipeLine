"""a notification group has sites

Revision ID: c5f0a8b2d4e9
Revises: b4e9f7a1c3d8
Create Date: 2026-11-28

Chunk 247 (program step 3, slice 1). A group is about every site of the customer or about the
sites listed in a small new table. Existing groups get `all_sites` true, so they behave as they
did until someone narrows them. Neither table is read by the agent's reader, so no row rule.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c5f0a8b2d4e9"
down_revision = "b4e9f7a1c3d8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("notification_groups", sa.Column(
        "all_sites", sa.Boolean(), nullable=False, server_default=sa.text("true")))
    op.create_table(
        "notification_group_sites",
        sa.Column("group_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("notification_groups.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("site_id", sa.String(16), primary_key=True),
    )


def downgrade() -> None:
    op.drop_table("notification_group_sites")
    op.drop_column("notification_groups", "all_sites")
