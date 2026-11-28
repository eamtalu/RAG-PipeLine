"""who connected a notification channel

Revision ID: e7b2c0d4f6a1
Revises: d6a1b9c3e5f0
Create Date: 2026-11-28

Chunk 250 (program step 3, slice 2). A channel belongs to the person who connected it from the
Teams app, and to Admins. Existing channels keep no owner, so only an Admin manages them.
"""

import sqlalchemy as sa
from alembic import op

revision = "e7b2c0d4f6a1"
down_revision = "d6a1b9c3e5f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("customer_notification_channels",
                  sa.Column("created_by", sa.String(128), nullable=True))


def downgrade() -> None:
    op.drop_column("customer_notification_channels", "created_by")
