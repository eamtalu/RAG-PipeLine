"""deliveries at risk: how late a van must run before a delivery "held" it

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-10-05

The owner's call: a delivery that went on seven minutes after the van's usual time is not worth a
word. "Held the van" now needs the delivery to have gone on `held_after_min` minutes or more after
the usual time (60 by default), a tenant setting.
"""

import sqlalchemy as sa
from alembic import op

revision = "e6f7a8b9c0d1"
down_revision = "d5e6f7a8b9c0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_at_risk_settings", sa.Column("held_after_min", sa.Integer(), nullable=False, server_default="60"))


def downgrade() -> None:
    op.drop_column("analytics_at_risk_settings", "held_after_min")
