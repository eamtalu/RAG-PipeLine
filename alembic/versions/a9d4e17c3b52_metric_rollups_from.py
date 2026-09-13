"""analytics_metrics.rollups_from: where a metric's history starts (chunk 86, metric builder part 2).

A metric activated from the builder folds from the moment of activation, or from an earlier instant
the person chose. NULL means unbounded, which is exactly what the three pre-existing metrics have
always been, so no row changes meaning.

Revision ID: a9d4e17c3b52
Revises: f3c9a7e21b64
Create Date: 2026-09-12
"""

import sqlalchemy as sa
from alembic import op

revision = "a9d4e17c3b52"
down_revision = "f3c9a7e21b64"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_metrics",
                  sa.Column("rollups_from", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("analytics_metrics", "rollups_from")
