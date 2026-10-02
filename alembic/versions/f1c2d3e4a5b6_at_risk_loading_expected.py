"""deliveries at risk: remember whether a delivery's route has a loading step

Revision ID: f1c2d3e4a5b6
Revises: e9a4c7d21f36
Create Date: 2026-10-02

Measured on the live data after the first deploy: the BRILA routes (the Gatwick run among them) are
picked and packed but never scanned onto a van, so judging them on loading flagged every one of them
every day. The board now decides per route whether loading is expected, and the delivery row keeps
that decision so the close and the history read it back without re-deriving it. Two outcomes join
the vocabulary for those routes: `picked_in_time` and `picked_late`.
"""

import sqlalchemy as sa
from alembic import op

revision = "f1c2d3e4a5b6"
down_revision = "e9a4c7d21f36"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_at_risk_deliveries",
                  sa.Column("loading_expected", sa.Boolean(), nullable=False, server_default="true"))


def downgrade() -> None:
    op.drop_column("analytics_at_risk_deliveries", "loading_expected")
