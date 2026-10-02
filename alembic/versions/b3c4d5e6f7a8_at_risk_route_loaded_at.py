"""deliveries at risk: when the route was fully loaded on the departure day

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-10-02

The WMS records no departure itself: the standard load screen answers "OK" per package and nothing
marks the shipment complete. The nearest real event is the last package scanned onto the route's
loading dock on the departure day, which with vehicle-load auto-despatch on is when M3 despatches
the shipment. The delivery row keeps it next to the target departure so a supervisor reads both.
"""

import sqlalchemy as sa
from alembic import op

revision = "b3c4d5e6f7a8"
down_revision = "a2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_at_risk_deliveries", sa.Column("route_loaded_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("analytics_at_risk_deliveries", "route_loaded_at")
