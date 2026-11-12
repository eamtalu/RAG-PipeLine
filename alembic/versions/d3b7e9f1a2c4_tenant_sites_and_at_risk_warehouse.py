"""tenant_sites, and the warehouse on analytics_at_risk_deliveries

Revision ID: d3b7e9f1a2c4
Revises: c9e5a1b63d04
Create Date: 2026-11-12

Stage D, step 4 of docs/esmart-agent-execution-plan.md. `tenant_sites` names a customer's
warehouses for the Teams app's site switcher and the per-site Home snapshot. The at-risk board
already reads each delivery's warehouse from the route rows and threw it away; the new nullable
column keeps it so the board can be shown per site. Open rows are rewritten every minute, so the
column fills within a minute of the deploy; closed history stays NULL and counts as "all sites".
Additive only: nothing is dropped, renamed or made NOT NULL.
"""

import sqlalchemy as sa
from alembic import op

revision = "d3b7e9f1a2c4"
down_revision = "c9e5a1b63d04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tenant_sites",
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("site_id", sa.String(16), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("customer_code", "site_id"),
    )
    op.add_column("analytics_at_risk_deliveries", sa.Column("warehouse", sa.String(64), nullable=True))
    # CONCURRENTLY, so the live board table is not locked while the index builds (repo rule 4).
    with op.get_context().autocommit_block():
        op.create_index("ix_analytics_at_risk_deliveries_site", "analytics_at_risk_deliveries",
                        ["customer_code", "warehouse", "departure_date"], postgresql_concurrently=True)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index("ix_analytics_at_risk_deliveries_site", table_name="analytics_at_risk_deliveries",
                      postgresql_concurrently=True)
    op.drop_column("analytics_at_risk_deliveries", "warehouse")
    op.drop_table("tenant_sites")
