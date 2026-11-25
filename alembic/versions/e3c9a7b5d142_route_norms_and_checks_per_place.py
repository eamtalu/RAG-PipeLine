"""at-risk route norms and checks carry company and warehouse

Revision ID: e3c9a7b5d142
Revises: d8b4f6a2c917
Create Date: 2026-11-25

Chunk 224 (step 48). A route norm is learned per warehouse and route, and a check says which
warehouse its delivery was in. Both tables are small (under a thousand rows on production), so
the key swap holds no lock worth naming. Existing norms take the warehouse of their route where
the route's deliveries name exactly one; the rest stay without, which reads as before.
"""

import sqlalchemy as sa
from alembic import op

revision = "e3c9a7b5d142"
down_revision = "d8b4f6a2c917"
branch_labels = None
depends_on = None

_KEY = "uq_analytics_at_risk_route_profiles_key"


def upgrade() -> None:
    for table in ("analytics_at_risk_route_profiles", "analytics_at_risk_checks"):
        op.add_column(table, sa.Column("company", sa.String(16), nullable=True))
        op.add_column(table, sa.Column("warehouse", sa.String(64), nullable=True))
    op.execute("""
        UPDATE analytics_at_risk_route_profiles p
           SET warehouse = one.warehouse
          FROM (SELECT customer_code, route, min(warehouse) AS warehouse
                  FROM analytics_at_risk_deliveries
                 WHERE route IS NOT NULL AND warehouse IS NOT NULL
                 GROUP BY customer_code, route
                HAVING count(DISTINCT warehouse) = 1) one
         WHERE p.customer_code = one.customer_code AND p.route = one.route
           AND p.warehouse IS NULL
    """)
    op.drop_constraint(_KEY, "analytics_at_risk_route_profiles", type_="unique")
    op.execute(f"""
        ALTER TABLE analytics_at_risk_route_profiles
          ADD CONSTRAINT {_KEY} UNIQUE NULLS NOT DISTINCT
              (customer_code, warehouse, route, as_of_date)
    """)


def downgrade() -> None:
    op.drop_constraint(_KEY, "analytics_at_risk_route_profiles", type_="unique")
    op.create_unique_constraint(_KEY, "analytics_at_risk_route_profiles",
                                ["customer_code", "route", "as_of_date"])
    for table in ("analytics_at_risk_checks", "analytics_at_risk_route_profiles"):
        op.drop_column(table, "warehouse")
        op.drop_column(table, "company")
