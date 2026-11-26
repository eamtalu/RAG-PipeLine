"""a route norm is keyed by company, warehouse and route

Revision ID: c9f4a2b6d8e3
Revises: b8e3f1a5c7d2
Create Date: 2026-11-26

Chunk 230 (step 52). The same warehouse code can exist under two WMS companies, so the key of a
route norm gains the company the table has carried, unfilled, since e3c9a7b5d142. The table is
small (under a thousand rows on the dev server), so the key swap holds no lock worth naming.
Existing rows are filled by `scripts/backfill_at_risk_places.py`.
"""

from alembic import op

revision = "c9f4a2b6d8e3"
down_revision = "b8e3f1a5c7d2"
branch_labels = None
depends_on = None

_TABLE = "analytics_at_risk_route_profiles"
_KEY = "uq_analytics_at_risk_route_profiles_key"


def _swap_key(columns: str) -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.drop_constraint(_KEY, _TABLE, type_="unique")
    op.execute(f"ALTER TABLE {_TABLE} ADD CONSTRAINT {_KEY} UNIQUE NULLS NOT DISTINCT ({columns})")


def upgrade() -> None:
    _swap_key("customer_code, company, warehouse, route, as_of_date")


def downgrade() -> None:
    _swap_key("customer_code, warehouse, route, as_of_date")
