"""at-risk deliveries carry the WMS company

Revision ID: b8e3f1a5c7d2
Revises: a7c2e9d4f1b6
Create Date: 2026-11-26

Chunk 229 (step 51). A delivery on the at-risk board says which company it belongs to, next to
its warehouse. The table is small (under four thousand rows on the dev server) and a nullable
column with no default is a catalogue change. Existing rows are filled by
`scripts/backfill_at_risk_places.py`, outside this migration.
"""

import sqlalchemy as sa
from alembic import op

revision = "b8e3f1a5c7d2"
down_revision = "a7c2e9d4f1b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.add_column("analytics_at_risk_deliveries",
                  sa.Column("company", sa.String(16), nullable=True))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.drop_column("analytics_at_risk_deliveries", "company")
