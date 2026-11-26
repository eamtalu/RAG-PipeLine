"""settled rows carry the WMS company

Revision ID: a7c2e9d4f1b6
Revises: f4d1a8c6b2e9
Create Date: 2026-11-26

Chunk 228 (step 50). A settled row says which company it belongs to, next to its warehouse. A
nullable column with no default is a catalogue change: no row is rewritten. The lock is short
but exclusive, so it is taken with a timeout, as in f4d1a8c6b2e9. Existing rows are filled by
`scripts/backfill_fact_company.py --table analytics_settled_rows`, outside this migration.
"""

import sqlalchemy as sa
from alembic import op

revision = "a7c2e9d4f1b6"
down_revision = "f4d1a8c6b2e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.add_column("analytics_settled_rows", sa.Column("company", sa.String(16), nullable=True))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.drop_column("analytics_settled_rows", "company")
