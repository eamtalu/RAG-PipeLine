"""advisories carry the WMS company

Revision ID: d0a5b3c7e9f4
Revises: c9f4a2b6d8e3
Create Date: 2026-11-26

Chunk 231 (step 53). An advisory says which company it is about, next to the site its agent
wrote. The table is small (a few hundred rows) and a nullable column with no default is a
catalogue change. Existing rows are filled by
`scripts/backfill_fact_company.py --table advisories`.
"""

import sqlalchemy as sa
from alembic import op

revision = "d0a5b3c7e9f4"
down_revision = "c9f4a2b6d8e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.add_column("advisories", sa.Column("company", sa.String(16), nullable=True))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    op.drop_column("advisories", "company")
