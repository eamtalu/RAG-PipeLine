"""facts and their ledger versions carry the WMS company

Revision ID: f4d1a8c6b2e9
Revises: e3c9a7b5d142
Create Date: 2026-11-26

Chunk 226 (step 49). A fact says which company it belongs to, next to its warehouse. Both tables
are partitioned and large (1.1 GB and 1.7 GB on the dev server), and a nullable column with no
default is a catalogue change: no row is rewritten. The lock on each table is short but exclusive,
so it is taken with a timeout: waiting behind a long fold would queue every reader behind this
migration, and failing fast and running it again is the cheaper outcome. Existing rows are filled
by `scripts/backfill_fact_company.py`, in batches, outside this migration.
"""

import sqlalchemy as sa
from alembic import op

revision = "f4d1a8c6b2e9"
down_revision = "e3c9a7b5d142"
branch_labels = None
depends_on = None

_TABLES = ("analytics_facts", "analytics_fact_ledger")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    for table in _TABLES:
        op.add_column(table, sa.Column("company", sa.String(16), nullable=True))


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '10s'")
    for table in _TABLES:
        op.drop_column(table, "company")
