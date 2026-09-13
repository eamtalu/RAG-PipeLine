"""Semantic catalog: descriptions and units on the two registries (chunk 84, metric builder part 0).

Meaning has to live in the data, because a chat agent and the metric wizard both read it and neither
can infer what `rec.STQT` or `units-observed-by-item` means from a name. `analytics_metrics` already
carries `description`; this adds the same to the field registry (plus a `unit`) and to the
transaction registry. All nullable, so no existing row changes.

Revision ID: f3c9a7e21b64
Revises: e7a92d4f1c58
Create Date: 2026-09-12
"""

import sqlalchemy as sa
from alembic import op

revision = "f3c9a7e21b64"
down_revision = "e7a92d4f1c58"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_field_registry", sa.Column("description", sa.Text(), nullable=True))
    op.add_column("analytics_field_registry", sa.Column("unit", sa.String(32), nullable=True))
    op.add_column("analytics_transaction_registry",
                  sa.Column("description", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("analytics_transaction_registry", "description")
    op.drop_column("analytics_field_registry", "unit")
    op.drop_column("analytics_field_registry", "description")
