"""add customers tenant registry

Revision ID: f2a3b4c5d6e7
Revises: e1f2a3b4c5d6
Create Date: 2026-06-12

A customer "log space" must now exist in the customers table before any log can be ingested under
its code (the API validates ingest against an ACTIVE row, and the frontend lists these rows for
selection). To keep existing data valid, we seed one registry row per distinct customer_code already
present in log_entries (today: 'mnp').
"""

import uuid
from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "f2a3b4c5d6e7"
down_revision = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "customers",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("display_name", sa.String(128), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_customers_customer_code", "customers", ["customer_code"], unique=True)

    # seed the registry from existing log tenants so current data keeps validating
    bind = op.get_bind()
    codes = [r[0] for r in bind.execute(
        sa.text("SELECT DISTINCT customer_code FROM log_entries ORDER BY 1")
    ).fetchall()]
    now = datetime.now(timezone.utc)
    for code in codes:
        bind.execute(
            sa.text("INSERT INTO customers (id, customer_code, display_name, active, created_at, updated_at) "
                    "VALUES (:id, :code, :name, true, :now, :now)"),
            {"id": str(uuid.uuid4()), "code": code, "name": code, "now": now},
        )


def downgrade() -> None:
    op.drop_index("ix_customers_customer_code", table_name="customers")
    op.drop_table("customers")
