"""add customer_display_names (many usernames per tenant)

Revision ID: a7b1c2d3e4f5
Revises: f2a3b4c5d6e7
Create Date: 2026-06-13

A customer_code is the stable tenant slug used everywhere downstream and stays one-row/unique in the
customers table. A tenant may, however, be labelled by more than one human name / username. This
adds the one-to-many side: customer_display_names, FK'd to customers.customer_code.

To preserve what users see today, we seed one row here per existing customers.display_name (skipping
nulls / duplicates). The original customers.display_name column is left untouched, so nothing that
reads it regresses.
"""

import uuid
from datetime import datetime, timezone

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "a7b1c2d3e4f5"
down_revision = "f2a3b4c5d6e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "customer_display_names",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["customer_code"], ["customers.customer_code"],
            name="fk_customer_display_names_customer_code", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("customer_code", "display_name", name="uq_customer_display_name"),
    )
    op.create_index(
        "ix_customer_display_names_customer_code", "customer_display_names", ["customer_code"]
    )

    # seed from existing tenant labels so current display names are visible in the new list
    bind = op.get_bind()
    rows = bind.execute(
        sa.text("SELECT customer_code, display_name FROM customers "
                "WHERE display_name IS NOT NULL AND display_name <> ''")
    ).fetchall()
    now = datetime.now(timezone.utc)
    for code, name in rows:
        bind.execute(
            sa.text(
                "INSERT INTO customer_display_names "
                "(id, customer_code, display_name, active, created_at, updated_at) "
                "VALUES (:id, :code, :name, true, :now, :now) "
                "ON CONFLICT (customer_code, display_name) DO NOTHING"
            ),
            {"id": str(uuid.uuid4()), "code": code, "name": name, "now": now},
        )


def downgrade() -> None:
    op.drop_index("ix_customer_display_names_customer_code", table_name="customer_display_names")
    op.drop_table("customer_display_names")
