"""Chunk 117: settlements, and one row per key.

A pick-list release is confirmed in several calls and its expected quantity is stamped on every one,
so nothing folded from the calls can add it up. Measured on tmp-live on 18 September 2026: release
540551 picked 9 and summed to 17 because eight refused attempts each carried a 1; across 6,160
releases the shortfall read -5,576 summed every call and -4,344 settled one row per release.

Two tables. `analytics_settlements` is the definition a person edits: which method, which key, what
to carry, what to settle and by which rule. `analytics_settled_rows` is one row per distinct key,
recomputed by the fold whenever a call for that key arrives, and shaped like a fact row so the
existing group-by and lookup machinery read it unchanged.

Revision ID: b7e4c9d21f58
Revises: a3d61f8c04e7
Create Date: 2026-09-18
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "b7e4c9d21f58"
down_revision = "a3d61f8c04e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analytics_settlements",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("definition", JSONB(), nullable=False, server_default="{}"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "name", name="uq_analytics_settlements_name"),
    )
    op.create_index("ix_analytics_settlements_customer_code", "analytics_settlements", ["customer_code"])

    op.create_table(
        "analytics_settled_rows",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("settlement", sa.String(64), nullable=False),
        sa.Column("key", sa.String(512), nullable=False),
        sa.Column("key_parts", JSONB(), nullable=False, server_default="[]"),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("business_date", sa.Date(), nullable=True),
        sa.Column("method", sa.String(128), nullable=True),
        sa.Column("transaction_name", sa.String(128), nullable=True),
        sa.Column("warehouse", sa.String(64), nullable=True),
        sa.Column("item_number", sa.String(64), nullable=True),
        sa.Column("delivery_number", sa.String(64), nullable=True),
        sa.Column("lot_number", sa.String(64), nullable=True),
        sa.Column("user_name", sa.String(128), nullable=True),
        sa.Column("attributes", JSONB(), nullable=False, server_default="{}"),
        sa.Column("calls", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("settled_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "settlement", "key", name="uq_analytics_settled_rows_key"),
    )
    op.create_index("ix_analytics_settled_rows_customer_code", "analytics_settled_rows", ["customer_code"])
    op.create_index("ix_analytics_settled_rows_when", "analytics_settled_rows",
                    ["customer_code", "settlement", "event_time"])
    op.create_index("ix_analytics_settled_rows_delivery", "analytics_settled_rows",
                    ["customer_code", "settlement", "delivery_number"])


def downgrade() -> None:
    op.drop_table("analytics_settled_rows")
    op.drop_table("analytics_settlements")
