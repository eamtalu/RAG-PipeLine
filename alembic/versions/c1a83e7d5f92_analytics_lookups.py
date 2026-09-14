"""Chunk 100: declared lookups, and the key-to-attribute values harvested for them.

A fact records one exchange, and that is often not enough. A pick carries its delivery number on every
one of 1,343 live records and the customer name on none of them; the name is on the packing and routing
records under three different spellings across four methods.

These two tables hold the map. The value is NEVER copied onto the fact: the rollup stores the key and
the attribute is resolved when somebody reads. Stamping it in would need this same map anyway, would
make every future lookup a rewrite of history (6,586 facts after two days, for an item description),
and would break the fold's source-fingerprint skip.

Revision ID: c1a83e7d5f92
Revises: d2f7b9c31e04
Create Date: 2026-09-14
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c1a83e7d5f92"
down_revision = "d2f7b9c31e04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analytics_lookups",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("key_field", sa.String(128), nullable=False),
        sa.Column("attributes", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "name", name="uq_analytics_lookups_name"),
    )
    op.create_index("ix_analytics_lookups_customer_code", "analytics_lookups", ["customer_code"])

    op.create_table(
        "analytics_lookup_values",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("lookup", sa.String(64), nullable=False),
        sa.Column("key", sa.String(128), nullable=False),
        sa.Column("attribute", sa.String(64), nullable=False),
        sa.Column("value", sa.String(512), nullable=False),
        # A key's first value is valid from the beginning of time, not from when it was observed, so a
        # delivery named after its picks still names them.
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("origin", sa.String(16), nullable=False, server_default="observed"),
        sa.Column("source_method", sa.String(128), nullable=True),
        sa.Column("observations", sa.BigInteger(), nullable=False, server_default="1"),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "lookup", "key", "attribute", "valid_from",
                            name="uq_analytics_lookup_values_key"),
    )
    op.create_index("ix_analytics_lookup_values_customer_code", "analytics_lookup_values",
                    ["customer_code"])
    # The read path asks for a handful of keys of one lookup at a time, never for a scan.
    op.create_index("ix_analytics_lookup_values_lookup_key", "analytics_lookup_values",
                    ["customer_code", "lookup", "key"])


def downgrade() -> None:
    op.drop_table("analytics_lookup_values")
    op.drop_table("analytics_lookups")
