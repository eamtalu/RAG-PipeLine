"""add idempotency_keys table (server-side Idempotency-Key store)

Revision ID: d2f6b9c04a18
Revises: c1e5a8f4b207
Create Date: 2026-07-23

Backs IdempotencyMiddleware: records a client Idempotency-Key per mutating POST so a retry /
double-submit replays the first response instead of duplicating the side effect. UNIQUE(customer_code,
idem_key) is the atomic de-dup guard. New empty table, so plain (transactional) DDL is fine.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d2f6b9c04a18"
down_revision = "c1e5a8f4b207"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "idempotency_keys",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(length=64), nullable=False),
        sa.Column("idem_key", sa.String(length=255), nullable=False),
        sa.Column("method", sa.String(length=8), nullable=False),
        sa.Column("path", sa.String(length=512), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("response_status", sa.Integer(), nullable=True),
        sa.Column("response_body", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "idem_key", name="uq_idempotency_customer_key"),
    )
    op.create_index("ix_idempotency_keys_customer_code", "idempotency_keys", ["customer_code"])
    op.create_index("ix_idempotency_expires_at", "idempotency_keys", ["expires_at"])


def downgrade() -> None:
    op.drop_index("ix_idempotency_expires_at", table_name="idempotency_keys")
    op.drop_index("ix_idempotency_keys_customer_code", table_name="idempotency_keys")
    op.drop_table("idempotency_keys")
