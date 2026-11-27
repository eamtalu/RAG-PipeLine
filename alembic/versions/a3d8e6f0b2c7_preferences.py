"""preferences of a person or a role

Revision ID: a3d8e6f0b2c7
Revises: f2c7d5e9a1b6
Create Date: 2026-11-27

Chunk 239 (program step 2, slice 1). One small table: a row per customer, subject (a role or a
person) and area, holding the settings that subject chose. It is customer-wide data in the
scoped data registry, so it also gets the agent reader's row rule; the rules and the reader's
rights are applied again for every registry table, which is safe to repeat.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from app.services.scoped_data import database_lock

revision = "a3d8e6f0b2c7"
down_revision = "f2c7d5e9a1b6"
branch_labels = None
depends_on = None

TABLE = "preferences"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("subject_kind", sa.String(8), nullable=False),
        sa.Column("subject_id", sa.String(128), nullable=False),
        sa.Column("area", sa.String(32), nullable=False),
        sa.Column("settings", postgresql.JSONB, nullable=False, server_default="{}"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.String(128), nullable=True),
        sa.UniqueConstraint("customer_code", "subject_kind", "subject_id", "area",
                            name="uq_preferences_subject_area"),
    )
    # The new table only: it is empty and nothing reads it yet, so its lock is free.
    for statement in database_lock.policy_statements([TABLE]):
        op.execute(statement)
    has_role = op.get_bind().scalar(text("SELECT 1 FROM pg_roles WHERE rolname = :role"),
                                    {"role": database_lock.ROLE})
    if has_role:
        op.execute(f"GRANT SELECT ON {TABLE} TO {database_lock.ROLE}")


def downgrade() -> None:
    op.drop_table(TABLE)
