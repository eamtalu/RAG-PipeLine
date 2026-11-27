"""roles per customer

Revision ID: b4e9f7a1c3d8
Revises: a3d8e6f0b2c7
Create Date: 2026-11-27

Chunk 245 (program step 4, slice 3). A customer may rename or retire the built-in roles and
add its own. One small table holds a customer's own roles: the ones it added, and a row for a
built-in role it changed. The shared list `access_roles` keeps every code, so the foreign keys
of the users and of the matrix cells are untouched and no existing table changes. The new
table is customer-wide data in the scoped data registry, so it gets the agent reader's row
rule here as well.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from app.services.scoped_data import database_lock

revision = "b4e9f7a1c3d8"
down_revision = "a3d8e6f0b2c7"
branch_labels = None
depends_on = None

TABLE = "access_customer_roles"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True),
        sa.Column("code", sa.String(32), sa.ForeignKey("access_roles.code"), primary_key=True),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("sort_order", sa.Integer, nullable=False, server_default="100"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("copied_from", sa.String(32), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.String(128), nullable=True),
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
