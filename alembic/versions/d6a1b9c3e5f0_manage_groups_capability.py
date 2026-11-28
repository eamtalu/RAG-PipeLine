"""the capability to make notification groups

Revision ID: d6a1b9c3e5f0
Revises: c5f0a8b2d4e9
Create Date: 2026-11-28

Chunk 249 (program step 3, slice 2). One row in access_capabilities: `manage_groups`. A
customer's cells for it are added on first use from matrix.yaml (Admin and Ops manager allowed,
the same two roles that could make groups before), so nobody gains or loses anything by this.
"""

import sqlalchemy as sa
from alembic import op

revision = "d6a1b9c3e5f0"
down_revision = "c5f0a8b2d4e9"
branch_labels = None
depends_on = None

CODE, NAME = "manage_groups", "Make & manage own groups"


def upgrade() -> None:
    # after configure_agents (6); export keeps its place in the table's own order
    op.execute(sa.text(
        "INSERT INTO access_capabilities (code, name, sort_order) VALUES (:code, :name, 7) "
        "ON CONFLICT (code) DO NOTHING").bindparams(code=CODE, name=NAME))


def downgrade() -> None:
    for table in ("teams_user_capabilities", "access_role_capabilities"):
        op.execute(sa.text(f"DELETE FROM {table} WHERE capability_code = :code")
                   .bindparams(code=CODE))
    op.execute(sa.text("DELETE FROM access_capabilities WHERE code = :code").bindparams(code=CODE))
