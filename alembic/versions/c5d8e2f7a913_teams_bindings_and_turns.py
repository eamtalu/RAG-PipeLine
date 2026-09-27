"""Teams bot: tenant bindings and conversation memory.

The Teams bot edge on AWS identifies a customer by the Entra tenant id Microsoft stamps on every
message. `teams_tenant_bindings` maps that id to a customer_code (one default log space per tenant)
and records when the row was last mirrored to the edge's DynamoDB copy. `teams_conversation_turns`
keeps the last questions and answers per Teams conversation so follow-ups carry context.

Revision ID: c5d8e2f7a913
Revises: b7e4c9d21f58
Create Date: 2026-09-21
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

revision = "c5d8e2f7a913"
down_revision = "b7e4c9d21f58"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teams_tenant_bindings",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", sa.String(64), nullable=False),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("display_name", sa.String(128), nullable=True),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("mirrored_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["customer_code"], ["customers.customer_code"],
                                name="fk_teams_tenant_bindings_customer_code", ondelete="CASCADE"),
        sa.UniqueConstraint("tenant_id", name="uq_teams_tenant_bindings_tenant_id"),
    )
    op.create_index("ix_teams_tenant_bindings_tenant_id", "teams_tenant_bindings", ["tenant_id"])
    op.create_index("ix_teams_tenant_bindings_customer_code", "teams_tenant_bindings", ["customer_code"])

    op.create_table(
        "teams_conversation_turns",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("conversation_id", sa.String(256), nullable=False),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("position", sa.SmallInteger(), nullable=False, server_default="0"),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("job_id", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_teams_conversation_turns_customer_code", "teams_conversation_turns",
                    ["customer_code"])
    op.create_index("ix_teams_conversation_turns_conv_created", "teams_conversation_turns",
                    ["conversation_id", "created_at"])


def downgrade() -> None:
    op.drop_table("teams_conversation_turns")
    op.drop_table("teams_tenant_bindings")
