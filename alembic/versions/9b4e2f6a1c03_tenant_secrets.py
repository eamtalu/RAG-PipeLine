"""tenant_secrets: a tenant's secret values, sealed at rest under envelope encryption

Revision ID: 9b4e2f6a1c03
Revises: 3f727cb28a14
Create Date: 2026-11-09

Step 8 of docs/esmart-agent-execution-plan.md. Webhook URLs leave the channel config JSON and
live here encrypted; the config keeps a reference. Connector credentials follow later.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "9b4e2f6a1c03"
down_revision = "3f727cb28a14"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tenant_secrets",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("owner", sa.String(128), nullable=False),
        sa.Column("key_id", sa.String(32), nullable=False),
        sa.Column("wrapped_key", sa.LargeBinary(), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rotated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_tenant_secrets_customer_code", "tenant_secrets", ["customer_code"])
    op.create_index(
        "ix_tenant_secrets_customer_owner", "tenant_secrets", ["customer_code", "owner"]
    )


def downgrade() -> None:
    op.drop_index("ix_tenant_secrets_customer_owner", table_name="tenant_secrets")
    op.drop_index("ix_tenant_secrets_customer_code", table_name="tenant_secrets")
    op.drop_table("tenant_secrets")
