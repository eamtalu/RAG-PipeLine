"""tenant_site_places: the company and warehouse pairs a site stands for

Revision ID: d8b4f6a2c917
Revises: c2f7a1e8d945
Create Date: 2026-11-25

Chunk 223 (step 47). A data row is identified by customer, company and warehouse, and one site can
stand for several such pairs. A site with no row here stands for the warehouse of its own id under
any company, so the table starts empty and nothing changes for an existing site. No existing table
is altered.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d8b4f6a2c917"
down_revision = "c2f7a1e8d945"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tenant_site_places",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("site_id", sa.String(16), nullable=False),
        sa.Column("company", sa.String(16), nullable=True),
        sa.Column("warehouse", sa.String(16), nullable=False),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["customer_code", "site_id"],
                                ["tenant_sites.customer_code", "tenant_sites.site_id"],
                                ondelete="CASCADE", name="fk_tenant_site_places_site"),
        sa.UniqueConstraint("customer_code", "site_id", "company", "warehouse",
                            name="uq_tenant_site_places_pair",
                            postgresql_nulls_not_distinct=True),
    )


def downgrade() -> None:
    op.drop_table("tenant_site_places")
