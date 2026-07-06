"""add log-space kinds (permanent/disposable) + presence

Revision ID: e5a2c9f10b34
Revises: d4f1b9c63a27
Create Date: 2026-07-06

The Switch-Logspace palette splits a "log space" into two kinds: a DISPOSABLE space owns a brand-new
customer_code (1:1), is stamped with an owner and an expires_at, and is auto-purged when it expires; a
PERMANENT space is admin-curated with a name/description and a live|test environment. This adds `kind`
(NOT NULL, default disposable so every existing row and every legacy omitted-kind create stays a
disposable) plus the per-kind columns, and a `logspace_presence` table recording who is currently in a
space (self-declared, ephemeral, TTL-swept). Permanent-only columns are NULL on disposables and vice
versa; `inactive` stays derived from active=false and is deliberately NOT an environment value.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "e5a2c9f10b34"
down_revision = "d4f1b9c63a27"
branch_labels = None
depends_on = None

logspace_kind = sa.Enum("permanent", "disposable", name="logspacekind")
logspace_environment = sa.Enum("live", "test", name="logspaceenvironment")


def upgrade() -> None:
    bind = op.get_bind()
    # add_column does not reliably emit CREATE TYPE, so create the enum types up front.
    logspace_kind.create(bind, checkfirst=True)
    logspace_environment.create(bind, checkfirst=True)

    # NOT NULL with a server_default backfills every existing row to 'disposable' in one shot (no NULL
    # window) — matching the product rule that all pre-existing customers are disposables.
    op.add_column("customers", sa.Column("kind", logspace_kind, nullable=False,
                                         server_default="disposable"))
    # permanent-only
    op.add_column("customers", sa.Column("name", sa.String(128), nullable=True))
    op.add_column("customers", sa.Column("description", sa.Text, nullable=True))
    op.add_column("customers", sa.Column("environment", logspace_environment, nullable=True))
    # disposable-only
    op.add_column("customers", sa.Column("owner_name", sa.String(128), nullable=True))
    op.add_column("customers", sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "logspace_presence",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("note", sa.String(256), nullable=True),
        sa.Column("since", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["customer_code"], ["customers.customer_code"], ondelete="CASCADE"),
        sa.UniqueConstraint("customer_code", "name", name="uq_logspace_presence"),
    )
    op.create_index("ix_logspace_presence_customer_code", "logspace_presence", ["customer_code"])


def downgrade() -> None:
    op.drop_index("ix_logspace_presence_customer_code", table_name="logspace_presence")
    op.drop_table("logspace_presence")

    op.drop_column("customers", "expires_at")
    op.drop_column("customers", "owner_name")
    op.drop_column("customers", "environment")
    op.drop_column("customers", "description")
    op.drop_column("customers", "name")
    op.drop_column("customers", "kind")

    logspace_environment.drop(op.get_bind(), checkfirst=True)
    logspace_kind.drop(op.get_bind(), checkfirst=True)
