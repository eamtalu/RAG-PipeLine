"""add customers.notifications_enabled - the per-tenant notifications switch

Revision ID: e4b28f5c9107
Revises: d5c81b60a473
Create Date: 2026-08-08

The notifications on/off switch used to be `settings.notifications_enabled`: one boolean for the whole
deployment, read ONCE at process boot to decide whether the worker task was ever created. That made a
product-level toggle impossible rather than merely inconvenient - flipping a flag at runtime had
nothing to observe it, because no task was running.

The switch moves here, per tenant, and the check moves into the worker loop. It then takes effect
within one poll interval (~10s) with no restart, and one customer can no longer silence another.

Defaults FALSE, deliberately. Every existing tenant acquires this column at once, and a default of
true would start alerting for people who never asked for it - including, on this deployment, a rule
matching 99% of transactions aimed at a live Teams channel. False is also the value the retired env
flag effectively had in production, so applying this changes no behaviour.

`server_default` rather than a Python-side default only, so rows written by anything that bypasses the
ORM still get a defined value instead of failing the NOT NULL.

Additive: one column with a default. No rewrite of existing rows beyond the default fill, no index, no
constraint change. Safe to apply with workers running.
"""

import sqlalchemy as sa
from alembic import op

revision = "e4b28f5c9107"
down_revision = "d5c81b60a473"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "customers",
        sa.Column("notifications_enabled", sa.Boolean(), nullable=False,
                  server_default=sa.text("false")),
    )


def downgrade() -> None:
    op.drop_column("customers", "notifications_enabled")
