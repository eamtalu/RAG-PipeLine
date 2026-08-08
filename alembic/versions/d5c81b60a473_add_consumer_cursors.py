"""add consumer_cursors — where each incremental reader of log_transactions has got to

Revision ID: d5c81b60a473
Revises: c7a02f68b1d4
Create Date: 2026-08-08

Step 8 of docs/plan/2026-08-08_notification-architecture.html.

Retention drops partitions past 60 days and already refuses to drop a day Stage 2 has not stitched.
A slow READER needs the same protection: dropping day 70 while a consumer sits at day 70 destroys that
data permanently, and the consumer would skip the gap without anything recording it.

One row per consumer, keyed by name. `position` is a `log_transactions.created_at` — write time, the
same thing every incremental reader cursors on. `updated_at` is a heartbeat: it is what separates
"behind" from "gone", so a consumer that died weeks ago cannot hold retention hostage until the disk
fills.

Created empty on purpose. Nothing is backfilled, because an invented position would assert that a
consumer has read data it never saw — and retention would believe it.

Additive: a new table, no change to anything existing. Safe to apply with workers running.
"""

import sqlalchemy as sa
from alembic import op

revision = "d5c81b60a473"
down_revision = "c7a02f68b1d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_cursors",
        sa.Column("consumer", sa.String(length=128), primary_key=True),
        sa.Column("position", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("clock_timestamp()")),
    )


def downgrade() -> None:
    op.drop_table("consumer_cursors")
