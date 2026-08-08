"""add notification_rules.cursor_at — per-rule position in the transaction feed

Revision ID: c7a02f68b1d4
Revises: b3d914c7ea52
Create Date: 2026-08-08

Step 2 of docs/plan/2026-08-08_notification-architecture.html.

Each rule remembers how far it has read `log_transactions`, as a `created_at` (WRITE time). That
replaces re-reading the last hour on every tick.

Nullable on purpose, and NULL is meaningful: it means "this rule has never run". `cursor.read_window`
bootstraps a NULL cursor to `now - notification_lookback_seconds`, which is exactly the window the
engine used before this change — so every existing rule keeps behaving as it does today, and newly
activated rules alert on recent data rather than replaying all history.

Backfilling a value here would be wrong for the same reason: it would assert that rules have already
read data they have never seen.

Additive and metadata-only — `ADD COLUMN` with no default does not rewrite the table.
"""

import sqlalchemy as sa
from alembic import op

revision = "c7a02f68b1d4"
down_revision = "b3d914c7ea52"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("notification_rules",
                  sa.Column("cursor_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("notification_rules", "cursor_at")
