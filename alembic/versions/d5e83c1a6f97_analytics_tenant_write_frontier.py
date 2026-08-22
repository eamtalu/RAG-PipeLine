"""analytics_tenant_state: per-tenant source write frontier (F6)

Adds `source_write_frontier` to `analytics_tenant_state`.

WHY IT IS PER TENANT. F6 publishes the analytics reader's retention position into `consumer_cursors`,
which holds exactly ONE row per consumer name, and retention is global. So the published value has to be
safe for the tenant that is furthest behind. Without a per-tenant frontier the worker could only publish
whatever it had just processed, letting a tenant that is far ahead advance the position past a tenant
that is far behind -- and `log_partition_worker` would then drop source partitions the lagging tenant had
never read, with its cursor moving past the gap unaware. `publish_retention_position` therefore takes the
MINIMUM of this column, the same shape `consumer_cursors.notifications_position` already uses over
`NotificationRule.cursor_at`.

Nullable with no backfill, on purpose. NULL means "this tenant has processed nothing", which is the
truth for every existing row, and `publish_retention_position` treats a single NULL tenant as a reason to
publish nothing at all. Defaulting it to `now()` would have been the dangerous choice: it would claim
every tenant was fully caught up the moment the migration ran.

`analytics_tenant_state` is not partitioned and holds one row per tenant, so ADD COLUMN with no default
is a catalogue-only change -- no table rewrite, no lock worth naming.

Revision ID: d5e83c1a6f97
Revises: a7c31f9e2b48
"""

import sqlalchemy as sa
from alembic import op

revision = "d5e83c1a6f97"
down_revision = "a7c31f9e2b48"
branch_labels = None
depends_on = None

_TABLE = "analytics_tenant_state"
_COLUMN = "source_write_frontier"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column(_COLUMN, sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column(_TABLE, _COLUMN)
