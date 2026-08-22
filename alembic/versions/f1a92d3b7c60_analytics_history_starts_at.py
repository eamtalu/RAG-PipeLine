"""analytics_tenant_state: history_starts_at

Adds the EARLIEST folded event_time per tenant.

WHY A COLUMN RATHER THAN A COMPUTED VALUE. There is no backfill (correction D8), so the interface must
state where a tenant's analytics history begins -- the period before it has to be labelled rather than
drawn as zero, because an empty chart reads as "no activity" when the truth is "not measured".

The first implementation reported `analytics_watermark` for this, which is the NEWEST folded instant. So
the notice said "no analytics history before 11:07" while the chart directly beneath it plotted data from
09:00. It contradicted itself on screen.

Computing `min(event_time)` on read was rejected: F5 requires the status endpoint to be exactly one
indexed row read, and that aggregate over a table designed to reach 13M rows is not one.

Nullable with no backfill. NULL means "nothing folded yet", which is the truth for every existing row,
and the worker fills it on the next cycle. Defaulting it to now() would have been the dangerous choice:
it would assert that no history exists before the migration ran, which is the very error being fixed.

`analytics_tenant_state` is unpartitioned with one row per tenant, so ADD COLUMN with no default is a
catalogue-only change.

Revision ID: f1a92d3b7c60
Revises: d5e83c1a6f97
"""

import sqlalchemy as sa
from alembic import op

revision = "f1a92d3b7c60"
down_revision = "d5e83c1a6f97"
branch_labels = None
depends_on = None

_TABLE = "analytics_tenant_state"
_COLUMN = "history_starts_at"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column(_COLUMN, sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column(_TABLE, _COLUMN)
