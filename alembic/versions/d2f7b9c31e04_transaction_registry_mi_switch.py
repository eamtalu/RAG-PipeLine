"""analytics_transaction_registry.mi: whether a transaction's facts carry its M3 calls (chunk 94).

Default off. A fact is the business event - request and response - and the M3 calls made between them
are the transaction's business, kept in full by Stage 2's timeline and as rows by `expand`. Measured
before deciding: `status` already captured every MI failure on the live tenant, and no metric named an
`mi.*` key. When a name switches MI on, the fact carries one group of counters per kind of call.

The same migration retires the four legacy auto-approved `mi_result` registry rows per method
(`mi.program`, `mi.transaction`, `mi.result`, `mi.record_count`) by flipping `captured` off, so the
catalog stops offering names the fold no longer writes. Rows are kept for history; `observe_fields`
never resurrects a decision.

Revision ID: d2f7b9c31e04
Revises: c4a8e6d17b39
Create Date: 2026-09-13
"""

import sqlalchemy as sa
from alembic import op

revision = "d2f7b9c31e04"
down_revision = "c4a8e6d17b39"
branch_labels = None
depends_on = None

_LEGACY = ("mi.program", "mi.transaction", "mi.result", "mi.record_count")


def upgrade() -> None:
    op.add_column("analytics_transaction_registry",
                  sa.Column("mi", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.execute(sa.text(
        "UPDATE analytics_field_registry SET captured = false, updated_at = now() "
        "WHERE source = 'mi_result' AND field IN :names").bindparams(sa.bindparam("names", expanding=True, value=list(_LEGACY))))


def downgrade() -> None:
    op.drop_column("analytics_transaction_registry", "mi")
