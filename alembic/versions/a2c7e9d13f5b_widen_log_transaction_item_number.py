"""widen log_transactions.item_number 64 -> 128

Revision ID: a2c7e9d13f5b
Revises: d1e4f7a92c63
Create Date: 2026-07-08

The WMS can put a composite/doubled ItemNumber in the request URL (observed up to 75 chars, e.g.
"BEC|V1|...|521BEC|V1|...|521"). At varchar(64) the Stage 2 INSERT raised
StringDataRightTruncationError, which aborted the whole finalize batch; because finalize retries the
oldest pending window first, that one row stalled ALL transaction stitching for the tenant. Widening
to 128 preserves the real value; a generic length guard in derive_transactions._persist is the
backstop for anything still longer. Widening a varchar is an in-place catalog change (no rewrite).
"""

import sqlalchemy as sa
from alembic import op

revision = "a2c7e9d13f5b"
down_revision = "d1e4f7a92c63"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "log_transactions", "item_number",
        existing_type=sa.String(length=64),
        type_=sa.String(length=128),
        existing_nullable=True,
    )


def downgrade() -> None:
    # Narrowing would truncate/fail on rows already >64; guard by truncating first.
    op.execute("UPDATE log_transactions SET item_number = left(item_number, 64) "
               "WHERE length(item_number) > 64")
    op.alter_column(
        "log_transactions", "item_number",
        existing_type=sa.String(length=128),
        type_=sa.String(length=64),
        existing_nullable=True,
    )
