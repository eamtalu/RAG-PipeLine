"""add user_ctx column to log_entries (user-aware Stage 2 response matching)

Revision ID: c9d3e8f02b15
Revises: b8c2d7e91a04
Create Date: 2026-06-11

The async RESPONSE line carries no user in its JSON payload and no ReqID, so Stage 2 previously
matched it to the oldest open request by FIFO alone. But every log line — the response included —
carries the log4net context user in its header prefix "(CPRICE)". We already parse it (LogRecord.user)
but dropped it at insert time. Persisting it lets Stage 2 attach a response to the oldest open request
*for that same user*, so a response can never be stitched across users.

Existing rows are backfilled from raw_body's header line (`... (user) [thread] LEVEL ...`),
normalising "(null)" / "" to NULL.
"""

from alembic import op
import sqlalchemy as sa

revision = "c9d3e8f02b15"
down_revision = "b8c2d7e91a04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("log_entries", sa.Column("user_ctx", sa.String(64), nullable=True))
    op.create_index("ix_log_entries_user_ctx", "log_entries", ["user_ctx"])
    # Backfill: extract the "(user)" token that sits between the ",mmm " timestamp tail and the
    # numeric "[thread]" group on the first line of raw_body. Non-greedy + the "[<digit>" anchor make
    # it robust to "((null))" double-parens and to any "...) [..." that appears later in the message.
    #   "2026-06-10 12:07:13,706 (CPRICE) [36] DEBUG ..." -> "CPRICE"
    #   "2026-06-05 10:38:53,465 ((null)) [60] INFO ..."  -> "(null)"  (nulled out below)
    op.execute(
        r"""
        UPDATE log_entries
        SET user_ctx = substring(split_part(raw_body, chr(10), 1) from '\d{3} \((.*?)\) \[\d')
        WHERE user_ctx IS NULL AND raw_body IS NOT NULL
        """
    )
    # Normalise the log4net "no user" sentinels to NULL (parser does the same via _normalise_user).
    op.execute(
        """
        UPDATE log_entries
        SET user_ctx = NULL
        WHERE lower(coalesce(user_ctx, '')) IN ('', 'null', '(null)')
        """
    )


def downgrade() -> None:
    op.drop_index("ix_log_entries_user_ctx", table_name="log_entries")
    op.drop_column("log_entries", "user_ctx")
