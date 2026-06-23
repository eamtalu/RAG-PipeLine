"""log_transactions.date → local (display-zone) day, not UTC day

Revision ID: b2d1f4a6c809
Revises: f1a2b3c4d5e6
Create Date: 2026-06-22

`log_transactions.date` is the per-day bucket used by the `?date=` filters and the customer/date index.
It was derived as `started_at.date()` in **UTC**, so a transaction in the first hour after local
midnight (e.g. 00:30 BST = 23:30 UTC the previous day) was filed under the WRONG calendar day. The
derive code now computes it in the display zone (Europe/London); this backfills existing rows to match.

Idempotent and self-limiting: only rows whose stored `date` actually disagrees with the local day are
touched (a no-op on data that already lines up — true for all current rows, which have no post-midnight
edge entries). `started_at` is a timestamptz (UTC instant); `AT TIME ZONE 'Europe/London'` yields the
UK wall-clock, and `::date` its calendar day.
"""

from alembic import op

revision = "b2d1f4a6c809"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None

# Matches settings.display_timezone. Hardcoded here on purpose: a migration is a historical record and
# must stay deterministic regardless of later config changes.
_ZONE = "Europe/London"


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE log_transactions
        SET date = (started_at AT TIME ZONE '{_ZONE}')::date
        WHERE started_at IS NOT NULL
          AND date IS DISTINCT FROM (started_at AT TIME ZONE '{_ZONE}')::date
        """
    )


def downgrade() -> None:
    # Revert to the UTC calendar day.
    op.execute(
        """
        UPDATE log_transactions
        SET date = (started_at AT TIME ZONE 'UTC')::date
        WHERE started_at IS NOT NULL
          AND date IS DISTINCT FROM (started_at AT TIME ZONE 'UTC')::date
        """
    )
