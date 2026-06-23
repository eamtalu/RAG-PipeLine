"""make customers.timezone nullable (NULL = not yet configured)

Revision ID: d4f1b9c63a27
Revises: c3e8a7b21d40
Create Date: 2026-06-22

A hard default of 'Europe/London' meant a non-UK customer that nobody configured silently got UK time
with no signal. Making the column nullable turns "never set" into a real, detectable state: ingestion
warns and GET /customers reports timezone_set=false, while behaviour still safely falls back to
settings.display_timezone. Existing rows keep their current value (so the UK 'mnp' tenant stays set);
only customers created WITHOUT an explicit timezone from now on are NULL/flagged.
"""

from alembic import op
import sqlalchemy as sa

revision = "d4f1b9c63a27"
down_revision = "c3e8a7b21d40"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # keep existing values; just allow NULL going forward and drop the auto-default
    op.alter_column("customers", "timezone", existing_type=sa.String(length=64),
                    nullable=True, server_default=None)


def downgrade() -> None:
    # backfill any NULLs before restoring NOT NULL + default
    op.execute("UPDATE customers SET timezone = 'Europe/London' WHERE timezone IS NULL")
    op.alter_column("customers", "timezone", existing_type=sa.String(length=64),
                    nullable=False, server_default="Europe/London")
