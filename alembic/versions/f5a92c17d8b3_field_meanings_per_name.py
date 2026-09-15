"""Chunk 108: what a field MEANS, recorded once per name rather than once per method.

Meaning lived on `analytics_field_registry`, whose rows are per field PER METHOD. That is right for a
DECISION - `EmployeeName` may be ticked on picking and not on counting - and wrong for a MEANING:
the name means the same thing on all 44 methods that carry it, so describing it meant writing the
same sentence 44 times. Nobody did. Measured on the live tenant the day before this migration: 1,492
registry rows, 0 with a description, 0 with a unit.

Per name, that is 176 entries, of which roughly twenty are numbers.

`kind` is the column no amount of looking at the data can fill. Discovery over the live facts
classified `ItemNumber`, `DeliveryNumber`, `LotNumber`, `UserID` and `DeviceID` as measures, because
they are numeric and they repeat exactly as a quantity does. A delivery number is a name spelled with
digits, and only a person can say so.

Anything already written into the old columns is carried across rather than dropped - none exists on
this tenant, but a migration that loses text somebody typed is not one worth writing. The old columns
stay for now: an older process still reads them, and removing them is a separate, later decision.

Revision ID: f5a92c17d8b3
Revises: e4b7c2f18a63
Create Date: 2026-09-16
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "f5a92c17d8b3"
down_revision = "e4b7c2f18a63"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analytics_field_meanings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("field", sa.String(128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("unit", sa.String(32), nullable=True),
        sa.Column("kind", sa.String(16), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewed_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "field", name="uq_analytics_field_meanings_field"),
    )
    op.create_index("ix_analytics_field_meanings_customer_code", "analytics_field_meanings",
                    ["customer_code"])

    # Carry across anything already written per method. One row per name, taking the first
    # non-null text and unit found for it, and marking it reviewed because a person wrote it.
    op.execute(sa.text("""
        INSERT INTO analytics_field_meanings
            (id, customer_code, field, description, unit, reviewed_at, reviewed_by,
             created_at, updated_at)
        SELECT gen_random_uuid(), customer_code, field,
               (array_remove(array_agg(description), NULL))[1],
               (array_remove(array_agg(unit), NULL))[1],
               now(), 'migrated from the per-method rows', now(), now()
        FROM analytics_field_registry
        WHERE description IS NOT NULL OR unit IS NOT NULL
        GROUP BY customer_code, field"""))


def downgrade() -> None:
    op.drop_table("analytics_field_meanings")
