"""M1: analytics_feature_sets and analytics_predictions

Cites docs/analytics-ml-architecture/final_architecture.md sections M1 and F10.

Neither is partitioned. A feature set is one row per training run and a prediction is one row per
(subject, horizon, model, target) - both are small next to the fact tables, so there is nothing worth
pruning and partitioning would add planning cost for no gain. Same reasoning that keeps
`analytics_monthly_rollups` unpartitioned.

The plan's phrase "the pinned revision" is corrected to a pinned INSTANT. `analytics_fact_ledger.revision`
is PER FACT (measured 1..2 per transaction) and the ledger carries no tenant-level revision, so there is
no global revision number to pin to. `pinned_at` resolves against `recorded_at`, which works because a
fold stamps every ledger row it writes with the same instant.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "b7e34c9a2f58"
down_revision = "a2d5f81c93e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analytics_feature_sets",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("pinned_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("code_version", sa.String(32), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("feature_names", pg.JSONB(), nullable=False, server_default="[]"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("built_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "name", "pinned_at", "code_version",
                            name="uq_analytics_feature_sets_pin"),
    )
    op.create_index("ix_analytics_feature_sets_customer_code", "analytics_feature_sets",
                    ["customer_code"])

    op.create_table(
        "analytics_predictions",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("subject", sa.String(128), nullable=False),
        sa.Column("subject_kind", sa.String(32), nullable=False, server_default="item_number"),
        sa.Column("horizon", sa.String(16), nullable=False),
        sa.Column("model_version", sa.String(64), nullable=False),
        sa.Column("target_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("predicted_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("value", sa.Numeric(20, 6), nullable=True),
        sa.Column("detail", pg.JSONB(), nullable=False, server_default="{}"),
        sa.Column("feature_set_id", pg.UUID(as_uuid=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "subject", "horizon", "model_version", "target_at",
                            name="uq_analytics_predictions_key"),
    )
    op.create_index("ix_analytics_predictions_customer_code", "analytics_predictions",
                    ["customer_code"])


def downgrade() -> None:
    op.drop_table("analytics_predictions")
    op.drop_table("analytics_feature_sets")
