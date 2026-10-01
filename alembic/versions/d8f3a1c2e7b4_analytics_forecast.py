"""demand forecast: widen analytics_predictions, add runs / series / accuracy tables

Revision ID: d8f3a1c2e7b4
Revises: c5d8e2f7a913
Create Date: 2026-10-01

`analytics_predictions` (M1) had the spine of a forecast row (subject, horizon, model version,
target and predicted instants, value, detail) and nothing else. The demand forecast adds what it
measures (`metric`), at what grain (`grain`), the interval (`p10`, `p90`), the scoring fields
(`actual`, `abs_error`, `scored_at`) and the run that wrote it (`run_id`). The unique key widens to
include metric, grain and subject kind: lines and units for the same item on the same day are two
rows, as are a day and the week that holds it. The table is empty on every deployment, so the key
change is free; server defaults keep the M1 tests' bare inserts valid.

Three new tables: `analytics_forecast_runs`, `analytics_forecast_series`,
`analytics_forecast_accuracy` (see app/persistence/models/analytics_forecast.py).

Indexes are built CONCURRENTLY in an autocommit block, as in b9d4f2a7c318.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "d8f3a1c2e7b4"
down_revision = "c5d8e2f7a913"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- analytics_predictions: new columns and the wider key ---
    op.add_column("analytics_predictions", sa.Column("metric", sa.String(32), nullable=False, server_default="lines"))
    op.add_column("analytics_predictions", sa.Column("grain", sa.String(8), nullable=False, server_default="day"))
    op.add_column("analytics_predictions", sa.Column("p10", sa.Numeric(20, 6), nullable=True))
    op.add_column("analytics_predictions", sa.Column("p90", sa.Numeric(20, 6), nullable=True))
    op.add_column("analytics_predictions", sa.Column("actual", sa.Numeric(20, 6), nullable=True))
    op.add_column("analytics_predictions", sa.Column("abs_error", sa.Numeric(20, 6), nullable=True))
    op.add_column("analytics_predictions", sa.Column("scored_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("analytics_predictions", sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.drop_constraint("uq_analytics_predictions_key", "analytics_predictions", type_="unique")
    op.create_unique_constraint(
        "uq_analytics_predictions_key", "analytics_predictions",
        ["customer_code", "metric", "grain", "subject_kind", "subject", "horizon", "model_version", "target_at"])

    # --- runs ---
    op.create_table(
        "analytics_forecast_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="queued"),
        sa.Column("trigger", sa.String(16), nullable=False, server_default="nightly"),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("model_version", sa.String(64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("points_written", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("scored", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("detail", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    # --- series ---
    op.create_table(
        "analytics_forecast_series",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("metric", sa.String(32), nullable=False),
        sa.Column("grain", sa.String(8), nullable=False),
        sa.Column("subject_kind", sa.String(32), nullable=False),
        sa.Column("subject", sa.String(128), nullable=False),
        sa.Column("classification", sa.String(16), nullable=False),
        sa.Column("model", sa.String(32), nullable=True),
        sa.Column("backtest_wape", sa.Numeric(10, 6), nullable=True),
        sa.Column("history_days", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("active_days_28d", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("volume_28d", sa.Numeric(20, 6), nullable=False, server_default="0"),
        sa.Column("last_run_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "metric", "grain", "subject_kind", "subject",
                            name="uq_analytics_forecast_series_key"),
    )

    # --- accuracy ---
    op.create_table(
        "analytics_forecast_accuracy",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("metric", sa.String(32), nullable=False),
        sa.Column("grain", sa.String(8), nullable=False),
        sa.Column("subject_kind", sa.String(32), nullable=False),
        sa.Column("subject", sa.String(128), nullable=False),
        sa.Column("horizon", sa.String(16), nullable=False),
        sa.Column("model_version", sa.String(64), nullable=False),
        sa.Column("window_start", sa.Date(), nullable=False),
        sa.Column("window_end", sa.Date(), nullable=False),
        sa.Column("n", sa.Integer(), nullable=False),
        sa.Column("mae", sa.Numeric(20, 6), nullable=False),
        sa.Column("wape", sa.Numeric(10, 6), nullable=True),
        sa.Column("mape", sa.Numeric(10, 6), nullable=True),
        sa.Column("mape_n", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("bias", sa.Numeric(20, 6), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "metric", "grain", "subject_kind", "subject", "horizon", "model_version",
                            name="uq_analytics_forecast_accuracy_key"),
    )

    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_predictions_read "
            "ON analytics_predictions (customer_code, metric, grain, subject_kind, subject, target_at, predicted_at DESC)")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_predictions_score "
            "ON analytics_predictions (customer_code, target_at)")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_forecast_runs_latest "
            "ON analytics_forecast_runs (customer_code, created_at DESC)")
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_forecast_series_volume "
            "ON analytics_forecast_series (customer_code, metric, grain, subject_kind, volume_28d DESC, subject)")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_analytics_forecast_series_volume")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_analytics_forecast_runs_latest")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_analytics_predictions_score")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_analytics_predictions_read")
    op.drop_table("analytics_forecast_accuracy")
    op.drop_table("analytics_forecast_series")
    op.drop_table("analytics_forecast_runs")
    op.drop_constraint("uq_analytics_predictions_key", "analytics_predictions", type_="unique")
    op.create_unique_constraint("uq_analytics_predictions_key", "analytics_predictions",
                                ["customer_code", "subject", "horizon", "model_version", "target_at"])
    for col in ("run_id", "scored_at", "abs_error", "actual", "p90", "p10", "grain", "metric"):
        op.drop_column("analytics_predictions", col)
