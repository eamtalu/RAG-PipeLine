"""deliveries at risk: the van is the clock

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-10-04

The WMS departure time (11:30) is a planning time: every route's van is fully loaded four to five
hours before it. Judging deliveries against a lead measured back from 11:30 produced rows that read
"at risk" and "4 h 31 before" at once. The rule now learns, per route, the time of day its van is
usually ready (the dock's last scan on nine days in ten), judges each delivery against that, and
calls a delivery the van went without "left behind".

Delivery rows: the usual-ready instant and its source replace the two lead thresholds, and the dock's
first scan joins its last. Route profiles: times of day replace the lead quantiles. Settings: the
warning and quiet windows and the days of history needed replace the floors and the sample floor.
"""

import sqlalchemy as sa
from alembic import op

revision = "d5e6f7a8b9c0"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("analytics_at_risk_deliveries", sa.Column("usual_ready_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("analytics_at_risk_deliveries", sa.Column("usual_ready_source", sa.String(16), nullable=True))
    op.add_column("analytics_at_risk_deliveries", sa.Column("route_loading_from", sa.DateTime(timezone=True), nullable=True))
    for col in ("load_threshold_min", "load_threshold_source", "pick_threshold_min", "pick_threshold_source"):
        op.drop_column("analytics_at_risk_deliveries", col)
    op.execute("UPDATE analytics_at_risk_deliveries SET tier = 'left_behind' WHERE tier = 'late'")
    op.execute("UPDATE analytics_at_risk_deliveries SET max_tier = 'left_behind' WHERE max_tier = 'late'")
    op.execute("UPDATE analytics_at_risk_deliveries SET first_flagged_tier = 'left_behind' WHERE first_flagged_tier = 'late'")
    op.execute("UPDATE analytics_at_risk_deliveries SET checked_tier = 'left_behind' WHERE checked_tier = 'late'")
    op.execute("UPDATE analytics_at_risk_checks SET tier = 'left_behind' WHERE tier = 'late'")

    for col in ("load_lead_p50", "load_lead_min", "pick_lead_p50", "pick_lead_min", "learned_load_min", "learned_pick_min"):
        op.drop_column("analytics_at_risk_route_profiles", col)
    op.add_column("analytics_at_risk_route_profiles", sa.Column("van_days", sa.Integer(), nullable=False, server_default="0"))
    for col in ("van_ready_usual_min", "van_ready_p50_min", "van_ready_latest_min", "loading_from_p50_min",
                "pick_done_usual_min", "pick_done_p50_min"):
        op.add_column("analytics_at_risk_route_profiles", sa.Column(col, sa.Numeric(10, 2), nullable=True))
    op.add_column("analytics_at_risk_route_profiles", sa.Column("pick_days", sa.Integer(), nullable=False, server_default="0"))

    op.drop_column("analytics_at_risk_settings", "load_floor_min")
    op.drop_column("analytics_at_risk_settings", "pick_floor_min")
    op.drop_column("analytics_at_risk_settings", "min_sample")
    op.add_column("analytics_at_risk_settings", sa.Column("warn_before_min", sa.Integer(), nullable=False, server_default="30"))
    op.add_column("analytics_at_risk_settings", sa.Column("gone_after_min", sa.Integer(), nullable=False, server_default="20"))
    op.add_column("analytics_at_risk_settings", sa.Column("min_days", sa.Integer(), nullable=False, server_default="5"))


def downgrade() -> None:
    op.drop_column("analytics_at_risk_settings", "min_days")
    op.drop_column("analytics_at_risk_settings", "gone_after_min")
    op.drop_column("analytics_at_risk_settings", "warn_before_min")
    op.add_column("analytics_at_risk_settings", sa.Column("min_sample", sa.Integer(), nullable=False, server_default="20"))
    op.add_column("analytics_at_risk_settings", sa.Column("pick_floor_min", sa.Integer(), nullable=False, server_default="180"))
    op.add_column("analytics_at_risk_settings", sa.Column("load_floor_min", sa.Integer(), nullable=False, server_default="120"))

    op.drop_column("analytics_at_risk_route_profiles", "pick_days")
    for col in ("pick_done_p50_min", "pick_done_usual_min", "loading_from_p50_min", "van_ready_latest_min",
                "van_ready_p50_min", "van_ready_usual_min"):
        op.drop_column("analytics_at_risk_route_profiles", col)
    op.drop_column("analytics_at_risk_route_profiles", "van_days")
    for col in ("learned_pick_min", "learned_load_min", "pick_lead_min", "pick_lead_p50", "load_lead_min", "load_lead_p50"):
        op.add_column("analytics_at_risk_route_profiles", sa.Column(col, sa.Numeric(10, 2), nullable=True))

    for table, col in (("analytics_at_risk_checks", "tier"), ("analytics_at_risk_deliveries", "checked_tier"),
                       ("analytics_at_risk_deliveries", "first_flagged_tier"), ("analytics_at_risk_deliveries", "max_tier"),
                       ("analytics_at_risk_deliveries", "tier")):
        op.execute(f"UPDATE {table} SET {col} = 'late' WHERE {col} = 'left_behind'")
    op.add_column("analytics_at_risk_deliveries", sa.Column("pick_threshold_source", sa.String(16), nullable=True))
    op.add_column("analytics_at_risk_deliveries", sa.Column("pick_threshold_min", sa.Numeric(10, 2), nullable=True))
    op.add_column("analytics_at_risk_deliveries", sa.Column("load_threshold_source", sa.String(16), nullable=True))
    op.add_column("analytics_at_risk_deliveries", sa.Column("load_threshold_min", sa.Numeric(10, 2), nullable=True))
    op.drop_column("analytics_at_risk_deliveries", "route_loading_from")
    op.drop_column("analytics_at_risk_deliveries", "usual_ready_source")
    op.drop_column("analytics_at_risk_deliveries", "usual_ready_at")
