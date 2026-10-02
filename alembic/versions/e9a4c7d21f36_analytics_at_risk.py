"""deliveries at risk: five tables, two indexes on existing tables

Revision ID: e9a4c7d21f36
Revises: d8f3a1c2e7b4
Create Date: 2026-10-02

The board of deliveries behind their route's rhythm (see app/persistence/models/analytics_at_risk.py):
`analytics_at_risk_deliveries` (one row per delivery per departure, state plus acknowledgement),
`analytics_at_risk_checks` (the acknowledgement ledger), `analytics_at_risk_route_profiles` (what each
route's history taught, per day), `analytics_at_risk_settings` (the tenant's floors) and
`analytics_at_risk_tenant_state` (one read for /status).

Two indexes on existing tables serve the board's reads:
- `analytics_lookup_values (customer_code, lookup, attribute, value)`: the pick-line lookup is read
  the other way round, from a delivery number to its pick lines. Built CONCURRENTLY.
- `analytics_facts (customer_code, method, event_time)`: the packages and loads of the last 36 hours
  for three methods. The parent is partitioned, so CONCURRENTLY is not allowed (c8d24e6f1a97); a plain
  build with the worker stopped, as that migration did.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e9a4c7d21f36"
down_revision = "d8f3a1c2e7b4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analytics_at_risk_deliveries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("delivery_number", sa.String(64), nullable=False),
        sa.Column("departure_date", sa.Date(), nullable=False),
        sa.Column("departure_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("route", sa.String(32), nullable=True),
        sa.Column("customer_name", sa.String(128), nullable=True),
        sa.Column("customer_number", sa.String(64), nullable=True),
        sa.Column("tier", sa.String(16), nullable=False, server_default="none"),
        sa.Column("max_tier", sa.String(16), nullable=False, server_default="none"),
        sa.Column("first_flagged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_flagged_tier", sa.String(16), nullable=True),
        sa.Column("tier_history", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("load_threshold_min", sa.Numeric(10, 2), nullable=True),
        sa.Column("load_threshold_source", sa.String(16), nullable=True),
        sa.Column("pick_threshold_min", sa.Numeric(10, 2), nullable=True),
        sa.Column("pick_threshold_source", sa.String(16), nullable=True),
        sa.Column("lines_expected", sa.Integer(), nullable=True),
        sa.Column("lines_confirmed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lines_picked", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lines_short", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("packages_created", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("packages_loaded", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_pick_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_load_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="open"),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(24), nullable=True),
        sa.Column("outcome_lead_min", sa.Numeric(10, 2), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checked_by", sa.String(128), nullable=True),
        sa.Column("check_note", sa.Text(), nullable=True),
        sa.Column("checked_tier", sa.String(16), nullable=True),
        sa.Column("reopened_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rule_version", sa.String(32), nullable=False),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "delivery_number", "departure_date",
                            name="uq_analytics_at_risk_deliveries_key"),
    )
    op.create_table(
        "analytics_at_risk_checks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("delivery_number", sa.String(64), nullable=False),
        sa.Column("departure_date", sa.Date(), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("tier", sa.String(16), nullable=False),
        sa.Column("actor", sa.String(128), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "analytics_at_risk_route_profiles",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("route", sa.String(32), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("window_days", sa.Integer(), nullable=False),
        sa.Column("sample", sa.Integer(), nullable=False),
        sa.Column("loaded_sample", sa.Integer(), nullable=False),
        sa.Column("load_lead_p50", sa.Numeric(10, 2), nullable=True),
        sa.Column("load_lead_min", sa.Numeric(10, 2), nullable=True),
        sa.Column("pick_lead_p50", sa.Numeric(10, 2), nullable=True),
        sa.Column("pick_lead_min", sa.Numeric(10, 2), nullable=True),
        sa.Column("learned_load_min", sa.Numeric(10, 2), nullable=True),
        sa.Column("learned_pick_min", sa.Numeric(10, 2), nullable=True),
        sa.Column("departure_time_mode", sa.String(4), nullable=True),
        sa.Column("coverage", sa.Numeric(4, 3), nullable=False),
        sa.Column("rule_version", sa.String(32), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("customer_code", "route", "as_of_date", name="uq_analytics_at_risk_route_profiles_key"),
    )
    op.create_table(
        "analytics_at_risk_settings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False, unique=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("load_floor_min", sa.Integer(), nullable=False, server_default="120"),
        sa.Column("pick_floor_min", sa.Integer(), nullable=False, server_default="180"),
        sa.Column("min_sample", sa.Integer(), nullable=False, server_default="20"),
        sa.Column("window_days", sa.Integer(), nullable=False, server_default="28"),
        sa.Column("close_grace_min", sa.Integer(), nullable=False, server_default="180"),
        sa.Column("coverage", sa.Numeric(4, 3), nullable=False, server_default="0.900"),
        sa.Column("updated_by", sa.String(128), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "analytics_at_risk_tenant_state",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False, unique=True),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_profiled_date", sa.Date(), nullable=True),
        sa.Column("open_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    # The facts parent is partitioned: a plain build, with the worker stopped (see the docstring).
    op.create_index("ix_analytics_facts_customer_method_event", "analytics_facts",
                    ["customer_code", "method", "event_time"])

    with op.get_context().autocommit_block():
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_at_risk_deliveries_board "
                   "ON analytics_at_risk_deliveries (customer_code, departure_date, tier)")
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_at_risk_deliveries_open "
                   "ON analytics_at_risk_deliveries (customer_code, departure_at) WHERE status = 'open'")
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_at_risk_deliveries_checked "
                   "ON analytics_at_risk_deliveries (customer_code, checked_at DESC) WHERE checked_at IS NOT NULL")
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_at_risk_deliveries_route "
                   "ON analytics_at_risk_deliveries (customer_code, route, departure_date)")
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_at_risk_checks_when "
                   "ON analytics_at_risk_checks (customer_code, at DESC)")
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_at_risk_route_profiles_latest "
                   "ON analytics_at_risk_route_profiles (customer_code, route, as_of_date DESC)")
        op.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_analytics_lookup_values_lookup_attr_value "
                   "ON analytics_lookup_values (customer_code, lookup, attribute, value)")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_analytics_lookup_values_lookup_attr_value")
    op.drop_index("ix_analytics_facts_customer_method_event", table_name="analytics_facts")
    op.drop_table("analytics_at_risk_tenant_state")
    op.drop_table("analytics_at_risk_settings")
    op.drop_table("analytics_at_risk_route_profiles")
    op.drop_table("analytics_at_risk_checks")
    op.drop_table("analytics_at_risk_deliveries")
