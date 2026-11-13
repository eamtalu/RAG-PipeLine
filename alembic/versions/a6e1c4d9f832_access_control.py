"""Access control: roles, capabilities, the matrix per customer, users, approvals, audit

Revision ID: a6e1c4d9f832
Revises: f5d9b3c8e721
Create Date: 2026-11-13

Stage D, step 7 of docs/esmart-agent-execution-plan.md: the matrix of screen 07 as data. The
roles and capabilities are seeded here from app/services/access/matrix.yaml; a customer's cells
are seeded by the service on first use, so a new customer starts from the file and an Admin may
change cells in the app. Users, their sites and their per-capability overrides; the approvals
queue; the audit log. Additive only: nothing existing is touched.
"""

from pathlib import Path

import sqlalchemy as sa
import yaml
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a6e1c4d9f832"
down_revision = "f5d9b3c8e721"
branch_labels = None
depends_on = None

MATRIX = Path(__file__).resolve().parents[2] / "app" / "services" / "access" / "matrix.yaml"


def upgrade() -> None:
    op.create_table(
        "access_roles",
        sa.Column("code", sa.String(32), primary_key=True),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_table(
        "access_capabilities",
        sa.Column("code", sa.String(32), primary_key=True),
        sa.Column("name", sa.String(80), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_table(
        "access_role_capabilities",
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), primary_key=True),
        sa.Column("role_code", sa.String(32), sa.ForeignKey("access_roles.code"), primary_key=True),
        sa.Column("capability_code", sa.String(32), sa.ForeignKey("access_capabilities.code"),
                  primary_key=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_by", sa.String(128), nullable=True),
    )
    op.create_table(
        "teams_users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("object_id", sa.String(128), nullable=False),
        sa.Column("display_name", sa.String(128), nullable=False),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("role_code", sa.String(32), sa.ForeignKey("access_roles.code"), nullable=False),
        sa.Column("all_sites", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("status", sa.String(16), nullable=False, server_default="active"),
        sa.Column("starts_on", sa.Date(), nullable=True),
        sa.Column("note", sa.String(300), nullable=True),
        sa.Column("created_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("last_active_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("customer_code", "object_id", name="uq_teams_users_customer_object"),
    )
    op.create_index("ix_teams_users_customer_status", "teams_users", ["customer_code", "status"])
    op.create_table(
        "teams_user_sites",
        sa.Column("user_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("teams_users.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("site_id", sa.String(16), primary_key=True),
        sa.ForeignKeyConstraint(["customer_code", "site_id"],
                                ["tenant_sites.customer_code", "tenant_sites.site_id"],
                                ondelete="CASCADE"),
    )
    op.create_table(
        "teams_user_capabilities",
        sa.Column("user_id", postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("teams_users.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("capability_code", sa.String(32), sa.ForeignKey("access_capabilities.code"),
                  primary_key=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("granted_by", sa.String(128), nullable=True),
        sa.Column("granted_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("approval_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_table(
        "access_approvals",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("reference", sa.String(16), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("requested_by", sa.String(128), nullable=False),
        sa.Column("requested_by_name", sa.String(128), nullable=True),
        sa.Column("requested_via", sa.String(16), nullable=False, server_default="ask"),
        sa.Column("site_id", sa.String(16), nullable=True),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("detail", sa.String(300), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("decided_by", sa.String(128), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("changes", postgresql.JSONB(), nullable=True),
        sa.Column("note", sa.String(300), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "reference", name="uq_access_approvals_reference"),
    )
    op.create_index("ix_access_approvals_customer_status", "access_approvals",
                    ["customer_code", "status", "created_at"])
    op.create_table(
        "audit_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64),
                  sa.ForeignKey("customers.customer_code", ondelete="CASCADE"), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("actor_object_id", sa.String(128), nullable=False),
        sa.Column("actor_name", sa.String(128), nullable=True),
        sa.Column("actor_kind", sa.String(16), nullable=False, server_default="user"),
        sa.Column("action", sa.String(64), nullable=False),
        sa.Column("capability_code", sa.String(32), nullable=True),
        sa.Column("decision", sa.String(16), nullable=False),
        sa.Column("subject_kind", sa.String(32), nullable=True),
        sa.Column("subject_id", sa.String(128), nullable=True),
        sa.Column("site_id", sa.String(16), nullable=True),
        sa.Column("request_id", sa.String(64), nullable=True),
        sa.Column("trace_id", sa.String(64), nullable=True),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("detail", postgresql.JSONB(), nullable=True),
    )
    op.create_index("ix_audit_events_customer_at", "audit_events", ["customer_code", "at"])

    # the roles and capabilities, from the one file the service reads too
    raw = yaml.safe_load(MATRIX.read_text())
    roles = sa.table("access_roles", sa.column("code"), sa.column("name"), sa.column("sort_order"))
    op.bulk_insert(roles, [{"code": r["code"], "name": r["name"], "sort_order": i}
                           for i, r in enumerate(raw["roles"])])
    caps = sa.table("access_capabilities", sa.column("code"), sa.column("name"), sa.column("sort_order"))
    op.bulk_insert(caps, [{"code": c["code"], "name": c["name"], "sort_order": i}
                          for i, c in enumerate(raw["capabilities"])])


def downgrade() -> None:
    for table in ("audit_events", "access_approvals", "teams_user_capabilities", "teams_user_sites",
                  "teams_users", "access_role_capabilities", "access_capabilities", "access_roles"):
        op.drop_table(table)
