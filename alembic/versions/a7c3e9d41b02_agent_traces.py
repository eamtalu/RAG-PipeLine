"""agent_traces: one row per agent run, redacted, for audit and troubleshooting

Revision ID: a7c3e9d41b02
Revises: 9b4e2f6a1c03
Create Date: 2026-11-10

Step 14 of docs/esmart-agent-execution-plan.md. The gateway stores what a run did after the answer
exists: question, answer, tool calls with timing, guard verdicts, model usage, versions, error.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "a7c3e9d41b02"
down_revision = "9b4e2f6a1c03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_traces",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("trace_id", sa.String(64), nullable=False),
        sa.Column("request_id", sa.String(64), nullable=False),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("principal_id", sa.String(128), nullable=False),
        sa.Column("principal_kind", sa.String(16), nullable=False),
        sa.Column("profile", sa.String(32), nullable=False),
        sa.Column("profile_version", sa.String(16), nullable=False),
        sa.Column("code_version", sa.String(16), nullable=False),
        sa.Column("model", sa.String(128), nullable=False),
        sa.Column("conversation_id", sa.String(128), nullable=True),
        sa.Column("conversation_type", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("stop_reason", sa.String(16), nullable=False),
        sa.Column("declined", sa.String(32), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("iterations", sa.Integer(), nullable=False),
        sa.Column("retries", sa.Integer(), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("answer", sa.Text(), nullable=False),
        sa.Column("guards", postgresql.JSONB(), nullable=False),
        sa.Column("tool_calls", postgresql.JSONB(), nullable=False),
        sa.Column("withheld_figures", postgresql.JSONB(), nullable=False),
        sa.Column("usage", postgresql.JSONB(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.UniqueConstraint("trace_id", name="uq_agent_traces_trace_id"),
    )
    op.create_index(
        "ix_agent_traces_customer_started", "agent_traces", ["customer_code", "started_at"]
    )
    op.create_index(
        "ix_agent_traces_customer_request", "agent_traces", ["customer_code", "request_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_agent_traces_customer_request", table_name="agent_traces")
    op.drop_index("ix_agent_traces_customer_started", table_name="agent_traces")
    op.drop_table("agent_traces")
