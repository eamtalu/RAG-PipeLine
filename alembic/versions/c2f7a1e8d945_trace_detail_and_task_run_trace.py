"""agent_traces: role, scope, model calls, steps, events; teams_task_runs.trace_id

Revision ID: c2f7a1e8d945
Revises: e5a9c3d7b214
Create Date: 2026-11-25

Chunk 221 (step 45). A trace now says who asked with which role and data scope at the time, which
check let the run start, what started it, every model call in order, the model's own text between
tool calls and the loop events. A routine run names the agent run that answered it. Every column
is nullable or has a constant default, so Postgres adds them without rewriting either table.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c2f7a1e8d945"
down_revision = "e5a9c3d7b214"
branch_labels = None
depends_on = None

_LISTS = ("model_calls", "steps", "events")


def upgrade() -> None:
    op.add_column("agent_traces", sa.Column("role_code", sa.String(32), nullable=True))
    op.add_column("agent_traces", sa.Column("scope", postgresql.JSONB(), nullable=True))
    op.add_column("agent_traces", sa.Column("capability_code", sa.String(32), nullable=True))
    op.add_column("agent_traces", sa.Column("capability_outcome", sa.String(16), nullable=True))
    op.add_column("agent_traces", sa.Column("trigger", sa.String(16), nullable=False,
                                            server_default="ask"))
    for name in _LISTS:
        op.add_column("agent_traces", sa.Column(name, postgresql.JSONB(), nullable=False,
                                                server_default="[]"))
    op.add_column("teams_task_runs", sa.Column("trace_id", sa.String(64), nullable=True))


def downgrade() -> None:
    op.drop_column("teams_task_runs", "trace_id")
    for name in (*reversed(_LISTS), "trigger", "capability_outcome", "capability_code", "scope",
                 "role_code"):
        op.drop_column("agent_traces", name)
