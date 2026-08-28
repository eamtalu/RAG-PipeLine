"""log_stream_frontier -> log_stitch_checkpoint (naming review, chunk 72)

The table is the stitcher's per-tenant checkpoint - the same concept as the SSH file checkpoints one
layer down - and the codebase already speaks 'checkpoint' for processed-up-to-here-and-safe-to-lose
positions. 'Frontier' said neither who owns it nor what it is.

Revision ID: a9c47e2d6b53
Revises: f3d94b8a5c17
Create Date: 2026-08-28
"""
from alembic import op

revision = "a9c47e2d6b53"
down_revision = "f3d94b8a5c17"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("log_stream_frontier", "log_stitch_checkpoint")
    op.alter_column("log_stitch_checkpoint", "frontier_ts", new_column_name="stitched_through")


def downgrade() -> None:
    op.alter_column("log_stitch_checkpoint", "stitched_through", new_column_name="frontier_ts")
    op.rename_table("log_stitch_checkpoint", "log_stream_frontier")
