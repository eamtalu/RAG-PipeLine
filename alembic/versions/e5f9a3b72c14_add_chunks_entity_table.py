"""add chunks_entity table and embedding_queue.chunk_entity_id

Revision ID: e5f9a3b72c14
Revises: d4e8f2a91b03
Create Date: 2026-03-08
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, ARRAY

revision = "e5f9a3b72c14"
down_revision = "d4e8f2a91b03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "chunks_entity",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_file", sa.String(512), nullable=False),
        sa.Column("chunk_index", sa.Integer, nullable=False),
        sa.Column("text", sa.Text, nullable=False),
        sa.Column("context_header", sa.Text, server_default="", nullable=False),
        sa.Column("full_text", sa.Text, nullable=False),
        sa.Column("context_path", ARRAY(sa.String), server_default="{}", nullable=False),
        sa.Column("context_depth", sa.Integer, server_default="0", nullable=False),
        sa.Column("page_numbers", ARRAY(sa.Integer), server_default="{}", nullable=False),
        sa.Column("chunk_type", sa.String(32), server_default="text", nullable=False),
        sa.Column("profile", sa.String(64), server_default="generic", nullable=False),
        sa.Column("token_estimate", sa.Integer, server_default="0", nullable=False),
        sa.Column("section_root", sa.String(512), nullable=True),
        sa.Column("section_parent", sa.String(512), nullable=True),
        sa.Column("section_heading", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    # Make embedding_queue.chunk_id nullable and add chunk_entity_id
    op.alter_column("embedding_queue", "chunk_id", nullable=True)
    op.add_column(
        "embedding_queue",
        sa.Column("chunk_entity_id", UUID(as_uuid=True),
                  sa.ForeignKey("chunks_entity.id", ondelete="CASCADE"), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("embedding_queue", "chunk_entity_id")
    op.alter_column("embedding_queue", "chunk_id", nullable=False)
    op.drop_table("chunks_entity")
