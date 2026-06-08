"""add hierarchical chunking

Revision ID: c3a1f7b80e42
Revises: b792944662d1
Create Date: 2026-03-01 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision: str = 'c3a1f7b80e42'
down_revision: Union[str, None] = 'b792944662d1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('chunks', sa.Column('chunk_type', sa.String(16), nullable=False, server_default='leaf'))
    op.add_column('chunks', sa.Column('parent_id', sa.UUID(), nullable=True))
    op.create_foreign_key(
        'fk_chunks_parent_id',
        'chunks', 'chunks',
        ['parent_id'], ['id'],
        ondelete='CASCADE',
    )
    op.create_index('ix_chunks_parent_id', 'chunks', ['parent_id'])


def downgrade() -> None:
    op.drop_index('ix_chunks_parent_id', table_name='chunks')
    op.drop_constraint('fk_chunks_parent_id', 'chunks', type_='foreignkey')
    op.drop_column('chunks', 'parent_id')
    op.drop_column('chunks', 'chunk_type')
