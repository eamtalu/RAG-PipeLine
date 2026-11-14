"""Advisories: the kind of finding and what it is about, for the tab's groups

Revision ID: c4d8e2a7f531
Revises: a7c3e5f9b012
Create Date: 2026-11-14

Stage D, step 17a. The Advisories tab becomes a grouped list: each row says what kind of finding
it is (`kind`: count_gap, chronic_short, empty_location, delivery_at_risk, other) and what it is
about (`about`: {kind, id, label, where, area}), words the agents write and the edge never parses.
The upgrade backfills both from `source`, which the agents wrote since step 9, so the rows
already open group on the first read; the words follow app/services/advisories/kinds.py.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c4d8e2a7f531"
down_revision = "a7c3e5f9b012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("advisories", sa.Column("kind", sa.String(32), nullable=False, server_default="other"))
    op.add_column("advisories", sa.Column("about", postgresql.JSONB(), nullable=False, server_default="{}"))
    # Step 1: Name the kind from the source the agents wrote, so the open rows group at once
    op.execute("""
        UPDATE advisories SET kind = CASE source->>'kind'
            WHEN 'count' THEN 'count_gap'
            WHEN 'item' THEN 'chronic_short'
            WHEN 'location' THEN 'empty_location'
            WHEN 'delivery' THEN 'delivery_at_risk'
            ELSE 'other' END
    """)
    # Step 2: Say what each row is about in the same words the agents now write, with the area
    # as the location code's leading letters (the code itself when it has none)
    op.execute(r"""
        UPDATE advisories SET about = CASE
            WHEN source->>'kind' IN ('count', 'item') AND source ? 'item' THEN jsonb_build_object(
                'kind', 'item', 'id', source->>'item', 'label', 'Item ' || (source->>'item'),
                'where', source->>'location',
                'area', COALESCE(NULLIF(substring(source->>'location' from '^[A-Za-z]+'), ''), source->>'location', ''))
            WHEN source->>'kind' = 'location' AND source ? 'location' THEN jsonb_build_object(
                'kind', 'location', 'id', source->>'location',
                'label', CASE WHEN source->>'location' = 'JIT' THEN 'JIT staging' ELSE 'Location ' || (source->>'location') END,
                'where', NULL,
                'area', COALESCE(NULLIF(substring(source->>'location' from '^[A-Za-z]+'), ''), source->>'location'))
            WHEN source->>'kind' = 'delivery' AND source ? 'delivery_number' THEN jsonb_build_object(
                'kind', 'delivery', 'id', source->>'delivery_number',
                'label', CASE WHEN source ? 'route' THEN 'Route ' || (source->>'route') ELSE source->>'delivery_number' END,
                'where', NULL, 'area', '')
            ELSE '{}'::jsonb END
    """)


def downgrade() -> None:
    op.drop_column("advisories", "about")
    op.drop_column("advisories", "kind")
