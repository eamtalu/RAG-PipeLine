"""Teams users invited by email: object_id may wait for the first sign-in; customer email domains

Revision ID: e5a9c3d7b214
Revises: c4d8e2a7f531
Create Date: 2026-11-23

Stage D, step 43 (chunk 218). An Admin invites a person by email before they have signed in to
Teams: a `pending` teams_users row with no object id, which their first sign-in claims. So
`object_id` becomes nullable (Postgres treats NULLs as distinct, so the per-customer unique
constraint on it still holds for every real id), existing emails are normalised to trimmed lower
case, and an email is unique per customer through a partial unique index on lower(email).
`claimed_at` records when an invite was claimed, so the Admin tab can show who joined.
`teams_tenant_bindings.email_domains` lists the customer's own domains: invites go only to them
and claims come only from them.

The upgrade refuses to run while two users of one customer share an email, rather than guessing
which one to keep; the message names them.
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e5a9c3d7b214"
down_revision = "c4d8e2a7f531"
branch_labels = None
depends_on = None

INDEX = "uq_teams_users_customer_email"


def upgrade() -> None:
    op.add_column("teams_tenant_bindings", sa.Column(
        "email_domains", postgresql.ARRAY(sa.String(253)), nullable=False, server_default="{}"))
    op.alter_column("teams_users", "object_id", existing_type=sa.String(128), nullable=True)
    op.add_column("teams_users", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    # Step 1: Store every email the way the app compares it, so the unique index sees one form
    op.execute("UPDATE teams_users SET email = NULLIF(lower(trim(email)), '') WHERE email IS NOT NULL")
    # Step 2: Refuse duplicates by name, because picking a survivor would silently drop access
    dupes = op.get_bind().execute(sa.text(
        "SELECT customer_code, email, count(*) FROM teams_users WHERE email IS NOT NULL "
        "GROUP BY customer_code, email HAVING count(*) > 1")).fetchall()
    if dupes:
        raise RuntimeError("teams_users share an email within a customer; fix these first: "
                           + ", ".join(f"{c}/{e} x{n}" for c, e, n in dupes))
    # Step 3: Build the index without locking the table, as every index on a live table here
    with op.get_context().autocommit_block():
        op.execute(f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {INDEX} "
                   "ON teams_users (customer_code, lower(email)) WHERE email IS NOT NULL")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX}")
    op.execute("DELETE FROM teams_users WHERE object_id IS NULL")
    op.alter_column("teams_users", "object_id", existing_type=sa.String(128), nullable=False)
    op.drop_column("teams_users", "claimed_at")
    op.drop_column("teams_tenant_bindings", "email_domains")
