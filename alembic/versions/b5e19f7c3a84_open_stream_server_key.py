"""log_open_stream: add server to the stream key (18v, chunk 76).

Thread ids are small integers reused by every app server process (18r), so a stream key of
(customer, thread, user) can hold only ONE server's open conversation per key - newest wins and the
other server's parked stream is silently dropped, which the head lane cannot tolerate. The table is
a self-cleaning cache ("the state is not the truth"), so existing rows are simply deleted: the next
rebuild of any window repopulates the tenant's state.

Revision ID: b5e19f7c3a84
Revises: a9c47e2d6b53
Create Date: 2026-08-28
"""

import sqlalchemy as sa
from alembic import op

revision = "b5e19f7c3a84"
down_revision = "a9c47e2d6b53"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DELETE FROM log_open_stream")
    op.add_column("log_open_stream",
                  sa.Column("server", sa.String(255), nullable=False, server_default=""))
    op.drop_constraint("uq_log_open_stream_key", "log_open_stream", type_="unique")
    op.execute("ALTER TABLE log_open_stream ADD CONSTRAINT uq_log_open_stream_key "
               "UNIQUE NULLS NOT DISTINCT (customer_code, server, thread, user_ctx)")


def downgrade() -> None:
    op.execute("DELETE FROM log_open_stream")
    op.drop_constraint("uq_log_open_stream_key", "log_open_stream", type_="unique")
    op.drop_column("log_open_stream", "server")
    op.execute("ALTER TABLE log_open_stream ADD CONSTRAINT uq_log_open_stream_key "
               "UNIQUE NULLS NOT DISTINCT (customer_code, thread, user_ctx)")
