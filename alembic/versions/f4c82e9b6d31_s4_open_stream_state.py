"""S4: log_open_stream and log_pending_request

Cites docs/analytics-ml-architecture/final_architecture.md sections 18 (S4) and 18d.

The grouper's live state, made durable so it survives a process boundary. `_group` has always held it
in memory and discarded it at the end of every batch, which is why Stage 2 pads its window wide enough
to re-derive everything from scratch.

NEITHER TABLE IS PARTITIONED, deliberately, and 18d says so: both are small self-cleaning working
sets - a few hundred rows, deleted when a stream closes. Nothing worth pruning, and partitioning adds
planning cost for no gain. Being unpartitioned they need no grain and no retention policy, so
partitioning's own tests are untouched.

NULLS NOT DISTINCT on the stream key is load-bearing rather than tidy. `thread` and `user_ctx` are both
nullable, and under PostgreSQL's default rule `(NULL, 'amin')` never conflicts with itself - so one
logical stream would accumulate several rows and the lookup would be non-deterministic. That is failure
mode 5 in the plan's table, and it is a constraint rather than a convention because nothing else could
enforce it.

SHIPPED IN SHADOW. `stage2_stream_lookup` defaults to "shadow": the state is written and read and the
seeded grouping is COMPARED against the re-derive, but the re-derive stays authoritative. S3 made the
six known miss modes permanent - nothing revisits a row whose fingerprint matched - so promoting
without a divergence measurement would make a silent split unrecoverable.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as pg

revision = "f4c82e9b6d31"
down_revision = "e6b93a4d7f12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "log_open_stream",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("thread", sa.String(64), nullable=True),
        sa.Column("user_ctx", sa.String(64), nullable=True),
        sa.Column("transaction_id", pg.UUID(as_uuid=True), nullable=False),
        sa.Column("has_request", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_entry_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open_ts_is_null", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("open_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open_source_file", sa.String(512), nullable=True),
        sa.Column("open_line_number", sa.Integer(), nullable=True),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
    )
    op.create_index("ix_log_open_stream_customer_code", "log_open_stream", ["customer_code"])
    # NULLS NOT DISTINCT cannot be expressed through op.create_table's UniqueConstraint on every
    # SQLAlchemy/Alembic combination, so it is stated in SQL where it is unambiguous.
    op.execute("""ALTER TABLE log_open_stream
                  ADD CONSTRAINT uq_log_open_stream_key
                  UNIQUE NULLS NOT DISTINCT (customer_code, thread, user_ctx)""")

    op.create_table(
        "log_pending_request",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("entry_id", pg.UUID(as_uuid=True), nullable=False),
        sa.Column("reqid", sa.String(128), nullable=True),
        sa.Column("req_user", sa.String(64), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "entry_id", name="uq_log_pending_request_entry"),
    )
    op.create_index("ix_log_pending_request_customer_code", "log_pending_request", ["customer_code"])


def downgrade() -> None:
    op.drop_table("log_pending_request")
    op.drop_table("log_open_stream")
