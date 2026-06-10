"""add log_transactions and log_entries tables

Revision ID: f6a0c4d18e25
Revises: e5f9a3b72c14
Create Date: 2026-06-08
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "f6a0c4d18e25"
down_revision = "e5f9a3b72c14"
branch_labels = None
depends_on = None


log_transaction_status = sa.Enum(
    "success", "soft", "error", "incomplete", name="logtransactionstatus"
)
log_entry_type = sa.Enum(
    "request", "request_body", "mi_call", "mi_result", "sql", "response", "info", "error",
    name="logentrytype",
)


def upgrade() -> None:
    # --- log_transactions: derived, one row per API request/response cycle ---
    op.create_table(
        "log_transactions",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("flow_id", UUID(as_uuid=True), nullable=True),  # Phase-3 hook, no FK yet
        sa.Column("source_file_start", sa.String(512), nullable=True),
        sa.Column("source_file_end", sa.String(512), nullable=True),
        # time
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("date", sa.Date, nullable=True),
        sa.Column("duration_ms", sa.Integer, nullable=True),
        # who
        sa.Column("user_name", sa.String(64), nullable=True),
        sa.Column("user_id", sa.String(64), nullable=True),
        sa.Column("employee_name", sa.String(128), nullable=True),
        # where (org)
        sa.Column("company", sa.String(16), nullable=True),
        sa.Column("warehouse", sa.String(16), nullable=True),
        sa.Column("warehouse_id", sa.String(16), nullable=True),
        sa.Column("division", sa.String(16), nullable=True),
        sa.Column("facility", sa.String(16), nullable=True),
        # where (device)
        sa.Column("device_id", sa.String(64), nullable=True),
        sa.Column("device_name", sa.String(64), nullable=True),
        sa.Column("reqid", sa.String(128), nullable=True),
        # what
        sa.Column("method", sa.String(128), nullable=True),
        sa.Column("http_method", sa.String(8), nullable=True),
        sa.Column("endpoint_url", sa.Text, nullable=True),
        sa.Column("transaction_name", sa.String(128), nullable=True),
        sa.Column("transaction_type", sa.String(32), nullable=True),
        # business keys
        sa.Column("route", sa.String(32), nullable=True),
        sa.Column("item_number", sa.String(64), nullable=True),
        sa.Column("delivery_number", sa.String(64), nullable=True),
        sa.Column("picklist_suffix", sa.String(16), nullable=True),
        sa.Column("order_number", sa.String(64), nullable=True),
        sa.Column("reporting_number", sa.String(64), nullable=True),
        # outcome
        sa.Column("status", log_transaction_status, nullable=False, server_default="incomplete"),
        sa.Column("error_text", sa.Text, nullable=True),
        sa.Column("entry_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("mi_program_count", sa.Integer, nullable=False, server_default="0"),
        # summaries
        sa.Column("request_summary", sa.Text, nullable=True),
        sa.Column("response_summary", sa.Text, nullable=True),
        # catch-all
        sa.Column("attributes", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_log_transactions_job_id", "log_transactions", ["job_id"])
    op.create_index("ix_log_transactions_flow_id", "log_transactions", ["flow_id"])
    op.create_index("ix_log_transactions_started_at", "log_transactions", ["started_at"])
    op.create_index("ix_log_transactions_date", "log_transactions", ["date"])
    op.create_index("ix_log_transactions_user_name", "log_transactions", ["user_name"])
    op.create_index("ix_log_transactions_warehouse", "log_transactions", ["warehouse"])
    op.create_index("ix_log_transactions_reqid", "log_transactions", ["reqid"])
    op.create_index("ix_log_transactions_method", "log_transactions", ["method"])
    op.create_index("ix_log_transactions_transaction_name", "log_transactions", ["transaction_name"])
    op.create_index("ix_log_transactions_transaction_type", "log_transactions", ["transaction_type"])
    op.create_index("ix_log_transactions_item_number", "log_transactions", ["item_number"])
    op.create_index("ix_log_transactions_delivery_number", "log_transactions", ["delivery_number"])
    op.create_index("ix_log_transactions_order_number", "log_transactions", ["order_number"])
    op.create_index("ix_log_transactions_status", "log_transactions", ["status"])
    # common composite for "how many for a user on a date"
    op.create_index("ix_log_transactions_user_date", "log_transactions", ["user_name", "date"])

    # --- log_entries: raw, lossless, one row per timestamped entry ---
    op.create_table(
        "log_entries",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("transaction_id", UUID(as_uuid=True),
                  sa.ForeignKey("log_transactions.id", ondelete="SET NULL"), nullable=True),
        sa.Column("job_id", UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source_file", sa.String(512), nullable=False),
        sa.Column("line_number", sa.Integer, nullable=True),
        sa.Column("seq", sa.Integer, nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("level", sa.String(8), nullable=True),
        sa.Column("logger", sa.String(256), nullable=True),
        sa.Column("method", sa.String(128), nullable=True),
        sa.Column("entry_type", log_entry_type, nullable=False, server_default="info"),
        sa.Column("mi_program", sa.String(32), nullable=True),
        sa.Column("mi_transaction", sa.String(64), nullable=True),
        sa.Column("result_status", sa.Text, nullable=True),
        sa.Column("record_count", sa.Integer, nullable=True),
        sa.Column("message", sa.Text, nullable=True),
        sa.Column("raw_body", sa.Text, nullable=True),
        sa.Column("fields", JSONB, nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_log_entries_transaction_id", "log_entries", ["transaction_id"])
    op.create_index("ix_log_entries_job_id", "log_entries", ["job_id"])
    op.create_index("ix_log_entries_timestamp", "log_entries", ["timestamp"])
    op.create_index("ix_log_entries_entry_type", "log_entries", ["entry_type"])
    op.create_index("ix_log_entries_mi_program", "log_entries", ["mi_program"])
    op.create_index("ix_log_entries_mi_transaction", "log_entries", ["mi_transaction"])
    # Stage 2 scans ungrouped entries in timestamp order
    op.create_index("ix_log_entries_txn_timestamp", "log_entries", ["transaction_id", "timestamp"])


def downgrade() -> None:
    op.drop_table("log_entries")
    op.drop_table("log_transactions")
    log_entry_type.drop(op.get_bind(), checkfirst=True)
    log_transaction_status.drop(op.get_bind(), checkfirst=True)
