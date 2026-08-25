"""R1: analytics_transaction_registry and analytics_field_registry

Cites docs/analytics-ml-architecture/final_architecture.md section 18b (the registry) and 18d (the
target-state map).

NEITHER TABLE IS PARTITIONED, deliberately. Both are small self-cleaning working sets - a handful of
rows per tenant for the transaction registry, at most a few hundred for the field registry - so there
is nothing worth pruning and partitioning would add planning cost for no gain. Same reasoning that
keeps `analytics_monthly_rollups` out of `partitioning.PARTITIONED`. Because they are unpartitioned
they need no grain and no retention policy, so both partitioning tests pass untouched.

DEFAULTS ARE THE SAFETY PROPERTY, and they are set at the DATABASE, not only in Python:

    capture  DEFAULT true    a transaction nobody has reviewed is still captured, because the entries
                             it would have been captured from are gone in 60 days
    show     DEFAULT true    hiding real activity by default under-counts every chart silently,
                             which is worse than an unreviewed transaction appearing on one
    captured DEFAULT false   (field registry) a field nobody has approved is RECORDED, not stored

A default enforced only in the ORM is one a raw INSERT can bypass, which would give the same
transaction different treatment depending on which code path created its row.

The field registry has NO COLUMN THAT COULD HOLD A VALUE, and that is load-bearing rather than an
omission. An unknown key is reported by NAME so a person can decide; if this table could store a
sample value, a discovery record could leak a credential by accident. `AccessToken` and
`M3UserCredentials` are the two most frequent response keys of the 145 measured, and `analytics_facts`
is KEEP_FOREVER.
"""

import sqlalchemy as sa
from alembic import op

revision = "d8f52c6a1b94"
down_revision = "c4e17b9d5a83"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analytics_transaction_registry",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        # NOT NULL: a NULL here would be a second way to say "the unnamed transactions", competing
        # with the code rule that handles them - and UNIQUE treats NULLs as distinct, so it would not
        # even be one row.
        sa.Column("transaction_name", sa.String(128), nullable=False),
        sa.Column("capture", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("show", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("expand", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewed_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "transaction_name",
                            name="uq_analytics_txn_registry_name"),
    )
    op.create_index("ix_analytics_transaction_registry_customer_code",
                    "analytics_transaction_registry", ["customer_code"])

    op.create_table(
        "analytics_field_registry",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_code", sa.String(64), nullable=False),
        sa.Column("method", sa.String(128), nullable=False),
        # request | response | mi_result. Part of the key because request and response both carry
        # `ItemNumber` and they are not the same observation.
        sa.Column("source", sa.String(16), nullable=False),
        sa.Column("field", sa.String(128), nullable=False),
        sa.Column("captured", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("seen_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reviewed_by", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("customer_code", "method", "source", "field",
                            name="uq_analytics_field_registry_key"),
    )
    op.create_index("ix_analytics_field_registry_customer_code",
                    "analytics_field_registry", ["customer_code"])


def downgrade() -> None:
    op.drop_table("analytics_field_registry")
    op.drop_table("analytics_transaction_registry")
