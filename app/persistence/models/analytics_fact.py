# analytics_fact.py — the wide fact row, and its append-only ledger.
#
#   Two tables, one file, because they are one decision: `analytics_facts` holds the CURRENT value of
#   each transaction's contribution, and `analytics_fact_ledger` holds EVERY version of it. Splitting
#   them across files would hide that the second exists only because the first is overwritten.
#
#   WHY THE ROW IS WIDE (section 11). Measures are chosen by the user from the interface, later, and the
#   raw log entries are dropped at 60 days. So a measure invented next year can only be backfilled
#   across history if its fields were ALREADY being written today. Anything absent from this table is
#   unrecoverable. That makes the column list the one irreversible decision in the plan, which is why it
#   is asserted against `analytics.contract.FACT_FIELDS` by a test rather than maintained by hand.
#
#   WHY THE LEDGER EXISTS FROM DAY ONE (F10). A rebuild overwrites the previous value, so without the
#   ledger a training set is not reproducible and a discarded version cannot be recovered. At a 98.7%
#   rebuild rate that is not a corner case, it is the norm. Adding the ledger later would mean the
#   history before that day simply does not exist.
#
#   IDENTITY IS (id, started_at), NOT id (F3). Source uniqueness on log_transactions is
#   `UNIQUE NULLS NOT DISTINCT (id, started_at)` because started_at is the partition key and is
#   nullable. Two rows can therefore share an id in different partitions. Keying on the id alone would
#   silently merge them, and zero duplicate pairs existing today is exactly when the extra column is
#   free to add.

import enum
import uuid
from datetime import date as date_type, datetime, timezone

from sqlalchemy import (BigInteger, Date, DateTime, Index, Integer, Numeric, String, Text,
                        UniqueConstraint)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


class QuantityClassification(str, enum.Enum):
    """Mirrors `analytics.contract.Classification`.

    Stored as a short string rather than a PostgreSQL enum: a new classification would otherwise need a
    migration, and the contract module is where that decision belongs.
    """

    pick = "pick"
    attempt = "attempt"
    correction = "correction"
    non_quantity = "non_quantity"
    unusable = "unusable"


class FactColumns:
    """The columns `analytics_facts` and `analytics_fact_ledger` share.

    A mixin so the two cannot drift: the ledger's whole purpose is to hold previous versions of a fact
    row, and a ledger missing a column the fact table has could not reproduce it. SQLAlchemy 2.0 copies
    plain `mapped_column` definitions from a mixin per mapped class, which is what lets one declaration
    serve two tables.
    """

    # --- identity (F3): both columns, never the id alone ---
    source_transaction_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    #: The source row's own partition key, kept as explicit provenance. Holds the same instant as
    #: `event_time`; recorded separately so a future change to how event_time is derived cannot quietly
    #: alter what the fact claims its source was.
    source_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: Fingerprint over every field affecting a measure. A matching one means a recheck writes NOTHING,
    #: which is what makes the 98.7% rebuild rate affordable (invariant 6).
    source_version_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    revision: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="1")

    # --- time ---
    #: When the event happened: the source transaction's `started_at`, carried under a name that says
    #: what it means rather than where it came from. Partition key of the fact table, and part of its
    #: identity, so the two roles are served by one column and cannot disagree.
    #:
    #: Nullable, matching `log_transactions.started_at`, which is why a DEFAULT partition is mandatory
    #: rather than defensive: a transaction all of whose entries lack a parsable timestamp has none.
    event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: Tenant-LOCAL calendar day, computed through the customer's timezone. Distinct from event_time's
    #: UTC day: for a UK warehouse the two diverge by an hour for half the year.
    business_date: Mapped[date_type | None] = mapped_column(Date, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # --- operation ---
    method: Mapped[str | None] = mapped_column(String(128), nullable=True)
    transaction_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    transaction_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # --- subject ---
    item_number: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lot_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    order_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    delivery_number: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- place ---
    warehouse: Mapped[str | None] = mapped_column(String(16), nullable=True)
    warehouse_id: Mapped[str | None] = mapped_column(String(16), nullable=True)
    from_location: Mapped[str | None] = mapped_column(String(32), nullable=True)
    to_location: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # --- actor ---
    user_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    device_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    device_name: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- measures ---
    #: NUMERIC, never float: quantities are fractional (0.333333 is a live value) and get summed over a
    #: month, so float would drift with nothing reporting it.
    quantity: Mapped[object | None] = mapped_column(Numeric(20, 6), nullable=True)
    quantity_classification: Mapped[str | None] = mapped_column(String(16), nullable=True)
    #: The long tail, for a measure nobody has thought of yet. Wide row or not, a field absent at 60
    #: days is gone, and this is the cheapest insurance against having missed one.
    attributes: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")


# Entity
class AnalyticsFact(FactColumns, Base):
    """One row per transaction: its CURRENT contribution. Written only by N3."""

    __tablename__ = "analytics_facts"

    __table_args__ = (
        # F3, adjusted for what PostgreSQL will accept. A unique constraint on a partitioned table
        # MUST contain every partition-key column, so the key is (customer_code, source_transaction_id,
        # event_time) rather than (source_transaction_id, source_started_at).
        #
        # That is the same identity, not a weaker one: `event_time` IS the source transaction's
        # `started_at` (see the column comment), which is exactly the value F3 asks for. Keying on it
        # disambiguates two source rows sharing an id across partitions identically, AND keeps the
        # partition key in the predicate every read carries, so reads prune. Keying on
        # `source_started_at` instead would satisfy F3 literally and then prune nothing.
        #
        # NULLS NOT DISTINCT so a transaction with no start instant is still unique, matching how
        # log_transactions enforces its own identity.
        UniqueConstraint("customer_code", "source_transaction_id", "event_time",
                         name="uq_analytics_facts_source", postgresql_nulls_not_distinct=True),
        # Every read pins the tenant first, then the time window.
        Index("ix_analytics_facts_customer_event", "customer_code", "event_time"),
        # The rollup folder groups by tenant and local day.
        Index("ix_analytics_facts_customer_date", "customer_code", "business_date"),
        # The retention cursor (F6) tracks the maximum created_at among fully processed rows.
        Index("ix_analytics_facts_created", "created_at"),
        {"postgresql_partition_by": "RANGE (event_time)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    #: Write time, not event time. F6's retention cursor is a WRITE-time position while the worker is
    #: driven by event-time ranges, and this is the field it reads. Held as ONE named constant in the
    #: worker, because a deferred upstream move to update-in-place would stop this advancing and break
    #: the cursor silently.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


# Entity
class AnalyticsFactLedger(FactColumns, Base):
    """Every version of every fact, append-only. Written only by N3; read by M1 at a pinned revision.

    Retained LONGER than raw data on purpose: it is the only thing that makes a training run repeatable
    months later, once the entries it derives from have been dropped.
    """

    __tablename__ = "analytics_fact_ledger"

    __table_args__ = (
        # One row per VERSION, so identity includes the revision. Deliberately NOT unique on the source
        # key alone: that is the fact table's constraint, and applying it here would allow one version.
        #
        # `recorded_at` is present because PostgreSQL requires a unique constraint on a partitioned
        # table to contain every partition-key column. It is functionally dependent on the revision --
        # a given version was written once, at one instant -- so including it neither weakens nor
        # widens the key.
        UniqueConstraint("customer_code", "source_transaction_id", "source_started_at", "revision",
                         "recorded_at",
                         name="uq_analytics_ledger_version", postgresql_nulls_not_distinct=True),
        Index("ix_analytics_ledger_customer_recorded", "customer_code", "recorded_at"),
        # M1 reads at a pinned revision across one tenant.
        Index("ix_analytics_ledger_revision", "customer_code", "revision"),
        # Partitioned on WRITE time, not event time: the ledger is genuinely append-only, so its
        # natural growth axis is when a version was recorded.
        {"postgresql_partition_by": "RANGE (recorded_at)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    #: Partition key. NOT NULL, unlike the fact table's event_time: a version is always recorded at a
    #: known instant even when the transaction it describes has no timestamp of its own.
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    #: Why this version was written, so a churning ledger can be explained rather than guessed at.
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
