# log_entry_assignment.py — which transaction currently owns each raw entry
#
#   One row means: "entry E currently belongs to transaction T at position N".
#
#   Why this table exists. Stage 2 used to write the grouping result back onto log_entries
#   (transaction_id / seq) and clear it again through an ON DELETE SET NULL cascade. Because
#   transaction_id was indexed, every rewrite touched the heap AND the index, and the unsealed tail is
#   regrouped repeatedly before it seals. Measured on production 2026-08-05:
#
#       log_entries: n_tup_upd = 105,838,123   n_tup_hot_upd = 162   -> 0.0% HOT
#
#   Separating the CURRENT INTERPRETATION from the RAW EVIDENCE fixed that: log_entries is
#   insert-only, and the churn lives here — in a small table whose whole purpose is to be replaced.
#
#   ---- Shape, and why it is this shape ----
#
#   SOFT REFERENCES, no foreign keys. This is not a style preference; it is forced. A foreign key
#   makes the referenced table's partitions impossible to remove:
#
#       ALTER TABLE ... DETACH PARTITION  -> ERROR: violates foreign key constraint
#       DROP TABLE <partition>            -> ERROR: other objects depend on it
#
#   Retention IS dropping partitions, so FKs and partitioning cannot coexist. Dropping them also made
#   the hottest write path ~4x faster: 200k assignment inserts went from 1,060 ms (two FK triggers,
#   200,000 calls each) to 249 ms.
#
#   The cost is that deletes no longer cascade. Every path that removes entries or transactions must
#   delete the matching assignments itself — see the four call sites listed in
#   docs/plan/2026-08-05_20-32_daily-partitioning.md, each covered by a test.
#
#   UNIQUE NULLS NOT DISTINCT (entry_id, entry_ts) — in that ORDER, and NOT a primary key.
#
#   Not a primary key because PostgreSQL silently forces every PK column to NOT NULL, and entry_ts
#   must stay nullable (log_entries.timestamp is). Verified: declaring a nullable column in a PK
#   silently makes it NOT NULL, and the NULL insert then fails.
#
#   NULLS NOT DISTINCT (PG15+; production is 16.14) is what preserves the guarantee for those rows —
#   a plain UNIQUE treats two NULLs as different and would let one timestamp-less entry collect
#   several assignments. Verified: the second insert for the same (entry_id, NULL) is rejected.
#
#   The ORDER matters independently. entry_ts has to be in the constraint so the table can be
#   partitioned by day (any unique index on a partitioned table must contain the partition key), but
#   it must NOT come first. Measured on 300k rows, looking up by entry_id alone:
#
#       (entry_id)              0.045 ms  index scan
#       (entry_ts, entry_id)   10.8   ms  SEQUENTIAL SCAN     <- 240x worse
#       (entry_id, entry_ts)    0.046 ms  index scan
#
#   Three hot paths filter on entry_id with no time bound (load_transaction_by_entry, is_unassigned,
#   belongs_to_transaction), so partition-key-first would be a severe regression.
#
#   entry_ts is denormalised from the owning entry so an assignment always lands in the same daily
#   partition as the entry it describes, letting the pair be dropped together. It is nullable because
#   log_entries.timestamp is: an entry with no parsed timestamp still needs an assignment, and once
#   partitioned both land in the DEFAULT partition.
#
#   customer_code stays a soft tenant key, consistent with every other log table, so tenant-scoped
#   cleanup and consistency checks need no join.

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.config.database import Base


# Entity
class LogEntryAssignment(Base):
    __tablename__ = "log_entry_assignment"

    __table_args__ = (
        # At most one current assignment per entry. entry_id FIRST (see docstring), and
        # NULLS NOT DISTINCT so a timestamp-less entry cannot collect several.
        UniqueConstraint("entry_id", "entry_ts", name="uq_log_entry_assignment_entry",
                         postgresql_nulls_not_distinct=True),
        # the hot read: "give me this transaction's entries, in order" — index-only for the ids.
        Index("ix_log_entry_assignment_txn", "transaction_id", "seq"),
        # tenant-scoped cleanup and consistency checks.
        Index("ix_log_entry_assignment_customer", "customer_code"),
        # supports dropping a day's assignments alongside the matching entry partition, and the
        # tenant+day consistency checks retention runs before a drop.
        Index("ix_log_entry_assignment_entry_ts", "entry_ts"),
        # NOTE: `primary_key=True` on the id column below is the ORM's row identity ONLY. The DDL
        # SQLAlchemy would emit from it (`PRIMARY KEY (id)`) is invalid on a partitioned table and is
        # never used — Alembic builds this schema, nothing calls create_all (pinned by a test in
        # tests/test_partitioning_chunk23.py). Identity is enforced in the database by the UNIQUE
        # above. Keeping the ORM key as `id` alone is deliberate: making it (id, key) would force
        # every `db.get(Model, id)` call site to pass a tuple.
        # Range-partitioned by UTC day (see app/persistence/partitioning.py and migration
        # a1f6d70b3e92). Retention is a DROP of the day's partition rather than a DELETE + VACUUM that
        # reads the whole table.
        # Co-partitioned with log_entries on the SAME grain, so a day's entries and that day's
        # assignments are dropped together and retention can never strand one without the other.
        {"postgresql_partition_by": "RANGE (entry_ts)"},
    )

    # entry_id alone identifies a row (entry_ts is functionally dependent on it), so it is the
    # ORM-level identity. The DATABASE guarantee is the UNIQUE above, not a PK — see the docstring.
    entry_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    # denormalised from log_entries.timestamp: the future partition key. Nullable, matching the
    # source column, which is why this cannot be part of a primary key.
    entry_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # soft reference — NO foreign key (see docstring).
    transaction_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    # position within the transaction, 0-based, in stream order.
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    # soft tenant key — denormalised so tenant cleanup needs no join.
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False)
    # clock_timestamp(), not now(): now() is transaction_timestamp(), so every row written by one
    # regroup would share an identical stamp and lose the ordering information.
    assigned_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("clock_timestamp()"), nullable=False)
