"""Persistence for `log_entry_assignment` — which transaction currently owns each raw entry.

This module holds ONLY the storage concern. Deciding which entries belong together is Stage 2's
grouping algorithm (`derive_transactions._group`); recording that decision is here. Keeping them
apart is what lets the grouping logic stay untouched while the storage shape changes underneath it.

Every function is deliberately small, single-purpose, and takes an explicit session so the caller
controls the transaction boundary. Stage 2 needs delete + insert to commit atomically inside one
window rebuild, which is only possible if this module never commits on its own.

Background: Stage 2 used to write the grouping result back onto `log_entries` and clear it again via
an ON DELETE SET NULL cascade. Because `transaction_id` was indexed, every rewrite touched the heap
and the index, and the unsealed tail is regrouped many times before it seals — 105.8M updates at 0.0%
HOT in production. Moving the assignment here makes the raw table insert-only.
"""

import uuid
from typing import Sequence

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment


def is_unassigned():
    """Predicate: this entry has no current assignment.

    Replaces `LogEntry.transaction_id IS NULL` as the "needs grouping" signal. Returned as a
    predicate rather than a full query so callers keep their own window bounds — every use MUST stay
    time-scoped, because a whole-table anti-join over an append-only table does not scale.
    """
    return ~select(LogEntryAssignment.entry_id).where(
        LogEntryAssignment.entry_id == LogEntry.id
    ).exists()


def belongs_to_transaction(transaction_id: uuid.UUID):
    """Predicate: this entry is currently assigned to `transaction_id`.

    Replaces `LogEntry.transaction_id == ?` as a filter on an entry query. Returned as a predicate so
    callers can combine it with their own conditions.
    """
    return select(LogEntryAssignment.entry_id).where(
        LogEntryAssignment.entry_id == LogEntry.id,
        LogEntryAssignment.transaction_id == transaction_id,
    ).exists()


async def write(db: AsyncSession, *, transaction_id: uuid.UUID,
                entries: Sequence[LogEntry], customer_code: str) -> int:
    """Record `entries` as belonging to `transaction_id`, in the order given (seq = index).

    Takes the ENTRIES rather than their ids because each row also stores `entry_ts`, copied from the
    entry's own timestamp. Passing the entry guarantees the two agree by construction; passing ids
    plus a parallel list of timestamps would let them drift, and re-reading the timestamps here would
    be a database round-trip for something the caller already holds.

    Upserts on (entry_id, entry_ts): a regroup rebuilds the same window with the same deterministic
    transaction id, so re-writing an existing assignment must REPLACE it rather than raise. The
    newest grouping wins.

    Does not commit — the caller owns the transaction boundary.
    """
    if not entries:
        return 0
    rows = [{"entry_id": e.id, "entry_ts": e.timestamp, "transaction_id": transaction_id,
             "seq": i, "customer_code": customer_code}
            for i, e in enumerate(entries)]
    stmt = pg_insert(LogEntryAssignment).values(rows)
    await db.execute(stmt.on_conflict_do_update(
        constraint="uq_log_entry_assignment_entry",
        set_={"transaction_id": stmt.excluded.transaction_id,
              "seq": stmt.excluded.seq,
              "customer_code": stmt.excluded.customer_code},
    ))
    return len(rows)


async def delete_for_transactions(db: AsyncSession,
                                  transaction_ids: list[uuid.UUID]) -> int:
    """Drop the assignments of the given transactions. Returns the number removed.

    This is the explicit replacement for relying on `ON DELETE SET NULL` to clear the raw rows.
    `regroup_window` deletes transactions and then re-selects unassigned entries in the SAME
    transaction with no intermediate commit, so the clearing must be visible to that re-select.
    Calling this alongside the transaction delete keeps identical MVCC visibility, while stating the
    intent in code instead of inferring it from a cascade.
    """
    if not transaction_ids:
        return 0
    res = await db.execute(delete(LogEntryAssignment).where(
        LogEntryAssignment.transaction_id.in_(transaction_ids)))
    return res.rowcount or 0


async def delete_for_customer(db: AsyncSession, customer_code: str | None) -> int:
    """Drop every assignment for one tenant, or for ALL tenants when `customer_code` is None.

    The None case mirrors `regroup_all`, which rebuilds every transaction from scratch and so must
    clear every assignment. Kept explicit rather than defaulted, because "delete everything" should
    never be something a caller reaches by omission.

    Does not commit — the caller owns the transaction boundary.
    """
    stmt = delete(LogEntryAssignment)
    if customer_code is not None:
        stmt = stmt.where(LogEntryAssignment.customer_code == customer_code)
    res = await db.execute(stmt)
    return res.rowcount or 0


async def delete_for_entries(db: AsyncSession, entry_ids: list[uuid.UUID]) -> int:
    """Drop the assignments of the given entries.

    Used where ENTRIES are deleted directly (the date-range delete), which the transaction-side
    cleanup would miss: the transaction survives, but its entries are gone, leaving assignments that
    reference rows which no longer exist.
    """
    if not entry_ids:
        return 0
    res = await db.execute(delete(LogEntryAssignment).where(
        LogEntryAssignment.entry_id.in_(entry_ids)))
    return res.rowcount or 0


async def load_seq_by_entry(db: AsyncSession,
                            transaction_ids: list[uuid.UUID]) -> dict[uuid.UUID, int]:
    """`{entry_id: seq}` for the given transactions — one bulk query, not one per entry.

    Readers need the position to order a transaction's timeline; this is what replaces reading
    `LogEntry.seq` directly.
    """
    if not transaction_ids:
        return {}
    rows = (await db.execute(
        select(LogEntryAssignment.entry_id, LogEntryAssignment.seq).where(
            LogEntryAssignment.transaction_id.in_(transaction_ids))
    )).all()
    return {entry_id: seq for entry_id, seq in rows}


async def load_transaction_by_entry(db: AsyncSession,
                                    entry_ids: list[uuid.UUID]) -> dict[uuid.UUID, uuid.UUID]:
    """`{entry_id: transaction_id}` for entries that have one.

    The inverse of `load_seq_by_entry`, for list endpoints that show which transaction each entry
    belongs to. One bulk query for a page of entries rather than one per row. Entries with no
    assignment are simply absent from the result — the caller reports them as unassigned.
    """
    if not entry_ids:
        return {}
    rows = (await db.execute(
        select(LogEntryAssignment.entry_id, LogEntryAssignment.transaction_id).where(
            LogEntryAssignment.entry_id.in_(entry_ids))
    )).all()
    return {entry_id: txn_id for entry_id, txn_id in rows}


async def load_entries(db: AsyncSession, transaction_ids: list[uuid.UUID], *,
                       limit: int) -> list[tuple[LogEntry, uuid.UUID, int]]:
    """`(entry, owning transaction_id, seq)` for the given transactions, in (transaction, seq) order.

    Readers need all three together — the row to render, which transaction to group it under, and
    where it sits in that transaction — so this returns them in one query instead of making callers
    re-derive the grouping from a column on LogEntry.

    `limit` is applied in SQL, not after materialising: the feed caps how many entries it will render
    and must not load an unbounded set to find that out.
    """
    if not transaction_ids:
        return []
    rows = (await db.execute(
        select(LogEntry, LogEntryAssignment.transaction_id, LogEntryAssignment.seq)
        .join(LogEntryAssignment, LogEntryAssignment.entry_id == LogEntry.id)
        .where(LogEntryAssignment.transaction_id.in_(transaction_ids))
        .order_by(LogEntryAssignment.transaction_id, LogEntryAssignment.seq)
        .limit(limit)
    )).all()
    return [(entry, txn_id, seq) for entry, txn_id, seq in rows]


async def entry_ids_for_transactions(db: AsyncSession,
                                     transaction_ids: list[uuid.UUID]) -> list[uuid.UUID]:
    """Entry ids belonging to the given transactions, in (transaction, seq) order.

    Ordered here rather than by the caller so the index `(transaction_id, seq)` does the work.
    """
    if not transaction_ids:
        return []
    return list((await db.execute(
        select(LogEntryAssignment.entry_id)
        .where(LogEntryAssignment.transaction_id.in_(transaction_ids))
        .order_by(LogEntryAssignment.transaction_id, LogEntryAssignment.seq)
    )).scalars().all())
