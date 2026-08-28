"""S4. Reading and writing the grouper's live state, so it survives a process boundary.

REPLACE, never incrementally mutate
-----------------------------------
The cycle is: read the state at the start of a window, seed `_group` with it, write the RESULT back.
Never "apply a delta to what is stored".

That is the crash-safety property, and it is the whole reason the design is shaped this way. All three
happen inside `regroup_window`'s existing transaction, so the state can never be AHEAD of the
assignments: either both commit or neither does. A failure leaves the ticket open (`:1062-1109`), the
next attempt reads the same pre-failure state, and it converges. An incremental mutation would leave
state describing entries that were never assigned, and nothing would ever notice.

Concurrency needs nothing new: `finalize_pending` already holds a per-tenant advisory lock, so two
workers cannot be inside the same tenant's window at once.

The state is not the truth
--------------------------
`log_transactions` and `log_entry_assignment` remain the truth. This is a CACHE of where the grouper
had got to, and every read of it is guarded (see `usable`). If it is wrong, stale, or missing, the
fallback is the re-derive that has always worked - which is why S4 keeps that path rather than deleting
it.
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.settings import settings

logger = logging.getLogger(__name__)

#: The three modes of `settings.stage2_stream_lookup`. Named here so a typo in configuration is a
#: startup-visible mistake rather than a silent fall through to "off".
OFF, SHADOW, ON = "off", "shadow", "on"
_MODES = (OFF, SHADOW, ON)


def mode() -> str:
    """The configured mode, defaulting to SHADOW for anything unrecognised.

    Defaults to SHADOW rather than OFF on a typo, deliberately: shadow costs a comparison and reports
    what it finds, whereas silently falling back to OFF would look exactly like S4 working perfectly
    and never diverging.
    """
    m = (settings.stage2_stream_lookup or "").strip().lower()
    if m not in _MODES:
        logger.warning("Stage 2: stage2_stream_lookup=%r is not one of %s; treating it as %r",
                       settings.stage2_stream_lookup, _MODES, SHADOW)
        return SHADOW
    return m


def usable(row, window_lo: datetime) -> tuple[bool, str | None]:
    """Whether a stored stream may be reused for a window starting at `window_lo`.

    Returns `(ok, reason_it_is_not)`, so a caller can COUNT the refusals by kind rather than merely
    knowing it fell back. That distinction is the point of shadow mode: "the guard declined 900 times
    because the tenant was idle" and "the guard declined 900 times because the clock went backwards"
    call for completely different responses.

    Two independent guards, each closing a specific failure mode from the plan's table:

      mode 2, the backfill: `last_entry_ts` must be BEFORE the window. A window replayed with an older
      clock would otherwise bind to a stream from the future, and there is no way to un-evict.

      mode 1, the quiet gap: the distance must be under `log_open_gap_seconds`, which is the same bound
      `evict_stale` applies in memory. Without it a stream idle for hours would absorb the next
      unrelated request into one bloated transaction.
    """
    if row.last_entry_ts is None:
        # A stream whose entries all lack a parsable timestamp cannot be reasoned about in time at all,
        # so it is never reused. Rare, and the fallback handles it correctly.
        return False, "no_timestamp"
    if row.last_entry_ts >= window_lo:
        return False, "clock_went_backwards"
    if (window_lo - row.last_entry_ts) >= timedelta(seconds=settings.log_open_gap_seconds):
        return False, "quiet_gap"
    return True, None


async def load(db: AsyncSession, customer_code: str, window_lo: datetime) -> dict:
    """The tenant's stored state, with each stream's entries reloaded and each refusal counted.

    Returns a plain dict rather than an object because `_group` needs the pieces separately and a
    wrapper would only be unpacked immediately.

    The entries come from `log_entry_assignment`, which already holds them - storing them a second time
    in the state tables would create a copy that can disagree with the first.
    """
    streams = (await db.execute(
        select(LogOpenStream).where(LogOpenStream.customer_code == customer_code))).scalars().all()

    refusals: dict[str, int] = {}
    fresh = []
    for row in streams:
        ok, why = usable(row, window_lo)
        if ok:
            fresh.append(row)
        else:
            refusals[why] = refusals.get(why, 0) + 1

    entries_by_txn: dict[uuid.UUID, list[LogEntry]] = {}
    if fresh:
        ids = [r.transaction_id for r in fresh]
        rows = (await db.execute(
            select(LogEntryAssignment.transaction_id, LogEntry)
            .join(LogEntry, LogEntry.id == LogEntryAssignment.entry_id)
            .where(LogEntryAssignment.customer_code == customer_code,
                   LogEntryAssignment.transaction_id.in_(ids))
            .order_by(LogEntryAssignment.transaction_id, LogEntryAssignment.seq))).all()
        for txn_id, entry in rows:
            entries_by_txn.setdefault(txn_id, []).append(entry)

    pending = (await db.execute(
        select(LogPendingRequest.entry_id)
        .where(LogPendingRequest.customer_code == customer_code))).scalars().all()
    pending_entries = []
    if pending:
        pending_entries = list((await db.execute(
            select(LogEntry).where(LogEntry.id.in_(list(pending))))).scalars().all())

    return {"streams": fresh, "entries_by_txn": entries_by_txn,
            "pending": pending_entries, "refusals": refusals,
            "stored_streams": len(streams)}


async def save(db: AsyncSession, customer_code: str, *, streams: list[dict],
               pending: list[LogEntry], touched: frozenset | None = None,
               consumed_entry_ids: set | None = None) -> None:
    """Write this tenant's state. Does NOT commit. Each stream dict carries its `server` (18v).

    Two contracts, chosen by `touched` (18v, chunk 76):

    - `touched is None`: `streams` and `pending` are the tenant's COMPLETE state. Whole
      DELETE-then-INSERT, as always. This is the head lane's apply: its plan re-parks every stream
      it still believes in, so the plan IS the complete description.
    - `touched` is a set of transaction ids: the caller is a WINDOWED rebuild, which can only speak
      for the transactions it freed or rebuilt. Deletes are scoped to those, to key collisions with
      the rows being written, and to pending requests among `consumed_entry_ids`; every other row
      SURVIVES. The whole-replace here was chunk 76's live incident: merged tickets overlap, and an
      overlapping older window's replace wiped a parked stream it never touched - the head lane
      then handed that conversation's response to the wrong transaction (the response FIFO shifts
      by one when the front conversation is missing).

    Crash safety is unchanged either way: everything runs inside the caller's transaction, so the
    state can never be AHEAD of the assignments.
    """
    if touched is None:
        await db.execute(delete(LogOpenStream).where(LogOpenStream.customer_code == customer_code))
        await db.execute(delete(LogPendingRequest).where(
            LogPendingRequest.customer_code == customer_code))
    else:
        if touched:
            await db.execute(delete(LogOpenStream).where(
                LogOpenStream.customer_code == customer_code,
                LogOpenStream.transaction_id.in_(list(touched))))
        for s in streams:
            await db.execute(delete(LogOpenStream).where(
                LogOpenStream.customer_code == customer_code,
                LogOpenStream.server == s["server"],
                LogOpenStream.thread.is_not_distinct_from(s["thread"]),
                LogOpenStream.user_ctx.is_not_distinct_from(s["user_ctx"])))
        # The new rows are the freshest activity on their threads, so any surviving stream on the
        # same (server, thread) is no longer the one a user-less line should inherit.
        for srv, th in {(s["server"], s["thread"]) for s in streams if s.get("is_current")}:
            await db.execute(update(LogOpenStream).where(
                LogOpenStream.customer_code == customer_code,
                LogOpenStream.server == srv,
                LogOpenStream.thread.is_not_distinct_from(th)).values(is_current=False))
        if consumed_entry_ids:
            await db.execute(delete(LogPendingRequest).where(
                LogPendingRequest.customer_code == customer_code,
                LogPendingRequest.entry_id.in_(list(consumed_entry_ids))))
    now = datetime.now(timezone.utc)
    for s in streams:
        pos = s["open_pos"]
        db.add(LogOpenStream(
            id=uuid.uuid4(), customer_code=customer_code, server=s["server"],
            thread=s["thread"], user_ctx=s["user_ctx"], transaction_id=s["transaction_id"],
            has_request=s["has_request"], last_entry_ts=s["last_entry_ts"],
            open_ts_is_null=bool(pos[0]), open_ts=pos[1] if not pos[0] else None,
            open_source_file=pos[2], open_line_number=pos[3],
            is_current=s["is_current"], created_at=now, updated_at=now))
    for e in pending:
        db.add(LogPendingRequest(
            id=uuid.uuid4(), customer_code=customer_code, entry_id=e.id,
            reqid=(e.fields or {}).get("reqid") if isinstance(e.fields, dict) else None,
            req_user=e.user_ctx, timestamp=e.timestamp, created_at=now))


async def reap() -> dict:
    """Delete state older than the TTL, across all tenants.

    REQUIRED, not optional (section 18d). `evict_stale` closes a stream when an ENTRY ARRIVES, so a
    tenant that stops ingesting leaves its streams open forever. Derived state could not leak because it
    was rebuilt from nothing every batch; persisted state can.

    Keyed on `updated_at` rather than on `last_entry_ts`: what matters is how long ago the GROUPER last
    touched the row, not how old the log line was. A backfill of month-old data writes state whose
    `last_entry_ts` is a month back and which is being actively used.

    `count(*)` on each table is the only health signal there is - a number that only grows is the alarm,
    and there is no upstream event to catch it - so both are returned on every sweep.
    """
    cutoff_seconds = settings.stage2_stream_ttl_seconds
    async with async_session() as db:
        cutoff = await db.scalar(
            select(func.clock_timestamp() - timedelta(seconds=cutoff_seconds)))
        s = await db.execute(delete(LogOpenStream).where(LogOpenStream.updated_at < cutoff))
        p = await db.execute(delete(LogPendingRequest).where(LogPendingRequest.created_at < cutoff))
        await db.commit()
        live_s = await db.scalar(select(func.count()).select_from(LogOpenStream)) or 0
        live_p = await db.scalar(select(func.count()).select_from(LogPendingRequest)) or 0
    stats = {"streams_reaped": s.rowcount or 0, "pending_reaped": p.rowcount or 0,
             "streams_live": live_s, "pending_live": live_p}
    if stats["streams_reaped"] or stats["pending_reaped"]:
        logger.info("Stage 2 stream-state reaper: %s", stats)
    return stats
