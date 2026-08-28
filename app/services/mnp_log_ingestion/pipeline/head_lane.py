"""P4, chunk 72: the head lane - process only NEW lines against parked state.

The rebuild lane re-reads a padded window every tick to conclude ~98.7% of it did not change; since
S3 the writes are already minimal, so the re-reads are the only remaining waste. This module removes
them for the common case: a window whose range lies entirely beyond the tenant's FRONTIER is planned
from just its own rows plus the parked open conversations (`log_open_stream`), producing one UPDATE
per continued conversation and one INSERT per new one.

Three design rules, each earned earlier in this system's history:

- **A plan is data.** `build_plan` is read-only and returns what WOULD be written; `apply_plan`
  writes it. Shadow mode exercises the exact code `on` runs, minus the writes - an unexercised
  persist path is how second write authorities go wrong (section 18p's lesson).
- **Never guess: fall back.** Anything surprising routes the window to the rebuild lane with a
  named reason. The rebuild lane remains the authority and the safety net, always.
- **Same grouper, same fingerprints.** The plan is built by the very `_group` the rebuild uses,
  seeded exactly as the S4a shadow seeds it, and the resulting fingerprints must be byte-identical
  to a rebuild's - which is what the shadow comparison checks on every window before `on` is ever
  trusted.
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import async_session
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_stream_frontier import LogStreamFrontier
from app.persistence.models.log_transaction import LogTransaction
from app.services.analytics import pending_windows as analytics_tickets
from app.services.mnp_log_ingestion.pipeline import assignments, fingerprints, stream_state
from app.services.mnp_log_ingestion.timefmt import set_display_timezone
from app.settings import settings

logger = logging.getLogger(__name__)


class PlanStale(RuntimeError):
    """The parked row changed between plan and apply. The window retries through the ordinary
    machinery; the fresh plan sees the new truth. Applying blind would clobber someone's write."""


def mode() -> str:
    m = (settings.stage2_head_lane or "").strip().lower()
    return m if m in ("off", "shadow", "on") else "off"


async def get_frontier(db: AsyncSession, customer_code: str) -> datetime | None:
    return await db.scalar(select(LogStreamFrontier.frontier_ts).where(
        LogStreamFrontier.customer_code == customer_code))


async def advance_frontier(customer_code: str, hi: datetime) -> None:
    """Greatest-wins upsert: both lanes call this after a committed window; a late backfill window
    must never drag the bookmark back."""
    async with async_session() as db:
        stmt = pg_insert(LogStreamFrontier).values(
            customer_code=customer_code, frontier_ts=hi)
        await db.execute(stmt.on_conflict_do_update(
            index_elements=["customer_code"],
            set_={"frontier_ts": func.greatest(LogStreamFrontier.frontier_ts, stmt.excluded.frontier_ts)}))
        await db.commit()


@dataclass
class Continuation:
    txn_id: uuid.UUID
    started_at: datetime
    old_row_fp: str | None
    values: dict
    is_sealed: bool
    row_fp: str
    members_fp: str
    entries: list


@dataclass
class Creation:
    txn_id: uuid.UUID
    values: dict
    is_sealed: bool
    row_fp: str
    members_fp: str
    entries: list


@dataclass
class HeadPlan:
    ok: bool
    fallback: str | None = None
    lo: datetime | None = None
    hi: datetime | None = None
    continued: list = field(default_factory=list)
    created: list = field(default_factory=list)
    open_streams: list = field(default_factory=list)
    pending: list = field(default_factory=list)


def _fallback(reason: str) -> HeadPlan:
    return HeadPlan(ok=False, fallback=reason)


async def build_plan(customer_code: str, lo: datetime, hi: datetime) -> HeadPlan:
    """Read-only: what the head lane WOULD write for this window, or a named fallback reason."""
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt

    async with async_session() as db:
        frontier = await get_frontier(db, customer_code)
        if frontier is None:
            return _fallback("no_frontier")
        if lo < frontier:
            return _fallback("behind_frontier")

        rows = list((await db.execute(
            select(LogEntry).where(
                LogEntry.customer_code == customer_code,
                LogEntry.timestamp >= lo,
                LogEntry.timestamp <= hi,
                assignments.is_unassigned(),
            ).order_by(LogEntry.timestamp.asc().nullslast(),
                       LogEntry.source_file.asc(), LogEntry.line_number.asc()))).scalars().all())
        if not rows:
            return _fallback("no_rows")
        # NULL-timestamp entries never enter a windowed read - `timestamp >= lo` excludes them in
        # the REBUILD lane too (same predicate in regroup_window's entry read), so ignoring them
        # here is parity, not a gap. Only a full rebuild reaches them, in both lanes.

        state = await stream_state.load(db, customer_code, lo)
        if state["refusals"].get("clock_went_backwards"):
            return _fallback("clock_went_backwards")
        parked = [r for r in state["streams"] if state["entries_by_txn"].get(r.transaction_id)]
        txn_of_entry = {e.id: r.transaction_id
                        for r in parked for e in state["entries_by_txn"][r.transaction_id]}
        seed = {"streams": [
                    {"thread": r.thread, "user_ctx": r.user_ctx, "is_current": r.is_current,
                     "open_pos": (r.open_ts_is_null, r.open_ts, r.open_source_file,
                                  r.open_line_number),
                     "entries": state["entries_by_txn"][r.transaction_id]}
                    for r in parked],
                "pending": state["pending"]}

        set_display_timezone(await dt.get_customer_timezone(db, customer_code))
        tz = await dt.get_customer_timezone(db, customer_code)
        seal_cutoff, abandon_cutoff = await dt._cutoffs(db, customer_code)

        builders = [b for b in dt._group(rows, seed=seed) if b.entries]
        gap = timedelta(seconds=settings.log_open_gap_seconds)

        # Which builders continue a parked conversation, which are new, which must stay open.
        seeded_ids = set(txn_of_entry)
        plan = HeadPlan(ok=True, lo=lo, hi=hi)
        created_ids: list[uuid.UUID] = []
        parked_rows: dict[uuid.UUID, object] = {}
        if parked:
            parked_rows = {r.id: r for r in (await db.execute(
                select(LogTransaction.id, LogTransaction.started_at, LogTransaction.sealed,
                       LogTransaction.row_fingerprint, LogTransaction.members_fingerprint)
                .where(LogTransaction.id.in_([r.transaction_id for r in parked])))).all()}
            if len(parked_rows) != len(parked):
                return _fallback("parked_vanished")
            if any(r.sealed for r in parked_rows.values()):
                return _fallback("parked_sealed")

        for b in builders:
            b.entries.sort(key=dt._entry_stream_order)
            owners = {txn_of_entry[e.id] for e in b.entries if e.id in seeded_ids}
            if len(owners) > 1:
                return _fallback("parked_merge")

            # still-open builders are parked, not persisted as final - mirror the S4a save rule
            last = max((e.timestamp for e in b.entries if e.timestamp is not None), default=None)
            is_open = last is not None and (hi - last) < gap
            anchor = b.entries[0]
            if is_open and anchor.user_ctx is None and dt._entry_user(anchor) is None:
                return _fallback("anonymous_open")

            values = dt._cap_over_length(b.compute(), customer_code)
            is_sealed = dt._is_sealed(values, seal_cutoff, abandon_cutoff)
            r_fp = fingerprints.row(values, sealed=is_sealed, tenant_timezone=tz)
            m_fp = fingerprints.members(e.id for e in b.entries)

            if owners:
                tid = owners.pop()
                stored = parked_rows[tid]
                if values.get("started_at") != stored.started_at:
                    return _fallback("started_at_moved")
                if r_fp == stored.row_fingerprint and m_fp == stored.members_fingerprint:
                    # The unchanged verdict, same as _persist's: a parked conversation this window
                    # merely re-derived (e.g. closed by a same-key successor without gaining an
                    # entry) writes nothing - but may still need re-parking below.
                    if is_open:
                        plan.open_streams.append({
                            "thread": anchor.thread, "user_ctx": anchor.user_ctx,
                            "transaction_id": tid,
                            "has_request": any(e.entry_type.value == "request"
                                               for e in b.entries),
                            "last_entry_ts": last, "open_pos": b.open_pos, "is_current": True})
                    continue
                plan.continued.append(Continuation(
                    txn_id=tid, started_at=stored.started_at, old_row_fp=stored.row_fingerprint,
                    values=values, is_sealed=is_sealed, row_fp=r_fp, members_fp=m_fp,
                    entries=list(b.entries)))
            else:
                tid = dt._txn_id(b.entries)
                created_ids.append(tid)
                plan.created.append(Creation(
                    txn_id=tid, values=values, is_sealed=is_sealed, row_fp=r_fp, members_fp=m_fp,
                    entries=list(b.entries)))

            if is_open:
                plan.open_streams.append({
                    "thread": anchor.thread, "user_ctx": anchor.user_ctx, "transaction_id": tid,
                    "has_request": any(e.entry_type.value == "request" for e in b.entries),
                    "last_entry_ts": last, "open_pos": b.open_pos, "is_current": True})

        if created_ids:
            existing = await dt._existing_transaction_ids(
                db, customer_code, created_ids,
                window=dt._clash_window([e.timestamp for b2 in builders for e in b2.entries]))
            if existing:
                return _fallback("id_clash")
        return plan


async def apply_plan(customer_code: str, plan: HeadPlan) -> dict:
    """Write the plan: the `on` path, and the code the shadow already exercised minus these writes."""
    from app.services.mnp_log_ingestion.pipeline import derive_transactions as dt
    from sqlalchemy import text as sa_text

    async with async_session() as db:
        await db.execute(select(func.pg_advisory_xact_lock(func.hashtext(customer_code))))
        await db.execute(sa_text(
            f"SET LOCAL statement_timeout = {int(settings.log_worker_statement_timeout_ms)}"))
        set_display_timezone(await dt.get_customer_timezone(db, customer_code))

        for c in plan.continued:
            # Optimistic concurrency: the parked row must still be what the plan saw. NULL-safe via
            # IS NOT DISTINCT FROM semantics (both sides may be NULL on pre-S3 rows).
            current = await db.scalar(select(LogTransaction.row_fingerprint).where(
                LogTransaction.id == c.txn_id,
                LogTransaction.started_at.is_(None) if c.started_at is None
                else LogTransaction.started_at == c.started_at))
            if current != c.old_row_fp:
                raise PlanStale(f"transaction {c.txn_id} changed between plan and apply")
            await dt._update_transaction(db, tid=c.txn_id, started_at=c.started_at,
                                         values=c.values, is_sealed=c.is_sealed,
                                         row_fp=c.row_fp, members_fp=c.members_fp)
            await assignments.write(db, transaction_id=c.txn_id, entries=c.entries,
                                    customer_code=customer_code)
        for c in plan.created:
            await dt._write_transaction(db, tid=c.txn_id, values=c.values, is_sealed=c.is_sealed,
                                        entries=c.entries, customer_code=customer_code,
                                        row_fp=c.row_fp, members_fp=c.members_fp)

        await stream_state.save(db, customer_code, streams=plan.open_streams, pending=plan.pending)
        starts = ([c.started_at for c in plan.continued]
                  + [c.values.get("started_at") for c in plan.created]) or [plan.lo]
        await analytics_tickets.publish_for_transactions(
            db, customer_code, started_ats=[min(s for s in starts if s is not None), plan.hi])
        await db.commit()
    await advance_frontier(customer_code, plan.hi)
    stats = {"mode": "head", "customers": 1, "by_status": {},
             "transactions_created": len(plan.created),
             "transactions_row_only": 0, "transactions_rewritten": len(plan.continued),
             "transactions_unchanged": 0, "transactions_deleted": 0, "transactions_skipped": 0,
             "entries_scanned": sum(len(c.entries) for c in plan.created)
                                + sum(len(c.entries) for c in plan.continued),
             "entries_assigned": sum(len(c.entries) for c in plan.created)
                                 + sum(len(c.entries) for c in plan.continued),
             "entries_skipped": 0, "orphan_entries": 0,
             "transactions_sealed": sum(int(c.is_sealed) for c in plan.created)
                                    + sum(int(c.is_sealed) for c in plan.continued)}
    logger.info("Stage 2 head lane [%s]: applied %d continuation(s), %d creation(s) for %s..%s",
                customer_code, len(plan.continued), len(plan.created), plan.lo, plan.hi)
    return stats


async def shadow_compare(customer_code: str, plan: HeadPlan) -> bool:
    """After the rebuild (the authority) executed the same window: does the plan's world match?

    Fingerprint equality per planned id, plus 'no transaction the rebuild created in the window is
    missing from the plan'. Logged either way; a DIVERGED line is the signal that stops `on`."""
    async with async_session() as db:
        planned = {c.txn_id: (c.row_fp, c.members_fp) for c in plan.continued} | {
            c.txn_id: (c.row_fp, c.members_fp) for c in plan.created}
        rows = {r.id: (r.row_fingerprint, r.members_fingerprint) for r in (await db.execute(
            select(LogTransaction.id, LogTransaction.row_fingerprint,
                   LogTransaction.members_fingerprint)
            .where(LogTransaction.id.in_(list(planned))))).all()} if planned else {}
        rebuilt_new = set((await db.execute(
            select(LogTransaction.id).where(
                LogTransaction.customer_code == customer_code,
                LogTransaction.started_at >= plan.lo,
                LogTransaction.started_at <= plan.hi))).scalars().all())
    mismatched = [str(t) for t, fps in planned.items() if rows.get(t) != fps]
    missing = [str(t) for t in rebuilt_new
               if t not in planned and t not in {c.txn_id for c in plan.continued}]
    agreed = not mismatched and not missing
    if agreed:
        logger.info("Stage 2 head lane [%s]: shadow AGREED for %s..%s - %d continuation(s), "
                    "%d creation(s)", customer_code, plan.lo, plan.hi,
                    len(plan.continued), len(plan.created))
    else:
        logger.warning("Stage 2 head lane [%s]: shadow DIVERGED for %s..%s - %d fingerprint "
                       "mismatch(es) %s, %d rebuilt id(s) missing from the plan %s. Not promoting.",
                       customer_code, plan.lo, plan.hi, len(mismatched), mismatched[:3],
                       len(missing), missing[:3])
    return agreed
