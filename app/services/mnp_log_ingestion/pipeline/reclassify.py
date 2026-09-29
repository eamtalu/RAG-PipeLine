"""Chunk 128: repair entries stored before the parser learned the new log format.

From 2026-09-29 13:00 the production servers stamp a request id on their request, response and M3
lines. Until this chunk shipped, Stage 1 classified those request and response lines as `info`, so
Stage 2 built headless, incomplete transactions from them. The raw text of every entry is kept in
`raw_body`, so the rows can be re-parsed in place: `entry_type`, `message`, `fields` and the M3
columns are set to what the parser says now; nothing else on the row changes (the id, the hash,
the timestamp and the provenance are the row's identity and stay). The touched time range is then
ticketed for re-stitching, which the Stage 2 worker picks up on its next cycle.

`log_entries` is append-only by design. This is a bounded, logged, one-off correction of rows that
were stored with a wrong classification, not a new write path: it only ever touches rows whose raw
text carries the new format's stamp and whose stored classification disagrees with the parser.

The transactions built from the broken rows are deleted outright (with their assignments and the
customer's saved stream state) before the window is ticketed. Rehearsed on the real sample: leaving
them for the windowed rebuild to free is not enough, because the broken rows were anchored on other
lines (a POST's body, a stray narration line) and had other spans, so the rebuilt conversations
clashed with sealed rows outside each window and were skipped, orphaning their entries. With the
rows gone, the rebuild starts clean and mints the same deterministic ids a full regroup would.
"""

import logging
from datetime import datetime

from sqlalchemy import delete, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_open_stream import LogOpenStream, LogPendingRequest
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_transaction import LogTransaction
from app.services.mnp_log_ingestion.parsers.m3_dotnet_parser import M3DotNetLogParser

logger = logging.getLogger(__name__)

STAMPED_LOGGERS = ("Server.CommonCode.ApiLogHandler", "M3WebServiceClassLib.CommonCode.WebService")
_BATCH = 500


async def reclassify_stamped(db: AsyncSession, customer_code: str, *, since: datetime, until: datetime | None = None,
                             dry_run: bool = False) -> dict:
    """Re-parse the stamped rows of one customer in [since, until) and fix the ones stored wrongly.
    Returns counts: scanned, changed, by former type; and the window ticketed for re-stitching."""
    parser = M3DotNetLogParser()
    stmt = (select(LogEntry)
            .where(LogEntry.customer_code == customer_code, LogEntry.timestamp >= since,
                   LogEntry.logger.in_(STAMPED_LOGGERS),
                   or_(LogEntry.message.like("REQUEST (ReqID%"), LogEntry.message.like("RESPONSE (ReqID%"),
                       LogEntry.message.like("(ReqID = %")))
            .order_by(LogEntry.timestamp))
    if until is not None:
        stmt = stmt.where(LogEntry.timestamp < until)
    rows = (await db.execute(stmt)).scalars().all()
    scanned = len(rows)
    changed = 0
    by_type: dict[str, int] = {}
    lo = hi = None
    for row in rows:
        records = parser.parse(row.raw_body or "")
        if not records:
            continue
        rec = records[0]
        wanted = LogEntryType(rec.entry_type)
        new_fields = rec.fields or {}
        if (row.entry_type == wanted and (row.fields or {}) == new_fields and row.message == rec.message):
            continue
        by_type[row.entry_type.value] = by_type.get(row.entry_type.value, 0) + 1
        changed += 1
        if row.timestamp is not None:
            lo = row.timestamp if lo is None or row.timestamp < lo else lo
            hi = row.timestamp if hi is None or row.timestamp > hi else hi
        if dry_run:
            continue
        await db.execute(update(LogEntry).where(LogEntry.id == row.id).values(
            entry_type=wanted, message=rec.message, fields=new_fields, mi_program=rec.mi_program,
            mi_transaction=rec.mi_transaction, result_status=rec.result_status, record_count=rec.record_count))
    ticket = None
    dropped = 0
    if changed and not dry_run and lo is not None and hi is not None:
        dropped = await _drop_transactions_owning(db, customer_code, lo, hi)
        db.add(LogRegroupPending(customer_code=customer_code, job_id=None, range_start=lo, range_end=hi))
        ticket = (lo, hi)
        await db.commit()
    logger.info("reclassify %s: scanned %d stamped rows, changed %d %s, dropped %d transaction(s)%s",
                customer_code, scanned, changed, by_type, dropped, " (dry run)" if dry_run else "")
    return {"customer_code": customer_code, "scanned": scanned, "changed": changed, "by_former_type": by_type,
            "transactions_dropped": dropped, "ticketed": [t.isoformat() for t in ticket] if ticket else None,
            "dry_run": dry_run}


async def _drop_transactions_owning(db: AsyncSession, customer_code: str, lo: datetime, hi: datetime) -> int:
    """Every transaction that owns an entry timestamped in [lo, hi], plus the customer's saved stream
    state (which points at transaction ids). The Stage 2 worker rebuilds the window from the raw
    lines on its next cycle."""
    owners = select(LogEntryAssignment.transaction_id).join(LogEntry, LogEntry.id == LogEntryAssignment.entry_id).where(
        LogEntryAssignment.customer_code == customer_code, LogEntry.timestamp >= lo, LogEntry.timestamp <= hi).distinct()
    ids = list((await db.execute(owners)).scalars().all())
    if ids:
        await db.execute(delete(LogEntryAssignment).where(LogEntryAssignment.transaction_id.in_(ids)))
        await db.execute(delete(LogTransaction).where(LogTransaction.id.in_(ids)))
    await db.execute(delete(LogOpenStream).where(LogOpenStream.customer_code == customer_code))
    await db.execute(delete(LogPendingRequest).where(LogPendingRequest.customer_code == customer_code))
    return len(ids)
