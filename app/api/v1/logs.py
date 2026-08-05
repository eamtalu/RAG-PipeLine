"""Transaction-log API — Phase 1a: ingest files + query raw entries (line level).

Transaction-level endpoints (GET /logs/transactions...) arrive in Phase 1b once Stage 2 grouping
exists. For now you can drop/upload a log file and query its parsed entries.
"""

import asyncio
import uuid
from datetime import date as date_type, datetime
from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import get_session
from app.settings import settings
from app.api.deps import get_current_customer, get_active_customer
from app.persistence.models.job import Job
from app.persistence.models.log_entry import LogEntry, LogEntryType
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_regroup_run import LogRegroupRun
from app.persistence.models.log_source_object import LogSourceObject, SourceObjectStatus
from app.services.mnp_log_ingestion.pipeline import assignments
from app.services.logspace_cleanup import purge_source_objects_files
from app.services.workers.log_parse_worker import reset_abandoned_objects
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion, get_log_ingestion, DOCUMENT_TYPE
from app.services.mnp_log_ingestion.pipeline.derive_transactions import (
    regroup_all, regroup_incremental, finalize_pending, run_finalize_tracked, reset_abandoned_windows,
)
from app.services.mnp_log_ingestion.render import render_transaction
from app.services.mnp_log_ingestion.timefmt import iso_display, from_display_to_utc, active_timezone_name
from app.services.mnp_log_ingestion.io_errors import is_disk_io_error, disk_io_detail

import logging
logger = logging.getLogger(__name__)
from app.services.log_agent.agent import LogDebugAgent, get_log_debug_agent

router = APIRouter(prefix="/logs", tags=["logs"])

MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MB — rotating log files can be large

# Runaway guard for the text-view renders. Rendering many entries into one big string is synchronous
# CPU work; we run it off the event loop via asyncio.to_thread (so it can't block a worker's
# heartbeat), and cap the entries a single render will materialize so a pathological selection (a
# huge burst, or future data growth) can't balloon memory. The normal max feed (limit=500) is
# ~14.5k entries, well under this. See docs/debugging-worker-timeout-outage.md.
MAX_RENDER_ENTRIES = 50_000


def _assigned_sort_key(pair: "tuple[int | None, LogEntry]"):
    """Sort key for ONE transaction's `(seq, entry)` pairs — `seq ASC NULLS LAST, line_number ASC`.

    Orders in Python rather than SQL (see view_transactions._render), replacing a global
    cross-transaction ORDER BY that spilled to disk. Each bucket is tiny (~17 entries), so the sorts
    are trivial.

    `seq` now comes from log_entry_assignment rather than a column on LogEntry, but it is still
    nullable in principle and `line_number` always was, so the key stays NULL-safe:
      - the `x is None` flags sort NULLs LAST (False < True), and
      - the `x or 0` substitutions stop Python comparing None to None — a plain `(seq, line_number)`
        key raises TypeError the moment it meets a NULL.
    """
    seq, e = pair
    return (seq is None, seq or 0, e.line_number is None, e.line_number or 0)


# strong refs to in-flight background finalize tasks (asyncio only weak-refs them, so without this
# the event loop could GC a task mid-run). Discarded on completion via add_done_callback.
_finalize_tasks: set = set()


async def read_pending_state(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
    finalize: bool = Query(
        default=False,
        description="Stitch first: apply the pending scoped regroup before reading, so the result "
                    "includes the freshest ingested data. Without it, reads return the last "
                    "fully-stitched data plus a `pending_regroup` flag (they never block).",
    ),
) -> dict:
    """SOFT read gate (shared by every transaction-read endpoint via Depends).

    Each ingest marks the time window it touched as pending (log_regroup_pending). Rather than
    blocking reads with a 409 until those windows are stitched, reads ALWAYS succeed and carry a
    `pending_regroup` block: the already-grouped transactions are complete and consistent (Stage 1
    only adds entries, so a query never shows a half-built transaction) — at worst the newest,
    not-yet-grouped tail is missing, and the flag says so. Passing finalize=true stitches first
    (same session, committed) so the read is fully current. This never raises: a burst of new data
    can't interrupt an analyst mid-session."""
    async def _open_backlog() -> tuple[int, datetime | None]:
        """Outstanding, still-retryable windows. Abandoned ones are excluded: they are parked
        awaiting a human, not queued work, and counting them would pin `pending` true forever.
        Matches what GET /regroup/status reports as its backlog."""
        return (await db.execute(
            select(func.count(), func.min(LogRegroupPending.created_at)).where(
                LogRegroupPending.customer_code == customer,
                LogRegroupPending.consumed_at.is_(None),
                LogRegroupPending.abandoned_at.is_(None),
            )
        )).one()

    count, oldest = await _open_backlog()
    if count and finalize:
        await finalize_pending(db, customer)
        # RE-COUNT rather than assume it is now clear. finalize_pending deliberately skips a window
        # that is inside its retry backoff (chunk 18), so it can legitimately leave work behind.
        # Asserting "pending: False" here would report a false all-clear — and the frontend now
        # polls this signal to decide when a fetched batch is visible, so a false clear makes it
        # reload a stale feed instead of waiting.
        count, oldest = await _open_backlog()
        return {"pending": bool(count), "pending_windows": count or 0,
                "oldest_pending_at": oldest.isoformat() if oldest else None,
                "finalized": True}
    return {"pending": bool(count), "pending_windows": count or 0,
            "oldest_pending_at": oldest.isoformat() if oldest else None}


def _pending_notice(pending: dict) -> str:
    """One-line banner for the plain-text views when newer data hasn't been stitched in yet."""
    if not pending.get("pending"):
        return ""
    since = pending.get("oldest_pending_at")
    when = f" since {since}" if since else ""
    return (f"⚠ {pending['pending_windows']} newer log window(s){when} are not yet stitched in — "
            f"re-request with finalize=true to include them.\n\n")


@router.post("/ingest", status_code=201)
async def ingest_log(
    file: UploadFile = File(...),
    customer: str = Depends(get_active_customer),
    log_ingestion: LogIngestion = Depends(get_log_ingestion),
):
    """Push a single log file for ingestion (Stage 1: parse → insert entries)."""
    data = await file.read()
    if len(data) > MAX_FILE_SIZE:
        raise HTTPException(413, detail="File exceeds 200 MB limit")
    if not data:
        raise HTTPException(400, detail="Empty file")

    job = await log_ingestion.ingest(data, file.filename or "unknown.log", customer)
    # pending_regroup here is OPTIMISTIC: Stage 1 runs in the background, so at this point we don't yet
    # know whether this file added new, timestamped entries (an all-duplicate or timestamp-less upload
    # leaves no window to stitch). Do NOT drive the banner off this value — it can disagree with reality
    # and make the banner appear then clear itself. Poll GET /logs/jobs/{id}: once status=completed its
    # `pending_regroup` is the authoritative per-upload signal; /logs/regroup/status is the tenant-wide
    # one. Then call POST /logs/regroup/finalize ("I'm done") to stitch the windows these files touched.
    return {"job_id": str(job.id), "filename": job.filename,
            "customer_code": job.customer_code, "status": job.status.value,
            "pending_regroup": True}


@router.post("/scan")
async def scan_logs(
    directory: str | None = Query(default=None, description="dir to scan; defaults to settings.log_source_dir"),
    customer: str = Depends(get_active_customer),
    db: AsyncSession = Depends(get_session),
    log_ingestion: LogIngestion = Depends(get_log_ingestion),
):
    """Read-only ingest of every file in a directory (e.g. live rotating logs).

    Files are NEVER moved or deleted — only their bytes are read and copied into storage.
    Safe to re-run: content-level dedup (entry_hash) means already-ingested lines are skipped,
    so re-scanning the same folder (or the growing active log) only adds genuinely-new entries.
    """
    src = Path(directory) if directory else Path(settings.log_source_dir)
    if not src.exists() or not src.is_dir():
        raise HTTPException(400, detail=f"Not a directory: {src}")

    files = sorted(p for p in src.iterdir() if p.is_file() and not p.name.startswith("."))
    results = []
    total_new = 0
    for p in files:
        try:
            data = p.read_bytes()
            job = await log_ingestion.ingest(data, p.name, customer, background=False)
            # Stage 1 ran in its own session; read the committed count via a fresh scalar query
            # (avoids the identity-map cached, stale Job object).
            inserted = await db.scalar(select(Job.chunk_count).where(Job.id == job.id)) or 0
            status_val = await db.scalar(select(Job.status).where(Job.id == job.id))
            total_new += inserted
            results.append({"file": p.name, "job_id": str(job.id), "inserted_new": inserted,
                            "status": status_val.value if status_val else "unknown"})
        except Exception as exc:  # one bad file shouldn't abort the whole scan
            results.append({"file": p.name, "error": str(exc)})

    return {"directory": str(src), "files_scanned": len(files), "total_new_entries": total_new, "results": results}


@router.get("/jobs/{job_id}")
async def get_log_job(job_id: uuid.UUID, customer: str = Depends(get_current_customer),
                      db: AsyncSession = Depends(get_session)):
    job = await db.get(Job, job_id)
    if not job or job.document_type != DOCUMENT_TYPE or job.customer_code != customer:
        raise HTTPException(404, detail="Log job not found")
    # AUTHORITATIVE per-upload finalize signal — the banner should key off THIS, not the optimistic
    # `pending_regroup` on the POST /ingest 201 (that fires before Stage 1 has run, so it can't know
    # the outcome). True iff this upload actually left an open window: a row is written only when the
    # file added new, timestamped entries (all-duplicate or timestamp-less uploads write none), and a
    # tenant-wide finalize clears it — so this flips to false exactly when the work is genuinely done.
    pending_regroup = bool(await db.scalar(
        select(func.count()).select_from(LogRegroupPending).where(
            LogRegroupPending.job_id == job.id,
            LogRegroupPending.consumed_at.is_(None),
        )
    ))
    return {
        "job_id": str(job.id),
        "filename": job.filename,
        "status": job.status.value,
        "entry_count": job.chunk_count,
        "pending_regroup": pending_regroup,
        "error": job.error,
        "created_at": job.created_at.isoformat(),
    }


@router.get("/entries")
async def list_entries(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
    job_id: uuid.UUID | None = None,
    entry_type: LogEntryType | None = None,
    level: str | None = None,
    mi_program: str | None = None,
    source_file: str | None = None,
    q: str | None = Query(default=None, description="case-insensitive match on message"),
    time_from: datetime | None = None,
    time_to: datetime | None = None,
    limit: int = Query(default=100, le=1000),
    offset: int = 0,
):
    """Line-level query over parsed log entries."""
    stmt = select(LogEntry).where(LogEntry.customer_code == customer)
    if job_id is not None:
        stmt = stmt.where(LogEntry.job_id == job_id)
    if entry_type is not None:
        stmt = stmt.where(LogEntry.entry_type == entry_type)
    if level is not None:
        stmt = stmt.where(LogEntry.level == level.upper())
    if mi_program is not None:
        stmt = stmt.where(LogEntry.mi_program == mi_program)
    if source_file is not None:
        stmt = stmt.where(LogEntry.source_file == source_file)
    if q:
        stmt = stmt.where(LogEntry.message.ilike(f"%{q}%"))
    if time_from is not None:
        stmt = stmt.where(LogEntry.timestamp >= from_display_to_utc(time_from))
    if time_to is not None:
        stmt = stmt.where(LogEntry.timestamp <= from_display_to_utc(time_to))

    stmt = stmt.order_by(LogEntry.timestamp, LogEntry.line_number).limit(limit).offset(offset)
    rows = (await db.execute(stmt)).scalars().all()

    return {
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "entries": [
            {
                "id": str(e.id),
                "job_id": str(e.job_id),
                "transaction_id": str(e.transaction_id) if e.transaction_id else None,
                "source_file": e.source_file,
                "line_number": e.line_number,
                "timestamp": iso_display(e.timestamp),
                "level": e.level,
                "logger": e.logger,
                "method": e.method,
                "entry_type": e.entry_type.value,
                "mi_program": e.mi_program,
                "mi_transaction": e.mi_transaction,
                "result_status": e.result_status,
                "record_count": e.record_count,
                "message": e.message,
            }
            for e in rows
        ],
    }


# ---------------------------------------------------------------------------
# Stage 2 (derived transactions)
# ---------------------------------------------------------------------------

def _txn_summary(t: LogTransaction) -> dict:
    return {
        "id": str(t.id),
        "method": t.method,
        "transaction_name": t.transaction_name,
        "transaction_type": t.transaction_type,
        "status": t.status.value,
        "user": t.user_name,
        "warehouse": t.warehouse,
        "company": t.company,
        "device_id": t.device_id,
        "reqid": t.reqid,
        "route": t.route,
        "item_number": t.item_number,
        "delivery_number": t.delivery_number,
        "order_number": t.order_number,
        "started_at": iso_display(t.started_at),
        "ended_at": iso_display(t.ended_at),
        "date": t.date.isoformat() if t.date else None,
        "duration_ms": t.duration_ms,
        "entry_count": t.entry_count,
        "mi_program_count": t.mi_program_count,
        "error_text": t.error_text,
        "request_summary": t.request_summary,
    }


@router.post("/regroup")
async def regroup_transactions(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
    incremental: bool = Query(default=False, description="True = only regroup the unsealed live tail (fast, what the worker runs); False = full rebuild of all transactions (historical backfill / repair)."),
):
    """Run Stage 2 grouping FOR THIS CUSTOMER. Default is a FULL rebuild; `incremental=true` regroups
    only the live tail.

    Both produce DETERMINISTIC transaction ids (uuid5 of each transaction's anchor entry), so a
    transaction keeps the same id across regroups — saved/cited ids stay valid.
    """
    return await (regroup_incremental(db, customer) if incremental else regroup_all(db, customer))


@router.post("/regroup/finalize", status_code=202)
async def finalize_regroup(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Finalize an upload session ("I'm done uploading") — regroup ONLY the time windows this
    customer's recent uploads touched, then clear them as pending. NON-BLOCKING.

    Each ingest records the time range it added (log_regroup_pending); the regroup consumes those
    ranges, coalesces them, and runs a padded, scoped Stage 2 regroup over each. It is lossless (the
    pad guarantees no transaction straddling a window edge is split) yet far cheaper than a full
    rebuild, and unlike `?incremental=true` it correctly re-stitches files back-filled into an
    already-sealed region.

    Because a large/sparse batch can take a while, this returns **202** immediately with a `run_id`
    and runs the regroup in the background. Poll **GET /logs/regroup/runs/{run_id}** until
    `status` is `completed` (or `failed`) — so the HTTP request never times out. The work is
    idempotent: a run with nothing pending completes with `windows: 0`. Call once after the last file
    of a session; if forgotten, the next finalize (or the directory watcher) still catches it.
    """
    run = LogRegroupRun(customer_code=customer)
    db.add(run)
    await db.commit()
    await db.refresh(run)
    # keep a strong reference so the loop can't garbage-collect the task before it finishes
    task = asyncio.create_task(run_finalize_tracked(run.id, customer))
    _finalize_tasks.add(task)
    task.add_done_callback(_finalize_tasks.discard)
    return {"run_id": str(run.id), "status": run.status.value,
            "poll": f"/api/v1/logs/regroup/runs/{run.id}"}


@router.get("/regroup/runs/{run_id}")
async def get_regroup_run(
    run_id: uuid.UUID,
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Poll the status of an async finalize (see POST /logs/regroup/finalize).

    `status` is `running` until the background regroup finishes, then `completed` (with `windows` /
    `pending_consumed` / full `result` stats) or `failed` (with `error`; pending windows stay open so
    you can retry). A run belonging to another customer 404s exactly like a missing one.
    """
    run = await db.get(LogRegroupRun, run_id)
    if not run or run.customer_code != customer:
        raise HTTPException(404, detail="Regroup run not found")
    return {
        "run_id": str(run.id),
        "customer_code": run.customer_code,
        "status": run.status.value,
        "windows": run.windows,
        "pending_consumed": run.pending_consumed,
        "error": run.error,
        "result": run.result,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
    }


@router.get("/regroup/status")
async def regroup_status(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
):
    """Stitching status for this customer — drives the UI's "is the log server up to date?" widget and
    the (non-blocking) 'finalize' prompt. Read-only and cheap: one indexed query over the small
    log_regroup_pending table. Transaction reads do NOT block on this; they return the last
    fully-stitched data plus the same signal (see read_pending_state).

    Fields:
      - pending_windows   : open (not-yet-stitched) regroup windows. The backlog size.
      - oldest_pending_at : when the oldest un-stitched window was queued (null if none).
      - last_regroup_at   : when a window was last stitched (max consumed_at). Shows the server IS
                            populating transactions. Null only if nothing has ever been stitched.
      - up_to_date        : true when there is NO backlog (pending_windows == 0) — the log
                            transactions are current with everything ingested so far.
      - pending           : legacy alias for `not up_to_date`, kept for existing clients.

    NOTE for the frontend: `up_to_date` reports the STITCHING backlog only. To also show that
    ingestion is live, pair it with each source's `status` / `last_ok_at` (see GET /ssh-sources).
    """
    # ONE query over this customer's pending rows: open-only aggregates via FILTER, plus the overall
    # max(consumed_at) as the "last stitched" timestamp. No writes — safe to poll frequently.
    _open = LogRegroupPending.consumed_at.is_(None)
    _retryable = (_open, LogRegroupPending.abandoned_at.is_(None))
    # A window that FAILED is held back by available_at until its backoff elapses. It is still part
    # of the backlog, but it is failing rather than merely queued — and `pending_windows` alone
    # cannot tell those apart, which is exactly the distinction that matters when a disk starts
    # going bad at 2am.
    _waiting = (*_retryable, LogRegroupPending.available_at > func.clock_timestamp())
    row = (await db.execute(
        select(
            # open backlog that will still be retried (excludes dead-lettered windows)
            func.count().filter(*_retryable),
            func.min(LogRegroupPending.created_at).filter(*_retryable),
            func.max(LogRegroupPending.consumed_at),
            # dead-lettered: open but abandoned after repeated failures — parked, NOT retried
            func.count().filter(_open, LogRegroupPending.abandoned_at.isnot(None)),
            # subset of the backlog currently serving a retry delay
            func.count().filter(*_waiting),
            func.min(LogRegroupPending.available_at).filter(*_waiting),
        ).where(LogRegroupPending.customer_code == customer)
    )).one()
    count, oldest, last_regroup, abandoned, backing_off, next_retry = row
    count = count or 0
    abandoned = abandoned or 0
    backing_off = backing_off or 0
    return {
        "customer_code": customer,
        "pending": bool(count),
        "pending_windows": count,
        "oldest_pending_at": oldest.isoformat() if oldest else None,
        "last_regroup_at": last_regroup.isoformat() if last_regroup else None,
        # windows given up on after log_regroup_max_attempts failures; >0 means investigate (likely a
        # disk fault). They do NOT count as backlog and can be re-armed via POST /regroup/reset-abandoned.
        "abandoned_windows": abandoned,
        # SUBSET of pending_windows that has already failed and is serving a retry delay. Distinguishes
        # "about to be stitched" from "failing repeatedly" — the same number otherwise. A rising value
        # is the early warning that precedes abandoned_windows.
        "backing_off_windows": backing_off,
        # when the earliest of those becomes eligible again (null if none are waiting)
        "next_retry_at": next_retry.isoformat() if next_retry else None,
        "up_to_date": count == 0,
    }


@router.post("/regroup/reset-abandoned")
async def reset_abandoned(
    customer: str = Depends(get_active_customer),
    db: AsyncSession = Depends(get_session),
):
    """Re-arm this tenant's dead-lettered stitch windows so finalize retries them again (clears the
    abandoned marker + attempt counter). Use after the cause of the failures is addressed. Returns the
    number re-armed; call POST /regroup/finalize afterwards to stitch them."""
    n = await reset_abandoned_windows(db, customer)
    return {"customer_code": customer, "reset": n}


@router.get("/ingest-queue")
async def ingest_queue_status(
    customer: str = Depends(get_current_customer),
    status: str | None = Query(default=None,
                               description="filter by queue status: pending / leased / ingested / abandoned"),
    limit: int = Query(default=50, ge=1, le=500),
    db: AsyncSession = Depends(get_session),
):
    """Inspect this tenant's ingest queue — byte ranges the fetcher downloaded and the parse worker
    has yet to (or failed to) process.

    Deliberately shaped like the Stage 2 pair above so operators learn one pattern. The common use
    is `?status=abandoned` to see dead-lettered ranges with the error that stopped them; the bytes
    are still on disk, so re-arming replays them with no network re-fetch.

    Read-only and bounded, so it is safe to poll.
    """
    conds = [LogSourceObject.customer_code == customer]
    if status:
        if status not in SourceObjectStatus.ALL:
            raise HTTPException(400, detail=f"unknown status {status!r}; "
                                            f"expected one of {', '.join(SourceObjectStatus.ALL)}")
        conds.append(LogSourceObject.status == status)

    rows = (await db.execute(
        select(LogSourceObject).where(*conds)
        .order_by(LogSourceObject.created_at.desc()).limit(limit)
    )).scalars().all()

    counts = dict((await db.execute(
        select(LogSourceObject.status, func.count())
        .where(LogSourceObject.customer_code == customer)
        .group_by(LogSourceObject.status)
    )).all())

    return {
        "customer_code": customer,
        "counts": counts,
        "count": len(rows),
        "objects": [{
            "id": str(o.id),
            "status": o.status,
            "source_name": o.source_name,
            "remote_path": o.remote_path,
            "start_offset": o.start_offset,
            "end_offset": o.end_offset,
            "attempts": o.attempts,
            "max_attempts": o.max_attempts,
            "available_at": o.available_at,
            "last_error": o.last_error,
            "entries_inserted": o.entries_inserted,
            "created_at": o.created_at,
            "ingested_at": o.ingested_at,
        } for o in rows],
    }


@router.post("/ingest-queue/reset-abandoned")
async def reset_abandoned_ingest_objects(
    customer: str = Depends(get_active_customer),
    db: AsyncSession = Depends(get_session),
):
    """Re-arm this tenant's dead-lettered ingest ranges so the parse worker retries them (clears the
    abandoned marker + attempt counter). The downloaded bytes are still in object storage, so the
    retry reads the local file — no re-fetch over SSH. Mirrors POST /regroup/reset-abandoned."""
    n = await reset_abandoned_objects(db, customer)
    return {"customer_code": customer, "reset": n}


@router.delete("/data")
async def delete_log_data(
    customer: str = Depends(get_current_customer),
    date_from: date_type | None = Query(default=None, description="inclusive lower bound on LOG date (YYYY-MM-DD); for a single day set date_from = date_to"),
    date_to: date_type | None = Query(default=None, description="inclusive upper bound on LOG date (YYYY-MM-DD)"),
    confirm: bool = Query(default=False, description="required true to delete ALL of this customer's data when no date range is given (guard against an accidental full wipe)"),
    db: AsyncSession = Depends(get_session),
):
    """Delete a customer's log data — either a LOG-date range, or everything for the tenant.

    Always scoped to the X-Customer-Code tenant: one customer can never delete another's data.

    - **Date range** (date_from and/or date_to): deletes log_transactions (by their `date`) and
      log_entries (by entry `timestamp`) in the range, then runs an incremental regroup so any
      transaction that straddled the boundary is reconciled. The file-level Job rows are kept (a file
      can span dates), so re-scanning the same source would only re-add genuinely-new lines.
    - **Full wipe** (no date range): deletes every log Job for the customer, which cascades to all of
      its entries and transactions. Requires `confirm=true`.
    """
    # ---- full wipe (no date filter) ----
    if date_from is None and date_to is None:
        if not confirm:
            raise HTTPException(
                400,
                detail="Refusing to delete ALL data for this customer without confirm=true. "
                       "Pass ?confirm=true, or give date_from/date_to to delete only a range.",
            )
        n_ent = await db.scalar(select(func.count()).select_from(LogEntry)
                                .where(LogEntry.customer_code == customer))
        n_txn = await db.scalar(select(func.count()).select_from(LogTransaction)
                                .where(LogTransaction.customer_code == customer))
        # delete only this customer's LOG jobs; entries + transactions cascade via job_id FK.
        res = await db.execute(delete(Job).where(
            Job.customer_code == customer, Job.document_type == DOCUMENT_TYPE))
        # The ingest queue has no job FK, so a wipe must clear it explicitly — and unlink the
        # downloaded bytes, or they stay in ./uploads with nothing referencing them. A DATE-RANGE
        # delete deliberately does NOT do this: queue rows describe byte ranges, not log dates, and
        # dropping them would strand work that was never parsed.
        await purge_source_objects_files(db, customer)
        q_res = await db.execute(delete(LogSourceObject).where(
            LogSourceObject.customer_code == customer))
        await db.commit()
        return {"customer_code": customer, "scope": "all",
                "jobs_deleted": res.rowcount or 0,
                "queued_objects_deleted": q_res.rowcount or 0,
                "entries_deleted": n_ent or 0, "transactions_deleted": n_txn or 0}

    # ---- date-range delete ----
    ent_conds = [LogEntry.customer_code == customer]
    txn_conds = [LogTransaction.customer_code == customer]
    if date_from is not None:
        ent_conds.append(LogEntry.timestamp >= datetime.combine(date_from, datetime.min.time()))
        txn_conds.append(LogTransaction.date >= date_from)
    if date_to is not None:
        ent_conds.append(LogEntry.timestamp <= datetime.combine(date_to, datetime.max.time()))
        txn_conds.append(LogTransaction.date <= date_to)

    txn_res = await db.execute(delete(LogTransaction).where(*txn_conds))
    ent_res = await db.execute(delete(LogEntry).where(*ent_conds))
    await db.commit()

    # reconcile entries orphaned by a deleted boundary-spanning transaction (and reseal the tail)
    regroup_stats = await regroup_incremental(db, customer)

    return {"customer_code": customer, "scope": "date_range",
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
            "transactions_deleted": txn_res.rowcount or 0,
            "entries_deleted": ent_res.rowcount or 0,
            "regroup": regroup_stats}


@router.get("/transactions")
async def list_transactions(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
    pending: dict = Depends(read_pending_state),
    user: str | None = Query(default=None, description="matches user_name"),
    date: date_type | None = Query(default=None, description="YYYY-MM-DD"),
    status: LogTransactionStatus | None = None,
    method: str | None = None,
    transaction_name: str | None = None,
    transaction_type: str | None = None,
    warehouse: str | None = None,
    delivery_number: str | None = None,
    item_number: str | None = None,
    order_number: str | None = None,
    reqid: str | None = None,
    time_from: datetime | None = None,
    time_to: datetime | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = 0,
):
    """Filter/aggregate over derived transactions. `total` answers 'how many for X on date Y'."""
    conds = [LogTransaction.customer_code == customer]
    if user is not None:
        conds.append(LogTransaction.user_name == user)
    if date is not None:
        conds.append(LogTransaction.date == date)
    if status is not None:
        conds.append(LogTransaction.status == status)
    if method is not None:
        conds.append(LogTransaction.method == method)
    if transaction_name is not None:
        conds.append(LogTransaction.transaction_name == transaction_name)
    if transaction_type is not None:
        conds.append(LogTransaction.transaction_type == transaction_type)
    if warehouse is not None:
        conds.append(LogTransaction.warehouse == warehouse)
    if delivery_number is not None:
        conds.append(LogTransaction.delivery_number == delivery_number)
    if item_number is not None:
        conds.append(LogTransaction.item_number == item_number)
    if order_number is not None:
        conds.append(LogTransaction.order_number == order_number)
    if reqid is not None:
        conds.append(LogTransaction.reqid == reqid)
    if time_from is not None:
        conds.append(LogTransaction.started_at >= from_display_to_utc(time_from))
    if time_to is not None:
        conds.append(LogTransaction.started_at <= from_display_to_utc(time_to))

    total = await db.scalar(select(func.count()).select_from(LogTransaction).where(*conds))
    rows = (await db.execute(
        select(LogTransaction).where(*conds)
        .order_by(LogTransaction.started_at.desc().nullslast())
        .limit(limit).offset(offset)
    )).scalars().all()

    return {
        "total": total,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "pending_regroup": pending,
        "transactions": [_txn_summary(t) for t in rows],
    }


# NOTE: declared BEFORE "/transactions/{transaction_id}" so the literal "view" segment is matched
# here instead of being parsed as a UUID path param (which would 422).
@router.get("/transactions/view", response_class=PlainTextResponse)
async def view_transactions(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
    pending: dict = Depends(read_pending_state),
    date: date_type = Query(..., description="YYYY-MM-DD (REQUIRED). The day to browse; the feed is scoped to this date."),
    limit: int = Query(default=100, ge=1, le=500, description="page size (transactions per page)"),
    offset: int = Query(default=0, ge=0, description="pagination offset; page number = offset // limit + 1"),
    user: str | None = Query(default=None, description="matches user_name; omit for all users"),
    hour: int | None = Query(default=None, ge=0, le=23, description="hour of day 0-23 (combine with date)"),
    status: LogTransactionStatus | None = Query(default=None, description="success / soft / error / incomplete"),
    order_number: str | None = Query(default=None, description="exact match on order_number; omit for all"),
    item_number: str | None = Query(default=None, description="exact match on item_number; omit for all"),
    verbose: bool = Query(default=False, description="also render plain INFO narration steps"),
):
    """Render one PAGE of a day's transactions as the §6 text view, oldest→newest (from 00:00).

    `date` is REQUIRED: the feed is always scoped to a single day, shown oldest→newest from the start
    of that day and paginated via `limit` (page size) + `offset`. Optional `user`, `hour`, `status`
    stack on top of the date. Pagination metadata is returned in response headers: `X-Total-Count`,
    `X-Offset`, `X-Limit`, `X-Page`, `X-Page-Count`.
    """
    conds = [LogTransaction.customer_code == customer, LogTransaction.date == date]
    if user is not None:
        conds.append(LogTransaction.user_name == user)
    if hour is not None:
        # `hour` is a LOCAL hour-of-day (matching the displayed times); started_at is a UTC instant, so
        # convert to the customer's display zone before extracting, else this is off by the tz offset.
        conds.append(
            func.extract("hour", func.timezone(active_timezone_name(), LogTransaction.started_at)) == hour
        )
    if status is not None:
        conds.append(LogTransaction.status == status)
    if order_number is not None:
        conds.append(LogTransaction.order_number == order_number)
    if item_number is not None:
        conds.append(LogTransaction.item_number == item_number)

    # total for the pager (cheap: index-only count over this day, see docs/debugging-worker-timeout-outage.md)
    total = (await db.scalar(select(func.count()).select_from(LogTransaction).where(*conds))) or 0

    # this page, oldest → newest (from 00:00 of the day). `id` is a stable tiebreak so offset paging is
    # deterministic across any equal `started_at`. Served by ix_log_transactions_customer_date_started.
    txns = (await db.execute(
        select(LogTransaction).where(*conds)
        .order_by(LogTransaction.started_at.asc().nullslast(), LogTransaction.id.asc())
        .limit(limit).offset(offset)
    )).scalars().all()

    page = offset // limit + 1
    page_count = max(1, (total + limit - 1) // limit)
    pager = {"X-Total-Count": str(total), "X-Offset": str(offset), "X-Limit": str(limit),
             "X-Page": str(page), "X-Page-Count": str(page_count)}

    header = f"Showing {len(txns)} of {total} transaction(s) on {date}"
    if user:
        header += f" for user {user}"
    if status is not None:
        header += f" · status={status.value}"
    if order_number is not None:
        header += f" · order {order_number}"
    if item_number is not None:
        header += f" · item {item_number}"
    if hour is not None:
        header += f" hour {hour:02d}:00"
    header += f" — page {page}/{page_count} — oldest → newest"
    header = _pending_notice(pending) + header

    if not txns:
        return PlainTextResponse(header + "\n\n(no transactions on this page)", headers=pager)

    # fetch entries for this page's transactions, BOUNDED so a pathological page can't balloon the
    # worker (fetch one past the cap to detect overflow)
    ids = [t.id for t in txns]
    try:
        # NB: no ORDER BY on purpose. A global (seq, line_number) sort across ALL of the page's
        # transactions would spill to disk (work_mem is small) on a big/bloated table — yet _render
        # regroups the rows per transaction below and discards that cross-transaction order anyway.
        # We fetch unordered (no Sort node, no temp-file, and LIMIT can short-circuit) and restore
        # the only ordering that matters — WITHIN each transaction — in Python via _entry_sort_key.
        # See docs/transactions-view-load-spike-and-db-concepts-primer.md.
        # (entry, owning transaction, seq) — the assignment table owns the grouping now, so the
        # renderer below no longer reads LogEntry.transaction_id / .seq.
        entry_rows = await assignments.load_entries(db, list(ids), limit=MAX_RENDER_ENTRIES + 1)
    except Exception as exc:
        # A dead disk sector under log_entries for this page must not 500 the whole feed: the
        # transaction list above is intact, so return it with a clear notice and let the user page on.
        if not is_disk_io_error(exc):
            raise
        logger.critical("DISK I/O ERROR rendering feed for %s on %s: %s — returning list without bodies.",
                        customer, date, disk_io_detail(exc))
        return PlainTextResponse(
            header + "\n\n(⚠ some log entries on this page sit on a damaged disk sector and can't be "
            "read right now — the transaction list above is complete; try another page or date.)",
            headers=pager,
        )
    if len(entry_rows) > MAX_RENDER_ENTRIES:
        return PlainTextResponse(
            header + f"\n\n(too many log entries to render — this page exceeds "
            f"{MAX_RENDER_ENTRIES:,} entries. Use a smaller `limit`.)",
            headers=pager,
        )

    # grouping + render are synchronous CPU work → run off the event loop so a large render can't
    # block the worker heartbeat. asyncio.to_thread propagates contextvars (incl. the display
    # timezone set in get_current_customer), which a bare run_in_executor would not.
    def _render() -> str:
        # Group by the OWNING transaction from the assignment row, and order by its seq. Both used to
        # come from columns on LogEntry; they now come from log_entry_assignment, which is what lets
        # the raw table be insert-only.
        by_txn: dict = {}
        for entry, txn_id, seq in entry_rows:
            by_txn.setdefault(txn_id, []).append((seq, entry))
        for bucket in by_txn.values():
            bucket.sort(key=_assigned_sort_key)
        sep = "\n\n" + ("─" * 90) + "\n\n"
        blocks = [render_transaction(t, [e for _seq, e in by_txn.get(t.id, [])], verbose=verbose)
                  for t in txns]
        return header + sep + sep.join(blocks)

    text = await asyncio.to_thread(_render)
    return PlainTextResponse(text, headers=pager)


async def _load_transaction_entries(transaction_id: uuid.UUID, customer: str, db: AsyncSession):
    """Fetch a transaction + its ordered entry timeline (bounded), or 404. Returns
    (transaction, entries, truncated). A transaction belonging to another customer 404s exactly like a
    missing one — no cross-tenant existence leak via id probing."""
    t = await db.get(LogTransaction, transaction_id)
    if not t or t.customer_code != customer:
        raise HTTPException(404, detail="Transaction not found")
    # ordered by the assignment's seq — LogEntry.seq is no longer the source of truth.
    rows = [e for e, _txn, _seq in
            await assignments.load_entries(db, [transaction_id], limit=MAX_RENDER_ENTRIES + 1)]
    truncated = len(rows) > MAX_RENDER_ENTRIES
    return t, list(rows[:MAX_RENDER_ENTRIES]), truncated


@router.get("/transactions/{transaction_id}/view", response_class=PlainTextResponse)
async def get_transaction_view(
    transaction_id: uuid.UUID,
    customer: str = Depends(get_current_customer),
    verbose: bool = Query(default=False, description="also render plain INFO narration steps"),
    db: AsyncSession = Depends(get_session),
    pending: dict = Depends(read_pending_state),
):
    """Canonical §6 Transaction Detail View as human-readable text (request → steps → response)."""
    t, entries, truncated = await _load_transaction_entries(transaction_id, customer, db)
    text = await asyncio.to_thread(render_transaction, t, entries, verbose=verbose)
    notice = _pending_notice(pending)
    if truncated:
        notice += f"(showing the first {MAX_RENDER_ENTRIES:,} steps — transaction truncated)\n\n"
    return notice + text


@router.get("/transactions/{transaction_id}")
async def get_transaction(transaction_id: uuid.UUID,
                          customer: str = Depends(get_current_customer),
                          db: AsyncSession = Depends(get_session),
                          pending: dict = Depends(read_pending_state)):
    """Canonical Transaction Detail View — header + the ordered step-by-step entry timeline.

    Includes a `rendered` field: the §6 text view (also available raw at `/view`).
    """
    t, entries, truncated = await _load_transaction_entries(transaction_id, customer, db)

    return {
        "rendered": await asyncio.to_thread(render_transaction, t, entries),
        "entries_truncated": truncated,
        "transaction": {
            **_txn_summary(t),
            "http_method": t.http_method,
            "endpoint_url": t.endpoint_url,
            "user_id": t.user_id,
            "employee_name": t.employee_name,
            "division": t.division,
            "facility": t.facility,
            "device_name": t.device_name,
            "picklist_suffix": t.picklist_suffix,
            "reporting_number": t.reporting_number,
            "response_summary": t.response_summary,
            "source_file_start": t.source_file_start,
            "source_file_end": t.source_file_end,
            "attributes": t.attributes,
        },
        "timeline": [
            {
                "seq": e.seq,
                "entry_type": e.entry_type.value,
                "timestamp": iso_display(e.timestamp),
                "level": e.level,
                "mi_program": e.mi_program,
                "mi_transaction": e.mi_transaction,
                "result_status": e.result_status,
                "record_count": e.record_count,
                "message": e.message,
                "fields": e.fields,
            }
            for e in entries
        ],
        "pending_regroup": pending,
    }


# ---------------------------------------------------------------------------
# Phase 2 — Claude tool-use debugging agent
# ---------------------------------------------------------------------------

class DebugAskRequest(BaseModel):
    question: str = Field(..., min_length=1, description="Natural-language debugging question.")


@router.post("/debug/ask")
async def debug_ask(
    body: DebugAskRequest,
    agent: LogDebugAgent = Depends(get_log_debug_agent),
    pending: dict = Depends(read_pending_state),
):
    """Ask the debugging agent a natural-language question about the logs.

    Claude picks read-only SQL-backed tools (search/count/find_errors/get_transaction/
    search_entries), runs them against the relational store, and answers with cited
    transaction ids. Returns the answer plus a trace of the tool calls it made.

    The answer reflects the last fully-stitched data; if `pending_regroup.pending` is true, the
    freshest ingested tail isn't included yet (finalize to include it).
    """
    try:
        result = await agent.ask(body.question)
    except RuntimeError as exc:  # missing API key, etc.
        raise HTTPException(503, detail=str(exc))
    if isinstance(result, dict):
        result["pending_regroup"] = pending
        # `refs` — LineIds ("<transactionId>#<bodyLineIndex>") the answer cites, so the frontend can
        # render jump-to-line chips / "pin as note". A valid LineId needs the line's index within the
        # transaction's rendered body, which the agent doesn't track — computing it would require
        # replicating the feed renderer, and the contract says do NOT guess. So we surface [] (the
        # frontend already derives chips client-side from method names as a fallback) until a reliable
        # index is available. Keep the key present so the shape is stable.
        result.setdefault("refs", [])
    return result
