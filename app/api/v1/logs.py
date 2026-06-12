"""Transaction-log API — Phase 1a: ingest files + query raw entries (line level).

Transaction-level endpoints (GET /logs/transactions...) arrive in Phase 1b once Stage 2 grouping
exists. For now you can drop/upload a log file and query its parsed entries.
"""

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
from app.services.mnp_log_ingestion.LogIngestion import LogIngestion, get_log_ingestion, DOCUMENT_TYPE
from app.services.mnp_log_ingestion.pipeline.derive_transactions import regroup_all, regroup_incremental
from app.services.mnp_log_ingestion.render import render_transaction
from app.services.log_agent.agent import LogDebugAgent, get_log_debug_agent

router = APIRouter(prefix="/logs", tags=["logs"])

MAX_FILE_SIZE = 200 * 1024 * 1024  # 200 MB — rotating log files can be large


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
    return {"job_id": str(job.id), "filename": job.filename,
            "customer_code": job.customer_code, "status": job.status.value}


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
    return {
        "job_id": str(job.id),
        "filename": job.filename,
        "status": job.status.value,
        "entry_count": job.chunk_count,
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
        stmt = stmt.where(LogEntry.timestamp >= time_from)
    if time_to is not None:
        stmt = stmt.where(LogEntry.timestamp <= time_to)

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
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
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
        "started_at": t.started_at.isoformat() if t.started_at else None,
        "ended_at": t.ended_at.isoformat() if t.ended_at else None,
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
        await db.commit()
        return {"customer_code": customer, "scope": "all",
                "jobs_deleted": res.rowcount or 0,
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
        conds.append(LogTransaction.started_at >= time_from)
    if time_to is not None:
        conds.append(LogTransaction.started_at <= time_to)

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
        "transactions": [_txn_summary(t) for t in rows],
    }


# NOTE: declared BEFORE "/transactions/{transaction_id}" so the literal "view" segment is matched
# here instead of being parsed as a UUID path param (which would 422).
@router.get("/transactions/view", response_class=PlainTextResponse)
async def view_transactions(
    customer: str = Depends(get_current_customer),
    db: AsyncSession = Depends(get_session),
    limit: int = Query(default=50, ge=1, le=500, description="how many most-recent transactions to render"),
    user: str | None = Query(default=None, description="matches user_name; omit for all users"),
    date: date_type | None = Query(default=None, description="YYYY-MM-DD (matches the transaction date)"),
    hour: int | None = Query(default=None, ge=0, le=23, description="hour of day 0-23 (combine with date)"),
    status: LogTransactionStatus | None = Query(default=None, description="success / soft / error / incomplete"),
    verbose: bool = Query(default=False, description="also render plain INFO narration steps"),
):
    """Render the last N transactions as the §6 text view, oldest→newest.

    Takes the most-recent `limit` transactions that match the optional filters, then prints them
    in ascending time order. Filters stack: `user`, `date`, `hour`, `status`. With none, you get
    the last `limit` transactions across all users.
    """
    conds = [LogTransaction.customer_code == customer]
    if user is not None:
        conds.append(LogTransaction.user_name == user)
    if date is not None:
        conds.append(LogTransaction.date == date)
    if hour is not None:
        conds.append(func.extract("hour", LogTransaction.started_at) == hour)
    if status is not None:
        conds.append(LogTransaction.status == status)

    # most-recent `limit` matching transactions ...
    rows = (await db.execute(
        select(LogTransaction).where(*conds)
        .order_by(LogTransaction.started_at.desc().nullslast())
        .limit(limit)
    )).scalars().all()
    txns = list(reversed(rows))  # ... shown oldest -> newest

    header = f"Showing {len(txns)} transaction(s)" + (f" for user {user}" if user else " (all users)")
    if status is not None:
        header += f" · status={status.value}"
    if date is not None:
        header += f" on {date}"
    if hour is not None:
        header += f" hour {hour:02d}:00"
    header += " — oldest → newest"

    if not txns:
        return header + "\n\n(no transactions match the given filters)"

    # fetch every entry for those transactions in one query, then group by transaction
    ids = [t.id for t in txns]
    entry_rows = (await db.execute(
        select(LogEntry).where(LogEntry.transaction_id.in_(ids))
        .order_by(LogEntry.seq.asc().nullslast(), LogEntry.line_number.asc())
    )).scalars().all()
    by_txn: dict = {}
    for e in entry_rows:
        by_txn.setdefault(e.transaction_id, []).append(e)

    sep = "\n\n" + ("─" * 90) + "\n\n"
    blocks = [render_transaction(t, by_txn.get(t.id, []), verbose=verbose) for t in txns]
    return header + sep + sep.join(blocks)


async def _load_transaction_entries(transaction_id: uuid.UUID, customer: str, db: AsyncSession):
    """Fetch a transaction + its ordered entry timeline, or 404. A transaction belonging to another
    customer 404s exactly like a missing one — no cross-tenant existence leak via id probing."""
    t = await db.get(LogTransaction, transaction_id)
    if not t or t.customer_code != customer:
        raise HTTPException(404, detail="Transaction not found")
    entries = (await db.execute(
        select(LogEntry).where(LogEntry.transaction_id == transaction_id)
        .order_by(LogEntry.seq.asc().nullslast(), LogEntry.line_number.asc())
    )).scalars().all()
    return t, list(entries)


@router.get("/transactions/{transaction_id}/view", response_class=PlainTextResponse)
async def get_transaction_view(
    transaction_id: uuid.UUID,
    customer: str = Depends(get_current_customer),
    verbose: bool = Query(default=False, description="also render plain INFO narration steps"),
    db: AsyncSession = Depends(get_session),
):
    """Canonical §6 Transaction Detail View as human-readable text (request → steps → response)."""
    t, entries = await _load_transaction_entries(transaction_id, customer, db)
    return render_transaction(t, entries, verbose=verbose)


@router.get("/transactions/{transaction_id}")
async def get_transaction(transaction_id: uuid.UUID,
                          customer: str = Depends(get_current_customer),
                          db: AsyncSession = Depends(get_session)):
    """Canonical Transaction Detail View — header + the ordered step-by-step entry timeline.

    Includes a `rendered` field: the §6 text view (also available raw at `/view`).
    """
    t, entries = await _load_transaction_entries(transaction_id, customer, db)

    return {
        "rendered": render_transaction(t, entries),
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
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
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
):
    """Ask the debugging agent a natural-language question about the logs.

    Claude picks read-only SQL-backed tools (search/count/find_errors/get_transaction/
    search_entries), runs them against the relational store, and answers with cited
    transaction ids. Returns the answer plus a trace of the tool calls it made.
    """
    try:
        return await agent.ask(body.question)
    except RuntimeError as exc:  # missing API key, etc.
        raise HTTPException(503, detail=str(exc))
