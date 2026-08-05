"""Read-only, SQL-backed tools the debugging agent can call.

Each tool maps to an indexed query over the promoted columns of log_transactions /
log_entries — the same dimensions the REST API filters on — so aggregate and drill-down
questions both run without a full-text scan. Results are returned as compact JSON strings
(token-frugal) that always carry transaction ids, so Claude can cite them in its answer.

Everything here is strictly SELECT-only. The agent is never given a tool that writes.
"""

import json
import uuid
from datetime import date as date_type, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_entry import LogEntry
from app.services.mnp_log_ingestion.pipeline import assignments
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.mnp_log_ingestion.timefmt import iso_display, from_display_to_utc

# ---------------------------------------------------------------------------
# Tool schemas (sent to Claude)
# ---------------------------------------------------------------------------

# Shared filter properties reused across the search/count/error tools so Claude sees one
# consistent vocabulary for the promoted WMS dimensions.
_TXN_FILTERS = {
    "user": {"type": "string", "description": "WMS user name (exact match)."},
    "date": {"type": "string", "description": "Transaction day, YYYY-MM-DD (exact match)."},
    "status": {
        "type": "string",
        "enum": ["success", "soft", "error", "incomplete"],
        "description": "success=clean, soft=M3 not-found/needs-value the app coped with, "
                       "error=real ERROR-level failure, incomplete=RESPONSE not yet ingested.",
    },
    "method": {"type": "string", "description": "API endpoint / MethodName, e.g. CheckServer."},
    "transaction_name": {"type": "string", "description": "Business transaction name."},
    "transaction_type": {"type": "string", "description": "Transaction type/category."},
    "warehouse": {"type": "string", "description": "Warehouse code."},
    "item_number": {"type": "string", "description": "Item / SKU number."},
    "delivery_number": {"type": "string", "description": "Delivery number."},
    "order_number": {"type": "string", "description": "Order number."},
    "reqid": {"type": "string", "description": "Request id (ReqID) of one specific call."},
    "time_from": {"type": "string", "description": "ISO-8601 lower bound on start time (inclusive)."},
    "time_to": {"type": "string", "description": "ISO-8601 upper bound on start time (inclusive)."},
}

TOOLS = [
    {
        "name": "search_transactions",
        "description": (
            "Find transactions (one API request/response cycle each) matching any combination "
            "of filters, newest first. Use this to locate candidate transactions before drilling "
            "in. Returns a compact summary row per transaction INCLUDING its id — pass that id to "
            "get_transaction for the full timeline. Also returns `total` (how many match in total, "
            "ignoring the limit)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                **_TXN_FILTERS,
                "limit": {"type": "integer", "description": "Max rows to return (default 20, max 100)."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "count_transactions",
        "description": (
            "Count transactions matching the filters, with a breakdown by status "
            "(success/soft/error/incomplete). Use this for 'how many ...' questions "
            "(e.g. how many transactions did user X run on a date, how many errored today). "
            "Cheaper than search_transactions when you only need totals."
        ),
        "input_schema": {
            "type": "object",
            "properties": dict(_TXN_FILTERS),
            "additionalProperties": False,
        },
    },
    {
        "name": "find_errors",
        "description": (
            "Shortcut for failure triage: return transactions whose status is error (and, if "
            "include_soft=true, soft), newest first, each with its error_text. Accepts the same "
            "filters as search_transactions to scope by user/date/warehouse/etc."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                **_TXN_FILTERS,
                "include_soft": {"type": "boolean", "description": "Also include soft results (default false)."},
                "limit": {"type": "integer", "description": "Max rows (default 20, max 100)."},
            },
            "additionalProperties": False,
        },
    },
    {
        "name": "get_transaction",
        "description": (
            "Fetch the full canonical detail view of ONE transaction by id: the header (who/where/"
            "what/outcome) plus the ordered step-by-step entry timeline (REQUEST, M3 MI calls and "
            "their results, SQL, errors, RESPONSE). This is how you see exactly what happened inside "
            "a transaction and why it failed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "transaction_id": {"type": "string", "description": "The transaction id (UUID)."},
                "max_entries": {"type": "integer", "description": "Cap timeline entries (default 80)."},
            },
            "required": ["transaction_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "search_entries",
        "description": (
            "Line-level search across raw log entries (use when a question is about a specific "
            "message, MI program, or SQL rather than a whole transaction). Filter by a case-"
            "insensitive substring `q`, mi_program (e.g. MMS200MI), level (INFO/WARN/ERROR), or a "
            "transaction_id. Each result carries its transaction_id so you can pivot to "
            "get_transaction."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "Case-insensitive substring to match in the message."},
                "mi_program": {"type": "string", "description": "M3 MI program, e.g. MMS200MI."},
                "level": {"type": "string", "description": "Log level: INFO, WARN, or ERROR."},
                "transaction_id": {"type": "string", "description": "Restrict to one transaction (UUID)."},
                "time_from": {"type": "string", "description": "ISO-8601 lower bound on timestamp."},
                "time_to": {"type": "string", "description": "ISO-8601 upper bound on timestamp."},
                "limit": {"type": "integer", "description": "Max rows (default 30, max 100)."},
            },
            "additionalProperties": False,
        },
    },
]


# ---------------------------------------------------------------------------
# Serialization helpers (kept compact to save tokens)
# ---------------------------------------------------------------------------

def _txn_summary(t: LogTransaction) -> dict:
    """Drop null fields so each row is as small as possible in the tool result."""
    row = {
        "id": str(t.id),
        "status": t.status.value,
        "method": t.method,
        "transaction_name": t.transaction_name,
        "user": t.user_name,
        "warehouse": t.warehouse,
        "company": t.company,
        "reqid": t.reqid,
        "item_number": t.item_number,
        "delivery_number": t.delivery_number,
        "order_number": t.order_number,
        "started_at": iso_display(t.started_at),
        "date": t.date.isoformat() if t.date else None,
        "duration_ms": t.duration_ms,
        "entry_count": t.entry_count,
        "error_text": t.error_text,
    }
    return {k: v for k, v in row.items() if v is not None}


# ---------------------------------------------------------------------------
# Filter parsing
# ---------------------------------------------------------------------------

def _parse_dt(value: str) -> datetime | None:
    """Parse an inbound time filter. A naive value is interpreted as the display zone (UK) — matching
    the times the agent surfaces — and returned as UTC-aware, so it compares to the UTC-stored column."""
    try:
        return from_display_to_utc(datetime.fromisoformat(value))
    except (TypeError, ValueError):
        return None


def _txn_conditions(args: dict, customer_code: str) -> list:
    """Translate the shared filter vocabulary into SQLAlchemy WHERE clauses, pinned to one customer.
    customer_code is injected server-side (never a model-exposed filter), so the agent can only ever
    see the tenant the request is scoped to."""
    conds = [LogTransaction.customer_code == customer_code]
    if args.get("user"):
        conds.append(LogTransaction.user_name == args["user"])
    if args.get("date"):
        try:
            conds.append(LogTransaction.date == date_type.fromisoformat(args["date"]))
        except ValueError:
            pass
    if args.get("status"):
        try:
            conds.append(LogTransaction.status == LogTransactionStatus(args["status"]))
        except ValueError:
            pass
    if args.get("method"):
        conds.append(LogTransaction.method == args["method"])
    if args.get("transaction_name"):
        conds.append(LogTransaction.transaction_name == args["transaction_name"])
    if args.get("transaction_type"):
        conds.append(LogTransaction.transaction_type == args["transaction_type"])
    if args.get("warehouse"):
        conds.append(LogTransaction.warehouse == args["warehouse"])
    if args.get("item_number"):
        conds.append(LogTransaction.item_number == args["item_number"])
    if args.get("delivery_number"):
        conds.append(LogTransaction.delivery_number == args["delivery_number"])
    if args.get("order_number"):
        conds.append(LogTransaction.order_number == args["order_number"])
    if args.get("reqid"):
        conds.append(LogTransaction.reqid == args["reqid"])
    if args.get("time_from") and (dt := _parse_dt(args["time_from"])):
        conds.append(LogTransaction.started_at >= dt)
    if args.get("time_to") and (dt := _parse_dt(args["time_to"])):
        conds.append(LogTransaction.started_at <= dt)
    return conds


def _clamp(value, default: int, hi: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(n, hi))


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------

async def _search_transactions(db: AsyncSession, args: dict, customer_code: str) -> dict:
    conds = _txn_conditions(args, customer_code)
    limit = _clamp(args.get("limit"), 20, 100)
    total = await db.scalar(select(func.count()).select_from(LogTransaction).where(*conds))
    rows = (await db.execute(
        select(LogTransaction).where(*conds)
        .order_by(LogTransaction.started_at.desc().nullslast())
        .limit(limit)
    )).scalars().all()
    return {"total": total or 0, "returned": len(rows),
            "transactions": [_txn_summary(t) for t in rows]}


async def _count_transactions(db: AsyncSession, args: dict, customer_code: str) -> dict:
    conds = _txn_conditions(args, customer_code)
    total = await db.scalar(select(func.count()).select_from(LogTransaction).where(*conds))
    rows = (await db.execute(
        select(LogTransaction.status, func.count())
        .where(*conds).group_by(LogTransaction.status)
    )).all()
    by_status = {status.value: count for status, count in rows}
    return {"total": total or 0, "by_status": by_status}


async def _find_errors(db: AsyncSession, args: dict, customer_code: str) -> dict:
    # status is owned by this tool — strip any status the caller passed before building conds
    conds = _txn_conditions({k: v for k, v in args.items() if k != "status"}, customer_code)
    statuses = [LogTransactionStatus.error]
    if args.get("include_soft"):
        statuses.append(LogTransactionStatus.soft)
    conds.append(LogTransaction.status.in_(statuses))
    limit = _clamp(args.get("limit"), 20, 100)
    total = await db.scalar(select(func.count()).select_from(LogTransaction).where(*conds))
    rows = (await db.execute(
        select(LogTransaction).where(*conds)
        .order_by(LogTransaction.started_at.desc().nullslast())
        .limit(limit)
    )).scalars().all()
    return {"total": total or 0, "returned": len(rows),
            "transactions": [_txn_summary(t) for t in rows]}


async def _get_transaction(db: AsyncSession, args: dict, customer_code: str) -> dict:
    raw = args.get("transaction_id", "")
    try:
        tid = uuid.UUID(str(raw))
    except (ValueError, AttributeError):
        return {"error": f"Not a valid transaction id: {raw!r}"}
    t = await db.get(LogTransaction, tid)
    if not t or t.customer_code != customer_code:  # another tenant's id reads as not-found
        return {"error": f"No transaction found with id {raw}"}
    max_entries = _clamp(args.get("max_entries"), 80, 200)
    # Ordered by the assignment's seq, which is where the position lives now. Each pair is
    # (entry, seq) so the timeline below can report the position without reading it off the row.
    entries = [(e, seq) for e, _txn, seq in
               await assignments.load_entries(db, [tid], limit=max_entries)]

    header = {
        **_txn_summary(t),
        "http_method": t.http_method,
        "endpoint_url": t.endpoint_url,
        "user_id": t.user_id,
        "employee_name": t.employee_name,
        "division": t.division,
        "facility": t.facility,
        "device_id": t.device_id,
        "device_name": t.device_name,
        "ended_at": iso_display(t.ended_at),
        "mi_program_count": t.mi_program_count,
        "request_summary": t.request_summary,
        "response_summary": t.response_summary,
        "source_file_start": t.source_file_start,
        "source_file_end": t.source_file_end,
        "attributes": t.attributes or None,
    }
    header = {k: v for k, v in header.items() if v is not None}

    timeline = []
    for e, seq in entries:
        step = {
            "seq": seq,
            "type": e.entry_type.value,
            "level": e.level,
            "timestamp": iso_display(e.timestamp),
            "mi_program": e.mi_program,
            "mi_transaction": e.mi_transaction,
            "result_status": e.result_status,
            "record_count": e.record_count,
            "message": e.message,
        }
        timeline.append({k: v for k, v in step.items() if v is not None})

    return {"transaction": header, "timeline_entries": len(timeline), "timeline": timeline}


async def _search_entries(db: AsyncSession, args: dict, customer_code: str) -> dict:
    conds = [LogEntry.customer_code == customer_code]
    if args.get("q"):
        conds.append(LogEntry.message.ilike(f"%{args['q']}%"))
    if args.get("mi_program"):
        conds.append(LogEntry.mi_program == args["mi_program"])
    if args.get("level"):
        conds.append(LogEntry.level == args["level"].upper())
    if args.get("transaction_id"):
        try:
            conds.append(assignments.belongs_to_transaction(uuid.UUID(str(args["transaction_id"]))))
        except (ValueError, AttributeError):
            return {"error": f"Not a valid transaction id: {args['transaction_id']!r}"}
    if args.get("time_from") and (dt := _parse_dt(args["time_from"])):
        conds.append(LogEntry.timestamp >= dt)
    if args.get("time_to") and (dt := _parse_dt(args["time_to"])):
        conds.append(LogEntry.timestamp <= dt)
    limit = _clamp(args.get("limit"), 30, 100)

    rows = (await db.execute(
        select(LogEntry).where(*conds)
        .order_by(LogEntry.timestamp.desc().nullslast(), LogEntry.line_number.desc())
        .limit(limit)
    )).scalars().all()

    # one bulk lookup for the page rather than one per row; an entry missing from the map is not
    # stitched yet.
    owner = await assignments.load_transaction_by_entry(db, [e.id for e in rows])
    entries = []
    for e in rows:
        row = {
            "transaction_id": str(owner[e.id]) if e.id in owner else None,
            "type": e.entry_type.value,
            "level": e.level,
            "timestamp": iso_display(e.timestamp),
            "mi_program": e.mi_program,
            "mi_transaction": e.mi_transaction,
            "result_status": e.result_status,
            "source_file": e.source_file,
            "message": e.message,
        }
        entries.append({k: v for k, v in row.items() if v is not None})
    return {"returned": len(entries), "entries": entries}


_DISPATCH = {
    "search_transactions": _search_transactions,
    "count_transactions": _count_transactions,
    "find_errors": _find_errors,
    "get_transaction": _get_transaction,
    "search_entries": _search_entries,
}


async def execute_tool(name: str, tool_input: dict, db: AsyncSession, customer_code: str) -> str:
    """Run one tool call and return its result as a JSON string for the tool_result block.

    customer_code is injected by the agent (from the request's tenant) into every tool, so the agent
    is hard-scoped to one customer and cannot read another tenant's logs."""
    fn = _DISPATCH.get(name)
    if fn is None:
        return json.dumps({"error": f"Unknown tool: {name}"})
    try:
        result = await fn(db, tool_input or {}, customer_code)
    except Exception as exc:  # surface the error to Claude instead of crashing the loop
        result = {"error": f"{type(exc).__name__}: {exc}"}
    return json.dumps(result, default=str)
