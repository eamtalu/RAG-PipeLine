# derive_transactions.py — Stage 2 of the log pipeline (derive transactions from entries)
#
#   Reads log_entries ORDERED BY timestamp (across ALL files), runs the REQUEST→RESPONSE state
#   machine, and builds log_transactions — promoting the common WMS dimensions to columns and
#   keeping the rest in attributes JSONB. Because it reads the whole table in time order, a
#   transaction whose REQUEST and RESPONSE live in different rotated files is stitched naturally.
#
#   This pass is a FULL REBUILD: it deletes existing log_transactions (which sets every
#   log_entries.transaction_id back to NULL via ON DELETE SET NULL) and regroups from scratch.
#   That keeps it simple, correct, and safely re-runnable (it never touches the raw entries).

"""Stage 2 — group raw log_entries into log_transactions."""

import logging
from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus

logger = logging.getLogger(__name__)

# request params / body keys we never want to keep verbatim in attributes
_SENSITIVE = {"password", "accesstoken", "m3credentials", "m3usercredentials", "cipher"}


class _TxnBuilder:
    """Accumulates the entries of one API request/response cycle."""

    def __init__(self) -> None:
        self.entries: list[LogEntry] = []
        self.open_pos: int = -1  # stream position when this transaction opened (for FIFO response match)

    def add(self, entry: LogEntry) -> None:
        self.entries.append(entry)

    # ---- merged request params + body (case-insensitive source for promotion) ----
    def _merged_attrs(self) -> tuple[dict, bool, str | None]:
        attrs: dict = {}
        has_body = False
        url: str | None = None
        for e in self.entries:
            et = e.entry_type.value
            f = e.fields or {}
            if et == "request":
                url = f.get("url")
                if isinstance(f.get("params"), dict):
                    attrs.update(f["params"])
            elif et == "request_body":
                has_body = True
                # request_body.fields is the parsed JSON body itself (a dict) or {"raw": ...}
                if isinstance(f, dict):
                    attrs.update({k: v for k, v in f.items() if not isinstance(v, (dict, list))})
        return attrs, has_body, url

    @staticmethod
    def _ci_get(d: dict, *names: str):
        low = {str(k).lower(): v for k, v in d.items()}
        for n in names:
            v = low.get(n.lower())
            if v not in (None, ""):
                return str(v)
        return None

    def compute(self) -> dict:
        attrs, has_body, url = self._merged_attrs()
        g = lambda *n: self._ci_get(attrs, *n)  # noqa: E731

        # timestamps from attached entries
        times = [e.timestamp for e in self.entries if e.timestamp is not None]
        started = min(times) if times else None
        ended = max(times) if times else None
        duration_ms = int((ended - started).total_seconds() * 1000) if (started and ended) else None

        # outcome rollup
        has_response = any(e.entry_type.value == "response" for e in self.entries)
        error_entry = next((e for e in self.entries if e.entry_type.value == "error"), None)
        soft_entry = next(
            (e for e in self.entries
             if (e.entry_type.value == "mi_result" and e.result_status and e.result_status != "OK")
             or (e.level == "WARN")),
            None,
        )
        if error_entry is not None:
            status = LogTransactionStatus.error
            error_text = error_entry.result_status or error_entry.message
        elif not has_response:
            status = LogTransactionStatus.incomplete
            error_text = soft_entry.result_status if soft_entry else None
        elif soft_entry is not None:
            status = LogTransactionStatus.soft
            error_text = soft_entry.result_status
        else:
            status = LogTransactionStatus.success
            error_text = None

        response_entry = next((e for e in self.entries if e.entry_type.value == "response"), None)
        mi_calls = sum(1 for e in self.entries if e.entry_type.value == "mi_call")

        method = g("MethodName")
        warehouse = g("Warehouse", "WHLO")
        route = g("Route")
        user_name = g("User", "m3user")
        if not user_name:  # GET with no REQUEST bound -> fall back to a mi_call's m3user
            for e in self.entries:
                if e.entry_type.value == "mi_call":
                    p = (e.fields or {}).get("params") or {}
                    if p.get("m3user"):
                        user_name = str(p["m3user"])
                        break

        # short human request summary
        summary_bits = [b for b in (method, f"WHLO {warehouse}" if warehouse else None,
                                    f"Route {route}" if route else None,
                                    f"user {user_name}" if user_name else None) if b]
        request_summary = " · ".join(summary_bits) or None

        response_summary = None
        if response_entry and response_entry.message:
            response_summary = response_entry.message.replace("RESPONSE:", "").strip()[:300]

        # catch-all: keep all merged params except secrets
        clean_attrs = {k: v for k, v in attrs.items() if str(k).lower() not in _SENSITIVE}

        return {
            "job_id": self.entries[0].job_id,
            "flow_id": None,
            "source_file_start": self.entries[0].source_file,
            "source_file_end": self.entries[-1].source_file,
            "started_at": started,
            "ended_at": ended,
            "date": started.date() if started else None,
            "duration_ms": duration_ms,
            "user_name": user_name,
            "user_id": g("UserID"),
            "employee_name": g("EmployeeName"),
            "company": g("Company", "CONO"),
            "warehouse": warehouse,
            "warehouse_id": g("WarehouseID"),
            "division": g("Division"),
            "facility": g("Facility"),
            "device_id": g("DeviceID"),
            "device_name": g("DeviceName"),
            "reqid": g("ReqID", "ReqId"),
            "method": method,
            "http_method": "POST" if has_body else "GET",
            "endpoint_url": url,
            "transaction_name": g("TransactionName"),
            "transaction_type": g("TransactionType"),
            "route": route,
            "item_number": g("ItemNumber"),
            "delivery_number": g("DeliveryNumber"),
            "picklist_suffix": g("PickListSuffix"),
            "order_number": g("OrderNumber"),
            "reporting_number": g("ReportingNumber"),
            "status": status,
            "error_text": error_text,
            "entry_count": len(self.entries),
            "mi_program_count": mi_calls,
            "request_summary": request_summary,
            "response_summary": response_summary,
            "attributes": clean_attrs,
        }


_INTERNAL = {"mi_call", "mi_result", "sql", "info", "error"}


def _entry_reqid(e: LogEntry) -> str | None:
    """ReqID for a request/body entry (GET carries it in the URL params, POST in the body)."""
    f = e.fields or {}
    p = f.get("params") if isinstance(f.get("params"), dict) else {}
    for d in (p, f):
        for k in ("ReqID", "ReqId", "reqid"):
            v = d.get(k)
            if v:
                return str(v)
    return None


def _entry_user(e: LogEntry) -> str | None:
    """The user an entry belongs to: request/GET → params User, body → User, mi_call → m3user."""
    f = e.fields or {}
    et = e.entry_type.value
    if et == "request":
        p = f.get("params") or {}
        return p.get("User") or p.get("m3user")
    if et == "request_body":
        return f.get("User")
    if et == "mi_call":
        p = f.get("params") or {}
        return p.get("m3user")
    return None


def _group(entries: list[LogEntry]) -> list[_TxnBuilder]:
    """Thread-aware grouping that demultiplexes concurrent requests.

    The M3 server processes multiple users at once, so the timestamp-ordered stream interleaves
    them. A single open-transaction stack mixes users (confirmed bug). Instead we key open
    transactions by **thread** — one request's internal MI work stays on one thread (~98%), and a
    POST's REQUEST BODY runs on that same thread (~99%). The async REQUEST (MoveNext) and RESPONSE
    (b__1) lines hop threads and the RESPONSE carries no id, so:
      - a REQUEST is paired to its work by ReqID (GET) or, for a POST whose MoveNext has no id, to
        the body it immediately precedes; a GET REQUEST is bound by User once its work appears;
      - a RESPONSE (no correlation id) is attached best-effort to the OLDEST still-open request
        (FIFO — responses arrive in request order).
    This guarantees no transaction mixes two users' internal work (responses carry no user, so
    best-effort response matching cannot cause user contamination).
    """
    builders: list[_TxnBuilder] = []
    open_by_thread: dict[str, _TxnBuilder] = {}
    pending_reqs: list[LogEntry] = []  # MoveNext REQUEST lines awaiting their processing thread
    req_pos: dict[int, int] = {}       # stream position of each pending request (for response match)

    def take_by_reqid(reqid: str | None) -> LogEntry | None:
        if reqid is None:
            return None
        for i, r in enumerate(pending_reqs):
            if _entry_reqid(r) == reqid:
                return pending_reqs.pop(i)
        return None

    def take_post_request() -> LogEntry | None:
        # a POST's MoveNext has no ReqID; it's the most-recent id-less pending request (emitted
        # immediately before its body).
        for i in range(len(pending_reqs) - 1, -1, -1):
            if _entry_reqid(pending_reqs[i]) is None:
                return pending_reqs.pop(i)
        return None

    def take_by_user(user: str | None) -> LogEntry | None:
        if not user:
            return None
        for i, r in enumerate(pending_reqs):
            if _entry_user(r) == user:
                return pending_reqs.pop(i)
        return None

    def bound_user(b: _TxnBuilder) -> str | None:
        for e in b.entries:
            u = _entry_user(e)
            if u:
                return u
        return None

    def has_request(b: _TxnBuilder) -> bool:
        return any(e.entry_type.value == "request" for e in b.entries)

    for i, e in enumerate(entries):
        et = e.entry_type.value
        th = e.thread

        if et == "request":
            pending_reqs.append(e)
            req_pos[id(e)] = i

        elif et == "request_body":
            if th is not None and th in open_by_thread:
                builders.append(open_by_thread.pop(th))  # prior cycle on this thread: no RESPONSE
            b = _TxnBuilder()
            req = take_by_reqid(_entry_reqid(e)) or take_post_request()
            if req is not None:
                b.add(req)
            b.add(e)
            b.open_pos = req_pos.pop(id(req), i) if req is not None else i
            if th is not None:
                open_by_thread[th] = b
            else:
                builders.append(b)

        elif et in _INTERNAL:
            u = _entry_user(e)
            b = open_by_thread.get(th) if th is not None else None
            # a different user on the same thread => a new request reused it (GET, no body to reset)
            if b is not None and u and bound_user(b) and u != bound_user(b):
                builders.append(open_by_thread.pop(th))
                b = None
            if b is None:
                b = _TxnBuilder()
                b.open_pos = i
                if th is not None:
                    open_by_thread[th] = b
            b.add(e)
            if u and not has_request(b):  # bind a pending GET REQUEST now that we know the user
                req = take_by_user(u)
                if req is not None:
                    b.add(req)
                    b.open_pos = req_pos.pop(id(req), b.open_pos)  # opened when its REQUEST arrived
            if th is None:
                builders.append(b)

        elif et == "response":
            # async, no correlation id. Responses arrive in REQUEST order, so close the OLDEST
            # still-open request (FIFO). Candidates: open thread builders (requests that did MI
            # work) AND pending requests with no work yet (simple request→response calls). This is
            # user-safe: a response carries no user, so a wrong guess can't mix two users' work.
            best_thread = min(open_by_thread, key=lambda k: open_by_thread[k].open_pos, default=None)
            best_thread_pos = open_by_thread[best_thread].open_pos if best_thread is not None else None
            best_req = min(pending_reqs, key=lambda r: req_pos.get(id(r), 0), default=None)
            best_req_pos = req_pos.get(id(best_req)) if best_req is not None else None

            if best_thread is not None and (best_req_pos is None or best_thread_pos <= best_req_pos):
                b = open_by_thread.pop(best_thread)
                b.add(e)
                builders.append(b)
            elif best_req is not None:
                pending_reqs.remove(best_req)
                b = _TxnBuilder()
                b.add(best_req)
                b.add(e)
                builders.append(b)
            else:
                b = _TxnBuilder()
                b.add(e)
                builders.append(b)

    builders.extend(open_by_thread.values())
    for r in pending_reqs:  # REQUESTs with no work and no RESPONSE -> their own (incomplete) txn
        b = _TxnBuilder()
        b.add(r)
        builders.append(b)
    return builders


async def regroup_all(db: AsyncSession) -> dict:
    """Full rebuild: delete existing transactions and regroup ALL entries by timestamp."""
    # 1. wipe derived transactions (entries.transaction_id -> NULL via FK ON DELETE SET NULL)
    await db.execute(delete(LogTransaction))
    await db.commit()

    # 2. fetch all entries in stream order (timestamp, then file + line as tiebreak)
    rows = list(
        (await db.execute(
            select(LogEntry).order_by(
                LogEntry.timestamp.asc().nullslast(),
                LogEntry.source_file.asc(),
                LogEntry.line_number.asc(),
            )
        )).scalars().all()
    )

    # 3. group + persist
    builders = _group(rows)
    created = 0
    orphan_entries = len(rows)
    by_status: dict[str, int] = {}
    for b in builders:
        # entries were attached out of stream order (a REQUEST is bound after its work appears);
        # re-sort chronologically so seq + source_file_start/end and the rendered timeline are right.
        b.entries.sort(key=lambda e: (e.timestamp is None, e.timestamp, e.line_number or 0))
        values = b.compute()
        txn = LogTransaction(**values)
        db.add(txn)
        await db.flush()  # get txn.id
        for i, e in enumerate(b.entries):
            e.transaction_id = txn.id
            e.seq = i
        orphan_entries -= len(b.entries)
        created += 1
        by_status[txn.status.value] = by_status.get(txn.status.value, 0) + 1

    await db.commit()
    stats = {
        "entries_scanned": len(rows),
        "transactions_created": created,
        "orphan_entries": orphan_entries,
        "by_status": by_status,
    }
    logger.info("Stage 2 regroup: %s", stats)
    return stats
