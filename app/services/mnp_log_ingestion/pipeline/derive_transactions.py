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
    """Thread+user-aware grouping that demultiplexes concurrent requests.

    The M3 server processes many users at once, so the timestamp-ordered stream interleaves them,
    and — because it's .NET async — a thread can even be reused MID-request to run another user's
    continuation, then resume the first. So thread alone is not a clean per-request lock. We key an
    open transaction by **(thread, user)**: every log line carries the log4net context user
    (`user_ctx`, e.g. "(CPRICE)"), so two users sharing a thread get two separate open builders and
    a thread that flips A→B→A re-merges A's work correctly instead of mixing or fragmenting it.

    Rules:
      - a line WITH a user routes to its (thread, user) builder (creating one if needed) and marks
        that stream as the thread's current one;
      - a line with NO user (some narration / mi bodies log as "(null)") inherits the thread's
        current stream — it belongs to whatever request is live on that thread right now;
      - a REQUEST is paired to its work by ReqID (GET) or, for a POST whose MoveNext has no id, to
        the body it immediately precedes; a GET REQUEST is bound by User once its work appears;
      - a RESPONSE (no payload user/id, but a header user) closes the OLDEST still-open request FOR
        THAT USER (FIFO within the user).
    Net guarantee: a transaction can never contain two users' lines, and a response can never be
    stitched onto another user's request.
    """
    builders: list[_TxnBuilder] = []
    # an open transaction is keyed by (thread, user). user is None only for anonymous streams that
    # never saw a user (then the stream position is appended to keep them distinct).
    open_by_key: dict[tuple, _TxnBuilder] = {}
    current_by_thread: dict[str | None, tuple] = {}  # thread -> its currently-active key (null inherit)
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
            if req_user(r) == user:
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

    def txn_user(b: _TxnBuilder) -> str | None:
        """The user a builder belongs to. Prefer the log4net context user (`user_ctx`) — it's on
        every line — then fall back to a request/body User or mi_call m3user."""
        for e in b.entries:
            if e.user_ctx:
                return e.user_ctx
        return bound_user(b)

    def req_user(r: LogEntry) -> str | None:
        return r.user_ctx or _entry_user(r)

    def close(key: tuple) -> _TxnBuilder | None:
        b = open_by_key.pop(key, None)
        if b is not None and current_by_thread.get(key[0]) == key:
            del current_by_thread[key[0]]
        return b

    for i, e in enumerate(entries):
        et = e.entry_type.value
        th = e.thread

        if et == "request":
            pending_reqs.append(e)
            req_pos[id(e)] = i

        elif et == "request_body":
            # the POST body line itself usually logs as "(null)"; its user is the JSON "User" field.
            u = e.user_ctx or _entry_user(e)
            key = (th, u)
            if key in open_by_key:
                builders.append(close(key))  # prior cycle for this (thread,user): no RESPONSE
            b = _TxnBuilder()
            req = take_by_reqid(_entry_reqid(e)) or take_post_request()
            if req is not None:
                b.add(req)
            b.add(e)
            b.open_pos = req_pos.pop(id(req), i) if req is not None else i
            open_by_key[key] = b
            current_by_thread[th] = key

        elif et in _INTERNAL:
            u = e.user_ctx
            if u is not None:
                key = (th, u)
                b = open_by_key.get(key)
                if b is None:
                    b = _TxnBuilder()
                    b.open_pos = i
                    open_by_key[key] = b
                current_by_thread[th] = key
            else:
                # no user on this line -> it belongs to whatever stream is live on this thread now
                key = current_by_thread.get(th)
                b = open_by_key.get(key) if key is not None else None
                if b is None:
                    key = (th, None, i)  # anonymous stream (no user seen yet on this thread)
                    b = _TxnBuilder()
                    b.open_pos = i
                    open_by_key[key] = b
                    current_by_thread[th] = key
            b.add(e)
            bu = u or _entry_user(e)
            if bu and not has_request(b):  # bind a pending GET REQUEST now that we know the user
                req = take_by_user(bu)
                if req is not None:
                    b.add(req)
                    b.open_pos = req_pos.pop(id(req), b.open_pos)  # opened when its REQUEST arrived

        elif et == "response":
            # async: no payload user/id, but the log4net header carries the context user. Restrict
            # candidates to that user so a response can never close another user's request, then pick
            # the OLDEST still-open request (FIFO). Candidates: open (thread,user) builders that did
            # work AND pending requests with no work yet. If the user filter leaves nothing (user-less
            # response, or its request isn't open), fall back to all candidates so it still lands.
            ru = e.user_ctx
            keys = list(open_by_key)
            if ru is not None:
                u_keys = [k for k in keys if txn_user(open_by_key[k]) == ru]
                u_reqs = [r for r in pending_reqs if req_user(r) == ru]
                if not u_keys and not u_reqs:
                    u_keys, u_reqs = keys, pending_reqs
            else:
                u_keys, u_reqs = keys, pending_reqs

            best_key = min(u_keys, key=lambda k: open_by_key[k].open_pos, default=None)
            best_key_pos = open_by_key[best_key].open_pos if best_key is not None else None
            best_req = min(u_reqs, key=lambda r: req_pos.get(id(r), 0), default=None)
            best_req_pos = req_pos.get(id(best_req)) if best_req is not None else None

            if best_key is not None and (best_req_pos is None or best_key_pos <= best_req_pos):
                b = close(best_key)
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

    builders.extend(open_by_key.values())
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
