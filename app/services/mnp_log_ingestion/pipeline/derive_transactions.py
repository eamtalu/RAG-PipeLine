# derive_transactions.py — Stage 2 of the log pipeline (derive transactions from entries)
#
#   Reads log_entries ORDERED BY timestamp, runs the REQUEST→RESPONSE state machine, and builds
#   log_transactions — promoting the common WMS dimensions to columns and keeping the rest in
#   attributes JSONB. Because it reads in time order, a transaction whose REQUEST and RESPONSE live
#   in different rotated files is stitched naturally.
#
#   Two entry points:
#     - regroup_all(db)         FULL rebuild — deletes ALL transactions, regroups every entry. Use
#                               for historical backfill / repair. Idempotent (deterministic ids).
#     - regroup_incremental(db) LIVE path — keeps SEALED transactions untouched, frees only the
#                               unsealed "live tail" + new entries, regroups just those. O(recent),
#                               so it scales for continuous ingestion. This is what the worker runs.
#
#   Transaction ids are DETERMINISTIC (uuid5 of the anchor entry's content hash), so regrouping the
#   same entries reproduces the same id — saved/cited ids stay valid across cycles.

"""Stage 2 — group raw log_entries into log_transactions."""

import logging
import uuid
from datetime import datetime, timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus

logger = logging.getLogger(__name__)

# request params / body keys we never want to keep verbatim in attributes
_SENSITIVE = {"password", "accesstoken", "m3credentials", "m3usercredentials", "cipher"}

# namespace for deterministic (uuid5) transaction ids — fixed so ids are reproducible forever
_TXN_NS = uuid.UUID("6f9c2a1e-7b54-4e2d-9a3c-1d0e8f5b4a21")


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
            "customer_code": self.entries[0].customer_code,
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

    def last_ts(b: _TxnBuilder) -> datetime | None:
        return next((e.timestamp for e in reversed(b.entries) if e.timestamp is not None), None)

    gap = timedelta(seconds=settings.log_open_gap_seconds)

    def evict_stale(now: datetime) -> None:
        """Abandon open builders / pending requests idle longer than the gap, so a far-later RESPONSE
        (esp. a user-less FIFO match) can't bind across a huge time gap into a bloated transaction."""
        horizon = now - gap
        for k in [k for k, bd in open_by_key.items()
                  if (lt := last_ts(bd)) is not None and lt < horizon]:
            builders.append(close(k))
        for r in [r for r in pending_reqs if r.timestamp is not None and r.timestamp < horizon]:
            pending_reqs.remove(r)
            nb = _TxnBuilder()
            nb.add(r)
            nb.open_pos = req_pos.pop(id(r), -1)
            builders.append(nb)

    for i, e in enumerate(entries):
        et = e.entry_type.value
        th = e.thread
        if e.timestamp is not None:
            evict_stale(e.timestamp)

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


def _anchor(entries: list[LogEntry]) -> str:
    """Stable seed for a transaction's deterministic id. Prefer the REQUEST/REQUEST BODY entry's
    content hash (the natural, content-unique start); else the earliest entry's. `entry_hash` is
    sha256(raw_body) incl. the ms timestamp, so it uniquely & permanently identifies the line.
    The seed is prefixed with the customer_code so two customers with an identical anchor line can
    never produce the same transaction id."""
    req = next((e for e in entries if e.entry_type.value in ("request", "request_body")), None)
    e = req or entries[0]
    seed = e.entry_hash or f"{e.source_file}:{e.line_number}"
    return f"{e.customer_code}:{seed}"


def _txn_id(entries: list[LogEntry]) -> uuid.UUID:
    return uuid.uuid5(_TXN_NS, _anchor(entries))


async def _cutoffs(db: AsyncSession, customer_code: str) -> tuple[datetime | None, datetime | None]:
    """(seal_cutoff, abandon_cutoff) measured against the NEWEST log timestamp FOR THIS CUSTOMER (the
    log's notion of 'now'), not wall-clock — so batch / back-dated ingestion seals correctly too, and
    one customer's stale logs still seal while another's active stream doesn't drag them. Terminal
    transactions seal at seal_cutoff; incomplete ones only at the much-older abandon_cutoff."""
    max_ts = await db.scalar(
        select(func.max(LogEntry.timestamp)).where(LogEntry.customer_code == customer_code)
    )
    if max_ts is None:
        return None, None
    return (max_ts - timedelta(seconds=settings.log_seal_window_seconds),
            max_ts - timedelta(seconds=settings.log_abandon_window_seconds))


def _is_sealed(values: dict, seal_cutoff: datetime | None, abandon_cutoff: datetime | None) -> bool:
    """Seal a transaction (never recompute it) once nothing more can join it:
      - TERMINAL (has a RESPONSE / hard error) and ended before the short seal window; or
      - INCOMPLETE (no response yet) but so old it's past the long abandon window — a late response
        is no longer plausible, so stop waiting. Incomplete-and-recent stays UNSEALED so a slow
        response can still join it (never split a slow request)."""
    ended = values.get("ended_at")
    if ended is None or seal_cutoff is None:
        return False
    if values["status"] == LogTransactionStatus.incomplete:
        return abandon_cutoff is not None and ended < abandon_cutoff
    return ended < seal_cutoff


async def _persist(db: AsyncSession, builders: list[_TxnBuilder], customer_code: str,
                   seal_cutoff: datetime | None, abandon_cutoff: datetime | None) -> dict:
    """Compute + insert each builder with a deterministic id, assign its entries, and seal those
    nothing more can join. Caller commits.

    Crash-proof against a deterministic-id clash: in the live in-order path, ids are unique by
    construction (each transaction's anchor entry is unique). A clash only arises from OUT-OF-ORDER
    / bulk ingestion via the incremental path (a tail builder reconstructs a transaction whose id a
    prior cycle already sealed). We SKIP such a builder (leaving its entries unassigned) and warn —
    the repair path is a full regroup — rather than letting one clash kill the whole grouping cycle.
    """
    created = sealed = assigned = skipped = 0
    by_status: dict[str, int] = {}
    seen: set[uuid.UUID] = set()
    existing: set[uuid.UUID] = set((await db.execute(
        select(LogTransaction.id).where(LogTransaction.customer_code == customer_code)
    )).scalars().all())
    for b in builders:
        # entries were attached out of stream order (a REQUEST is bound after its work appears);
        # re-sort chronologically so seq + source_file_start/end and the rendered timeline are right.
        b.entries.sort(key=lambda e: (e.timestamp is None, e.timestamp, e.line_number or 0))
        tid = _txn_id(b.entries)
        if tid in seen or tid in existing:
            skipped += 1
            continue
        seen.add(tid)
        values = b.compute()
        is_sealed = _is_sealed(values, seal_cutoff, abandon_cutoff)
        txn = LogTransaction(id=tid, sealed=is_sealed, **values)
        db.add(txn)
        await db.flush()  # get txn.id
        for i, e in enumerate(b.entries):
            e.transaction_id = txn.id
            e.seq = i
        assigned += len(b.entries)
        created += 1
        sealed += int(is_sealed)
        by_status[txn.status.value] = by_status.get(txn.status.value, 0) + 1
    if skipped:
        logger.warning("Stage 2: skipped %d builder(s) with an already-sealed id (out-of-order/bulk "
                       "ingest). Run a full regroup (POST /logs/regroup) to rebuild cleanly.", skipped)
    return {"transactions_created": created, "transactions_sealed": sealed,
            "entries_assigned": assigned, "transactions_skipped": skipped, "by_status": by_status}


def _merge_stats(into: dict, part: dict) -> None:
    """Accumulate one customer's _persist result into the running totals."""
    for k in ("transactions_created", "transactions_sealed", "entries_assigned",
              "transactions_skipped", "entries_scanned", "orphan_entries"):
        into[k] = into.get(k, 0) + part.get(k, 0)
    for status, n in part.get("by_status", {}).items():
        into["by_status"][status] = into["by_status"].get(status, 0) + n


async def regroup_all(db: AsyncSession, customer_code: str | None = None) -> dict:
    """FULL rebuild: delete every transaction and regroup ALL entries by timestamp, PER CUSTOMER. For
    historical backfill / repair. Idempotent — deterministic ids mean a rebuild reproduces the same
    ids. Grouping is partitioned by customer_code so .NET thread ids can never cross-stitch tenants.

    Pass `customer_code` to rebuild only one tenant (the manual API path); None rebuilds every
    customer (used for a full repair)."""
    del_stmt = delete(LogTransaction)  # entries.transaction_id -> NULL via ON DELETE SET NULL
    if customer_code is not None:
        del_stmt = del_stmt.where(LogTransaction.customer_code == customer_code)
    await db.execute(del_stmt)
    await db.commit()

    code_stmt = select(LogEntry.customer_code).distinct()
    if customer_code is not None:
        code_stmt = code_stmt.where(LogEntry.customer_code == customer_code)
    codes = (await db.execute(code_stmt)).scalars().all()

    stats = {"mode": "full", "customers": len(codes), "by_status": {}}
    for code in codes:
        rows = list((await db.execute(
            select(LogEntry).where(LogEntry.customer_code == code).order_by(
                LogEntry.timestamp.asc().nullslast(),
                LogEntry.source_file.asc(),
                LogEntry.line_number.asc(),
            )
        )).scalars().all())
        seal_cutoff, abandon_cutoff = await _cutoffs(db, code)
        result = await _persist(db, _group(rows), code, seal_cutoff, abandon_cutoff)
        await db.commit()
        _merge_stats(stats, {**result, "entries_scanned": len(rows),
                             "orphan_entries": len(rows) - result["entries_assigned"]})
    logger.info("Stage 2 regroup (full): %s", stats)
    return stats


async def regroup_incremental(db: AsyncSession, customer_code: str | None = None) -> dict:
    """LIVE path: keep SEALED transactions untouched; free only the unsealed tail, regroup it
    together with newly-ingested (unassigned) entries, PER CUSTOMER. Per-cycle work is bounded by the
    seal window, so this scales for continuous ingestion. Sealed transactions keep their ids.

    Pass `customer_code` to touch only one tenant (manual API path); None processes every customer
    with unassigned entries (what the background worker runs)."""
    # 1. free unsealed transactions only (their entries.transaction_id -> NULL); sealed rows stay
    free_stmt = delete(LogTransaction).where(LogTransaction.sealed.is_(False))
    if customer_code is not None:
        free_stmt = free_stmt.where(LogTransaction.customer_code == customer_code)
    await db.execute(free_stmt)
    await db.commit()

    # 2. customers that have any still-unassigned entry (freed unsealed + brand-new)
    code_stmt = select(LogEntry.customer_code).where(LogEntry.transaction_id.is_(None)).distinct()
    if customer_code is not None:
        code_stmt = code_stmt.where(LogEntry.customer_code == customer_code)
    codes = (await db.execute(code_stmt)).scalars().all()
    if not codes:
        return {"mode": "incremental", "customers": 0, "entries_scanned": 0,
                "transactions_created": 0, "transactions_sealed": 0, "by_status": {}}

    stats = {"mode": "incremental", "customers": len(codes), "by_status": {}}
    for code in codes:
        # the live tail for this customer, in stream order
        rows = list((await db.execute(
            select(LogEntry).where(
                LogEntry.customer_code == code, LogEntry.transaction_id.is_(None)
            ).order_by(
                LogEntry.timestamp.asc().nullslast(),
                LogEntry.source_file.asc(),
                LogEntry.line_number.asc(),
            )
        )).scalars().all())
        if not rows:
            continue
        seal_cutoff, abandon_cutoff = await _cutoffs(db, code)
        result = await _persist(db, _group(rows), code, seal_cutoff, abandon_cutoff)
        await db.commit()
        _merge_stats(stats, {**result, "entries_scanned": len(rows),
                             "orphan_entries": len(rows) - result["entries_assigned"]})
    logger.info("Stage 2 regroup (incremental): %s", stats)
    return stats
