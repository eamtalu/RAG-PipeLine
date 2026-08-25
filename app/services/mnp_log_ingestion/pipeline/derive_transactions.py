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
from datetime import datetime, timedelta, timezone
from functools import lru_cache

from sqlalchemy import (String, Enum as SAEnum, delete, func, inspect as sa_inspect, or_, select,
                        false as sa_false, text as sa_text, true as sa_true, update)
from sqlalchemy.ext.asyncio import AsyncSession

from app.settings import settings
from app.config.database import async_session
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.mnp_log_ingestion.timefmt import to_display, set_display_timezone
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_regroup_run import LogRegroupRun, LogRegroupRunStatus
from app.services.mnp_log_ingestion.io_errors import is_disk_io_error, disk_io_detail
from app.services.mnp_log_ingestion.pipeline import (assignments, continuity,
                                                    fingerprints, stream_state,
                                                    time_bounds)
from app.services.queueing import retry_policy
from app.services.analytics import pending_windows as analytics_tickets

logger = logging.getLogger(__name__)

# request params / body keys we never want to keep verbatim in attributes
_SENSITIVE = {"password", "accesstoken", "m3credentials", "m3usercredentials", "cipher"}

# namespace for deterministic (uuid5) transaction ids — fixed so ids are reproducible forever
_TXN_NS = uuid.UUID("6f9c2a1e-7b54-4e2d-9a3c-1d0e8f5b4a21")


@lru_cache(maxsize=1)
def _txn_str_limits() -> dict[str, int]:
    """{attribute: max_length} for every bounded VARCHAR column on LogTransaction (Text has no limit;
    Enum is excluded — its values are fixed and valid). Used to defensively cap promoted dimension
    values to their column width so a single over-length source value (e.g. a composite ItemNumber the
    WMS puts in the request URL) can never raise StringDataRightTruncationError and abort a whole
    Stage 2 batch. Computed once from the ORM mapping, so new/resized columns are picked up for free."""
    return {c.key: c.type.length
            for c in sa_inspect(LogTransaction).columns
            if isinstance(c.type, String) and not isinstance(c.type, SAEnum)
            and getattr(c.type, "length", None)}


class _TxnBuilder:
    """Accumulates the entries of one API request/response cycle."""

    def __init__(self) -> None:
        self.entries: list[LogEntry] = []
        # S2: a DURABLE stream position, not a batch index. See `_stream_pos`.
        self.open_pos: tuple = _NO_POS

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
            response_summary = response_entry.message.replace("RESPONSE:", "").strip()[:500]

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
            # `date` is the day for cheap grouping/filtering and must be the LOCAL (display-zone) day,
            # not the UTC day — else a transaction in the first hour after local midnight (e.g. 00:30
            # BST = 23:30 UTC the previous day) would be filed under the wrong date.
            "date": to_display(started).date() if started else None,
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


def _group(entries: list[LogEntry], seed: dict | None = None) -> list[_TxnBuilder]:
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

    # S4. `seed` is state read back from `log_open_stream`, so a stream can CONTINUE across a process
    # boundary instead of being re-derived from a padded window. Absent (the default) reproduces the
    # pre-S4 behaviour exactly, which is what keeps every existing caller and test unaffected.
    #
    # Only streams the guard already accepted are ever passed in; `stream_state.usable` does that
    # filtering, so nothing here has to reason about clocks.
    seeded_ids: set = set()
    for st in (seed or {}).get("streams", ()):
        b = _TxnBuilder()
        b.open_pos = st["open_pos"]
        for e in st["entries"]:
            b.add(e)
            seeded_ids.add(e.id)
        key = (st["thread"], st["user_ctx"])
        open_by_key[key] = b
        if st["is_current"]:
            current_by_thread[st["thread"]] = key
    pending_reqs: list[LogEntry] = list((seed or {}).get("pending") or [])
    # S2: there is no `req_pos` map any more. It existed to remember where in the stream each pending
    # request arrived, which is a property of the ENTRY - so it is derived by `_stream_pos` rather than
    # stored. That also removed a leak: the old `req_pos.pop(id(r), -1)` left an orphaned key behind
    # whenever a request was consumed by a path that did not pop it.

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
            nb.open_pos = _stream_pos(r)
            builders.append(nb)

    for i, e in enumerate(entries):
        # S4 fix: an entry a seeded builder already carries must not be processed again. Replaying it
        # would close the seeded builder as "a prior cycle" the moment its own REQUEST re-arrived,
        # duplicating the transaction as two groups. Measured on live data: one window went from 1
        # cold group to 17 seeded ones through exactly this plus out-of-scope seeding.
        if seeded_ids and e.id in seeded_ids:
            continue
        et = e.entry_type.value
        th = e.thread
        if e.timestamp is not None:
            evict_stale(e.timestamp)

        if et == "request":
            pending_reqs.append(e)

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
            b.open_pos = _stream_pos(req) if req is not None else _stream_pos(e)
            open_by_key[key] = b
            current_by_thread[th] = key

        elif et in _INTERNAL:
            u = e.user_ctx
            if u is not None:
                key = (th, u)
                b = open_by_key.get(key)
                if b is None:
                    b = _TxnBuilder()
                    b.open_pos = _stream_pos(e)
                    open_by_key[key] = b
                current_by_thread[th] = key
            else:
                # no user on this line -> it belongs to whatever stream is live on this thread now
                key = current_by_thread.get(th)
                b = open_by_key.get(key) if key is not None else None
                if b is None:
                    key = (th, None, i)  # anonymous stream (no user seen yet on this thread)
                    b = _TxnBuilder()
                    b.open_pos = _stream_pos(e)
                    open_by_key[key] = b
                    current_by_thread[th] = key
            b.add(e)
            bu = u or _entry_user(e)
            if bu and not has_request(b):  # bind a pending GET REQUEST now that we know the user
                req = take_by_user(bu)
                if req is not None:
                    b.add(req)
                    b.open_pos = _stream_pos(req)  # opened when its REQUEST arrived

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
            best_req = min(u_reqs, key=_stream_pos, default=None)
            best_req_pos = _stream_pos(best_req) if best_req is not None else None

            if best_key is not None and (best_req_pos is None or best_key_pos <= best_req_pos):
                b = close(best_key)
                b.add(e)
                builders.append(b)
            elif best_req is not None:
                pending_reqs.remove(best_req)
                b = _TxnBuilder()
                b.add(best_req)
                b.add(e)
                # S2: set, where it previously stayed at the -1 default. Harmless while it was a batch
                # index, because these builders are appended immediately and never compared again; not
                # harmless once S4 reads the field back from a table and expects it to mean something.
                b.open_pos = _stream_pos(best_req)
                builders.append(b)
            else:
                b = _TxnBuilder()
                b.add(e)
                b.open_pos = _stream_pos(e)     # S2: an orphan response opens at its own position
                builders.append(b)

    builders.extend(open_by_key.values())
    for r in pending_reqs:  # REQUESTs with no work and no RESPONSE -> their own (incomplete) txn
        b = _TxnBuilder()
        b.add(r)
        b.open_pos = _stream_pos(r)   # S2: as above - every builder now carries a real position
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


def _entry_stream_order(e: LogEntry):
    """Chronological order within one transaction, NULL timestamps LAST and stable on line number.

    The `is None` flag rather than the raw value because Python refuses to compare None to a datetime,
    and a transaction can legitimately contain an entry whose timestamp did not parse.

    (This docstring said "NULL timestamps first" until S2. It was wrong and always had been: `False`
    sorts before `True`, so an entry that HAS a timestamp comes first and the unparsable ones trail.
    The behaviour is right and is what the renderer wants; only the sentence was inverted.)
    """
    return (e.timestamp is None, e.timestamp, e.line_number or 0)


def _stream_pos(e: LogEntry):
    """S2. Where this entry sits in the tenant's stream, as a DURABLE comparable tuple.

    Derived from the entry rather than assigned by whoever is looping over it, which is the entire
    point: nothing has to be remembered, so nothing can be remembered wrongly across a process
    boundary. S4 reads this state back from a table, and the two things it replaced could not survive
    that at all:

        _TxnBuilder.open_pos     an index within the CURRENT BATCH. Batch 2's index 0 is not batch 1's
                                 index 0, so the number means nothing once written down.
        req_pos[id(entry)]       a CPython object address. Not stable across processes, and not even
                                 stable within one, since CPython reuses addresses after collection.

    `source_file` is included and is deliberately NOT in `_entry_stream_order` above. That helper
    orders entries WITHIN one transaction, where the file is effectively constant; this one is a key
    across a whole window, which routinely spans several files. Without the filename two entries on
    line 5 of two different files compare equal - exactly the collision a durable key must not have.

    Ordering matches the ORDER BY of both entry reads, so the grouper's notion of "earlier" agrees with
    the order it receives rows in.
    """
    return (e.timestamp is None, e.timestamp, e.source_file or "", e.line_number or 0)


#: A position that sorts before every real one, for a builder opened by something with no request to
#: attribute it to. `False` because a real NULL-timestamp entry carries `True` and must sort after.
_NO_POS = (False, datetime.min.replace(tzinfo=timezone.utc), "", -1)


def _clash_window(timestamps) -> "time_bounds.UtcWindow | None":
    """The span in which a transaction colliding with these entries could have started.

    A transaction's id is `uuid5` of its anchor entry's hash, so a colliding row was built from the
    SAME anchor entry and shares its timestamp. Its `started_at` is the minimum over its own entries,
    which the system guarantees lie within one pad of each other — so it sits in
    `[anchor_ts - pad, anchor_ts]`. Padding the span of the entries being written therefore cannot
    miss a real clash; it is exact, not an approximation that happens to work.

    None when no timestamp can be derived at all, which tells the caller to look the ids up without a
    time bound rather than filter everything out.
    """
    return time_bounds.from_instants(timestamps, pad=_regroup_pad())


def _existing_ids_stmt(customer_code: str, ids, *, window):
    """The clash-check SELECT, separated so a test can EXPLAIN it and confirm it prunes."""
    stmt = select(LogTransaction.id).where(
        LogTransaction.customer_code == customer_code, LogTransaction.id.in_(list(ids)))
    if window is not None:
        # include_null: a transaction whose entries all lack a parsable timestamp has a NULL
        # started_at and lives in the DEFAULT partition. A range predicate is FALSE for NULL, so
        # without this branch that clash would go unseen and the rebuild would overwrite the row.
        stmt = stmt.where(window.covers(LogTransaction.started_at, include_null=True))
    return stmt


async def _existing_transaction_ids(db: AsyncSession, customer_code: str, ids, *,
                                    window) -> set[uuid.UUID]:
    """Which of `ids` already exist for this tenant.

    This used to load EVERY transaction id the tenant had ever had into a Python set — 109k rows per
    call in production, growing forever, and after partitioning an Append across all ~130 partitions.
    Asking only about the ids actually being written is both exact and bounded by the size of the
    batch rather than by the tenant's history.
    """
    ids = list(ids)
    if not ids:
        return set()
    return set((await db.execute(_existing_ids_stmt(customer_code, ids, window=window)))
               .scalars().all())


def _recent_max_ts_stmt(customer_code: str):
    """The newest entry timestamp within the recent lookback — the fast path for `_cutoffs`.

    Carries an explicit `timestamp >=` bound so a partitioned `log_entries` prunes to the last few
    days instead of taking the max of all 60 partitions. Kept as a statement so a test can EXPLAIN it
    and confirm the partition key is actually in the predicate.
    """
    floor = datetime.now(timezone.utc) - timedelta(days=settings.log_cutoff_lookback_days)
    return select(func.max(LogEntry.timestamp)).where(
        LogEntry.customer_code == customer_code, LogEntry.timestamp >= floor)


async def _max_entry_ts(db: AsyncSession, customer_code: str) -> datetime | None:
    """Newest entry timestamp for the tenant: bounded probe first, unbounded scan only if it misses.

    The bound alone would be wrong. A tenant whose ingestion has stopped, or one importing back-dated
    logs, has its newest entry OUTSIDE the lookback — the probe returns NULL, `_cutoffs` reports "no
    entries", and nothing that tenant owns ever seals. So a miss falls back to the full scan and pays
    the old cost, which is the rare case; a live tenant always hits the bounded path.
    """
    recent = await db.scalar(_recent_max_ts_stmt(customer_code))
    if recent is not None:
        return recent
    return await db.scalar(
        select(func.max(LogEntry.timestamp)).where(LogEntry.customer_code == customer_code)
    )


async def _cutoffs(db: AsyncSession, customer_code: str) -> tuple[datetime | None, datetime | None]:
    """(seal_cutoff, abandon_cutoff) measured against the NEWEST log timestamp FOR THIS CUSTOMER (the
    log's notion of 'now'), not wall-clock — so batch / back-dated ingestion seals correctly too, and
    one customer's stale logs still seal while another's active stream doesn't drag them. Terminal
    transactions seal at seal_cutoff; incomplete ones only at the much-older abandon_cutoff."""
    max_ts = await _max_entry_ts(db, customer_code)
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


async def _resolve_ids(db: AsyncSession, builders: list[_TxnBuilder], customer_code: str,
                       cont: continuity.Continuity = continuity.EMPTY
                       ) -> tuple[list[uuid.UUID], set[uuid.UUID]]:
    """Each builder's id, plus the subset of them that already exist.

    The sort comes first and is not cosmetic: entries are attached out of stream order (a REQUEST is
    bound after its work appears), and `_txn_id` reads the ANCHOR entry, which is whichever entry the
    order puts first. Computing ids before sorting would produce a different id for the same
    transaction and break the idempotency the whole design rests on. Sorting also makes seq,
    source_file_start/end and the rendered timeline correct.

    An id is INHERITED where the builder rebuilds a transaction that just got freed, and MINTED by
    `_txn_id` where it does not. Inheriting is what stops an id changing when a backfill alters the
    anchor entry — see `continuity` for why a content-derived id cannot be stable. `_txn_id` is
    untouched, so a first-time group is identified exactly as it is today and no existing id is ever
    rewritten.

    `cont` defaults to no predecessors, which reduces this to the previous behaviour. Only
    `regroup_window` can supply one, because only it frees transactions.
    """
    for b in builders:
        b.entries.sort(key=_entry_stream_order)
    ids = continuity.assign([b.entries for b in builders], cont, fallback=_txn_id)
    existing = await _existing_transaction_ids(
        db, customer_code, ids,
        window=_clash_window([e.timestamp for b in builders for e in b.entries]))
    return ids, existing


def _over_length(values: dict, limits: dict[str, int]):
    """(key, limit, value) for each promoted string longer than the column that has to hold it."""
    for key, limit in limits.items():
        value = values.get(key)
        if isinstance(value, str) and len(value) > limit:
            yield key, limit, value


def _cap_over_length(values: dict, customer_code: str) -> dict:
    """Trim promoted string dimensions to their column width, in place.

    The full value is preserved on the raw log_entry; only the queryable transaction column is capped.
    Without this, one over-length value (e.g. a 70-char composite ItemNumber) raises
    StringDataRightTruncationError, aborting the batch — and since finalize retries oldest-first, that
    stalls ALL stitching for the tenant. See docs/stage2-stitching-stall-postmortem-and-fix.md.
    """
    for key, limit, value in _over_length(values, _txn_str_limits()):
        logger.warning("Stage 2 [%s]: capped over-length %s (%d > %d) to fit column",
                       customer_code, key, len(value), limit)
        values[key] = value[:limit]
    return values


async def _update_transaction(db: AsyncSession, *, tid: uuid.UUID, started_at, values: dict,
                              is_sealed: bool, row_fp: str, members_fp: str) -> None:
    """S3. UPDATE one transaction in place. Caller commits.

    `n_tup_upd` on these tables was exactly 0 everywhere before this function existed.

    Addressed by the FULL key `(id, started_at)`, never by id alone. `started_at` is the partition key,
    so without it PostgreSQL cannot prune and the UPDATE has to consider all 95 partitions. It is also
    nullable, so the NULL case needs `IS NULL` rather than `= NULL`: a transaction all of whose entries
    lack a parsable timestamp lives in the DEFAULT partition, and that is a real case (A7).

    `created_at` is deliberately NOT touched, which is what finally makes it mean "first written".
    `updated_at` moves, and the notification cursor reads that column - which is why S1 moved the
    cursor BEFORE this shipped rather than alongside it.
    """
    await db.execute(
        update(LogTransaction)
        .where(LogTransaction.id == tid,
               LogTransaction.started_at.is_(None) if started_at is None
               else LogTransaction.started_at == started_at)
        .values(**values, sealed=is_sealed, updated_at=datetime.now(timezone.utc),
                row_fingerprint=row_fp, members_fingerprint=members_fp)
        .execution_options(synchronize_session=False))


async def _write_transaction(db: AsyncSession, *, tid: uuid.UUID, values: dict, is_sealed: bool,
                             entries: list[LogEntry], customer_code: str,
                             row_fp: str | None = None,
                             members_fp: str | None = None) -> LogTransaction:
    """Insert one transaction and record which entries belong to it. Caller commits."""
    # S1: `updated_at` is stamped equal to `created_at` at birth, so a row nothing ever updates
    # behaves exactly as it did before the column existed. The notification cursor reads it, so a row
    # written without it would be invisible to every rule.
    now = datetime.now(timezone.utc)
    txn = LogTransaction(id=tid, sealed=is_sealed, created_at=now, updated_at=now,
                         row_fingerprint=row_fp, members_fingerprint=members_fp,
                         **values)
    db.add(txn)
    await db.flush()  # get txn.id
    await assignments.write(db, transaction_id=txn.id, entries=entries,
                            customer_code=customer_code)
    return txn


async def _persist(db: AsyncSession, builders: list[_TxnBuilder], customer_code: str,
                   seal_cutoff: datetime | None, abandon_cutoff: datetime | None,
                   cont: continuity.Continuity = continuity.EMPTY,
                   stored: dict | None = None) -> dict:
    """Compute + insert each builder with a deterministic id, assign its entries, and seal those
    nothing more can join. Caller commits.

    Crash-proof against a deterministic-id clash: in the live in-order path, ids are unique by
    construction (each transaction's anchor entry is unique). A clash only arises from OUT-OF-ORDER
    / bulk ingestion via the incremental path (a tail builder reconstructs a transaction whose id a
    prior cycle already sealed). We SKIP such a builder (leaving its entries unassigned) and warn —
    the repair path is a full regroup — rather than letting one clash kill the whole grouping cycle.
    """
    # pin the display zone to THIS customer so each builder's `date` (computed via to_display) is the
    # customer's LOCAL calendar day, not the host's / a global default.
    set_display_timezone(await get_customer_timezone(db, customer_code))

    def _status_of(values) -> str:
        st = values["status"]
        return st.value if hasattr(st, "value") else str(st)

    created = sealed = assigned = skipped = 0
    unchanged = row_only = rewritten = 0
    by_status: dict[str, int] = {}
    seen: set[uuid.UUID] = set()
    stored = stored or {}
    tz = await get_customer_timezone(db, customer_code)
    ids, existing = await _resolve_ids(db, builders, customer_code, cont)
    for b, tid in zip(builders, ids):
        if tid in seen:
            skipped += 1
            continue
        # S3: a stored row this window is rebuilding is NOT a clash - it is the row being compared
        # against. Before S3 every such row had already been deleted, so `existing` could only mean a
        # genuine out-of-order collision. Now it means both, and only ids absent from `stored` are the
        # collision the warning below is about.
        if tid in existing and tid not in stored:
            skipped += 1
            continue
        seen.add(tid)
        values = _cap_over_length(b.compute(), customer_code)
        is_sealed = _is_sealed(values, seal_cutoff, abandon_cutoff)
        r_fp = fingerprints.row(values, sealed=is_sealed, tenant_timezone=tz)
        m_fp = fingerprints.members(e.id for e in b.entries)
        status = _status_of(values)

        prior = stored.get(tid)
        if prior is not None:
            # The 98.7% case. A NULL stored digest never matches a real one, which is what lets the
            # migration skip a backfill: every pre-S3 row is rewritten exactly once, filling its
            # columns in as a side effect of work the pipeline was doing anyway.
            if prior.row_fingerprint == r_fp and prior.members_fingerprint == m_fp:
                unchanged += 1
                by_status[status] = by_status.get(status, 0) + 1
                continue

            members_changed = prior.members_fingerprint != m_fp
            await _update_transaction(db, tid=tid, started_at=prior.started_at, values=values,
                                      is_sealed=is_sealed, row_fp=r_fp, members_fp=m_fp)
            if members_changed:
                # Only when MEMBERSHIP moved. This split is what takes assignments from 18.1 writes
                # per surviving row to 1.0: a seal flip changes the row and not the members, so it
                # never touches an assignment row at all.
                await assignments.delete_for_transactions(db, [tid])
                await assignments.write(db, transaction_id=tid, entries=b.entries,
                                        customer_code=customer_code)
                assigned += len(b.entries)
                rewritten += 1
            else:
                row_only += 1
            sealed += int(is_sealed)
            by_status[status] = by_status.get(status, 0) + 1
            continue

        txn = await _write_transaction(db, tid=tid, values=values, is_sealed=is_sealed,
                                       entries=b.entries, customer_code=customer_code,
                                       row_fp=r_fp, members_fp=m_fp)
        assigned += len(b.entries)
        created += 1
        sealed += int(is_sealed)
        by_status[txn.status.value] = by_status.get(txn.status.value, 0) + 1

    # S3: anything stored in this window that the rebuild did NOT produce has genuinely vanished - a
    # merge, a split, or an upstream delete - and has to go. This branch is not optional: leaving it
    # out would keep a transaction alive that corresponds to no entries, and the analytics range diff
    # would never see it disappear.
    vanished = [tid for tid in stored if tid not in seen]
    if vanished:
        await assignments.delete_for_transactions(db, vanished)
        await db.execute(delete(LogTransaction).where(LogTransaction.id.in_(vanished)))
    if skipped:
        logger.warning("Stage 2: skipped %d builder(s) with an already-sealed id (out-of-order/bulk "
                       "ingest). Run a full regroup (POST /logs/regroup) to rebuild cleanly.", skipped)
    return {"transactions_created": created, "transactions_sealed": sealed,
            "entries_assigned": assigned, "transactions_skipped": skipped, "by_status": by_status,
            # S3's own counters, so a run can be read for what it actually WROTE rather than inferred
            # from row counts. `transactions_unchanged` is the one to watch: it should be the large
            # majority, and a run where it is zero means the skip is not working.
            "transactions_unchanged": unchanged, "transactions_row_only": row_only,
            "transactions_rewritten": rewritten, "transactions_deleted": len(vanished)}


def _merge_stats(into: dict, part: dict) -> None:
    """Accumulate one customer's _persist result into the running totals."""
    for k in ("transactions_created", "transactions_sealed", "entries_assigned",
              "transactions_skipped", "entries_scanned", "orphan_entries",
              # S3's counters. Added here rather than only returned by `_persist`, because the E2E
              # found them silently absent from every caller's result: the numbers that say whether the
              # skip is working were computed and then dropped one function short of anywhere they
              # could be read.
              "transactions_unchanged", "transactions_row_only", "transactions_rewritten",
              "transactions_deleted"):
        into[k] = into.get(k, 0) + part.get(k, 0)
    for status, n in part.get("by_status", {}).items():
        into["by_status"][status] = into["by_status"].get(status, 0) + n


async def regroup_all(db: AsyncSession, customer_code: str | None = None) -> dict:
    """FULL rebuild: delete every transaction and regroup ALL entries by timestamp, PER CUSTOMER. For
    historical backfill / repair. Idempotent — deterministic ids mean a rebuild reproduces the same
    ids. Grouping is partitioned by customer_code so .NET thread ids can never cross-stitch tenants.

    Pass `customer_code` to rebuild only one tenant (the manual API path); None rebuilds every
    customer (used for a full repair)."""
    # Assignments first: the FK cascade is gone (it made partitions undroppable), so they must be
    # removed explicitly or every full regroup leaves orphans pointing at deleted transactions.
    # N1, publish site 3 of 5. The span has to be read BEFORE the delete: this frees the tenant's whole
    # history and commits, so afterwards there is nothing left to derive bounds from. One aggregate per
    # tenant rather than every row, since only the extremes matter and `publish` splits by day.
    span_stmt = select(LogTransaction.customer_code,
                       func.min(LogTransaction.started_at),
                       func.max(LogTransaction.started_at)).group_by(LogTransaction.customer_code)
    if customer_code is not None:
        span_stmt = span_stmt.where(LogTransaction.customer_code == customer_code)
    for code, lo, hi in (await db.execute(span_stmt)).all():
        await analytics_tickets.publish_for_transactions(db, code, started_ats=[lo, hi])

    await assignments.delete_for_customer(db, customer_code)
    del_stmt = delete(LogTransaction)
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


async def _codes_needing_regroup(db: AsyncSession, customer_code: str | None) -> list[str]:
    """Tenants with at least one unassigned entry.

    When the caller already named the tenant — which both real call sites do — this is an EXISTENCE
    check for that one code, not a `SELECT DISTINCT customer_code` anti-join over every entry in the
    database. The old query rediscovered a fact the caller had already supplied, at the cost of a
    whole-table scan; after partitioning, one across ~130 partitions.

    The None path keeps the documented "process every tenant" behaviour, scan and all. It is the
    manual/administrative path — the live loop goes through `finalize_pending` -> `regroup_window`,
    which is time-bounded — so the cost lands where someone asked for it.
    """
    if customer_code is not None:
        found = await db.scalar(
            select(LogEntry.customer_code).where(
                LogEntry.customer_code == customer_code, assignments.is_unassigned()
            ).limit(1))
        return [customer_code] if found else []
    return list((await db.execute(
        select(LogEntry.customer_code).where(assignments.is_unassigned()).distinct()
    )).scalars().all())


async def _live_tail(db: AsyncSession, customer_code: str) -> list[LogEntry]:
    """One tenant's unassigned entries that can still join a live transaction, in stream order.

    Bounded to the abandon window (plus a pad) behind the tenant's newest entry. That is not an
    arbitrary cap: past the abandon window every transaction is sealed, so an older unassigned entry
    has nothing left to join and reloading it on every cycle is work that can never produce a result.
    Unbounded, this pulled every unassigned entry for the tenant into one session with no LIMIT — so a
    backlog grew the identity map without limit, the same failure `log_regroup_max_window_seconds`
    was added to prevent in `finalize_pending`.

    Entries with NO timestamp are always included. A range predicate is FALSE for NULL, so bounding
    without that branch would mean a timestamp-less entry is never grouped and never reported —
    silent loss, which is the one outcome worse than a slow query.

    The cost of the bound is that a tenant whose entire backlog predates the window looks idle. That
    is surfaced loudly rather than skipped: the repair is a full regroup, and nothing else would tell
    an operator it is needed.
    """
    floor = None
    max_ts = await _max_entry_ts(db, customer_code)
    if max_ts is not None:
        floor = max_ts - timedelta(seconds=settings.log_abandon_window_seconds) - _regroup_pad()

    stmt = select(LogEntry).where(
        LogEntry.customer_code == customer_code, assignments.is_unassigned())
    if floor is not None:
        stmt = stmt.where(or_(LogEntry.timestamp >= floor, LogEntry.timestamp.is_(None)))
    rows = list((await db.execute(stmt.order_by(
        LogEntry.timestamp.asc().nullslast(),
        LogEntry.source_file.asc(),
        LogEntry.line_number.asc(),
    ))).scalars().all())

    if not rows and floor is not None:
        logger.warning(
            "Stage 2 [%s]: has unassigned entries but all of them predate the live window (before "
            "%s), so incremental regrouping cannot reach them. Run a full regroup to repair.",
            customer_code, floor.isoformat())
    return rows


async def regroup_incremental(db: AsyncSession, customer_code: str | None = None) -> dict:
    """LIVE path: keep SEALED transactions untouched; free only the unsealed tail, regroup it
    together with newly-ingested (unassigned) entries, PER CUSTOMER. Per-cycle work is bounded by the
    seal window, so this scales for continuous ingestion. Sealed transactions keep their ids.

    Pass `customer_code` to touch only one tenant (manual API path); None processes every customer
    with unassigned entries (what the background worker runs)."""
    # 1. free unsealed transactions only; sealed rows stay. Their assignments go first — no cascade
    #    does it for us, and this runs on the live path every cycle, so an orphan here compounds.
    # Selects `started_at` alongside the id now: N1 needs the instant, and after the delete below there
    # is nothing left to derive it from (F1). Bounds MUST come from the freed set rather than from
    # incoming entries, because this delete has no time predicate at all -- an older unsealed row caught
    # in the same sweep would otherwise never be ticketed and its contribution would drift for good.
    unsealed_stmt = select(LogTransaction.id, LogTransaction.started_at,
                           LogTransaction.customer_code).where(LogTransaction.sealed.is_(False))
    free_stmt = delete(LogTransaction).where(LogTransaction.sealed.is_(False))
    if customer_code is not None:
        unsealed_stmt = unsealed_stmt.where(LogTransaction.customer_code == customer_code)
        free_stmt = free_stmt.where(LogTransaction.customer_code == customer_code)
    freed_rows = (await db.execute(unsealed_stmt)).all()
    await assignments.delete_for_transactions(db, [r.id for r in freed_rows])
    await db.execute(free_stmt)

    # N1, publish site 2 of 5. Before the commit, so ticket and change are atomic (invariant 3).
    # Grouped per tenant because `customer_code=None` frees every tenant's unsealed rows at once, and a
    # ticket is meaningless without knowing whose window it describes.
    for code in {r.customer_code for r in freed_rows}:
        await analytics_tickets.publish_for_transactions(
            db, code, started_ats=[r.started_at for r in freed_rows if r.customer_code == code])
    await db.commit()

    # 2. customers that have any still-unassigned entry (freed unsealed + brand-new)
    codes = await _codes_needing_regroup(db, customer_code)
    if not codes:
        return {"mode": "incremental", "customers": 0, "entries_scanned": 0,
                "transactions_created": 0, "transactions_sealed": 0, "by_status": {}}

    stats = {"mode": "incremental", "customers": len(codes), "by_status": {}}
    for code in codes:
        rows = await _live_tail(db, code)
        if not rows:
            continue
        seal_cutoff, abandon_cutoff = await _cutoffs(db, code)
        result = await _persist(db, _group(rows), code, seal_cutoff, abandon_cutoff)
        await db.commit()
        _merge_stats(stats, {**result, "entries_scanned": len(rows),
                             "orphan_entries": len(rows) - result["entries_assigned"]})
    logger.info("Stage 2 regroup (incremental): %s", stats)
    return stats


def _regroup_pad() -> timedelta:
    """Pad applied around a windowed regroup. Floored at the seal window — the max a transaction can
    span — so a misconfigured (too-small) pad can never make the rebuild lossy, only wider."""
    return timedelta(seconds=max(settings.log_regroup_pad_seconds, settings.log_seal_window_seconds))


async def _shadow_compare(db: AsyncSession, customer_code: str, rows: list[LogEntry],
                          authoritative: list[_TxnBuilder], window_lo: datetime,
                          s4_mode: str, rebuilding: frozenset = frozenset()) -> dict:
    """S4a. Group the same entries a second time from the STORED state, and report the difference.

    What is compared is the PARTITION - which entries ended up together - not builder identity, since
    those are new objects on both sides. Compared as a set of frozensets so a different iteration order
    is not mistaken for a different grouping.

    The point of shadow mode is not "do they match" but "how, and how often, do they not". So the
    refusal reasons the guard produced are reported too: "the guard declined 900 times because the
    tenant was idle" and "declined 900 times because the clock went backwards" are the same number and
    completely different problems.

    Then the state is saved from the AUTHORITATIVE grouping, not from the seeded one. That matters: the
    stored state has to describe what was actually written, or the next window would seed from a
    grouping nobody persisted.
    """
    state = await stream_state.load(db, customer_code, window_lo)

    # DIAGNOSED 2026-08-25, on live divergence data. Only streams whose transaction this window is
    # REBUILDING may be seeded. A stream outside `rebuilding` describes a persisted, owned transaction
    # whose entries the authoritative run cannot even see - so seeding it makes the two runs group
    # different universes, and worse: each phantom open stream steals user-FIFO responses from streams
    # both runs DO see, cascading into the "one extra group, 7-8 shifted" signature the shadow logs
    # showed on every seeded run. One measured window went from 1 cold group to 17 seeded ones.
    #
    # This is not only a shadow-comparison nicety. Under mode=on, `_persist` would skip such a
    # builder's id as an out-of-order clash (the transaction exists and is not in `stored`), so an
    # out-of-scope seed measures a capability the system cannot persist. Joining a late response
    # across the pad boundary to a NOT-rebuilt transaction is S4b's genuinely new power, and it needs
    # its own `_persist` design before it can be measured honestly - excluded until then, and counted
    # in the report as `out_of_scope` so the exclusion is visible rather than silent.
    in_scope = [r for r in state["streams"] if r.transaction_id in rebuilding]
    seed = {"streams": [
        {"thread": r.thread, "user_ctx": r.user_ctx, "is_current": r.is_current,
         "open_pos": (r.open_ts_is_null, r.open_ts, r.open_source_file, r.open_line_number),
         "entries": state["entries_by_txn"].get(r.transaction_id, [])}
        for r in in_scope],
        "pending": state["pending"]}

    seeded = _group(rows, seed=seed)

    def partition(bs):
        return {frozenset(str(e.id) for e in b.entries) for b in bs}

    a, b = partition(authoritative), partition(seeded)
    agreed = a == b
    report = {"mode": s4_mode, "agreed": agreed,
              "stored_streams": state["stored_streams"], "seeded_streams": len(in_scope),
              "out_of_scope": len(state["streams"]) - len(in_scope),
              "refusals": state["refusals"],
              "groups_authoritative": len(authoritative), "groups_seeded": len(seeded)}
    if not agreed:
        # Logged at WARNING with counts rather than contents: a divergence report that dumps entry ids
        # is unreadable at the volume this runs at, and the counts are what decides whether to promote.
        report["only_authoritative"] = len(a - b)
        report["only_seeded"] = len(b - a)
        logger.warning("Stage 2 [%s]: S4 shadow DIVERGED - %d grouping(s) only in the re-derive, "
                       "%d only in the seeded run. Not promoting. %s",
                       customer_code, len(a - b), len(b - a), report)

    # Save from the authoritative grouping. `_group` leaves finished builders in its return, so an OPEN
    # stream is one whose transaction is still receiving entries - which after S3 is precisely a row
    # that is not sealed. Recomputed here rather than tracked, because the sealer is the authority on
    # what is settled and duplicating that decision is how the two drift apart.
    # KEYED, not appended, and the unique constraint is what taught me that. `_group`'s `open_by_key`
    # is a dict, so at most ONE stream per (thread, user_ctx) can be open at a time - but the list this
    # iterates holds FINISHED builders too, and a thread that flipped A -> B -> A contributes two
    # builders under the same key. Appending both violated `uq_log_open_stream_key` immediately.
    #
    # The newest wins, which is also the right answer rather than merely a way to satisfy the
    # constraint: the most recent activity on a key IS the stream a following entry would join.
    by_key: dict[tuple, dict] = {}
    for bldr in authoritative:
        if not bldr.entries:
            continue
        last = max((e.timestamp for e in bldr.entries if e.timestamp is not None), default=None)
        if last is None or (window_lo - last) >= timedelta(seconds=settings.log_open_gap_seconds):
            continue          # already past the gap, so it can never receive another entry
        anchor = bldr.entries[0]
        key = (anchor.thread, anchor.user_ctx)
        prior = by_key.get(key)
        if prior is not None and prior["last_entry_ts"] >= last:
            continue
        by_key[key] = {
            "thread": anchor.thread, "user_ctx": anchor.user_ctx,
            "transaction_id": _txn_id(bldr.entries), "has_request": any(
                e.entry_type.value == "request" for e in bldr.entries),
            "last_entry_ts": last, "open_pos": bldr.open_pos, "is_current": True}
    open_streams = list(by_key.values())
    await stream_state.save(db, customer_code, streams=open_streams, pending=[])
    report["saved_streams"] = len(open_streams)
    return report


async def regroup_window(db: AsyncSession, customer_code: str, lo: datetime, hi: datetime,
                         commit: bool = True) -> dict:
    """SCOPED rebuild for ONE customer over the time range a recent ingest touched ([lo, hi] = the
    min/max entry timestamp). Cost is bounded by that span, not the whole table.

    Pads to [lo - pad, hi + pad] (pad >= seal window). Deletes EVERY transaction — sealed or not —
    whose start falls in [lo - pad, hi], frees their entries, then regroups all unassigned entries in
    [lo - pad, hi + pad]. Deleting sealed transactions inside the window is what makes back-filling a
    file into an already-grouped/sealed region lossless (regroup_incremental can't — it never frees
    sealed rows, so late entries there get orphaned or clash-skipped).

    Lossless because the system guarantees no transaction spans more than pad: every transaction a new
    entry can belong to has its start in [lo - pad, hi], so it is deleted and fully rebuilt here, and
    every entry it owns lies in [lo - pad, hi + pad], so it is all read. Transactions outside that
    band keep their entries assigned and are excluded by the `transaction_id IS NULL` filter, so they
    are never touched. Deterministic ids mean any innocent neighbour caught in the pad rebuilds
    identically (idempotent).

    The delete + regroup run in ONE transaction (no intermediate commit): the cascaded entries.->NULL
    is visible to the same-transaction read, and readers see only the pre- or post-rebuild state, never
    a torn one. Pass commit=False to fold several windows into one outer transaction (finalize_pending
    does this so a transaction-level advisory lock can guard the whole batch)."""
    pad = _regroup_pad()
    lo_p, hi_p = lo - pad, hi + pad

    # 1. free every transaction anchored in [lo_p, hi] (sealed included).
    #    The assignment rows go with them: the FK cascades, but we delete them EXPLICITLY first so
    #    the intent is stated rather than inferred, and so the freeing is visible to the re-select in
    #    step 2 — which happens in this same transaction with no intermediate commit. This is what
    #    replaces relying on `ON DELETE SET NULL` to blank the raw rows.
    freed = list((await db.execute(
        select(LogTransaction.id).where(
            LogTransaction.customer_code == customer_code,
            LogTransaction.started_at >= lo_p,
            LogTransaction.started_at <= hi,
        )
    )).scalars().all())

    # 1b. record which transaction owned each entry, BEFORE the delete below destroys the evidence.
    #     Ordering is the correctness condition, not a preference: reading this afterwards returns an
    #     empty map, which does not fail — it silently reverts to minting a fresh id per rebuild, and
    #     the identity instability returns unnoticed. One bulk query, bounded by the same window; see
    #     `assignments.owners_in_window_stmt` for why it is keyed on entry_ts and not on `freed`.
    cont = continuity.Continuity(
        owner_by_entry=await assignments.load_owners_in_window(
            db, customer_code, time_bounds.from_instants([lo_p, hi_p], pad=timedelta(0))),
        reusable=frozenset(freed))

    # S3. The delete is GONE from the ordinary path, and this is the change the whole stage is about.
    #
    # It was never a storage decision - it was the TRIGGER. `assignments.is_unassigned()` was how a
    # rebuild found work, and only the delete made entries eligible again. So removing the delete means
    # replacing the trigger, not merely deleting a statement: eligibility is now decided in Python by
    # `_eligible` below, from the owner map that was already loaded a few lines up.
    #
    # What is read instead is the stored digest of every row about to be recomputed, so the rebuild can
    # tell "identical" from "changed" and write only the difference.
    stored = {}
    if settings.stage2_fingerprint_skip and freed:
        stored = {r.id: r for r in (await db.execute(
            select(LogTransaction.id, LogTransaction.started_at, LogTransaction.row_fingerprint,
                   LogTransaction.members_fingerprint)
            .where(LogTransaction.id.in_(freed)))).all()}
    else:
        # The pre-S3 path, kept behind the flag: delete everything and rebuild it. Byte-identical to
        # what shipped before, so turning the flag off is a real rollback rather than a different
        # third behaviour.
        await assignments.delete_for_transactions(db, freed)
        await db.execute(delete(LogTransaction).where(LogTransaction.id.in_(freed)) if freed
                         else delete(LogTransaction).where(sa_false()))

    # N1 (Phase 2), publish site 1 of 5. Inside THIS transaction, so the ticket and the change commit
    # together or neither does (invariant 3).
    #
    # Published HERE rather than at the end of the function, and that placement is the point: the rows
    # above are already gone, and the `if not rows` early return below can exit before anything is
    # rebuilt. Publishing after that return would miss exactly the case where transactions were freed
    # and NOT recreated -- the case where facts most need reversing.
    #
    # The padded window is used rather than the freed rows' own span: it is what this function already
    # guarantees is lossless, so a ticket cannot describe less than the rebuild does.
    await analytics_tickets.publish(db, customer_code, lo=lo_p, hi=hi_p)

    # 2. read the now-unassigned entries across the full padded span, in stream order. The upper read
    #    bound is hi_p (= hi + pad), not hi: a freed transaction anchored at hi can own entries up to
    #    pad later, and we must see all of them to stitch it whole.
    #    "Unassigned" is now "has no row in log_entry_assignment" rather than transaction_id IS NULL.
    #    The anti-join stays bounded by the window below — a whole-table anti-join over an
    #    append-only table would not scale.
    # S3: WITHOUT the `is_unassigned()` anti-join when the skip is on, because nothing was deleted so
    # every entry in the window still has an owner. Eligibility is decided in Python instead:
    #
    #     an entry is eligible if it has NO owner (genuinely new), or if its owner is one of the
    #     transactions this window is rebuilding (`freed`)
    #
    # Provably the same set the post-delete anti-join produced: the delete removed exactly the rows in
    # `freed`, which is exactly what made their entries ownerless. It also takes a NOT EXISTS off the
    # hot path, and both inputs were already in memory - `cont.owner_by_entry` and `freed`.
    _rebuilding = frozenset(freed)

    def _eligible(entry_id) -> bool:
        owner = cont.owner_by_entry.get(entry_id)
        return owner is None or owner in _rebuilding

    entry_stmt = select(LogEntry).where(
        LogEntry.customer_code == customer_code,
        LogEntry.timestamp >= lo_p,
        LogEntry.timestamp <= hi_p,
    )
    if not (settings.stage2_fingerprint_skip and freed):
        entry_stmt = entry_stmt.where(assignments.is_unassigned())
    rows = [e for e in (await db.execute(entry_stmt.order_by(
                LogEntry.timestamp.asc().nullslast(),
                LogEntry.source_file.asc(),
                LogEntry.line_number.asc(),
            ))).scalars().all()
            if not (settings.stage2_fingerprint_skip and freed) or _eligible(e.id)]

    stats = {"mode": "window", "customers": 1, "by_status": {},
             "window_start": lo_p.isoformat(), "window_end": hi_p.isoformat()}
    if not rows:
        # S3. Before this stage the delete happened ABOVE, so reaching here with stored rows was
        # impossible - they were already gone. Now nothing has been deleted, so a window whose entries
        # have all disappeared would leave every one of its transactions alive, pointing at nothing,
        # with the analytics diff never seeing them go.
        #
        # Caught by `test_site_1_publishes_even_when_the_rebuild_finds_nothing`, whose premise is
        # exactly this: freed and not rebuilt. The ticket was already published above for precisely
        # this case; without the delete the ticket would have described a reversal that never happened.
        if stored:
            await assignments.delete_for_transactions(db, list(stored))
            await db.execute(delete(LogTransaction).where(LogTransaction.id.in_(list(stored))))
            logger.info("Stage 2 regroup (window) %s..%s [%s]: %d transaction(s) had no entries left "
                        "and were removed", lo_p, hi_p, customer_code, len(stored))
        if commit:
            await db.commit()
        logger.info("Stage 2 regroup (window) %s..%s [%s]: no unassigned entries", lo_p, hi_p, customer_code)
        return {**stats, "customers": 0, "entries_scanned": 0,
                "transactions_created": 0, "transactions_sealed": 0}
    seal_cutoff, abandon_cutoff = await _cutoffs(db, customer_code)
    # S4. The re-derive is what actually persists, in every mode. Shadow mode additionally SEEDS a
    # second grouping from the stored stream state and compares the two, so divergence is measured on
    # real traffic before anything is promoted.
    #
    # The re-derive stays authoritative because S3 made the six known miss modes PERMANENT: nothing
    # revisits a row whose fingerprint matched, so a split that should have merged never heals. Before
    # S3 it healed on the next of 22 rebuilds, which is exactly why none has ever been observed.
    groups = _group(rows)
    s4_mode = stream_state.mode()
    if s4_mode != stream_state.OFF:
        try:
            stats["s4"] = await _shadow_compare(db, customer_code, rows, groups, lo_p, s4_mode,
                                                rebuilding=frozenset(freed))
        except Exception:
            # Swallowed on purpose, and only here. Shadow mode is a MEASUREMENT; a fault in it must
            # never fail a stitch that would otherwise have succeeded. In `on` mode this would be
            # different, which is one more reason `on` is not the default yet.
            logger.exception("Stage 2 [%s]: S4 shadow comparison failed; stitching continues",
                             customer_code)
            stats["s4"] = {"error": True}

    result = await _persist(db, groups, customer_code, seal_cutoff, abandon_cutoff,
                            cont, stored=stored)
    if commit:
        await db.commit()
    _merge_stats(stats, {**result, "entries_scanned": len(rows),
                         "orphan_entries": len(rows) - result["entries_assigned"]})
    logger.info("Stage 2 regroup (window): %s", stats)
    return stats


def _coalesce(ranges: list[tuple[datetime, datetime]], gap: timedelta) -> list[tuple[datetime, datetime]]:
    """Merge time ranges whose padded windows would overlap into disjoint runs. Two raw ranges merge
    when the later one starts within `gap` of the earlier one's end — at gap = 2*pad their padded
    [s-pad, e+pad] windows touch, so merging avoids rebuilding the shared seam twice. Sparse ranges
    (e.g. a January file and a June file) stay separate, so each is regrouped over its own narrow
    window instead of one giant span."""
    if not ranges:
        return []
    ordered = sorted(ranges)
    merged = [ordered[0]]
    for s, e in ordered[1:]:
        ps, pe = merged[-1]
        if s <= pe + gap:
            merged[-1] = (ps, max(pe, e))
        else:
            merged.append((s, e))
    return merged


def _coalesce_pending(pend: list[LogRegroupPending], gap: timedelta
                      ) -> list[tuple[datetime, datetime, list[LogRegroupPending]]]:
    """Like _coalesce, but keeps each merged run tied to the pending rows that formed it — so a run can
    be marked consumed independently once (and only if) it finishes. This is what lets one poison run
    fail in isolation without either blocking the other runs or wrongly consuming its own rows."""
    runs: list[list] = []  # [lo, hi, [rows]]
    for p in sorted(pend, key=lambda r: r.range_start):
        if runs and p.range_start <= runs[-1][1] + gap:
            runs[-1][1] = max(runs[-1][1], p.range_end)
            runs[-1][2].append(p)
        else:
            runs.append([p.range_start, p.range_end, [p]])
    return [(lo, hi, rows) for lo, hi, rows in runs]


def _split_run(lo: datetime, hi: datetime, max_seconds: int):
    """Yield consecutive sub-windows covering [lo, hi], each spanning at most max_seconds. regroup_window
    pads every sub-window by >= the seal window and rebuilds with deterministic ids, so consecutive
    sub-windows overlap at their seam and rebuild it identically — the split is lossless. max_seconds<=0
    disables splitting (one window). Steady-state runs are seconds wide and yield a single window."""
    if max_seconds <= 0 or (hi - lo).total_seconds() <= max_seconds:
        yield (lo, hi)
        return
    step = timedelta(seconds=max_seconds)
    cur = lo
    while cur < hi:
        nxt = min(cur + step, hi)
        yield (cur, nxt)
        cur = nxt


async def finalize_pending(db: AsyncSession, customer_code: str) -> dict:
    """Consume a customer's open log_regroup_pending rows: coalesce their ranges into disjoint runs and
    run a scoped regroup_window over each, then stamp that run's rows consumed. This is the single
    "I'm done ingesting — stitch what I added" operation behind the console finalize endpoint, the
    fetch/poll path, and the watcher's drain-empty flush. Idempotent: with nothing pending, windows=0.

    Isolation + bounded memory (why this is NOT one big transaction):
      - Each RUN is processed on its OWN short session and split into padded sub-windows of at most
        settings.log_regroup_max_window_seconds; each sub-window is a scoped regroup_window that
        COMMITS on its own. So the per-window entry set — and the session identity map — stays bounded
        no matter how large the backlog is, and progress persists as it goes.
      - A run that fails (any sub-window raises) is caught, logged, and recorded under `failures`; its
        pending rows are left OPEN (retried next cycle) while every OTHER run still completes. One
        poison record can therefore never again stall all stitching for the tenant (the original bug:
        a single over-length ItemNumber aborted the whole batch, and the oldest-first retry meant it
        blocked forever). A run that keeps failing is not retried forever: each failure bumps the
        pending rows' `attempts`, and once attempts reaches settings.log_regroup_max_attempts the rows
        are ABANDONED (abandoned_at set, excluded from the open query) and alerted — a dead-letter so a
        permanently-broken window (e.g. one on a dead disk block) stops burning the timeout every cycle.
      - Each sub-window acquires pg_advisory_xact_lock(hashtext(customer_code)) inside its own
        transaction, so concurrent finalizes for the same customer still serialize at window
        granularity; regroup_window is idempotent (deterministic ids), so interleaving is safe.

    The passed `db` is used only to READ the open pending rows; all writes happen on fresh sessions."""
    pend = list((await db.execute(
        select(LogRegroupPending).where(
            LogRegroupPending.customer_code == customer_code,
            LogRegroupPending.consumed_at.is_(None),
            LogRegroupPending.abandoned_at.is_(None),  # dead-lettered windows are not retried
            # Backoff gate: a window that just failed is held back until its delay elapses, so the
            # attempts are genuinely spread out instead of all being spent on consecutive ticks.
            # clock_timestamp(), NOT now(): now() is transaction_timestamp(), so in a session whose
            # transaction began before the row was written the row would look permanently not-yet-due.
            LogRegroupPending.available_at <= func.clock_timestamp(),
        ).order_by(LogRegroupPending.range_start.asc())
    )).scalars().all())
    if not pend:
        return {"mode": "finalize", "customer_code": customer_code, "windows": 0,
                "pending_consumed": 0, "abandoned": 0, "by_window": []}

    runs = _coalesce_pending(pend, gap=2 * _regroup_pad())
    max_window = settings.log_regroup_max_window_seconds
    by_window: list[dict] = []
    failures: list[dict] = []
    consumed = 0
    abandoned = 0
    lock = func.pg_advisory_xact_lock(func.hashtext(customer_code))

    for lo, hi, rows in runs:
        try:
            for w_lo, w_hi in _split_run(lo, hi, max_window):
                async with async_session() as wdb:
                    # serialize same-customer finalizes at window granularity, then rebuild + COMMIT
                    # this window in its own transaction (releasing the lock).
                    await wdb.execute(select(lock))
                    # Same as ingestion: a window rebuild reads + deletes + reinserts many entries and,
                    # on the degraded disk, legitimately exceeds the 30s web-tier statement_timeout. Relax
                    # it FOR THIS TRANSACTION to a generous but finite cap so the window can complete;
                    # SET LOCAL reverts on commit. Pair with a smaller LOG_REGROUP_MAX_WINDOW_SECONDS so
                    # each window is small enough to finish within the cap on a slow disk.
                    await wdb.execute(sa_text(
                        f"SET LOCAL statement_timeout = {int(settings.log_worker_statement_timeout_ms)}"))
                    by_window.append(await regroup_window(wdb, customer_code, w_lo, w_hi, commit=True))
            # only now that every sub-window of this run committed, mark the run's pending consumed
            async with async_session() as cdb:
                await cdb.execute(
                    update(LogRegroupPending)
                    .where(LogRegroupPending.id.in_([p.id for p in rows]))
                    .values(consumed_at=datetime.now(timezone.utc))
                )
                await cdb.commit()
            consumed += len(rows)
        except Exception as exc:
            # Isolate this run; other runs still run. Its pending rows stay OPEN for a retry — UNLESS
            # they have now failed `log_regroup_max_attempts` times, in which case they are ABANDONED
            # (dead-lettered) so a poison window can't be retried forever. The attempt bookkeeping is a
            # tiny, independently-committed write on log_regroup_pending (the window rebuild itself
            # rolled back).
            io = is_disk_io_error(exc)
            err_text = (f"disk fault: {disk_io_detail(exc)}" if io else str(exc))[:2000]
            ids = [p.id for p in rows]
            now = datetime.now(timezone.utc)
            max_attempts = settings.log_regroup_max_attempts
            # Classify with the SAME policy the ingest queue uses (retry_policy). A PERMANENT failure
            # — a builder that can never be persisted, corrupt content — will fail identically on
            # every attempt, so spending the remaining budget on it just triples the log noise and
            # re-reads a bad disk area for nothing.
            transient = retry_policy.is_transient(exc)
            newly_abandoned = 0
            async with async_session() as edb:
                await edb.execute(
                    update(LogRegroupPending).where(LogRegroupPending.id.in_(ids)).values(
                        attempts=LogRegroupPending.attempts + 1,
                        last_error=err_text, last_attempt_at=now,
                    )
                )
                if transient:
                    # Push the retry into the future so the attempts are actually spread out; a
                    # transient condition then has time to clear between them.
                    delay = retry_policy.backoff_seconds(
                        (rows[0].attempts or 0) + 1,
                        base=settings.log_regroup_backoff_base_seconds,
                        cap=settings.log_regroup_backoff_max_seconds)
                    # clock_timestamp() + interval, evaluated by the DATABASE — the same clock the
                    # open-window query compares against.
                    await edb.execute(
                        update(LogRegroupPending).where(LogRegroupPending.id.in_(ids)).values(
                            available_at=func.clock_timestamp()
                            + func.make_interval(0, 0, 0, 0, 0, 0, delay)))
                    cond = LogRegroupPending.attempts >= max_attempts
                else:
                    cond = sa_true()          # permanent: dead-letter on the first failure
                newly_abandoned = len((await edb.execute(
                    update(LogRegroupPending)
                    .where(LogRegroupPending.id.in_(ids), cond)
                    .values(abandoned_at=now)
                    .returning(LogRegroupPending.id)
                )).scalars().all())
                await edb.commit()
            abandoned += newly_abandoned

            if newly_abandoned:
                logger.critical(
                    "Stage 2 finalize [%s]: run %s..%s ABANDONED after %d failed attempts (%s) — it will "
                    "NOT be retried; investigate (likely a permanent disk fault). Ensure backups.",
                    customer_code, lo, hi, max_attempts, err_text,
                )
            elif io:
                logger.critical(
                    "Stage 2 finalize [%s]: run %s..%s hit a DISK fault (%s) — skipping, will retry.",
                    customer_code, lo, hi, disk_io_detail(exc),
                )
            else:
                logger.exception("Stage 2 finalize [%s]: run %s..%s failed — pending stays open for retry",
                                 customer_code, lo, hi)
            failures.append({"window_start": lo.isoformat(), "window_end": hi.isoformat(),
                             "error": err_text, "io_error": io, "abandoned": bool(newly_abandoned)})

    logger.info("Stage 2 finalize [%s]: %d pending rows -> %d run(s), %d window(s), %d run(s) failed, "
                "%d window(s) abandoned",
                customer_code, len(pend), len(runs), len(by_window), len(failures), abandoned)
    result = {"mode": "finalize", "customer_code": customer_code, "windows": len(by_window),
              "pending_consumed": consumed, "abandoned": abandoned, "by_window": by_window}
    if failures:
        result["failures"] = failures
    return result


async def reset_abandoned_windows(db: AsyncSession, customer_code: str) -> int:
    """Un-park (re-arm) dead-lettered stitch windows for a customer so finalize retries them again.

    For every ABANDONED, not-yet-consumed log_regroup_pending row of the customer, clears abandoned_at,
    resets attempts to 0, and clears last_error - so the next finalize picks the window up again. Use
    once the underlying cause is addressed (disk replaced, load subsided, etc.). Returns how many
    windows were re-armed. Commits on the passed session."""
    res = await db.execute(
        update(LogRegroupPending)
        .where(
            LogRegroupPending.customer_code == customer_code,
            LogRegroupPending.abandoned_at.isnot(None),
            LogRegroupPending.consumed_at.is_(None),
        )
        .values(abandoned_at=None, attempts=0, last_error=None)
    )
    await db.commit()
    n = res.rowcount or 0
    if n:
        logger.info("Stage 2: re-armed %d abandoned window(s) for %s — they will be retried next finalize",
                    n, customer_code)
    return n


async def run_finalize_tracked(run_id: uuid.UUID, customer_code: str) -> None:
    """Background entry point for the async finalize endpoint: run finalize_pending and record the
    outcome on the log_regroup_runs row so the frontend can poll it. Uses its own session — the HTTP
    request that scheduled this has already returned. Never raises (a background task has no caller to
    catch it); failures are captured as status=failed with the error text, and the pending rows stay
    open (finalize_pending rolled them back) so a later finalize retries them."""
    async with async_session() as db:
        try:
            stats = await finalize_pending(db, customer_code)
            values = dict(status=LogRegroupRunStatus.completed, windows=stats.get("windows"),
                          pending_consumed=stats.get("pending_consumed"), result=stats,
                          finished_at=datetime.now(timezone.utc))
        except Exception as exc:
            logger.exception("Tracked finalize failed (run=%s customer=%s)", run_id, customer_code)
            await db.rollback()
            values = dict(status=LogRegroupRunStatus.failed, error=str(exc),
                          finished_at=datetime.now(timezone.utc))
        await db.execute(update(LogRegroupRun).where(LogRegroupRun.id == run_id).values(**values))
        await db.commit()
