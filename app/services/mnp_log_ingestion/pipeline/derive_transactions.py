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


def _group(entries: list[LogEntry]) -> list[_TxnBuilder]:
    """REQUEST→RESPONSE state machine over time-ordered entries."""
    builders: list[_TxnBuilder] = []
    current: _TxnBuilder | None = None

    for e in entries:
        et = e.entry_type.value
        if et == "request":
            if current is not None:
                builders.append(current)  # previous had no RESPONSE -> closed as incomplete
            current = _TxnBuilder()
            current.add(e)
        elif et == "request_body":
            if current is None:
                current = _TxnBuilder()
            current.add(e)
        elif et == "response":
            if current is None:
                current = _TxnBuilder()
            current.add(e)
            builders.append(current)  # RESPONSE closes the cycle
            current = None
        else:  # mi_call / mi_result / sql / info / error
            if current is not None:
                current.add(e)
            # else: orphan internal entry (file starts mid-flow) -> left unassigned (transaction_id NULL)
    if current is not None:
        builders.append(current)
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
