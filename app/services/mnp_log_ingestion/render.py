# render.py — Canonical "Transaction Detail View" (design doc §6) text renderer.
#
#   Turns a derived log_transaction + its ordered log_entries into the locked, human-readable
#   boundary view from docs/transaction-log-ingestion-design.md §6:
#
#       TRANSACTION 5f53035f-06e8-4855-80d3-b3c2ad1fcdea   /api/receiving/ListOpenPOHead   ✅ SUCCESS
#       user BECWHLO · 2026-06-13 · 10:38:53.935 → 10:38:54.818 · 0.88 s · 56 steps
#
#         ▶ REQUEST   WHLO BRI · Route BRI05 · user BECWHLO
#          1  🔄 PPS200MI/SearchHead      CONO=911 SUNO=…           ✅ 14 recs
#          2  ⚙  Stored Procedure          usp_GetReceivingLocation  ✅
#          …
#          3  🛑 ERROR  "Printer Error Code = 1801"
#         ◀ RESPONSE ✅  → [{"PoNumber":"1000092",…}]
#
#   Every element is reconstructable from the two tables (see §6.1 field mapping). The pipeline
#   already produces clean boundaries; this is purely the presentation layer the endpoints/agent
#   hand back instead of (or alongside) raw JSON.

from __future__ import annotations

from urllib.parse import urlsplit

from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.mnp_log_ingestion.timefmt import to_display

_STATUS_ICON = {
    LogTransactionStatus.success: "✅ SUCCESS",
    LogTransactionStatus.soft: "⚠️ SOFT",
    LogTransactionStatus.error: "🛑 ERROR",
    LogTransactionStatus.incomplete: "⏳ INCOMPLETE",
}


def render_transaction(txn: LogTransaction, entries: list[LogEntry], *, verbose: bool = False) -> str:
    """Render the §6 Transaction Detail View as text.

    verbose=False (default): show the signal — MI calls (with their results folded in), SQL,
    soft results, and real errors. Plain INFO narration is collapsed.
    verbose=True: also render every INFO step.
    """
    lines: list[str] = []
    lines.append(_header(txn))
    lines.append(_subheader(txn, entries))
    lines.append("")

    req = next((e for e in entries if e.entry_type.value in ("request", "request_body")), None)
    lines.append("  " + _request_line(txn, req))

    for n, step in enumerate(_steps(entries, verbose=verbose), start=1):
        lines.append(f"  {n:>3}  {step}")

    resp = next((e for e in entries if e.entry_type.value == "response"), None)
    lines.append("  " + _response_line(txn, resp))
    return "\n".join(lines)


# --------------------------------------------------------------------------- header
def _header(t: LogTransaction) -> str:
    status = _STATUS_ICON.get(t.status, str(t.status))
    return f"TRANSACTION {t.id}   {_endpoint_path(t)}   {status}"


def _subheader(t: LogTransaction, entries: list[LogEntry]) -> str:
    bits: list[str] = []
    if t.user_name:
        bits.append(f"user {t.user_name}")
    if t.started_at:
        bits.append(_date(t.started_at))
    if t.started_at and t.ended_at:
        bits.append(f"{_hms(t.started_at)} → {_hms(t.ended_at)}")
    elif t.started_at:
        bits.append(_hms(t.started_at))
    if t.duration_ms is not None:
        bits.append(_dur(t.duration_ms))
    bits.append(f"{len(entries)} steps")
    if t.transaction_name:
        bits.append(f'"{t.transaction_name}"')
    return " · ".join(bits)


def _request_line(t: LogTransaction, req: LogEntry | None) -> str:
    label = "▶ REQUEST BODY" if (req and req.entry_type.value == "request_body") else "▶ REQUEST"
    dims: list[str] = []
    if t.warehouse:
        dims.append(f"WHLO {t.warehouse}")
    if t.route:
        dims.append(f"Route {t.route}")
    if t.delivery_number:
        dims.append(f"Delivery {t.delivery_number}")
    if t.order_number:
        dims.append(f"Order {t.order_number}")
    if t.item_number:
        dims.append(f"Item {t.item_number}")
    if t.user_name:
        dims.append(f"user {t.user_name}")
    summary = " · ".join(dims) or (t.request_summary or "")
    return f"{label}   {summary}".rstrip()


def _response_line(t: LogTransaction, resp: LogEntry | None) -> str:
    icon = "✅" if t.status in (LogTransactionStatus.success, LogTransactionStatus.soft) else (
        "🛑" if t.status == LogTransactionStatus.error else "…")
    body = t.response_summary
    if not body and resp and resp.message:
        body = resp.message.replace("RESPONSE:", "").strip()
    if not body and t.status == LogTransactionStatus.incomplete:
        return "◀ (no RESPONSE ingested yet — incomplete)"
    body = (body or "").strip()
    if len(body) > 500:
        body = body[:500] + "…"
    return f"◀ RESPONSE {icon}  → {body}".rstrip()


# --------------------------------------------------------------------------- steps
def _steps(entries: list[LogEntry], *, verbose: bool) -> list[str]:
    """Fold each mi_call with its matching mi_result into one step; render SQL/error/soft/info."""
    steps: list[str] = []
    pending: LogEntry | None = None  # an mi_call awaiting its result

    def flush_pending() -> None:
        nonlocal pending
        if pending is not None:
            steps.append(_mi_step(pending, None))
            pending = None

    for e in entries:
        et = e.entry_type.value
        if et in ("request", "request_body", "response"):
            continue
        if et == "mi_call":
            flush_pending()
            pending = e
        elif et == "mi_result":
            if pending is not None and pending.mi_program == e.mi_program and pending.mi_transaction == e.mi_transaction:
                steps.append(_mi_step(pending, e))
                pending = None
            else:
                flush_pending()
                steps.append(_mi_step(None, e))  # standalone result (soft/error result without a paired call)
        elif et == "error":
            flush_pending()
            steps.append(f"🛑 ERROR  \"{(e.result_status or e.message or '').strip()}\"")
        elif et == "sql":
            flush_pending()
            steps.append(f"⚙  SQL  {_sql_name(e)}")
        elif et == "info":
            if verbose:
                flush_pending()
                steps.append(f"⚙  {(e.message or '').strip()}")
            # else: collapse plain narration
    flush_pending()
    return steps


def _mi_step(call: LogEntry | None, result: LogEntry | None) -> str:
    src = call or result
    prog = (src.mi_program or "") if src else ""
    txn = (src.mi_transaction or "") if src else ""
    head = f"🔄 {prog}/{txn}".rstrip("/")
    inputs = _inputs(call) if call else ""
    outcome = _result_outcome(result)
    return "  ".join(p for p in (head, inputs, outcome) if p)


def _inputs(call: LogEntry) -> str:
    """All params used to make the MI call (the 'Inputs:' block, else the URL query params)."""
    f = call.fields or {}
    src = f.get("inputs") if isinstance(f.get("inputs"), dict) else f.get("params")
    if not isinstance(src, dict):
        return ""
    pairs = [f"{k}={v}" for k, v in src.items() if v not in (None, "") and str(k).lower() != "m3user"]
    return " ".join(pairs)


def _result_outcome(result: LogEntry | None) -> str:
    if result is None:
        return ""
    status = (result.result_status or "").strip()
    if status == "OK":
        return f"✅ {result.record_count} recs" if result.record_count is not None else "✅ OK"
    if status:
        return f'⚠️ SOFT  "{status}"'
    return "✅"


# --------------------------------------------------------------------------- helpers
def _endpoint_path(t: LogTransaction) -> str:
    if t.endpoint_url:
        path = urlsplit(t.endpoint_url).path
        if path and path != "/":
            return path
    return f"/{t.method}" if t.method else "(unknown endpoint)"


def _sql_name(e: LogEntry) -> str:
    proc = (e.fields or {}).get("stored_procedure", "") if isinstance(e.fields, dict) else ""
    first = (proc or e.message or "").strip().splitlines()[0] if (proc or e.message) else ""
    return first[:60]


# started_at/ended_at are stored as UTC instants; show them in the display zone (UK) to match the log.
def _date(dt) -> str:
    dt = to_display(dt)
    return dt.strftime("%Y-%m-%d")


def _hms(dt) -> str:
    dt = to_display(dt)
    return dt.strftime("%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"


def _dur(ms: int) -> str:
    return f"{ms / 1000:.2f} s" if ms >= 1000 else f"{ms} ms"
