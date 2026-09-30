"""Where an alert's button takes the reader in eSmart Eye.

A transaction with a request id opens the log explorer filtered by that id on its day, exactly as if
the reader had typed it into the filter: `/?date=<day>&reqid=<id>`. The explorer keeps that address
through the logspace picker a reader coming from Teams sees first. A transaction without a request
id (alerts from before the 29 Sep log format, a manual alert on an id-less line) opens its own page.
"""

from __future__ import annotations

from urllib.parse import quote, urlencode

from app.persistence.models.log_transaction import LogTransaction


def link_keys(txn: LogTransaction) -> dict:
    """The payload keys the link is built from: the request id and the tenant's day, when known."""
    keys: dict = {}
    reqid = (txn.reqid or "").strip()
    if reqid:
        keys["reqid"] = reqid
    if txn.date is not None:
        keys["date"] = txn.date.isoformat()
    return keys


def transaction_url(base: str | None, payload: dict) -> str | None:
    """The page to open for this payload, or None when there is no address or nothing to open."""
    root = (base or "").rstrip("/")
    if not root:
        return None
    reqid, day = payload.get("reqid"), payload.get("date")
    if reqid and day:
        # matches the explorer's URL filters: src/lib/logsApi.ts filtersFromParams
        return f"{root}/?{urlencode({'date': day, 'reqid': reqid}, quote_via=quote)}"
    txn_id = payload.get("transaction_id")
    if txn_id:
        # matches the matrix-log-explorer App Router route: src/app/transactions/[id]/page.tsx
        return f"{root}/transactions/{quote(str(txn_id), safe='')}"
    return None
