"""The feed's filter as one shared rule (chunk 132).

`GET /logs/transactions/view` shows the records a filter selects, and the logspace agent answers
from those same records. Both build their WHERE clauses here, so "what I see" and "what the agent
reads" cannot drift apart. A missing filter means today, on the customer's clock.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_type, datetime

from sqlalchemy import func

from app.persistence.models.log_transaction import LogTransaction, LogTransactionStatus
from app.services.mnp_log_ingestion.pipeline import time_bounds
from app.services.mnp_log_ingestion.timefmt import _zone


@dataclass(frozen=True)
class FeedScope:
    """One day of one logspace's transactions, narrowed by the feed's optional filters."""
    day: date_type
    user: str | None = None
    hour: int | None = None
    status: LogTransactionStatus | None = None
    order_number: str | None = None
    item_number: str | None = None
    reqid: str | None = None
    #: the person fetched with this filter; False = nothing fetched, so today by default
    explicit: bool = False


# the frontend's ViewFilters keys (camelCase) and the API's query names both map onto one field
_TEXT_KEYS = {"user": "user", "order_number": "order_number", "orderNumber": "order_number",
              "item_number": "item_number", "itemNumber": "item_number", "reqid": "reqid"}


def local_today(tz_name: str) -> date_type:
    return datetime.now(_zone(tz_name)).date()


def _text(value) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def scope_from(filters: dict | None, today: date_type) -> FeedScope:
    """The scope a feed filter names; values that are blank or not valid are dropped, never guessed."""
    f = filters or {}
    day = today
    try:
        if f.get("date"):
            day = date_type.fromisoformat(str(f["date"]).strip())
    except ValueError:
        pass
    hour = None
    try:
        h = int(str(f.get("hour")).strip()) if f.get("hour") not in (None, "") else None
        hour = h if h is not None and 0 <= h <= 23 else None
    except ValueError:
        hour = None
    try:
        status = LogTransactionStatus(f["status"]) if f.get("status") else None
    except ValueError:
        status = None
    texts: dict[str, str] = {}
    for key, field in _TEXT_KEYS.items():
        value = _text(f.get(key))
        if value and field not in texts:
            texts[field] = value
    explicit = bool(f.get("date")) or hour is not None or status is not None or bool(texts)
    return FeedScope(day=day, hour=hour, status=status, explicit=explicit, **texts)


def day_conditions(customer: str, day: date_type, tz_name: str) -> list:
    """The WHERE conditions selecting one customer-LOCAL day of transactions.

    `LogTransaction.date` is that local day and is what the feed is specified in terms of, but
    `log_transactions` is partitioned on `started_at` (a UTC instant), and PostgreSQL cannot derive
    one from the other — `date` alone prunes nothing and opens all 60 partitions. So the equivalent
    UTC window goes in beside it.

    The window is padded well beyond any real timezone offset, which makes it a strict SUPERSET of
    the instants that can carry this local date. That is deliberate: `date` was computed with
    whatever display zone the customer had when the row was WRITTEN, so a later timezone change would
    otherwise slide the two apart and blank out the day view. `date` stays the exact filter; the
    window only prunes.
    """
    conds = [LogTransaction.customer_code == customer, LogTransaction.date == day]
    window = time_bounds.from_local_dates(day, day, tz_name)
    if window is not None:
        # include_null=False is safe rather than lossy: `date` is derived FROM `started_at`, so a row
        # with a NULL `started_at` also has a NULL `date` and never matches the equality above.
        conds.append(window.covers(LogTransaction.started_at, include_null=False))
    return conds


def scope_conditions(customer: str, scope: FeedScope, tz_name: str) -> list:
    """The feed's WHERE clauses for this scope, exactly as `/transactions/view` applies them."""
    conds = day_conditions(customer, scope.day, tz_name)
    if scope.user is not None:
        conds.append(LogTransaction.user_name == scope.user)
    if scope.hour is not None:
        # a LOCAL hour of day (matching the displayed times); started_at is a UTC instant
        conds.append(func.extract("hour", func.timezone(tz_name, LogTransaction.started_at)) == scope.hour)
    if scope.status is not None:
        conds.append(LogTransaction.status == scope.status)
    if scope.order_number is not None:
        conds.append(LogTransaction.order_number == scope.order_number)
    if scope.item_number is not None:
        conds.append(LogTransaction.item_number == scope.item_number)
    if scope.reqid is not None:
        conds.append(LogTransaction.reqid == scope.reqid)
    return conds


def describe(scope: FeedScope, *, today: date_type) -> str:
    """The records in words: "today (2026-09-30)" or "2026-09-29 · user PEVANS · status error"."""
    parts = [f"today ({scope.day.isoformat()})" if scope.day == today and not scope.explicit
             else scope.day.isoformat()]
    if scope.user:
        parts.append(f"user {scope.user}")
    if scope.hour is not None:
        parts.append(f"hour {scope.hour:02d}:00")
    if scope.status is not None:
        parts.append(f"status {scope.status.value}")
    if scope.order_number:
        parts.append(f"order {scope.order_number}")
    if scope.item_number:
        parts.append(f"item {scope.item_number}")
    if scope.reqid:
        parts.append(f"request {scope.reqid}")
    return " · ".join(parts)
