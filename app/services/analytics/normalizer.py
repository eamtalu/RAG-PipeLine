"""N2, the fact normaliser: one `log_transactions` row to one typed fact row, or a quarantine record.

Phase 3 of docs/analytics-ml-architecture/final_architecture.md.

**Pure: no database, no clock, no configuration.** The plan calls this "where correctness is won, which
is why the module has no I/O", and the purity is what makes that true rather than aspirational: every
rule here can be asserted without a database and reasoned about without a pipeline, and the output is
reproducible, which the ledger and the fingerprint both depend on. The tenant timezone therefore arrives
as a PARAMETER; the worker looks it up and passes it in.

Two properties carry more weight than the rest of the module.

**The fingerprint decides whether anything is written at all.** 98.7% of transactions are rewritten
after their first write, so almost every recheck must be absorbed as a no-op by a matching fingerprint
(invariant 6). That forces one non-obvious exclusion: `log_transactions.created_at` is refreshed on
EVERY rebuild by construction, since Stage 2 deletes and re-inserts rather than updating. Fingerprinting
it would make every hash differ every cycle, so nothing would ever be absorbed and the rebuild rate
would become a write rate. Nothing would visibly break; the system would simply churn.

**Absent is never zero, and a placeholder is not a rejection.** A quantity method whose quantity cannot
be read is quarantined, never folded in as a zero-unit attempt -- zero means "the operator picked
nothing", a real and separately counted event. Conversely a placeholder `transaction_type` invalidates
only that DIMENSION: all 83 live `AddStockCountLine` rows carry a real `CountedQuantity` under one, so
rejecting the row would silently drop real stock counts.

Where each field comes from
---------------------------
Most fact fields are columns on `log_transactions`. Three are not, and are read from `attributes`
because no column exists for them: `lot_number` (`LotNumber`), `from_location` (`FromLocation`) and
`to_location` (`ToLocation`). Key names verified against the live server.

`item_number` deliberately does NOT fall back to the `ItemNumber` attribute. Measured: the column is set
on exactly the rows carrying the attribute, with zero disagreements, so a fallback would add a second
source of truth for no gain.
"""

import hashlib
import json
import logging
from functools import lru_cache
from datetime import date as date_type, datetime, timezone
from decimal import Decimal
from typing import Any, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import Enum as SAEnum, String, inspect as sa_inspect

from app.persistence.models.analytics_fact import AnalyticsFact
from app.services.analytics import contract as c

logger = logging.getLogger(__name__)

#: `attributes` keys promoted to fact columns, because `log_transactions` has no column for them.
_FROM_ATTRIBUTES: dict[str, str] = {
    "lot_number": "LotNumber",
    "from_location": "FromLocation",
    "to_location": "ToLocation",
}

#: Fact fields NOT part of the fingerprint, and why each is excluded. Named explicitly rather than
#: derived, because a field silently drifting into the hash is how invariant 6 stops holding.
_NOT_FINGERPRINTED: frozenset[str] = frozenset({
    # The hash cannot contain itself.
    "source_version_hash",
    # Bookkeeping assigned by N3 from what is already stored, not a property of the source row.
    # Including it would make a fact's second version differ from its first by definition, so no
    # recheck could ever be a no-op.
    "revision",
})

#: The value a fact is given on a FIRST write. N3 owns the real number -- it is a function of what is
#: already stored, which a pure module cannot see -- so this is the initial value and nothing more.
_INITIAL_REVISION = 1


def _as_text(value: Any) -> str | None:
    """A column value as text, unwrapping the enums Stage 2 stores.

    `log_transactions.status` is a Python enum, so a raw `str()` would yield
    `LogTransactionStatus.success` and put the class name in both the fact row and the fingerprint.
    """
    if value is None:
        return None
    if hasattr(value, "value"):
        return str(value.value)
    text = str(value).strip()
    return text or None


def _local_date(event_time: datetime | None, tenant_timezone: str | None) -> date_type | None:
    """The tenant-LOCAL calendar day of `event_time`.

    Distinct from the UTC day, and the difference is not academic: 23:30 UTC on 5 August is 00:30 on the
    6th in London, so an operator asking for "the 6th" means a different set of rows than a UTC day
    would give.

    An unusable timezone falls back to UTC rather than failing the row. Being wrong by at most an hour
    for one tenant beats refusing to fold anything for them at all, and the fallback is visible because
    it changes the fingerprint.
    """
    if event_time is None:
        return None
    zone: Any = timezone.utc
    if tenant_timezone:
        try:
            zone = ZoneInfo(tenant_timezone)
        except (ZoneInfoNotFoundError, ValueError):
            zone = timezone.utc
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return event_time.astimezone(zone).date()


def _canonical(value: Any) -> Any:
    """A value in a form that hashes identically across two reads of the same unchanged row."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, Decimal):
        # `10` and `10.0` are the same quantity and must hash the same. `ExpectedQuantity` arrives as
        # `30.000000` while `QuantityPicked` arrives as `30.0`, so this is the live case, not a
        # hypothetical one.
        return f"{value.normalize():f}"
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date_type):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _canonical(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    return str(value)


def _fingerprint(fact: Mapping[str, Any]) -> str:
    """A stable hash over every field of the fact that could affect a measure.

    Everything except the exclusions above, INCLUDING the `attributes` blob. Fingerprinting the blob is
    deliberate: it is retained precisely so a measure nobody has thought of yet can be built from it
    later, which makes a change inside it a change to a potential measure. Nothing in it is volatile --
    it is derived deterministically from the log entries -- so this costs no spurious rewrites.

    `business_date` is included, which also makes a tenant's timezone part of the hash. That is the
    point: the zone decides which day a fact rolls up into, so a zone change with an unchanged
    fingerprint would leave every rollup on the old day with nothing to trigger a recompute.
    """
    payload = {k: _canonical(v) for k, v in sorted(fact.items()) if k not in _NOT_FINGERPRINTED}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _quarantine(row: Mapping[str, Any], reason: str, detail: str, observed: dict) -> dict:
    """A quality issue, shaped for `analytics_quality_issues`.

    Carries BOTH identity columns (F3) and whatever was actually seen, because the raw entry is dropped
    at 60 days: a quarantine row recording only that something went wrong is useless a year later, which
    is exactly when someone asks why two totals disagree.
    """
    return {
        "source_transaction_id": row.get("id"),
        "source_started_at": row.get("started_at"),
        "reason": reason,
        "detail": detail,
        "observed": observed,
    }


@lru_cache(maxsize=1)
def _fact_str_limits() -> dict[str, int]:
    """{field: max_length} for every bounded VARCHAR column on `AnalyticsFact`.

    A PORT of `derive_transactions._txn_str_limits`, and the fact that it had to be ported is the point.
    That function exists because one over-length `ItemNumber` once stalled all stitching for a tenant -
    there is a postmortem, `docs/stage2-stitching-stall-postmortem-and-fix.md`. This module was written
    afterwards, promotes the same kind of value out of the same JSONB, and had no equivalent: 49
    analytics windows were dead-lettered over two days in production with

        StringDataRightTruncationError: value too long for type character varying(32)

    Measured at the time: `ToLocation` reached 37 characters against a 32-wide `to_location`, and
    `LotNumber` the same against `lot_number`.

    Read from the ORM MAPPING rather than written as a literal, so a resized or newly added column is
    picked up for free. A hardcoded list is how the same bug returns after the next migration.

    `Enum` is excluded because its values are fixed and valid, and `Text` has no limit to enforce. This
    is ORM introspection over the Python mapping, not a query, so the module's no-database property is
    intact - nothing here opens a session or emits SQL.
    """
    return {col.key: col.type.length
            for col in sa_inspect(AnalyticsFact).columns
            if isinstance(col.type, String) and not isinstance(col.type, SAEnum)
            and getattr(col.type, "length", None)}


def _cap_over_length(fact: dict) -> dict:
    """Trim promoted string dimensions to their column width, in place.

    CAPPED rather than quarantined, deliberately. These are DIMENSIONS, not measures: quarantining would
    discard a real transaction's quantity because a label was long, and the full value is still on the
    raw log entry - only the queryable column is trimmed. The same trade-off Stage 2 already made for the
    same reason.

    Runs BEFORE the fingerprint, so the hash describes what is actually STORED. That is for coherence
    rather than for the diff: the diff recomputes through this same function, so the order could not
    produce a mismatch either way. What the diff needs is DETERMINISM, which capping preserves.
    """
    for field, limit in _fact_str_limits().items():
        value = fact.get(field)
        if isinstance(value, str) and len(value) > limit:
            logger.warning("Analytics: capped over-length %s (%d > %d) to fit its column; the full "
                           "value remains on the raw log entry", field, len(value), limit)
            fact[field] = value[:limit]
    return fact


def normalise(row: Mapping[str, Any], *, tenant_timezone: str | None,
              response_attributes: Mapping[str, Any] | None = None
              ) -> tuple[dict | None, dict | None]:
    """One transaction row as `(fact, None)` or `(None, quality_issue)`.

    Returns a record rather than raising, always. A1: quarantine must never halt a tenant, because
    halting freezes every metric until a human intervenes and the row that halts it is by definition the
    one nobody understands yet.

    `row` is a mapping of `log_transactions` columns plus `attributes`; `tenant_timezone` is an IANA name
    or None for UTC.

    R3: `response_attributes` are the APPROVED, already-namespaced response scalars for this
    transaction (`resp.*`, `mi.*`). A parameter rather than something read here, so this module keeps
    its no-database property - N3 does the reading and the approving, and hands the result in.

    Merged rather than replaced, and merged UNDER the request keys so a namespaced response field can
    never displace one. It cannot collide in practice, because everything here is prefixed and nothing
    from the request is; the ordering makes that a guarantee rather than an observation.
    """
    attributes = dict(row.get("attributes") or {})
    if response_attributes:
        attributes.update(response_attributes)
    #: May legitimately be None, and such a row is a fact like any other. 25 of 397 live transactions
    #: (6.3%) have no method: real stitched activity with 2 to 28 entries, durations up to 172 seconds,
    #: `mi_program_count = 0` on 24 of them, and `status = incomplete` on 9. Quarantining them would hide
    #: 6.3% of transactions from every volume, duration and status metric while the totals still looked
    #: plausible. The quantity allow-list already handles it correctly -- None is not on the list, so the
    #: row is `non_quantity`, which is a decision rather than a gap.
    method = _as_text(row.get("method"))

    quantity = None
    if c.carries_quantity(method):
        field = c.quantity_field(method)
        quantity = c.parse_quantity(attributes.get(field))
        if quantity is None:
            # The most dangerous defaulting available, and the reason this branch exists rather than a
            # `or 0`. Zero is a real business event; absent is an unanswered question, and once the raw
            # entry is dropped at 60 days there is nothing left to recompute from.
            return None, _quarantine(
                row, "unusable_quantity",
                f"{method} must carry a readable {field}, but it is absent or unparsable",
                {"method": method, "field": field, "raw": attributes.get(field, None),
                 "present": field in attributes})

    event_time = row.get("started_at")
    transaction_type = _as_text(row.get("transaction_type"))

    fact: dict[str, Any] = {
        # identity and lineage
        "source_transaction_id": row.get("id"),
        "source_started_at": event_time,
        "revision": _INITIAL_REVISION,
        # time
        "event_time": event_time,
        "business_date": _local_date(event_time, tenant_timezone),
        "duration_ms": row.get("duration_ms"),
        # operation
        "method": method,
        "transaction_name": _as_text(row.get("transaction_name")),
        # A placeholder is dropped as a DIMENSION and nothing more -- keeping it would split one item's
        # total across `002001` and `xxxxxx`, and rejecting the row would drop a real stock count.
        "transaction_type": transaction_type if c.usable_dimension_value(transaction_type) else None,
        "status": _as_text(row.get("status")),
        # subject
        "item_number": _as_text(row.get("item_number")),
        "order_number": _as_text(row.get("order_number")),
        "delivery_number": _as_text(row.get("delivery_number")),
        # place
        "warehouse": _as_text(row.get("warehouse")),
        "warehouse_id": _as_text(row.get("warehouse_id")),
        # actor
        "user_name": _as_text(row.get("user_name")),
        "device_id": _as_text(row.get("device_id")),
        "device_name": _as_text(row.get("device_name")),
        # measures
        "quantity": quantity,
        "quantity_classification": c.classify(method, quantity).value,
        # the long tail, kept whole
        "attributes": attributes,
    }
    for field, key in _FROM_ATTRIBUTES.items():
        fact[field] = _as_text(attributes.get(key))

    # BEFORE the fingerprint, so the hash describes what is actually stored. See `_cap_over_length`.
    _cap_over_length(fact)
    fact["source_version_hash"] = _fingerprint(fact)
    return fact, None
