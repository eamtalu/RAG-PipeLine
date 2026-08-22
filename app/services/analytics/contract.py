"""What "consumption" means. Phase 0 of docs/analytics-ml-architecture/final_architecture.md.

This module is the contract every later component is measured against, and it is deliberately the
first thing built. The fact row's width is the one irreversible decision in the plan: raw data is
dropped at 60 days, so a measure invented next year can only be backfilled across history if its
fields were already being written today. Pinning the fields and the classification rules before any
schema exists is what makes that decision a deliberate one.

**Pure: no database, no clock, no configuration.** Same discipline N2 (the normaliser) is held to,
which is what lets these rules be asserted without a database and reasoned about without a pipeline.

Three rules here each prevent a plausible wrong number rather than an error:

*Absent is never zero.* A missing quantity is not a pick of nothing, it is an unanswered question.
Folding it in as 0 inflates the confirmation count and drags the zero-pick rate toward zero, and once
the raw entry is dropped at 60 days there is nothing left to recompute from.

*A quantity is read only from an allow-listed method.* Three of 49 carry one. Trusting the presence of
a JSONB key instead picks up parser leakage: measured, 3 of 7,307 `ListItemAlternateUnitsOfMeasure`
rows carry a stray `CountedQuantity`, and a listing call has no business reporting a stock count.

*Quantities are Decimal and compared numerically.* `ExpectedQuantity` arrives as `30.000000` while
`QuantityPicked` arrives as `30.0`; both are thirty. Values are genuinely fractional (`0.333333`,
`2.666664` are real), so float would drift across a month of sums with nothing reporting it.

Every constant below is measured from the live server on 2026-08-21, and the measurements are recorded
beside them so a future reader can tell a fact from an assumption.

What is deliberately NOT here
-----------------------------
Any metric's counters, dimensions or folding. The doc is explicit that "nothing about dimensions or
measures may be hardcoded", so those belong to a registry row and live in `analytics.definition`. This
module holds only what is invariant about the SOURCE DATA and about the fact row's shape - both of
which genuinely are fixed, the second because anything missing from it is unrecoverable after 60 days.
An earlier draft of this file exported `COUNTERS`, `DIMENSIONS` and a hardcoded `fold`, which would
have made consumption the only metric the system could ever have.
"""

import enum
import re
from decimal import Decimal, InvalidOperation

#: method -> the `attributes` key holding its quantity. This mapping IS the allow-list: a method absent
#: from it carries no quantity, whatever its JSONB happens to contain.
#:
#: Measured 2026-08-21, all rows of each method carrying the named key:
#:   ConfirmPickLine    16,075 / 16,075  QuantityPicked   (and ExpectedQuantity, which is NOT a measure)
#:   ReportCount        11,343 / 11,343  CountedQuantity
#:   AddStockCountLine       83 /     83  CountedQuantity
#:
#: The plan's ground truth says "only 2 of 49 methods carry quantities". AddStockCountLine is the
#: third; the plan names it one paragraph earlier as the trap where a real quantity sits under a
#: PLACEHOLDER transaction_type, so it was measured but not counted. Three, not two.
QUANTITY_FIELD: dict[str, str] = {
    "ConfirmPickLine": "QuantityPicked",
    "ReportCount": "CountedQuantity",
    "AddStockCountLine": "CountedQuantity",
}

#: A `transaction_type` the WMS filled with placeholder characters. A PATTERN rather than a literal
#: list because the literal list has already been wrong once: the plan names `xxxxxx`, `XXXXX`,
#: `00xxxx` and `0050XX`, and the live server also has `XXXXXX`. Empirically every value containing an
#: x or X on this data is a placeholder, and no legitimate type does.
_PLACEHOLDER_TYPE = re.compile(r"[xX]")

#: Every field the fact row carries. Anything not here is unrecoverable after 60 days, so this list is
#: the irreversible decision and is asserted as a whole by the tests: adding a measure later must be a
#: deliberate edit here, not an accident of whatever the normaliser extracted.
FACT_FIELDS: tuple[str, ...] = (
    # identity and lineage
    "source_transaction_id", "source_started_at", "source_version_hash", "revision",
    # time
    "event_time", "business_date", "duration_ms",
    # operation
    "method", "transaction_name", "transaction_type", "status",
    # subject
    "item_number", "lot_number", "order_number", "delivery_number",
    # place
    "warehouse", "warehouse_id", "from_location", "to_location",
    # actor
    "user_name", "device_id", "device_name",
    # measures
    "quantity", "quantity_classification",
)


class Classification(str, enum.Enum):
    """What one row's quantity means. Five outcomes, because collapsing any two loses a real event."""

    pick = "pick"                  # quantity above zero: units actually moved
    attempt = "attempt"            # exactly zero: the operator tried and the location was empty
    correction = "correction"      # below zero: a reversal or adjustment
    non_quantity = "non_quantity"  # the method carries no quantity at all (47 of 49)
    unusable = "unusable"          # a quantity method whose quantity is absent -> quarantine


def quantity_field(method: str | None) -> str | None:
    """The `attributes` key holding `method`'s quantity, or None if it carries none."""
    return QUANTITY_FIELD.get(method or "")


def carries_quantity(method: str | None) -> bool:
    return quantity_field(method) is not None


def parse_quantity(raw) -> Decimal | None:
    """A raw attribute value as an exact Decimal, or None when it is absent or unreadable.

    None rather than zero, always. Zero is a real and separately counted business event ("picked
    nothing"); absent means the question was never answered. The caller decides what to do about it,
    and the only correct options are to quarantine or to skip, never to sum.

    Decimal rather than float because quantities are fractional and get summed over a month.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def classify(method: str | None, quantity: Decimal | None) -> Classification:
    """What this row contributes.

    The allow-list is consulted FIRST and wins: a method that carries no quantity is `non_quantity`
    even when a value was passed, because that value is parser leakage rather than a measurement.
    """
    if not carries_quantity(method):
        return Classification.non_quantity
    if quantity is None:
        return Classification.unusable
    if quantity > 0:
        return Classification.pick
    if quantity == 0:
        return Classification.attempt
    return Classification.correction


def is_placeholder_type(transaction_type: str | None) -> bool:
    """Whether `transaction_type` is a WMS placeholder rather than a real code."""
    if not transaction_type:
        return False
    return bool(_PLACEHOLDER_TYPE.search(transaction_type))


def usable_dimension_value(transaction_type: str | None) -> bool:
    """Whether `transaction_type` can be grouped on.

    A placeholder makes the DIMENSION unusable and nothing more. It does not invalidate the row or its
    quantity: all 83 AddStockCountLine rows carry a real CountedQuantity under a placeholder type, so
    rejecting the row would silently drop real stock counts.
    """
    return bool(transaction_type) and not is_placeholder_type(transaction_type)
