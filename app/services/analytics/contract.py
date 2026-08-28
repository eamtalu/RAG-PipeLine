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

#: 18y: every plain field an `analytics_record_facts` row carries. The record grain's counterpart of
#: `FACT_FIELDS`, and deliberately much shorter: a record's real payload lives in its `rec.*`
#: attributes (addressed as `attr:rec.STQT`), and the plain columns are only the parent's identity
#: and the M3 call that answered. No `status`, no `quantity_classification` - records carry neither,
#: which is why `validate` refuses those filters on record metrics.
RECORD_FIELDS: tuple[str, ...] = (
    "source_transaction_id", "source_started_at", "record_index",
    "event_time", "business_date",
    "method", "transaction_name", "mi_program", "mi_transaction",
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


# ============================================================== R1b: attribute-backed fields
#: The prefix that says "this name is a key inside `attributes`, not a typed fact column".
#:
#: A prefix rather than a bare name for two reasons. A bare `resp.BaseUoM` could not be told apart
#: from a typed column that happened to contain a dot, so the resolver would have to guess. And in a
#: stored definition the two read paths are then distinguishable at a glance, which matters when
#: someone is reading a registry row months later trying to work out why a chart is empty.
ATTR_PREFIX = "attr:"


def is_attr_path(name: str) -> bool:
    """Whether `name` addresses a key inside `attributes` rather than a typed column.

    Checked on the full prefix INCLUDING the colon, so a legitimate column called `attr_like` is not
    mistaken for a path. The colon cannot appear in a Python identifier, so no column name can collide
    with this by accident.
    """
    return name.startswith(ATTR_PREFIX)


def attr_key(name: str) -> str:
    """The key inside `attributes` that `name` addresses. Only meaningful for an `attr:` path."""
    return name[len(ATTR_PREFIX):]


def resolve_field(row, name: str):
    """One field of a fact row, whether it is a typed column or a key inside `attributes`.

    THE single resolution point for both read paths - `definition.fold` for measures and
    `rollups._dim_key` for dimensions. Written once here because those two resolving a name differently
    is how the same value ends up in two rollup buckets, and one item's total silently halves.

    A missing key is `None`, never an exception. A response that happens not to carry a field on one
    call must not fail the whole window's fold; that is the existing "absent is never zero" rule
    applied to a nested value.
    """
    if is_attr_path(name):
        attributes = row.get("attributes")
        if not isinstance(attributes, dict):
            return None            # a fact assembled in memory may carry no `attributes` key at all
        return attributes.get(attr_key(name))
    return row.get(name)


def numeric_or_none(raw) -> Decimal | None:
    """`raw` as a Decimal, or None when it is not a number.

    This exists because of a real difference between the two field kinds, not for symmetry. A typed
    measure field is already numeric - `quantity` is a Decimal, `duration_ms` an int. A value out of
    JSONB is whatever the WMS logged, and the live M3 records carry `"STQT": "624"`: a STRING. Without
    coercion the first such row raises `TypeError` inside the fold, inside the worker's transaction,
    rolling back a whole window because one field was quoted.

    A value that cannot be coerced returns None, and `fold` then skips the row under the same "absent
    is never zero" rule it already applies to a NULL quantity. Skipping rather than counting zero is
    the load-bearing half: a denominator drawn from rows that contributed no value makes every rate
    wrong in a way that looks entirely plausible.

    `bool` is rejected explicitly. It is a subclass of `int` in Python, so a naive coercion would fold
    a flag in as 1 and no test would notice until someone questioned a total.
    """
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, Decimal):
        return raw
    if isinstance(raw, int):
        return Decimal(raw)
    if isinstance(raw, float):
        return Decimal(str(raw))       # via str, so 0.1 does not become 0.1000000000000000055511151
    if isinstance(raw, str):
        try:
            return Decimal(raw.strip())
        except (InvalidOperation, ValueError):
            return None
    return None


def dimension_value(raw) -> str | None:
    """`raw` as a dimension value: the ONE normalisation both read paths must share.

    Decision C (section 18e) turns on this. Promotion copies rather than moves - `_FROM_ATTRIBUTES`
    puts `attributes` on the fact and only then reads out of it - so for a while a value is reachable
    both as `attr:resp.BaseUoM` and as a promoted `base_uom` column. If those two normalised
    differently by so much as trimming or case, the same base UoM would land in two rollup rows and one
    item's total would split in half, with both halves looking plausible.

    Empty and whitespace-only collapse to None rather than to `""`. A JSONB field is frequently an
    empty string where a typed column would be NULL, and two spellings of "no value" would split a
    bucket exactly as two spellings of a real value would.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None
