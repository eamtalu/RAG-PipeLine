"""Chunk 37, Phase 0 of docs/analytics-ml-architecture/final_architecture.md: the consumption contract.

Phase 0 builds no tables. It pins what "consumption" MEANS, because the fact row's width is the one
irreversible decision in the plan: raw data is dropped at 60 days, so a measure invented next year can
only be backfilled across history if its fields were already being written today.

Everything here is PURE. No database, no clock, no configuration. That is the same discipline N2 will
be held to, and it is why these rules can be pinned before any of the machinery exists.

Three things this file exists to prevent, all of which produce a plausible wrong number rather than an
error:

1. **Treating absent as zero.** A missing quantity is not a pick of nothing, it is an unanswered
   question. Folding it in as 0 inflates `attempt_count` and drags the zero-pick rate.
2. **Reading a quantity off a method that does not have one.** Only three of 49 methods carry one.
   Trusting the presence of a JSONB key instead of an allow-list picks up parser leakage.
3. **Comparing quantities as strings.** `ExpectedQuantity` arrives as `30.000000` while
   `QuantityPicked` arrives as `30.0`. Both are thirty.

Values below are measured from the live server on 2026-08-21, not assumed.
"""

from decimal import Decimal

import pytest

from app.services.analytics import contract as c


# ==================================================== which methods carry a quantity
def test_exactly_three_methods_carry_a_quantity_and_each_names_its_own_field():
    """Measured: ConfirmPickLine 16,075 rows all with QuantityPicked; ReportCount 11,343 all with
    CountedQuantity; AddStockCountLine 83 all with CountedQuantity. The plan says "only 2 of 49";
    AddStockCountLine is the third and is called out in the ground-truth section as a trap, because it
    carries a real quantity under a PLACEHOLDER transaction_type."""
    assert c.QUANTITY_FIELD == {
        "ConfirmPickLine": "QuantityPicked",
        "ReportCount": "CountedQuantity",
        "AddStockCountLine": "CountedQuantity",
    }


def test_a_listing_method_is_not_a_quantity_method_even_when_the_key_is_present():
    """3 of 7,307 `ListItemAlternateUnitsOfMeasure` rows carry a CountedQuantity (values 3.415, 18, 0,
    all under transaction_type `xxxxxx`). A listing call has no business reporting a counted quantity,
    so this is parser leakage. The allow-list is what rejects it; trusting `attributes ? key` would
    silently fold three phantom stock counts into the totals."""
    assert not c.carries_quantity("ListItemAlternateUnitsOfMeasure")
    assert c.quantity_field("ListItemAlternateUnitsOfMeasure") is None
    for m in c.QUANTITY_FIELD:
        assert c.carries_quantity(m)


# ==================================================== parsing a quantity
@pytest.mark.parametrize("raw,expected", [
    ("30.0", Decimal("30")),          # QuantityPicked shape
    ("30.000000", Decimal("30")),     # ExpectedQuantity shape
    ("30", Decimal("30")),            # ExpectedQuantity also arrives bare
    ("0.333333", Decimal("0.333333")),
    ("2.666664", Decimal("2.666664")),
    ("0.0", Decimal("0")),
    ("-1", Decimal("-1")),
])
def test_quantities_parse_to_exact_decimals(raw, expected):
    assert c.parse_quantity(raw) == expected


def test_the_three_spellings_of_thirty_are_all_equal():
    """The plan's warning, as an assertion. A string comparison of ExpectedQuantity against
    QuantityPicked would report every full pick as a short pick."""
    assert c.parse_quantity("30") == c.parse_quantity("30.0") == c.parse_quantity("30.000000")


def test_quantities_are_decimal_not_float():
    """Quantities are fractional (0.333333, 2.666664 are real values), so float would accumulate error
    across a month of sums and the total would drift with no error raised."""
    assert isinstance(c.parse_quantity("0.1"), Decimal)
    total = sum((c.parse_quantity("0.1") for _ in range(10)), Decimal(0))
    assert total == Decimal("1.0"), "float arithmetic would give 0.9999999999999999"


@pytest.mark.parametrize("raw", ["", "   ", None, "abc", "1.2.3", "N/A"])
def test_absent_or_unreadable_is_none_and_never_zero(raw):
    """The single most dangerous defaulting in this design. Zero means "the operator picked nothing",
    which is a real and separately counted business event; absent means "we do not know". Collapsing
    the second into the first is unrecoverable once the raw data is dropped at 60 days."""
    assert c.parse_quantity(raw) is None


# ==================================================== classification
def test_a_positive_quantity_is_a_pick():
    assert c.classify("ConfirmPickLine", Decimal("3")) is c.Classification.pick


def test_zero_is_an_attempt_not_a_pick():
    """F8. Measured 1,333 of 16,075 ConfirmPickLine rows (8.3%) record zero units and still report
    success, typically an empty location. Counting those as picks understates the zero-pick rate to
    nothing, and that rate is a first-class metric because it names specific empty locations."""
    assert c.classify("ConfirmPickLine", Decimal("0")) is c.Classification.attempt


def test_a_negative_quantity_is_a_correction_not_a_pick():
    """One negative CountedQuantity exists on the live server. `pick_count` is defined as confirmations
    ABOVE zero, so a reversal is neither a pick nor a zero-unit attempt. Left as its own class rather
    than folded into either, because folding it into picks would let a correction inflate the count of
    successful picks while decreasing the quantity."""
    assert c.classify("ReportCount", Decimal("-2")) is c.Classification.correction


def test_a_method_with_no_quantity_is_classified_as_such_not_as_zero():
    """47 of 49 methods. Their meaningful measures are volume, duration, status and actor, so they must
    not appear in a quantity denominator at all."""
    assert c.classify("ListPickLines", None) is c.Classification.non_quantity
    assert c.classify("ListPickLines", Decimal("5")) is c.Classification.non_quantity, \
        "the allow-list wins over whatever happens to be in the JSONB"


def test_a_quantity_method_with_an_absent_quantity_is_unusable_not_zero():
    """Currently zero occurrences (all 16,075 ConfirmPickLine rows carry the field), which is exactly
    when to pin it: this is the quarantine path, and quarantine must never halt a tenant."""
    assert c.classify("ConfirmPickLine", None) is c.Classification.unusable


# ==================================================== placeholder transaction types
def test_every_placeholder_transaction_type_on_the_live_server_is_rejected():
    """Measured set: 0050XX, 00xxxx, XXXXX, xxxxxx, XXXXXX. The plan lists four and misses XXXXXX,
    which is why this is a PATTERN and not a literal list: a list has already been wrong once."""
    for t in ("0050XX", "00xxxx", "XXXXX", "xxxxxx", "XXXXXX"):
        assert c.is_placeholder_type(t), t


def test_a_real_transaction_type_is_not_rejected():
    for t in ("0050", "0100", "", None):
        assert not c.is_placeholder_type(t), t


def test_a_placeholder_type_does_not_invalidate_the_quantity():
    """The trap the ground-truth section calls out. All 83 AddStockCountLine rows carry a real
    CountedQuantity under a placeholder transaction_type, so rejecting the ROW because its type is a
    placeholder would silently drop real stock counts. The placeholder makes `transaction_type`
    unusable as a DIMENSION, nothing more."""
    assert c.classify("AddStockCountLine", Decimal("7")) is c.Classification.pick
    assert not c.usable_dimension_value("0050XX")
    assert c.usable_dimension_value("0050")


# ==================================================== the irreversible decision
def test_the_fact_row_field_list_covers_every_group_the_plan_names():
    """The one decision that cannot be revisited: anything not written here is unrecoverable after 60
    days. Asserted as a whole so adding a measure later is a deliberate edit to a pinned list rather
    than an accident of whatever the normaliser happened to extract."""
    for field in ("source_transaction_id", "source_started_at", "source_version_hash", "revision",
                  "event_time", "business_date", "duration_ms",
                  "method", "transaction_name", "transaction_type", "status",
                  "item_number", "lot_number", "order_number", "delivery_number",
                  "warehouse", "warehouse_id", "from_location", "to_location",
                  "user_name", "device_id", "device_name",
                  "quantity", "quantity_classification"):
        assert field in c.FACT_FIELDS, f"{field} missing from the fact row"


def test_the_contract_exposes_no_metric_specific_names():
    """The doc is explicit: "nothing about dimensions or measures may be hardcoded". A metric's
    dimensions, measures and counters belong to a REGISTRY ROW, not to this module. Constants named
    COUNTERS or DIMENSIONS here would make the consumption metric the only one the system can have.

    What stays is only what is invariant about the SOURCE DATA and about the fact row's shape, both of
    which are genuinely fixed: the fact row is pinned because anything missing from it is unrecoverable
    after 60 days.
    """
    for leaked in ("COUNTERS", "DIMENSIONS", "fold", "add", "zero_pick_rate", "empty"):
        assert not hasattr(c, leaked), (
            f"{leaked} is definition-level and belongs in analytics.definition, not the contract")
