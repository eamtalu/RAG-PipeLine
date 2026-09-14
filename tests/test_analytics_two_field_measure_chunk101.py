"""Chunk 101: a measure made from two fields, so "what should have happened minus what did" is a setting.

The most valuable number in a warehouse is a difference, and until now no definition could express one.
Measured on tmp-live, both halves sit side by side on the same record:

| record          | what happened      | what was expected  |
|-----------------|--------------------|--------------------|
| ConfirmPickLine | QuantityPicked, 166 distinct values | ExpectedQuantity, 121 |
| ReportCount     | CountedQuantity, 133               | BalanceQuantity, 189  |

So pick shortfall and count variance need no new data at all, only the ability to subtract. Chunk 99
made both halves reportable; this makes the subtraction expressible.

**Deliberately one operation, not an expression language.** `minus` and nothing else. Every case
measured is "expected against actual", a general expression surface would need parsing, precedence and
its own validation, and none of that is bought by the data in front of us. A second operation can be
added the day something needs it.

**Absent is still never zero.** A row missing EITHER half contributes nothing. Treating a missing
expected quantity as zero would turn every such pick into a full shortfall, which is the exact
"no data reads as a number" failure this station keeps refusing.
"""

from decimal import Decimal

import pytest

from app.services.analytics import definition as d
from app.services.analytics import registry

APPROVED = frozenset({"ExpectedQuantity", "QuantityPicked", "BalanceQuantity", "CountedQuantity"})


def _shortfall(**kw):
    """Picked minus expected: negative when a picker came up short, which is the whole point."""
    return d.MetricDefinition(
        name="pick-shortfall", dimensions=("user_name",),
        measures=(d.Measure(name="shortfall", aggregation=kw.pop("aggregation", d.Aggregation.sum),
                            field=kw.pop("field", "attr:QuantityPicked"),
                            minus=kw.pop("minus", "attr:ExpectedQuantity"), unit="units"),),
        grains=("hourly", "daily"), method_filter=("ConfirmPickLine",), **kw)


def _row(picked=None, expected=None, **kw):
    attributes = {}
    if picked is not None:
        attributes["QuantityPicked"] = picked
    if expected is not None:
        attributes["ExpectedQuantity"] = expected
    return {"method": "ConfirmPickLine", "transaction_name": "Pick", "status": "success",
            "quantity_classification": "pick", "attributes": attributes, **kw}


# ==================================================================== 1. the arithmetic

def test_a_measure_sums_the_difference_between_two_fields():
    out = d.fold([_row(picked="8", expected="10"), _row(picked="5", expected="5")], _shortfall())
    assert out["shortfall"][d.Role.sum_value] == Decimal("-2")
    assert out["shortfall"][d.Role.count_value] == 2


def test_a_shortfall_stays_negative_rather_than_being_clamped():
    """A negative total is the answer, not an error. Clamping at zero would hide every short pick."""
    out = d.fold([_row(picked="1", expected="10")], _shortfall())
    assert out["shortfall"][d.Role.sum_value] == Decimal("-9")


def test_an_over_pick_is_positive():
    out = d.fold([_row(picked="12", expected="10")], _shortfall())
    assert out["shortfall"][d.Role.sum_value] == Decimal("2")


@pytest.mark.parametrize("row", [_row(picked="8"), _row(expected="10"), _row(),
                                 _row(picked="8", expected=""), _row(picked="x", expected="10")])
def test_a_row_missing_either_half_contributes_nothing(row):
    """Absent is never zero, applied to both halves. A missing expected quantity read as zero would
    report every such pick as a complete shortfall."""
    out = d.fold([row], _shortfall())
    assert out["shortfall"][d.Role.count_value] == 0
    assert out["shortfall"][d.Role.sum_value] == Decimal(0)


def test_the_values_are_exact_decimals_not_floats():
    """Quantities are fractional and get summed over a month; 0.1 + 0.2 must not drift."""
    out = d.fold([_row(picked="0.3", expected="0.1")], _shortfall())
    assert out["shortfall"][d.Role.sum_value] == Decimal("0.2")


def test_a_difference_works_with_every_aggregation_that_takes_a_number():
    """The subtraction happens before the aggregation, so it composes with all of them rather than
    being a special kind of sum."""
    rows = [_row(picked="8", expected="10"), _row(picked="12", expected="10")]
    extent = d.fold(rows, _shortfall(aggregation=d.Aggregation.extent))
    assert (extent["shortfall"][d.Role.min_value], extent["shortfall"][d.Role.max_value]) \
        == (Decimal("-2"), Decimal("2"))
    stats = d.fold(rows, _shortfall(aggregation=d.Aggregation.stats))
    assert stats["shortfall"][d.Role.sum_sq] == Decimal("8"), "minus two squared plus two squared"


def test_a_plain_measure_is_untouched_by_the_new_field():
    """Every existing definition has no `minus`, and must fold exactly as before."""
    plain = d.MetricDefinition(
        name="picked", dimensions=("user_name",),
        measures=(d.Measure("units", d.Aggregation.sum, field="attr:QuantityPicked"),),
        grains=("hourly",), method_filter=("ConfirmPickLine",))
    out = d.fold([_row(picked="8", expected="10")], plain)
    assert out["units"][d.Role.sum_value] == Decimal("8")


# ==================================================================== 2. what is refused

def test_a_difference_needs_a_field_to_subtract_from():
    bad = _shortfall(field=None)
    assert any("names no field" in p for p in d.validate(bad, known_attributes=APPROVED))


def test_a_count_cannot_subtract_because_it_reads_no_value():
    bad = _shortfall(aggregation=d.Aggregation.count, field=None)
    problems = d.validate(bad, known_attributes=APPROVED)
    assert any("count" in p and "subtract" in p for p in problems), problems


def test_a_distinct_count_cannot_subtract_because_its_field_is_an_identity():
    """`distinct` counts different values of a name or a number, and subtracting two identities is
    meaningless rather than merely useless."""
    bad = _shortfall(aggregation=d.Aggregation.distinct)
    problems = d.validate(bad, known_attributes=APPROVED)
    assert any("distinct" in p and "subtract" in p for p in problems), problems


def test_the_subtracted_field_is_validated_exactly_like_the_first():
    """Fails closed. An unapproved or misspelled second field would make every row contribute
    nothing, which reads as "no shortfall" - the most dangerous possible wrong answer here."""
    problems = d.validate(_shortfall(minus="attr:NoSuchField"), known_attributes=APPROVED)
    assert any("NoSuchField" in p and "not approved" in p for p in problems), problems
    problems = d.validate(_shortfall(minus="nonesuch_column"), known_attributes=APPROVED)
    assert any("nonesuch_column" in p for p in problems), problems


def test_a_valid_difference_has_no_problems_at_all():
    assert d.validate(_shortfall(), known_attributes=APPROVED) == []


def test_a_typed_column_can_be_subtracted_too():
    """`quantity` against an expected attribute, which is what a picking metric would use where the
    quantity has already been promoted to a column."""
    definition = d.MetricDefinition(
        name="x", dimensions=("user_name",),
        measures=(d.Measure("shortfall", d.Aggregation.sum, field="quantity",
                            minus="attr:ExpectedQuantity", unit="units"),),
        grains=("hourly",), method_filter=("ConfirmPickLine",))
    assert d.validate(definition, known_attributes=APPROVED) == []
    out = d.fold([_row(expected="10", quantity=Decimal("8"))], definition)
    assert out["shortfall"][d.Role.sum_value] == Decimal("-2")


# ==================================================================== 3. it survives storage

def test_a_difference_round_trips_through_the_stored_form():
    original = _shortfall()
    row = registry.to_row(original, customer_code="test")
    assert row["measures"][0]["minus"] == "attr:ExpectedQuantity"

    class _Row:
        def __init__(self, r):
            self.name, self.dimensions, self.measures = r["name"], r["dimensions"], r["measures"]
            self.grains, self.filter, self.status = r["grains"], r["filter"], r["status"]
            self.source, self.rollups_from = r["source"], r["rollups_from"]

    assert registry.from_row(_Row(row)) == original


def test_a_measure_without_a_difference_stores_no_key_for_it():
    """Older stored forms are unchanged, exactly as chunk 89 did for `unit`."""
    plain = d.Measure("units", d.Aggregation.sum, field="quantity")
    assert "minus" not in registry.measure_to_json(plain)
    assert registry.measure_from_json(registry.measure_to_json(plain)) == plain
