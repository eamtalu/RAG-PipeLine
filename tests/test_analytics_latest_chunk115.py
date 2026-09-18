"""Chunk 115: the value as it stands, not the values added up.

Every aggregation so far answers a question about a SET of rows: their total, their spread, how many
there were. A level needs a different question - what does it say NOW - and nothing could ask it.

The case that prompted it, measured on tmp-live on 16 September 2026. A picker works a delivery line
in several goes; 178 of 3,278 lines were picked in more than one, and the worst has nine rows.
Delivery 25810, item 104526, line 4 reads like this in time order:

    04:09:05  expected  9   picked  5
    04:09:07  expected  9   picked  0     the operator went and the location was empty
    04:58:14  expected  9   picked  0
    04:58:24  expected  6   picked  0     the expectation was revised
    05:07:24  expected 15   picked  0     and revised again
    05:39:32  expected 15   picked  0
    05:55:43  expected 15   picked  0
    06:05:28  expected 15   picked  5
    06:06:20  expected 15   picked 10

Picked totals 20, which is right: those units moved. Expected totals 108 for a line that expects 15,
because the expectation is stamped on every row and revised as the work goes on. Across the tenant,
summing it gives 26,447 where taking each line's expectation once gives 22,778, so a shortfall built
by subtracting the sums reads -3,729 where the truth is about -60.

`latest` keeps the reading with the greatest event_time. It composes: merging two hours takes the
later hour's, merging a month of days takes the last day's, and that is exact rather than
approximate. It is the honest way to read any level, which is also why "how much stock is on the
shelf now" was unanswerable before this.

**Its one limit, stated rather than hidden.** A latest is one value per GROUP. Read at a grouping
coarser than the thing it belongs to, it gives one member's value, not a total: the latest expected
across a warehouse is whichever line was touched last. The role is therefore non-additive, and every
reader blanks it on a parent row exactly as it already blanks a median.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.services.analytics import definition as d

T0 = datetime(2026, 9, 16, 4, 0, tzinfo=timezone.utc)
APPROVED = frozenset({"ExpectedQuantity", "QuantityPicked", "DeliveryNumber"})
KINDS = {"ExpectedQuantity": "level", "QuantityPicked": "measure", "DeliveryNumber": "slice"}


def _metric(aggregation=d.Aggregation.latest, field="attr:ExpectedQuantity", minus=None):
    return d.MetricDefinition(
        name="expected", dimensions=("delivery_number",),
        measures=(d.Measure(name="expected", aggregation=aggregation, field=field, minus=minus,
                            unit="units"),),
        grains=("hourly", "daily"))


def _row(expected, minutes, picked=None):
    attributes = {"ExpectedQuantity": str(expected)}
    if picked is not None:
        attributes["QuantityPicked"] = str(picked)
    return {"method": "ConfirmPickLine", "transaction_name": "Pick", "status": "success",
            "quantity_classification": "pick", "event_time": T0 + timedelta(minutes=minutes),
            "attributes": attributes}


#: The nine rows above, in the order they happened.
LINE = [_row(9, 0, 5), _row(9, 0, 0), _row(9, 49, 0), _row(6, 49, 0), _row(15, 58, 0),
        _row(15, 90, 0), _row(15, 106, 0), _row(15, 116, 5), _row(15, 117, 10)]


# ==================================================== 1. what it answers

def test_it_keeps_the_reading_from_the_last_event():
    """The line expects 15. Summing its rows says 108, and 108 is a number nothing ever expected."""
    out = d.fold(LINE, _metric())
    assert out["expected"][d.Role.latest]["value"] == Decimal("15")


def test_a_later_reading_replaces_an_earlier_one_whatever_its_size():
    """Not the biggest, the last. The expectation went 9, then 6, then 15; a maximum would have said
    15 here by luck and 9 on a line that was revised downwards."""
    out = d.fold([_row(15, 0), _row(6, 10)], _metric())
    assert out["expected"][d.Role.latest]["value"] == Decimal("6")


def test_rows_arriving_out_of_order_do_not_change_the_answer():
    """A fold reads whatever the query returns. Ordering is the row's own event_time, never arrival."""
    assert (d.fold(LINE, _metric())["expected"][d.Role.latest]
            == d.fold(list(reversed(LINE)), _metric())["expected"][d.Role.latest])


def test_it_counts_the_readings_beside_the_value():
    """The honest denominator, as `distinct` and `percentile` already carry. Nine readings, one answer."""
    assert d.fold(LINE, _metric())["expected"][d.Role.count_value] == 9


def test_a_row_carrying_no_value_is_not_a_reading():
    """Absent is never zero, and it is never the latest either. A call that did not state an
    expectation must not erase the one that did."""
    rows = [_row(15, 0), {**_row(0, 10), "attributes": {}}]
    assert d.fold(rows, _metric())["expected"][d.Role.latest]["value"] == Decimal("15")


# ==================================================== 2. it composes

def test_merging_two_hours_takes_the_later_hour():
    """The property that makes it storable at all. A month is folded from its days, so every
    aggregation has to survive being merged, and a latest survives by comparing instants."""
    early = d.fold([_row(9, 0)], _metric())
    late = d.fold([_row(15, 120)], _metric())
    merged = d.add_roles(early["expected"], late["expected"])
    assert merged[d.Role.latest]["value"] == Decimal("15")
    assert merged[d.Role.count_value] == 2


def test_merging_is_the_same_whichever_side_is_given_first():
    """`add_roles` is called in both orders by the fold and the read, so it has to commute."""
    early = d.fold([_row(9, 0)], _metric())["expected"]
    late = d.fold([_row(15, 120)], _metric())["expected"]
    assert d.add_roles(early, late)[d.Role.latest] == d.add_roles(late, early)[d.Role.latest]


def test_merging_an_empty_bucket_keeps_the_reading():
    empty = d.empty(_metric())["expected"]
    one = d.fold([_row(15, 0)], _metric())["expected"]
    assert d.add_roles(empty, one)[d.Role.latest]["value"] == Decimal("15")
    assert d.add_roles(one, empty)[d.Role.latest]["value"] == Decimal("15")


def test_the_value_and_its_instant_move_together():
    """Stored as ONE role rather than two, so `add_roles` keeps its uniformity: it never asks which
    measure it is looking at. Two separate roles could be merged apart, pairing one hour's value with
    another hour's clock."""
    assert d.roles_for(d.Aggregation.latest) == frozenset({d.Role.latest, d.Role.count_value})
    merged = d.add_roles(d.fold([_row(9, 0)], _metric())["expected"],
                         d.fold([_row(15, 120)], _metric())["expected"])
    assert merged[d.Role.latest]["at"] == (T0 + timedelta(minutes=120)).isoformat()


# ==================================================== 3. how it reaches a reader

def test_it_leaves_the_service_as_a_value_and_an_instant():
    """A reader needs the number and needs to know how stale it is. The raw pair never leaves."""
    public = d.public_roles(d.fold(LINE, _metric())["expected"])
    assert public["latest"] == "15"
    assert public["latest_at"] == (T0 + timedelta(minutes=117)).isoformat()


def test_an_empty_bucket_publishes_no_latest_at_all():
    """Rather than a null that reads as "the latest was nothing"."""
    assert "latest" not in d.public_roles(d.empty(_metric())["expected"])


def test_it_is_not_additive_and_says_so():
    """One value per group. Read at a grouping coarser than the thing it belongs to it gives one
    member's value, not a total, so every reader must blank it on a parent row."""
    assert d.Role.latest in d.NON_ADDITIVE_ROLES


# ==================================================== 4. what may be read this way

def test_a_level_may_be_read_at_its_latest():
    """The whole point. Chunk 109 refused to add a level up and left nothing that answered
    "what does it say now"."""
    assert d.validate(_metric(), known_attributes=APPROVED, field_kinds=KINDS) == []


def test_an_ordinary_quantity_may_be_read_at_its_latest_too():
    """The last quantity picked on a line is a real question, and refusing it would be arbitrary."""
    assert d.validate(_metric(field="attr:QuantityPicked"),
                      known_attributes=APPROVED, field_kinds=KINDS) == []


def test_the_latest_of_a_name_is_refused():
    """It reads the value as a number, exactly as every other aggregation does, so a name would be
    skipped on every row and the answer would be silently absent."""
    problems = d.validate(_metric(field="attr:DeliveryNumber"),
                          known_attributes=APPROVED, field_kinds=KINDS)
    assert any("NAME" in p for p in problems)


def test_the_latest_difference_of_two_levels_is_allowed():
    """The shortfall as it stands, which is the question the chunk was asked for."""
    kinds = {**KINDS, "QuantityPicked": "level"}
    assert d.validate(_metric(field="attr:QuantityPicked", minus="attr:ExpectedQuantity"),
                      known_attributes=APPROVED, field_kinds=kinds) == []


def test_it_needs_a_field():
    """`count` is the only aggregation that reads no value."""
    problems = d.validate(_metric(field=None), known_attributes=APPROVED, field_kinds=KINDS)
    assert any("names no field" in p for p in problems)
