"""Chunk 116: settling many call rows into one row per key.

Every metric so far is folded straight from the call rows, and one class of question cannot be
answered from them. A pick-list release is confirmed in several calls and the expected quantity is
stamped on every one, so adding it across calls gives a number nothing ever expected.

Measured on tmp-live on 18 September 2026, over 6,292 ConfirmPickLine calls and 6,160 releases:

- the reporting number is M3's identity for one pick-list line release. It never spans two lines and
  never changes across a picker's several goes; it changes only when M3 re-releases a short line.
- `ExpectedQuantity` is the amount still to pick at that moment. It only falls within a release, and
  the first call's value is the maximum on 6,160 of 6,160 releases.
- most repeated calls on one release are FAILURES. Release 540551: one success of 9, then eight
  attempts to confirm the last unit, every one refused by M3, each still carrying `QuantityPicked = 1`.
  Summed without a status filter that release picked 17. Nine units moved.
- shortfall, expected summed every call: -5,576. One row per release, successful picks only: -4,344.

A SETTLEMENT is a named rule for turning the rows that share a key into one row. Nothing about
picking is coded into it; picking is the first thing it is used for. The module is pure and has no
database, exactly as `definition.py` and `lookup.py` do not.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.services.analytics import settle as st

T0 = datetime(2026, 9, 18, 5, 44, 42, tzinfo=timezone.utc)


def _call(minutes, expected, picked, status="success", lot="2609161191", **over):
    """One ConfirmPickLine row as the fold sees it."""
    row = {
        "method": "ConfirmPickLine", "transaction_name": "JIT and Shorts Pick (Brighton)",
        "status": status,
        "quantity_classification": "pick" if Decimal(str(picked)) > 0 else "attempt",
        "event_time": T0 + timedelta(minutes=minutes),
        "warehouse": "BRI", "delivery_number": "27907", "item_number": "104568",
        "lot_number": lot, "user_name": "FNACHONLEO",
        "attributes": {"ReportingNumber": "540551", "ExpectedQuantity": str(expected),
                       "QuantityPicked": str(picked), "OrderLine": "21", "PickListSuffix": "3"},
    }
    row.update(over)
    return row


#: Release 540551, exactly as it happened. One success, eight refusals.
RELEASE_540551 = [_call(0, 10, 9)] + [_call(7 + i, 1, 1, status="error") for i in range(8)]

PICK_RELEASE = st.Settlement(
    name="pick_release",
    reads=("ConfirmPickLine",),
    key=("attr:ReportingNumber",),
    carry=("delivery_number", "item_number", "attr:OrderLine", "attr:PickListSuffix",
           "warehouse", "transaction_name", "user_name", "lot_number"),
    values=(
        st.Settled("expected", st.Rule.first, field="attr:ExpectedQuantity"),
        st.Settled("picked", st.Rule.sum, field="attr:QuantityPicked", statuses=frozenset({"success"})),
        st.Settled("calls", st.Rule.count),
        st.Settled("empty_visits", st.Rule.count, only=frozenset({"attempt"})),
        st.Settled("refused", st.Rule.count, statuses=frozenset({"error"})),
        st.Settled("lots", st.Rule.distinct_count, field="lot_number", statuses=frozenset({"success"})),
        st.Settled("shortfall", st.Rule.difference, left="picked", right="expected"),
        st.Settled("is_short", st.Rule.flag, left="shortfall", op="<", right_value=Decimal(0)),
        st.Settled("first_at", st.Rule.min, field="event_time"),
        st.Settled("last_at", st.Rule.max, field="event_time"),
    ),
)


# ==================================================== 1. the worked example

def test_nine_calls_become_one_row():
    rows = st.settle(RELEASE_540551, PICK_RELEASE)
    assert list(rows) == [("540551",)]


def test_expected_is_the_first_call_not_the_sum():
    """The release expected 10. Summed across its nine rows the field reads 18."""
    row = st.settle(RELEASE_540551, PICK_RELEASE)[("540551",)]
    assert row.values["expected"] == Decimal("10")


def test_picked_counts_only_what_m3_accepted():
    """Eight refused calls each carry a picked quantity of 1. Nine units moved, not seventeen."""
    row = st.settle(RELEASE_540551, PICK_RELEASE)[("540551",)]
    assert row.values["picked"] == Decimal("9")


def test_the_failures_are_a_number_in_their_own_right():
    row = st.settle(RELEASE_540551, PICK_RELEASE)[("540551",)]
    assert row.values["calls"] == 9
    assert row.values["refused"] == 8
    assert row.values["empty_visits"] == 0


def test_the_shortfall_is_a_difference_of_two_settled_values():
    """Computed on the settled row, never on the calls. 9 minus 10, not 17 minus 18."""
    row = st.settle(RELEASE_540551, PICK_RELEASE)[("540551",)]
    assert row.values["shortfall"] == Decimal("-1")
    assert row.values["is_short"] == 1


def test_the_row_carries_what_it_will_be_grouped_by():
    """Copied once, so a settled row has the shape of a fact row and every existing group-by works."""
    row = st.settle(RELEASE_540551, PICK_RELEASE)[("540551",)]
    assert row.carried["delivery_number"] == "27907"
    assert row.carried["item_number"] == "104568"
    assert row.carried["OrderLine"] == "21"
    assert row.carried["warehouse"] == "BRI"
    assert row.carried["lot_number"] == "2609161191"


def test_the_row_is_placed_in_time_at_its_first_call():
    row = st.settle(RELEASE_540551, PICK_RELEASE)[("540551",)]
    assert row.values["first_at"] == T0
    assert row.values["last_at"] == T0 + timedelta(minutes=14)
    assert row.event_time == T0


# ==================================================== 2. the rules, one at a time

def _one(rule, rows, **kw):
    s = st.Settlement(name="t", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",), carry=(),
                      values=(st.Settled("v", rule, **kw),))
    return st.settle(rows, s)[("540551",)].values["v"]


def test_first_and_last_order_by_event_time_not_arrival():
    """A fold reads whatever the query returns, and a rebuild hands rows back in any order."""
    rows = [_call(10, 1, 0), _call(0, 10, 9), _call(5, 5, 0)]
    assert _one(st.Rule.first, rows, field="attr:ExpectedQuantity") == Decimal("10")
    assert _one(st.Rule.last, rows, field="attr:ExpectedQuantity") == Decimal("1")


def test_first_skips_a_call_that_carries_no_value():
    """Absent is never zero and never the first either. A call that did not state an expectation
    must not erase the one that did."""
    blank = _call(0, 10, 9)
    blank["attributes"] = {"ReportingNumber": "540551", "QuantityPicked": "9"}
    assert _one(st.Rule.first, [blank, _call(1, 7, 0)], field="attr:ExpectedQuantity") == Decimal("7")


def test_sum_honours_a_status_filter():
    assert _one(st.Rule.sum, RELEASE_540551, field="attr:QuantityPicked") == Decimal("17")
    assert _one(st.Rule.sum, RELEASE_540551, field="attr:QuantityPicked",
                statuses=frozenset({"success"})) == Decimal("9")


def test_count_honours_a_classification_filter():
    rows = [_call(0, 10, 9), _call(1, 1, 0), _call(2, 1, 0)]
    assert _one(st.Rule.count, rows) == 3
    assert _one(st.Rule.count, rows, only=frozenset({"attempt"})) == 2


def test_min_and_max_read_the_value_or_the_time():
    rows = [_call(0, 10, 9), _call(5, 6, 0), _call(9, 15, 0)]
    assert _one(st.Rule.min, rows, field="attr:ExpectedQuantity") == Decimal("6")
    assert _one(st.Rule.max, rows, field="attr:ExpectedQuantity") == Decimal("15")
    assert _one(st.Rule.max, rows, field="event_time") == T0 + timedelta(minutes=9)


def test_distinct_count_counts_different_values_and_ignores_blanks():
    """175 lines on the live tenant were picked from several lots, and 911 releases have picks with
    no lot at all. A blank is not a lot."""
    rows = [_call(0, 10, 4, lot="A"), _call(1, 10, 3, lot="B"), _call(2, 10, 2, lot="A"), _call(3, 10, 1, lot="")]
    assert _one(st.Rule.distinct_count, rows, field="lot_number") == 2


def test_difference_and_flag_read_settled_values_not_calls():
    s = st.Settlement(name="t", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",), carry=(),
                      values=(st.Settled("a", st.Rule.first, field="attr:ExpectedQuantity"),
                              st.Settled("b", st.Rule.sum, field="attr:QuantityPicked",
                                         statuses=frozenset({"success"})),
                              st.Settled("d", st.Rule.difference, left="b", right="a"),
                              st.Settled("f", st.Rule.flag, left="d", op="<", right_value=Decimal(0)),
                              st.Settled("g", st.Rule.flag, left="d", op=">=", right_value=Decimal(0))))
    v = st.settle(RELEASE_540551, s)[("540551",)].values
    assert v["d"] == Decimal("-1") and v["f"] == 1 and v["g"] == 0


def test_a_sum_over_no_qualifying_rows_is_zero_but_a_first_is_absent():
    """Zero picked is a real answer: the release picked nothing. No expected is not "expected zero"."""
    rows = [_call(0, 10, 1, status="error")]
    assert _one(st.Rule.sum, rows, field="attr:QuantityPicked", statuses=frozenset({"success"})) == Decimal("0")
    blank = _call(0, 10, 0)
    blank["attributes"] = {"ReportingNumber": "540551"}
    assert _one(st.Rule.first, [blank], field="attr:ExpectedQuantity") is None


# ==================================================== 3. keys, carrying, and several releases

def test_a_line_re_released_seven_times_is_seven_rows():
    """Delivery 25810, item 104526, line 4: the reporting number changed each time M3 re-released it
    after a short. Seven releases, seven rows, however many calls each took."""
    reps = ["509464", "514512", "514522", "514540", "514966", "514970", "515066"]
    rows = []
    for i, rep in enumerate(reps):
        r = _call(i * 10, 9, 0)
        r["attributes"]["ReportingNumber"] = rep
        rows.append(r)
    assert len(st.settle(rows, PICK_RELEASE)) == 7


def test_a_row_with_no_key_is_skipped_not_grouped_under_blank():
    r = _call(0, 10, 9)
    r["attributes"].pop("ReportingNumber")
    assert st.settle([r] + RELEASE_540551, PICK_RELEASE).keys() == {("540551",)}


def test_carry_takes_the_first_call_that_has_a_value():
    """The last two picks on line 25810/4 were sent with no lot. The lot is still on the row."""
    rows = [_call(0, 10, 5, lot=""), _call(1, 10, 5, lot="L1"), _call(2, 10, 5, lot="L2")]
    row = st.settle(rows, PICK_RELEASE)[("540551",)]
    assert row.carried["lot_number"] == "L1"


def test_rows_from_other_methods_are_ignored():
    other = _call(0, 10, 9, method="GetOldestItemBalanceAPI")
    assert st.settle([other], PICK_RELEASE) == {}


def test_a_composite_key_is_supported():
    s = st.Settlement(name="line", reads=("ConfirmPickLine",),
                      key=("delivery_number", "item_number", "attr:OrderLine"), carry=(),
                      values=(st.Settled("n", st.Rule.count),))
    assert list(st.settle(RELEASE_540551, s)) == [("27907", "104568", "21")]


# ==================================================== 4. what a settlement refuses

def test_a_settlement_needs_a_key_and_a_method():
    assert any("key" in p for p in st.validate(st.Settlement(
        name="x", reads=("ConfirmPickLine",), key=(), carry=(), values=())))
    assert any("reads" in p for p in st.validate(st.Settlement(
        name="x", reads=(), key=("attr:ReportingNumber",), carry=(), values=())))


def test_a_rule_that_reads_a_field_must_name_one_and_count_must_not():
    problems = st.validate(st.Settlement(
        name="x", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",), carry=(),
        values=(st.Settled("a", st.Rule.sum), st.Settled("b", st.Rule.count, field="attr:X"))))
    assert any("'a'" in p and "field" in p for p in problems)
    assert any("'b'" in p and "count" in p for p in problems)


def test_a_difference_must_name_two_earlier_settled_values():
    problems = st.validate(st.Settlement(
        name="x", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",), carry=(),
        values=(st.Settled("d", st.Rule.difference, left="picked", right="expected"),)))
    assert any("'picked'" in p for p in problems)


def test_two_settled_values_may_not_share_a_name():
    problems = st.validate(st.Settlement(
        name="x", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",), carry=(),
        values=(st.Settled("n", st.Rule.count), st.Settled("n", st.Rule.count))))
    assert any("'n'" in p and "twice" in p for p in problems)


def test_a_settled_value_may_not_shadow_a_carried_field():
    """Both land in the same attribute bag on the settled row."""
    problems = st.validate(st.Settlement(
        name="x", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",), carry=("warehouse",),
        values=(st.Settled("warehouse", st.Rule.count),)))
    assert any("'warehouse'" in p for p in problems)


def test_the_worked_example_validates():
    assert st.validate(PICK_RELEASE) == []


def test_the_module_needs_no_database():
    import inspect
    source = inspect.getsource(st)
    assert "sqlalchemy" not in source and "async_session" not in source
