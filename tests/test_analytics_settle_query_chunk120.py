"""Chunk 120: the query language over settled rows. Filters, stats, time buckets and sort, parsed
and validated before any SQL, so a mistyped field is refused by name."""

import pytest

from app.services.analytics import settle as st
from app.services.analytics import settle_query as q

PICK = st.Settlement(
    name="pick_release", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",),
    carry=("delivery_number", "attr:OrderLine", "user_name"),
    values=(st.Settled("expected", st.Rule.first, field="attr:ExpectedQuantity"),
            st.Settled("picked", st.Rule.sum, field="attr:QuantityPicked"),
            st.Settled("is_short", st.Rule.flag, left="picked", op="<", right_value=None),
            st.Settled("duration_s", st.Rule.difference, left="picked", right="expected")))


def test_a_filter_is_field_comparison_value():
    assert q.parse_filter("is_short==1") == q.Filter("is_short", "==", "1")
    assert q.parse_filter(" duration_s > 300 ") == q.Filter("duration_s", ">", "300")
    assert q.parse_filter("user_name!=BCHAM") == q.Filter("user_name", "!=", "BCHAM")
    assert q.parse_filter("event_time>=2026-09-19T00:00:00+01:00") == q.Filter("event_time", ">=", "2026-09-19T00:00:00+01:00")


def test_a_filter_without_a_comparison_is_refused_with_an_example():
    with pytest.raises(ValueError, match="is_short==1"):
        q.parse_filter("is_short")


def test_a_stat_is_kind_and_field():
    assert q.parse_stat("median:duration_s") == q.Stat("median", "duration_s")
    assert q.parse_stat("distinct:user_name").label == "distinct_user_name"
    assert q.parse_stat("p95:attr:duration_s").label == "p95_duration_s"
    with pytest.raises(ValueError, match="median, p90"):
        q.parse_stat("average:duration_s")


def test_the_known_fields_are_the_row_the_settlement_makes():
    known = q.known_fields(PICK)
    assert {"key", "event_time", "delivery_number", "OrderLine", "user_name", "expected", "is_short", "calls"} <= known
    assert "ReportingNumber" not in known


def test_validation_names_the_bad_field_and_lists_the_good_ones():
    out = q.validate(PICK, filters=(q.Filter("shortfal", "==", "1"),))
    assert len(out) == 1 and "shortfal" in out[0] and "is_short" in out[0]
    assert q.validate(PICK, filters=(q.Filter("is_short", "==", "1"), q.Filter("attr:OrderLine", "==", "21"))) == []


def test_validation_refuses_a_median_of_text_but_allows_distinct():
    assert q.validate(PICK, stats=(q.Stat("median", "user_name"),)) != []
    assert q.validate(PICK, stats=(q.Stat("distinct", "user_name"), q.Stat("p95", "duration_s"))) == []


def test_time_buckets_are_allowed_in_the_grouping_and_nothing_else_unknown_is():
    assert q.validate(PICK, group_by=("hour", "hour_start", "day", "week", "user_name", "key")) == []
    assert q.validate(PICK, group_by=("minute",)) != []
    assert q.validate(PICK, sort="duration_s") == []
    assert q.validate(PICK, sort="speed") != []
