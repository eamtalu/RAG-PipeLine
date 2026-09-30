"""Chunk 129: where a pick came from, against where the pick list said, and how the picker looked.

Four locations sit around one pick on tmp-live (measured 2026-09-30):
  designated  the pick-list line's `Location`, inside the `ListPickLinesByUser` RESPONSE LIST, keyed
              by `ReportingNumber` (the release)
  suggested   `GetOldestItemBalanceAPI` resp.Location, same user and item, seconds before
  checked     `GetSummarisedBalanceDetails` / `IsStockInLocation` Location, same user and item
  actual      `ConfirmPickLine` FromLocation, on every confirmed pick
and the zone of a location comes from `GetSummarisedBalanceDetails` resp.StockZone. Today's sample:
405 of 406 picks matched their line; 314 from the designated location, 91 elsewhere, almost all from
JIT after the picker checked JIT and found stock.

Three mechanisms, each generic:
  1. a lookup source that reads a list response element by element (`list: true`);
  2. a conflict rule that keeps every value a key was given (`all_values`): 6 of 133 locations were
     seen in two zones, and the person asked to see both and have them flagged;
  3. settlement rules that read a looked-up value (`lookup`) and the other calls the same user made
     on the same item just before the release (`nearby_*`).
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.services.analytics import lookup as lk
from app.services.analytics import settle as st

T0 = datetime(2026, 9, 30, 9, 23, tzinfo=timezone.utc)

PICK_LINE = lk.Lookup(name="pick line", key_field="attr:ReportingNumber", attributes=(
    lk.Attribute("Location", sources=(lk.Source("ListPickLinesByUser", "ReportingNumber", "Location", list=True),)),))
LOCATION = lk.Lookup(name="location", key_field="attr:Location", attributes=(
    lk.Attribute("StockZone", on_conflict="all_values",
                 sources=(lk.Source("GetSummarisedBalanceDetails", "resp.Location", "resp.StockZone"),)),))


# ==================================================== 1. list sources

def test_a_list_source_harvests_every_element_of_the_response():
    rows = [{"id": "t1", "method": "ListPickLinesByUser", "event_time": T0}]
    entries = {"t1": [("response", {"response": [
        {"ReportingNumber": "620498", "Location": "A03A", "ItemNumber": "105723"},
        {"ReportingNumber": "620499", "Location": "H04B"},
        {"ReportingNumber": "620500", "Location": ""},          # blank says nothing
        {"Location": "K01A"},                                   # no key
    ]}), ("mi_result", {"records": []})]}
    obs = lk.harvest_lists(rows, entries, [PICK_LINE, LOCATION])
    assert sorted((o.key, o.value) for o in obs) == [("620498", "A03A"), ("620499", "H04B")]
    assert all(o.lookup == "pick line" and o.attribute == "Location" and o.at == T0 for o in obs)


def test_a_list_source_is_ignored_by_the_fact_harvest_and_a_plain_one_by_the_list_harvest():
    fact = {"method": "ListPickLinesByUser", "event_time": T0, "attributes": {"ReportingNumber": "1", "Location": "X"}}
    assert lk.harvest([fact], [PICK_LINE]) == []
    rows = [{"id": "t2", "method": "GetSummarisedBalanceDetails", "event_time": T0}]
    assert lk.harvest_lists(rows, {"t2": [("response", {"response": [{"Location": "A03A"}]})]}, [LOCATION]) == []


def test_all_values_is_a_known_conflict_rule_and_the_combined_value_is_sorted_and_stable():
    assert "all_values" in lk.CONFLICT_RULES
    assert lk.combine_values("C1", "A1") == "A1 | C1"
    assert lk.combine_values("A1 | C1", "A1") == "A1 | C1"
    assert lk.combine_values("A1 | C1", "FZ") == "A1 | C1 | FZ"
    assert lk.is_combined("A1 | C1") and not lk.is_combined("A1")


# ==================================================== 2. settlement rules, pure

def _call(method, at_s, **attrs):
    base = {"method": method, "event_time": T0 + timedelta(seconds=at_s), "user_name": "HWORREL",
            "item_number": "105723", "status": attrs.pop("status", "success"), "quantity_classification": "pick"}
    base["attributes"] = attrs
    return base


PICK = st.Settlement(
    name="pick_release", reads=("ConfirmPickLine",), key=("attr:ReportingNumber",),
    carry=("item_number", "user_name"),
    values=(
        st.Settled("from_location", st.Rule.first_text, field="attr:FromLocation"),
        st.Settled("designated_location", st.Rule.lookup, field="lookup:pick line.Location",
                   left="attr:ReportingNumber"),
        st.Settled("from_zone", st.Rule.lookup, field="lookup:location.StockZone", left="from_location"),
        st.Settled("designated_zone", st.Rule.lookup, field="lookup:location.StockZone", left="designated_location"),
        st.Settled("from_designated", st.Rule.flag, left="from_location", op="==", right="designated_location"),
        st.Settled("same_zone", st.Rule.flag, left="from_zone", op="==", right="designated_zone"),
        st.Settled("lookups", st.Rule.nearby_count, methods=("GetSummarisedBalanceDetails", "IsStockInLocation",
                                                             "GetOldestItemBalanceAPI"),
                   match=("user_name", "item_number"), window_s=180),
        st.Settled("empty_checks", st.Rule.nearby_count, methods=("GetSummarisedBalanceDetails",),
                   match=("user_name", "item_number"), window_s=180, statuses=frozenset({"soft", "error"})),
        st.Settled("locations_checked", st.Rule.nearby_distinct, field="attr:Location",
                   methods=("GetSummarisedBalanceDetails", "IsStockInLocation"),
                   match=("user_name", "item_number"), window_s=180),
        st.Settled("suggested_location", st.Rule.nearby_last, field="attr:resp.Location",
                   methods=("GetOldestItemBalanceAPI",), match=("user_name", "item_number"), window_s=180),
        st.Settled("picked_from_checked", st.Rule.nearby_has, field="attr:Location", left="from_location",
                   methods=("GetSummarisedBalanceDetails", "IsStockInLocation"),
                   match=("user_name", "item_number"), window_s=180),
        st.Settled("followed_suggestion", st.Rule.flag, left="from_location", op="==", right="suggested_location"),
    ))


class Table:
    """A resolver over a small in-memory lookup table, the shape `settle` is handed."""

    def __init__(self, values):
        self.values = values
        self.asked = []

    def __call__(self, lookup, attribute, key):
        self.asked.append((lookup, attribute, key))
        return self.values.get((lookup, attribute, key))


def test_a_pick_off_its_designated_location_after_checking_jit():
    calls = [_call("ConfirmPickLine", 60, ReportingNumber="620491", FromLocation="JIT")]
    context = [
        _call("GetOldestItemBalanceAPI", 10, **{"resp.Location": "K04A"}),
        _call("GetSummarisedBalanceDetails", 20, Location="K04A", status="soft"),   # empty
        _call("IsStockInLocation", 30, Location="JIT"),
        _call("GetSummarisedBalanceDetails", 40, Location="JIT"),
        _call("GetSummarisedBalanceDetails", -500, Location="ZZZ"),                  # outside the window
        {**_call("IsStockInLocation", 35, Location="JIT"), "user_name": "OTHER"},     # another picker
    ]
    table = Table({("pick line", "Location", "620491"): "K04A", ("location", "StockZone", "K04A"): "A1",
                   ("location", "StockZone", "JIT"): "JT"})
    row = st.settle(calls, PICK, context=context, resolve=table)[("620491",)]
    v = row.values
    assert v["from_location"] == "JIT" and v["designated_location"] == "K04A"
    assert v["from_zone"] == "JT" and v["designated_zone"] == "A1"
    assert v["from_designated"] == 0 and v["same_zone"] == 0
    assert v["lookups"] == 4 and v["empty_checks"] == 1 and v["locations_checked"] == 2
    assert v["suggested_location"] == "K04A" and v["picked_from_checked"] == 1 and v["followed_suggestion"] == 0


def test_a_pick_from_its_designated_location_with_no_lookups():
    calls = [_call("ConfirmPickLine", 60, ReportingNumber="620498", FromLocation="A03A")]
    table = Table({("pick line", "Location", "620498"): "A03A", ("location", "StockZone", "A03A"): "A1"})
    v = st.settle(calls, PICK, context=[], resolve=table)[("620498",)].values
    assert v["from_designated"] == 1 and v["same_zone"] == 1
    assert v["lookups"] == 0 and v["locations_checked"] == 0 and v["picked_from_checked"] == 0
    assert v["suggested_location"] is None and v["followed_suggestion"] is None


def test_unknown_designated_location_is_unknown_not_a_miss():
    calls = [_call("ConfirmPickLine", 60, ReportingNumber="1", FromLocation="A03A")]
    v = st.settle(calls, PICK, context=[], resolve=Table({}))[("1",)].values
    assert v["designated_location"] is None and v["from_designated"] is None and v["designated_zone"] is None


def test_without_a_resolver_or_context_the_new_rules_are_unknown_and_the_old_ones_unchanged():
    calls = [_call("ConfirmPickLine", 60, ReportingNumber="1", FromLocation="A03A")]
    v = st.settle(calls, PICK)[("1",)].values
    assert v["from_location"] == "A03A" and v["designated_location"] is None and v["lookups"] == 0


def test_first_text_keeps_a_code_and_first_is_unchanged():
    s = st.Settlement(name="x", reads=("M",), key=("attr:K",), carry=(),
                      values=(st.Settled("loc", st.Rule.first_text, field="attr:L"), st.Settled("old", st.Rule.first, field="attr:L"),
                              st.Settled("q", st.Rule.first_text, field="attr:Q")))
    row = st.settle([{"method": "M", "event_time": T0, "attributes": {"K": "1", "L": " A03A ", "Q": "2.5"}}], s)[("1",)]
    assert row.values["loc"] == "A03A" and row.values["old"] is None and row.values["q"] == "2.5"


def test_validation_of_the_new_rules():
    bad = st.Settlement(name="x", reads=("M",), key=("attr:K",), carry=(), values=(
        st.Settled("a", st.Rule.lookup, field="attr:X", left="attr:K"),           # not a lookup path
        st.Settled("b", st.Rule.nearby_count, methods=(), match=("user_name",), window_s=60),
        st.Settled("c", st.Rule.nearby_last, methods=("M2",), match=(), window_s=60),   # no field, no match
        st.Settled("d", st.Rule.nearby_has, field="attr:L", methods=("M2",), match=("user_name",), window_s=0),
    ))
    problems = " | ".join(st.validate(bad))
    assert "lookup path" in problems and "which methods" in problems and "must name a field" in problems
    assert "match" in problems and "window" in problems and "left-hand" in problems
    assert st.validate(PICK) == []


def test_lookup_keys_a_settlement_needs_are_reported_for_loading():
    calls = [_call("ConfirmPickLine", 60, ReportingNumber="620491", FromLocation="JIT")]
    table = Table({("pick line", "Location", "620491"): "K04A"})
    st.settle(calls, PICK, context=[], resolve=table)
    assert ("pick line", "Location", "620491") in table.asked
    assert ("location", "StockZone", "JIT") in table.asked and ("location", "StockZone", "K04A") in table.asked


# ==================================================== 3. the store: context, lookups, conflicts

import uuid  # noqa: E402

from sqlalchemy import delete, select  # noqa: E402

from app.config.database import async_session  # noqa: E402
from app.persistence.models.analytics_fact import AnalyticsFact  # noqa: E402
from app.persistence.models.analytics_lookup import AnalyticsLookup, AnalyticsLookupValue  # noqa: E402
from app.persistence.models.analytics_settlement import AnalyticsSettledRow, AnalyticsSettlement  # noqa: E402
from app.persistence.models.customer import Customer  # noqa: E402
from app.services.analytics import lookup_store, settle_store  # noqa: E402

CC = "test_chunk129"


async def _wipe():
    async with async_session() as db:
        for model in (AnalyticsSettledRow, AnalyticsSettlement, AnalyticsLookupValue, AnalyticsLookup, AnalyticsFact):
            await db.execute(delete(model).where(model.customer_code == CC))
        await db.execute(delete(Customer).where(Customer.customer_code == CC))
        await db.commit()


@pytest.fixture
async def clean():
    await _wipe()
    async with async_session() as db:
        db.add(Customer(customer_code=CC, name="pick locations probe", timezone="Europe/London"))
        await db.commit()
    yield
    await _wipe()


def _fact(method, at_s, *, user="HWORREL", item="105723", status="success", **attrs):
    when = T0 + timedelta(seconds=at_s)
    return AnalyticsFact(
        id=uuid.uuid4(), customer_code=CC, source_transaction_id=uuid.uuid4(), source_started_at=when,
        source_version_hash=uuid.uuid4().hex[:8], revision=1, event_time=when, business_date=when.date(),
        transaction_name="Brighton Stock Pick", method=method, status=status, quantity_classification="pick",
        warehouse="BRI", item_number=item, user_name=user, attributes=attrs, created_at=when)


async def test_settling_a_release_loads_its_lookups_in_rounds_and_its_context_calls(clean):
    async with async_session() as db:
        db.add_all([
            _fact("ConfirmPickLine", 60, ReportingNumber="620491", FromLocation="JIT"),
            _fact("GetOldestItemBalanceAPI", 10, **{"resp.Location": "K04A"}),
            _fact("GetSummarisedBalanceDetails", 20, status="soft", Location="K04A"),
            _fact("IsStockInLocation", 30, Location="JIT"),
            _fact("GetSummarisedBalanceDetails", 40, Location="JIT"),
            _fact("IsStockInLocation", 30, user="OTHER", Location="JIT"),
            _fact("GetSummarisedBalanceDetails", 30, item="999999", Location="JIT"),
        ])
        for lookup in (PICK_LINE, LOCATION):
            db.add(AnalyticsLookup(customer_code=CC, name=lookup.name, key_field=lookup.key_field,
                                   attributes=lookup_store.to_row(lookup), enabled=True))
        for lookup_name, key, attribute, value in (("pick line", "620491", "Location", "K04A"),
                                                   ("location", "K04A", "StockZone", "A1"),
                                                   ("location", "JIT", "StockZone", "JT")):
            db.add(AnalyticsLookupValue(customer_code=CC, lookup=lookup_name, key=key, attribute=attribute, value=value,
                                        valid_from=lk.BEGINNING, first_seen_at=T0, last_seen_at=T0))
        await db.commit()
    async with async_session() as db:
        written = await settle_store.settle_keys(db, CC, PICK, [("620491",)])
        await db.commit()
    assert written == 1
    async with async_session() as db:
        row = (await db.execute(select(AnalyticsSettledRow).where(AnalyticsSettledRow.customer_code == CC))).scalar_one()
    a = row.attributes
    assert (a["from_location"], a["designated_location"], a["from_zone"], a["designated_zone"]) == ("JIT", "K04A", "JT", "A1")
    assert (a["from_designated"], a["same_zone"], a["lookups"], a["empty_checks"]) == ("0", "0", "4", "1")
    assert (a["locations_checked"], a["suggested_location"], a["picked_from_checked"], a["followed_suggestion"]) == \
        ("2", "K04A", "1", "0")


def test_a_settlement_with_the_new_rules_round_trips_through_its_document():
    assert settle_store.from_json("pick_release", settle_store.to_json(PICK)) == PICK


async def test_all_values_keeps_both_zones_of_a_location_and_counts_the_conflict_once(clean):
    obs = [lk.Observation("location", "C04A", "StockZone", "C1", T0, "GetSummarisedBalanceDetails"),
           lk.Observation("location", "C04A", "StockZone", "C1", T0 + timedelta(minutes=1), "GetSummarisedBalanceDetails")]
    async with async_session() as db:
        stats = await lookup_store.record(db, CC, obs, {"location": LOCATION})
        await db.commit()
    assert stats["inserted"] == 1 and stats["conflicts"] == 0
    async with async_session() as db:
        stats = await lookup_store.record(db, CC, [lk.Observation("location", "C04A", "StockZone", "A1",
                                                                  T0 + timedelta(hours=1), "GetSummarisedBalanceDetails")],
                                          {"location": LOCATION})
        await db.commit()
        again = await lookup_store.record(db, CC, [lk.Observation("location", "C04A", "StockZone", "A1",
                                                                  T0 + timedelta(hours=2), "GetSummarisedBalanceDetails")],
                                          {"location": LOCATION})
        await db.commit()
    assert stats["conflicts"] == 1 and again["conflicts"] == 0
    async with async_session() as db:
        values = (await db.execute(select(AnalyticsLookupValue).where(AnalyticsLookupValue.customer_code == CC))).scalars().all()
    assert [v.value for v in values] == ["A1 | C1"] and lk.is_combined(values[0].value)


async def test_a_backfill_with_more_keys_than_postgres_takes_parameters_is_written_in_batches(clean):
    """The first live `pick line` backfill named 150k+ release keys in one statement and failed."""
    obs = [lk.Observation("pick line", str(700000 + i), "Location", "A03A", T0, "ListPickLinesByUser") for i in range(40_000)]
    async with async_session() as db:
        stats = await lookup_store.record(db, CC, obs, {"pick line": PICK_LINE})
        await db.commit()
        again = await lookup_store.record(db, CC, obs[:35_000], {"pick line": PICK_LINE})
        await db.commit()
    assert stats["inserted"] == 40_000 and again["extended"] == 35_000 and again["inserted"] == 0
