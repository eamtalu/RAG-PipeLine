"""Chunk 100, the pure half: declaring a lookup, harvesting it, and resolving it at read time.

The live shape this is built from, measured on tmp-live. A pick carries `delivery_number` on all 1,343
records and a customer name on none. Four methods carry both a delivery and a customer, under three
spellings: `NewDeliveryPackage` and `PrintPackageLabel` as bare request keys, `GetNextDeliveryByRoute`
and `GetPackageLabelDetails` as `resp.`-prefixed response keys. Together they name 100 of 114 delivery
keys, 69 distinct customers, and 3,057 of the 3,059 facts that carry a delivery number resolve.

Nothing here touches a database. The store is a separate chunk; what a lookup MEANS is decided here.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

from app.services.analytics import definition as d
from app.services.analytics import lookup as lk

T0 = datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc)

DELIVERY = lk.Lookup(
    name="delivery", key_field="delivery_number",
    attributes=(
        lk.Attribute(name="customer_name", stable=True, sources=(
            lk.Source("NewDeliveryPackage", "DeliveryNumber", "CustomerName"),
            lk.Source("GetNextDeliveryByRoute", "resp.DeliveryNumber", "resp.CustomerName"),
            lk.Source("PrintPackageLabel", "DeliveryNumber", "CustomerName"),
        )),
        lk.Attribute(name="route", stable=True, sources=(
            lk.Source("NewDeliveryPackage", "DeliveryNumber", "Route"),
        )),
    ))
LOOKUPS = {DELIVERY.name: DELIVERY}


def _fact(method, at=T0, **attributes):
    return {"method": method, "event_time": at, "attributes": dict(attributes)}


# ==================================================================== 1. the path spelling

def test_a_lookup_path_is_told_apart_from_a_column_and_from_an_attribute():
    """Three spellings that resolve in three completely different places, so none may be mistaken for
    another: a bare column on the row, `attr:` inside the attributes bag, `lookup:` through this map."""
    assert lk.is_lookup_path("lookup:delivery.customer_name") is True
    assert lk.is_lookup_path("delivery_number") is False
    assert lk.is_lookup_path("attr:CustomerName") is False


def test_a_path_splits_on_the_first_dot_only():
    assert lk.split_path("lookup:delivery.customer_name") == ("delivery", "customer_name")
    assert lk.split_path("lookup:item.description.long") == ("item", "description.long")
    assert lk.path("delivery", "customer_name") == "lookup:delivery.customer_name"


@pytest.mark.parametrize("bad", ["lookup:delivery", "lookup:.customer", "lookup:delivery.",
                                 "delivery.customer_name", "lookup:"])
def test_a_malformed_path_raises_rather_than_grouping_by_nothing(bad):
    """Silently grouping everything into one unlabelled bucket is the exact failure chunk 80 fixed
    live. A malformed path is a caller error and says so."""
    with pytest.raises(ValueError):
        lk.split_path(bad)


# ==================================================================== 2. the declaration

def test_a_declaration_names_a_key_that_is_on_the_fact():
    assert lk.validate(DELIVERY, fact_fields=("delivery_number", "method")) == []


def test_a_key_that_is_not_on_the_fact_row_is_refused():
    bad = lk.Lookup(name="x", key_field="nonesuch", attributes=DELIVERY.attributes)
    problems = lk.validate(bad, fact_fields=("delivery_number",))
    assert any("not a field on the fact row" in p for p in problems)


def test_an_attribute_key_field_must_be_approved_for_capture():
    """Fails CLOSED, exactly as `definition.validate` does for an `attr:` dimension: an unapproved key
    would key the whole lookup on nothing and read as "no data"."""
    bad = lk.Lookup(name="x", key_field="attr:PackageNumber", attributes=DELIVERY.attributes)
    assert any("nobody has ticked" in p for p in lk.validate(bad, fact_fields=("delivery_number",)))
    assert lk.validate(bad, fact_fields=("delivery_number",),
                       known_attributes={"PackageNumber"}) == []


def test_a_lookup_with_no_attributes_or_no_sources_is_refused():
    assert any("declares no attributes" in p for p in
               lk.validate(lk.Lookup("x", "delivery_number"), fact_fields=("delivery_number",)))
    empty = lk.Lookup("x", "delivery_number", (lk.Attribute("customer_name", sources=()),))
    assert any("no source" in p for p in lk.validate(empty, fact_fields=("delivery_number",)))


def test_an_unknown_conflict_rule_is_refused_at_construction():
    with pytest.raises(ValueError):
        lk.Attribute("customer_name", sources=(lk.Source("m", "k", "v"),), on_conflict="whatever")


# ==================================================================== 3. harvesting

def test_one_pass_harvests_every_spelling_of_the_same_relationship():
    """The three spellings are why a source names both fields explicitly. Guessing across namespaces
    is how an approved request field would silently authorise an unapproved response one."""
    rows = [
        _fact("NewDeliveryPackage", DeliveryNumber="25810", CustomerName="BAGELMAN BRIGHTON",
              Route="BRI01"),
        _fact("GetNextDeliveryByRoute", **{"resp.DeliveryNumber": "27481",
                                           "resp.CustomerName": "SOUTHDOWNS MANOR"}),
        _fact("PrintPackageLabel", DeliveryNumber="25811", CustomerName="JUNIPER CATERING"),
    ]
    got = {(o.key, o.attribute): o.value for o in lk.harvest(rows, [DELIVERY])}
    assert got == {("25810", "customer_name"): "BAGELMAN BRIGHTON",
                   ("25810", "route"): "BRI01",
                   ("27481", "customer_name"): "SOUTHDOWNS MANOR",
                   ("25811", "customer_name"): "JUNIPER CATERING"}


def test_a_method_that_supplies_nothing_is_never_consulted():
    """A pick is 1,343 of the records and supplies no customer. Harvest must not pay for it."""
    assert lk.harvest([_fact("ConfirmPickLine", DeliveryNumber="25810", ItemNumber="100006")],
                      [DELIVERY]) == []


def test_a_blank_value_is_nothing_not_a_value():
    """The live data is full of `""` for fields the handheld had no answer for. Stored, a blank would
    beat a real name under first_wins."""
    rows = [_fact("NewDeliveryPackage", DeliveryNumber="25810", CustomerName="   ", Route=""),
            _fact("NewDeliveryPackage", DeliveryNumber="", CustomerName="REAL NAME")]
    assert lk.harvest(rows, [DELIVERY]) == []


def test_a_fact_with_no_event_time_supplies_nothing():
    """`event_time` is nullable on the fact and it is what dates the observation, so a row without
    one cannot say WHEN its value was true."""
    row = _fact("NewDeliveryPackage", DeliveryNumber="25810", CustomerName="X")
    row["event_time"] = None
    assert lk.harvest([row], [DELIVERY]) == []


def test_the_observation_records_which_method_said_it():
    o = lk.harvest([_fact("NewDeliveryPackage", DeliveryNumber="1", CustomerName="X")], [DELIVERY])[0]
    assert (o.lookup, o.source_method, o.at) == ("delivery", "NewDeliveryPackage", T0)


# ==================================================================== 4. resolving in time

def _resolver(*periods):
    r = lk.Resolver()
    for key, value, start, end in periods:
        r.add("delivery", key, "customer_name", lk.Period(value, start, end))
    return r


def test_the_first_value_learned_is_true_from_the_beginning():
    """The rule that makes a late name work. A delivery named AFTER its picks would otherwise leave
    those picks permanently unattributed."""
    r = _resolver(("25810", "BAGELMAN BRIGHTON", lk.BEGINNING, None))
    picked_before_the_name_existed = T0 - timedelta(days=30)
    assert r.value("delivery", "25810", "customer_name", picked_before_the_name_existed) \
        == "BAGELMAN BRIGHTON"


def test_a_later_value_applies_only_from_when_it_was_seen():
    """Point in time, as chosen. A renamed customer must not rewrite last month's report."""
    r = _resolver(("1", "OLD NAME", lk.BEGINNING, T0), ("1", "NEW NAME", T0, None))
    assert r.value("delivery", "1", "customer_name", T0 - timedelta(hours=1)) == "OLD NAME"
    assert r.value("delivery", "1", "customer_name", T0) == "NEW NAME"
    assert r.value("delivery", "1", "customer_name", T0 + timedelta(days=400)) == "NEW NAME"


def test_a_daily_bucket_resolves_at_the_start_of_its_day():
    """Daily and monthly buckets are dates, not instants, so they need an agreed instant to resolve
    at. The start of the day, so a value that began mid-day does not claim the whole of it."""
    r = _resolver(("1", "OLD NAME", lk.BEGINNING, T0), ("1", "NEW NAME", T0, None))
    assert r.value("delivery", "1", "customer_name", date(2026, 9, 14)) == "OLD NAME"
    assert r.value("delivery", "1", "customer_name", date(2026, 9, 15)) == "NEW NAME"


def test_an_unknown_key_and_a_missing_key_both_read_as_not_known():
    """Two facts with the 2 of 3,059 that resolve to nothing, and a fact with no delivery at all.
    Both are "not known". Neither is a blank group at zero."""
    r = _resolver(("1", "KNOWN", lk.BEGINNING, None))
    assert r.value("delivery", "9999", "customer_name", T0) is None
    assert r.value("delivery", None, "customer_name", T0) is None


# ==================================================================== 5. the read plan

def test_a_lookup_grouping_is_read_by_its_key_and_translated_back():
    p = lk.plan(("warehouse_id", "lookup:delivery.customer_name"), LOOKUPS)
    assert p.stored_group_by == ("warehouse_id", "delivery_number")
    assert p.steps == (None, ("delivery", "customer_name"))
    assert p.translates is True


def test_a_grouping_with_no_lookup_needs_no_translation_at_all():
    p = lk.plan(("warehouse_id", "user_name"), LOOKUPS)
    assert p.stored_group_by == ("warehouse_id", "user_name")
    assert p.translates is False


def test_a_path_naming_an_undeclared_lookup_or_attribute_raises():
    with pytest.raises(ValueError, match="not declared"):
        lk.plan(("lookup:nothing.customer_name",), LOOKUPS)
    with pytest.raises(ValueError, match="no attribute"):
        lk.plan(("lookup:delivery.nonesuch",), LOOKUPS)


def test_only_the_keys_in_the_answer_are_needed():
    """The store loads what one answer names, never the whole table. That is what keeps this cheap at
    millions of records: the table grows with distinct deliveries, not with picks."""
    p = lk.plan(("lookup:delivery.customer_name",), LOOKUPS)
    points = {(T0, ("25810",)): {}, (T0, ("25811",)): {}, (T0, (None,)): {}}
    assert p.keys_needed(points) == {"delivery": {"25810", "25811"}}


# ==================================================================== 6. translating an answer

def test_several_keys_collapsing_into_one_value_are_merged_exactly():
    """The normal path, not an edge case: 114 deliveries map to 69 customers on the live tenant. Every
    role a rollup can hold has to survive the merge, which is what makes read-time resolution viable."""
    r = _resolver(("25810", "BAGELMAN", lk.BEGINNING, None), ("25811", "BAGELMAN", lk.BEGINNING, None),
                  ("27481", "SOUTHDOWNS", lk.BEGINNING, None))
    p = lk.plan(("lookup:delivery.customer_name",), LOOKUPS)
    from decimal import Decimal
    points = {
        (T0, ("25810",)): {d.Role.sum_value: Decimal("10"), d.Role.count_value: 2,
                           d.Role.min_value: Decimal("3"), d.Role.max_value: Decimal("7")},
        (T0, ("25811",)): {d.Role.sum_value: Decimal("5"), d.Role.count_value: 1,
                           d.Role.min_value: Decimal("5"), d.Role.max_value: Decimal("5")},
        (T0, ("27481",)): {d.Role.sum_value: Decimal("4"), d.Role.count_value: 1},
    }
    out = lk.translate(points, p, r, merge=d.add_roles)
    assert out[(T0, ("BAGELMAN",))] == {d.Role.sum_value: Decimal("15"), d.Role.count_value: 3,
                                        d.Role.min_value: Decimal("3"), d.Role.max_value: Decimal("7")}
    assert out[(T0, ("SOUTHDOWNS",))][d.Role.sum_value] == Decimal("4")


def test_a_total_survives_the_translation_unchanged():
    """The property that matters most. Re-keying may never create or lose units."""
    from decimal import Decimal
    r = _resolver(*[(str(k), "ONE CUSTOMER", lk.BEGINNING, None) for k in range(20)])
    p = lk.plan(("lookup:delivery.customer_name",), LOOKUPS)
    points = {(T0, (str(k),)): {d.Role.sum_value: Decimal(k), d.Role.count_value: 1}
              for k in range(20)}
    out = lk.translate(points, p, r, merge=d.add_roles)
    assert sum(v[d.Role.sum_value] for v in out.values()) == sum(Decimal(k) for k in range(20))
    assert sum(v[d.Role.count_value] for v in out.values()) == 20


def test_an_unresolvable_key_keeps_its_units_under_not_known():
    """Never dropped and never zero. The 2 picks of 1,343 whose delivery nobody named still happened."""
    from decimal import Decimal
    r = _resolver(("25810", "BAGELMAN", lk.BEGINNING, None))
    p = lk.plan(("lookup:delivery.customer_name",), LOOKUPS)
    points = {(T0, ("25810",)): {d.Role.sum_value: Decimal("10")},
              (T0, ("99999",)): {d.Role.sum_value: Decimal("3")}}
    out = lk.translate(points, p, r, merge=d.add_roles)
    assert out[(T0, (None,))][d.Role.sum_value] == Decimal("3")


def test_the_other_grouping_positions_pass_through_untouched():
    from decimal import Decimal
    r = _resolver(("25810", "BAGELMAN", lk.BEGINNING, None))
    p = lk.plan(("warehouse_id", "lookup:delivery.customer_name"), LOOKUPS)
    points = {(T0, ("1", "25810")): {d.Role.sum_value: Decimal("10")}}
    out = lk.translate(points, p, r, merge=d.add_roles)
    assert list(out) == [(T0, ("1", "BAGELMAN"))]
