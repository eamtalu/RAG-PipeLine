"""Chunk 110: a name is not a quantity, whatever it is spelled with.

Chunk 109 taught the catalogue what a LEVEL is and refused to add one up. It left the other three
kinds as documentation: marking a field a slice recorded an opinion and changed nothing. This gives
the opinion teeth, and generalises the rule so a fourth kind would cost one line rather than a fork.

The case that prompted it, measured on tmp-live on 16 September 2026:

| field            | values | distinct | all digits | what it holds                    |
|------------------|--------|----------|------------|----------------------------------|
| PackageNumber    |  7,015 |    1,597 |         0% | 27904/1-2                        |
| ToLocation       |  4,111 |      136 |         0% | A01A, WASTE                      |
| DeliveryNumber   |  8,841 |      368 |       100% | 24960 to 27946                   |
| OrderNumber      |  6,064 |      396 |       100% | 1000008371 to 1002364            |
| ExpectedQuantity |  3,556 |      199 |       100% | 0.00004 to 9.375                 |

`DeliveryNumber` and `ExpectedQuantity` are both entirely numeric and one is a name and one is a
quantity. No test on the values separates them, which is the whole reason a person records the kind.
And a delivery number that is all digits today may be alphanumeric tomorrow without anything saying so.

Two different failures, and the quiet one is the dangerous one. Adding up `PackageNumber` parses
nothing and the chart reads as no data, which is loud. Adding up `DeliveryNumber` gives a confident
seven-figure total that is entirely meaningless, and nothing about it looks wrong.

What a slice still allows is `count` and `distinct`, because counting rows and counting how many
different deliveries there were are the two real questions a name answers.
"""

import pytest

from app.persistence.models.analytics_field_meaning import KINDS
from app.services.analytics import definition as d

APPROVED = frozenset({
    "DeliveryNumber", "PackageNumber", "OrderNumber", "ToLocation",
    "QuantityPicked", "ExpectedQuantity", "resp.QuantityOnHand", "rec.ITNO",
})

KINDS_LIVE = {
    "DeliveryNumber": "slice",
    "PackageNumber": "slice",
    "OrderNumber": "slice",
    "ToLocation": "slice",
    "QuantityPicked": "measure",
    "ExpectedQuantity": "measure",
    "resp.QuantityOnHand": "level",
}


def _metric(aggregation=d.Aggregation.sum, field="attr:DeliveryNumber", minus=None,
            dimensions=("warehouse",), source="transaction", **kw):
    return d.MetricDefinition(
        name="probe", source=source, dimensions=dimensions,
        measures=(d.Measure(name="m", aggregation=aggregation, field=field, minus=minus,
                            unit="units"),),
        grains=("hourly", "daily"), **kw)


def _problems(definition, *, kinds=None, approved=APPROVED):
    return d.validate(definition, known_attributes=approved,
                      field_kinds=KINDS_LIVE if kinds is None else kinds)


def _about(problems, word):
    return [p for p in problems if word in p]


# ==================================================== 1. what a slice refuses

@pytest.mark.parametrize("aggregation", [
    d.Aggregation.sum, d.Aggregation.average, d.Aggregation.stats,
    d.Aggregation.extent, d.Aggregation.percentile,
])
def test_arithmetic_on_a_name_is_refused(aggregation):
    """Every one of these reads the VALUE and does sums with it. A delivery number has no size: the
    average of 24960 and 27946 is not a delivery, and the highest one is not the biggest anything."""
    assert len(_about(_problems(_metric(aggregation=aggregation)), "NAME")) == 1


def test_the_refusal_names_the_field_and_offers_the_two_questions_a_name_answers():
    problem = _about(_problems(_metric()), "NAME")[0]
    assert "DeliveryNumber" in problem
    assert "count" in problem and "different" in problem


@pytest.mark.parametrize("aggregation", [d.Aggregation.count, d.Aggregation.distinct])
def test_counting_a_name_is_allowed(aggregation):
    """How many lines mentioned a delivery, and how many different deliveries there were. Both read
    the row rather than doing arithmetic on the value, and both are what a name is for."""
    field = None if aggregation is d.Aggregation.count else "attr:DeliveryNumber"
    assert _about(_problems(_metric(aggregation=aggregation, field=field)), "NAME") == []


def test_a_name_may_still_be_grouped_by():
    """The point of marking it. Grouping by package number is exactly what it is for, and it works
    whether the value reads 27904/1-2 or 24960."""
    definition = _metric(aggregation=d.Aggregation.count, field=None,
                         dimensions=("warehouse", "attr:PackageNumber"))
    assert _about(_problems(definition), "NAME") == []


def test_subtracting_a_name_is_refused_from_either_side():
    """A quantity minus a delivery number is not a smaller quantity."""
    assert len(_about(_problems(_metric(field="attr:QuantityPicked",
                                        minus="attr:DeliveryNumber")), "NAME")) == 1
    assert len(_about(_problems(_metric(field="attr:DeliveryNumber",
                                        minus="attr:QuantityPicked")), "NAME")) == 1


def test_an_alphanumeric_name_and_an_all_digit_one_are_refused_alike():
    """PackageNumber is never numeric and DeliveryNumber always is. The rule reads the person's
    decision, not the data, which is the only thing that could tell them apart."""
    for field in ("attr:PackageNumber", "attr:DeliveryNumber", "attr:ToLocation"):
        assert len(_about(_problems(_metric(field=field)), "NAME")) == 1


# ==================================================== 2. the other kinds are unaffected

def test_a_quantity_is_still_added_up():
    """ExpectedQuantity is 100 per cent numeric exactly as DeliveryNumber is, and somebody said it is
    an amount. That single difference is the whole catalogue."""
    assert _problems(_metric(field="attr:QuantityPicked")) == []
    assert _problems(_metric(field="attr:ExpectedQuantity")) == []


def test_pick_shortfall_is_accepted():
    """Chunk 101's other flagship. Both halves are amounts, so the difference adds."""
    assert _problems(_metric(field="attr:QuantityPicked", minus="attr:ExpectedQuantity")) == []


def test_a_level_still_refuses_only_the_sum():
    """Chunk 109's rule is unchanged by this one. The average stock on hand is a real answer; the
    average delivery number is not."""
    level = _metric(field="attr:resp.QuantityOnHand")
    assert len(_about(_problems(level), "LEVEL")) == 1
    assert _problems(_metric(aggregation=d.Aggregation.average,
                             field="attr:resp.QuantityOnHand")) == []


def test_an_undecided_field_refuses_nothing():
    """Nobody has said what `rec.ITNO` is. Absent is not a decision, and a screen that refused on a
    blank would punish people for not having got to it yet."""
    assert d.validate(_metric(field="attr:rec.ITNO", source="record",
                              dimensions=("attr:rec.ITNO",)),
                      known_attributes=APPROVED, field_kinds=KINDS_LIVE) == []


def test_a_field_marked_noise_refuses_nothing_either():
    """Noise says "nobody will report on this", not "this is forbidden". If somebody does report on
    it, the marking was wrong, and the fix is to change the marking."""
    kinds = {**KINDS_LIVE, "DeliveryNumber": "noise"}
    assert _about(_problems(_metric(), kinds=kinds), "NAME") == []


# ==================================================== 3. the shape of the rule

def test_omitting_the_kinds_refuses_nothing():
    """The same guarantee chunk 109 relies on, now covering every kind. The fold passes no kinds, so
    a metric that has been running for months keeps running whatever anybody marks today."""
    assert d.validate(_metric(), known_attributes=APPROVED) == []


def test_every_kind_declares_what_it_refuses():
    """A fifth kind added to the model without a line here would silently refuse nothing. This is the
    test that makes somebody decide."""
    for kind in KINDS:
        assert kind in d.REFUSED_BY_KIND, f"{kind!r} does not say what it refuses"
    assert set(d.REFUSED_BY_KIND) == set(KINDS)


def test_what_each_kind_refuses_in_full():
    A = d.Aggregation
    assert d.REFUSED_BY_KIND["measure"] == frozenset()
    assert d.REFUSED_BY_KIND["noise"] == frozenset()
    assert d.REFUSED_BY_KIND["level"] == frozenset({A.sum})
    # `latest` joined this set in chunk 115. It does no arithmetic, but it reads the value as a
    # NUMBER exactly as the others do, so on a name every row is skipped and the answer is silently
    # absent rather than visibly wrong.
    assert d.REFUSED_BY_KIND["slice"] == frozenset(
        {A.sum, A.average, A.stats, A.extent, A.percentile, A.latest})
    # The two a name still answers.
    assert not d.refuses("slice", A.count) and not d.refuses("slice", A.distinct)


def test_the_kinds_here_agree_with_the_model():
    """`definition.py` names them itself so it keeps its no-database property, exactly as it does for
    the transaction statuses. This is the test that stops the two copies drifting."""
    assert set(d.REFUSED_BY_KIND) == set(KINDS)
