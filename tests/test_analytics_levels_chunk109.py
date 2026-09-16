"""Chunk 109: a level is a number you must not add up.

A field holds two different sorts of number and nothing in the values tells them apart.

An AMOUNT is how much happened. Item 104353 was picked 82 times for 2,869 units, and those add to
2,869 because 2,869 units genuinely left the shelf. Adding them is the point.

A LEVEL is how much there IS at a moment. The same item was read at 714, then 700, then 672, then 664,
and on downwards; all 73 of its on-hand readings add to 41,206 where 427 are on the shelf. It is one
shelf counted 73 times. Across the whole tenant, adding every on-hand reading gives 340,206 where the
stock is 18,248.

Every figure here was measured on tmp-live on 16 September 2026 and will drift as facts arrive. What
does not drift is the shape of the error: the sum is an order of magnitude out, and always will be.

Discovery over the live facts cannot tell them apart: both are numeric, both repeat, both go up and
down. Only a person can say which is which, which is why `kind` exists on the meaning row at all.

Six live fields are levels and all six are ticked and available to a metric today: `BalanceQuantity`,
`OnHandBalanceToCompare`, `resp.QuantityOnHand`, `resp.OnHandQuantity`, `resp.AllocatedQuantity` and
`CurrentTotalOnHandQty`. No active metric adds one up yet, so this shuts the door before anybody walks
through it.

**The default is the whole design.** `known_attributes=None` refuses everything, because an unapproved
field would write to a table kept forever. `field_kinds=None` refuses NOTHING, because the caller that
omits it is the fold, and a fold that stopped folding a metric somebody has read for months is worse
than a wrong label on a chart. The refusal is a gate on the way in, never a re-argument of a decision
already taken.
"""

import pytest

from app.services.analytics import definition as d

#: Approved for capture. Approval and meaning are separate questions: every field here is ticked, and
#: only some of them are levels.
APPROVED = frozenset({
    "resp.QuantityOnHand", "BalanceQuantity", "CountedQuantity", "QuantityPicked",
    "ExpectedQuantity", "rec.STQT", "rec.ITNO",
})

#: What a person has said each field IS. Namespaced exactly as it appears in `attributes`.
#: Chunk 110 generalised the single level set into this map, so a fourth kind costs no new argument.
LEVELS = {"resp.QuantityOnHand": "level", "BalanceQuantity": "level", "CountedQuantity": "level"}


def _metric(aggregation=d.Aggregation.sum, field="attr:resp.QuantityOnHand", minus=None,
            dimensions=("warehouse",), source="transaction", **kw):
    return d.MetricDefinition(
        name="stock", source=source, dimensions=dimensions,
        measures=(d.Measure(name="on_hand", aggregation=aggregation, field=field, minus=minus,
                            unit="units"),),
        grains=("hourly", "daily"), **kw)


def _problems(definition, *, levels=LEVELS, approved=APPROVED):
    return d.validate(definition, known_attributes=approved, field_kinds=levels)


def _about_levels(problems):
    """Only the problems this chunk adds, so an unrelated complaint cannot pass a test by accident."""
    return [p for p in problems if "level" in p]


# ==================================================================== 1. the refusal

def test_adding_a_level_up_is_refused():
    """73 on-hand readings of item 104353 add to 41,206 where 427 are on the shelf.

    The sum is not merely imprecise, it is a number the warehouse never held at any instant.
    """
    problems = _problems(_metric())
    assert len(_about_levels(problems)) == 1


def test_the_refusal_names_the_field_and_says_what_to_use_instead():
    """A refusal that only states the rule leaves somebody stuck. This one gives them the next move."""
    problem = _about_levels(_problems(_metric()))[0]
    assert "resp.QuantityOnHand" in problem
    assert "on_hand" in problem
    for alternative in ("average", "extent", "percentile"):
        assert alternative in problem


def test_only_adding_up_is_refused():
    """Asserted against the enum itself, so adding an eighth aggregation forces a decision rather than
    quietly inheriting whichever default the loop happens to give it."""
    assert d.REFUSED_BY_KIND["level"] == frozenset({d.Aggregation.sum})
    assert d.refuses("level", d.Aggregation.sum) is True
    for other in d.Aggregation:
        if other is not d.Aggregation.sum:
            assert d.refuses("level", other) is False


# ==================================================================== 2. the default that protects live metrics

def test_omitting_the_level_set_refuses_nothing():
    """The most important test in the chunk.

    `registry.active_definitions` revalidates every live metric before the worker folds it, and skips
    one that fails. If it were handed the level set, describing a field would silently switch off a
    metric somebody has been reading for months. It passes nothing, so nothing is refused, and this
    test is what stops a later reader "fixing" the omission.
    """
    assert d.validate(_metric(), known_attributes=APPROVED) == []


def test_an_empty_level_set_also_refuses_nothing():
    """Nobody has marked anything yet, which is the state every tenant starts in."""
    assert _about_levels(_problems(_metric(), levels={})) == []


# ==================================================================== 3. what a level may still do

@pytest.mark.parametrize("aggregation", [
    d.Aggregation.count,      # reads no value at all; it counts how many times somebody looked
    d.Aggregation.average,    # mean stock over the window is a real answer
    d.Aggregation.stats,      # how much stock swings; the answer is a spread, never a total
    d.Aggregation.extent,     # the lowest and highest stock seen; both are readings
    d.Aggregation.percentile, # median stock; the histogram never adds two readings together
    d.Aggregation.distinct,   # counts different values; useless on most levels, never wrong
])
def test_every_other_aggregation_over_a_level_is_allowed(aggregation):
    """Exactly one aggregation is refused. Refusing a harmless one teaches people the rule is
    arbitrary, and they then work around the useful part of it too."""
    assert _about_levels(_problems(_metric(aggregation=aggregation))) == []


def test_a_level_may_still_be_grouped_by():
    """Grouping by stock band is a perfectly good question. Only adding is refused."""
    definition = _metric(aggregation=d.Aggregation.count, field=None,
                         dimensions=("warehouse", "attr:resp.QuantityOnHand"))
    assert _about_levels(_problems(definition)) == []


# ==================================================================== 4. subtraction

def test_adding_up_the_difference_of_two_levels_is_allowed():
    """Count variance, which is chunk 101's flagship: `CountedQuantity` minus `BalanceQuantity`.

    Both halves are levels, and a stock minus a stock is a CHANGE. Changes happened, so they add. This
    is why the rule cannot simply be "refuse a sum with a level anywhere in it".
    """
    definition = _metric(field="attr:CountedQuantity", minus="attr:BalanceQuantity")
    assert _about_levels(_problems(definition)) == []


def test_adding_up_a_level_minus_an_amount_is_refused():
    """A stock minus a flow is neither, so the total means nothing at all."""
    problems = _about_levels(_problems(
        _metric(field="attr:BalanceQuantity", minus="attr:QuantityPicked")))
    assert len(problems) == 1
    assert "BalanceQuantity" in problems[0]


def test_adding_up_an_amount_minus_a_level_is_refused_and_says_which_half():
    """The same mistake the other way round. Naming the half is what makes it fixable."""
    problems = _about_levels(_problems(
        _metric(field="attr:QuantityPicked", minus="attr:BalanceQuantity")))
    assert len(problems) == 1
    assert "BalanceQuantity" in problems[0]


def test_averaging_the_difference_of_a_level_and_an_amount_is_allowed():
    """Only the SUM is refused. An average of a difference adds nothing across rows as its answer."""
    definition = _metric(aggregation=d.Aggregation.average,
                         field="attr:QuantityPicked", minus="attr:BalanceQuantity")
    assert _about_levels(_problems(definition)) == []


# ==================================================================== 5. how a level is matched

def test_a_level_is_matched_by_key_not_by_the_attr_prefix():
    """A metric addresses a field as `attr:resp.QuantityOnHand`; the meaning row stores
    `resp.QuantityOnHand`. Comparing the two spellings directly would match nothing and refuse
    nothing, which is a silent failure rather than a loud one."""
    assert len(_about_levels(_problems(_metric(field="attr:resp.QuantityOnHand")))) == 1


def test_marking_one_namespace_does_not_mark_the_bare_name():
    """`resp.QuantityOnHand` being a level says nothing about a request field spelled
    `QuantityOnHand`. Matching on the bare name would let an unmarked field inherit a marking it never
    got, which is the same reasoning `approved_attributes` already carries."""
    assert _about_levels(_problems(_metric(field="attr:QuantityOnHand"),
                                   approved=APPROVED | {"QuantityOnHand"})) == []


def test_a_typed_column_is_never_a_level():
    """`quantity` is a column on the fact row, not an attribute, so no meaning row can describe it and
    the seeded `consumption` metric that sums it can never be caught by this rule."""
    definition = _metric(field="quantity", method_filter=("ConfirmPickLine",))
    assert _about_levels(_problems(definition)) == []


def test_a_level_on_a_record_metric_is_refused_too():
    """Record facts carry stock readings as readily as transaction facts do, and the arithmetic does
    not change because the row came from a different table."""
    definition = _metric(field="attr:rec.STQT", source="record", dimensions=("attr:rec.ITNO",))
    problems = _about_levels(_problems(definition, levels={"rec.STQT": "level"}))
    assert len(problems) == 1


# ==================================================================== 6. the module stays pure

def test_the_rule_needs_no_database():
    """`validate` is handed a map of names, exactly as it is handed `known_attributes`. It never learns
    that `analytics_field_meanings` exists, which is what keeps the whole module testable without one.
    """
    import inspect
    source = inspect.getsource(d)
    assert "AnalyticsFieldMeaning" not in source
    assert "field_kinds" in inspect.signature(d.validate).parameters
