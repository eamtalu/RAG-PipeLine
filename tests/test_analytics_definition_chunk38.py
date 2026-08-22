"""Chunk 38, Phase 0: the metric registry's shape, and consumption as ONE row in it.

The doc is emphatic, in bold: *"nothing about dimensions or measures may be hardcoded into a rollup
schema"*, and N4 requires *"a registry, not an if-chain"*. So the three consumption counters are not
constants anywhere; they are the measures of one `MetricDefinition`, and everything that folds or
validates reads them off the definition.

The structural decision this file pins is that **a rollup stores additive ROLES, never finished
answers** (invariant 8). The complete set of additive primitives is small and already fixed by the
doc's own composition table: sum, count, sum of squares, min, max, and a histogram. Every legal measure
decomposes into exactly those, so the rollup columns are named for them.

Why that matters more than it looks: with role-named columns you *cannot* store an average, because no
column accepts one. Today "never store a finished answer" is a rule someone has to catch in review.
Here it is unrepresentable. `average` is therefore an aggregation that DECLARES sum+count and is
divided at read time.

The cost, chosen deliberately: consumption needs one sum and two counts, which does not fit one set of
role columns, so a rollup row is keyed per (definition, measure). Consumption emits three rows per
bucket rather than one.
"""

from decimal import Decimal

import pytest

from app.services.analytics import contract as c
from app.services.analytics import definition as d


# ==================================================== the additive primitives
def test_the_roles_are_exactly_the_additive_primitives_the_doc_allows():
    """Sums and counts direct; averages as sum+count; variance as sum, sum_sq, count; percentiles as a
    20-bucket log histogram; first and last as min and max. Nothing else composes, so nothing else is a
    role."""
    assert {r.value for r in d.Role} == {
        "sum_value", "count_value", "sum_sq", "min_value", "max_value", "histogram"}


def test_no_role_can_hold_a_finished_answer():
    """The whole point of naming the columns by role. `average`, `rate`, `p95`, `stddev` and `median` do
    not compose, so if one were a role a rollup could store it and the yearly figure would be the
    average of twelve monthly averages."""
    for forbidden in ("average", "mean", "rate", "median", "p95", "stddev", "percentile"):
        assert forbidden not in {r.value for r in d.Role}, f"{forbidden} is not additive"


@pytest.mark.parametrize("aggregation,roles", [
    (d.Aggregation.sum, {d.Role.sum_value, d.Role.count_value}),
    (d.Aggregation.count, {d.Role.count_value}),
    (d.Aggregation.average, {d.Role.sum_value, d.Role.count_value}),
    (d.Aggregation.stats, {d.Role.sum_value, d.Role.count_value, d.Role.sum_sq}),
    (d.Aggregation.extent, {d.Role.min_value, d.Role.max_value}),
    (d.Aggregation.percentile, {d.Role.histogram}),
])
def test_each_aggregation_declares_the_roles_it_needs(aggregation, roles):
    """This mapping is the doc's composition table, executable. An average declares sum+count and is
    divided at READ time, which is what makes it composable across grains."""
    assert d.roles_for(aggregation) == roles


# ==================================================== consumption is one registry row
def test_consumption_is_a_definition_instance_not_a_module_constant():
    assert isinstance(d.CONSUMPTION, d.MetricDefinition)
    assert d.CONSUMPTION.name == "consumption"


def test_consumption_carries_the_three_counters_f8_requires():
    """F8: `quantity` (sum of units), `pick_count` (confirmations above zero), `attempt_count` (all
    confirmations). Read off the definition, so a second definition can carry entirely different ones."""
    assert [m.name for m in d.CONSUMPTION.measures] == ["quantity", "pick_count", "attempt_count"]


def test_consumption_aggregates_by_both_method_and_transaction_name():
    """`method` is the API level (49 values), `transaction_name` the operator's screen (22). The doc
    aggregates by both because each answers a question the other cannot."""
    assert d.CONSUMPTION.dimensions == ("method", "transaction_name")


def test_consumption_is_filtered_to_the_methods_that_actually_carry_a_quantity():
    assert set(d.CONSUMPTION.method_filter) == set(c.QUANTITY_FIELD)


def test_a_definition_starts_as_draft_because_it_has_no_history_until_backfilled():
    """N4: "a definition cannot go active until its backfill has run, or its chart shows a false start
    date"."""
    assert d.CONSUMPTION.status is d.Status.draft


# ==================================================== validation N4 must enforce
def test_a_dimension_that_is_not_on_the_fact_row_is_rejected():
    """N4: "dimensions must exist on the fact row". A dimension nobody writes produces a chart that is
    silently empty rather than an error."""
    bad = d.MetricDefinition(name="bad", dimensions=("nonexistent_column",),
                             measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    problems = d.validate(bad)
    assert any("nonexistent_column" in p for p in problems)


def test_every_consumption_dimension_and_measure_field_exists_on_the_fact_row():
    assert d.validate(d.CONSUMPTION) == []
    for dim in d.CONSUMPTION.dimensions:
        assert dim in c.FACT_FIELDS
    for m in d.CONSUMPTION.measures:
        if m.field:
            assert m.field in c.FACT_FIELDS


def test_a_quantity_measure_is_refused_where_the_methods_carry_no_quantity():
    """N4's first validation rule. 46 of 49 methods have no quantity, so summing one over them yields a
    confident zero rather than an error."""
    bad = d.MetricDefinition(
        name="picks-on-a-listing-call", dimensions=("method",),
        measures=(d.Measure("qty", d.Aggregation.sum, field="quantity"),),
        grains=("daily",), method_filter=("ListPickLines",))
    assert any("quantity" in p for p in d.validate(bad))


def test_an_unknown_grain_is_rejected():
    bad = d.MetricDefinition(name="b", dimensions=("method",),
                             measures=(d.Measure("n", d.Aggregation.count),), grains=("fortnightly",))
    assert any("fortnightly" in p for p in d.validate(bad))


def test_weekly_is_a_legal_grain_even_though_it_has_no_table():
    """The doc derives ISO Monday weeks from daily at read time, so a definition may ask for weekly."""
    ok = d.MetricDefinition(name="w", dimensions=("method",),
                            measures=(d.Measure("n", d.Aggregation.count),), grains=("weekly",))
    assert d.validate(ok) == []


# ==================================================== folding is definition-driven
def _rows():
    """Fact rows as the normaliser will hand them over: already classified, quantity already Decimal.

    `status` is present because a real fact always has one -- `log_transactions.status` is NOT NULL --
    and CONSUMPTION's measures now filter on it. An errored pick carries a QuantityPicked just like a
    successful one, so a row without a status is not a simpler fact, it is an impossible one."""
    def row(method, qty, cls, name="Pick", status="success"):
        return {"method": method, "transaction_name": name, "quantity": qty,
                "quantity_classification": cls, "status": status}
    return [
        row("ConfirmPickLine", Decimal("3.5"), c.Classification.pick),
        row("ConfirmPickLine", Decimal("0"), c.Classification.attempt),
        row("ConfirmPickLine", Decimal("2"), c.Classification.pick),
        row("ReportCount", Decimal("-2"), c.Classification.correction, "Count"),
        row("ListPickLines", None, c.Classification.non_quantity, "List"),
        row("ConfirmPickLine", None, c.Classification.unusable),
    ]


def test_folding_produces_one_bucket_of_roles_per_measure():
    """Not one row per definition. Consumption needs a sum and TWO counts, which cannot share one set
    of role columns, so the rollup is keyed per (definition, measure)."""
    got = d.fold(_rows(), d.CONSUMPTION)
    assert set(got) == {"quantity", "pick_count", "attempt_count"}
    assert got["quantity"][d.Role.sum_value] == Decimal("3.5"), "3.5 + 0 + 2 - 2, signed"
    assert got["pick_count"][d.Role.count_value] == 2
    assert got["attempt_count"][d.Role.count_value] == 4


def test_a_row_the_definition_filters_out_contributes_to_nothing():
    """`ListPickLines` is outside the method filter and `unusable` has no readable quantity. Neither may
    appear in any counter, and in particular neither may land in a denominator as a zero."""
    got = d.fold(_rows(), d.CONSUMPTION)
    assert got["attempt_count"][d.Role.count_value] == 4, "6 rows in, 2 excluded"


def test_folding_reads_the_measures_off_the_definition_not_a_constant():
    """The registry requirement. A different definition over the same rows yields different counters
    with no code change anywhere."""
    only_volume = d.MetricDefinition(
        name="volume", dimensions=("method",),
        measures=(d.Measure("txns", d.Aggregation.count),), grains=("daily",))
    got = d.fold(_rows(), only_volume)
    assert set(got) == {"txns"}
    assert got["txns"][d.Role.count_value] == 6, "no method filter, so every row counts"


def test_a_definition_can_measure_a_field_that_has_nothing_to_do_with_quantity():
    """Proof that the design is not quantity-shaped: 46 of 49 methods carry no quantity, and their
    measures are volume, duration, status and actor."""
    dur = d.MetricDefinition(
        name="slowest", dimensions=("method",),
        measures=(d.Measure("duration", d.Aggregation.extent, field="duration_ms"),),
        grains=("hourly",))
    rows = [{"method": "X", "duration_ms": 120}, {"method": "X", "duration_ms": 30},
            {"method": "X", "duration_ms": 900}]
    got = d.fold(rows, dur)
    assert got["duration"][d.Role.min_value] == 30
    assert got["duration"][d.Role.max_value] == 900


# ==================================================== additivity, the schema rule
def test_adding_two_folds_equals_folding_the_whole():
    """Invariant 8, as an assertion. This is what lets hourly compose into daily into monthly."""
    rows = _rows()
    whole = d.fold(rows, d.CONSUMPTION)
    halves = d.add(d.fold(rows[:3], d.CONSUMPTION), d.fold(rows[3:], d.CONSUMPTION))
    assert whole == halves


def test_add_is_uniform_across_roles_with_no_per_measure_logic():
    """The registry-not-if-chain requirement, structurally: sums add, counts add, mins take the min,
    maxes take the max, histograms add element-wise. Nothing consults which measure it is."""
    a = {d.Role.sum_value: Decimal(1), d.Role.count_value: 1, d.Role.sum_sq: Decimal(1),
         d.Role.min_value: 5, d.Role.max_value: 5, d.Role.histogram: (1, 0, 2)}
    b = {d.Role.sum_value: Decimal(2), d.Role.count_value: 3, d.Role.sum_sq: Decimal(4),
         d.Role.min_value: 2, d.Role.max_value: 9, d.Role.histogram: (0, 4, 1)}
    got = d.add_roles(a, b)
    assert got[d.Role.sum_value] == Decimal(3)
    assert got[d.Role.count_value] == 4
    assert got[d.Role.sum_sq] == Decimal(5)
    assert got[d.Role.min_value] == 2
    assert got[d.Role.max_value] == 9
    assert got[d.Role.histogram] == (1, 4, 3)


def test_adding_an_empty_fold_changes_nothing():
    """`empty` is the identity, so a rollup can start from nothing and a bucket with no rows is
    distinguishable from a bucket that was never folded."""
    got = d.fold(_rows(), d.CONSUMPTION)
    assert d.add(got, d.empty(d.CONSUMPTION)) == got


# ==================================================== finished answers are derived, never stored
def test_an_average_is_divided_at_read_time_from_stored_components():
    folded = {d.Role.sum_value: Decimal("10"), d.Role.count_value: 4}
    assert d.average(folded) == Decimal("2.5")


def test_an_average_over_nothing_is_none_rather_than_zero():
    """Zero reads as "the average was zero"; None says "there was nothing to average"."""
    assert d.average({d.Role.sum_value: Decimal(0), d.Role.count_value: 0}) is None


def test_the_zero_pick_rate_is_derived_from_two_stored_counters():
    """F8: the rate derives from pick_count and attempt_count, so it is never stored and always
    composes. Twelve monthly rates cannot be averaged into a yearly one; twelve pairs of counts can."""
    folded = d.fold([
        {"method": "ConfirmPickLine", "status": "success", "quantity": Decimal(1), "quantity_classification": c.Classification.pick},
        {"method": "ConfirmPickLine", "status": "success", "quantity": Decimal(0), "quantity_classification": c.Classification.attempt},
        {"method": "ConfirmPickLine", "status": "success", "quantity": Decimal(0), "quantity_classification": c.Classification.attempt},
        {"method": "ConfirmPickLine", "status": "success", "quantity": Decimal(5), "quantity_classification": c.Classification.pick},
    ], d.CONSUMPTION)
    assert d.zero_pick_rate(folded) == Decimal("0.5")


def test_the_zero_pick_rate_is_none_when_nothing_was_confirmed():
    assert d.zero_pick_rate(d.fold([], d.CONSUMPTION)) is None
