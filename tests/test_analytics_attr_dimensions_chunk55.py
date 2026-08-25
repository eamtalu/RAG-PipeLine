"""Chunk 55 (R1b of docs/analytics-ml-architecture/final_architecture.md, 18b/18c): a metric may name
a field inside `attributes`, so captured response values are measurable.

The gap R1b closes
------------------
`validate` rejects any dimension or measure that is not a typed fact column, checked against
`contract.FACT_FIELDS` (`definition.py:216` and `:226`). `attributes` is not in that set. And even if
it were, both read points take the value off the FLAT fact dict:

    definition.fold:297      value = row.get(m.field)
    rollups._dim_key:125     values = [row.get(name) for name in definition.dimensions]

so `row.get("resp.BaseUoM")` is `None` when the value lives nested under `row["attributes"]`.

Section 18a claimed the namespacing meant "fold's row.get(field) keeps working unchanged". True of
`fold`, false of the path: `validate` refuses the definition long before `fold` sees a row. Without
R1b, R3 captures response scalars that nothing can read - storage with no product.

The prefix is `attr:`
---------------------
`attr:resp.BaseUoM` reads `row["attributes"]["resp.BaseUoM"]`. A prefix rather than a bare name because
a bare `resp.BaseUoM` could not be told apart from a typed column that happened to contain a dot, and
because it makes the two read paths distinguishable at a glance in a stored definition.

Purity is preserved, deliberately
---------------------------------
`definition.py` has no database access - Phase 0 tests it without one, and giving it a database would
end that. So `validate` does NOT query `analytics_field_registry`. It takes the known attribute paths
as an argument, and the caller (the API) supplies them. The registry stays the authority without the
pure module learning about it.

The numeric hazard, which is the real find here
-----------------------------------------------
A typed measure field is already numeric: `quantity` is a `Decimal`, `duration_ms` an `int`. A JSONB
value is whatever the WMS logged, and the measured M3 records carry `"STQT": "624"` - a STRING. So
`bucket[Role.sum_value] += value` would raise `TypeError` on the first row, inside the fold, inside the
worker's transaction. Coercion is therefore part of resolution rather than an afterthought, and a value
that cannot be coerced is SKIPPED under the existing "absent is never zero" rule rather than crashing
the run or, worse, being counted as nothing.
"""

from decimal import Decimal

import pytest

from app.services.analytics import contract as c
from app.services.analytics import definition as d
from app.services.analytics import rollups as r5


def _row(**kw):
    base = {"method": "ConfirmPickLine", "transaction_name": "Full Stock Count",
            "status": "success", "quantity_classification": "pick", "attributes": {}}
    base.update(kw)
    return base


# =============================================================== 1. the resolver
def test_a_typed_column_resolves_as_before():
    """R1b must be additive. Every existing definition names typed columns, so those have to resolve
    through exactly the same path they did before, or R1b is a rewrite of the fold."""
    assert c.resolve_field(_row(item_number="101978"), "item_number") == "101978"


def test_an_attr_path_reads_from_attributes():
    assert c.resolve_field(_row(attributes={"resp.BaseUoM": "KG"}), "attr:resp.BaseUoM") == "KG"


def test_a_missing_attr_path_is_none_not_an_error():
    """Absent is never zero, and it is never an exception either. A response that happens not to carry
    a field on one call must not fail the whole window's fold."""
    assert c.resolve_field(_row(attributes={"resp.Other": 1}), "attr:resp.BaseUoM") is None
    assert c.resolve_field(_row(attributes={}), "attr:resp.BaseUoM") is None


def test_a_row_with_no_attributes_at_all_is_safe():
    """`attributes` is NOT NULL with a `{}` default in the schema, but a fact dict assembled in memory
    or read with a narrower column list may not carry the key at all."""
    row = {"method": "X"}
    assert c.resolve_field(row, "attr:resp.BaseUoM") is None


def test_the_prefix_is_not_stripped_from_a_typed_lookup():
    """A typed column called `attr_something` must not be mistaken for a prefixed path."""
    assert c.resolve_field(_row(attr_like="v"), "attr_like") == "v"


def test_is_attr_path_identifies_the_prefix():
    assert c.is_attr_path("attr:resp.BaseUoM") is True
    assert c.is_attr_path("item_number") is False
    assert c.is_attr_path("attr_like") is False


# =============================================================== 2. numeric coercion
@pytest.mark.parametrize("raw,expected", [
    ("624", Decimal("624")),          # the measured shape: M3 records carry quantities as STRINGS
    (624, Decimal("624")),
    (624.5, Decimal("624.5")),
    (Decimal("624.5"), Decimal("624.5")),
    ("  624  ", Decimal("624")),
    ("1974.0", Decimal("1974.0")),
])
def test_a_numeric_looking_value_coerces(raw, expected):
    """`bucket[Role.sum_value] += value` on a str raises TypeError inside the worker's transaction. The
    live M3 records carry `"STQT": "624"`, so this is the common case, not the edge case."""
    assert c.numeric_or_none(raw) == expected


@pytest.mark.parametrize("raw", ["KG", "", "  ", None, "N/A", [], {}, True])
def test_a_non_numeric_value_is_none_not_a_crash(raw):
    """A dimension-shaped value like "KG" landing in a measure must be skipped, not counted as zero and
    not raised. `True` is included on purpose: `bool` is a subclass of `int` in Python, so a naive
    coercion would silently fold a flag in as 1."""
    assert c.numeric_or_none(raw) is None


# =============================================================== 3. dimensions
def test_a_definition_can_group_by_an_attr_path():
    defn = d.MetricDefinition(
        name="by_uom", dimensions=("item_number", "attr:resp.BaseUoM"),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    assert d.validate(defn, known_attributes={"resp.BaseUoM"}) == []


def test_the_fold_groups_on_the_resolved_attr_value():
    """The point of the whole chunk: two rows differing only inside `attributes` must land in DIFFERENT
    rollup buckets. Before R1b both resolved to None and collapsed into one."""
    defn = d.MetricDefinition(
        name="by_uom", dimensions=("attr:resp.BaseUoM",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    kg = _row(attributes={"resp.BaseUoM": "KG"})
    ea = _row(attributes={"resp.BaseUoM": "EA"})
    assert r5._dim_key(kg, defn) != r5._dim_key(ea, defn)
    assert r5._dim_key(kg, defn)[0] == "KG"


def test_an_unresolvable_attr_dimension_is_null_not_a_collapse():
    """A row missing the field groups under NULL, exactly as a row with a NULL typed column does. It
    must not silently join the bucket of a row that HAS the field."""
    defn = d.MetricDefinition(
        name="by_uom", dimensions=("attr:resp.BaseUoM",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    assert r5._dim_key(_row(attributes={}), defn)[0] is None


# =============================================================== 4. measures
def test_a_measure_can_sum_an_attr_path():
    defn = d.MetricDefinition(
        name="on_hand", dimensions=("item_number",),
        measures=(d.Measure("total", d.Aggregation.sum, field="attr:resp.QuantityOnHand"),),
        grains=("daily",))
    assert d.validate(defn, known_attributes={"resp.QuantityOnHand"}) == []

    folded = d.fold([_row(attributes={"resp.QuantityOnHand": "1974.0"}),
                     _row(attributes={"resp.QuantityOnHand": 26})], defn)
    assert folded["total"][d.Role.sum_value] == Decimal("2000.0")


def test_a_non_numeric_measure_value_contributes_nothing():
    """Not zero, and not an exception. Same rule the typed path already applies to a NULL quantity:
    absent is never zero, so it must not reach a count either - a denominator drawn from rows that
    contributed no value would make every rate wrong in a plausible-looking way."""
    defn = d.MetricDefinition(
        name="on_hand", dimensions=("item_number",),
        measures=(d.Measure("total", d.Aggregation.sum, field="attr:resp.QuantityOnHand"),),
        grains=("daily",))
    folded = d.fold([_row(attributes={"resp.QuantityOnHand": "KG"}),
                     _row(attributes={"resp.QuantityOnHand": "10"})], defn)
    assert folded["total"][d.Role.sum_value] == Decimal("10")
    assert folded["total"][d.Role.count_value] == 1


def test_the_fold_does_not_raise_on_a_string_quantity():
    """The specific crash R1b would otherwise have introduced: a TypeError inside the fold, inside the
    worker's transaction, rolling back a whole window because one WMS field was a string."""
    defn = d.MetricDefinition(
        name="on_hand", dimensions=(),
        measures=(d.Measure("total", d.Aggregation.sum, field="attr:x"),), grains=("daily",))
    d.fold([_row(attributes={"x": "not a number"})], defn)   # must not raise


# =============================================================== 5. validation
def test_an_unknown_attr_path_is_refused():
    """The registry is the authority. A typo must still be refused, or a chart is silently empty - the
    exact failure `validate` exists to prevent for typed columns."""
    defn = d.MetricDefinition(
        name="typo", dimensions=("attr:resp.BaseUoMm",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    problems = d.validate(defn, known_attributes={"resp.BaseUoM"})
    assert problems and "resp.BaseUoMm" in problems[0]


def test_a_bare_unknown_column_is_still_refused():
    defn = d.MetricDefinition(
        name="bad", dimensions=("no_such_column",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    assert d.validate(defn) != []


def test_validate_stays_pure_and_needs_no_database():
    """`definition.py` is tested without a database and must stay that way, so `validate` takes the
    known paths as an argument rather than querying `analytics_field_registry`. The registry remains
    the authority; the pure module never learns it exists."""
    import inspect, io, tokenize
    # Checked against the CODE, with comments and docstrings stripped - the same lesson chunk 29
    # already learned. `validate`'s docstring has to EXPLAIN that the registry is the authority and
    # that it is passed in rather than queried; naming it in prose is the opposite of a violation.
    code_only = "".join(
        t.string for t in tokenize.generate_tokens(io.StringIO(inspect.getsource(d)).readline)
        if t.type not in (tokenize.COMMENT, tokenize.STRING))
    for forbidden in ("AsyncSession", "select", "AnalyticsFieldRegistry", "db"):
        assert forbidden not in code_only, f"definition.py must not touch the database: {forbidden}"
    assert "known_attributes" in inspect.signature(d.validate).parameters


def test_omitting_known_attributes_refuses_every_attr_path():
    """Fails CLOSED. A caller that forgets to supply the registry must not accidentally accept any
    attribute path at all - that would make the allowlist optional, which is the one thing it cannot
    be for a table that is KEEP_FOREVER."""
    defn = d.MetricDefinition(
        name="x", dimensions=("attr:resp.BaseUoM",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    assert d.validate(defn) != []


def test_a_known_attr_path_passes_when_supplied():
    defn = d.MetricDefinition(
        name="x", dimensions=("attr:resp.BaseUoM",),
        measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    assert d.validate(defn, known_attributes={"resp.BaseUoM"}) == []


# =============================================================== 6. the promotion invariant (decision C)
def test_a_promoted_column_and_its_attr_path_normalise_identically():
    """Decision C's one hard requirement (18e). Promotion COPIES rather than moves - `_FROM_ATTRIBUTES`
    puts `attributes` on the fact first and only then reads out of it - so for a while both paths
    resolve the same value. If they normalised differently by so much as trimming or case, the same
    base UoM would land in two rollup rows and one item's total would split in half."""
    promoted = _row(base_uom="KG", attributes={"resp.BaseUoM": "KG"})
    via_column = c.dimension_value(c.resolve_field(promoted, "base_uom"))
    via_attr = c.dimension_value(c.resolve_field(promoted, "attr:resp.BaseUoM"))
    assert via_column == via_attr == "KG"


def test_dimension_value_normalisation_is_shared_by_both_paths():
    """Asserted on whitespace and type because those are the two ways a JSONB value differs from a
    typed column holding "the same" thing."""
    assert c.dimension_value("  KG  ") == c.dimension_value("KG")
    assert c.dimension_value(624) == c.dimension_value("624")
    assert c.dimension_value(None) is None
    assert c.dimension_value("") is None
