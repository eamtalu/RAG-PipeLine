"""Chunk 46, Phase 3c: the status filter, and the measured defect it closes.

Found by running the pipeline end to end rather than by reading the fixtures, which is the only way it
could have been found: the fixtures asserted the right intent and modelled the wrong data.

    Fixture "error": "A hard failure carries no units. It must not appear in the quantity total, and
    must not be silently folded in as a zero-unit attempt either."

It modelled that row as `quantity=None`. The real projection does not produce that. `QuantityPicked` is
stated on the REQUEST line, and whether the pick errored is decided by what arrives afterwards -- so an
errored `ConfirmPickLine` carries a full quantity, and so does an `incomplete` one. Measured through the
real parser and stitcher:

    method             status      QuantityPicked   classification   summed into consumption
    ConfirmPickLine    success     10.0             pick             yes
    ConfirmPickLine    success     0.0              attempt          yes
    ConfirmPickLine    incomplete  10.0             pick             YES  <- wrong
    ConfirmPickLine    error       10.0             pick             YES  <- wrong

Nothing could express the rule. `Measure.only` filters classifications; `MetricDefinition.status` is the
lifecycle field (draft/active/inactive), not a row filter. Hence `Measure.statuses`, per measure for the
same reason `only` is per measure: one definition can then hold both a total and an error count.

The fact rows were never wrong, and are unchanged. A fact faithfully records `status=error,
quantity=10.0`; whether that counts toward consumption is a metric question, and metrics are registry
data. This is that data.
"""

from decimal import Decimal

import pytest

from app.persistence.models.log_transaction import LogTransactionStatus
from app.services.analytics import contract as c
from app.services.analytics import definition as d


def row(status: str, qty="10.0", *, method="ConfirmPickLine") -> dict:
    q = c.parse_quantity(qty)
    return {"method": method, "transaction_name": "Pick", "status": status, "quantity": q,
            "quantity_classification": c.classify(method, q)}


def summed(rows) -> Decimal:
    return d.fold(list(rows), d.CONSUMPTION)["quantity"][d.Role.sum_value]


def counted(rows, measure: str) -> int:
    return d.fold(list(rows), d.CONSUMPTION)[measure][d.Role.count_value]


# ==================================================== the defect, closed
def test_an_errored_pick_contributes_no_units():
    """The whole reason this chunk exists. It carries a quantity; it must not be counted."""
    assert summed([row("error")]) == Decimal(0)
    assert counted([row("error")], "pick_count") == 0
    assert counted([row("error")], "attempt_count") == 0


def test_an_incomplete_pick_contributes_no_units():
    """Its RESPONSE has not been ingested, so the quantity is unknown rather than confirmed. Excluding
    it is self-correcting: a later Stage 2 pass closes the transaction and it counts under its real
    status. Including it would report units that may never have moved."""
    assert summed([row("incomplete")]) == Decimal(0)


def test_a_successful_pick_still_counts_exactly_as_before():
    """The filter must not be a silent across-the-board reduction."""
    assert summed([row("success")]) == Decimal(10)
    assert counted([row("success")], "pick_count") == 1


def test_the_measured_four_row_case_now_totals_ten_not_thirty():
    """The end-to-end number that exposed this. Three picks of 10 plus a zero, of which only one pick
    and the zero are complete."""
    rows = [row("success", "10.0"), row("success", "0.0"),
            row("incomplete", "10.0"), row("error", "10.0")]
    assert summed(rows) == Decimal(10), "was 30 before the filter"
    assert counted(rows, "pick_count") == 1
    assert counted(rows, "attempt_count") == 2, "the successful pick and the successful zero"


# ==================================================== the rate stays coherent
def test_the_numerator_and_denominator_share_the_same_status_filter():
    """A denominator drawn from a wider set than its numerator makes the rate wrong in a way that looks
    entirely plausible: adding errored rows to attempt_count alone would drag the zero-pick rate down
    without changing a single pick."""
    quantity, pick, attempt = d.CONSUMPTION.measures
    assert pick.statuses == attempt.statuses == quantity.statuses


def test_the_zero_pick_rate_ignores_failures_on_both_sides():
    rows = [row("success", "0.0"), row("success", "5.0"),
            row("error", "0.0"), row("incomplete", "0.0")]
    folded = d.fold(rows, d.CONSUMPTION)
    assert d.zero_pick_rate(folded) == Decimal("0.5"), "one zero of two complete confirmations"


# ==================================================== the mechanism is generic, not a special case
def test_an_empty_status_set_means_every_status():
    """What a volume or an error-rate measure wants. 46 of 49 methods carry no quantity at all, and
    their measures are volume, duration, status and actor."""
    volume = d.MetricDefinition(
        name="volume", dimensions=("method",), grains=("daily",),
        measures=(d.Measure("transactions", d.Aggregation.count),))
    rows = [row(s) for s in ("success", "soft", "error", "incomplete")]
    assert counted_for(rows, volume, "transactions") == 4


def counted_for(rows, definition, measure) -> int:
    return d.fold(list(rows), definition)[measure][d.Role.count_value]


def test_one_definition_can_hold_both_a_total_and_an_error_count():
    """Why the filter is per MEASURE and not per definition. Per definition, an error rate would need
    two registry rows and two backfills to express one chart."""
    reliability = d.MetricDefinition(
        name="reliability", dimensions=("method",), grains=("daily",),
        measures=(d.Measure("total", d.Aggregation.count),
                  d.Measure("errors", d.Aggregation.count, statuses=frozenset({"error"}))))
    rows = [row("success"), row("success"), row("error"), row("soft")]
    assert counted_for(rows, reliability, "total") == 4
    assert counted_for(rows, reliability, "errors") == 1


def test_the_filter_is_data_not_code():
    """N4's requirement: a registry, not an if-chain. Inventing a metric that counts only incomplete
    transactions must need no code at all."""
    stuck = d.MetricDefinition(
        name="stuck", dimensions=("method",), grains=("daily",),
        measures=(d.Measure("stuck", d.Aggregation.count, statuses=frozenset({"incomplete"})),))
    assert d.validate(stuck) == []
    assert counted_for([row("incomplete"), row("success")], stuck, "stuck") == 1


# ==================================================== validation
def test_a_status_the_projection_never_emits_is_rejected():
    """It would contribute nothing and look exactly like no data, which is the failure mode the whole
    validate() function exists to prevent."""
    typo = d.MetricDefinition(
        name="typo", dimensions=("method",), grains=("daily",),
        measures=(d.Measure("n", d.Aggregation.count, statuses=frozenset({"succeeded"})),))
    problems = d.validate(typo)
    assert any("succeeded" in p for p in problems)


@pytest.mark.parametrize("status", ["success", "soft", "error", "incomplete"])
def test_every_real_status_validates(status):
    ok = d.MetricDefinition(
        name="ok", dimensions=("method",), grains=("daily",),
        measures=(d.Measure("n", d.Aggregation.count, statuses=frozenset({status})),))
    assert d.validate(ok) == []


def test_the_known_status_list_matches_the_model():
    """The pure module names the four statuses rather than importing the model, to keep its "no
    database" property. This is the test that stops the two drifting -- a status added to the enum and
    not here would be silently unusable in every metric."""
    assert d._STATUSES == {s.value for s in LogTransactionStatus}


# ==================================================== the seed definition's choice, stated
def test_consumption_counts_only_completed_transactions():
    assert d.CONSUMPTION.measures[0].statuses == frozenset({"success"})


def test_soft_is_excluded_and_that_is_a_recorded_decision_not_an_oversight():
    """`soft` means "M3 returned not-found/needs-value but the app coped". If the ERP returned
    not-found, the confirmation did not register in the system of record -- so it is excluded on a lack
    of evidence rather than a judgement.

    Measured: ZERO soft transactions carry a quantity-bearing method (69 exist, all on the other 46),
    so this choice moves no current number. That is precisely why it is safe to make it the strict one
    and revisit it with data."""
    assert "soft" not in d.CONSUMPTION.measures[0].statuses
    assert summed([row("soft")]) == Decimal(0)
    assert "soft" in d._STATUSES, "still a legal filter value for a metric that wants it"
