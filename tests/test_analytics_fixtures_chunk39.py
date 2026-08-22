"""Chunk 39, Phase 0: the nine fixtures, and the proof that the range diff is necessary.

The plan's most expensive decision is that the analytics worker must compare a RANGE rather than
upsert per record. Verification item 4 states the reason:

    Merge and split: a merged record's vanished id reverses, a split's new id applies.
    A per-record update passes test 3 and fails this one.

Phase 0 is the right place to prove that rather than trust it, because everything built afterwards
assumes it. So this file implements BOTH strategies as reference code and runs them over all nine
fixtures. The per-id one is not a straw man: it is the obvious, reasonable implementation that anyone
would reach for first, and it gets seven of the nine right.

The two it gets wrong are wrong SILENTLY. No exception, no mismatch, just a total that is permanently
too large -- and once the raw entries are dropped at 60 days, nothing is left to recount against.
"""

from decimal import Decimal

import pytest

from tests.analytics_fixtures import FIXTURES, BY_NAME, NON_QUANTITY_ROWS, fact
from app.services.analytics import contract as c
from app.services.analytics import definition as d


# ==================================================== the two strategies
# `stored` models `analytics_facts`: one row per transaction, keyed by its source id. Both strategies
# take the stored state plus the current truth for the range, and return the new stored state. What the
# rollups then show is `fold` over that, so a strategy that leaks a row leaks it into every total.

def range_diff(stored: dict, source: tuple[dict, ...]) -> dict:
    """The design. Compare the whole range: reverse what is no longer there, apply what is new.

    A vanished id is handled by the same branch that handles a merge, a split and a delete, without any
    of them being special-cased -- which is the argument for the range diff in one line.
    """
    incoming = {r["source_transaction_id"]: r for r in source}
    out = {}
    for txn_id, row in incoming.items():
        old = stored.get(txn_id)
        if old is not None and old["source_version_hash"] == row["source_version_hash"]:
            out[txn_id] = old          # fingerprint matches: no write at all
        else:
            out[txn_id] = row          # new, or changed: reverse old and apply new
    # Anything stored but absent from the source is REVERSED by simply not carrying it forward.
    return out


def per_id_update(stored: dict, source: tuple[dict, ...]) -> dict:
    """The obvious alternative: upsert each incoming record by its id.

    Correct for every case where a transaction keeps its identity. Blind to any id that disappeared,
    because nothing ever asks about ids the source no longer mentions.
    """
    out = dict(stored)
    for row in source:
        out[row["source_transaction_id"]] = row
    return out


def _stored(rows) -> dict:
    return {r["source_transaction_id"]: r for r in rows}


def _total(stored: dict) -> Decimal:
    return d.fold(list(stored.values()), d.CONSUMPTION)["quantity"][d.Role.sum_value]


def _truth(fixture) -> Decimal:
    """What the total SHOULD be: fold the source of truth directly."""
    return d.fold(list(fixture.after), d.CONSUMPTION)["quantity"][d.Role.sum_value]


# ==================================================== the nine exist and are what the plan named
def test_all_nine_named_fixtures_are_present():
    """Named in the plan: zero pick, short pick, error, incomplete, late backfill, rebuild, merge,
    split, and a multi-confirmation line whose ExpectedQuantity changes. Plus an unchanged rebuild,
    which is the no-op path the fingerprint exists for."""
    for name in ("zero pick", "short pick", "error", "incomplete", "late backfill", "rebuild",
                 "merge", "split", "multi-confirmation, ExpectedQuantity changes"):
        assert name in BY_NAME, f"the plan names {name!r} and it is missing"
    assert len(FIXTURES) == 10, "the nine named, plus the unchanged-rebuild no-op case"


def test_every_fixture_says_what_it_catches():
    """A fixture whose purpose is not written down gets deleted by the next person who finds it
    inconvenient."""
    for f in FIXTURES:
        assert len(f.catches) > 80, f"{f.name} needs a real explanation"


# ==================================================== the range diff is correct for all nine
@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda f: f.name)
def test_the_range_diff_reproduces_the_truth_for_every_fixture(fixture):
    got = _total(range_diff(_stored(fixture.before), fixture.after))
    assert got == _truth(fixture), f"{fixture.name}: {fixture.catches}"


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda f: f.name)
def test_the_range_diff_is_idempotent(fixture):
    """Applying the same range twice must change nothing the second time. This is what makes a retry
    free, and a retry happens on every one of the 98.7% of rows that get rebuilt."""
    once = range_diff(_stored(fixture.before), fixture.after)
    twice = range_diff(once, fixture.after)
    assert once == twice
    assert _total(once) == _total(twice)


# ==================================================== and a per-id update is not
@pytest.mark.parametrize("name", ["merge", "split"])
def test_a_per_id_update_gets_merge_and_split_silently_wrong(name):
    """The whole justification for the range diff, as an executable assertion.

    Merge: two ids become one, so 2.0 + 3.0 + 5.0 = 10.0 is reported where the truth is 5.0.
    Split: one becomes two, so 7.0 + 4.0 + 3.0 = 14.0 is reported where the truth is 7.0.

    Both are exactly double. Neither raises.
    """
    f = BY_NAME[name]
    truth = _truth(f)
    naive = _total(per_id_update(_stored(f.before), f.after))
    assert naive != truth, "if this ever passes, the range diff is no longer justified"
    assert naive == truth * 2, f"{name} double-counts: reported {naive}, truth {truth}"
    assert f.vanished, "merge and split are exactly the cases where an id disappears"


def test_a_per_id_update_passes_the_rebuild_fixture():
    """Why the defect is dangerous rather than obvious: the naive strategy is CORRECT for the ordinary
    case. A test suite containing only 'rebuild' would sign it off."""
    f = BY_NAME["rebuild"]
    assert _total(per_id_update(_stored(f.before), f.after)) == _truth(f)


def test_the_per_id_update_is_wrong_on_exactly_the_fixtures_with_a_vanished_id():
    """Stated as a property rather than a list, so a fixture added later is covered automatically."""
    for f in FIXTURES:
        naive_ok = _total(per_id_update(_stored(f.before), f.after)) == _truth(f)
        assert naive_ok == (not f.vanished), (
            f"{f.name}: vanished ids {f.vanished or 'none'}, naive strategy "
            f"{'passed' if naive_ok else 'failed'}")


# ==================================================== individual fixture semantics
def test_a_zero_pick_counts_as_an_attempt_and_contributes_no_units():
    after = BY_NAME["zero pick"].after
    folded = d.fold(list(after), d.CONSUMPTION)
    assert folded["quantity"][d.Role.sum_value] == Decimal(0)
    assert folded["pick_count"][d.Role.count_value] == 0
    assert folded["attempt_count"][d.Role.count_value] == 1
    assert d.zero_pick_rate(folded) == Decimal(1), "one confirmation, nothing picked"


def test_a_short_pick_is_a_pick_of_what_was_actually_taken():
    folded = d.fold(list(BY_NAME["short pick"].after), d.CONSUMPTION)
    assert folded["quantity"][d.Role.sum_value] == Decimal("12")
    assert folded["pick_count"][d.Role.count_value] == 1


@pytest.mark.parametrize("name", ["error", "incomplete"])
def test_an_error_or_incomplete_row_lands_in_no_counter_at_all(name):
    """Their quantity is unknown, not zero. Folding either in as a zero-unit attempt would drag the
    zero-pick rate and invent a confirmation that never happened."""
    folded = d.fold(list(BY_NAME[name].after), d.CONSUMPTION)
    assert folded["quantity"][d.Role.sum_value] == Decimal(0)
    assert folded["attempt_count"][d.Role.count_value] == 0
    assert d.zero_pick_rate(folded) is None, "no confirmations means the rate is undefined"


def test_a_late_backfill_adds_the_older_row_without_disturbing_the_newer_one():
    f = BY_NAME["late backfill"]
    after = range_diff(_stored(f.before), f.after)
    assert set(after) == {"t-old", "t-new"}
    assert _total(after) == Decimal("7")


def test_an_unchanged_rebuild_carries_the_stored_row_forward_untouched():
    """The fingerprint no-op. The returned object must be the ORIGINAL, not an equal copy: that
    identity is what proves nothing was rewritten."""
    f = BY_NAME["rebuild, unchanged"]
    stored = _stored(f.before)
    out = range_diff(stored, f.after)
    assert out["t-same"] is stored["t-same"], "a matching fingerprint must write nothing"


def test_a_changed_rebuild_reverses_the_old_contribution_exactly_once():
    f = BY_NAME["rebuild"]
    assert _total(range_diff(_stored(f.before), f.after)) == Decimal("3"), "not 5, and not 8"


def test_the_multi_confirmation_fixture_sums_picked_never_expected():
    """ExpectedQuantity is mutable per instruction and is NOT an order-line total, so it is not a
    measure. Only the picked quantities compose, and they are fractional."""
    f = BY_NAME["multi-confirmation, ExpectedQuantity changes"]
    out = range_diff(_stored(f.before), f.after)
    assert _total(out) == Decimal("1.000000"), "0.333333 + 0.666667, exact under Decimal"


def test_the_fixtures_include_rows_from_a_method_with_no_quantity():
    """46 of 49 methods. They must be foldable without contributing to a quantity counter, or the
    design is quantity-shaped and 96% of the traffic has no metrics."""
    folded = d.fold(list(NON_QUANTITY_ROWS), d.CONSUMPTION)
    assert folded["attempt_count"][d.Role.count_value] == 0
    volume = d.MetricDefinition(name="volume", dimensions=("method",),
                                measures=(d.Measure("n", d.Aggregation.count),), grains=("daily",))
    assert d.fold(list(NON_QUANTITY_ROWS), volume)["n"][d.Role.count_value] == 2


# ==================================================== the fixtures are usable by the generator
def test_fact_rows_carry_every_field_the_diff_and_the_fold_need():
    """The synthetic generator will emit these same shapes at 100x load, so the shape has to be
    complete here rather than patched there."""
    for f in FIXTURES:
        for row in f.before + f.after:
            for required in ("source_transaction_id", "source_version_hash", "method",
                             "quantity", "quantity_classification"):
                assert required in row, f"{f.name}: {required} missing"
            assert row["quantity_classification"] in set(c.Classification)


def test_a_fixture_row_uses_the_real_classifier_rather_than_a_hand_written_label():
    """Otherwise the fixtures could drift from the contract and still pass."""
    row = fact("x", "ConfirmPickLine", "0.0")
    assert row["quantity_classification"] is c.Classification.attempt
    assert fact("y", "ListPickLines", None)["quantity_classification"] is c.Classification.non_quantity
