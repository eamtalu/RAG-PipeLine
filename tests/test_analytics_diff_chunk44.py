"""Chunk 44, Phase 3b: the range diff. The most expensive decision in the plan, and its evidence.

    Range diff, never per-record update. ... A merged record's vanished id reverses, a split's new id
    applies. A per-record update passes test 3 and fails this one.

Phase 0 proved the STRATEGY with a reference implementation living in a test file. This proves the real
module, and adds what a reference dict could not express: outcomes that drive writes, and the dirty
buckets that drive the rollups. (An earlier design moved rollups by signed deltas; the rollups have
recomputed dirty buckets from the fact table since Phase 3c, and the delta helper was removed on
13 Sep 2026. The net-change checks below now go through the applied state, which is what the fold reads.)

The property everything rests on: **a contribution is reversed exactly once.** Not zero times (the total
keeps a vanished row forever) and not twice (the total goes negative). Both failures are silent, which is
why they are asserted over all ten fixtures rather than spot-checked.

Two things here are load-bearing and not obvious.

**Identity is `(source_transaction_id, event_time)`, never the id alone** (F3). `log_transactions`
enforces `UNIQUE NULLS NOT DISTINCT (id, started_at)` because `started_at` is the partition key and is
nullable, so two rows can share an id in different partitions. A diff keyed on the id alone would merge
them and lose one.

**The diff only ever touches keys whose `event_time` is inside the range it was given, on BOTH sides.**
That is what makes it idempotent and order-independent, which in turn is what lets a ticket be retried
and lets two adjacent day-chunks be processed in either order without one undoing the other.
"""

from decimal import Decimal

import pytest

from app.services.analytics import definition as d
from app.services.analytics import diff as dd
from tests.analytics_fixtures import BY_NAME, FIXTURES, NON_QUANTITY_ROWS


def total(rows) -> Decimal:
    return d.fold(list(rows), d.CONSUMPTION)["quantity"][d.Role.sum_value]


def applied(stored, source) -> list[dict]:
    """The stored state after applying the diff -- what the fact table would then hold."""
    out = {dd.key_of(r): r for r in stored}
    for o in dd.diff(stored, source):
        if o.action is dd.Action.reverse:
            out.pop(o.key, None)
        elif o.action is not dd.Action.unchanged:
            out[o.key] = o.fact
    return list(out.values())


def net_change(stored, source) -> Decimal:
    """How far the total moves once the diff is applied: what a recomputed rollup would show minus what
    it showed before. The rollups read the applied fact table, so this is the number they see."""
    return total(applied(stored, source)) - total(stored)


# ==================================================== correctness over all ten fixtures
@pytest.mark.parametrize("f", FIXTURES, ids=lambda f: f.name)
def test_the_diff_reproduces_the_truth_for_every_fixture(f):
    """The whole point, stated once. `after` IS the truth, so the stored state the diff produces must
    fold to the same total as folding `after` directly."""
    assert total(applied(f.before, f.after)) == total(f.after), f"{f.name}: {f.catches}"


@pytest.mark.parametrize("f", FIXTURES, ids=lambda f: f.name)
def test_applying_the_diff_moves_the_total_by_exactly_the_right_amount(f):
    """The rollups recompute dirty buckets from the applied fact table, so the applied state must move
    the total by exactly the difference between before and after - a diff that double-counts one row
    corrupts every bucket while the fact table looks correct."""
    assert total(f.before) + net_change(f.before, f.after) == total(f.after), f.catches


@pytest.mark.parametrize("f", FIXTURES, ids=lambda f: f.name)
def test_the_diff_is_idempotent(f):
    """A retried ticket must be free. Tickets stay open on failure and are retried, so a diff that
    applied twice moved the total twice would corrupt totals precisely when something went wrong."""
    once = applied(f.before, f.after)
    second = dd.diff(once, f.after)
    assert all(o.action is dd.Action.unchanged for o in second), \
        f"{f.name}: second pass wrote {[o.action.value for o in second if o.writes]}"
    assert net_change(once, f.after) == 0


@pytest.mark.parametrize("f", FIXTURES, ids=lambda f: f.name)
def test_the_diff_does_not_depend_on_input_order(f):
    """The worker reads both sides with no ORDER BY, so PostgreSQL may hand them over in any order.
    An order-dependent diff would be intermittently wrong, which is the worst way to be wrong."""
    a = applied(f.before, f.after)
    b = applied(tuple(reversed(f.before)), tuple(reversed(f.after)))
    assert total(a) == total(b)
    assert {dd.key_of(r) for r in a} == {dd.key_of(r) for r in b}


# ==================================================== the two fixtures that justify the design
@pytest.mark.parametrize("name", ["merge", "split"])
def test_a_vanished_id_is_reversed(name):
    """The argument for the range diff in one assertion. A per-id upsert never asks about an id the
    source no longer mentions, so the departed row's contribution stays in the total permanently and
    nothing raises."""
    f = BY_NAME[name]
    outcomes = dd.diff(f.before, f.after)
    reversed_ids = {o.key[0] for o in outcomes if o.action is dd.Action.reverse}
    assert reversed_ids == f.vanished, f"{name}: {f.catches}"
    assert reversed_ids, "this fixture exists because an id vanishes"


def test_merge_lands_on_five_not_ten():
    """Concrete numbers, because "double" is exactly what a per-id upsert produces here: 2 + 3 stay in
    the total and the merged 5 is added on top."""
    f = BY_NAME["merge"]
    assert total(f.before) == Decimal("5")
    assert total(applied(f.before, f.after)) == Decimal("5"), "not 10"


def test_split_reverses_one_and_applies_two():
    f = BY_NAME["split"]
    actions = [o.action for o in dd.diff(f.before, f.after)]
    assert actions.count(dd.Action.reverse) == 1
    assert actions.count(dd.Action.insert) == 2
    assert total(applied(f.before, f.after)) == Decimal("7")


def test_a_changed_rebuild_reverses_the_old_contribution_exactly_once():
    f = BY_NAME["rebuild"]
    outcomes = dd.diff(f.before, f.after)
    assert [o.action for o in outcomes] == [dd.Action.update]
    assert net_change(f.before, f.after) == Decimal("-2"), "5 becomes 3: down two, not down five or up three"


# ==================================================== the no-op path, which must be free
def test_an_unchanged_fingerprint_writes_nothing_at_all():
    """Invariant 6. 98.7% of transactions are rewritten after their first write, so if a recheck that
    changed nothing is not free the worker produces a constant stream of pointless aggregate writes."""
    f = BY_NAME["rebuild, unchanged"]
    outcomes = dd.diff(f.before, f.after)
    assert [o.action for o in outcomes] == [dd.Action.unchanged]
    assert not any(o.writes for o in outcomes)
    assert net_change(f.before, f.after) == 0


def test_a_whole_unchanged_range_produces_no_writes():
    rows = list(NON_QUANTITY_ROWS) + list(BY_NAME["merge"].before)
    outcomes = dd.diff(rows, rows)
    assert not any(o.writes for o in outcomes)


def test_unchanged_carries_the_stored_row_forward_rather_than_the_incoming_one():
    """The stored row holds bookkeeping the incoming one does not: its revision, and its `created_at`,
    which is what F6's retention cursor reads. Replacing it with an identical-looking incoming row
    would reset both and drag the cursor backwards."""
    stored = [{**BY_NAME["rebuild, unchanged"].before[0], "revision": 7}]
    source = [{**BY_NAME["rebuild, unchanged"].after[0], "revision": 1}]
    out = applied(stored, source)
    assert out[0]["revision"] == 7


# ==================================================== identity is both columns (F3)
def test_two_rows_sharing_an_id_at_different_event_times_are_distinct():
    """`log_transactions` allows this: `started_at` is the partition key and is nullable, so the source
    uniqueness is (id, started_at) and two partitions can each hold the id. Keying the diff on the id
    alone would silently merge them, and zero such pairs existing today is exactly when the extra
    column is free."""
    from datetime import datetime, timezone
    a = {"source_transaction_id": "same", "event_time": datetime(2026, 8, 5, tzinfo=timezone.utc),
         "source_version_hash": "h1", "method": "ConfirmPickLine", "quantity": Decimal("2"),
         "quantity_classification": "pick", "status": "success"}
    b = {**a, "event_time": datetime(2026, 8, 6, tzinfo=timezone.utc)}
    outcomes = dd.diff([], [a, b])
    assert len(outcomes) == 2, "one row would mean the second overwrote the first"
    assert total(applied([], [a, b])) == Decimal("4")


def test_a_null_event_time_is_a_usable_key():
    """A transaction all of whose entries lack a parsable timestamp has no instant and lives in the
    DEFAULT partition. It still has to be diffed, so None must be a key the diff can hold rather than
    a row it drops."""
    row = {"source_transaction_id": "t-null", "event_time": None, "source_version_hash": "h",
           "method": "ConfirmPickLine", "quantity": Decimal("3"), "quantity_classification": "pick",
           "status": "success"}
    outcomes = dd.diff([], [row])
    assert [o.action for o in outcomes] == [dd.Action.insert]
    assert total(applied([], [row])) == Decimal("3")


def test_an_event_time_that_moved_is_a_reversal_plus_an_insert():
    """A rebuild can absorb an earlier entry and move a transaction's start instant. `event_time` is the
    partition key, so this is not an update in place -- the old row must reverse and a new one apply, or
    the fact table would hold the same transaction twice."""
    from datetime import datetime, timezone
    old = {"source_transaction_id": "t-moved", "event_time": datetime(2026, 8, 5, 10, tzinfo=timezone.utc),
           "source_version_hash": "h1", "method": "ConfirmPickLine", "quantity": Decimal("6"),
           "quantity_classification": "pick", "status": "success"}
    new = {**old, "event_time": datetime(2026, 8, 5, 9, 59, tzinfo=timezone.utc),
           "source_version_hash": "h2"}
    outcomes = dd.diff([old], [new])
    assert {o.action for o in outcomes} == {dd.Action.reverse, dd.Action.insert}
    assert total([old]) == Decimal("6"), "the fold must actually see these rows"
    assert net_change([old], [new]) == 0, "same quantity, so the total must not move"
    assert len(applied([old], [new])) == 1, "and the transaction must not be held twice"


# ==================================================== both sides are carried
def test_an_update_carries_the_stored_row_so_the_old_bucket_can_be_dirtied():
    """The rollups recompute every bucket a change touched. When a rebuild moves a fact, the bucket it
    LEFT is known only from the stored row, so an update outcome must carry it alongside the new one."""
    f = BY_NAME["rebuild"]
    (outcome,) = dd.diff(f.before, f.after)
    assert outcome.action is dd.Action.update
    assert outcome.stored["quantity"] == Decimal("5"), "the OLD version"
    assert outcome.fact["quantity"] == Decimal("3"), "the NEW version"


def test_a_diff_of_nothing_is_nothing():
    assert dd.diff([], []) == []


# ==================================================== nothing is invented
def test_the_diff_reports_one_outcome_per_key_and_no_more():
    f = BY_NAME["multi-confirmation, ExpectedQuantity changes"]
    outcomes = dd.diff(f.before, f.after)
    keys = [o.key for o in outcomes]
    assert len(keys) == len(set(keys)), "a duplicated key would apply a contribution twice"
    expected = {dd.key_of(r) for r in f.before} | {dd.key_of(r) for r in f.after}
    assert set(keys) == expected, "the diff must consider exactly the union of both sides"


def test_an_empty_range_on_both_sides_does_nothing():
    assert dd.diff([], []) == []


def test_everything_stored_going_away_reverses_all_of_it():
    """The date-range delete and the full wipe both produce exactly this shape: a ticket whose range
    now holds no transactions at all. If it did not reverse, the deleted rows' contribution would stay
    in every total permanently -- leak F12."""
    stored = list(BY_NAME["merge"].before)
    outcomes = dd.diff(stored, [])
    assert all(o.action is dd.Action.reverse for o in outcomes)
    assert net_change(stored, []) == -total(stored)
    assert applied(stored, []) == []
