"""Chunk 63: bound the fold's work per run, so a backlog cannot exhaust the statement timeout.

FOUND IN PRODUCTION. 49 windows dead-lettered, then re-ticketed after the truncation fix, then failed
again with

    QueryCanceledError: canceling statement due to statement timeout

leaving ~32,400 facts unbuilt across five days. The truncation was one bug; this is a second,
independent one that the first was hiding.

Three causes, and only the third is a number
--------------------------------------------
1. `_coalesce` merged merely-ADJACENT tickets, not just overlapping ones. That destroyed
   `_MAX_TICKET_SPAN = 1 day` - eight daily tickets became one eight-day run in a single transaction.

2. `_read_response_entries` (R3) scaled with every transaction in the window rather than with the ones
   that changed: measured 22.8 s of a 23.7 s read, 96%.

3. The fold inherited the web tier's 30 s statement timeout, which is wrong for a background worker -
   Stage 1's bulk insert already relaxes it for exactly this reason.

Row-limiting is NOT available as a fix, which is why the bound has to be the window: the fold is a
range diff, so truncating the source read makes it conclude every fact past the cut has vanished and
REVERSE them. `_read_source`'s own comment says so.
"""

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.services.analytics import consume as n3
from app.settings import settings

T0 = datetime(2026, 8, 18, 7, 0, tzinfo=timezone.utc)
DAY = timedelta(days=1)


def _ticket(lo, hi):
    return AnalyticsPendingWindow(id=uuid.uuid4(), customer_code="c63",
                                 range_start=lo, range_end=hi)


# =============================================================== 1. merge, then split
#
# An earlier attempt at this fix tried to keep runs small by REFUSING to merge - strict overlap only.
# Two things killed that, and both are recorded here because the wrong version looked reasonable:
#
#   tickets are PADDED +/-900s (pending_windows explains why: invariant 2, a rebuild can move a
#   transaction's `started_at` outside the span of what was freed), so consecutive daily tickets
#   genuinely overlap by 30 minutes and merge under ANY comparison. The gap was the wrong lever.
#
#   merging is load-bearing for CORRECTNESS, not just efficiency. Chunk 45 says why: a transaction whose
#   rebuild moved it across a ticket boundary is reversed by one ticket and inserted by the next, and
#   merging puts both sides in one diff instead of leaving two facts transiently double-counted.
#
# So: merge for correctness, then SPLIT for bounded work - which is exactly Stage 2's shape, and this
# module had copied only the first half.

def test_overlapping_tickets_still_merge():
    """The correctness property, unchanged. Chunk 45 asserts it end to end; this pins the mechanism."""
    runs = n3._coalesce([_ticket(T0, T0 + DAY), _ticket(T0 + DAY / 2, T0 + 2 * DAY)],
                        gap=timedelta(0))
    assert len(runs) == 1


def test_touching_tickets_merge_too():
    """Nothing can fall strictly between two ranges that touch, so treating them as one loses nothing -
    and the bound comes from the split, not from refusing to merge."""
    assert len(n3._coalesce([_ticket(T0, T0 + DAY), _ticket(T0 + DAY, T0 + 2 * DAY)],
                            gap=timedelta(0))) == 1


def test_tickets_separated_by_a_real_gap_stay_separate():
    """The gap is now ZERO, not `2 * pad`. Two ranges with genuine empty time between them describe
    unrelated changes, and merging them would fold the quiet middle for no reason."""
    assert len(n3._coalesce([_ticket(T0, T0 + DAY), _ticket(T0 + 3 * DAY, T0 + 4 * DAY)],
                            gap=timedelta(0))) == 2


def test_the_gap_is_no_longer_two_pads():
    """Asserted on the source, because the defect was a PARAMETER - a correct-looking call that merged
    unrelated ranges and, with padding, made the run span unbounded."""
    import inspect
    src = inspect.getsource(n3)
    assert "_coalesce(tickets, gap=timedelta(0))" in src
    assert "gap=2 * _regroup_pad()" not in src


def test_eight_padded_daily_tickets_become_bounded_slices():
    """THE production case. Eight daily tickets, padded exactly as `publish` writes them, coalesce into
    ONE eight-day range - which is right - and must then execute as bounded slices rather than one
    transaction. One transaction is what exhausted the 30 s timeout and left 32,400 facts unbuilt."""
    from app.services.analytics.pending_windows import _pad
    pad = _pad()
    tickets = [_ticket(T0 + i * DAY - pad, T0 + (i + 1) * DAY + pad) for i in range(8)]
    runs = n3._coalesce(tickets, gap=timedelta(0))
    assert len(runs) == 1, "padded daily tickets genuinely overlap, so they merge - as they should"

    lo, hi, _ = runs[0]
    assert (hi - lo) > timedelta(days=7), "the merged range really is multi-day"
    slices = list(n3._split_run(lo, hi, settings.analytics_max_window_seconds))
    assert len(slices) >= 32, f"an 8-day range must split into many slices, got {len(slices)}"
    for a, b in slices:
        assert (b - a).total_seconds() <= settings.analytics_max_window_seconds


def test_the_slices_cover_the_range_with_no_gap():
    """A missed instant is a fact never folded. Contiguity is the whole safety condition of splitting."""
    slices = list(n3._split_run(T0, T0 + 3 * DAY, settings.analytics_max_window_seconds))
    assert slices[0][0] == T0 and slices[-1][1] == T0 + 3 * DAY
    for (_, end), (start, _) in zip(slices, slices[1:]):
        assert end == start, "a gap between slices would silently skip that time"


def test_a_short_range_is_a_single_slice():
    """Steady state. The normal incremental window is seconds wide, so splitting must add nothing to the
    common path."""
    assert len(list(n3._split_run(T0, T0 + timedelta(minutes=5),
                                  settings.analytics_max_window_seconds))) == 1


def test_the_window_bound_matches_stage_2():
    """One number to reason about rather than two that can drift. The two walk the same ranges."""
    assert settings.analytics_max_window_seconds == settings.log_regroup_max_window_seconds


def test_the_cycle_splits_after_coalescing():
    import inspect
    src = inspect.getsource(n3.consume_tenant)
    assert src.index("_coalesce(") < src.index("_split_run("), "merge first, then split"


def test_tickets_are_claimed_once_not_once_per_slice():
    """A bug this introduced and chunk 45 caught: every slice stamped every ticket, so two tickets split
    into two slices reported four consumed. The claim goes to the LAST slice, because a ticket covers its
    whole range - consuming it earlier would let a crash leave the remainder with nothing to retry it."""
    import inspect
    src = inspect.getsource(n3.consume_tenant)
    assert "index == len(slices) - 1" in src


# =============================================================== 2. the response-read skip
def _src(fp="abc", txn_id=None):
    return {"id": txn_id or uuid.uuid4(), "row_fingerprint": fp}


def _fact(fp="abc", version=None):
    return {"attributes": {n3._SRC_FP_KEY: fp,
                           n3._NORM_V_KEY: n3._NORMALISE_VERSION if version is None else version}}


def test_a_new_transaction_needs_its_entries():
    assert n3._needs_entries(_src(), None) is True


def test_an_unchanged_transaction_does_not():
    """The 96% saving. Stage 2's digest covers the row AND which entries it is made of, so an unchanged
    digest means the response entries are byte-identical - there is nothing to re-read."""
    assert n3._needs_entries(_src("abc"), _fact("abc")) is False


def test_a_changed_transaction_does():
    assert n3._needs_entries(_src("NEW"), _fact("abc")) is True


def test_a_pre_s3_row_with_no_digest_always_needs_them():
    """Fails OPEN. Without a digest nothing can be PROVEN about the transaction, and re-reading is
    merely slow whereas skipping would be wrong."""
    assert n3._needs_entries(_src(fp=None), _fact("abc")) is True


def test_a_normalisation_version_bump_invalidates_every_skip():
    """The `_DERIVE_VERSION` lesson, in its second location. Reusing a stored fact is only sound while
    the code that built it is unchanged - otherwise an edited normalisation would never reach a settled
    fact, silently and forever."""
    assert n3._needs_entries(_src("abc"), _fact("abc", version=n3._NORMALISE_VERSION - 1)) is True


def test_the_skip_keys_live_in_attributes_and_are_marked_internal():
    """Prefixed `__` so they cannot be mistaken for a WMS field, and in `attributes` rather than as new
    columns because they are bookkeeping for this optimisation, not something a metric measures."""
    assert n3._SRC_FP_KEY.startswith("__") and n3._NORM_V_KEY.startswith("__")


def test_the_entry_read_is_restricted_to_the_transactions_that_need_it():
    import inspect
    src = inspect.getsource(n3._read_response_entries)
    assert "transaction_id.in_" in src, "the read must be narrowed, or the 96% is still paid"
    assert "only" in inspect.signature(n3._read_response_entries).parameters


def test_the_stored_read_happens_before_the_skip_decision():
    """Ordering, and it is load-bearing: the decision needs the stored facts, so reading them after
    would make `needs` always-true and the optimisation a no-op."""
    import inspect
    src = inspect.getsource(n3._consume_run)
    assert src.index("_read_stored") < src.index("_needs_entries")
    assert src.index("_needs_entries") < src.index("_read_response_entries(db, customer_code, window, only=")


# =============================================================== 3. the timeout
def test_the_fold_relaxes_the_web_tier_timeout():
    """Stage 1's bulk insert already does this and CLAUDE.md rule 8 names it a deliberate exception. The
    fold never got the same treatment, which is why a legitimate one-day catch-up died at 30 s."""
    import inspect
    src = inspect.getsource(n3._consume_run)
    assert "statement_timeout" in src
    assert "analytics_fold_statement_timeout_ms" in src


def test_the_timeout_is_finite_and_sized_for_one_day():
    """FINITE, unlike Stage 1's `= 0`. Rule 6: a long-open transaction pins the vacuum horizon and
    blocks online DDL, and this one spans nine tables. Stage 1 accepts that for a disk fault it cannot
    bound; the fold's runaway mode is data volume, which the ticket span now bounds.

    120 s rather than 600 s BECAUSE of that bound - the earlier figure was headroom for an unbounded
    backlog, which no longer exists.
    """
    ms = settings.analytics_fold_statement_timeout_ms
    assert ms > 0, "unlimited would remove rule 8's safety net entirely"
    assert 60_000 <= ms <= 300_000, f"{ms} is not a plausible bound for one ticket span"
    assert ms >= 4 * 23_700, "must comfortably exceed the 23.7 s of reads measured on the worst day"
