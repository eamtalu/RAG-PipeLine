"""Chunk 51, Phase 5a: N6, the read layer. Answers every question, and is the only component that does.

    **Grain selection.** The coarsest grain covering the window, targeting under 100,000 rows scanned.
    A twelve-month request resolves to monthly, never daily.

    **Two-tier read.** Pre-aggregated rollups for settled ranges, unioned with a bounded live scan of
    the recent tail. Both halves use **one boundary value read from the persisted cursor**, never a
    freshly computed one, or a lagging worker produces double counts or gaps.

    **Ad-hoc fallback.** A query no definition covers falls back to a bounded fact-table scan, and the
    response marks itself as such so the interface can show it rather than silently running slow.

The two-tier read is where the silent failure lives, and it has two halves that fail in opposite
directions. If the boundary is computed independently for each half, a worker that folds a bucket
between the two reads makes the same rows appear in both -- a double count. If the boundary moves the
other way, rows appear in neither -- a gap. Both look like plausible numbers. So the boundary is read
ONCE, from `analytics_tenant_state.analytics_watermark`, and both halves are derived from that single
value.

The second trap is bucket alignment. A request for 09:00 to 09:30 cannot use the 09:00 hourly bucket:
that bucket holds the whole hour, so counting it would include half an hour the caller did not ask for.
Whole buckets come from the rollups, and the partial edges come from a bounded fact scan.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.services.analytics import read as n6
from app.services.mnp_log_ingestion.pipeline.time_bounds import UtcWindow

H = timedelta(hours=1)
D = timedelta(days=1)
T0 = datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc)


def win(start, end) -> UtcWindow:
    return UtcWindow(start=start, end=end)


# ==================================================== grain selection
def test_a_twelve_month_request_resolves_to_monthly_never_daily():
    """Stated verbatim in the plan, and the reason is scan size: 365 days times the dimension
    cardinality times three measures is the difference between a chart and a timeout."""
    assert n6.choose_grain(win(T0 - 365 * D, T0)) == "monthly"


def test_a_few_hours_resolves_to_hourly():
    """The finest grain is only worth its row count over a short window, which is exactly when someone
    wants to see the shape of a shift."""
    assert n6.choose_grain(win(T0 - 6 * H, T0)) == "hourly"


def test_a_month_resolves_to_daily():
    assert n6.choose_grain(win(T0 - 30 * D, T0)) == "daily"


def test_the_grain_is_the_COARSEST_that_still_covers_the_window():
    """Coarsest, not finest. Finer is always more accurate and eventually unservable, so the choice is
    the cheapest grain whose buckets still fit inside what was asked for."""
    span = win(T0 - 90 * D, T0)
    assert n6.choose_grain(span) == "monthly"
    assert n6.choose_grain(win(T0 - 2 * D, T0)) in ("hourly", "daily")


def test_a_grain_the_definition_does_not_maintain_is_never_chosen():
    """A definition declares its grains. Choosing one it never folded would return an empty chart that
    looks like zero activity, which is the failure mode the whole plan is written against."""
    assert n6.choose_grain(win(T0 - 6 * H, T0), available=("daily", "monthly")) == "daily"
    assert n6.choose_grain(win(T0 - 365 * D, T0), available=("hourly",)) == "hourly"


def test_an_unbounded_window_takes_the_coarsest_grain_available():
    """"All of history" is the request most likely to be unservable at a fine grain, so it gets the
    cheapest one rather than defaulting to the finest."""
    assert n6.choose_grain(win(None, None)) == "monthly"
    assert n6.choose_grain(win(None, T0)) == "monthly"


def test_dimension_cardinality_forces_a_coarser_grain_for_the_same_window():
    """Not a hardcoded span table: the row budget is real, so a tenant whose dimension cardinality is
    high resolves coarser for an identical request.

    Note what a CHEAP tenant does NOT buy: 480 hourly points for a 20-day window is servable and
    useless, so the resolution rule (MAX_BUCKETS) still applies. Cost can only make the answer coarser,
    never finer -- which is why both constraints exist."""
    span = win(T0 - 20 * D, T0)
    assert n6.choose_grain(span, rows_per_bucket=1) == "daily", "cheap does not mean finer"
    assert n6.choose_grain(span, rows_per_bucket=50_000) == "monthly", "expensive means coarser"


def test_both_constraints_are_named_and_documented():
    """The plan gives one hard case -- twelve months resolves to monthly -- and the budget alone cannot
    produce it: 365 daily buckets at ~20 rows each is 7,300, well inside 100,000."""
    assert n6.ROW_BUDGET == 100_000 and n6.MAX_BUCKETS == 60
    assert n6.buckets_in(win(T0 - 365 * D, T0), "daily") == 365
    assert 365 * 20 < n6.ROW_BUDGET, "so cost is not what rules daily out; resolution is"


def test_the_budget_is_named_and_matches_the_plan():
    assert n6.ROW_BUDGET == 100_000


@pytest.mark.parametrize("grain,expected", [("hourly", 24), ("daily", 1), ("monthly", 1)])
def test_bucket_counts_are_computed_not_guessed(grain, expected):
    """A day is 24 hourly buckets, 1 daily bucket, and (sharing a month) 1 monthly one."""
    assert n6.buckets_in(win(T0, T0 + D), grain) == expected


# ==================================================== the two-tier boundary
def test_whole_buckets_come_from_rollups_and_the_edges_from_facts():
    """A request for 09:00 to 09:30 cannot use the 09:00 hourly bucket: it holds the whole hour, so
    counting it would include thirty minutes nobody asked for."""
    plan = n6.plan_read(win(T0, T0 + timedelta(minutes=30)), "hourly", watermark=T0 + 10 * H)
    assert plan.rollup_window.start is None and plan.rollup_window.end is None, \
        "no whole hour fits inside a 30-minute request"
    assert plan.live_windows, "so the whole request must come from a fact scan"


def test_a_window_on_exact_bucket_edges_uses_rollups_only():
    plan = n6.plan_read(win(T0, T0 + 3 * H), "hourly", watermark=T0 + 10 * H)
    assert plan.rollup_window.start == T0 and plan.rollup_window.end == T0 + 3 * H
    assert plan.live_windows == [], "nothing is left over, so no fact scan is needed"


def test_a_ragged_window_splits_into_rollups_plus_two_edges():
    lo, hi = T0 + timedelta(minutes=20), T0 + 3 * H + timedelta(minutes=40)
    plan = n6.plan_read(win(lo, hi), "hourly", watermark=hi + H)
    assert plan.rollup_window.start == T0 + H and plan.rollup_window.end == T0 + 3 * H
    assert plan.live_windows == [(lo, T0 + H), (T0 + 3 * H, hi)]


def test_the_two_halves_never_overlap():
    """A double count is the failure this shape exists to prevent, and it produces a plausible number
    rather than an error."""
    lo, hi = T0 + timedelta(minutes=20), T0 + 5 * H + timedelta(minutes=40)
    plan = n6.plan_read(win(lo, hi), "hourly", watermark=hi + H)
    r = plan.rollup_window
    for s, e in plan.live_windows:
        assert e <= r.start or s >= r.end, f"live {s}..{e} overlaps rollups {r.start}..{r.end}"


def test_the_two_halves_leave_no_gap():
    """The opposite failure, equally silent. Together they must cover exactly the request."""
    lo, hi = T0 + timedelta(minutes=20), T0 + 5 * H + timedelta(minutes=40)
    plan = n6.plan_read(win(lo, hi), "hourly", watermark=hi + H)
    covered = sorted(plan.live_windows + [(plan.rollup_window.start, plan.rollup_window.end)])
    assert covered[0][0] == lo and covered[-1][1] == hi
    for (_, a_end), (b_start, _) in zip(covered, covered[1:]):
        assert a_end == b_start, "a gap between the halves loses rows silently"


# ==================================================== the boundary is ONE persisted value
def test_everything_past_the_watermark_is_read_live():
    """The rollups cannot be trusted past the watermark: the worker has not folded there yet, so a
    rollup read would return zero for a range that has data."""
    lo, hi = T0, T0 + 6 * H
    plan = n6.plan_read(win(lo, hi), "hourly", watermark=T0 + 2 * H)
    assert plan.rollup_window.end <= T0 + 2 * H
    assert any(s >= T0 + 2 * H for s, _ in plan.live_windows)


def test_a_tenant_that_has_folded_nothing_is_read_entirely_live():
    """A NULL watermark means nothing has been folded. Reading rollups would report zero for every
    range, which is the "empty chart that looks like no activity" failure."""
    plan = n6.plan_read(win(T0, T0 + 6 * H), "hourly", watermark=None)
    assert plan.rollup_window.start is None
    assert plan.live_windows == [(T0, T0 + 6 * H)]
    assert plan.reason and "watermark" in plan.reason.lower()


def test_the_watermark_is_a_parameter_so_it_can_only_be_read_once():
    """The plan's requirement, made structural. If `plan_read` fetched the watermark itself, a caller
    doing two reads would get two boundaries and the halves could overlap or gap."""
    import inspect
    assert "watermark" in inspect.signature(n6.plan_read).parameters
    src = inspect.getsource(n6.plan_read)
    for forbidden in ("select(", "await", "db"):
        assert forbidden not in src, f"plan_read must be pure: found {forbidden!r}"


def test_planning_is_deterministic_for_the_same_inputs():
    args = (win(T0, T0 + 5 * H), "hourly")
    assert n6.plan_read(*args, watermark=T0 + 3 * H) == n6.plan_read(*args, watermark=T0 + 3 * H)


# ==================================================== the daily grain works on local dates
def test_the_daily_grain_aligns_on_the_business_date():
    """Daily buckets are keyed on the tenant-LOCAL `business_date`, not a UTC day, so alignment is a
    date operation. Treating it as a UTC-midnight instant would mis-align every tenant not on UTC."""
    lo = datetime(2026, 8, 10, 5, 0, tzinfo=timezone.utc)
    hi = datetime(2026, 8, 14, 19, 0, tzinfo=timezone.utc)
    plan = n6.plan_read(win(lo, hi), "daily", watermark=hi + D)
    assert plan.rollup_dates == (date(2026, 8, 11), date(2026, 8, 13)), \
        "the whole local days strictly inside the request"
    assert plan.live_windows, "the ragged ends still need a fact scan"


def test_a_request_shorter_than_one_day_uses_no_daily_bucket():
    lo = datetime(2026, 8, 10, 5, 0, tzinfo=timezone.utc)
    plan = n6.plan_read(win(lo, lo + 6 * H), "daily", watermark=lo + 30 * D)
    assert plan.rollup_dates is None
    assert plan.live_windows == [(lo, lo + 6 * H)]


# ==================================================== the ad-hoc fallback
def test_a_dimension_no_definition_covers_falls_back_and_says_so():
    """The plan is explicit that the response marks itself, "so the interface can show it rather than
    silently running slow"."""
    from app.services.analytics import definition as d
    decision = n6.resolve(d.CONSUMPTION, group_by=("item_number",))
    assert decision.ad_hoc is True
    assert "item_number" in decision.reason


def test_a_dimension_the_definition_declares_uses_the_rollups():
    from app.services.analytics import definition as d
    decision = n6.resolve(d.CONSUMPTION, group_by=("method",))
    assert decision.ad_hoc is False


def test_a_dimension_that_is_not_on_the_fact_row_is_an_error_not_a_fallback():
    """A fallback would scan the fact table and return nothing, which reads as "no data" rather than
    "you asked for a field that does not exist"."""
    from app.services.analytics import definition as d
    with pytest.raises(ValueError, match="not a field"):
        n6.resolve(d.CONSUMPTION, group_by=("no_such_column",))


def test_the_ad_hoc_scan_is_bounded():
    """"Bounded fact-table scan", per the plan. Unbounded, one careless request over a 13M-row table
    would be the outage the CLAUDE.md rules exist to prevent."""
    import inspect
    src = inspect.getsource(n6)
    assert "AD_HOC_MAX_ROWS" in src
    assert n6.AD_HOC_MAX_ROWS <= 1_000_000


# ==================================================== freshness (F4), both numbers
def test_freshness_reports_lag_and_settledness_separately():
    """F4. A screen can truthfully say "updated 2 seconds ago" about a number still due to move, so
    "how far behind am I" and "is what I have still going to change" are different questions."""
    f = n6.freshness(analytics_watermark=T0, source_watermark=T0 + timedelta(seconds=5),
                     unsealed_share=Decimal("0.25"), oldest_unsealed_at=T0 - H)
    assert f["lag_seconds"] == 5
    assert f["provisional"] is True
    assert f["unsealed_share"] == Decimal("0.25")


def test_a_window_with_unsealed_contributors_is_provisional_not_stale():
    """Different words for the user and different actions for an operator, which is why both numbers
    are stored rather than one being derived from the other."""
    f = n6.freshness(analytics_watermark=T0, source_watermark=T0,
                     unsealed_share=Decimal("0.4"), oldest_unsealed_at=T0 - H)
    assert f["lag_seconds"] == 0 and f["stale"] is False
    assert f["provisional"] is True


def test_a_settled_and_caught_up_tenant_is_neither_stale_nor_provisional():
    f = n6.freshness(analytics_watermark=T0, source_watermark=T0,
                     unsealed_share=Decimal(0), oldest_unsealed_at=None)
    assert f["stale"] is False and f["provisional"] is False


def test_a_tenant_that_has_never_folded_is_reported_as_such_not_as_zero_lag():
    """NULL is not "caught up". Reporting zero lag for a tenant that has folded nothing would show a
    green light over an empty chart."""
    f = n6.freshness(analytics_watermark=None, source_watermark=T0,
                     unsealed_share=None, oldest_unsealed_at=None)
    assert f["lag_seconds"] is None
    assert f["never_folded"] is True
