"""The rules. Pure: no database, no clock, no settings.

The clock is the VAN, not the WMS departure time. On every live route the van is fully loaded four
to five hours before the 11:30 the WMS prints: 11:30 is a planning time nobody loads against. What a
route does have is a rhythm: its dock's last scan lands at much the same time of day, day after day.
So each route learns the time its van is usually ready (the coverage quantile of those daily times,
nine days in ten), and every delivery on that route is judged against that instant on its departure
day:

- `watch`: picking is not finished and the van is usually ready within the warning window;
- `at_risk`: packages are still off the van and the van is usually ready within the window, or the
  usual time has passed;
- `left_behind`: the usual time has passed and the dock has been quiet for the gone window, so the
  van is taken as gone, and this delivery is not on it.

A route with too few days of history has no rhythm yet; the WMS departure stands in, which flags late
but never falsely. A route that never scans a load (the BRILA runs) is judged on its last pick of the
day the same way.

Once the day is over, a delivery is `missed` when the van went without it, `held` when its last
package went on after the van's usual time (the van ran late and this delivery was on it late), and
`fine` otherwise.
"""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

#: Times of day outside this band (minutes after local midnight) are data problems, not rhythm.
LEAD_MIN = Decimal(0)
LEAD_MAX = Decimal(1440)


class Tier(str, enum.Enum):
    none = "none"
    watch = "watch"
    at_risk = "at_risk"
    left_behind = "left_behind"


TIER_RANK: dict[Tier, int] = {Tier.none: 0, Tier.watch: 1, Tier.at_risk: 2, Tier.left_behind: 3}


@dataclass(frozen=True)
class DeliveryState:
    """What is known about one delivery for one departure, as the stores read it."""

    delivery_number: str
    route: str | None
    customer_name: str | None
    customer_number: str | None
    #: The WMS departure: a planning time, kept for reference and as the clock of last resort.
    departure_at: datetime
    #: None when the pick-line lookup has not seen this delivery's lines. Never zero for "unknown".
    lines_expected: int | None
    lines_confirmed: int
    lines_picked: int
    lines_short: int
    #: Packages KNOWN for the delivery: the distinct package numbers on its pick confirmations that moved
    #: stock. Measured live, that is where packages are born (99.8% of them were loaded); a hand-made
    #: package no pick ever filled is an empty box, and a short line's package number is noise.
    packages_created: int
    packages_loaded: int
    last_pick_at: datetime | None
    last_load_at: datetime | None
    #: Whether this delivery's route has a loading step at all. The BRILA routes (the Gatwick run among
    #: them) are picked and packed but never scanned onto a van, measured over 30 days, so judging them
    #: on loading would flag every one of them every day. False means picking alone decides.
    loading_expected: bool = True
    #: The picking screens this delivery's lines went through (Brighton Stock Pick, JIT and Shorts
    #: Pick, Milk Pick, Freezer Pick), so a supervisor can look at one kind of picking at a time. A
    #: delivery that spans two kinds appears under both.
    transaction_names: tuple[str, ...] = ()
    #: The last package scanned onto the ROUTE's dock on the departure day, so far: the van's "ready"
    #: moment once the day is over, and while the day runs, how long the dock has been quiet.
    route_loaded_at: datetime | None = None
    #: The first scan on that dock on the departure day: the van's loading started.
    route_loading_from: datetime | None = None

    @property
    def last_at(self) -> datetime | None:
        """The clock the outcome reads: the last load, or the last pick on a route without loading."""
        return self.last_load_at if self.loading_expected else self.last_pick_at


@dataclass(frozen=True)
class RouteClock:
    """When this delivery's van is usually ready, and the windows around it."""

    usual_ready_at: datetime
    #: `learned` from the route's days, or `wms_departure` when the route has no rhythm yet.
    source: str
    warn_before: timedelta
    gone_after: timedelta


# ============================================================== departure

def _digits(value: Any) -> str | None:
    """A WMS date or time as the digit string it was logged as. A settlement's `last` rule reads
    `"0930"` as the number 930 and `"20261002"` as 20261002; both come back here as digits."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        value = int(value)
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    return text if text.isdigit() else None


def departure_at(date_value: Any, time_value: Any, tz: tzinfo) -> datetime | None:
    """`20261002` + `1130` on the tenant's clock as one aware instant, or None when unreadable."""
    date_text, time_text = _digits(date_value), _digits(time_value)
    if date_text is None or time_text is None or len(date_text) != 8 or len(time_text) > 4:
        return None
    time_text = time_text.rjust(4, "0")
    try:
        local = datetime(int(date_text[:4]), int(date_text[4:6]), int(date_text[6:8]),
                         int(time_text[:2]), int(time_text[2:]), tzinfo=tz)
    except ValueError:
        return None
    return local


def minutes_to_departure(departure: datetime, now: datetime) -> Decimal:
    """Signed minutes from `now` to the departure, exact to the second. Negative once it has gone."""
    gap: timedelta = departure - now
    return (Decimal(gap.days * 86400 + gap.seconds) + Decimal(gap.microseconds) / Decimal(1_000_000)) / Decimal(60)


# ============================================================== packages

def parse_packages_to_load(raw: Any) -> list[tuple[str, str]]:
    """The milk load's `PackagesToLoad`, a JSON string list of {DeliveryNumber, PackageNumber}, as
    (delivery, package) pairs. Anything unreadable is nothing, never an error: this runs inside the
    worker and one odd row must not stop the board."""
    elements: Any = raw
    if isinstance(raw, str):
        try:
            elements = json.loads(raw)
        except ValueError:
            return []
    if not isinstance(elements, list):
        return []
    out: list[tuple[str, str]] = []
    for element in elements:
        if not isinstance(element, dict):
            continue
        delivery = str(element.get("DeliveryNumber") or "").strip()
        package = str(element.get("PackageNumber") or "").strip()
        if delivery and package:
            out.append((delivery, package))
    return out


# ============================================================== tiers

def picking_open(state: DeliveryState) -> bool:
    """Lines still to pick. A line is done once it is CONFIRMED, whether it moved stock or was declared
    short: a short pick is the warehouse's answer for that line, not a line still waiting. With the
    expected count unknown, only a delivery with NO confirmation yet counts as open: a lookup gap must
    not flag every delivery that has been picked."""
    if state.lines_expected is None:
        return state.lines_confirmed == 0
    return state.lines_confirmed < state.lines_expected


def loading_open(state: DeliveryState) -> bool:
    """Packages still to load. A route without a loading step is never open. Otherwise open while
    nothing has been loaded, or fewer packages are loaded than are known; a load of a package nobody
    "created" through the app still counts, because the pick confirmations are where packages are born."""
    if not state.loading_expected:
        return False
    return state.packages_loaded == 0 or state.packages_loaded < state.packages_created


def minutes_to_departure(departure: datetime, now: datetime) -> Decimal:
    """Signed minutes from `now` to an instant, exact to the second. Negative once it has passed."""
    gap: timedelta = departure - now
    return (Decimal(gap.days * 86400 + gap.seconds) + Decimal(gap.microseconds) / Decimal(1_000_000)) / Decimal(60)


def van_gone(state: DeliveryState, now: datetime, clock: RouteClock) -> bool:
    """The van is taken as gone once its usual time has passed and the dock has been quiet for the gone
    window: no scan since `route_loaded_at`, or never a scan at all. On a route without a loading step
    the usual time plus the window decides alone."""
    if now < clock.usual_ready_at + clock.gone_after:
        return False
    if not state.loading_expected or state.route_loaded_at is None:
        return True
    return now - state.route_loaded_at >= clock.gone_after


def tier_for(state: DeliveryState, now: datetime, clock: RouteClock) -> Tier:
    picking, loading = picking_open(state), loading_open(state)
    if not (picking or loading):
        return Tier.none
    if van_gone(state, now, clock):
        return Tier.left_behind
    if now < clock.usual_ready_at - clock.warn_before:
        return Tier.none
    return Tier.at_risk if loading else Tier.watch


# ============================================================== replaying the rule over the clocks

#: The worker judges every minute, so a window crossed at 06:26:00 is seen at the next tick. The replay
#: evaluates one minute after each crossing to land on the same instant.
TICK = timedelta(minutes=1)


@dataclass(frozen=True)
class TierChange:
    tier: Tier
    at: datetime
    minutes_to_usual_ready: Decimal


@dataclass(frozen=True)
class Replay:
    changes: tuple[TierChange, ...]
    first_flagged_at: datetime | None
    first_flagged_tier: Tier | None
    max_tier: Tier
    final_tier: Tier


def state_as_of(state: DeliveryState, at: datetime) -> DeliveryState:
    """The delivery as the board would have seen it at `at`, from its final clocks: before its last
    pick the picking was still open, before its last load the loading was, and until the dock's last
    scan the dock was still busy. Exact for the questions the rule asks."""
    # a clock that was never recorded leaves the final counts as they are: nothing is known to undo
    picking_done = state.last_pick_at is None or at >= state.last_pick_at
    loading_done = state.last_load_at is None or at >= state.last_load_at
    dock_last = None if state.route_loaded_at is None else min(state.route_loaded_at, at)
    dock_from = None if state.route_loading_from is None or at < state.route_loading_from else state.route_loading_from
    return DeliveryState(
        delivery_number=state.delivery_number, route=state.route, customer_name=state.customer_name,
        customer_number=state.customer_number, departure_at=state.departure_at, lines_expected=state.lines_expected,
        lines_confirmed=state.lines_confirmed if picking_done else 0, lines_picked=state.lines_picked if picking_done else 0,
        lines_short=state.lines_short if picking_done else 0, packages_created=state.packages_created,
        packages_loaded=state.packages_loaded if loading_done else 0,
        last_pick_at=state.last_pick_at if picking_done else None, last_load_at=state.last_load_at if loading_done else None,
        loading_expected=state.loading_expected, transaction_names=state.transaction_names,
        route_loaded_at=dock_last, route_loading_from=dock_from)


def replay_tiers(state: DeliveryState, clock: RouteClock, *, close_at: datetime) -> Replay:
    """What the minute pass would have recorded for a delivery whose clocks are all known: the tier
    changes in order, the first flag, the highest tier and the tier at the close. The tier can only
    change at a handful of instants: one tick after the warning window opens, after the usual time,
    after the gone window (from the usual time or from the dock's last scan), the moments picking and
    loading finished, and the close itself."""
    usual = clock.usual_ready_at
    instants = {usual - clock.warn_before + TICK, usual + TICK, usual + clock.gone_after + TICK, close_at}
    if state.route_loaded_at is not None:
        instants.add(state.route_loaded_at + clock.gone_after + TICK)
    for finished in (state.last_pick_at, state.last_load_at):
        if finished is not None:
            instants.add(finished)
    changes: list[TierChange] = []
    current = Tier.none
    for at in sorted(x for x in instants if x <= close_at):
        tier = tier_for(state_as_of(state, at), at, clock)
        if tier is not current:
            changes.append(TierChange(tier=tier, at=at, minutes_to_usual_ready=minutes_to_departure(usual, at)))
            current = tier
    flagged = [c for c in changes if c.tier is not Tier.none]
    max_tier = max((c.tier for c in changes), key=lambda t: TIER_RANK[t], default=Tier.none)
    return Replay(changes=tuple(changes), first_flagged_at=flagged[0].at if flagged else None,
                  first_flagged_tier=flagged[0].tier if flagged else None, max_tier=max_tier, final_tier=current)


# ============================================================== outcome

def outcome_for(state: DeliveryState) -> tuple[str, Decimal | None]:
    """How the delivery ended, once its departure is behind us: `(outcome, lead minutes of the last
    load, negative when it came after the departure)`. On a route without a loading step the last
    PICK decides instead: `picked_in_time` when every line was picked before the departure, else
    `picked_late`, with the lead measured to that last pick."""
    if not state.loading_expected:
        lead = None if state.last_pick_at is None else minutes_to_departure(state.departure_at, state.last_pick_at)
        if picking_open(state) or lead is None or lead < 0:
            return "picked_late", lead
        return "picked_in_time", lead
    lead = None if state.last_load_at is None else minutes_to_departure(state.departure_at, state.last_load_at)
    if loading_open(state):
        return "never_loaded", lead
    if lead is not None and lead < 0:
        return "loaded_late", lead
    return "loaded_in_time", lead


#: Outcomes that count as "actually late" when the flags are scored.
LATE_OUTCOMES = ("loaded_late", "never_loaded", "picked_late")

#: The three plain words a supervisor reads, plus the two edge cases.
CATEGORIES = ("missed", "held", "fine", "unknown", "open")


def category_for(*, outcome: str | None, last_at: datetime | None, usual_ready_at: datetime | None,
                 lines_expected: int | None, lines_confirmed: int, held_after: timedelta = timedelta(0)) -> str:
    """One plain word for a closed delivery.

    - `missed`: the van went without it. A package was never loaded, or lines were never confirmed
      (picked or declared short).
    - `held`: it was on the van, but `held_after` or more past the van's usual ready time, or after the
      WMS departure: the van ran noticeably late and this delivery was one of those still going on.
    - `fine`: on the van before the usual time.
    - `unknown`: the board lost sight of it before it closed; `open`: not closed yet.
    """
    if outcome is None:
        return "open"
    if outcome == "unknown":
        return "unknown"
    if outcome == "never_loaded":
        return "missed"
    if outcome == "picked_late":
        incomplete = lines_confirmed < lines_expected if lines_expected is not None else lines_confirmed == 0
        if incomplete:
            return "missed"
        return "held"
    if outcome == "loaded_late":
        return "held"
    if last_at is not None and usual_ready_at is not None and last_at - usual_ready_at >= held_after and (held_after or last_at > usual_ready_at):
        return "held"
    return "fine"


# ============================================================== learning

def coverage_quantile(leads: Iterable[Decimal], *, coverage: Decimal, min_sample: int) -> Decimal | None:
    """The lead that `coverage` of the deliveries met or beat: the (1 - coverage) quantile of lead
    minutes, by linear interpolation, over the leads inside the plausible band. None below the sample
    floor, because a threshold learned from three deliveries is noise with a decimal point."""
    usable = sorted(x for x in leads if x is not None and LEAD_MIN <= x <= LEAD_MAX)
    n = len(usable)
    if n == 0 or n < min_sample:
        return None
    p = Decimal(1) - coverage
    if p <= 0:
        return usable[0]
    if p >= 1:
        return usable[-1]
    position = p * (n - 1)
    lower = int(position)
    fraction = position - lower
    if lower + 1 >= n:
        return usable[lower]
    return usable[lower] + (usable[lower + 1] - usable[lower]) * fraction


def usual_time(times: Iterable[Decimal], *, coverage: Decimal, min_days: int) -> Decimal | None:
    """The time of day (minutes after local midnight) by which the van was ready on `coverage` of the
    days: the `coverage` quantile, since for a time of day smaller is earlier. None below `min_days`."""
    return coverage_quantile(times, coverage=Decimal(1) - coverage, min_sample=min_days)


def numeric(value: Any) -> Decimal | None:
    """A stored measure (settled values and lookup values are text) as a Decimal, or None."""
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
