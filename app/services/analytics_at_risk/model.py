"""The rules. Pure: no database, no clock, no settings.

A delivery is described by what the warehouse has done to it (`DeliveryState`) and judged against
its departure instant and its route's thresholds. Three tiers:

- `watch`: picking is still open and the departure is closer than the route's normal pick lead;
- `at_risk`: loading is still open and the departure is closer than the route's normal load lead;
- `late`: the departure has passed and either is still open.

The lead a route "normally" keeps is learned as a coverage quantile: the lead that nine in ten
loaded deliveries met or beat, which is the TENTH percentile of lead minutes, not the ninetieth.
On the live routes the ninetieth is five to eight hours and would flag every delivery before loading
even starts. The learned value is unknown below a sample floor, and whatever is learned is held to a
configured floor: the effective threshold is the larger of the two, so a slow week can never teach
the system to hide risk.
"""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

#: Leads outside this band are data problems (a load logged before the departure was even known, or
#: a departure that moved a day), not rhythm. They are dropped before the sample is counted.
LEAD_MIN = Decimal(0)
LEAD_MAX = Decimal(1440)


class Tier(str, enum.Enum):
    none = "none"
    watch = "watch"
    at_risk = "at_risk"
    late = "late"


TIER_RANK: dict[Tier, int] = {Tier.none: 0, Tier.watch: 1, Tier.at_risk: 2, Tier.late: 3}


@dataclass(frozen=True)
class DeliveryState:
    """What is known about one delivery for one departure, as the stores read it."""

    delivery_number: str
    route: str | None
    customer_name: str | None
    customer_number: str | None
    departure_at: datetime
    #: None when the pick-line lookup has not seen this delivery's lines. Never zero for "unknown".
    lines_expected: int | None
    lines_confirmed: int
    lines_picked: int
    lines_short: int
    #: Packages KNOWN for the delivery: the distinct package numbers on its pick confirmations plus any
    #: extra package created by hand. Not only the hand-created ones: measured live, most packages are
    #: made at pick time and `NewDeliveryPackage` alone undercounts them on 3 deliveries in 4.
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
    #: When the ROUTE was fully loaded on the departure day: the last package scanned onto that
    #: loading dock. The WMS records no departure itself; with vehicle-load auto-despatch on, this is
    #: the moment the shipment is despatched, so it is the nearest thing to the van leaving.
    route_loaded_at: datetime | None = None


@dataclass(frozen=True)
class Thresholds:
    """The effective leads for one route, in minutes, and where each came from."""

    load_min: Decimal
    load_source: str
    pick_min: Decimal
    pick_source: str


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
    short: a short pick is the warehouse's answer for that line, not a line still waiting, and judging
    on `lines_picked` instead flagged one closed delivery in four as late on the live data. With the
    expected count unknown, only a delivery with NO confirmation yet counts as open: a lookup gap must
    not flag every delivery that has been picked."""
    if state.lines_expected is None:
        return state.lines_confirmed == 0
    return state.lines_confirmed < state.lines_expected


def loading_open(state: DeliveryState) -> bool:
    """Packages still to load. A route without a loading step is never open. Otherwise open while
    nothing has been loaded, or fewer packages are loaded than are known; a load of a package nobody
    "created" through the app still counts, because the pick confirmations are where most packages
    are born."""
    if not state.loading_expected:
        return False
    return state.packages_loaded == 0 or state.packages_loaded < state.packages_created


def tier_for(state: DeliveryState, now: datetime, thresholds: Thresholds) -> Tier:
    minutes = minutes_to_departure(state.departure_at, now)
    picking, loading = picking_open(state), loading_open(state)
    if minutes < 0 and (picking or loading):
        return Tier.late
    if loading and minutes < thresholds.load_min:
        return Tier.at_risk
    if picking and minutes < thresholds.pick_min:
        return Tier.watch
    return Tier.none


# ============================================================== replaying the rule over the clocks

#: The worker judges every minute, so a lead crossed at 09:30:00 is seen at the next tick. The replay
#: evaluates one minute after each crossing to land on the same instant.
TICK = timedelta(minutes=1)


@dataclass(frozen=True)
class TierChange:
    tier: Tier
    at: datetime
    minutes_to_departure: Decimal


@dataclass(frozen=True)
class Replay:
    changes: tuple[TierChange, ...]
    first_flagged_at: datetime | None
    first_flagged_tier: Tier | None
    max_tier: Tier
    final_tier: Tier


def state_as_of(state: DeliveryState, at: datetime) -> DeliveryState:
    """The delivery as the board would have seen it at `at`, from its final clocks: before its last
    pick the picking was still open, before its last load the loading was. Exact for the question the
    rule asks (open or not), whatever the counts were on the way."""
    picking_done = state.last_pick_at is not None and at >= state.last_pick_at
    loading_done = state.last_load_at is not None and at >= state.last_load_at
    return DeliveryState(
        delivery_number=state.delivery_number, route=state.route, customer_name=state.customer_name,
        customer_number=state.customer_number, departure_at=state.departure_at, lines_expected=state.lines_expected,
        lines_confirmed=state.lines_confirmed if picking_done else 0, lines_picked=state.lines_picked if picking_done else 0,
        lines_short=state.lines_short if picking_done else 0, packages_created=state.packages_created,
        packages_loaded=state.packages_loaded if loading_done else 0,
        last_pick_at=state.last_pick_at if picking_done else None, last_load_at=state.last_load_at if loading_done else None,
        loading_expected=state.loading_expected, transaction_names=state.transaction_names, route_loaded_at=state.route_loaded_at)


def replay_tiers(state: DeliveryState, thresholds: Thresholds, *, close_at: datetime) -> Replay:
    """What the minute pass would have recorded for a delivery whose clocks are all known: the tier
    changes in order, the first flag, the highest tier and the tier at the close. The tier can only
    change at a handful of instants: one tick after each lead is crossed and after the departure, the
    moment picking finished, the moment loading finished, and the close itself."""
    departure = state.departure_at
    instants = {departure - timedelta(minutes=float(thresholds.pick_min)) + TICK,
                departure - timedelta(minutes=float(thresholds.load_min)) + TICK, departure + TICK, close_at}
    for finished in (state.last_pick_at, state.last_load_at):
        if finished is not None:
            instants.add(finished)
    changes: list[TierChange] = []
    current = Tier.none
    for at in sorted(x for x in instants if x <= close_at):
        tier = tier_for(state_as_of(state, at), at, thresholds)
        if tier is not current:
            changes.append(TierChange(tier=tier, at=at, minutes_to_departure=minutes_to_departure(departure, at)))
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
CATEGORIES = ("missed", "delayed", "fine", "unknown", "open")


def category_for(*, outcome: str | None, max_tier: str | None, lines_expected: int | None, lines_confirmed: int) -> str:
    """One plain word for a closed delivery.

    - `missed`: the van left without it. A package was never loaded, or lines were never confirmed
      (picked or declared short).
    - `delayed`: it got away, but behind the route's rhythm (flagged Watch or At risk before the
      departure) or after the departure time.
    - `fine`: in time and never flagged.
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
        return "missed" if incomplete else "delayed"
    if outcome == "loaded_late":
        return "delayed"
    if TIER_RANK[Tier(max_tier or "none")] > 0:
        return "delayed"
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


def effective_threshold(learned: Decimal | None, floor: Decimal) -> tuple[Decimal, str]:
    """The larger of the learned lead and the floor, and which one won. A tie is `learned`: the
    floor changed nothing."""
    if learned is None or learned < floor:
        return floor, "floor"
    return learned, "learned"


def numeric(value: Any) -> Decimal | None:
    """A stored measure (settled values and lookup values are text) as a Decimal, or None."""
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None
