"""The board rows: one per delivery per departure, kept current by the worker.

`apply` takes the states the board read produced and upserts a row for each: the progress columns are
overwritten, the tier is recomputed against the route's van clock, every tier change is appended to
the history, and a check a person left is kept but counted re-opened when the tier rises past it.
`close_due` turns open rows whose departure plus grace has passed into outcomes; `sweep` closes a row
left open for a day as unknown, so a disabled settlement can never leave a delivery "open" forever.

Nothing here commits. The runner holds the tenant's advisory lock around a whole write phase and
commits once, so a manual evaluate and the worker cannot interleave their upserts.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from decimal import Decimal
from typing import Callable, Iterable, Mapping, Sequence

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.analytics_at_risk import AnalyticsAtRiskDelivery
from app.services.analytics_at_risk import check_store, model

#: An open row this long past its departure has lost its board data (the settlement was disabled, the
#: tenant's clock moved, the delivery was renamed). It closes as `unknown` rather than lingering.
SWEEP_AFTER = timedelta(hours=24)
#: Most open rows one tenant is read with. Live volume is about 100 departures a day.
OPEN_ROWS_CAP = 5000
BATCH = 500


@dataclass
class ApplyStats:
    evaluated: int = 0
    created: int = 0
    flagged: int = 0
    escalated: int = 0
    reopened: int = 0
    moved: int = 0

    def as_dict(self) -> dict:
        return dict(self.__dict__)


def _rank(tier: str | None) -> int:
    return model.TIER_RANK[model.Tier(tier or "none")]


def _money(value: Decimal | None) -> str | None:
    """A minutes figure as the short decimal string the history and the API carry."""
    return None if value is None else format(value.quantize(Decimal("0.01")).normalize(), "f")


def history_entry(tier: model.Tier, at: datetime, minutes: Decimal, clock: model.RouteClock) -> dict:
    """One `tier_history` element, the shape the API carries: the tier, when, how many minutes before
    the van's usual ready time, and which clock that was."""
    return {"tier": tier.value, "at": at.isoformat(), "minutes_to_usual_ready": _money(minutes),
            "usual_ready_at": clock.usual_ready_at.isoformat(), "source": clock.source}


# ============================================================== reading rows

async def rows_for(db: AsyncSession, cc: str, deliveries: Sequence[str]) -> dict[str, list[AnalyticsAtRiskDelivery]]:
    """Every row (open or closed) for these delivery numbers, by number."""
    out: dict[str, list[AnalyticsAtRiskDelivery]] = defaultdict(list)
    for i in range(0, len(deliveries), BATCH):
        batch = list(deliveries[i:i + BATCH])
        rows = (await db.execute(select(AnalyticsAtRiskDelivery).where(
            AnalyticsAtRiskDelivery.customer_code == cc, AnalyticsAtRiskDelivery.delivery_number.in_(batch)))).scalars().all()
        for row in rows:
            out[row.delivery_number].append(row)
    return dict(out)


async def open_rows(db: AsyncSession, cc: str, *, limit: int = OPEN_ROWS_CAP) -> list[AnalyticsAtRiskDelivery]:
    return list((await db.execute(select(AnalyticsAtRiskDelivery).where(
        AnalyticsAtRiskDelivery.customer_code == cc, AnalyticsAtRiskDelivery.status == "open",
    ).order_by(AnalyticsAtRiskDelivery.departure_at).limit(limit))).scalars().all())


# ============================================================== writing progress and tier

def _write_progress(row: AnalyticsAtRiskDelivery, state: model.DeliveryState) -> None:
    row.departure_at = state.departure_at
    row.route, row.customer_name, row.customer_number = state.route, state.customer_name, state.customer_number
    row.lines_expected, row.lines_confirmed = state.lines_expected, state.lines_confirmed
    row.lines_picked, row.lines_short = state.lines_picked, state.lines_short
    row.packages_created, row.packages_loaded = state.packages_created, state.packages_loaded
    row.last_pick_at, row.last_load_at = state.last_pick_at, state.last_load_at
    row.loading_expected = state.loading_expected
    if state.transaction_names:
        row.transaction_names = list(state.transaction_names)
    if state.route_loaded_at is not None:
        row.route_loaded_at = state.route_loaded_at
    if state.route_loading_from is not None:
        row.route_loading_from = state.route_loading_from


def _write_clock(row: AnalyticsAtRiskDelivery, clock: model.RouteClock) -> None:
    row.usual_ready_at, row.usual_ready_source = clock.usual_ready_at, clock.source


async def _write_tier(db: AsyncSession, row: AnalyticsAtRiskDelivery, tier: model.Tier, *, now: datetime,
                      minutes: Decimal, clock: model.RouteClock, stats: ApplyStats) -> None:
    """The tier, its history, the first flag, max_tier and a re-open when a check is overtaken."""
    old = row.tier or "none"
    if tier.value == old:
        return
    row.tier_history = [*(row.tier_history or []), history_entry(tier, now, minutes, clock)]
    if _rank(tier.value) > _rank(row.max_tier):
        row.max_tier = tier.value
    if tier is not model.Tier.none and row.first_flagged_at is None:
        row.first_flagged_at, row.first_flagged_tier = now, tier.value
        stats.flagged += 1
    elif _rank(tier.value) > _rank(old):
        stats.escalated += 1
    row.tier = tier.value
    if row.checked_at is not None and _rank(tier.value) > _rank(row.checked_tier):
        row.reopened_count = (row.reopened_count or 0) + 1
        stats.reopened += 1
        await check_store.record(db, row, action="reopened", actor=check_store.WORKER_ACTOR, note=None, at=now)


def _state_from_row(row: AnalyticsAtRiskDelivery) -> model.DeliveryState:
    return model.DeliveryState(
        delivery_number=row.delivery_number, route=row.route, customer_name=row.customer_name,
        customer_number=row.customer_number, departure_at=row.departure_at, lines_expected=row.lines_expected,
        lines_confirmed=row.lines_confirmed or 0, lines_picked=row.lines_picked or 0, lines_short=row.lines_short or 0,
        packages_created=row.packages_created or 0, packages_loaded=row.packages_loaded or 0,
        last_pick_at=row.last_pick_at, last_load_at=row.last_load_at,
        loading_expected=True if row.loading_expected is None else bool(row.loading_expected),
        transaction_names=tuple(row.transaction_names or ()), route_loaded_at=row.route_loaded_at,
        route_loading_from=row.route_loading_from)


def _clock_from_row(row: AnalyticsAtRiskDelivery, *, warn_before: timedelta, gone_after: timedelta) -> model.RouteClock:
    """The clock the row was last judged with; the WMS departure when none was written."""
    if row.usual_ready_at is None:
        return model.RouteClock(usual_ready_at=row.departure_at, source="wms_departure", warn_before=warn_before, gone_after=gone_after)
    return model.RouteClock(usual_ready_at=row.usual_ready_at, source=row.usual_ready_source or "learned",
                            warn_before=warn_before, gone_after=gone_after)


# ============================================================== apply

async def apply(db: AsyncSession, cc: str, states: Iterable[model.DeliveryState], *, now: datetime, tz: tzinfo,
                clock_for: Callable[[model.DeliveryState], model.RouteClock], rule_version: str) -> ApplyStats:
    """Upsert one row per state and judge its tier. A delivery whose row for that departure is already
    closed is left alone: the board still names it the next morning, and that is not news."""
    stats = ApplyStats()
    states = list(states)
    existing = await rows_for(db, cc, [s.delivery_number for s in states])
    for state in states:
        departure_date = state.departure_at.astimezone(tz).date()
        rows = existing.get(state.delivery_number, [])
        row = next((r for r in rows if r.status == "open"), None)
        if row is None:
            if any(r.departure_date == departure_date for r in rows):
                continue  # closed for this departure already
            row = AnalyticsAtRiskDelivery(customer_code=cc, delivery_number=state.delivery_number,
                                          departure_date=departure_date, departure_at=state.departure_at,
                                          tier="none", max_tier="none", tier_history=[], rule_version=rule_version,
                                          last_evaluated_at=now)
            db.add(row)
            existing.setdefault(state.delivery_number, []).append(row)
            stats.created += 1
        elif row.departure_date != departure_date:
            if any(r is not row and r.departure_date == departure_date for r in rows):
                continue  # moved onto a departure that already has its closed row
            row.departure_date = departure_date
            stats.moved += 1
        clock = clock_for(state)
        _write_progress(row, state)
        _write_clock(row, clock)
        tier = model.tier_for(state, now, clock)
        await _write_tier(db, row, tier, now=now, minutes=model.minutes_to_departure(clock.usual_ready_at, now),
                          clock=clock, stats=stats)
        row.rule_version, row.last_evaluated_at = rule_version, now
        stats.evaluated += 1
    await db.flush()
    return stats


# ============================================================== closing

async def close_due(db: AsyncSession, cc: str, states: Mapping[str, model.DeliveryState], *, now: datetime,
                    grace: timedelta, tz: tzinfo, warn_before: timedelta = timedelta(minutes=30),
                    gone_after: timedelta = timedelta(minutes=20)) -> int:
    """Close every open row whose WMS departure plus `grace` has passed (the van is hours gone by then).
    The latest state, when the board still has one, refreshes the progress first so a load that landed
    after the last evaluation counts. The final tier is judged at `now`: a row still open closes
    `left_behind`."""
    closed = 0
    for row in await open_rows(db, cc):
        if row.departure_at + grace > now:
            continue
        state = states.get(row.delivery_number)
        if state is not None and state.departure_at.astimezone(tz).date() == row.departure_date:
            _write_progress(row, state)
        state = _state_from_row(row)
        clock = _clock_from_row(row, warn_before=warn_before, gone_after=gone_after)
        await _write_tier(db, row, model.tier_for(state, now, clock), now=now,
                          minutes=model.minutes_to_departure(clock.usual_ready_at, now), clock=clock, stats=ApplyStats())
        outcome, lead = model.outcome_for(state)
        row.status, row.closed_at, row.outcome, row.outcome_lead_min = "closed", now, outcome, lead
        row.last_evaluated_at = now
        closed += 1
    await db.flush()
    return closed


def write_closed(db: AsyncSession, cc: str, state: model.DeliveryState, *, clock: model.RouteClock,
                 replay: model.Replay, close_at: datetime, now: datetime, tz: tzinfo, rule_version: str) -> AnalyticsAtRiskDelivery:
    """A CLOSED row written after the fact by the backfill: the progress and clock as `apply` writes
    them, the tier story from the replay, the outcome from the final state, and `reconstructed` set so
    the accuracy score can leave it out. The caller has checked no row exists for this delivery and
    departure date. Does NOT commit."""
    row = AnalyticsAtRiskDelivery(customer_code=cc, delivery_number=state.delivery_number,
                                  departure_date=state.departure_at.astimezone(tz).date(), departure_at=state.departure_at,
                                  tier=replay.final_tier.value, max_tier=replay.max_tier.value,
                                  first_flagged_at=replay.first_flagged_at,
                                  first_flagged_tier=None if replay.first_flagged_tier is None else replay.first_flagged_tier.value,
                                  tier_history=[history_entry(c.tier, c.at, c.minutes_to_usual_ready, clock) for c in replay.changes],
                                  reconstructed=True, rule_version=rule_version, last_evaluated_at=now)
    _write_progress(row, state)
    _write_clock(row, clock)
    outcome, lead = model.outcome_for(state)
    row.status, row.closed_at, row.outcome, row.outcome_lead_min = "closed", close_at, outcome, lead
    db.add(row)
    return row


async def sweep(db: AsyncSession, cc: str, *, now: datetime, older_than: timedelta = SWEEP_AFTER) -> int:
    """Close rows still open a day after their departure as `unknown`."""
    swept = 0
    for row in await open_rows(db, cc):
        if row.departure_at + older_than > now:
            continue
        row.status, row.closed_at, row.outcome, row.last_evaluated_at = "closed", now, "unknown", now
        swept += 1
    await db.flush()
    return swept


# ============================================================== reads for the screens

#: Outcomes that count as "actually late" when the flags are scored (the model's list).
LATE_OUTCOMES = model.LATE_OUTCOMES
SCORED_OUTCOMES = ("loaded_in_time", "loaded_late", "never_loaded", "picked_in_time", "picked_late", "unknown")
TIER_ORDER = case({"left_behind": 3, "at_risk": 2, "watch": 1}, value=AnalyticsAtRiskDelivery.tier, else_=0)


async def board_rows(db: AsyncSession, cc: str, *, dates: Sequence[date], tiers: Sequence[str] | None = None,
                     include_closed: bool = False, limit: int = 500) -> list[AnalyticsAtRiskDelivery]:
    """The rows for these departure dates, worst tier first, then the nearest departure."""
    stmt = select(AnalyticsAtRiskDelivery).where(AnalyticsAtRiskDelivery.customer_code == cc,
                                                 AnalyticsAtRiskDelivery.departure_date.in_(list(dates)))
    if not include_closed:
        stmt = stmt.where(AnalyticsAtRiskDelivery.status == "open")
    if tiers:
        stmt = stmt.where(AnalyticsAtRiskDelivery.tier.in_(list(tiers)))
    stmt = stmt.order_by(TIER_ORDER.desc(), AnalyticsAtRiskDelivery.departure_at, AnalyticsAtRiskDelivery.delivery_number).limit(limit)
    return list((await db.execute(stmt)).scalars().all())


def last_at_expr():
    """The clock the outcome reads, as SQL: the last load, or the last pick on a route without loading."""
    d = AnalyticsAtRiskDelivery
    return case((d.loading_expected.is_(False), d.last_pick_at), else_=d.last_load_at)


def category_expr(held_after: timedelta = timedelta(0)):
    """The three plain words as SQL, the same rule as `model.category_for`, so a filter and a count
    agree with what each row says. `held_after` is the tenant's setting."""
    d = AnalyticsAtRiskDelivery
    late_enough = (last_at_expr() - d.usual_ready_at >= held_after) if held_after else (last_at_expr() > d.usual_ready_at)
    incomplete = case((d.lines_expected.is_(None), d.lines_confirmed == 0), else_=d.lines_confirmed < d.lines_expected)
    return case(
        (d.outcome.is_(None), "open"),
        (d.outcome == "unknown", "unknown"),
        (d.outcome == "never_loaded", "missed"),
        ((d.outcome == "picked_late") & incomplete, "missed"),
        (d.outcome.in_(("picked_late", "loaded_late")), "held"),
        (d.usual_ready_at.is_not(None) & late_enough, "held"),
        else_="fine")


def _history_where(cc: str, *, start: date, end: date, tier: str | None, checked: bool | None, route: str | None,
                   outcome: str | None, categories: Sequence[str] | None, transaction: str | None, delivery: str | None,
                   held_after: timedelta = timedelta(0), customer: str | None = None):
    d = AnalyticsAtRiskDelivery
    clauses = [d.customer_code == cc, d.status == "closed", d.departure_date >= start, d.departure_date <= end]
    if tier:
        clauses.append(d.max_tier == tier)
    if checked is True:
        clauses.append(d.checked_at.is_not(None))
    elif checked is False:
        clauses.append(d.checked_at.is_(None))
    if route:
        clauses.append(d.route == route)
    if outcome:
        clauses.append(d.outcome == outcome)
    if categories:
        clauses.append(category_expr(held_after).in_(list(categories)))
    if transaction:
        clauses.append(d.transaction_names.contains([transaction]))
    if delivery:
        clauses.append(d.delivery_number.ilike(f"{delivery.strip()}%"))
    if customer:
        clauses.append(d.customer_name.ilike(f"%{customer.strip()}%"))
    return clauses


async def history_rows(db: AsyncSession, cc: str, *, start: date, end: date, tier: str | None = None,
                       checked: bool | None = None, route: str | None = None, outcome: str | None = None,
                       categories: Sequence[str] | None = None, transaction: str | None = None, delivery: str | None = None,
                       limit: int = 200, after: tuple[datetime, str] | None = None,
                       held_after: timedelta = timedelta(0), customer: str | None = None) -> tuple[list[AnalyticsAtRiskDelivery], bool]:
    """Closed rows with a departure between `start` and `end`, newest departure first, keyset-paged on
    `(departure_at, delivery_number)`. `tier` filters on the highest tier the row reached; `categories`
    on the plain word; `transaction` on a picking screen the delivery went through; `delivery` on the
    start of the delivery number."""
    stmt = select(AnalyticsAtRiskDelivery).where(*_history_where(
        cc, start=start, end=end, tier=tier, checked=checked, route=route, outcome=outcome, categories=categories,
        transaction=transaction, delivery=delivery, held_after=held_after, customer=customer))
    if after is not None:
        at, number = after
        stmt = stmt.where((AnalyticsAtRiskDelivery.departure_at < at) |
                          ((AnalyticsAtRiskDelivery.departure_at == at) & (AnalyticsAtRiskDelivery.delivery_number < number)))
    rows = list((await db.execute(stmt.order_by(AnalyticsAtRiskDelivery.departure_at.desc(), AnalyticsAtRiskDelivery.delivery_number.desc())
                                  .limit(limit + 1))).scalars().all())
    return rows[:limit], len(rows) > limit


async def history_counts(db: AsyncSession, cc: str, *, start: date, end: date, tier: str | None = None,
                         checked: bool | None = None, route: str | None = None, outcome: str | None = None,
                         transaction: str | None = None, delivery: str | None = None, held_after: timedelta = timedelta(0),
                         customer: str | None = None) -> dict:
    """The pie beside the history: how many closed rows fall in each category over the range, under
    every filter EXCEPT the category one, plus the picking screens seen, so the filter pills can be
    drawn from what is there. Two small aggregates, never a row read."""
    d = AnalyticsAtRiskDelivery
    where = _history_where(cc, start=start, end=end, tier=tier, checked=checked, route=route, outcome=outcome,
                           categories=None, transaction=transaction, delivery=delivery, held_after=held_after, customer=customer)
    cat = category_expr(held_after).label("category")
    counts = {c: 0 for c in ("missed", "held", "fine", "unknown")}
    for category, n in (await db.execute(select(cat, func.count()).where(*where).group_by(cat))).all():
        counts[category] = int(n)
    names = (await db.execute(select(func.distinct(func.jsonb_array_elements_text(d.transaction_names))).where(*where))).scalars().all()
    return {"counts": counts, "transactions": sorted(n for n in names if n)}


async def accuracy(db: AsyncSession, cc: str, *, start: date, end: date) -> dict:
    """Flags scored against outcomes over the closed rows departing between `start` and `end`, as one
    SQL aggregate per route. Reconstructed rows are left out. "Flagged" is a row that ever reached a tier; "late" is an outcome in
    `LATE_OUTCOMES`. Returns `{"routes": {route: counts}, "by_tier": {tier: {flagged, late}}}` with
    plain integers; the API shapes the totals and the ratios."""
    d = AnalyticsAtRiskDelivery
    flagged = d.max_tier != "none"
    late = d.outcome.in_(LATE_OUTCOMES)
    # a reconstructed row's flags were computed from its clocks, not observed, so scoring them would score the replay
    base = select(d.route).where(d.customer_code == cc, d.status == "closed", d.departure_date >= start, d.departure_date <= end,
                                 d.reconstructed.is_(False))
    rows = (await db.execute(base.add_columns(
        func.count().label("departures"),
        func.count().filter(flagged).label("flagged"),
        func.count().filter(flagged & late).label("flagged_late"),
        func.count().filter(~flagged & late).label("late_not_flagged"),
        func.count().filter(flagged & ~late).label("flagged_not_late"),
        *[func.count().filter(d.outcome == o).label(o) for o in SCORED_OUTCOMES],
    ).group_by(d.route).order_by(d.route))).mappings().all()
    tiers = (await db.execute(select(d.max_tier, func.count().label("flagged"), func.count().filter(late).label("late")).where(
        d.customer_code == cc, d.status == "closed", d.departure_date >= start, d.departure_date <= end, flagged,
        d.reconstructed.is_(False),
    ).group_by(d.max_tier))).mappings().all()
    return {"routes": {r["route"]: {k: int(v) for k, v in r.items() if k != "route"} for r in rows},
            "by_tier": {t["max_tier"]: {"flagged": int(t["flagged"]), "late": int(t["late"])} for t in tiers}}


async def vans(db: AsyncSession, cc: str, *, start: date, end: date, limit: int = 2000, held_after: timedelta = timedelta(0)) -> list[dict]:
    """One row per route per departure day over the closed rows: when the van's loading started, when
    it was ready, when it is usually ready, how late it ran, and how many deliveries it carried, held
    or went without. The month-end view of the vans themselves. Bounded by `limit` rows."""
    d = AnalyticsAtRiskDelivery
    cat = category_expr(held_after)
    rows = (await db.execute(select(
        d.route, d.departure_date, func.min(d.route_loading_from), func.max(d.route_loaded_at), func.max(d.usual_ready_at),
        func.max(d.usual_ready_source), func.bool_and(d.loading_expected), func.count(),
        func.count().filter(cat == "held"), func.count().filter(cat == "missed"), func.count().filter(cat == "fine"),
        func.max(d.departure_at), func.bool_or(d.reconstructed),
    ).where(d.customer_code == cc, d.status == "closed", d.departure_date >= start, d.departure_date <= end, d.route.is_not(None))
      .group_by(d.route, d.departure_date).order_by(d.departure_date.desc(), d.route).limit(limit))).all()
    out = []
    for route, day, loading_from, ready_at, usual_at, source, loading, n, held, missed, fine, departure_at, reconstructed in rows:
        late = None if ready_at is None or usual_at is None else model.minutes_to_departure(ready_at, usual_at)
        out.append({"route": route, "departure_date": day, "loading_from": loading_from, "ready_at": ready_at,
                    "usual_ready_at": usual_at, "usual_ready_source": source, "loading_expected": bool(loading),
                    "deliveries": int(n), "held": int(held), "missed": int(missed), "fine": int(fine),
                    "late_min": late, "departure_at": departure_at, "reconstructed": bool(reconstructed)})
    return out
