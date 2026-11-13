"""The watches tick (stage D, step 14): claim the watches whose check is due, read each kind's data
once for all of them, work out every watch's live value and status, write one event per status
change (a downgrade only once it has settled, the day's resting status never), end the watches
whose thing is finished, whose time is up or whose owner lost the site, and rewrite the rows of
the owners touched. Called by the consumer's watches loop every few seconds.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.watch import Watch
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import records
from app.services.watchlist import readers, registry, store
from app.services.watchlist import mirror as watch_mirror
from app.services.teams.mirror import ChangeTracker, MirrorWriter
from app.settings import settings

logger = logging.getLogger(__name__)


class Assessment:
    """What one check found: the status, the live value, the words, and whether the thing is over."""

    __slots__ = ("status", "value", "summary", "ended_reason")

    def __init__(self, status: str, value: dict[str, Any], summary: str, ended_reason: str | None = None):
        self.status, self.value, self.summary, self.ended_reason = status, value, summary, ended_reason


# ============================================================== one kind at a time

async def assess_deliveries(db: AsyncSession, customer_code: str, watches: list[Watch], *, now: datetime, tz: ZoneInfo) -> dict[uuid.UUID, Assessment]:
    rows = await readers.delivery_rows(db, customer_code, [uuid.UUID(str(w.entity_ref.get("row_id"))) for w in watches if w.entity_ref.get("row_id")])
    out: dict[uuid.UUID, Assessment] = {}
    for w in watches:
        row = rows.get(uuid.UUID(str(w.entity_ref["row_id"]))) if w.entity_ref.get("row_id") else None
        if row is None:
            out[w.id] = Assessment("unknown", dict(w.last_value or {}), "the delivery is no longer on the board", "not_found")
            continue
        status = registry.delivery_status(row.status, row.tier, row.outcome)
        value = {"lines_picked": int(row.lines_picked or 0), "lines_expected": row.lines_expected,
                 "lines_confirmed": int(row.lines_confirmed or 0),
                 "lines_total": registry.lines_total(row.lines_expected, row.lines_confirmed),
                 "lines_short": int(row.lines_short or 0), "route": row.route, "customer_name": row.customer_name,
                 "departure_at": row.departure_at.isoformat() if row.departure_at else None, "tier": row.tier,
                 "outcome": row.outcome, "warehouse": row.warehouse}
        lines = registry.lines_words(value["lines_picked"], row.lines_expected, row.lines_confirmed, short=value["lines_short"])
        leaves = f"leaves {registry.when_words(value['departure_at'], now, tz)}" + (f" on Route {row.route}" if row.route else "")
        summary = f"{lines} · {leaves}" if row.status == "open" else f"{registry.status_text(status)} · {lines}"
        out[w.id] = Assessment(status, value, summary, "delivered" if row.status == "closed" else None)
    return out


async def assess_routes(db: AsyncSession, customer_code: str, watches: list[Watch], *, now: datetime, tz: ZoneInfo) -> dict[uuid.UUID, Assessment]:
    aggregates = await readers.route_aggregates(db, customer_code, [w.entity_id for w in watches], now.astimezone(tz).date())
    out: dict[uuid.UUID, Assessment] = {}
    for w in watches:
        agg = aggregates.get(w.entity_id) or {"open": 0, "by_tier": {}, "lines_picked": 0, "lines_expected": 0, "lines_confirmed": 0,
                                               "lines_total": 0, "lines_short": 0, "next_departure": None, "closed_today": 0,
                                               "late_today": 0, "warehouses": []}
        status = registry.route_status(agg["by_tier"], agg["closed_today"])
        value = {k: v for k, v in agg.items() if k != "warehouses"}
        if status == "idle":
            summary = "no deliveries on it yet today"
        elif status == "done":
            summary = f"{agg['closed_today']} gone today" + (f" · {agg['late_today']} late" if agg["late_today"] else " · all in time")
        else:
            summary = (f"{registry.route_words(agg['open'], agg['by_tier'])} · "
                       f"{registry.lines_words(agg['lines_picked'], agg['lines_total'], None, short=agg['lines_short'])}"
                       f" · next leaves {registry.when_words(agg['next_departure'], now, tz)}")
        out[w.id] = Assessment(status, value, summary)
    return out


def assess_measures(customer_code: str, watches: list[Watch]) -> dict[uuid.UUID, Assessment]:
    out: dict[uuid.UUID, Assessment] = {}
    for w in watches:
        latest = readers.fill_rate(customer_code, str(w.entity_ref.get("site_id") or "all"))
        rate = latest["fill_rate"] if latest else None
        status = registry.measure_status(w.alert or {}, rate, w.last_status)
        level = float((w.alert or {}).get("level") or registry.DEFAULT_LEVEL)
        value = {"fill_rate": rate, "as_of": latest["as_of"] if latest else None,
                 "provisional": bool(latest and latest.get("provisional")), "level": level}
        summary = (f"{registry.pct(rate)} · your line {level * 100:g}%" + (" · provisional" if value["provisional"] else "")
                   if rate is not None else "no figure yet today")
        out[w.id] = Assessment(status, value, summary)
    return out


async def assess_stock(db: AsyncSession, customer_code: str, watches: list[Watch], *, now: datetime, tz: ZoneInfo) -> dict[uuid.UUID, Assessment]:
    """Products, locations and zones together: one facts read for the day, one for the counts."""
    since = registry.day_start(now, tz)
    products = [w for w in watches if w.kind == "product"]
    locations = [w for w in watches if w.kind == "location"]
    zones = [w for w in watches if w.kind == "zone"]
    zone_members = {w.entity_id: list(w.entity_ref.get("locations") or []) for w in zones}
    all_locations = {w.entity_id for w in locations} | {loc for locs in zone_members.values() for loc in locs}
    by_item, by_location = await readers.pick_activity(db, customer_code, since=since, items=[w.entity_id for w in products],
                                                        locations=all_locations)
    item_counts, location_counts = readers.count_signals(await readers.latest_counts(db, customer_code, now - timedelta(hours=24)))
    out: dict[uuid.UUID, Assessment] = {}
    for w in products:
        picks = by_item.get(w.entity_id, {})
        count = item_counts.get(w.entity_id, {})
        status = registry.product_status(picks.get("picks", 0), picks.get("zero_picks", 0), bool(count.get("empty")))
        value = {"picks": picks.get("picks", 0), "zero_picks": picks.get("zero_picks", 0), "last_pick_at": picks.get("last_pick_at"),
                 "count": {"text": count.get("text"), "at": count.get("at"), "location": count.get("location")} if count else None}
        if status == "counted_empty":
            summary = f"counted empty at {count.get('location')} · {count.get('text')}"
        elif status == "zero_picked":
            summary = f"{value['zero_picks']} pick{'s' if value['zero_picks'] != 1 else ''} found it empty today · {value['picks']} picked fine"
        else:
            summary = f"{value['picks']} picks today" if value["picks"] else "no picks yet today"
        out[w.id] = Assessment(status, value, summary)
    for w in locations:
        picks = by_location.get(w.entity_id, {})
        count = location_counts.get(w.entity_id, {})
        from_ = int((w.alert or {}).get("count") or registry.ZERO_PICKS_FROM)
        status = registry.location_status(picks.get("picks", 0), picks.get("zero_picks", 0), bool(count.get("variance")), from_)
        value = {"picks": picks.get("picks", 0), "zero_picks": picks.get("zero_picks", 0), "items_empty": picks.get("items_empty", 0),
                 "count": {"text": count.get("text"), "at": count.get("at"), "item": count.get("item")} if count else None}
        out[w.id] = Assessment(status, value, _location_summary(status, value))
    for w in zones:
        members = zone_members.get(w.entity_id) or []
        from_ = int((w.alert or {}).get("count") or registry.ZERO_PICKS_FROM)
        picks = sum(by_location.get(loc, {}).get("picks", 0) for loc in members)
        zero = sum(by_location.get(loc, {}).get("zero_picks", 0) for loc in members)
        empty_locations = [loc for loc in members if by_location.get(loc, {}).get("zero_picks", 0) >= from_]
        variance_locations = [loc for loc in members if location_counts.get(loc, {}).get("variance")]
        status = registry.location_status(picks, max([by_location.get(loc, {}).get("zero_picks", 0) for loc in members] or [0]),
                                          bool(variance_locations), from_)
        value = {"locations": len(members), "picks": picks, "zero_picks": zero, "empty_locations": empty_locations[:10],
                 "variance_locations": variance_locations[:10]}
        if status == "count_variance":
            summary = f"a count disagrees at {', '.join(variance_locations[:3])}"
        elif status == "empty_picks":
            summary = f"picks found {len(empty_locations)} of {len(members)} locations empty today"
        else:
            summary = f"{picks} picks today across {len(members)} locations" if picks else "no picks yet today"
        out[w.id] = Assessment(status, value, summary)
    return out


def _location_summary(status: str, value: dict[str, Any]) -> str:
    if status == "count_variance" and value.get("count"):
        return f"a count disagrees · {value['count']['text']}"
    if status == "empty_picks":
        return f"{value['zero_picks']} picks found it empty today · {value['items_empty']} items affected"
    return f"{value['picks']} picks today" if value["picks"] else "no picks yet today"


# ============================================================== the tick

async def apply(db: AsyncSession, watch: Watch, found: Assessment, *, now: datetime, settle: timedelta) -> bool:
    """Move one watch to what the check found: the value always, the status when it changed and
    counts (an upgrade at once, a downgrade once it has held for the settle time, the day's resting
    status silently), an event per counted change; whether an event was written.

    (on_track -> at_risk) -> True ; (at_risk -> needs_look, first seen) -> False, pending ; (same) -> False
    """
    old = watch.last_status
    new = found.status
    watch.last_value, watch.last_summary, watch.last_checked_at = found.value, found.summary, now
    watch.next_check_at = now + store.cadence_of(watch.kind)
    wrote = False
    if new != old:
        # Step 1: A step down waits for the settle time, so a tier the worker bounces for a scan
        # does not become two events; a step up never waits
        if registry.is_downgrade(watch.kind, old, new) and found.ended_reason is None:
            if watch.pending_status != new:
                watch.pending_status, watch.pending_since = new, now
                return False
            if now - (watch.pending_since or now) < settle:
                return False
        watch.pending_status = watch.pending_since = None
        if registry.is_silent(watch.kind, old, new):
            watch.last_status = new
        else:
            is_alert = registry.is_alert(watch.kind, watch.alert or {}, old, new, found.value)
            await store.record_event(db, watch, old=old, new=new, summary=found.summary, value=found.value,
                                     is_alert=is_alert, now=now)
            wrote = True
    elif watch.pending_status is not None:
        watch.pending_status = watch.pending_since = None
    # Step 2: A thing that is over frees its place, after its last status landed above
    if found.ended_reason is not None:
        store.end(watch, found.ended_reason, now=now)
    return wrote


async def evaluate_customer(db: AsyncSession, customer_code: str, watches: list[Watch], *, now: datetime,
                            tz: ZoneInfo, settle: timedelta) -> tuple[set[str], int]:
    """Check every claimed watch of one customer; the owners touched and the events written."""
    owners: set[str] = set()
    events = 0
    # Step 1: A watch whose owner lost the site (or the record) ends quietly, so a watch never
    # shows a person something their access no longer covers
    record_cache: dict[str, records.AccessRecord | None] = {}
    alive: list[Watch] = []
    for w in watches:
        if w.owner_object_id not in record_cache:
            record_cache[w.owner_object_id] = await records.access_record_for(db, customer_code, w.owner_object_id)
        record = record_cache[w.owner_object_id]
        if record is None or not record.is_active or (w.site_id and not record.may_see(w.site_id)):
            store.end(w, "access", now=now)
            owners.add(w.owner_object_id)
        elif w.until is not None and w.until <= now:
            store.end(w, "until", now=now)
            owners.add(w.owner_object_id)
        else:
            alive.append(w)
    by_kind: dict[str, list[Watch]] = {}
    for w in alive:
        by_kind.setdefault(w.kind, []).append(w)
    found: dict[uuid.UUID, Assessment] = {}
    if by_kind.get("delivery"):
        found.update(await assess_deliveries(db, customer_code, by_kind["delivery"], now=now, tz=tz))
    if by_kind.get("route"):
        found.update(await assess_routes(db, customer_code, by_kind["route"], now=now, tz=tz))
    if by_kind.get("measure"):
        found.update(assess_measures(customer_code, by_kind["measure"]))
    stock = [w for k in ("product", "location", "zone") for w in by_kind.get(k, [])]
    if stock:
        found.update(await assess_stock(db, customer_code, stock, now=now, tz=tz))
    for w in alive:
        a = found.get(w.id)
        if a is None:
            continue
        if await apply(db, w, a, now=now, settle=settle):
            events += 1
        owners.add(w.owner_object_id)
    await db.flush()
    return owners, events


async def sweep_once(mirror: MirrorWriter | None, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     session_factory=None, now: datetime | None = None) -> dict[str, int]:
    """One tick over every customer with due watches (or `only`): events written per customer; the
    rows of the owners touched are rewritten at once. One customer's failure does not stop the rest."""
    from app.config.database import async_session

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    settle = timedelta(seconds=settings.teams_watches_settle_seconds)
    written: dict[str, int] = {}
    async with factory() as db:
        codes = list(only) if only is not None else await store.customers_with_watches(db)
        for code in codes:
            try:
                due = await store.claim_due(db, now=now, customer_code=code)
                if not due:
                    continue
                tz = ZoneInfo(await get_customer_timezone(db, code))
                owners, events = await evaluate_customer(db, code, due, now=now, tz=tz, settle=settle)
                await db.commit()
                written[code] = events
                if mirror is not None:
                    for owner in sorted(owners):
                        await watch_mirror.mirror_user(db, mirror, code, owner, tracker=tracker, now=now)
            except Exception:
                logger.exception("watches tick failed for %s", code)
                await db.rollback()
    return written
