"""The rows the edge reads for the watchlist (stage D, step 14): `USER#<id>#WATCHES` per person
(their watches as cards, today's events with their seen flags, the unseen count, the places used,
the Suggested box) and `WATCHABLES` per customer (what can be watched right now, for the search
and the eye toggle: open deliveries, routes, sites, zones, locations, items). Rewritten by the
watches loop (the person's row after every tick that touched them, everything once a minute) and
after every command.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.watch import Watch
from app.persistence.repositories.customer_repository import get_customer_timezone
from app.services.access import records
from app.services.teams.mirror import ChangeTracker, MirrorWriter, envelope, write_if_changed
from app.services.teams.sites import list_sites
from app.services.watchlist import readers, registry, store, suggest
from app.settings import settings

logger = logging.getLogger(__name__)

LEVEL_ORDER = {"high": 0, "medium": 1, "fine": 2}


def _iso(at: datetime | None) -> str | None:
    return at.astimezone(timezone.utc).isoformat() if at else None


def ends_text(watch: Watch, tz: ZoneInfo) -> str:
    """How a watch ends, in the card's words.

    (delivery) -> "stops by itself once it has gone" ; (until 20 Nov) -> "until Fri 20 Nov" ; (route) -> ""
    """
    if watch.until is not None:
        at = watch.until.astimezone(tz)
        return f"until {at.strftime('%a %d %b')}"
    if watch.kind == "delivery":
        return "stops by itself once it has gone"
    return ""


def card(watch: Watch, tz: ZoneInfo) -> dict:
    """One watch as the Watchlist tab draws it."""
    status = watch.last_status
    return {
        "id": str(watch.id), "kind": watch.kind, "kind_label": registry.LABELS.get(watch.kind, watch.kind),
        "entity_id": watch.entity_id, "label": watch.label, "site_id": watch.site_id,
        "status": status, "status_text": registry.status_text(status), "level": registry.level_of(status),
        "value": dict(watch.last_value or {}), "summary": watch.last_summary or "not checked yet",
        "alert": dict(watch.alert or {}), "alert_text": registry.alert_text(watch.kind, watch.alert or {}),
        "watching_since": _iso(watch.created_at), "checked_at": _iso(watch.last_checked_at),
        "changed_at": _iso(watch.last_changed_at), "until": _iso(watch.until), "ends_text": ends_text(watch, tz),
        "created_via": watch.created_via,
    }


def sort_key(c: dict) -> tuple:
    return (LEVEL_ORDER.get(c["level"], 2), -(datetime.fromisoformat(c["changed_at"]).timestamp() if c["changed_at"] else 0),
            c["label"].lower())


async def user_payload(db: AsyncSession, customer_code: str, owner_object_id: str, *, now: datetime | None = None,
                       tz: ZoneInfo | None = None) -> dict:
    """The person's watchlist as the tab draws it: cards worst first, today's events, the unseen
    count, the places used, one suggestion."""
    now = now or datetime.now(timezone.utc)
    tz = tz or ZoneInfo(await get_customer_timezone(db, customer_code))
    watches = await store.active_for_owner(db, customer_code, owner_object_id)
    cards = sorted((card(w, tz) for w in watches), key=sort_key)
    since = registry.day_start(now, tz)
    events = await store.events_since(db, customer_code, owner_object_id, since)
    labels = {w.id: w for w in watches}
    missing = [e.watch_id for e in events if e.watch_id not in labels]
    if missing:
        for w in (await db.execute(select(Watch).where(Watch.id.in_(missing)))).scalars():
            labels[w.id] = w
    record = await records.access_record_for(db, customer_code, owner_object_id)
    suggestion = None
    limit = int(settings.teams_watches_limit)
    if record is not None and record.is_active and len(watches) < limit:
        try:
            suggestion = await suggest.suggestion_for(db, customer_code, record, now=now)
        except Exception:
            logger.warning("no suggestion for %s", owner_object_id, exc_info=True)
    counts = {"high": 0, "medium": 0, "fine": 0}
    for c in cards:
        counts[c["level"]] = counts.get(c["level"], 0) + 1
    return {
        "as_of": now.isoformat(), "object_id": owner_object_id, "limit": limit, "used": len(watches),
        "counts": {"needs_look": counts["high"], "keep_an_eye": counts["medium"], "fine": counts["fine"]},
        "watches": cards,
        "events_today": [{
            "id": str(e.id), "at": _iso(e.at), "watch_id": str(e.watch_id),
            "kind": labels[e.watch_id].kind if e.watch_id in labels else None,
            "label": labels[e.watch_id].label if e.watch_id in labels else "?",
            "old_status": e.old_status, "new_status": e.new_status, "status_text": registry.status_text(e.new_status),
            "level": registry.level_of(e.new_status), "summary": e.summary, "is_alert": e.is_alert,
            "seen": e.seen_at is not None,
        } for e in events],
        "unseen": await store.unseen_count(db, customer_code, owner_object_id),
        "suggestion": suggestion,
    }


async def mirror_user(db: AsyncSession, mirror: MirrorWriter, customer_code: str, owner_object_id: str, *,
                      tracker: ChangeTracker | None = None, now: datetime | None = None) -> bool:
    """Rewrite one person's row; whether it was written (unchanged rows are skipped)."""
    now = now or datetime.now(timezone.utc)
    payload = await user_payload(db, customer_code, owner_object_id, now=now)
    env = envelope("user_watches", customer_code, payload=payload, as_of=payload["as_of"], now=now.timestamp(),
                   user_id=owner_object_id)
    return await write_if_changed(mirror, tracker, env, now=now.timestamp())


async def watchables_payload(db: AsyncSession, customer_code: str, *, now: datetime | None = None,
                             tz: ZoneInfo | None = None) -> dict:
    """What can be watched right now, for the search box and the eye toggle; every entry carries
    the site that decides who may see it (None for things the records do not place)."""
    now = now or datetime.now(timezone.utc)
    tz = tz or ZoneInfo(await get_customer_timezone(db, customer_code))
    rows = await readers.open_rows(db, customer_code)
    deliveries = [{
        "entity_id": r.delivery_number, "label": f"{r.delivery_number} · {r.customer_name}" if r.customer_name else r.delivery_number,
        "detail": f"{registry.lines_words(int(r.lines_picked or 0), r.lines_expected, r.lines_confirmed, short=int(r.lines_short or 0))} · leaves {r.departure_at.astimezone(tz).strftime('%H:%M') if r.departure_at else '–'}",
        "route": r.route, "tier": r.tier, "site_id": r.warehouse, "departure_date": r.departure_date.isoformat() if r.departure_date else None,
    } for r in rows]
    routes = sorted({r.route for r in rows if r.route})
    aggregates = await readers.route_aggregates(db, customer_code, routes, now.astimezone(tz).date())
    route_rows = [{
        "entity_id": route, "label": f"Route {route}",
        "detail": f"{a['open']} open · {registry.lines_words(a['lines_picked'], a['lines_total'], None)}",
        "site_id": a["warehouses"][0] if len(a["warehouses"]) == 1 else None, "site_ids": a["warehouses"],
    } for route, a in ((r, aggregates[r]) for r in routes)]
    sites = await list_sites(db, customer_code)
    measures = [{"entity_id": f"fill_rate:{s.site_id}", "label": f"Fill rate · {s.name}", "site_id": s.site_id,
                 "detail": f"{registry.pct((readers.fill_rate(customer_code, s.site_id) or {}).get('fill_rate'))} today so far"} for s in sites]
    zones = await readers.zone_locations(db, customer_code)
    zone_rows = [{"entity_id": z, "label": f"Zone {z}", "detail": f"{len(locs)} locations", "site_id": None} for z, locs in zones.items()]
    location_rows = [{"entity_id": loc, "label": f"Location {loc}", "detail": f"zone {z}", "zone": z, "site_id": None}
                     for z, locs in zones.items() for loc in locs]
    items = [{"entity_id": item, "label": f"{name} · {item}", "detail": "", "site_id": None}
             for item, name in sorted((await readers.item_names(db, customer_code)).items())]
    return {"as_of": now.isoformat(), "deliveries": deliveries, "routes": route_rows, "measures": measures,
            "zones": zone_rows, "locations": location_rows, "products": items}


async def mirror_watchables(db: AsyncSession, mirror: MirrorWriter, customer_code: str, *,
                            tracker: ChangeTracker | None = None, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    payload = await watchables_payload(db, customer_code, now=now)
    env = envelope("watchables", customer_code, payload=payload, as_of=payload["as_of"], now=now.timestamp())
    return await write_if_changed(mirror, tracker, env, now=now.timestamp())


async def sweep_once(mirror: MirrorWriter, *, only: list[str] | None = None, tracker: ChangeTracker | None = None,
                     session_factory=None, now: datetime | None = None) -> dict[str, int]:
    """Rewrite every bound customer's WATCHABLES row and the row of everyone with a watch or an
    event today; rows written per customer."""
    from app.config.database import async_session
    from app.persistence.repositories.teams_repository import TeamsBindingRepository

    factory = session_factory or async_session
    now = now or datetime.now(timezone.utc)
    written: dict[str, int] = {}
    async with factory() as db:
        codes = list(only) if only is not None else sorted(
            {b.customer_code for b in await TeamsBindingRepository(db).list_all() if b.enabled})
        for code in codes:
            try:
                n = int(await mirror_watchables(db, mirror, code, tracker=tracker, now=now))
                tz = ZoneInfo(await get_customer_timezone(db, code))
                owners = set(await store.owners_with_watches(db, code))
                owners.update(await _owners_with_events_today(db, code, registry.day_start(now, tz)))
                for owner in sorted(owners):
                    n += int(await mirror_user(db, mirror, code, owner, tracker=tracker, now=now))
                written[code] = n
            except Exception:
                logger.exception("watch rows for %s not mirrored", code)
    return written


async def _owners_with_events_today(db: AsyncSession, customer_code: str, since: datetime) -> list[str]:
    from app.persistence.models.watch import WatchEvent

    rows = (await db.execute(select(WatchEvent.owner_object_id).where(
        WatchEvent.customer_code == customer_code, WatchEvent.at >= since).distinct())).scalars()
    return sorted(set(rows))


def watch_ids(payload: dict) -> set[uuid.UUID]:
    return {uuid.UUID(c["id"]) for c in payload.get("watches", [])}
