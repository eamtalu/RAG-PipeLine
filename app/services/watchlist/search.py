"""Finding what a person wants to watch (stage D, step 14): `resolve` turns a kind and an id into
the watch's label, site and reference (or says why not), `find` turns typed words into candidates
with their live state. Every candidate is filtered by the person's own sites. Used by commands.py
(adding), by the WATCHABLES row and, from step 14c, by Ask's watch tools.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy.ext.asyncio import AsyncSession

from app.persistence.models.watch import KINDS
from app.services.access.records import AccessRecord
from app.services.teams.sites import ALL_SITES_ID, list_sites
from app.services.watchlist import readers, registry

MEASURE_PREFIX = "fill_rate:"


class NotFound(ValueError):
    """Nothing of that kind with that id or name."""


class NoAccess(ValueError):
    """The thing exists but belongs to a site outside the person's access."""


@dataclass(frozen=True)
class Resolved:
    """A watchable thing, pinned down: what the watch row stores."""

    kind: str
    entity_id: str
    label: str
    site_id: str | None
    entity_ref: dict[str, Any] = field(default_factory=dict)
    detail: str = ""


def _check_site(record: AccessRecord, site_id: str | None, what: str) -> None:
    """Refuse a thing outside the person's sites with the words the page shows."""
    if site_id is None:
        return
    if not record.may_see(site_id):
        mine = ", ".join(record.site_ids) if record.site_ids else "no site"
        raise NoAccess(f"{what} belongs to {site_id}, and your access covers {mine} only")


async def resolve(db: AsyncSession, customer_code: str, record: AccessRecord, kind: str, entity_id: str, *,
                  tz: ZoneInfo) -> Resolved:
    """Pin down one thing by kind and id; NotFound or NoAccess with the reason.

    ("delivery", "D-48213") -> Resolved(label "D-48213 · Riverside Kitchens", site BRI, ref {row_id}) ;
    ("route", "R99") -> NotFound ; ("delivery" of another site) -> NoAccess
    """
    kind = str(kind or "").strip().lower()
    entity_id = str(entity_id or "").strip()
    if kind not in KINDS:
        raise NotFound(f"kind must be one of {', '.join(KINDS)}")
    if not entity_id:
        raise NotFound("name what to watch")
    if kind == "delivery":
        row = await readers.open_delivery(db, customer_code, entity_id)
        if row is None:
            raise NotFound(f"no open delivery {entity_id}")
        _check_site(record, row.warehouse, f"delivery {entity_id}")
        label = f"{row.delivery_number} · {row.customer_name}" if row.customer_name else row.delivery_number
        return Resolved("delivery", row.delivery_number, label, row.warehouse,
                        {"row_id": str(row.id), "route": row.route, "departure_date": row.departure_date.isoformat() if row.departure_date else None},
                        detail=f"{registry.lines_words(int(row.lines_picked or 0), row.lines_expected, row.lines_confirmed, short=int(row.lines_short or 0))} · leaves {registry.when_words(row.departure_at, datetime.now(timezone.utc), tz)}")
    if kind == "route":
        aggregates = await readers.route_aggregates(db, customer_code, [entity_id], datetime.now(tz).date())
        agg = aggregates.get(entity_id)
        if agg is None or (agg["open"] == 0 and agg["closed_today"] == 0):
            raise NotFound(f"no deliveries on route {entity_id} today")
        site = agg["warehouses"][0] if len(agg["warehouses"]) == 1 else None
        for warehouse in agg["warehouses"]:
            _check_site(record, warehouse, f"route {entity_id}")
        return Resolved("route", entity_id, f"Route {entity_id}", site, {"warehouses": agg["warehouses"]},
                        detail=f"{agg['open']} open · {registry.lines_words(agg['lines_picked'], agg['lines_total'], None, short=agg['lines_short'])}")
    if kind == "measure":
        site_id = entity_id[len(MEASURE_PREFIX):] if entity_id.startswith(MEASURE_PREFIX) else entity_id
        sites = {s.site_id: s.name for s in await list_sites(db, customer_code)}
        if site_id == ALL_SITES_ID:
            if not record.all_sites:
                raise NoAccess("the whole customer's fill rate needs access to every site")
            name = "all sites"
        elif site_id in sites:
            _check_site(record, site_id, "that site")
            name = sites[site_id]
        else:
            raise NotFound(f"no site {site_id}")
        latest = readers.fill_rate(customer_code, site_id)
        return Resolved("measure", f"{MEASURE_PREFIX}{site_id}", f"Fill rate · {name}", None if site_id == ALL_SITES_ID else site_id,
                        {"metric": "fill_rate", "site_id": site_id},
                        detail=f"{registry.pct(latest['fill_rate']) if latest else '–'} today so far")
    if kind == "product":
        names = await readers.item_names(db, customer_code, [entity_id])
        if entity_id not in names:
            raise NotFound(f"no item {entity_id} in the records")
        return Resolved("product", entity_id, f"{names[entity_id]} · {entity_id}", None, {"description": names[entity_id]})
    if kind == "location":
        zones = await readers.locations_matching(db, customer_code, entity_id, limit=50)
        zone = zones.get(entity_id)
        if entity_id not in zones:
            raise NotFound(f"no location {entity_id} in the records")
        return Resolved("location", entity_id, f"Location {entity_id}", None, {"zone": zone}, detail=f"zone {zone}" if zone else "")
    locations = (await readers.zone_locations(db, customer_code, [entity_id])).get(entity_id)
    if not locations:
        raise NotFound(f"no zone {entity_id} in the records")
    return Resolved("zone", entity_id, f"Zone {entity_id}", None, {"locations": locations},
                    detail=f"{len(locations)} locations")


def _route_code(text: str) -> str:
    """A route's bare code for matching: "route 14", "R14" and "r14" are all "14".

    >>> _route_code("Route 14"), _route_code("r14"), _route_code("14")
    ('14', '14', '14')
    """
    return text.lower().replace("route", "").strip().lstrip("r").strip().upper()


code_of = _route_code


@dataclass(frozen=True)
class Candidate:
    kind: str
    entity_id: str
    label: str
    detail: str
    site_id: str | None


async def find(db: AsyncSession, customer_code: str, record: AccessRecord, text: str, *, kind: str | None = None,
               tz: ZoneInfo, limit: int = 8) -> list[Candidate]:
    """Candidates for typed words, exact ids first, within the person's sites.

    ("route 14") -> [Candidate(route, "R14", ...)] ; ("ginger") -> [Candidate(product, "104568", "Ginger · 104568")]
    """
    needle = " ".join(str(text or "").split()).strip()
    if not needle:
        return []
    kinds = [kind] if kind in KINDS else list(KINDS)
    words = needle.lower()
    out: list[Candidate] = []
    if "delivery" in kinds:
        for row in await readers.open_rows(db, customer_code):
            hay = f"{row.delivery_number} {row.customer_name or ''}".lower()
            if words in hay and record.may_see(row.warehouse):
                label = f"{row.delivery_number} · {row.customer_name}" if row.customer_name else row.delivery_number
                out.append(Candidate("delivery", row.delivery_number, label,
                                     f"{registry.lines_words(int(row.lines_picked or 0), row.lines_expected, row.lines_confirmed, short=int(row.lines_short or 0))} · leaves {registry.when_words(row.departure_at, datetime.now(timezone.utc), tz)}",
                                     row.warehouse))
    if "route" in kinds:
        code = _route_code(words)
        rows = await readers.open_rows(db, customer_code)
        routes = sorted({r.route for r in rows if r.route and code and _route_code(r.route).startswith(code)})
        aggregates = await readers.route_aggregates(db, customer_code, routes, datetime.now(tz).date())
        for route in routes:
            agg = aggregates[route]
            if all(record.may_see(w) for w in agg["warehouses"]):
                out.append(Candidate("route", route, f"Route {route}", f"{agg['open']} open · {registry.lines_words(agg['lines_picked'], agg['lines_total'], None, short=agg['lines_short'])}",
                                     agg["warehouses"][0] if len(agg["warehouses"]) == 1 else None))
    if "measure" in kinds and ("fill" in words or "rate" in words):
        for site in await list_sites(db, customer_code):
            if record.may_see(site.site_id):
                latest = readers.fill_rate(customer_code, site.site_id)
                out.append(Candidate("measure", f"{MEASURE_PREFIX}{site.site_id}", f"Fill rate · {site.name}",
                                     f"{registry.pct(latest['fill_rate']) if latest else '–'} today so far", site.site_id))
    if "product" in kinds:
        for item, name in (await readers.items_matching(db, customer_code, needle, limit=limit)).items():
            out.append(Candidate("product", item, f"{name} · {item}", "", None))
    if "location" in kinds:
        for location, zone in (await readers.locations_matching(db, customer_code, needle, limit=limit)).items():
            out.append(Candidate("location", location, f"Location {location}", f"zone {zone}" if zone else "", None))
    if "zone" in kinds:
        for zone, locations in (await readers.zone_locations(db, customer_code)).items():
            if words in f"zone {zone}".lower():
                out.append(Candidate("zone", zone, f"Zone {zone}", f"{len(locations)} locations", None))
    exact = [c for c in out if c.entity_id.lower() == words or (c.kind == "route" and _route_code(c.entity_id) == code_of(words))]
    rest = [c for c in out if c not in exact]
    return (exact + rest)[:limit]
